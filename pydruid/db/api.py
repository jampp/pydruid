import asyncio
import functools
import itertools
import warnings
from base64 import b64encode
from collections import namedtuple
from urllib import parse

import requests
from ujson import loads as ujson_loads

from pydruid.db import exceptions


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(
    host="localhost",
    port=8082,
    path="/druid/v2/sql/",
    scheme="http",
    user=None,
    password=None,
    context=None,
    header=None,
    ssl_verify_cert=True,
    ssl_client_cert=None,
    proxies=None,
    timeout=None,
):  # noqa: E125
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    context = context or {}

    return Connection(
        host,
        port,
        path,
        scheme,
        user,
        password,
        context,
        header,
        ssl_verify_cert,
        ssl_client_cert,
        proxies,
        timeout,
    )


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    if asyncio.iscoroutinefunction(f):

        async def g(self, *args, **kwargs):
            if self.closed:
                raise exceptions.Error(
                    "{klass} already closed".format(klass=self.__class__.__name__)
                )

            return await f(self, *args, **kwargs)

    else:

        def g(self, *args, **kwargs):
            if self.closed:
                raise exceptions.Error(
                    "{klass} already closed".format(klass=self.__class__.__name__)
                )

            return f(self, *args, **kwargs)

    return functools.wraps(f)(g)


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    if asyncio.iscoroutinefunction(f):

        async def g(self, *args, **kwargs):
            if self._results is None:
                raise exceptions.Error("Called before `execute`")
            return await f(self, *args, **kwargs)

    else:

        def g(self, *args, **kwargs):
            if self._results is None:
                raise exceptions.Error("Called before `execute`")
            return f(self, *args, **kwargs)

    return functools.wraps(f)(g)


def get_description_from_row(row):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            name,  # name
            get_type(value),  # type_code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            get_type(value) == Type.STRING,  # [null_ok]
        )
        for name, value in row.items()
    ]


def get_type(value):
    """
    Infer type from value.

    Note that bool is a subclass of int so order of statements matter.
    """

    if isinstance(value, str) or value is None:
        return Type.STRING
    elif isinstance(value, bool):
        return Type.BOOLEAN
    elif isinstance(value, (int, float)):
        return Type.NUMBER

    raise exceptions.Error("Value of unknown type: {value}".format(value=value))


class BaseConnection(object):
    def __init__(
        self,
        host="localhost",
        port=8082,
        path="/druid/v2/sql/",
        scheme="http",
        user=None,
        password=None,
        context=None,
        header=None,
        ssl_verify_cert=True,
        ssl_client_cert=None,
        proxies=None,
        timeout=None,
    ):
        netloc = "{host}:{port}".format(host=host, port=port)
        self.url = parse.urlunparse((scheme, netloc, path, None, None, None))
        self.context = context or {}
        self.closed = False
        self.cursors = []
        self.header = header
        self.user = user
        self.password = password
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_client_cert = ssl_client_cert
        self.proxies = proxies
        self.timeout = timeout

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new cursor Object using the connection."""
        raise NotImplementedError("Subclasses must implement this method")

    @check_closed
    def execute(self, operation, parameters=None):
        raise NotImplementedError("Subclasses must implement this method")

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Connection(BaseConnection):
    """Connection to a Druid database."""

    @check_closed
    def cursor(self):
        cursor = Cursor(
            self.url,
            self.user,
            self.password,
            self.context,
            self.header,
            self.ssl_verify_cert,
            self.ssl_client_cert,
            self.proxies,
            self.timeout,
        )

        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)


class BaseCursor(object):
    def __init__(
        self,
        url,
        user=None,
        password=None,
        context=None,
        header=None,
        ssl_verify_cert=True,
        proxies=None,
        ssl_client_cert=None,
        timeout=None,
    ):
        if header is not None and not header:
            warnings.warn(
                "Disabling the `header` parameter is not supported in this version of the lib."  # noqa: E501
                " The value will be ignored and we will force `header=True`.",
            )

        self.url = url
        self.context = context or {}
        self.user = user
        self.password = password
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_client_cert = ssl_client_cert
        self.proxies = proxies
        self.timeout = timeout

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to an iterator after a successfull query
        self._results = None

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @property
    @check_result
    @check_closed
    def rowcount(self):
        # consume the iterator
        raise NotImplementedError("Subclasses must implement this method")

    @check_closed
    def execute(self, operation, parameters=None):
        raise NotImplementedError("Subclasses must implement this method")

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead"
        )

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    def _stream_query(self, query):
        """
        Stream rows from a query.

        This method will yield rows as the data is returned from the server.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def _prepare_headers_and_payload(self, query):
        headers = {"Content-Type": "application/json"}

        payload = {
            "query": query,
            "context": self.context,
            "header": True,
            "resultFormat": "arrayLines",
        }

        if self.user is not None:
            authstring = "{}:{}".format(self.user, self.password)
            b64string = b64encode(authstring.encode()).decode()
            headers["Authorization"] = "Basic {}".format(b64string)

        return headers, payload

    @staticmethod
    def _handle_http_error(response):
        try:
            payload = response.json()
        except Exception:
            payload = {
                "error": "Unknown error",
                "errorClass": "Unknown",
                "errorMessage": response.text,
            }
        if "errorClass" not in payload and "errorCode" in payload:
            payload["errorClass"] = payload["errorCode"]
        msg = "{error} ({errorClass}): {errorMessage}".format(**payload)
        raise exceptions.ProgrammingError(msg)

    def _set_description(self, field_names):
        self.description = [(name, None) for name in field_names]


class Cursor(BaseCursor):
    """Connection cursor."""

    @property
    @check_result
    @check_closed
    def rowcount(self):
        # consume the iterator
        results = list(self._results)
        n = len(results)
        self._results = iter(results)
        return n

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters)
        results = self._stream_query(query)

        # `_stream_query` returns a generator that produces the rows.
        # We need to consume it once so the query is executed and the
        # `description` is properly set.
        next(results)

        self._results = results

        return self

    @check_result
    @check_closed
    def fetchone(self):
        try:
            return self.next()
        except StopIteration:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        size = size or self.arraysize
        return list(itertools.islice(self._results, size))

    @check_result
    @check_closed
    def fetchall(self):
        return list(self._results)

    @check_result
    @check_closed
    def __next__(self):
        return next(self._results)

    next = __next__

    @check_closed
    def __iter__(self):
        return self

    def _stream_query(self, query):
        self.description = None

        headers, payload = self._prepare_headers_and_payload(query)

        r = requests.post(
            self.url,
            stream=True,
            headers=headers,
            json=payload,
            verify=self.ssl_verify_cert,
            cert=self.ssl_client_cert,
            proxies=self.proxies,
            timeout=self.timeout,
        )
        if r.encoding is None:
            r.encoding = "utf-8"
        # raise any error messages
        if r.status_code != 200:
            self._handle_http_error(r)

        # Druid will stream the data in chunks of 8k bytes
        # setting `chunk_size` to `None` makes it use the server size
        lines = r.iter_lines(chunk_size=None, decode_unicode=False)

        field_names = ujson_loads(next(lines))
        Row = namedtuple("Row", field_names, rename=True)
        make_row = Row._make

        yield self._set_description(field_names)

        for row in lines:
            if not row:
                break

            yield make_row(ujson_loads(row))
        else:
            raise ValueError("Truncated response. Trailer line not found.")


def apply_parameters(operation, parameters):
    if not parameters:
        return operation

    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value):
    """
    Escape the parameter value.

    Note that bool is a subclass of int so order of statements matter.
    """

    if value == "*":
        return value
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)
