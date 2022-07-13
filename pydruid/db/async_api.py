from collections import namedtuple

from ujson import loads as ujson_loads

try:
    import httpx
except ImportError:
    print("Warning: unable to import HTTPX. The asynchronous api will not work.")


from pydruid.db.api import (
    BaseConnection,
    BaseCursor,
    apply_parameters,
    check_closed,
    check_result,
)


def async_connect(
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
    Constructor for creating an async connection to the database.

        >>> conn = async_connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    context = context or {}

    return AsyncConnection(
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


class AsyncConnection(BaseConnection):
    """Async connection to a Druid database."""

    @check_closed
    def cursor(self):
        cursor = AsyncCursor(
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
    async def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return await cursor.execute(operation, parameters)


class AsyncCursor(BaseCursor):
    """AsyncConnection cursor."""

    @property
    @check_result
    @check_closed
    async def rowcount(self):
        # consume the iterator
        results = await self.fetchall()
        n = len(results)

        async def _aiter():
            for row in results:
                yield row

        self._results = _aiter()
        return n

    @check_closed
    async def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters)
        results = self._stream_query(query)

        # `_stream_query` returns a generator that produces the rows.
        # We need to consume it once so the query is executed and the
        # `description` is properly set.
        await results.__anext__()

        self._results = results

        return self

    @check_result
    @check_closed
    async def fetchone(self):
        try:
            return await self.anext()
        except StopAsyncIteration:
            return None

    @check_result
    @check_closed
    async def fetchmany(self, size=None):
        size = size or self.arraysize
        rows = []
        async for i, row in self._results:
            rows.append(row)
            if i >= size - 1:
                break

        return rows

    @check_result
    @check_closed
    async def fetchall(self):
        return [row async for row in self._results]

    @check_result
    @check_closed
    def __anext__(self):
        return self._results.__anext__()

    anext = __anext__

    @check_closed
    def __aiter__(self):
        return self

    async def _stream_query(self, query):
        self.description = None

        headers, payload = self._prepare_headers_and_payload(query)

        http_client = httpx.AsyncClient(
            headers=headers,
            verify=self.ssl_verify_cert,
            cert=self.ssl_client_cert,
            proxies=self.proxies,
            timeout=self.timeout,
        )

        async with http_client.stream("POST", self.url, json=payload) as response:
            if response.encoding is None:
                response.encoding = "utf-8"
            # raise any error messages
            if response.status_code != 200:
                await response.aread()
                self._handle_http_error(response)

            # Druid will stream the data in chunks of 8k bytes
            # setting `chunk_size` to `None` makes it use the server size
            lines = self._aiter_lines(response, chunk_size=None)

            field_names = ujson_loads(await lines.__anext__())
            Row = namedtuple("Row", field_names, rename=True)
            make_row = Row._make

            self.description = [(name, None) for name in field_names]

            yield None

            async for row in lines:
                if not row:
                    break

                yield make_row(ujson_loads(row))
            else:
                raise ValueError("Truncated response. Trailer line not found.")

    @staticmethod
    async def _aiter_lines(response, chunk_size=None):
        # HTTPX aiter_lines implementation is not compatible with requests'
        # iter_lines implementation

        pending = None

        async for chunk in response.aiter_bytes(chunk_size=chunk_size):
            if pending is not None:
                chunk = pending + chunk

            lines = chunk.splitlines()

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield line

        if pending is not None:
            yield pending
