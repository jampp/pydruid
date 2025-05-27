# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import warnings
from collections import namedtuple
from io import BytesIO
from unittest.mock import patch

from requests.models import Response

from pydruid.db.api import apply_parameters, Cursor
from pydruid.db import exceptions

class CursorTestSuite(unittest.TestCase):
    @patch("requests.post")
    def test_execute(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["name"]\n["alice"]\n["bob"]\n["charlie"]\n\n')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        cursor = Cursor("http://example.com/")
        cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])
        result = cursor.fetchall()
        expected = [Row(name="alice"), Row(name="bob"), Row(name="charlie")]
        self.assertEqual(result, expected)

    @patch("requests.post")
    def test_execute_error(self, requests_post_mock):
        response = Response()
        response.status_code = 500
        response.raw = BytesIO(b'{"error": "Internal Server Error", "errorClass": "some error class", "errorMessage": "some error message"}')
        requests_post_mock.return_value = response

        cursor = Cursor("http://example.com/")
        with self.assertRaises(exceptions.ProgrammingError) as cm:
            cursor.execute("SELECT * FROM table")
        self.assertEqual(cm.exception.args[0], "Internal Server Error (some error class): some error message")

    @patch("requests.post")
    def test_execute_error_no_errorClass(self, requests_post_mock):
        response = Response()
        response.status_code = 500
        response.raw = BytesIO(b'{"error": "druidException", "errorCode": "invalidInput", "persona": "USER", "category": "INVALID_INPUT", "errorMessage": "Aggregate expressions cannot be nested (line [1], column [215])", "context": {"sourceType": "sql", "line": "1", "column": "215", "endLine": "1", "endColumn": "1691"}}')
        requests_post_mock.return_value = response

        cursor = Cursor("http://example.com/")
        with self.assertRaises(exceptions.ProgrammingError) as cm:
            cursor.execute("SELECT * FROM table")
        self.assertEqual(cm.exception.args[0], "druidException (invalidInput): Aggregate expressions cannot be nested (line [1], column [215])")


    @patch("requests.post")
    def test_execute_empty_result(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["name"]\n\n')
        requests_post_mock.return_value = response

        cursor = Cursor("http://example.com/")
        cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])
        result = cursor.fetchall()
        expected = []
        self.assertEqual(result, expected)

    @patch("requests.post")
    def test_truncated_response(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["name"]\n["alice"]\n')
        requests_post_mock.return_value = response

        cursor = Cursor("http://example.com/")
        cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])

        with self.assertRaises(ValueError) as cm:
            cursor.fetchall()

        self.assertEqual(
            cm.exception.args[0], "Truncated response. Trailer line not found."
        )

    @patch("requests.post")
    def test_context(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b"[]")
        requests_post_mock.return_value = response

        url = "http://example.com/"
        query = "SELECT * FROM table"
        context = {"source": "unittest"}

        cursor = Cursor(url, user=None, password=None, context=context)
        cursor.execute(query)

        requests_post_mock.assert_called_with(
            "http://example.com/",
            stream=True,
            headers={"Content-Type": "application/json"},
            json={
                "query": query,
                "context": context,
                "header": True,
                "resultFormat": "arrayLines",
            },
            verify=True,
            cert=None,
            proxies=None,
            timeout=None,
        )

    @patch("requests.post")
    def test_header_false(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["name"]\n["alice"]\n\n')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")

            cursor = Cursor("http://example.com/", header=False)

        self.assertIn(
            "Disabling the `header` parameter is not supported in this version of the lib."  # noqa: E501
            " The value will be ignored and we will force `header=True`.",
            str(warning_list[-1].message),
        )

        cursor.execute("SELECT * FROM table")
        result = cursor.fetchall()
        self.assertEqual(result, [Row(name="alice")])
        self.assertEqual(cursor.description, [("name", None)])

    @patch("requests.post")
    def test_header_true(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["name"]\n["alice"]\n\n')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, header=True)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEqual(result, [Row(name="alice")])
        self.assertEqual(cursor.description, [("name", None)])

    @patch("requests.post")
    def test_names_with_underscores(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'["_name"]\n["alice"]\n\n')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["_name"], rename=True)

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEqual(result, [Row(_0="alice")])
        self.assertEqual(cursor.description, [("_name", None)])

    def test_apply_parameters(self):
        self.assertEqual(
            apply_parameters('SELECT 100 AS "100%"', None), 'SELECT 100 AS "100%"'
        )

        self.assertEqual(
            apply_parameters('SELECT 100 AS "100%"', {}), 'SELECT 100 AS "100%"'
        )

        self.assertEqual(
            apply_parameters('SELECT %(key)s AS "100%%"', {"key": 100}),
            'SELECT 100 AS "100%"',
        )

        self.assertEqual(apply_parameters("SELECT %(key)s", {"key": "*"}), "SELECT *")

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": "bar"}), "SELECT 'bar'"
        )

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": True}), "SELECT TRUE"
        )

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": False}), "SELECT FALSE"
        )


if __name__ == "__main__":
    unittest.main()
