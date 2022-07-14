# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import unittest
from collections import namedtuple

import tornado.web
from tornado.testing import AsyncHTTPTestCase, gen_test

from pydruid.db.async_api import AsyncCursor


class DruidMockServer(tornado.web.Application):
    mock_response_status_code: int = 200
    mock_response_body: str = ""
    received_request: dict = {}

    def set_mock_response(self, status_code, body):
        self.mock_response_status_code = status_code
        self.mock_response_body = body


class DruidSQLHandler(tornado.web.RequestHandler):
    def post(self):
        self.application.received_request = {
            "headers": dict(self.request.headers),
            "body": self.request.body,
        }
        self.set_status(self.application.mock_response_status_code)
        self.write(self.application.mock_response_body)


class AsyncCursorTestSuite(AsyncHTTPTestCase):
    def get_app(self):
        return DruidMockServer(
            [
                (r"/druid/v2/sql", DruidSQLHandler),
            ]
        )

    def get_sql_endpoint_url(self):
        return f"http://localhost:{self.get_http_port()}/druid/v2/sql"

    def set_mock_response(self, status_code, body):
        self._app.set_mock_response(status_code, body)

    def get_received_request(self):
        return self._app.received_request

    @gen_test
    async def test_execute(self):
        self.set_mock_response(200, '["name"]\n["alice"]\n["bob"]\n["charlie"]\n\n')
        Row = namedtuple("Row", ["name"])

        cursor = AsyncCursor(self.get_sql_endpoint_url())
        await cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])
        result = await cursor.fetchall()
        expected = [Row(name="alice"), Row(name="bob"), Row(name="charlie")]
        self.assertEqual(result, expected)

    @gen_test
    async def test_execute_empty_result(self):
        self.set_mock_response(200, '["name"]\n\n')

        cursor = AsyncCursor(self.get_sql_endpoint_url())
        await cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])
        result = await cursor.fetchall()
        expected = []
        self.assertEqual(result, expected)

    @gen_test
    async def test_truncated_response(self):
        self.set_mock_response(200, '["name"]\n["alice"]\n')

        cursor = AsyncCursor(self.get_sql_endpoint_url())
        await cursor.execute("SELECT * FROM table")
        self.assertEqual(cursor.description, [("name", None)])

        with self.assertRaises(ValueError) as cm:
            await cursor.fetchall()

        self.assertEqual(
            cm.exception.args[0], "Truncated response. Trailer line not found."
        )

    @gen_test
    async def test_context(self):
        self.set_mock_response(200, "[]")

        query = "SELECT * FROM table"
        context = {"source": "unittest"}

        cursor = AsyncCursor(self.get_sql_endpoint_url(), context=context)
        await cursor.execute(query)

        request = self.get_received_request()
        self.assertDictEqual(
            json.loads(request["body"]),
            {
                "query": query,
                "context": context,
                "header": True,
                "resultFormat": "arrayLines",
            },
        )


if __name__ == "__main__":
    unittest.main()
