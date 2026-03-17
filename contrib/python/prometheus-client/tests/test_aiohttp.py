from __future__ import annotations

import gzip
from typing import TYPE_CHECKING
from unittest import skipUnless

from prometheus_client import CollectorRegistry, Counter
from prometheus_client.exposition import CONTENT_TYPE_PLAIN_0_0_4

try:
    from aiohttp import ClientResponse, hdrs, web
    from aiohttp.test_utils import AioHTTPTestCase

    from prometheus_client.aiohttp import make_aiohttp_handler

    AIOHTTP_INSTALLED = True
except ImportError:
    if TYPE_CHECKING:
        assert False

    from unittest import IsolatedAsyncioTestCase as AioHTTPTestCase

    AIOHTTP_INSTALLED = False


class AioHTTPTest(AioHTTPTestCase):
    @skipUnless(AIOHTTP_INSTALLED, "AIOHTTP is not installed")
    def setUp(self) -> None:
        self.registry = CollectorRegistry()

    async def get_application(self) -> web.Application:
        app = web.Application()
        # The AioHTTPTestCase requires that applications be static, so we need
        # both versions to be available so the test can choose between them
        app.router.add_get("/metrics", make_aiohttp_handler(self.registry))
        app.router.add_get(
            "/metrics_uncompressed",
            make_aiohttp_handler(self.registry, disable_compression=True),
        )
        return app

    def increment_metrics(
        self,
        metric_name: str,
        help_text: str,
        increments: int,
    ) -> None:
        c = Counter(metric_name, help_text, registry=self.registry)
        for _ in range(increments):
            c.inc()

    def assert_metrics(
        self,
        output: str,
        metric_name: str,
        help_text: str,
        increments: int,
    ) -> None:
        self.assertIn("# HELP " + metric_name + "_total " + help_text + "\n", output)
        self.assertIn("# TYPE " + metric_name + "_total counter\n", output)
        self.assertIn(metric_name + "_total " + str(increments) + ".0\n", output)

    def assert_not_metrics(
        self,
        output: str,
        metric_name: str,
        help_text: str,
        increments: int,
    ) -> None:
        self.assertNotIn("# HELP " + metric_name + "_total " + help_text + "\n", output)
        self.assertNotIn("# TYPE " + metric_name + "_total counter\n", output)
        self.assertNotIn(metric_name + "_total " + str(increments) + ".0\n", output)

    async def assert_outputs(
        self,
        response: ClientResponse,
        metric_name: str,
        help_text: str,
        increments: int,
    ) -> None:
        self.assertIn(
            CONTENT_TYPE_PLAIN_0_0_4,
            response.headers.getall(hdrs.CONTENT_TYPE),
        )
        output = await response.text()
        self.assert_metrics(output, metric_name, help_text, increments)

    async def validate_metrics(
        self,
        metric_name: str,
        help_text: str,
        increments: int,
    ) -> None:
        """
        AIOHTTP handler serves the metrics from the provided registry.
        """
        self.increment_metrics(metric_name, help_text, increments)
        async with self.client.get("/metrics") as response:
            response.raise_for_status()
            await self.assert_outputs(response, metric_name, help_text, increments)

    async def test_report_metrics_1(self):
        await self.validate_metrics("counter", "A counter", 2)

    async def test_report_metrics_2(self):
        await self.validate_metrics("counter", "Another counter", 3)

    async def test_report_metrics_3(self):
        await self.validate_metrics("requests", "Number of requests", 5)

    async def test_report_metrics_4(self):
        await self.validate_metrics("failed_requests", "Number of failed requests", 7)

    async def test_gzip(self):
        # Increment a metric.
        metric_name = "counter"
        help_text = "A counter"
        increments = 2
        self.increment_metrics(metric_name, help_text, increments)

        async with self.client.get(
            "/metrics",
            auto_decompress=False,
            headers={hdrs.ACCEPT_ENCODING: "gzip"},
        ) as response:
            response.raise_for_status()
            self.assertIn(hdrs.CONTENT_ENCODING, response.headers)
            self.assertIn("gzip", response.headers.getall(hdrs.CONTENT_ENCODING))
            body = await response.read()
            output = gzip.decompress(body).decode("utf8")
            self.assert_metrics(output, metric_name, help_text, increments)

    async def test_gzip_disabled(self):
        # Increment a metric.
        metric_name = "counter"
        help_text = "A counter"
        increments = 2
        self.increment_metrics(metric_name, help_text, increments)

        async with self.client.get(
            "/metrics_uncompressed",
            auto_decompress=False,
            headers={hdrs.ACCEPT_ENCODING: "gzip"},
        ) as response:
            response.raise_for_status()
            self.assertNotIn(hdrs.CONTENT_ENCODING, response.headers)
            output = await response.text()
            self.assert_metrics(output, metric_name, help_text, increments)

    async def test_openmetrics_encoding(self):
        """Response content type is application/openmetrics-text when appropriate Accept header is in request"""
        async with self.client.get(
            "/metrics",
            auto_decompress=False,
            headers={hdrs.ACCEPT: "application/openmetrics-text; version=1.0.0"},
        ) as response:
            response.raise_for_status()
            self.assertEqual(
                response.headers.getone(hdrs.CONTENT_TYPE).split(";", maxsplit=1)[0],
                "application/openmetrics-text",
            )

    async def test_plaintext_encoding(self):
        """Response content type is text/plain when Accept header is missing in request"""
        async with self.client.get("/metrics") as response:
            response.raise_for_status()
            self.assertEqual(
                response.headers.getone(hdrs.CONTENT_TYPE).split(";", maxsplit=1)[0],
                "text/plain",
            )

    async def test_qs_parsing(self):
        """Only metrics that match the 'name[]' query string param appear"""

        metrics = [("asdf", "first test metric", 1), ("bsdf", "second test metric", 2)]

        for m in metrics:
            self.increment_metrics(*m)

        for i_1 in range(len(metrics)):
            async with self.client.get(
                "/metrics",
                params={"name[]": f"{metrics[i_1][0]}_total"},
            ) as response:
                output = await response.text()
                self.assert_metrics(output, *metrics[i_1])

                for i_2 in range(len(metrics)):
                    if i_1 == i_2:
                        continue

                    self.assert_not_metrics(output, *metrics[i_2])
