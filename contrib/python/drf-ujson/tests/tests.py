from unittest import TestCase
from io import BytesIO

from django.conf import settings
import ujson

settings.configure()

from drf_ujson.renderers import UJSONRenderer
from drf_ujson.parsers import UJSONParser


class UJSONRendererTests(TestCase):
    def setUp(self):
        self.renderer = UJSONRenderer()
        self.data = {
            'a': [1, 2, 3],
            'b': True,
            'c': 1.23,
            'd': 'test',
            'e': {'foo': 'bar'},
        }

    def test_basic_data_structures_rendered_correctly(self):

        rendered = self.renderer.render(self.data)
        reloaded = ujson.loads(rendered)

        self.assertEqual(reloaded, self.data)

    def test_renderer_works_correctly_when_media_type_and_context_provided(self):

        rendered = self.renderer.render(
            data=self.data,
            media_type='application/json',
            renderer_context={},
        )
        reloaded = ujson.loads(rendered)

        self.assertEqual(reloaded, self.data)


class UJSONParserTests(TestCase):
    def setUp(self):
        self.parser = UJSONParser()
        self.data = {
            'a': [1, 2, 3],
            'b': True,
            'c': 1.23,
            'd': 'test',
            'e': {'foo': 'bar'},
        }

    def test_basic_data_structures_parsed_correctly(self):

        dumped = ujson.dumps(self.data)
        parsed = self.parser.parse(BytesIO(dumped.encode('utf-8')))

        self.assertEqual(parsed, self.data)

    def test_parser_works_correctly_when_media_type_and_context_provided(self):
        dumped = ujson.dumps(self.data)
        parsed = self.parser.parse(
            stream=BytesIO(dumped.encode('utf-8')),
            media_type='application/json',
            parser_context={},
        )

        self.assertEqual(parsed, self.data)
