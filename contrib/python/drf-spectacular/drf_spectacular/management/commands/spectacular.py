from textwrap import dedent

from django.core.management.base import BaseCommand, CommandError
from django.utils import translation
from django.utils.module_loading import import_string

from drf_spectacular.drainage import GENERATOR_STATS
from drf_spectacular.renderers import OpenApiJsonRenderer, OpenApiYamlRenderer
from drf_spectacular.settings import patched_settings, spectacular_settings
from drf_spectacular.validation import validate_schema


class SchemaGenerationError(CommandError):
    pass


class SchemaValidationError(CommandError):
    pass


class Command(BaseCommand):
    help = dedent("""
        Generate a spectacular OpenAPI3-compliant schema for your API.

        The warnings serve as a indicator for where your API could not be properly
        resolved. @extend_schema and @extend_schema_field are your friends.
        The spec should be valid in any case. If not, please open an issue
        on github: https://github.com/tfranzel/drf-spectacular/issues

        Remember to configure your APIs meta data like servers, version, url,
        documentation and so on in your SPECTACULAR_SETTINGS."
    """)

    def add_arguments(self, parser):
        parser.add_argument('--format', dest="format", choices=['openapi', 'openapi-json'], default='openapi', type=str)
        parser.add_argument('--urlconf', dest="urlconf", default=None, type=str)
        parser.add_argument('--generator-class', dest="generator_class", default=None, type=str)
        parser.add_argument('--file', dest="file", default=None, type=str)
        parser.add_argument('--fail-on-warn', dest="fail_on_warn", default=False, action='store_true')
        parser.add_argument('--validate', dest="validate", default=False, action='store_true')
        parser.add_argument('--api-version', dest="api_version", default=None, type=str)
        parser.add_argument('--lang', dest="lang", default=None, type=str)
        parser.add_argument('--color', dest="color", default=False, action='store_true')
        parser.add_argument('--custom-settings', dest="custom_settings", default=None, type=str)

    def handle(self, *args, **options):
        if options['generator_class']:
            generator_class = import_string(options['generator_class'])
        else:
            generator_class = spectacular_settings.DEFAULT_GENERATOR_CLASS

        GENERATOR_STATS.enable_trace_lineno()

        if options['color']:
            GENERATOR_STATS.enable_color()

        generator = generator_class(
            urlconf=options['urlconf'],
            api_version=options['api_version'],
        )

        if options['custom_settings']:
            custom_settings = import_string(options['custom_settings'])
        else:
            custom_settings = None

        with patched_settings(custom_settings):
            if options['lang']:
                with translation.override(options['lang']):
                    schema = generator.get_schema(request=None, public=True)
            else:
                schema = generator.get_schema(request=None, public=True)

        GENERATOR_STATS.emit_summary()

        if options['fail_on_warn'] and GENERATOR_STATS:
            raise SchemaGenerationError('Failing as requested due to warnings')
        if options['validate']:
            try:
                validate_schema(schema)
            except Exception as e:
                raise SchemaValidationError(e)

        renderer = self.get_renderer(options['format'])
        output = renderer.render(schema, renderer_context={})

        if options['file']:
            with open(options['file'], 'wb') as f:
                f.write(output)
        else:
            self.stdout.write(output.decode())

    def get_renderer(self, format):
        renderer_cls = {
            'openapi': OpenApiYamlRenderer,
            'openapi-json': OpenApiJsonRenderer,
        }[format]
        return renderer_cls()
