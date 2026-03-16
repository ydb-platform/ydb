import logging
import os

from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string
from rest_framework.settings import api_settings
from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework.views import APIView

from ... import openapi
from ...app_settings import swagger_settings
from ...codecs import OpenAPICodecJson, OpenAPICodecYaml


class Command(BaseCommand):
    help = "Write the Swagger schema to disk in JSON or YAML format."

    def add_arguments(self, parser):
        parser.add_argument(
            "output_file",
            metavar="output-file",
            nargs="?",
            default="-",
            type=str,
            help='Output path for generated swagger document, or "-" for stdout.',
        )
        parser.add_argument(
            "-o",
            "--overwrite",
            default=False,
            action="store_true",
            help="Overwrite the output file if it already exists. "
            "Default behavior is to stop if the output file exists.",
        )
        parser.add_argument(
            "-f",
            "--format",
            dest="format",
            default="",
            choices=["json", "yaml"],
            type=str,
            help="Output format. If not given, it is guessed from the output file "
            "extension and defaults to json.",
        )
        parser.add_argument(
            "-u",
            "--url",
            dest="api_url",
            default="",
            type=str,
            help="Base API URL - sets the host and scheme attributes of the generated "
            "document.",
        )
        parser.add_argument(
            "-m",
            "--mock-request",
            dest="mock",
            default=False,
            action="store_true",
            help="Use a mock request when generating the swagger schema. This is "
            "useful if your views or serializers depend on context from a request in "
            "order to function.",
        )
        parser.add_argument(
            "--api-version",
            dest="api_version",
            type=str,
            help="Version to use to generate schema. This option implies "
            "--mock-request.",
        )
        parser.add_argument(
            "--user",
            dest="user",
            help="Username of an existing user to use for mocked authentication. This "
            "option implies --mock-request.",
        )
        parser.add_argument(
            "-p",
            "--private",
            default=False,
            action="store_true",
            help="Hides endpoints not accessible to the target user. If --user is not "
            "given, only shows endpoints that are accessible to unauthenticated users."
            "\n"
            "This has the same effect as passing public=False to get_schema_view() or "
            "OpenAPISchemaGenerator.get_schema().\n"
            "This option implies --mock-request.",
        )
        parser.add_argument(
            "-g",
            "--generator-class",
            dest="generator_class_name",
            default="",
            help="Import string pointing to an OpenAPISchemaGenerator subclass to use "
            "for schema generation.",
        )

    def write_schema(self, schema, stream, format):
        if format == "json":
            codec = OpenAPICodecJson(validators=[], pretty=True)
            swagger_json = codec.encode(schema).decode("utf-8")
            stream.write(swagger_json)
        elif format == "yaml":
            codec = OpenAPICodecYaml(validators=[])
            swagger_yaml = codec.encode(schema).decode("utf-8")
            # YAML is already pretty!
            stream.write(swagger_yaml)
        else:  # pragma: no cover
            raise ValueError("unknown format %s" % format)

    def get_mock_request(self, url, format, user=None):
        factory = APIRequestFactory()

        request = factory.get(url + "/swagger." + format)
        if user is not None:
            force_authenticate(request, user=user)
        request = APIView().initialize_request(request)
        return request

    def get_schema_generator(
        self, generator_class_name, api_info, api_version, api_url
    ):
        generator_class = swagger_settings.DEFAULT_GENERATOR_CLASS
        if generator_class_name:
            generator_class = import_string(generator_class_name)

        return generator_class(
            info=api_info,
            version=api_version,
            url=api_url,
        )

    def get_schema(self, generator, request, public):
        return generator.get_schema(request=request, public=public)

    def handle(
        self,
        output_file,
        overwrite,
        format,
        api_url,
        mock,
        api_version,
        user,
        private,
        generator_class_name,
        *args,
        **kwargs,
    ):
        # disable logs of WARNING and below
        logging.disable(logging.WARNING)

        info = getattr(swagger_settings, "DEFAULT_INFO", None)
        if not isinstance(info, openapi.Info):
            raise ImproperlyConfigured(
                'settings.SWAGGER_SETTINGS["DEFAULT_INFO"] should be an '
                "import string pointing to an openapi.Info object"
            )

        if not format:
            if os.path.splitext(output_file)[1] in (".yml", ".yaml"):
                format = "yaml"
        format = format or "json"

        api_url = api_url or swagger_settings.DEFAULT_API_URL

        if user:
            # Only call get_user_model if --user was passed in order to
            # avoid crashing if auth is not configured in the project
            user = get_user_model().objects.get(
                **{get_user_model().USERNAME_FIELD: user}
            )

        mock = mock or private or (user is not None) or (api_version is not None)
        if mock and not api_url:
            raise ImproperlyConfigured(
                "--mock-request requires an API url; either provide "
                "the --url argument or set the DEFAULT_API_URL setting"
            )

        request = None
        if mock:
            request = self.get_mock_request(api_url, format, user)

        api_version = api_version or api_settings.DEFAULT_VERSION
        if request and api_version:
            request.version = api_version

        generator = self.get_schema_generator(
            generator_class_name, info, api_version, api_url
        )
        schema = self.get_schema(generator, request, not private)

        if output_file == "-":
            self.write_schema(schema, self.stdout, format)
        else:
            flags = "w" if overwrite else "x"
            with open(output_file, flags) as stream:
                self.write_schema(schema, stream, format)
