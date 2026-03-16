import sys

from django.core.management.base import BaseCommand

from import_export.command_utils import (
    get_default_format_names,
    get_format_class,
    get_resource_class,
)


class Command(BaseCommand):
    help = "Export data from a specified resource or model in a chosen format."

    def add_arguments(self, parser):
        default_format_names = get_default_format_names()
        parser.add_argument(
            "format",
            help=f"""Specify the export format. Can be one of the default formats
            ({default_format_names}), or a custom format class provided as a dotted path
            (e.g., 'XLSX' or 'mymodule.CustomCSV').""",
        )
        parser.add_argument(
            "resource",
            help="""Specify the resource or model to export. Accepts a resource class or
            a model class in dotted path format "(e.g., 'mymodule.resources.MyResource'
            or 'auth.User').""",
        )
        parser.add_argument(
            "--encoding",
            help="Specify the encoding to use for the exported data (e.g., 'utf-8'). "
            "This applies to text-based formats.",
        )

    def handle(self, *args, **options):
        model_or_resource_class = options.get("resource")
        format_name = options.get("format")
        encoding = options.get("encoding")

        resource = get_resource_class(model_or_resource_class)()
        format_class = get_format_class(format_name, None, encoding)

        data = resource.export()
        export_data = format_class.export_data(data)

        if not format_class.is_binary():
            if encoding:
                export_data = export_data.encode(encoding)
            else:
                export_data = export_data.encode()

        if format_class.is_binary() and self.stdout.isatty():
            self.stderr.write(
                self.style.ERROR(
                    "This is a binary format and your terminal does not support "
                    "binary data. Redirect the output to a file."
                )
            )
            sys.exit(1)
        self.stdout.buffer.write(export_data)
