import sys

from django.core.management.base import BaseCommand, CommandError

from import_export.command_utils import (
    get_default_format_names,
    get_format_class,
    get_resource_class,
)
from import_export.results import RowResult


class Command(BaseCommand):
    help = "Import data from various formats into a specified Django model."

    def add_arguments(self, parser):
        parser.add_argument(
            "resource",
            help="""The resource class or model class specified as a dotted path,
                    e.g., mymodule.resources.MyResource or auth.User.""",
        )
        parser.add_argument(
            "import_file_name",
            help="""The file to import from (use "-" for stdin).""",
        )
        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_false",
            dest="interactive",
            help="Do NOT prompt the user for input of any kind.",
        )
        parser.add_argument(
            "--raise-errors",
            action="store_true",
            help="Raise errors if encountered during execution.",
        )
        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            help="Perform a dry run without making any changes.",
        )
        default_format_names = get_default_format_names()
        parser.add_argument(
            "--format",
            help=f"""The data format. If not provided, it will be guessed from the
            mimetype. You can use a format from DEFAULT_FORMATS ({default_format_names})
            or specify a custom format class using a dotted path
            (e.g., XLSX or mymodule.CustomCSV).""",
        )
        parser.add_argument(
            "--encoding",
            help="The character encoding of the data.",
        )

    def handle(self, *args, **options):
        interactive = options["interactive"]
        dry_run = options.get("dry_run")
        raise_errors = options.get("raise_errors")
        file_name = options.get("import_file_name")
        model_or_resource_class = options.get("resource")
        format_name = options.get("format")
        encoding = options.get("encoding")

        if interactive:
            message = "Are you sure you want to import the data? [yes/no]: "
            if input(message) != "yes":
                raise CommandError("Import cancelled.")

        resource = get_resource_class(model_or_resource_class)()
        format_class = get_format_class(format_name, file_name, encoding)
        if file_name == "-":
            if format_class.is_binary():
                data = sys.stdin.buffer.read()
            else:
                data = sys.stdin.read()
        else:
            with open(file_name, format_class.get_read_mode()) as file:
                data = file.read()

        dataset = format_class.create_dataset(data)

        result = resource.import_data(
            dataset, dry_run=dry_run, raise_errors=raise_errors
        )

        if dry_run:
            self.stderr.write(
                self.style.NOTICE(
                    "You have activated the --dry-run option"
                    " so no data will be modified."
                )
            )

        if result.has_errors():
            self.stderr.write(self.style.ERROR("Import errors!"))
            for error in result.base_errors:
                self.stderr.write(repr(error.error), self.style.ERROR)
            for line, errors in result.row_errors():
                for error in errors:
                    self.stderr.write(
                        self.style.ERROR(f"Line number: {line} - {repr(error.error)}")
                    )
            sys.exit(1)
        else:
            success_message = (
                "Import finished: {} new, {} updated, {} deleted and {} skipped {}."
            ).format(
                result.totals[RowResult.IMPORT_TYPE_NEW],
                result.totals[RowResult.IMPORT_TYPE_UPDATE],
                result.totals[RowResult.IMPORT_TYPE_DELETE],
                result.totals[RowResult.IMPORT_TYPE_SKIP],
                resource._meta.model._meta.verbose_name_plural,
            )
            self.stderr.write(self.style.NOTICE(success_message))
