import functools
import importlib
import json
import os

from django.core.management.base import BaseCommand, CommandError
from django.utils import autoreload
from graphql import print_schema

from graphene_django.settings import graphene_settings


class CommandArguments(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--schema",
            type=str,
            dest="schema",
            default=graphene_settings.SCHEMA,
            help="Django app containing schema to dump, e.g. myproject.core.schema.schema",
        )

        parser.add_argument(
            "--out",
            type=str,
            dest="out",
            default=graphene_settings.SCHEMA_OUTPUT,
            help="Output file, --out=- prints to stdout (default: schema.json)",
        )

        parser.add_argument(
            "--indent",
            type=int,
            dest="indent",
            default=graphene_settings.SCHEMA_INDENT,
            help="Output file indent (default: None)",
        )

        parser.add_argument(
            "--watch",
            dest="watch",
            default=False,
            action="store_true",
            help="Updates the schema on file changes (default: False)",
        )


class Command(CommandArguments):
    help = "Dump Graphene schema as a JSON or GraphQL file"
    can_import_settings = True
    requires_system_checks = []

    def save_json_file(self, out, schema_dict, indent):
        with open(out, "w") as outfile:
            json.dump(schema_dict, outfile, indent=indent, sort_keys=True)

    def save_graphql_file(self, out, schema):
        with open(out, "w", encoding="utf-8") as outfile:
            outfile.write(print_schema(schema.graphql_schema))

    def get_schema(self, schema, out, indent):
        schema_dict = {"data": schema.introspect()}
        if out == "-" or out == "-.json":
            self.stdout.write(json.dumps(schema_dict, indent=indent, sort_keys=True))
        elif out == "-.graphql":
            self.stdout.write(print_schema(schema.graphql_schema))
        else:
            # Determine format
            _, file_extension = os.path.splitext(out)

            if file_extension == ".graphql":
                self.save_graphql_file(out, schema)
            elif file_extension == ".json":
                self.save_json_file(out, schema_dict, indent)
            else:
                raise CommandError(f'Unrecognised file format "{file_extension}"')

            style = getattr(self, "style", None)
            success = getattr(style, "SUCCESS", lambda x: x)

            self.stdout.write(success(f"Successfully dumped GraphQL schema to {out}"))

    def handle(self, *args, **options):
        options_schema = options.get("schema")

        if options_schema and isinstance(options_schema, str):
            module_str, schema_name = options_schema.rsplit(".", 1)
            mod = importlib.import_module(module_str)
            schema = getattr(mod, schema_name)

        elif options_schema:
            schema = options_schema

        else:
            schema = graphene_settings.SCHEMA

        out = options.get("out") or graphene_settings.SCHEMA_OUTPUT

        if not schema:
            raise CommandError(
                "Specify schema on GRAPHENE.SCHEMA setting or by using --schema"
            )

        indent = options.get("indent")
        watch = options.get("watch")
        if watch:
            autoreload.run_with_reloader(
                functools.partial(self.get_schema, schema, out, indent)
            )
        else:
            self.get_schema(schema, out, indent)
