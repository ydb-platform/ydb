import contextlib
import logging

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from pgtrigger import core, installation, registry, runtime


def _setup_logging():  # pragma: no cover
    installation.LOGGER.addHandler(logging.StreamHandler())
    if not installation.LOGGER.level:
        installation.LOGGER.setLevel(logging.INFO)


class Command(BaseCommand):
    help = "Core django-pgtrigger subcommands."

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(
            title="sub-commands",
            required=True,
        )

        ls_parser = subparsers.add_parser("ls", help="List triggers and their installation state.")
        ls_parser.add_argument("uris", nargs="*", type=str)
        ls_parser.add_argument("-d", "--database", help="The database")
        ls_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        ls_parser.set_defaults(method=self.ls)

        install_parser = subparsers.add_parser("install", help="Install triggers.")
        install_parser.add_argument("uris", nargs="*", type=str)
        install_parser.add_argument("-d", "--database", help="The database")
        install_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        install_parser.set_defaults(method=self.install)

        uninstall_parser = subparsers.add_parser("uninstall", help="Uninstall triggers.")
        uninstall_parser.add_argument("uris", nargs="*", type=str)
        uninstall_parser.add_argument("-d", "--database", help="The database")
        uninstall_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        uninstall_parser.set_defaults(method=self.uninstall)

        enable_parser = subparsers.add_parser("enable", help="Enable triggers.")
        enable_parser.add_argument("uris", nargs="*", type=str)
        enable_parser.add_argument("-d", "--database", help="The database")
        enable_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        enable_parser.set_defaults(method=self.enable)

        disable_parser = subparsers.add_parser("disable", help="Disable triggers.")
        disable_parser.add_argument("uris", nargs="*", type=str)
        disable_parser.add_argument("-d", "--database", help="The database")
        disable_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        disable_parser.set_defaults(method=self.disable)

        prune_parser = subparsers.add_parser(
            "prune", help="Prune installed triggers that are no longer in the codebase."
        )
        prune_parser.add_argument("-d", "--database", help="The database")
        prune_parser.add_argument(
            "-s",
            "--schema",
            action="append",
            help="Set the search path to this schema",
        )
        prune_parser.set_defaults(method=self.prune)

    def handle(self, *args, method, **options):
        database = options["database"] or DEFAULT_DB_ALIAS
        schemas = options["schema"] or []

        if schemas:
            context = runtime.schema(*schemas, databases=[database])
        else:
            context = contextlib.nullcontext()

        with context:
            return method(*args, **options)

    def ls(self, *args, **options):
        uris = options["uris"]

        status_formatted = {
            core.UNINSTALLED: "\033[91mUNINSTALLED\033[0m",
            core.INSTALLED: "\033[92mINSTALLED\033[0m",
            core.OUTDATED: "\033[93mOUTDATED\033[0m",
            core.PRUNE: "\033[96mPRUNE\033[0m",
            core.UNALLOWED: "\033[94mUNALLOWED\033[0m",
        }

        enabled_formatted = {
            True: "\033[92mENABLED\033[0m",
            False: "\033[91mDISABLED\033[0m",
            None: "\033[94mN/A\033[0m",
        }

        def _format_status(status, enabled, uri):
            if status in (core.UNINSTALLED, core.UNALLOWED):
                enabled = None

            return status_formatted[status], enabled_formatted[enabled], uri

        formatted = []

        for model, trigger in registry.registered(*uris):
            uri = trigger.get_uri(model)
            status, enabled = trigger.get_installation_status(model, database=options["database"])
            formatted.append(_format_status(status, enabled, uri))

        if not uris:
            for trigger in installation.prunable(database=options["database"]):
                formatted.append(_format_status("PRUNE", trigger[2], f"{trigger[0]}:{trigger[1]}"))

        max_status_len = max(len(val) for val, _, _ in formatted)
        max_enabled_len = max(len(val) for _, val, _ in formatted)
        for status, enabled, uri in formatted:
            print(
                f"{{: <{max_status_len}}} {{: <{max_enabled_len}}} {{}}".format(
                    status, enabled, uri
                )
            )

    def install(self, *args, **options):
        _setup_logging()
        installation.install(*options["uris"], database=options["database"])

    def uninstall(self, *args, **options):
        _setup_logging()
        installation.uninstall(*options["uris"], database=options["database"])

    def enable(self, *args, **options):
        _setup_logging()
        installation.enable(*options["uris"], database=options["database"])

    def disable(self, *args, **options):
        _setup_logging()
        installation.disable(*options["uris"], database=options["database"])

    def prune(self, *args, **options):
        _setup_logging()
        installation.prune(database=options["database"])
