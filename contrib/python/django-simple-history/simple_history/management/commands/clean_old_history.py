from django.db import transaction
from django.utils import timezone

from ... import models, utils
from ...exceptions import NotHistoricalModelError
from . import populate_history


class Command(populate_history.Command):
    args = "<app.model app.model ...>"
    help = "Scans HistoricalRecords for old entries " "and deletes them."

    DONE_CLEANING_FOR_MODEL = "Removed {count} historical records for {model}\n"

    def add_arguments(self, parser):
        parser.add_argument("models", nargs="*", type=str)
        parser.add_argument(
            "--auto",
            action="store_true",
            dest="auto",
            default=False,
            help="Automatically search for models with the HistoricalRecords field "
            "type",
        )
        parser.add_argument(
            "--days",
            help="Only Keep the last X Days of history, default is 30",
            dest="days",
            type=int,
            default=30,
        )

        parser.add_argument(
            "-d", "--dry", action="store_true", help="Dry (test) run only, no changes"
        )

    def handle(self, *args, **options):
        self.verbosity = options["verbosity"]

        to_process = set()
        model_strings = options.get("models", []) or args

        if model_strings:
            for model_pair in self._handle_model_list(*model_strings):
                to_process.add(model_pair)

        elif options["auto"]:
            to_process = self._auto_models()

        else:
            self.log(self.COMMAND_HINT)

        self._process(to_process, days_back=options["days"], dry_run=options["dry"])

    def _process(self, to_process, days_back=None, dry_run=True):
        start_date = timezone.now() - timezone.timedelta(days=days_back)
        for model, history_model in to_process:
            history_model_manager = history_model.objects
            history_model_manager = history_model_manager.filter(
                history_date__lt=start_date
            )
            found = history_model_manager.count()
            self.log(f"{model} has {found} old historical entries", 2)
            if not found:
                continue
            if not dry_run:
                history_model_manager.delete()

            self.log(self.DONE_CLEANING_FOR_MODEL.format(model=model, count=found))

    def log(self, message, verbosity_level=1):
        if self.verbosity >= verbosity_level:
            self.stdout.write(message)
