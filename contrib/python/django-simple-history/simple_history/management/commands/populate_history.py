from django.apps import apps
from django.core.management.base import BaseCommand, CommandError

from ... import models, utils
from ...exceptions import NotHistoricalModelError

get_model = apps.get_model


class Command(BaseCommand):
    args = "<app.model app.model ...>"
    help = (
        "Populates the corresponding HistoricalRecords field with "
        "the current state of all instances in a model"
    )

    COMMAND_HINT = "Please specify a model or use the --auto option"
    MODEL_NOT_FOUND = "Unable to find model"
    MODEL_NOT_HISTORICAL = "No history model found"
    NO_REGISTERED_MODELS = "No registered models were found\n"
    START_SAVING_FOR_MODEL = "Saving historical records for {model}\n"
    DONE_SAVING_FOR_MODEL = "Finished saving historical records for {model}\n"
    EXISTING_HISTORY_FOUND = "Existing history found, skipping model"
    INVALID_MODEL_ARG = "An invalid model was specified"

    def add_arguments(self, parser):
        super().add_arguments(parser)
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
            "--batchsize",
            action="store",
            dest="batchsize",
            default=200,
            type=int,
            help="Set a custom batch size when bulk inserting historical records.",
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
            if self.verbosity >= 1:
                self.stdout.write(self.COMMAND_HINT)

        self._process(to_process, batch_size=options["batchsize"])

    def _auto_models(self):
        to_process = set()
        for model in models.registered_models.values():
            try:  # avoid issues with multi-table inheritance
                history_model = utils.get_history_model_for_model(model)
            except NotHistoricalModelError:
                continue
            to_process.add((model, history_model))
        if not to_process:
            if self.verbosity >= 1:
                self.stdout.write(self.NO_REGISTERED_MODELS)
        return to_process

    def _handle_model_list(self, *args):
        failing = False
        for natural_key in args:
            try:
                model, history = self._model_from_natural_key(natural_key)
            except ValueError as e:
                failing = True
                self.stderr.write(f"{e}\n")
            else:
                if not failing:
                    yield (model, history)
        if failing:
            raise CommandError(self.INVALID_MODEL_ARG)

    def _model_from_natural_key(self, natural_key):
        try:
            app_label, model = natural_key.split(".", 1)
        except ValueError:
            model = None
        else:
            try:
                model = get_model(app_label, model)
            except LookupError:
                model = None
        if not model:
            msg = self.MODEL_NOT_FOUND + f" < {natural_key} >\n"
            raise ValueError(msg)
        try:
            history_model = utils.get_history_model_for_model(model)
        except NotHistoricalModelError:
            msg = self.MODEL_NOT_HISTORICAL + f" < {natural_key} >\n"
            raise ValueError(msg)
        return model, history_model

    def _bulk_history_create(self, model, batch_size):
        """Save a copy of all instances to the historical model.

        :param model: Model you want to bulk create
        :param batch_size: number of models to create at once.
        :return:
        """

        instances = []
        history = utils.get_history_manager_for_model(model)
        if self.verbosity >= 1:
            self.stdout.write(
                "Starting bulk creating history models for {} instances {}-{}".format(
                    model, 0, batch_size
                )
            )

        iterator_kwargs = {"chunk_size": batch_size}
        for index, instance in enumerate(
            model._default_manager.iterator(**iterator_kwargs)
        ):
            # Can't Just pass batch_size to bulk_create as this can lead to
            # Out of Memory Errors as we load too many models into memory after
            # creating them. So we only keep batch_size worth of models in
            # historical_instances and clear them after we hit batch_size
            if index % batch_size == 0:
                history.bulk_history_create(instances, batch_size=batch_size)

                instances = []

                if self.verbosity >= 1:
                    self.stdout.write(
                        "Finished bulk creating history models for {} "
                        "instances {}-{}, starting next {}".format(
                            model, index - batch_size, index, batch_size
                        )
                    )

            instances.append(instance)

        # create any we didn't get in the last loop
        if instances:
            history.bulk_history_create(instances, batch_size=batch_size)

    def _process(self, to_process, batch_size):
        for model, history_model in to_process:
            if history_model.objects.exists():
                self.stderr.write(
                    "{msg} {model}\n".format(
                        msg=self.EXISTING_HISTORY_FOUND, model=model
                    )
                )
                continue
            if self.verbosity >= 1:
                self.stdout.write(self.START_SAVING_FOR_MODEL.format(model=model))
            self._bulk_history_create(model, batch_size)
            if self.verbosity >= 1:
                self.stdout.write(self.DONE_SAVING_FOR_MODEL.format(model=model))
