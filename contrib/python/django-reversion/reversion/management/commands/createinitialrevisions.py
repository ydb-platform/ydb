import json

from django.apps import apps
from django.core.management import CommandError
from django.db import connections, reset_queries, transaction, router
from reversion.models import Revision, Version, _safe_subquery
from reversion.management.commands import BaseRevisionCommand
from reversion.revisions import create_revision, set_comment, add_to_revision, add_meta


class Command(BaseRevisionCommand):

    help = "Creates initial revisions for a given app [and model]."

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--comment",
            action="store",
            default="Initial version.",
            help="Specify the comment to add to the revisions. Defaults to 'Initial version'.")
        parser.add_argument(
            "--batch-size",
            action="store",
            type=int,
            default=500,
            help="For large sets of data, revisions will be populated in batches. Defaults to 500.",
        )
        parser.add_argument(
            "--meta",
            action="store",
            default={},
            type=json.loads,
            help=("Specify meta models and corresponding values for each initial revision as JSON"
                  "eg. --meta \"{\"core.RevisionMeta\", {\"hello\": \"world\"}}\""),
        )

    def handle(self, *app_labels, **options):
        verbosity = options["verbosity"]
        using = options["using"]
        model_db = options["model_db"]
        comment = options["comment"]
        batch_size = options["batch_size"]
        meta = options["meta"]
        meta_models = []
        for label in meta.keys():
            try:
                model = apps.get_model(label)
                meta_models.append(model)
            except LookupError:
                raise CommandError(f"Unknown model: {label}")
        meta_values = meta.values()
        # Determine if we should use queryset.iterator()
        using = using or router.db_for_write(Revision)
        server_side_cursors = not connections[using].settings_dict.get('DISABLE_SERVER_SIDE_CURSORS')
        use_iterator = connections[using].vendor in ("postgresql",) and server_side_cursors
        # Create revisions.
        with transaction.atomic(using=using):
            for model in self.get_models(options):
                # Check all models for empty revisions.
                if verbosity >= 1:
                    self.stdout.write("Creating revisions for {name}".format(
                        name=model._meta.verbose_name,
                    ))
                created_count = 0
                live_objs = _safe_subquery(
                    "exclude",
                    model._default_manager.using(model_db),
                    model._meta.pk.name,
                    Version.objects.using(using).get_for_model(
                        model,
                        model_db=model_db,
                    ),
                    "object_id",
                )
                live_objs = live_objs.order_by()
                # Save all the versions.
                if use_iterator:
                    total = live_objs.count()
                    if total:
                        for obj in live_objs.iterator(batch_size):
                            self.create_revision(obj, using, meta, meta_models, meta_values, comment, model_db)
                            created_count += 1
                            # Print out a message every batch_size if feeling extra verbose
                            if not created_count % batch_size:
                                self.batch_complete(verbosity, created_count, total)
                else:
                    # Save all the versions.
                    ids = list(live_objs.values_list("pk", flat=True))
                    total = len(ids)
                    for i in range(0, total, batch_size):
                        chunked_ids = ids[i:i+batch_size]
                        objects = live_objs.in_bulk(chunked_ids)
                        for obj in objects.values():
                            self.create_revision(obj, using, meta, meta_models, meta_values, comment, model_db)
                            created_count += 1
                        # Print out a message every batch_size if feeling extra verbose
                        self.batch_complete(verbosity, created_count, total)

                # Print out a message, if feeling verbose.
                if verbosity >= 1:
                    self.stdout.write("- Created {total} / {total}".format(
                        total=total,
                    ))

    def create_revision(self, obj, using, meta, meta_models, meta_values, comment, model_db):
        with create_revision(using=using):
            if meta:
                for model, values in zip(meta_models, meta_values):
                    add_meta(model, **values)
            set_comment(comment)
            add_to_revision(obj, model_db=model_db)

    def batch_complete(self, verbosity, created_count, total):
        reset_queries()
        if verbosity >= 2:
            self.stdout.write("- Created {created_count} / {total}".format(
                created_count=created_count,
                total=total,
            ))
