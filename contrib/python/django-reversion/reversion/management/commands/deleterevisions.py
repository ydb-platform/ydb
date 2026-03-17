from datetime import timedelta
from django.db import transaction, models, router
from django.utils import timezone
from reversion.models import Revision, Version
from reversion.management.commands import BaseRevisionCommand


class Command(BaseRevisionCommand):

    help = "Deletes revisions for a given app [and model]."

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--days",
            default=0,
            type=int,
            help="Delete only revisions older than the specified number of days.",
        )
        parser.add_argument(
            "--keep",
            default=0,
            type=int,
            help="Keep the specified number of revisions (most recent) for each object.",
        )

    def handle(self, *app_labels, **options):
        verbosity = options["verbosity"]
        using = options["using"]
        model_db = options["model_db"]
        days = options["days"]
        keep = options["keep"]
        # Delete revisions.
        using = using or router.db_for_write(Revision)
        with transaction.atomic(using=using):
            revision_query = models.Q()
            keep_revision_ids = set()
            # By default, delete nothing.
            can_delete = False
            # Get all revisions for the given revision manager and model.
            for model in self.get_models(options):
                if verbosity >= 1:
                    self.stdout.write("Finding stale revisions for {name}".format(
                        name=model._meta.verbose_name,
                    ))
                # Find all matching revision IDs.
                model_query = Version.objects.using(using).get_for_model(
                    model,
                    model_db=model_db,
                )
                if keep:
                    overflow_object_ids = list(Version.objects.using(using).get_for_model(
                        model,
                        model_db=model_db,
                    ).order_by().values_list("object_id").annotate(
                        count=models.Count("object_id"),
                    ).filter(
                        count__gt=keep,
                    ).values_list("object_id", flat=True).iterator())
                    # Only delete overflow revisions.
                    model_query = model_query.filter(object_id__in=overflow_object_ids)
                    for object_id in overflow_object_ids:
                        if verbosity >= 2:
                            self.stdout.write("- Finding stale revisions for {name} #{object_id}".format(
                                name=model._meta.verbose_name,
                                object_id=object_id,
                            ))
                        # But keep the underflow revisions.
                        keep_revision_ids.update(Version.objects.using(using).get_for_object_reference(
                            model,
                            object_id,
                            model_db=model_db,
                        ).values_list("revision_id", flat=True)[:keep].iterator())
                # Add to revision query.
                revision_query |= models.Q(
                    pk__in=model_query.order_by().values_list("revision_id", flat=True)
                )
                # If we have at least one model, then we can delete.
                can_delete = True
            if can_delete:
                revisions_to_delete = Revision.objects.using(using).filter(
                    revision_query,
                    date_created__lt=timezone.now() - timedelta(days=days),
                ).exclude(
                    pk__in=keep_revision_ids
                ).order_by()
            else:
                revisions_to_delete = Revision.objects.using(using).none()
            # Print out a message, if feeling verbose.
            if verbosity >= 1:
                self.stdout.write("Deleting {total} revisions...".format(
                    total=revisions_to_delete.count(),
                ))
            revisions_to_delete.delete()
