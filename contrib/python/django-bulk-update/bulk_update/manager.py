from django.db import models
from .helper import bulk_update


class BulkUpdateManager(models.Manager):
    def bulk_update(self, objs, update_fields=None,
                    exclude_fields=None, batch_size=None):
        return bulk_update(
            objs, update_fields=update_fields,
            exclude_fields=exclude_fields, using=self.db,
            batch_size=batch_size)
