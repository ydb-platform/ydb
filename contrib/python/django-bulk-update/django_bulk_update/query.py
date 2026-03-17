from django.db import models
from .helper import bulk_update


class BulkUpdateQuerySet(models.QuerySet):

    def bulk_update(self, objs, update_fields=None,
                    exclude_fields=None, batch_size=None):

        self._for_write = True
        using = self.db

        return bulk_update(
            objs, update_fields=update_fields,
            exclude_fields=exclude_fields, using=using,
            batch_size=batch_size)
