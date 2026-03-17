from django.db import models
from .query import BulkUpdateQuerySet


class BulkUpdateManager(models.Manager.from_queryset(BulkUpdateQuerySet)):
    pass
