from django.conf import settings
from django.db import models
from django.db.models import Exists, OuterRef, Q, QuerySet
from django.utils import timezone

from simple_history.utils import (
    get_app_model_primary_key_name,
    get_change_reason_from_object,
)

# when converting a historical record to an instance, this attribute is added
# to the instance so that code can reverse the instance to its historical record
SIMPLE_HISTORY_REVERSE_ATTR_NAME = "_history"


class HistoricalQuerySet(QuerySet):
    """
    Enables additional functionality when working with historical records.

    For additional history on this topic, see:
        - https://github.com/django-commons/django-simple-history/pull/229
        - https://github.com/django-commons/django-simple-history/issues/354
        - https://github.com/django-commons/django-simple-history/issues/397
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._as_instances = False
        self._as_of = None
        self._pk_attr = self.model.instance_type._meta.pk.attname

    def as_instances(self) -> "HistoricalQuerySet":
        """
        Return a queryset that generates instances instead of historical records.
        Queries against the resulting queryset will translate `pk` into the
        primary key field of the original type.
        """
        if not self._as_instances:
            result = self.exclude(history_type="-")
            result._as_instances = True
        else:
            result = self._clone()
        return result

    def filter(self, *args, **kwargs) -> "HistoricalQuerySet":
        """
        If a `pk` filter arrives and the queryset is returning instances
        then the caller actually wants to filter based on the original
        type's primary key, and not the history_id (historical record's
        primary key); this happens frequently with DRF.
        """
        if self._as_instances and "pk" in kwargs:
            kwargs[self._pk_attr] = kwargs.pop("pk")
        return super().filter(*args, **kwargs)

    def latest_of_each(self) -> "HistoricalQuerySet":
        """
        Ensures results in the queryset are the latest historical record for each
        primary key. This includes deletion records.
        """
        # Subquery for finding the records that belong to the same history-tracked
        # object as the record from the outer query (identified by `_pk_attr`),
        # and that have a later `history_date` than the outer record.
        # The very latest record of a history-tracked object should be excluded from
        # this query - which will make it included in the `~Exists` query below.
        later_records = self.filter(
            Q(**{self._pk_attr: OuterRef(self._pk_attr)}),
            Q(history_date__gt=OuterRef("history_date")),
        )

        # Filter the records to only include those for which the `later_records`
        # subquery does not return any results.
        return self.filter(~Exists(later_records))

    def _select_related_history_tracked_objs(self) -> "HistoricalQuerySet":
        """
        A convenience method that calls ``select_related()`` with all the names of
        the model's history-tracked ``ForeignKey`` fields.
        """
        field_names = [
            field.name
            for field in self.model.tracked_fields
            if isinstance(field, models.ForeignKey)
        ]
        return self.select_related(*field_names)

    def _clone(self) -> "HistoricalQuerySet":
        c = super()._clone()
        c._as_instances = self._as_instances
        c._as_of = self._as_of
        c._pk_attr = self._pk_attr
        return c

    def _fetch_all(self) -> None:
        super()._fetch_all()
        self._instanceize()

    def _instanceize(self) -> None:
        """
        Convert the result cache to instances if possible and it has not already been
        done.  If a query extracts `.values(...)` then the result cache will not contain
        historical objects to be converted.
        """
        if (
            self._result_cache
            and self._as_instances
            and isinstance(self._result_cache[0], self.model)
        ):
            self._result_cache = [item.instance for item in self._result_cache]
            for item in self._result_cache:
                historic = getattr(item, SIMPLE_HISTORY_REVERSE_ATTR_NAME)
                setattr(historic, "_as_of", self._as_of)


class HistoryManager(models.Manager):
    def __init__(self, model, instance=None):
        super().__init__()
        self.model = model
        self.instance = instance

    def get_super_queryset(self):
        return super().get_queryset()

    def get_queryset(self):
        qs = self.get_super_queryset()
        if self.instance is None:
            return qs

        key_name = get_app_model_primary_key_name(self.instance)
        return self.get_super_queryset().filter(**{key_name: self.instance.pk})

    def most_recent(self):
        """
        Returns the most recent copy of the instance available in the history.
        """
        if not self.instance:
            raise TypeError(
                "Can't use most_recent() without a {} instance.".format(
                    self.model._meta.object_name
                )
            )
        tmp = []

        for field in self.model.tracked_fields:
            if isinstance(field, models.ForeignKey):
                tmp.append(field.name + "_id")
            else:
                tmp.append(field.name)
        fields = tuple(tmp)
        try:
            values = self.get_queryset().values(*fields)[0]
        except IndexError:
            raise self.instance.DoesNotExist(
                "%s has no historical record." % self.instance._meta.object_name
            )
        return self.instance.__class__(**values)

    def as_of(self, date):
        """
        Get a snapshot as of a specific date.

        When this is used on an instance, it will return the instance based
        on the specific date.  If the instance did not exist yet, or had been
        deleted, then a DoesNotExist error is railed.

        When this is used on a model's history manager, the resulting queryset
        will locate the most recent historical record before the specified date
        for each primary key, generating instances.  If the most recent historical
        record is a deletion, that instance is dropped from the result.

        A common usage pattern for querying is to accept an optional time
        point `date` and then use:

            `qs = <Model>.history.as_of(date) if date else <Model>.objects`

        after which point one can add filters, values - anything a normal
        queryset would support.

        To retrieve historical records, query the model's history directly;
        for example:
            `qs = <Model>.history.filter(history_date__lte=date, pk=...)`

        To retrieve the most recent historical record, including deletions,
        you could then use:
            `qs = qs.latest_of_each()`
        """
        queryset = self.get_queryset().filter(history_date__lte=date)
        if not self.instance:
            if isinstance(queryset, HistoricalQuerySet):
                queryset._as_of = date
            queryset = queryset.latest_of_each().as_instances()
            return queryset

        try:
            # historical records are sorted in reverse chronological order
            history_obj = queryset[0]
        except IndexError:
            raise self.instance.DoesNotExist(
                "%s had not yet been created." % self.instance._meta.object_name
            )
        if history_obj.history_type == "-":
            raise self.instance.DoesNotExist(
                "%s had already been deleted." % self.instance._meta.object_name
            )
        result = history_obj.instance
        historic = getattr(result, SIMPLE_HISTORY_REVERSE_ATTR_NAME)
        setattr(historic, "_as_of", date)
        return result

    def bulk_history_create(
        self,
        objs,
        batch_size=None,
        update=False,
        default_user=None,
        default_change_reason="",
        default_date=None,
        custom_historical_attrs=None,
    ):
        """
        Bulk create the history for the objects specified by objs.
        If called by bulk_update_with_history, use the update boolean and
        save the history_type accordingly.
        """
        if not getattr(settings, "SIMPLE_HISTORY_ENABLED", True):
            return

        history_type = "+"
        if update:
            history_type = "~"

        historical_instances = []
        for instance in objs:
            history_user = getattr(
                instance,
                "_history_user",
                default_user or self.model.get_default_history_user(instance),
            )
            row = self.model(
                history_date=getattr(
                    instance, "_history_date", default_date or timezone.now()
                ),
                history_user=history_user,
                history_change_reason=get_change_reason_from_object(instance)
                or default_change_reason,
                history_type=history_type,
                **{
                    field.attname: getattr(instance, field.attname)
                    for field in self.model.tracked_fields
                },
                **(custom_historical_attrs or {}),
            )
            if hasattr(self.model, "history_relation"):
                row.history_relation_id = instance.pk
            historical_instances.append(row)

        return self.model.objects.bulk_create(
            historical_instances, batch_size=batch_size
        )


class HistoryDescriptor:
    def __init__(self, model, manager=HistoryManager, queryset=HistoricalQuerySet):
        self.model = model
        self.queryset_class = queryset
        self.manager_class = manager

    def __get__(self, instance, owner):
        return self.manager_class.from_queryset(self.queryset_class)(
            self.model, instance
        )
