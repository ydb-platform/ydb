from functools import partial

from django.db import models, router, transaction
from django.db.models import Max, Model, signals
from django.db.models.fields.related import ManyToManyField as _ManyToManyField
from django.db.models.fields.related import lazy_related_operation, resolve_relation
from django.db.models.fields.related_descriptors import ManyToManyDescriptor, create_forward_many_to_many_manager
from django.db.models.utils import make_model_tuple
from django.utils.encoding import force_str
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from .compat import get_rel
from .forms import SortedMultipleChoiceField

SORT_VALUE_FIELD_NAME = 'sort_value'


def create_sorted_many_related_manager(superclass, rel, *args, **kwargs):
    RelatedManager = create_forward_many_to_many_manager(
        superclass, rel, *args, **kwargs)

    class SortedRelatedManager(RelatedManager):
        def _apply_rel_ordering(self, queryset):
            return queryset.extra(order_by=['%s.%s' % (
                self.through._meta.db_table,
                self.through._sort_field_name,  # pylint: disable=protected-access
            )])

        def get_queryset(self):
            # We use ``extra`` method here because we have no other access to
            # the extra sorting field of the intermediary model. The fields
            # are hidden for joins because we set ``auto_created`` on the
            # intermediary's meta options.
            try:
                # pylint: disable=protected-access
                return self.instance._prefetched_objects_cache[self.prefetch_cache_name]
            except (AttributeError, KeyError):
                queryset = super().get_queryset()
                return self._apply_rel_ordering(queryset)

        def get_prefetch_querysets(self, instances, queryset=None):
            # Apply the same ordering for prefetch ones
            result = super().get_prefetch_querysets(instances, queryset)
            return (self._apply_rel_ordering(result[0]),) + result[1:]

        def get_prefetch_queryset(self, instances, queryset=None):
            # Apply the same ordering for prefetch ones
            result = super().get_prefetch_queryset(instances, queryset)
            return (self._apply_rel_ordering(result[0]),) + result[1:]

        def set(self, objs, **kwargs):  # pylint: disable=arguments-differ
            # Choosing to clear first will ensure the order is maintained.
            kwargs['clear'] = True
            super().set(objs, **kwargs)
        set.alters_data = True

        # pylint: disable=arguments-differ
        def _add_items(self, source_field_name, target_field_name, *objs, **kwargs):
            # source_field_name: the PK fieldname in join table for the source object
            # target_field_name: the PK fieldname in join table for the target object
            # *objs - objects to add. Either object instances, or primary keys of object instances.
            # **kwargs: in Django >= 2.2; contains `through_defaults` key.
            through_defaults = kwargs.get('through_defaults') or {}

            # If there aren't any objects, there is nothing to do.
            if objs:
                # Django uses a set here, we need to use a list to keep the
                # correct ordering.
                new_ids = []
                for obj in objs:
                    if isinstance(obj, self.model):
                        if not router.allow_relation(obj, self.instance):
                            raise ValueError(
                                'Cannot add "%r": instance is on database "%s", value is on database "%s"' %
                                (obj, self.instance._state.db, obj._state.db)  # pylint: disable=protected-access
                            )

                        fk_val = self.through._meta.get_field(target_field_name).get_foreign_related_value(obj)[0]

                        if fk_val is None:
                            raise ValueError(
                                'Cannot add "%r": the value for field "%s" is None' %
                                (obj, target_field_name)
                            )

                        new_ids.append(fk_val)
                    elif isinstance(obj, Model):
                        raise TypeError(
                            "'%s' instance expected, got %r" %
                            (self.model._meta.object_name, obj)
                        )
                    else:
                        new_ids.append(obj)

                db = router.db_for_write(self.through, instance=self.instance)
                manager = self.through._default_manager.using(db)  # pylint: disable=protected-access
                vals = (self.through._default_manager.using(db)  # pylint: disable=protected-access
                        .values_list(target_field_name, flat=True)
                        .filter(**{
                            source_field_name: self.related_val[0],
                            '%s__in' % target_field_name: new_ids,
                        }))

                # make set.difference_update() keeping ordering
                new_ids_set = set(new_ids)
                new_ids_set.difference_update(vals)

                new_ids = list(filter(lambda _id: _id in new_ids_set, new_ids))

                # Add the ones that aren't there already
                with transaction.atomic(using=db, savepoint=False):
                    if self.reverse or source_field_name == self.source_field_name:
                        # Don't send the signal when we are inserting the
                        # duplicate data row for symmetrical reverse entries.
                        signals.m2m_changed.send(
                            sender=self.through, action='pre_add',
                            instance=self.instance, reverse=self.reverse,
                            model=self.model, pk_set=new_ids_set, using=db,
                        )

                    rel_source_fk = self.related_val[0]
                    rel_through = self.through
                    sort_field_name = rel_through._sort_field_name  # pylint: disable=protected-access

                    # Use the max of all indices as start index...
                    # maybe an autoincrement field should do the job more efficiently ?
                    source_queryset = manager.filter(**{'%s_id' % source_field_name: rel_source_fk})
                    sort_value_max = source_queryset.aggregate(max=Max(sort_field_name))['max'] or 0

                    bulk_data = [
                        dict(through_defaults, **{
                            '%s_id' % source_field_name: rel_source_fk,
                            '%s_id' % target_field_name: obj_id,
                            sort_field_name: obj_idx,
                        })
                        for obj_idx, obj_id in enumerate(new_ids, sort_value_max + 1)
                    ]

                    manager.bulk_create([rel_through(**data) for data in bulk_data])

                    if self.reverse or source_field_name == self.source_field_name:
                        # Don't send the signal when we are inserting the
                        # duplicate data row for symmetrical reverse entries.
                        signals.m2m_changed.send(
                            sender=self.through, action='post_add',
                            instance=self.instance, reverse=self.reverse,
                            model=self.model, pk_set=new_ids_set, using=db,
                        )

    return SortedRelatedManager


class SortedManyToManyDescriptor(ManyToManyDescriptor):
    def __init__(self, field):
        super().__init__(field.remote_field)

    @cached_property
    def related_manager_cls(self):
        model = self.rel.model
        return create_sorted_many_related_manager(
            model._default_manager.__class__,  # pylint: disable=protected-access
            self.rel,
            # This is the new `reverse` argument (which ironically should
            # be False)
            reverse=False,
        )


class SortedManyToManyField(_ManyToManyField):
    """
    Providing a many to many relation that remembers the order of related
    objects.

    Accept a boolean ``sorted`` attribute which specifies if relation is
    ordered or not. Default is set to ``True``. If ``sorted`` is set to
    ``False`` the field will behave exactly like django's ``ManyToManyField``.

    Accept a class ``base_class`` attribute which specifies the base class of
    the intermediate model. It allows to customize the intermediate model.
    """

    def __init__(self, to, sorted=True, base_class=None, **kwargs):  # pylint: disable=redefined-builtin
        self.sorted = sorted
        self.sort_value_field_name = kwargs.pop(
            'sort_value_field_name',
            SORT_VALUE_FIELD_NAME)

        # Base class of through model
        self.base_class = base_class

        super().__init__(to, **kwargs)
        if self.sorted:
            self.help_text = kwargs.get('help_text', None)

    def deconstruct(self):
        # We have to persist custom added options in the ``kwargs``
        # dictionary. For readability only non-default values are stored.
        name, path, args, kwargs = super().deconstruct()
        if self.sort_value_field_name is not SORT_VALUE_FIELD_NAME:
            kwargs['sort_value_field_name'] = self.sort_value_field_name
        if self.sorted is not True:
            kwargs['sorted'] = self.sorted
        return name, path, args, kwargs

    def check(self, **kwargs):
        return (
            super().check(**kwargs) +
            self._check_through_sortedm2m()
        )

    def _check_through_sortedm2m(self):
        rel = get_rel(self)

        # Check if the custom through model of a SortedManyToManyField as a
        # valid '_sort_field_name' attribute
        if self.sorted and rel.through:
            assert hasattr(rel.through, '_sort_field_name'), (
                "The model is used as an intermediate model by "
                "'%s' but has no defined '_sort_field_name' attribute" % rel.through
            )

        return []

    # pylint: disable=inconsistent-return-statements
    def contribute_to_class(self, cls, name, **kwargs):
        if not self.sorted:
            return super().contribute_to_class(cls, name, **kwargs)

        # To support multiple relations to self, it's useful to have a non-None
        # related name on symmetrical relations for internal reasons. The
        # concept doesn't make a lot of sense externally ("you want me to
        # specify *what* on my non-reversible relation?!"), so we set it up
        # automatically. The funky name reduces the chance of an accidental
        # clash.
        rel = get_rel(self)

        if rel.symmetrical and (rel.model == "self" or rel.model == cls._meta.object_name):
            rel.related_name = "%s_rel_+" % name
        elif rel.hidden:
            # If the backwards relation is disabled, replace the original
            # related_name with one generated from the m2m field name. Django
            # still uses backwards relations internally and we need to avoid
            # clashes between multiple m2m fields with related_name == '+'.
            rel.related_name = "_%s_%s_+" % (cls.__name__.lower(), name)

        # pylint: disable=bad-super-call
        super(_ManyToManyField, self).contribute_to_class(cls, name, **kwargs)

        # The intermediate m2m model is not auto created if:
        #  1) There is a manually specified intermediate, or
        #  2) The class owning the m2m field is abstract.
        #  3) The class owning the m2m field has been swapped out.
        if not cls._meta.abstract:
            if rel.through:
                def resolve_through_model(_, model):
                    rel.through = model
                lazy_related_operation(resolve_through_model, cls, rel.through)
            elif not cls._meta.swapped:
                rel.through = self.create_intermediate_model(cls)

        # Add the descriptor for the m2m relation
        setattr(cls, self.name, SortedManyToManyDescriptor(self))

        # Set up the accessor for the m2m table name for the relation
        self.m2m_db_table = partial(self._get_m2m_db_table, cls._meta)  # pylint: disable=attribute-defined-outside-init

    def get_internal_type(self):
        return 'ManyToManyField'

    def formfield(self, **kwargs):  # pylint: disable=arguments-differ
        defaults = {}
        if self.sorted:
            defaults['form_class'] = SortedMultipleChoiceField
        defaults.update(kwargs)
        return super().formfield(**defaults)

    def create_intermediate_model(self, klass):
        base_classes = (self.base_class, models.Model) if self.base_class else (models.Model,)

        return create_sortable_many_to_many_intermediary_model(
            self, klass, self.sort_value_field_name,
            base_classes=base_classes)


def create_sortable_many_to_many_intermediary_model(field, klass, sort_field_name, base_classes=None):
    def set_managed(model, related, through):
        through._meta.managed = model._meta.managed or related._meta.managed

    to_model = resolve_relation(klass, field.remote_field.model)
    name = '%s_%s' % (klass._meta.object_name, field.name)
    lazy_related_operation(set_managed, klass, to_model, name)
    base_classes = base_classes if base_classes else (models.Model,)

    # TODO : use autoincrement here ?
    sort_field = models.IntegerField(default=0)

    to = make_model_tuple(to_model)[1]
    from_ = klass._meta.model_name
    if to == from_:
        to = 'to_%s' % to
        from_ = 'from_%s' % from_

    meta = type('Meta', (), {
        'db_table': field._get_m2m_db_table(klass._meta),  # pylint: disable=protected-access
        'auto_created': klass,
        'app_label': klass._meta.app_label,
        'db_tablespace': klass._meta.db_tablespace,
        'unique_together': (from_, to),
        'ordering': (sort_field_name,),
        'verbose_name': _('%(from)s-%(to)s relationship') % {'from': from_, 'to': to},
        'verbose_name_plural': _('%(from)s-%(to)s relationships') % {'from': from_, 'to': to},
        'apps': field.model._meta.apps,
    })

    # Construct and return the new class.
    return type(force_str(name), base_classes, {
        'Meta': meta,
        '__module__': klass.__module__,
        from_: models.ForeignKey(
            klass,
            related_name='%s+' % name,
            db_tablespace=field.db_tablespace,
            db_constraint=field.remote_field.db_constraint,
            on_delete=models.CASCADE,
        ),
        to: models.ForeignKey(
            to_model,
            related_name='%s+' % name,
            db_tablespace=field.db_tablespace,
            db_constraint=field.remote_field.db_constraint,
            on_delete=models.CASCADE,
        ),
        # Sort fields
        sort_field_name: sort_field,
        '_sort_field_name': sort_field_name,
    })
