import django
from django.db import router
from django.db.models import signals
try:
    from django.db.models.fields.related import ReverseManyRelatedObjectsDescriptor as ReverseManyToOneDescriptor
except ImportError:
    from django.db.models.fields.related import ReverseManyToOneDescriptor
from django.utils.functional import cached_property


class SortableReverseManyRelatedObjectsDescriptor(ReverseManyToOneDescriptor):

    @cached_property
    def related_manager_cls(self):
        ManyRelatedManagerBase = super(
            SortableReverseManyRelatedObjectsDescriptor, self).related_manager_cls

        class ManyRelatedManager(ManyRelatedManagerBase):

            def _add_items(self, source_field_name, target_field_name, *objs):
                """
                By default, auto_created through objects from form instances are saved using
                Manager.bulk_create(). Manager.bulk_create() is passed a list containing
                instances of the through model with the target and source foreign keys defined.

                In order to set the position field we need to tweak this logic (the modified
                lines are marked out with comments below).

                This method is added to ManyRelatedManager below in
                SortableDescriptorMixin.related_manager_cls
                """
                # source_field_name: the PK fieldname in join table for the source object
                # target_field_name: the PK fieldname in join table for the target object
                # *objs - objects to add. Either object instances, or primary keys of object instances.

                # If there aren't any objects, there is nothing to do.
                from django.db.models import Model
                if objs:
                    new_ids = set()
                    for obj in objs:
                        if isinstance(obj, self.model):
                            if not router.allow_relation(obj, self.instance):
                                raise ValueError('Cannot add "%r": instance is on database "%s", value is on database "%s"' %
                                                   (obj, self.instance._state.db, obj._state.db))
                            # _get_fk_val wasn't introduced until django 1.4.2
                            if hasattr(self, '_get_fk_val'):
                                fk_val = self._get_fk_val(obj, target_field_name)
                            else:
                                fk_val = obj.pk
                            if fk_val is None:
                                raise ValueError('Cannot add "%r": the value for field "%s" is None' %
                                                 (obj, target_field_name))
                            new_ids.add(fk_val)
                        elif isinstance(obj, Model):
                            raise TypeError("'%s' instance expected, got %r" % (self.model._meta.object_name, obj))
                        else:
                            new_ids.add(obj)
                    db = router.db_for_write(self.through, instance=self.instance)
                    vals = self.through._default_manager.using(db).values_list(target_field_name, flat=True)
                    vals = vals.filter(**{
                        source_field_name: getattr(self, '_pk_val', getattr(self, '_fk_val', self.instance.pk)),
                        '%s__in' % target_field_name: new_ids,
                    })
                    new_ids = new_ids - set(vals)

                    if self.reverse or source_field_name == self.source_field_name:
                        # Don't send the signal when we are inserting the
                        # duplicate data row for symmetrical reverse entries.
                        signals.m2m_changed.send(sender=self.through, action='pre_add',
                            instance=self.instance, reverse=self.reverse,
                            model=self.model, pk_set=new_ids, using=db)

                    ######################################################################
                    # This is where we modify the default logic for _add_items().
                    # We use get_or_create for ALL objects. Typically it calls bulk_create
                    # ONLY on ids which have not yet been created.
                    ######################################################################
                    # sort_field = self.field.sort_field
                    sort_field_attname = self.field.sort_field.attname
                    for obj in objs:
                        sort_position = getattr(obj, sort_field_attname)
                        new_obj, created = self.through._default_manager.using(db).get_or_create(**{
                            sort_field_attname: sort_position,
                            '%s_id' % source_field_name: getattr(self, '_pk_val', getattr(self, '_fk_val', self.instance.pk)),
                            '%s_id' % target_field_name: obj.pk,
                        })
                        if getattr(new_obj, sort_field_attname) is not sort_position:
                            setattr(new_obj, sort_field_attname, sort_position)
                            new_obj.save()
                    ######################################################################
                    # End custom logic
                    ######################################################################

                    if self.reverse or source_field_name == self.source_field_name:
                        # Don't send the signal when we are inserting the
                        # duplicate data row for symmetrical reverse entries.
                        signals.m2m_changed.send(sender=self.through, action='post_add',
                            instance=self.instance, reverse=self.reverse,
                            model=self.model, pk_set=new_ids, using=db)

            def get_queryset(self):
                """
                Adds ordering to ManyRelatedManager.get_queryset(). This is
                necessary in order for form widgets to display authors ordered by
                position.
                """
                try:
                    return self.instance._prefetched_objects_cache[self.prefetch_cache_name]
                except (AttributeError, KeyError):
                    if django.VERSION < (1, 7):
                        qset = super(ManyRelatedManager, self).get_query_set()
                    else:
                        qset = super(ManyRelatedManager, self).get_queryset()
                    opts = self.through._meta
                    # If the through table has Meta.ordering defined, order the objects
                    # returned by the ManyRelatedManager by those fields.
                    if self.field.sort_value_field_name:
                        object_name = opts.object_name.lower()
                        order_by = ['%s__%s' % (object_name, self.field.sort_value_field_name)]
                        if self.model._meta.ordering != order_by:
                            return qset.order_by(*order_by)
                    return qset

            if django.VERSION < (1, 7):
                get_query_set = get_queryset

            def get_prefetch_queryset(self, instances, *args):
                if django.VERSION < (1, 7):
                    rel_qs, rel_obj_attr, instance_attr, single, cache_name = \
                        super(ManyRelatedManager, self).get_prefetch_query_set(instances, *args)
                else:
                    rel_qs, rel_obj_attr, instance_attr, single, cache_name = \
                        super(ManyRelatedManager, self).get_prefetch_queryset(instances, *args)
                opts = self.through._meta
                # If the through table has Meta.ordering defined, order the objects
                # returned by the ManyRelatedManager by those fields.
                if self.field.sort_value_field_name:
                    object_name = opts.object_name.lower()
                    order_by = ['%s__%s' % (object_name, self.field.sort_value_field_name)]
                    if self.model._meta.ordering != order_by:
                        rel_qs = rel_qs.order_by(*order_by)
                return (rel_qs, rel_obj_attr, instance_attr, single, cache_name)

            if django.VERSION < (1, 7):
                get_prefetch_query_set = get_prefetch_queryset

        ManyRelatedManager.field = self.field
        return ManyRelatedManager
