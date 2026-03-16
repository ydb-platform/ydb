# Copyright 2015 Ocado Innovation Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Models for closuretree application."""

# We like magic.
# pylint: disable=W0142

# We have lots of dynamically generated things, hard for pylint to solve.
# pylint: disable=E1101

# It may not be our class, but we made the attribute on it
# pylint: disable=W0212

# Public methods are useful!
# pylint: disable=R0904

from django.db import models
from django.db.models.base import ModelBase
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver
from six import with_metaclass
import sys

def _closure_model_unicode(self):
    """__unicode__ implementation for the dynamically created
        <Model>Closure model.
    """
    return "Closure from %s to %s" % (self.parent, self.child)

def create_closure_model(cls):
    """Creates a <Model>Closure model in the same module as the model."""
    meta_vals = {
        'unique_together':  (("parent", "child"),)
    }
    if getattr(cls._meta, 'db_table', None):
        meta_vals['db_table'] = '%sclosure' % getattr(cls._meta, 'db_table')
    model = type('%sClosure' % cls.__name__, (models.Model,), {
        'parent': models.ForeignKey(
            cls.__name__,
            on_delete=models.CASCADE,
            related_name=cls.closure_parentref()
        ),
        'child': models.ForeignKey(
            cls.__name__,
            on_delete=models.CASCADE,
            related_name=cls.closure_childref()
        ),
        'depth': models.IntegerField(),
        '__module__':   cls.__module__,
        '__unicode__': _closure_model_unicode,
        'Meta': type('Meta', (object,), meta_vals),
    })
    setattr(cls, "_closure_model", model)
    return model

class ClosureModelBase(ModelBase):
    """Metaclass for Models inheriting from ClosureModel,
        to ensure the <Model>Closure model is created.
    """
    #This is a metaclass. MAGIC!
    def __init__(cls, name, bases, dct):
        """Create the closure model in addition
            to doing all the normal django stuff.
        """
        super(ClosureModelBase, cls).__init__(name, bases, dct)
        if not cls._meta.get_parent_list() and not cls._meta.abstract:
            setattr(
                sys.modules[cls.__module__],
                '%sClosure' % cls.__name__,
                create_closure_model(cls)
            )

class ClosureModel(with_metaclass(ClosureModelBase, models.Model)):
    """Provides methods to assist in a tree based structure."""
    # pylint: disable=W5101

    class Meta:
        """We make this an abstract class, it needs to be inherited from."""
        # pylint: disable=W0232
        # pylint: disable=R0903
        abstract = True

    def __setattr__(self, name, value):
        if name.endswith('_id'):
            id_field_name = name
        else:
            id_field_name = "%s_id" % name
        if (
            name.startswith(self._closure_sentinel_attr) and  # It's the right attribute
            (  # It's already been set
                (hasattr(self, 'get_deferred_fields') and id_field_name not in self.get_deferred_fields() and hasattr(self, id_field_name)) or  # Django>=1.8
                (not hasattr(self, 'get_deferred_fields') and hasattr(self, id_field_name))  # Django<1.8
            ) and
            not self._closure_change_check()  # The old value isn't stored
        ):
            if name.endswith('_id'):
                obj_id = value
            elif value:
                obj_id = value.pk
            else:
                obj_id = None
            # If this is just setting the same value again, we don't need to do anything
            if getattr(self, id_field_name) != obj_id:
                # Already set once, and not already stored the old
                # value, need to take a copy before it changes
                self._closure_change_init()
        super(ClosureModel, self).__setattr__(name, value)

    @classmethod
    def _toplevel(cls):
        """Find the top level of the chain we're in.

            For example, if we have:
            C inheriting from B inheriting from A inheriting from ClosureModel
            C._toplevel() will return A.
        """
        superclasses = (
            list(set(ClosureModel.__subclasses__()) &
                 set(cls._meta.get_parent_list()))
        )
        return next(iter(superclasses)) if superclasses else cls

    @classmethod
    def rebuildtable(cls):
        """Regenerate the entire closuretree."""
        cls._closure_model.objects.all().delete()
        cls._closure_model.objects.bulk_create([cls._closure_model(
            parent_id=x['pk'],
            child_id=x['pk'],
            depth=0
        ) for x in cls.objects.values("pk")])
        for node in cls.objects.all():
            node._closure_createlink()

    @classmethod
    def closure_parentref(cls):
        """How to refer to parents in the closure tree"""
        return "%sclosure_children" % cls._toplevel().__name__.lower()

    # Backwards compatibility:
    _closure_parentref = closure_parentref

    @classmethod
    def closure_childref(cls):
        """How to refer to children in the closure tree"""
        return "%sclosure_parents" % cls._toplevel().__name__.lower()

    # Backwards compatibility:
    _closure_childref = closure_childref

    @property
    def _closure_sentinel_attr(self):
        """The attribute we need to watch to tell if the
            parent/child relationships have changed
        """
        meta = getattr(self, 'ClosureMeta', None)
        return getattr(meta, 'sentinel_attr', self._closure_parent_attr)

    @property
    def _closure_parent_attr(self):
        '''The attribute or property that holds the parent object.'''
        meta = getattr(self, 'ClosureMeta', None)
        return getattr(meta, 'parent_attr', 'parent')

    @property
    def _closure_parent_pk(self):
        """What our parent pk is in the closure tree."""
        if hasattr(self, "%s_id" % self._closure_parent_attr):
            return getattr(self, "%s_id" % self._closure_parent_attr)
        else:
            parent = getattr(self, self._closure_parent_attr)
            return parent.pk if parent else None

    def _closure_deletelink(self, oldparentpk):
        """Remove incorrect links from the closure tree."""
        self._closure_model.objects.filter(
            **{
                "parent__%s__child" % self._closure_parentref(): oldparentpk,
                "child__%s__parent" % self._closure_childref(): self.pk
            }
        ).delete()

    def _closure_createlink(self):
        """Create a link in the closure tree."""
        linkparents = self._closure_model.objects.filter(
            child__pk=self._closure_parent_pk
        ).values("parent", "depth")
        linkchildren = self._closure_model.objects.filter(
            parent__pk=self.pk
        ).values("child", "depth")
        newlinks = [self._closure_model(
            parent_id=p['parent'],
            child_id=c['child'],
            depth=p['depth']+c['depth']+1
        ) for p in linkparents for c in linkchildren]
        self._closure_model.objects.bulk_create(newlinks)

    def get_ancestors(self, include_self=False, depth=None):
        """Return all the ancestors of this object."""
        if self.is_root_node():
            if not include_self:
                return self._toplevel().objects.none()
            else:
                # Filter on pk for efficiency.
                return self._toplevel().objects.filter(pk=self.pk)

        params = {"%s__child" % self._closure_parentref():self.pk}
        if depth is not None:
            params["%s__depth__lte" % self._closure_parentref()] = depth
        ancestors = self._toplevel().objects.filter(**params)
        if not include_self:
            ancestors = ancestors.exclude(pk=self.pk)
        return ancestors.order_by("%s__depth" % self._closure_parentref())

    def get_descendants(self, include_self=False, depth=None):
        """Return all the descendants of this object."""
        params = {"%s__parent" % self._closure_childref():self.pk}
        if depth is not None:
            params["%s__depth__lte" % self._closure_childref()] = depth
        descendants = self._toplevel().objects.filter(**params)
        if not include_self:
            descendants = descendants.exclude(pk=self.pk)
        return descendants.order_by("%s__depth" % self._closure_childref())

    def prepopulate(self, queryset):
        """Perpopulate a descendants query's children efficiently.
            Call like: blah.prepopulate(blah.get_descendants().select_related(stuff))
        """
        objs = list(queryset)
        hashobjs = dict([(x.pk, x) for x in objs] + [(self.pk, self)])
        for descendant in hashobjs.values():
            descendant._cached_children = []
        for descendant in objs:
            assert descendant._closure_parent_pk in hashobjs
            parent = hashobjs[descendant._closure_parent_pk]
            parent._cached_children.append(descendant)

    def get_children(self):
        """Return all the children of this object."""
        if hasattr(self, '_cached_children'):
            children = self._toplevel().objects.filter(
                pk__in=[n.pk for n in self._cached_children]
            )
            children._result_cache = self._cached_children
            return children
        else:
            return self.get_descendants(include_self=False, depth=1)

    def get_root(self):
        """Return the furthest ancestor of this node."""
        if self.is_root_node():
            return self

        return self.get_ancestors().order_by(
            "-%s__depth" % self._closure_parentref()
        )[0]

    def is_child_node(self):
        """Is this node a child, i.e. has a parent?"""
        return not self.is_root_node()

    def is_root_node(self):
        """Is this node a root, i.e. has no parent?"""
        return self._closure_parent_pk is None

    def is_descendant_of(self, other, include_self=False):
        """Is this node a descendant of `other`?"""
        if other.pk == self.pk:
            return include_self

        return self._closure_model.objects.filter(
            parent=other,
            child=self
        ).exclude(pk=self.pk).exists()

    def is_ancestor_of(self, other, include_self=False):
        """Is this node an ancestor of `other`?"""
        return other.is_descendant_of(self, include_self=include_self)

    def _closure_change_init(self):
        """Part of the change detection. Setting up"""
        # More magic. We're setting this inside setattr...
        # pylint: disable=W0201
        self._closure_old_parent_pk = self._closure_parent_pk

    def _closure_change_check(self):
        """Part of the change detection. Have we changed since we began?"""
        return hasattr(self,"_closure_old_parent_pk")

    def _closure_change_oldparent(self):
        """Part of the change detection. What we used to be"""
        return self._closure_old_parent_pk


@receiver(post_save, dispatch_uid='closure-model-save')
def closure_model_save(sender, **kwargs):
    if issubclass(sender, ClosureModel):
        instance = kwargs['instance']
        create = kwargs['created']
        if create:
            closure_instance = instance._closure_model(
                parent=instance,
                child=instance,
                depth=0
            )
            closure_instance.save()
        if instance._closure_change_check():
            #Changed parents.
            if instance._closure_change_oldparent():
                instance._closure_deletelink(instance._closure_change_oldparent())
            instance._closure_createlink()
            delattr(instance, "_closure_old_parent_pk")
        elif create:
            # We still need to create links when we're first made
            instance._closure_createlink()


@receiver(pre_delete, dispatch_uid='closure-model-delete')
def closure_model_delete(sender, **kwargs):
    if issubclass(sender, ClosureModel):
        instance = kwargs['instance']
        instance._closure_deletelink(instance._closure_parent_pk)
