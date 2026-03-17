"""
Model fields for working with trees.
"""

from django.db import models

from mptt.forms import TreeNodeChoiceField, TreeNodeMultipleChoiceField


__all__ = ("TreeForeignKey", "TreeManyToManyField", "TreeOneToOneField")


class TreeForeignKey(models.ForeignKey):
    """
    Extends the foreign key, but uses mptt's ``TreeNodeChoiceField`` as
    the default form field.

    This is useful if you are creating models that need automatically
    generated ModelForms to use the correct widgets.
    """

    def formfield(self, **kwargs):
        """
        Use MPTT's ``TreeNodeChoiceField``
        """
        kwargs.setdefault("form_class", TreeNodeChoiceField)
        return super().formfield(**kwargs)


class TreeOneToOneField(models.OneToOneField):
    def formfield(self, **kwargs):
        kwargs.setdefault("form_class", TreeNodeChoiceField)
        return super().formfield(**kwargs)


class TreeManyToManyField(models.ManyToManyField):
    def formfield(self, **kwargs):
        kwargs.setdefault("form_class", TreeNodeMultipleChoiceField)
        return super().formfield(**kwargs)
