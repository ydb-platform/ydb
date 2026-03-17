from django.forms.forms import DeclarativeFieldsMetaclass
from django.forms.models import ModelFormMetaclass

from .base import SiteformsMixin


class BaseMeta(DeclarativeFieldsMetaclass):

    def __new__(mcs, name, bases, attrs):
        cls: SiteformsMixin = super().__new__(mcs, name, bases, attrs)
        cls._meta_hook()
        return cls


class ModelBaseMeta(BaseMeta, ModelFormMetaclass):
    pass
