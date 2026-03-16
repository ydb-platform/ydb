"""
    This module enables automatic Model registration with custom Moderators

    usage example:

        class MyModel(ModeratedModel):
            desc = models.TextField()

            class Moderator:
                notify_user = False

"""
import inspect

from django.db.models import base
from django.utils.six import with_metaclass

from .moderator import GenericModerator
from .register import ModerationManager
from .utils import clear_builtins

moderation = ModerationManager()


class ModeratedModelBase(type):
    """
    Metaclass for the ``ModeratedModel`` type

        -- automatically registers ``ModeratedModel's``
        -- resolves subclass ``Moderator`` into
           a instance of ``GenericModerator``

    """
    def _resolve_moderator(cls):
        """
        ``ModeratedModel`` that defines the class Moderator
        will have that class resolved into
        a class derived from ``GenericModerator``

        usage example:

        class MyModel(ModeratedModel):
            desc = models.TextField()

            # ``Moderator`` below will extend ``GenericModerator``
            # and will be used when the ``Model`` is registered
            class Moderator:
                notify_user = False

        """
        if hasattr(cls, 'Moderator') and inspect.isclass(cls.Moderator):
            Moderator = cls.Moderator
            # in python3 __dict__ is dictproxy
            attrs = dict(Moderator.__dict__)
            attrs = clear_builtins(attrs)

            return type(
                '%sModerator' % cls.__name__,
                (GenericModerator,),
                attrs,
            )
        else:
            return None

    def __init__(cls, name, bases, clsdict):
        """
        Registers ``ModeratedModel``

        """
        super().__init__(name, bases, clsdict)

        if any(x.__name__ == 'ModeratedModel' for x in cls.mro()[1:]):
            moderation.register(cls, cls._resolve_moderator())


class ModelBase(ModeratedModelBase, base.ModelBase):
    """
    Common metaclass for ``ModeratedModel`` enabling it to inherit
    the behavior of django ``Model`` objects

    """


class ModeratedModel(with_metaclass(ModelBase, base.Model)):
    class Meta:
        abstract = True
