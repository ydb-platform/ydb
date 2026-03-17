from typing import Optional, TypeVar

from django.contrib.admin.decorators import register
from django.db import models
from django.http import HttpRequest, HttpResponse
from django.utils.translation import gettext_lazy as _

from .admins import CustomPageModelAdmin, EtcAdmin

TypeEtcAdmin = TypeVar('TypeEtcAdmin', bound=EtcAdmin)
TypeHttpResponse = TypeVar('TypeHttpResponse', bound=HttpResponse)


class CustomModelPage(models.Model):
    """Allows construction of admin pages based on user input.

    Define your fields (as usual in models) and override .save() method.

    .. code-block:: python

        class MyPage(CustomModelPage):

            title = 'Test page 1'  # set page title

            admin_cls = MyAdmin  # set admin class

            # Define some fields.
            my_field = models.CharField('some title', max_length=10)

            def save(self):
                ...  # Implement data handling.
                super().save()

        # Register my page within Django admin.
        MyPage.register()

    """
    title: str = _('Custom page')
    """Page title to be used."""

    app_label: str = 'admin'
    """Application label to relate page to. Default: admin"""

    bound_request: Optional[HttpRequest] = None
    """Request object bound runtime to this page model."""

    bound_response: Optional[HttpResponse] = None
    """Response object that could be bound runtime to pass to admin model,
    to return in .response_add()."""

    admin_cls: TypeEtcAdmin = CustomPageModelAdmin
    """Django admin model class to use with this model page."""

    bound_admin: Optional[EtcAdmin] = None
    """Django admin model instance bound runtime to this model."""

    class Meta:
        abstract = True
        managed = False

    @classmethod
    def __init_subclass__(cls) -> None:
        meta = cls.Meta
        meta.verbose_name = meta.verbose_name_plural = cls.title
        meta.app_label = cls.app_label
        super().__init_subclass__()

    @classmethod
    def register(cls, *, admin_model: TypeEtcAdmin = None):
        """Registers this model page class in Django admin.

        :param admin_model:

        """
        register(cls)(admin_model or cls.admin_cls)

    def save(self):  # noqa
        """Heirs should implement their own save handling."""
        self.bound_admin.message_success(self.bound_request, _('Done.'))
