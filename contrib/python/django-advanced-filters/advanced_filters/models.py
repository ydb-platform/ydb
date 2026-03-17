from django.conf import settings
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext_lazy as _

from .q_serializer import QSerializer


class UserLookupManager(models.Manager):
    def filter_by_user(self, user):
        """All filters that should be displayed to a user (by users/group)"""

        return self.filter(Q(users=user) | Q(groups__in=user.groups.all()))


class AdvancedFilter(models.Model):
    class Meta:
        verbose_name = _('Advanced Filter')
        verbose_name_plural = _('Advanced Filters')

    title = models.CharField(max_length=255, null=False, blank=False, verbose_name=_('Title'))
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='created_advanced_filters',
        verbose_name=_('Created by'),
        on_delete=models.CASCADE,
    )
    created_at = models.DateTimeField(auto_now_add=True, null=True, verbose_name=_('Created at'))
    url = models.CharField(max_length=255, null=False, blank=False, verbose_name=_('URL'))
    users = models.ManyToManyField(settings.AUTH_USER_MODEL, blank=True, verbose_name=_('Users'))
    groups = models.ManyToManyField('auth.Group', blank=True, verbose_name=_('Groups'))

    objects = UserLookupManager()

    b64_query = models.CharField(max_length=2048)
    model = models.CharField(max_length=64, blank=True, null=True)

    @property
    def query(self):
        """
        De-serialize, decode and return an ORM query stored in b64_query.
        """
        if not self.b64_query:
            return None
        s = QSerializer(base64=True)
        return s.loads(self.b64_query)

    @query.setter
    def query(self, value):
        """
        Serialize an ORM query, Base-64 encode it and set it to
        the b64_query field
        """
        if not isinstance(value, Q):
            raise Exception('Must only be passed a Django (Q)uery object')
        s = QSerializer(base64=True)
        self.b64_query = s.dumps(value)

    def list_fields(self):
        s = QSerializer(base64=True)
        d = s.loads(self.b64_query, raw=True)
        return s.get_field_values_list(d)
