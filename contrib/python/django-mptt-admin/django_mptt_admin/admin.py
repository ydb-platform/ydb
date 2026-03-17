from mptt.admin import MPTTModelAdmin

from .django_mptt_admin_mixin import DjangoMpttAdminMixin


class DjangoMpttAdmin(DjangoMpttAdminMixin, MPTTModelAdmin):
    pass


class FilterableDjangoMpttAdmin(DjangoMpttAdmin):
    pass
