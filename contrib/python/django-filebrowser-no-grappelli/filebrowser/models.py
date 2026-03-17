from django.db import models
from django.utils.translation import gettext_lazy as _


class FileBrowser(models.Model):
    class Meta:
        managed = False
        verbose_name = _("FileBrowser")
        verbose_name_plural = _("FileBrowser")
