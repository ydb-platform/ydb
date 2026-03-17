from django.db import models
from django.utils.translation import gettext_lazy as _


class Summary(models.Model):

    class Meta:
        app_label = 'uwsgify'
        managed = False
        verbose_name = _('Summary')
        verbose_name_plural = _('Summary')


class Configuration(models.Model):

    class Meta:
        app_label = 'uwsgify'
        managed = False
        verbose_name = _('Configuration')
        verbose_name_plural = _('Configuration')


class Workers(models.Model):

    class Meta:
        app_label = 'uwsgify'
        managed = False
        verbose_name = _('Workers')
        verbose_name_plural = _('Workers')


class Maintenance(models.Model):

    class Meta:
        app_label = 'uwsgify'
        managed = False
        verbose_name = _('Maintenance')
        verbose_name_plural = _('Maintenance')
