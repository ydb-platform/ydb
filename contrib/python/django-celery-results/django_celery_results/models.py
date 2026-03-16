"""Database models."""

import json

from celery import states
from celery.result import GroupResult as CeleryGroupResult
from celery.result import result_from_tuple
from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _

from . import managers

ALL_STATES = sorted(states.ALL_STATES)
TASK_STATE_CHOICES = sorted(zip(ALL_STATES, ALL_STATES))


class TaskResult(models.Model):
    """Task result/status."""

    task_id = models.CharField(
        max_length=getattr(
            settings,
            'DJANGO_CELERY_RESULTS_TASK_ID_MAX_LENGTH',
            255
        ),
        unique=True,
        verbose_name=_('Task ID'),
        help_text=_('Celery ID for the Task that was run'))
    periodic_task_name = models.CharField(
        null=True, max_length=255,
        verbose_name=_('Periodic Task Name'),
        help_text=_('Name of the Periodic Task which was run'))
    task_name = models.CharField(
        null=True, max_length=getattr(
            settings,
            'DJANGO_CELERY_RESULTS_TASK_ID_MAX_LENGTH',
            255
        ),
        verbose_name=_('Task Name'),
        help_text=_('Name of the Task which was run'))
    task_args = models.TextField(
        null=True,
        verbose_name=_('Task Positional Arguments'),
        help_text=_('JSON representation of the positional arguments '
                    'used with the task'))
    task_kwargs = models.TextField(
        null=True,
        verbose_name=_('Task Named Arguments'),
        help_text=_('JSON representation of the named arguments '
                    'used with the task'))
    status = models.CharField(
        max_length=50, default=states.PENDING,
        verbose_name=_('Task State'),
        help_text=_('Current state of the task being run'))
    worker = models.CharField(
        max_length=100, default=None, null=True,
        verbose_name=_('Worker'), help_text=_('Worker that executes the task')
    )
    content_type = models.CharField(
        max_length=128,
        verbose_name=_('Result Content Type'),
        help_text=_('Content type of the result data'))
    content_encoding = models.CharField(
        max_length=64,
        verbose_name=_('Result Encoding'),
        help_text=_('The encoding used to save the task result data'))
    result = models.TextField(
        null=True, default=None, editable=False,
        verbose_name=_('Result Data'),
        help_text=_('The data returned by the task.  '
                    'Use content_encoding and content_type fields to read.'))
    date_created = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_('Created DateTime'),
        help_text=_('Datetime field when the task result was created in UTC'))
    date_started = models.DateTimeField(
        null=True, default=None,
        verbose_name=_('Started DateTime'),
        help_text=_('Datetime field when the task was started in UTC'))
    date_done = models.DateTimeField(
        auto_now=True,
        verbose_name=_('Completed DateTime'),
        help_text=_('Datetime field when the task was completed in UTC'))
    traceback = models.TextField(
        blank=True, null=True,
        verbose_name=_('Traceback'),
        help_text=_('Text of the traceback if the task generated one'))
    meta = models.TextField(
        null=True, default=None, editable=False,
        verbose_name=_('Task Meta Information'),
        help_text=_('JSON meta information about the task, '
                    'such as information on child tasks'))

    objects = managers.TaskResultManager()

    class Meta:
        """Table information."""

        ordering = ['-date_done']

        verbose_name = _('task result')
        verbose_name_plural = _('task results')

        # Explicit names to solve https://code.djangoproject.com/ticket/33483
        indexes = [
            models.Index(fields=['task_name'],
                         name='django_cele_task_na_08aec9_idx'),
            models.Index(fields=['status'],
                         name='django_cele_status_9b6201_idx'),
            models.Index(fields=['worker'],
                         name='django_cele_worker_d54dd8_idx'),
            models.Index(fields=['date_created'],
                         name='django_cele_date_cr_f04a50_idx'),
            models.Index(fields=['date_done'],
                         name='django_cele_date_do_f59aad_idx'),
            models.Index(fields=['periodic_task_name'],
                         name='django_cele_periodi_1993cf_idx'),
        ]

    def as_dict(self):
        return {
            'task_id': self.task_id,
            'task_name': self.task_name,
            'task_args': self.task_args,
            'task_kwargs': self.task_kwargs,
            'status': self.status,
            'result': self.result,
            'date_done': self.date_done,
            'traceback': self.traceback,
            'meta': self.meta,
            'worker': self.worker
        }

    def __str__(self):
        return '<Task: {0.task_id} ({0.status})>'.format(self)


class ChordCounter(models.Model):
    """Chord synchronisation."""

    group_id = models.CharField(
        max_length=getattr(
            settings,
            "DJANGO_CELERY_RESULTS_TASK_ID_MAX_LENGTH",
            255),
        unique=True,
        verbose_name=_("Group ID"),
        help_text=_("Celery ID for the Chord header group"),
    )
    sub_tasks = models.TextField(
        help_text=_(
            "JSON serialized list of task result tuples. "
            "use .group_result() to decode"
        )
    )
    count = models.PositiveIntegerField(
        help_text=_(
            "Starts at len(chord header) and decrements after each task is "
            "finished"
        )
    )

    def group_result(self, app=None):
        """Return the :class:`celery.result.GroupResult` of self.

        Arguments:
            app (celery.app.base.Celery): app instance to create the
               :class:`celery.result.GroupResult` with.

        """
        return CeleryGroupResult(
            self.group_id,
            [result_from_tuple(r, app=app)
             for r in json.loads(self.sub_tasks)],
            app=app
        )


class GroupResult(models.Model):
    """Task Group result/status."""

    group_id = models.CharField(
        max_length=getattr(
            settings,
            "DJANGO_CELERY_RESULTS_TASK_ID_MAX_LENGTH",
            255
        ),
        unique=True,
        verbose_name=_("Group ID"),
        help_text=_("Celery ID for the Group that was run"),
    )
    date_created = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Created DateTime"),
        help_text=_("Datetime field when the group result was created in UTC"),
    )
    date_done = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Completed DateTime"),
        help_text=_("Datetime field when the group was completed in UTC"),
    )
    content_type = models.CharField(
        max_length=128,
        verbose_name=_("Result Content Type"),
        help_text=_("Content type of the result data"),
    )
    content_encoding = models.CharField(
        max_length=64,
        verbose_name=_("Result Encoding"),
        help_text=_("The encoding used to save the task result data"),
    )
    result = models.TextField(
        null=True, default=None, editable=False,
        verbose_name=_('Result Data'),
        help_text=_('The data returned by the task.  '
                    'Use content_encoding and content_type fields to read.'))

    def as_dict(self):
        return {
            'group_id': self.group_id,
            'result': self.result,
            'date_done': self.date_done,
        }

    def __str__(self):
        return f'<Group: {self.group_id}>'

    objects = managers.GroupResultManager()

    class Meta:
        """Table information."""

        ordering = ['-date_done']

        verbose_name = _('group result')
        verbose_name_plural = _('group results')

        # Explicit names to solve https://code.djangoproject.com/ticket/33483
        indexes = [
            models.Index(fields=['date_created'],
                         name='django_cele_date_cr_bd6c1d_idx'),
            models.Index(fields=['date_done'],
                         name='django_cele_date_do_caae0e_idx'),
        ]
