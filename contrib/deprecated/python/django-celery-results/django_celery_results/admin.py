"""Result Task Admin interface."""
from __future__ import absolute_import, unicode_literals

from django.contrib import admin

from django.conf import settings
from django.utils.translation import ugettext_lazy as _

try:
    ALLOW_EDITS = settings.DJANGO_CELERY_RESULTS['ALLOW_EDITS']
except (AttributeError, KeyError):
    ALLOW_EDITS = False
    pass

from .models import TaskResult


class TaskResultAdmin(admin.ModelAdmin):
    """Admin-interface for results of tasks."""

    model = TaskResult
    date_hierarchy = 'date_done'
    list_display = ('task_id', 'task_name', 'date_done', 'status', 'worker')
    list_filter = ('status', 'date_done', 'task_name', 'worker')
    readonly_fields = ('date_created', 'date_done', 'result', 'meta')
    search_fields = ('task_name', 'task_id', 'status')
    fieldsets = (
        (None, {
            'fields': (
                'task_id',
                'task_name',
                'status',
                'worker',
                'content_type',
                'content_encoding',
            ),
            'classes': ('extrapretty', 'wide')
        }),
        (_('Parameters'), {
            'fields': (
                'task_args',
                'task_kwargs',
            ),
            'classes': ('extrapretty', 'wide')
        }),
        (_('Result'), {
            'fields': (
                'result',
                'date_created',
                'date_done',
                'traceback',
                'meta',
            ),
            'classes': ('extrapretty', 'wide')
        }),
    )

    def get_readonly_fields(self, request, obj=None):
        if ALLOW_EDITS:
            return self.readonly_fields
        else:
            return list(set(
                [field.name for field in self.opts.local_fields]
            ))


admin.site.register(TaskResult, TaskResultAdmin)
