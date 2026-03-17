# coding: utf-8

from __future__ import unicode_literals

from django.conf import settings
try:
    from dir_data_sync.org_ctx import get_org
except ImportError:
    def get_org():
        pass

from .base import BaseProvider


class Provider(BaseProvider):
    def org(self):
        if not getattr(settings, 'DIRSYNC_IS_BUSINESS', False):
            return None

        try:
            org = get_org()
        except Exception:
            org = None

        return {
            'dir_id': org.dir_id if org else None,
        }
