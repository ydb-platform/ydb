# coding: utf8
from __future__ import absolute_import, division, print_function, unicode_literals

import os
from django.forms.renderers import BaseRenderer
from django.utils.functional import cached_property

from library.python.django.template.backends.arcadia import ArcadiaTemplates

ROOT = os.path.dirname(__loader__.get_filename('django.forms'))  # noqa


class ArcadiaRenderer(BaseRenderer):
    backend = ArcadiaTemplates

    def get_template(self, template_name):
        return self.engine.get_template(template_name)

    @cached_property
    def engine(self):
        return self.backend({
            'NAME': 'djangoforms',
            'OPTIONS': {
                'debug': False,
                'loaders': [
                    'library.python.django.template.loaders.resource.Loader',
                    'library.python.django.template.loaders.app_resource.Loader',
                ],
            },
            'DIRS': [os.path.join(ROOT, self.backend.app_dirname)],
            'APP_DIRS': False,
        })
