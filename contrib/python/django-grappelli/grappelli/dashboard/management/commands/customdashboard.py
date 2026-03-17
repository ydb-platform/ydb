# coding: utf-8

# PYTHON IMPORTS
import os

# DJANGO IMPORTS
from django.core.management.base import BaseCommand, CommandError
from django.template.loader import render_to_string

DEFAULT_FILE = 'dashboard.py'


class Command(BaseCommand):
    help = ('Creates a template file containing the base code to get you '
            'started with your custom dashboard.')
    args = '[file]'
    label = 'application name'

    def handle(self, file=None, **options):
        context = {}
        context['project'] = os.path.basename(os.getcwd())
        tpl = ['dashboard/dashboard.txt', 'grappelli/dashboard/dashboard.txt']
        dst = file is not None and file or DEFAULT_FILE
        if os.path.exists(dst):
            raise CommandError('file "%s" already exists' % dst)
        context['file'] = os.path.basename(dst).split('.')[0]
        open(dst, 'w').write(render_to_string(tpl, context))
        print('"%s" written.' % os.path.join(dst))
