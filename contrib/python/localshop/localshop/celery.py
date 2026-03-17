import os

import celery  # noqa isort:skip
from django.conf import settings  # noqa isort:skip


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'localshop.settings')

app = celery.Celery('localshop')
app.config_from_object('django.conf:settings')
