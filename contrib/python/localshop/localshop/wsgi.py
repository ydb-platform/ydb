import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'localshop.settings')

from django.core.wsgi import get_wsgi_application  # noqa isort:skip

application = get_wsgi_application()
