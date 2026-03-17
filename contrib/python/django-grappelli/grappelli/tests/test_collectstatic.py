import tempfile

from django.core.management import call_command
from django.test import TestCase, override_settings

class TestCollectstatic(TestCase):
    """
    Test manage.py collectstatic --noinput --link

    with different versions of STATICFILES_STORAGE. See
    https://github.com/sehmaschine/django-grappelli/issues/1022
    """

    def test_collect_static(self):
        for storage in [
            "django.contrib.staticfiles.storage.StaticFilesStorage",
            "django.contrib.staticfiles.storage.ManifestStaticFilesStorage",
        ]:
            with override_settings(
                STATICFILES_STORAGE=storage,
                STATIC_ROOT=tempfile.mkdtemp(),
            ):
                call_command("collectstatic", "--noinput", "--link")
