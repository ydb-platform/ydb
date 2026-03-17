from django.apps import AppConfig


class SpectacularConfig(AppConfig):
    name = 'drf_spectacular'
    verbose_name = "drf-spectacular"

    def ready(self):
        import drf_spectacular.checks  # noqa: F401
