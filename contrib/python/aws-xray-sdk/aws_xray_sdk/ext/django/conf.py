import os

from django.conf import settings as django_settings
from django.test.signals import setting_changed

DEFAULTS = {
    'AWS_XRAY_DAEMON_ADDRESS': '127.0.0.1:2000',
    'AUTO_INSTRUMENT': True,
    'AWS_XRAY_CONTEXT_MISSING': 'LOG_ERROR',
    'PLUGINS': (),
    'SAMPLING': True,
    'SAMPLING_RULES': None,
    'SAMPLER': None,
    'AWS_XRAY_TRACING_NAME': None,
    'DYNAMIC_NAMING': None,
    'STREAMING_THRESHOLD': None,
    'MAX_TRACE_BACK': None,
    'STREAM_SQL': True,
    'PATCH_MODULES': [],
    'AUTO_PATCH_PARENT_SEGMENT_NAME': None,
    'IGNORE_MODULE_PATTERNS': [],
    'URLS_AS_ANNOTATION': 'LAMBDA',  # 3 valid values, NONE -> don't ever, LAMBDA -> only for AWS Lambdas, ALL -> every time  
}

XRAY_NAMESPACE = 'XRAY_RECORDER'

SUPPORTED_ENV_VARS = ('AWS_XRAY_DAEMON_ADDRESS',
                      'AWS_XRAY_CONTEXT_MISSING',
                      'AWS_XRAY_TRACING_NAME',
                      )


class XRaySettings:
    """
    A object of Django settings to easily modify certain fields.
    The precedence for configurations at different places is as follows:
    environment variables > user settings in settings.py > default settings
    """
    def __init__(self, user_settings=None):

        self.defaults = DEFAULTS

        if user_settings:
            self._user_settings = user_settings

    @property
    def user_settings(self):

        if not hasattr(self, '_user_settings'):
            self._user_settings = getattr(django_settings, XRAY_NAMESPACE, {})

        return self._user_settings

    def __getattr__(self, attr):

        if attr not in self.defaults:
            raise AttributeError('Invalid setting: %s' % attr)

        if self.user_settings.get(attr, None) is not None:
            if attr in SUPPORTED_ENV_VARS:
                return os.getenv(attr, self.user_settings[attr])
            else:
                return self.user_settings[attr]
        elif attr in SUPPORTED_ENV_VARS:
            return os.getenv(attr, self.defaults[attr])
        else:
            return self.defaults[attr]


settings = XRaySettings()


def reload_settings(*args, **kwargs):
    """
    Reload X-Ray user settings upon Django server hot restart
    """
    global settings
    setting, value = kwargs['setting'], kwargs['value']
    if setting == XRAY_NAMESPACE:
        settings = XRaySettings(value)


setting_changed.connect(reload_settings)
