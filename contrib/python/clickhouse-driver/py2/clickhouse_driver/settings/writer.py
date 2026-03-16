import logging

from ..writer import write_binary_str, write_binary_uint8
from .available import settings as available_settings


logger = logging.getLogger(__name__)


def write_settings(settings, buf, settings_as_strings, is_important=False):
    for setting, value in (settings or {}).items():
        # If the server support settings as string we do not need to know
        # anything about them, so we can write any setting.
        if settings_as_strings:
            write_binary_str(setting, buf)
            write_binary_uint8(int(is_important), buf)
            write_binary_str(str(value), buf)

        else:
            # If the server requires string in binary,
            # then they cannot be written without type.
            setting_writer = available_settings.get(setting)
            if not setting_writer:
                logger.warning('Unknown setting %s. Skipping', setting)
                continue
            write_binary_str(setting, buf)
            setting_writer.write(value, buf)

    write_binary_str('', buf)  # end of settings
