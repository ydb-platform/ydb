"""
Record tqdm progress bar fail during session
"""

import functools
import logging

# pylint: disable=invalid-name
_SHOW_PROGRESS: bool = True


def allow_show_progress() -> bool:
    """Return False if any progressbar errors have occurred this session"""
    return _SHOW_PROGRESS


def _disable_progress(e: Exception) -> None:
    """Print an exception and disable progress bars for this session"""
    # pylint: disable=global-statement
    global _SHOW_PROGRESS
    if _SHOW_PROGRESS:
        _SHOW_PROGRESS = False
        logging.getLogger('cmdstanpy').error(
            'Error in progress bar initialization:\n'
            '\t%s\n'
            'Disabling progress bars for this session',
            str(e),
        )


def wrap_callback(func):  # type: ignore
    """Wrap a callback generator so it fails safely"""

    @functools.wraps(func)
    def safe_progress(*args, **kwargs):  # type: ignore
        # pylint: disable=unused-argument
        def callback(*args, **kwargs):  # type: ignore
            # totally empty callback
            return None

        if not allow_show_progress():
            return callback

        try:
            return func(*args, **kwargs)
        # pylint: disable=broad-except
        except Exception as e:
            _disable_progress(e)
            return callback

    return safe_progress
