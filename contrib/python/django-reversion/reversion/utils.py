"""
Utility functions for django-reversion.
"""
from contextlib import contextmanager


@contextmanager
def mute_signals(*signals):
    """
    Context manager that temporarily disables and then restores Django signals.

    This is useful when performing operations that shouldn't trigger signal handlers,
    such as viewing historical revisions where the signals were already fired when
    the data was originally saved.

    Args:
        *signals (django.dispatch.dispatcher.Signal): any Django signals to mute

    Example:
        from django.db.models.signals import pre_save, post_save

        with mute_signals(pre_save, post_save):
            # Any save operations here won't fire pre_save or post_save signals
            obj.save()
    """
    paused = {}

    # Store current receivers and mute signals (thread-safe)
    for signal in signals:
        with signal.lock:
            # Store the current receivers for restoration later
            paused[signal] = signal.receivers[:]
            # Clear the receivers list to mute the signal
            signal.receivers = []
            # Clear cache since we're bypassing connect/disconnect
            signal.sender_receivers_cache.clear()

    try:
        yield
    finally:
        # Restore the original receivers (thread-safe)
        for signal, original_receivers in paused.items():
            with signal.lock:
                # Restore original receivers, preserving any new ones added during muting
                signal.receivers = original_receivers + signal.receivers
                # Clear cache to ensure consistency
                signal.sender_receivers_cache.clear()
