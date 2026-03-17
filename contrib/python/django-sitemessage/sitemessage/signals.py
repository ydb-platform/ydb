"""This file contains signals emitted by sitemessage."""
import django.dispatch


sig_unsubscribe_success = django.dispatch.Signal()
"""Emitted when user unsubscribe requested is successful.
providing_args=['request', 'message', 'dispatch']

"""

sig_unsubscribe_failed = django.dispatch.Signal()
"""Emitted when user unsubscribe requested fails.
providing_args=['request', 'message', 'dispatch']

"""

sig_mark_read_success = django.dispatch.Signal()
"""Emitted when mark read requested is successful.
providing_args=['request', 'message', 'dispatch']

"""

sig_mark_read_failed = django.dispatch.Signal()
"""Emitted when mark read requested fails.
providing_args=['request', 'message', 'dispatch']

"""
