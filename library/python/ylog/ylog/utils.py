# -*- coding:utf-8 -*-
import logging
import sys

from ylog.format import exception_str


def log_warning():
    '''
    A simple helper to log current exception as a WARNING. Saves boilerplate.
    '''
    exc_info = sys.exc_info()
    logging.getLogger('django.request').warning(exception_str(exc_info[1]), exc_info=exc_info)
