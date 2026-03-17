# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
from .version import __version__


def get_path():
    """ Return path to the dictionary. """
    return os.path.join(os.path.dirname(__file__), 'data')
