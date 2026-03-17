# -*- coding: utf-8 -*-
from __future__ import absolute_import


def test_version():
    from pycrfsuite import CRFSUITE_VERSION
    assert bool(CRFSUITE_VERSION), CRFSUITE_VERSION
