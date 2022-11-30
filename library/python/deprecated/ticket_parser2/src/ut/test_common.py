#!/usr/bin/env python
from __future__ import print_function

import ticket_parser2 as tp2
from ticket_parser2 import BlackboxClientId


def test_version():
    assert tp2.__version__[:-5] == 'py_'


def test_blackbox_client_id():
    assert BlackboxClientId.Prod.value == '222'
    assert BlackboxClientId.Test.value == '224'
    assert BlackboxClientId.ProdYateam.value == '223'
    assert BlackboxClientId.TestYateam.value == '225'
    assert BlackboxClientId.Stress.value == '226'
    assert BlackboxClientId.Mimino.value == '239'
