#!/usr/bin/env python
from __future__ import print_function

import tvmauth
from tvmauth import BlackboxTvmId
from tvmauth.exceptions import TicketParsingException


def test_version():
    assert tvmauth.__version__[:-5] == 'py_'


def test_blackbox_tvm_id():
    assert BlackboxTvmId.Prod.value == '222'
    assert BlackboxTvmId.Test.value == '224'
    assert BlackboxTvmId.ProdYateam.value == '223'
    assert BlackboxTvmId.TestYateam.value == '225'
    assert BlackboxTvmId.Stress.value == '226'
    assert BlackboxTvmId.Mimino.value == '239'


def test_exceptions():
    e = TicketParsingException('aaa', 'bbb', 'ccc')
    assert str(e) == 'aaa: ccc'
