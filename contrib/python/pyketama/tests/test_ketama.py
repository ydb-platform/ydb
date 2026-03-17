# -*- coding: utf-8 -*-

from ketama import Continuum


def test_init():
    data = ['127.0.0.1:11211', '127.0.0.1:12212']
    cont = Continuum(data)

    assert cont["test"] == '127.0.0.1:11211'


def test_init_weight():
    data = [('127.0.0.1:11211', 100), ('127.0.0.1:12212', 800)]
    cont = Continuum(data)

    assert cont["test"] == '127.0.0.1:12212'


def test_init_value():
    data = [('tom', {'a': "dict"}, 500), ('jerry', [123], 800),
            ("goffy", None, 500)]

    cont = Continuum(data)

    assert cont['yellow'] == {'a': "dict"}


def test_setitem():
    cont = Continuum()

    cont['127.0.0.1:11211'] = '127.0.0.1:11211'
    cont['127.0.0.1:11212'] = '127.0.0.1:11212'

    assert cont['test'] == '127.0.0.1:11211'


def test_setitem_weight():
    cont = Continuum()

    cont[('127.0.0.1:11211', 100)] = '127.0.0.1:11211'
    cont[('127.0.0.1:11212', 800)] = '127.0.0.1:11212'

    assert cont['test'] == '127.0.0.1:11212'
