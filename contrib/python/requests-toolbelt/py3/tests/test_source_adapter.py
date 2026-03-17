# -*- coding: utf-8 -*-
from requests.adapters import DEFAULT_POOLSIZE, DEFAULT_POOLBLOCK
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
from requests_toolbelt.adapters.source import SourceAddressAdapter

import pytest


@patch('requests_toolbelt.adapters.source.poolmanager')
def test_source_address_adapter_string(poolmanager):
    SourceAddressAdapter('10.10.10.10')

    poolmanager.PoolManager.assert_called_once_with(
        num_pools=DEFAULT_POOLSIZE,
        maxsize=DEFAULT_POOLSIZE,
        block=DEFAULT_POOLBLOCK,
        source_address=('10.10.10.10', 0)
    )


@patch('requests_toolbelt.adapters.source.poolmanager')
def test_source_address_adapter_tuple(poolmanager):
    SourceAddressAdapter(('10.10.10.10', 80))

    poolmanager.PoolManager.assert_called_once_with(
        num_pools=DEFAULT_POOLSIZE,
        maxsize=DEFAULT_POOLSIZE,
        block=DEFAULT_POOLBLOCK,
        source_address=('10.10.10.10', 80)
    )


@patch('requests_toolbelt.adapters.source.poolmanager')
def test_source_address_adapter_type_error(poolmanager):
    with pytest.raises(TypeError):
        SourceAddressAdapter({'10.10.10.10': 80})

    assert not poolmanager.PoolManager.called
