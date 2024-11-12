#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import os
import pytest
import json

from google.protobuf.internal.containers import RepeatedScalarContainer

from ydb.tests.library.common import yatest_common
from ydb.tests.library.common.local_db_scheme import get_scheme
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start
from ydb.tests.oss.canonical import set_canondata_root


def write_canonical_file(tablet, content):
    def convert_to_serializable(obj):
        if isinstance(obj, RepeatedScalarContainer):
            return list(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    fn = os.path.join(yatest_common.output_path(), tablet + '.schema')
    with open(fn, 'w') as w:
        w.write(
            json.dumps(
                content, indent=4, default=convert_to_serializable
            )
        )

    return fn


def get_tablets():
    return [
        (mod,)
        for mod in [
            'flat_bs_controller',
            'flat_datashard',
            'flat_hive',
            'flat_schemeshard',
            'flat_tx_coordinator',
            'tx_allocator',
            'keyvalueflat',
            'tx_mediator',
            'persqueue',
            'kesus',
        ]
    ]


class TestTabletSchemes(object):
    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.client = cls.cluster.client
        cls.shard_index = itertools.count(start=1)
        cls.to_prepare = (
            TabletTypes.PERSQUEUE, TabletTypes.KEYVALUEFLAT, TabletTypes.FLAT_DATASHARD, TabletTypes.KESUS)
        set_canondata_root('ydb/tests/functional/scheme_tests/canondata')

    def prepare_tablet(self, tablet_type):
        if tablet_type in self.to_prepare:
            create_tablets_and_wait_for_start(
                self.client, number_of_tablets=1, tablet_type=tablet_type,
                binded_channels=self.cluster.default_channel_bindings,
                owner_id=next(
                    self.shard_index
                )
            )
        return tablet_type

    def get_tablet_id(self, tablet_type):
        resp = self.client.tablet_state(tablet_type)
        infos = [info for info in resp.TabletStateInfo]
        tablet_id = infos[0].TabletId
        return tablet_id

    @pytest.mark.parametrize(['tablet'], get_tablets())
    def test_tablet_schemes(self, tablet):
        tablet_type = self.prepare_tablet(getattr(TabletTypes, tablet.upper()))
        scheme = get_scheme(self.client, self.get_tablet_id(tablet_type))
        scheme = [element.data for element in scheme]
        return {
            'schema': yatest_common.canonical_file(
                local=True,
                universal_lines=True,
                path=write_canonical_file(
                    tablet,
                    scheme
                )
            )
        }
