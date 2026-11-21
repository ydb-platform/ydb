import unittest

from ydb.apps.dstool.lib import common


class TestPDiskLocationFilter(unittest.TestCase):
    def setUp(self):
        self.config = common.kikimr_bsconfig.TBaseConfig()
        self.pdisk = self.config.PDisk.add(NodeId=1, PDiskId=1, BoxId=1, Type=common.EPDiskType.ROT)
        self.location = self.config.Node.add(NodeId=1).Location
        self.pool = common.kikimr_bsconfig.TDefineStoragePool(BoxId=1, StoragePoolId=1)
        self.filter = self.pool.PDiskFilter.add()
        self.filter.Property.add(Type=common.EPDiskType.ROT)
        self.required = self.filter.Property.add().Location
        self.required.SetInParent()

    def matches(self):
        return common.pdisk_matches_storage_pool(self.pdisk, self.pool, self.location)

    def test_supplied_fields_and_presence(self):
        self.assertTrue(self.matches())
        for name in ('DataCenter', 'Module', 'Rack', 'Unit', 'BridgePileName'):
            for value in ('value', ''):
                with self.subTest(field=name, value=value):
                    self.required.Clear()
                    self.location.Clear()
                    setattr(self.required, name, value)
                    self.assertFalse(self.matches())
                    setattr(self.location, name, value)
                    self.assertTrue(self.matches())
                    setattr(self.location, name, 'different')
                    self.assertFalse(self.matches())

    def test_conjunction_and_alternative_filters(self):
        self.required.DataCenter = 'dc1'
        self.required.Rack = 'rack1'
        self.location.DataCenter = 'dc1'
        self.location.Rack = 'rack2'
        self.assertFalse(self.matches())
        self.pool.PDiskFilter.add().CopyFrom(self.filter)
        self.pool.PDiskFilter[1].Property[1].Location.Rack = 'rack2'
        self.assertTrue(self.matches())
        self.pdisk.Type = common.EPDiskType.SSD
        self.assertFalse(self.matches())
        self.pdisk.Type = common.EPDiskType.ROT
        self.pdisk.BoxId = 2
        self.assertFalse(self.matches())

    def test_usage_excludes_disks_outside_location(self):
        self.location.DataCenter = 'dc1'
        self.required.DataCenter = 'dc1'
        self.pdisk.NumStaticSlots = 2
        self.config.Node.add(NodeId=2).Location.DataCenter = 'dc2'
        self.config.PDisk.add(NodeId=2, PDiskId=1, BoxId=1, Type=common.EPDiskType.ROT)
        self.assertEqual(common.build_pdisk_usage_map(self.config, storage_pool=self.pool), {(1, 1): 2})
        self.assertEqual(set(common.build_pdisk_usage_map(self.config)), {(1, 1), (2, 1)})
        self.required.DataCenter = 'missing'
        self.assertEqual(common.build_pdisk_usage_map(self.config, storage_pool=self.pool), {})
