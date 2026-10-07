import unittest

from ydb.tools.mnc.agent.services.features import FeatureService, FeatureStatus


class FeatureServiceTest(unittest.TestCase):
    def setUp(self):
        self.registry = FeatureService()

    def test_enabled_feature_appears_in_enabled_features(self):
        self.registry.set_feature_status("nodes", FeatureStatus.ENABLED)
        self.assertEqual(self.registry.get_enabled_features(), ["nodes"])

    def test_disabled_feature_is_not_in_enabled_features(self):
        self.registry.set_feature_status("nodes", FeatureStatus.DISABLED)
        self.assertEqual(self.registry.get_enabled_features(), [])

    def test_experimental_feature_is_not_in_enabled_features(self):
        self.registry.set_feature_status("nodes", FeatureStatus.EXPERIMENTAL)
        self.assertEqual(self.registry.get_enabled_features(), [])

    def test_get_all_features_returns_string_values(self):
        self.registry.set_feature_status("nodes", FeatureStatus.ENABLED)
        self.registry.set_feature_status("disks", FeatureStatus.DISABLED)
        result = self.registry.get_all_features()
        self.assertEqual(result, {"nodes": "enabled", "disks": "disabled"})

    def test_unknown_feature_status_is_none(self):
        self.assertIsNone(self.registry.get_feature_status("missing"))

    def test_unknown_feature_is_not_enabled(self):
        self.assertFalse(self.registry.is_feature_enabled("missing"))

    def test_is_feature_enabled_true_only_for_enabled(self):
        self.registry.set_feature_status("nodes", FeatureStatus.ENABLED)
        self.registry.set_feature_status("disks", FeatureStatus.DISABLED)
        self.assertTrue(self.registry.is_feature_enabled("nodes"))
        self.assertFalse(self.registry.is_feature_enabled("disks"))

    def test_set_feature_status_overwrites_previous_value(self):
        self.registry.set_feature_status("nodes", FeatureStatus.ENABLED)
        self.registry.set_feature_status("nodes", FeatureStatus.DISABLED)
        self.assertEqual(self.registry.get_feature_status("nodes"), FeatureStatus.DISABLED)
        self.assertFalse(self.registry.is_feature_enabled("nodes"))
