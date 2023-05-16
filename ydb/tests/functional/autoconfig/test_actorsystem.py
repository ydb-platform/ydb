#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys


from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


def make_test_for_specific_actor_system(node_type, cpu_count):
    class TestWithSpecificActorSystem(object):
        @classmethod
        def setup_class(cls):
            actor_system_config = {
                "node_type": node_type,
                "cpu_count": cpu_count,
                "use_auto_config": True
            }
            configuration = KikimrConfigGenerator(overrided_actor_system_config=actor_system_config)
            cls.kikimr_cluster = kikimr_cluster_factory(configuration)
            cls.kikimr_cluster.start()

        @classmethod
        def teardown_class(cls):
            cls.kikimr_cluster.stop()

        def test(self):
            pass

    return TestWithSpecificActorSystem


for node_type in ("Compute", "Storage", "Hybrid"):
    for cpu_count in range(1, 40):
        test = make_test_for_specific_actor_system(node_type.upper(), cpu_count)
        setattr(sys.modules[__name__], "TestWith%sNodeWith%dCpu" % (node_type, cpu_count), test)
