#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/string/strip.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardInfoTypesTest) {

    void VerifyDeduplicateColumnFamiliesById(
            const TString& srcProtoText,
            const TString& dstProtoText,
            const TString& posMapExpected)
    {
        NKikimrSchemeOp::TPartitionConfig config;
        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(srcProtoText, &config);
        UNIT_ASSERT_C(parseOk, "Failed to parse TPartitionConfig:\n" << srcProtoText);

        auto posById = TPartitionConfigMerger::DeduplicateColumnFamiliesById(config);

        TString result;
        ::google::protobuf::TextFormat::PrintToString(config, &result);
        TString expected = StripStringLeft(dstProtoText, [](const auto* it) { return *it == '\n'; });

        UNIT_ASSERT_VALUES_EQUAL(result, expected);

        TVector<ui32> ids;
        for (const auto& kv : posById) {
            ids.push_back(kv.first);
        }
        std::sort(ids.begin(), ids.end());

        TStringBuilder posMapResult;
        posMapResult << '{';
        bool first = true;
        for (ui32 id : ids) {
            if (first) {
                first = false;
            } else {
                posMapResult << ',';
            }
            posMapResult << ' ';
            posMapResult << id << " -> " << posById[id];
        }
        posMapResult << " }";
        UNIT_ASSERT_VALUES_EQUAL(TString(posMapResult), posMapExpected);
    }

    Y_UNIT_TEST(EmptyFamilies) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
}
)",
            R"(
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0 }");
    }

    Y_UNIT_TEST(LostId) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(DeduplicationOrder) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(MultipleDeduplications) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(IndexBuildInfoAddParent) {
        TIndexBuildInfo info;

        info.KMeans.ParentBegin = 1;
        info.KMeans.Parent = 1;
        info.KMeans.ChildBegin = 201;
        info.KMeans.Child = 201;

        TVector<TCell> from = {TCell::Make((ui64)1), TCell::Make(123)};
        TVector<TCell> to = {TCell::Make((ui64)3), TCell::Make(123)};
        auto range = TSerializedTableRange(from, true, to, true);
        info.AddParent(range, TShardIdx(1, 1));

        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(3).From, 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(3).Shards.size(), 1);

        // add an intersecting shard. it even crashed here until 02.07.2025 :)
        from = {TCell::Make((ui64)3), TCell::Make(123)};
        to = {TCell::Make((ui64)4), TCell::Make(123)};
        range = TSerializedTableRange(from, true, to, true);
        info.AddParent(range, TShardIdx(1, 2));

        // Cluster2Shards is a rather stupid thing - for now, it just merges all intersecting
        // shard ranges into a single item, thus losing the range info for individual cluster IDs
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(4).From, 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(4).Shards.size(), 2);

        // add a non-intersecting shard
        from = {TCell::Make((ui64)5), TCell::Make(123)};
        to = {TCell::Make((ui64)6), TCell::Make(123)};
        range = TSerializedTableRange(from, true, to, true);
        info.AddParent(range, TShardIdx(1, 3));

        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(4).From, 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(4).Shards.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(6).From, 5);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(6).Shards.size(), 1);

        // incomplete range is "from (99, +infinity)", thus equal to "from [100]"
        from = {TCell::Make((ui64)99)};
        to = {TCell::Make((ui64)100), TCell::Make(123)};
        range = TSerializedTableRange(from, true, to, true);
        info.AddParent(range, TShardIdx(1, 4));

        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(100).From, 100);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(100).Shards.size(), 1);

        // insert and check something before an existing item
        from = {TCell::Make((ui64)50), TCell::Make(123)};
        to = {TCell::Make((ui64)51), TCell::Make(123)};
        range = TSerializedTableRange(from, true, to, true);
        info.AddParent(range, TShardIdx(1, 5));

        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(51).From, 50);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.at(51).Shards.size(), 1);

    }

}
