#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

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

        auto addShard = [&](ui64 fromCluster, ui64 toCluster, ui32 shard, bool partialFrom = false) {
            TVector<TCell> from = {TCell::Make(fromCluster), TCell::Make(123)};
            if (partialFrom) {
                from.pop_back();
            }
            TVector<TCell> to = {TCell::Make(toCluster), TCell::Make(123)};
            auto range = TSerializedTableRange(from, true, to, true);
            info.AddParent(range, TShardIdx(1, shard));
        };
        auto checkShards = [&](ui64 from, ui64 to, std::vector<ui32> shards) {
            auto rng = info.Cluster2Shards.at(to);
            if (rng.From != from || rng.Shards.size() != shards.size()) {
                return false;
            }
            for (size_t i = 0; i < shards.size(); i++) {
                if (rng.Shards[i] != TShardIdx(1, shards[i])) {
                    return false;
                }
            }
            return true;
        };

        // no intersection + empty list
        addShard(1, 3, 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 1);
        UNIT_ASSERT(checkShards(1, 3, {1}));

        // add an intersecting shard. it even crashed here until 02.07.2025 :)
        // intersection by start + both larger than 1
        addShard(3, 4, 2);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 3);
        UNIT_ASSERT(checkShards(1, 2, {1}));
        UNIT_ASSERT(checkShards(3, 3, {1, 2}));
        UNIT_ASSERT(checkShards(4, 4, {2}));

        // intersection by start + both 1 (duplicate range)
        // incomplete range is "from (3, +infinity)", thus equal to "from 4"
        addShard(3, 4, 3, true);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 3);
        UNIT_ASSERT(checkShards(4, 4, {2, 3}));

        // intersection by start + old 1 + new larger than 1
        addShard(4, 6, 4);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 4);
        UNIT_ASSERT(checkShards(4, 4, {2, 3, 4}));
        UNIT_ASSERT(checkShards(5, 6, {4}));

        // intersection by start + old larger than 1 + new 1
        addShard(6, 6, 5);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 5);
        UNIT_ASSERT(checkShards(5, 5, {4}));
        UNIT_ASSERT(checkShards(6, 6, {4, 5}));

        // no intersection + after non-empty list
        addShard(19, 20, 6);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 6);
        UNIT_ASSERT(checkShards(19, 20, {6}));

        // intersection by end + both larger than 1
        addShard(18, 19, 7);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 8);
        UNIT_ASSERT(checkShards(18, 18, {7}));
        UNIT_ASSERT(checkShards(19, 19, {6, 7}));
        UNIT_ASSERT(checkShards(20, 20, {6}));

        // intersection by end + both 1 (duplicate range)
        addShard(18, 18, 8);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 8);
        UNIT_ASSERT(checkShards(18, 18, {7, 8}));

        // intersection by end + old 1 + new larger than 1
        addShard(16, 18, 9);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 9);
        UNIT_ASSERT(checkShards(16, 17, {9}));
        UNIT_ASSERT(checkShards(18, 18, {7, 8, 9}));

        // intersection by end + old larger than 1 + new 1
        addShard(16, 16, 10);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 10);
        UNIT_ASSERT(checkShards(16, 16, {9, 10}));
        UNIT_ASSERT(checkShards(17, 17, {9}));

        // intersection by both
        addShard(7, 9, 11);
        addShard(13, 15, 12);
        addShard(9, 13, 13);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 15);
        UNIT_ASSERT(checkShards(7, 8, {11}));
        UNIT_ASSERT(checkShards(9, 9, {11, 13}));
        UNIT_ASSERT(checkShards(10, 12, {13}));
        UNIT_ASSERT(checkShards(13, 13, {12, 13}));
        UNIT_ASSERT(checkShards(14, 15, {12}));

    }

}
