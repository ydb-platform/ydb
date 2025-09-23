#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/string/strip.h>

#include <source_location>

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

    Y_UNIT_TEST(FillItemsFromSchemaMappingTest) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);

        const TPathId domainPathId = TPath::Resolve("/MyRoot", schemeshard).Base()->PathId;

        auto getImportInfo = [&](const TString& settingsProto, const TString& schemaMapping, bool expectSuccess = true, const TString& expectedError = {}) -> TImportInfo::TPtr {
            Ydb::Import::ImportFromS3Settings settings;
            UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(settingsProto, &settings));

            TImportInfo::TPtr importInfo = new TImportInfo(42, "uid_42", TImportInfo::EKind::S3, settings, domainPathId, "localhost");

            for (const auto& item : settings.items()) {
                auto& importInfoItem = importInfo->Items.emplace_back(item.destination_path());
                importInfoItem.SrcPath = item.source_path();
                importInfoItem.SrcPrefix = item.source_prefix();
            }

            importInfo->SchemaMapping.ConstructInPlace();
            TString error;
            UNIT_ASSERT_C(importInfo->SchemaMapping->Deserialize(schemaMapping, error), error);

            auto fillResult = runtime.RunCall(
                [&]() {
                    return importInfo->FillItemsFromSchemaMapping(schemeshard);
                }
            );
            if (expectSuccess) {
                UNIT_ASSERT_C(fillResult.Success, fillResult.ErrorMessage);
            } else {
                UNIT_ASSERT(!fillResult.Success);
                UNIT_ASSERT_STRING_CONTAINS(fillResult.ErrorMessage, expectedError);
            }

            std::sort(importInfo->Items.begin(), importInfo->Items.end(), [](const TImportInfo::TItem& item1, const TImportInfo::TItem& item2) { return item1.SrcPath < item2.SrcPath; });
            return importInfo;
        };

        auto validateImportItem = [](const TImportInfo::TItem& item, const TString& dstPath, const TString& srcPrefix, const TString& srcPath, TMaybe<NBackup::TEncryptionIV> iv = Nothing(), const std::source_location location = std::source_location::current()) {
            UNIT_ASSERT_VALUES_EQUAL_C(item.DstPathName, dstPath, location.file_name() << ':' << location.line());
            UNIT_ASSERT_VALUES_EQUAL_C(item.SrcPrefix, srcPrefix, location.file_name() << ':' << location.line());
            UNIT_ASSERT_VALUES_EQUAL_C(item.SrcPath, srcPath, location.file_name() << ':' << location.line());
            UNIT_ASSERT_EQUAL_C(item.ExportItemIV, iv, location.file_name() << ':' << location.line());
        };

        const TString settingsWithoutFilter = "";

        const TString settingsWithDestinationPath = R"proto(
            destination_path: "/MyRoot/restored//"
        )proto";

        const TString settingsWithFilterByPath = R"proto(
            items {
                source_path: "dir1/Table1"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPath = R"proto(
            items {
                source_path: "dir1"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPathWithDestinationPath = R"proto(
            destination_path: "/MyRoot/dst"
            items {
                source_path: "dir2"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPathAndDestination = R"proto(
            items {
                source_path: "dir1"
                destination_path: "/MyRoot/dir1dst"
            }
        )proto";

        const TString settingsWithExplicitParams = R"proto(
            items {
                source_prefix: "prefix1"
                destination_path: "/MyRoot/d1"
            }
            items {
                source_prefix: "prefix/prefix3"
                destination_path: "/MyRoot/d3"
            }
        )proto";

        const TString settingsWithExplicitParamsTwoDestinations = R"proto(
            destination_path: "/MyRoot/common_dest"
            items {
                source_prefix: "prefix1"
                destination_path: "d1"
            }
            items {
                source_prefix: "prefix/prefix3"
                destination_path: "/MyRoot/d3"
            }
        )proto";

        const TString settingsWithInvalidFilterByPath = R"proto(
            items {
                source_path: "invalid"
            }
            items {
                source_path: "dir1"
            }
        )proto";

        const TString settingsWithInvalidFilterByPrefix = R"proto(
            items {
                source_prefix: "invalid"
            }
            items {
                source_path: "dir1"
            }
        )proto";

        const TString emptySchemaMapping = R"json(
        {
            "exportedObjects": {}
        }
        )json";

        const TString schemaMapping = R"json(
        {
            "exportedObjects": {
                "dir1/Table1": {
                    "exportPrefix": "prefix1"
                },
                "dir2/Table2": {
                    "exportPrefix": "prefix2"
                },
                "dir1/dir2/Table3": {
                    "exportPrefix": "prefix/prefix3"
                }
            }
        }
        )json";

        const TString schemaMappingWithIVs = R"json(
        {
            "exportedObjects": {
                "dir1/Table1": {
                    "exportPrefix": "prefix1",
                    "iv": "1234567890ABCDEF98765432"
                },
                "dir2/Table2": {
                    "exportPrefix": "prefix2",
                    "iv": "1234567890ABCDEF98765432"
                },
                "dir1/dir2/Table3": {
                    "exportPrefix": "prefix/prefix3",
                    "iv": "1234567890ABCDEF98765432"
                }
            }
        }
        )json";

        const NBackup::TEncryptionIV iv = NBackup::TEncryptionIV::FromHexString("1234567890ABCDEF98765432");

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
            validateImportItem(importInfo->Items[2], "/MyRoot/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithDestinationPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/restored/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/restored/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
            validateImportItem(importInfo->Items[2], "/MyRoot/restored/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithFilterByPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 1);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPathWithDestinationPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 1);
            validateImportItem(importInfo->Items[0], "/MyRoot/dst/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPathAndDestination, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1dst/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1dst/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithExplicitParams, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/d1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/d3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithExplicitParamsTwoDestinations, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/common_dest/d1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/d3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, schemaMappingWithIVs);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1", iv);
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3", iv);
            validateImportItem(importInfo->Items[2], "/MyRoot/dir2/Table2", "prefix2", "dir2/Table2", iv);
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPath, schemaMappingWithIVs);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1", iv);
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3", iv);
        }

        {
            auto importInfo = getImportInfo(settingsWithInvalidFilterByPath, schemaMapping, false, "cannot find source path \"invalid\" in schema mapping");
        }

        {
            auto importInfo = getImportInfo(settingsWithInvalidFilterByPrefix, schemaMapping, false, "cannot find prefix \"invalid\" in schema mapping");
        }

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, emptySchemaMapping, false, "no items to import");
        }
    }

}
