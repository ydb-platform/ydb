#include "index_metadata.h"
#include "metadata.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(IndexMetadataTest) {
    Y_UNIT_TEST(SchemaMappingIndexIVs) {
        const TString json = R"({"exportedObjects": {
            "Table": {
                "exportPrefix": "001",
                "iv": "000102030405060708090A0B",
                "indexIVs": {"index/indexImplTable": "010102030405060708090A0B"}
            }
        }})";
        TSchemaMapping mapping;
        TString error;
        UNIT_ASSERT_C(mapping.Deserialize(json, error), error);
        TSchemaMapping roundTrip;
        UNIT_ASSERT_C(roundTrip.Deserialize(mapping.Serialize(), error), error);
        UNIT_ASSERT_VALUES_EQUAL(roundTrip.Items.size(), 1);
        const auto& item = roundTrip.Items.front();
        UNIT_ASSERT_VALUES_EQUAL(item.ObjectPath, "Table");
        UNIT_ASSERT_VALUES_EQUAL(item.ExportPrefix, "001");
        UNIT_ASSERT(item.IV);
        UNIT_ASSERT(*item.IV == TEncryptionIV::FromHexString("000102030405060708090A0B"));
        UNIT_ASSERT_VALUES_EQUAL(item.IndexIVs.size(), 1);
        UNIT_ASSERT(item.IndexIVs.at("index/indexImplTable") == TEncryptionIV::FromHexString("010102030405060708090A0B"));
    }

    Y_UNIT_TEST(LegacySchemaMappingWithoutIndexIVs) {
        for (const TString extra : {"", ",\"iv\":\"000102030405060708090A0B\""}) {
            TSchemaMapping mapping;
            TString error;
            UNIT_ASSERT_C(mapping.Deserialize(
                TString(R"({"exportedObjects":{"Table":{"exportPrefix":"001")") + extra + "}}}", error), error);
            UNIT_ASSERT_VALUES_EQUAL(mapping.Items.size(), 1);
            UNIT_ASSERT(mapping.Items.front().IndexIVs.empty());
        }
    }

    Y_UNIT_TEST(InvalidSchemaMappingIndexIV) {
        for (const TString iv : {"null", "123", "\"\"", "\"00\"", "\"000102030405060708090A0B0C\"", "\"GG0102030405060708090A0B\""}) {
            TSchemaMapping mapping;
            TString error;
            UNIT_ASSERT(!mapping.Deserialize(TString(R"({"exportedObjects":{"Table":{
                "exportPrefix":"001", "iv":"000102030405060708090A0B",
                "indexIVs":{"index/indexImplTable":)") + iv + "}}}}", error));
            UNIT_ASSERT_C(!error.empty(), iv);
        }
    }

    Y_UNIT_TEST(IndexIVIsNotSerialized) {
        // Authoritative index IVs live in the authenticated schema mapping only
        TMetadata metadata;
        metadata.AddIndex({
            .ExportPrefix = "001",
            .ImplTablePrefix = "index/indexImplTable",
            .IV = TEncryptionIV::FromHexString("000102030405060708090A0B"),
        });
        const TString serialized = metadata.Serialize();
        UNIT_ASSERT_C(!serialized.Contains("\"iv\""), serialized);

        const auto deserialized = TMetadata::Deserialize(serialized);
        UNIT_ASSERT(deserialized.GetIndexes());
        UNIT_ASSERT_VALUES_EQUAL(deserialized.GetIndexes()->size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(deserialized.GetIndexes()->front().ExportPrefix, "001");
        UNIT_ASSERT_VALUES_EQUAL(deserialized.GetIndexes()->front().ImplTablePrefix, "index/indexImplTable");
        UNIT_ASSERT(!deserialized.GetIndexes()->front().IV);
    }

    Y_UNIT_TEST(MakeIndexesMetadata) {
        google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex> indexes;
        {
            // Async index data is not exported and must not consume a prefix number.
            auto& index = *indexes.Add();
            index.set_name("async");
            index.add_index_columns("value");
            index.mutable_global_async_index();
        }
        {
            auto& index = *indexes.Add();
            index.set_name("global");
            index.add_index_columns("value");
            index.mutable_global_index();
        }
        {
            // Local indexes have no impl tables and must not consume a prefix number
            auto& index = *indexes.Add();
            index.set_name("bloom");
            index.add_index_columns("key");
            index.mutable_local_bloom_filter_index();
        }
        {
            auto& index = *indexes.Add();
            index.set_name("vector");
            index.add_index_columns("embedding");
            index.mutable_global_vector_kmeans_tree_index();
        }

        const auto encrypted = NBackup::MakeIndexesMetadata(indexes, 3, true, false);
        UNIT_ASSERT_VALUES_EQUAL(encrypted.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(encrypted[0].ImplTablePrefix, "global/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(encrypted[0].ExportPrefix, "003");
        UNIT_ASSERT_VALUES_EQUAL(encrypted[1].ImplTablePrefix, "vector/indexImplLevelTable");
        UNIT_ASSERT_VALUES_EQUAL(encrypted[1].ExportPrefix, "004");
        UNIT_ASSERT_VALUES_EQUAL(encrypted[2].ImplTablePrefix, "vector/indexImplPostingTable");
        UNIT_ASSERT_VALUES_EQUAL(encrypted[2].ExportPrefix, "005");

        const auto plain = NBackup::MakeIndexesMetadata(indexes, 3, false, false);
        UNIT_ASSERT_VALUES_EQUAL(plain.size(), 3);
        for (const auto& index : plain) {
            UNIT_ASSERT_VALUES_EQUAL(index.ExportPrefix, index.ImplTablePrefix);
        }
    }
}

Y_UNIT_TEST_SUITE(BackupMetadataTest) {
    Y_UNIT_TEST(SourceDatabase) {
        TMetadata metadata;
        UNIT_ASSERT(TMetadata::Deserialize(metadata.Serialize()).GetSourceDatabase().empty());
        UNIT_ASSERT(TMetadata::Deserialize(R"({"version": 1})").GetSourceDatabase().empty());
        metadata.SetSourceDatabase("/Root/tenant/database");
        UNIT_ASSERT_VALUES_EQUAL(TMetadata::Deserialize(metadata.Serialize()).GetSourceDatabase(), "/Root/tenant/database");
    }
}

Y_UNIT_TEST_SUITE(PathsNormalizationTest) {
    Y_UNIT_TEST(NormalizeItemPath) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/a/b/c/"), "a/b/c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("///a///b///"), "a/b");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("a/b/c"), "a/b/c");
    }

    Y_UNIT_TEST(NormalizeItemPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("///a///b///c///"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("a///b///c"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix(""), "");
    }

    Y_UNIT_TEST(NormalizeExportPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("///a///"), "///a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a/"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix//"), "/prefix");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix"), "/prefix");
    }
}

Y_UNIT_TEST_SUITE(RateLimiterMetadataTest) {
    Y_UNIT_TEST(AbsentList) {
        const auto metadata = TMetadata::Deserialize("{}");
        UNIT_ASSERT(!metadata.GetRateLimiterResources());
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(metadata.Serialize(), &json));
        UNIT_ASSERT(!json.Has("rate_limiter_resources"));
    }

    Y_UNIT_TEST(EmptyListReplacesNonemptyList) {
        TMetadata metadata;
        metadata.SetRateLimiterResources({{.ExportPrefix = "rate_limiter_resources/001", .ResourcePath = "A"}});
        metadata.SetRateLimiterResources({});
        const TString serialized = metadata.Serialize();
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(serialized, &json));
        UNIT_ASSERT(json["rate_limiter_resources"].IsArray());
        UNIT_ASSERT(json["rate_limiter_resources"].GetArraySafe().empty());
        const auto parsed = TMetadata::Deserialize(serialized);
        UNIT_ASSERT(parsed.GetRateLimiterResources());
        UNIT_ASSERT(parsed.GetRateLimiterResources()->empty());
    }

    Y_UNIT_TEST(ResourcePathsAndOrderRoundTrip) {
        TMetadata metadata;
        metadata.SetVersion(1);
        metadata.SetEnablePermissions();
        metadata.AddChangefeed({.ExportPrefix = "changefeed", .Name = "feed"});
        metadata.AddIndex({.ExportPrefix = "index", .ImplTablePrefix = "indexImplTable"});
        metadata.SetRateLimiterResources({
            {.ExportPrefix = "rate_limiter_resources/001", .ResourcePath = "parent"},
            {.ExportPrefix = "rate_limiter_resources/002", .ResourcePath = "parent/child"},
        });
        const auto parsed = TMetadata::Deserialize(metadata.Serialize());
        UNIT_ASSERT(parsed.GetRateLimiterResources());
        const auto& resources = *parsed.GetRateLimiterResources();
        UNIT_ASSERT_VALUES_EQUAL(resources.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resources[0].ExportPrefix, "rate_limiter_resources/001");
        UNIT_ASSERT_VALUES_EQUAL(resources[0].ResourcePath, "parent");
        UNIT_ASSERT_VALUES_EQUAL(resources[1].ExportPrefix, "rate_limiter_resources/002");
        UNIT_ASSERT_VALUES_EQUAL(resources[1].ResourcePath, "parent/child");
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetVersion(), 1);
        UNIT_ASSERT(parsed.GetEnablePermissions());
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetChangefeeds()->front().Name, "feed");
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetIndexes()->front().ImplTablePrefix, "indexImplTable");
    }
}

Y_UNIT_TEST_SUITE(SemaphoreMetadataTest) {
    Y_UNIT_TEST(AbsentAndEmptyListsDiffer) {
        TMetadata metadata;
        UNIT_ASSERT(!TMetadata::Deserialize(metadata.Serialize()).GetSemaphores());
        metadata.SetSemaphores({{.ExportPrefix = "semaphores/001", .Name = "../name"}});
        metadata.SetSemaphores({});
        const auto parsed = TMetadata::Deserialize(metadata.Serialize());
        UNIT_ASSERT(parsed.GetSemaphores());
        UNIT_ASSERT(parsed.GetSemaphores()->empty());
    }

    Y_UNIT_TEST(NamesAndPrefixesRoundTrip) {
        TMetadata metadata;
        metadata.SetRateLimiterResources({{.ExportPrefix = "rate_limiter_resources/001", .ResourcePath = "resource"}});
        metadata.SetSemaphores({
            {.ExportPrefix = "semaphores/001", .Name = "../name"},
            {.ExportPrefix = "semaphores/002", .Name = "name/with/slashes"},
        });
        const auto parsed = TMetadata::Deserialize(metadata.Serialize());
        UNIT_ASSERT(parsed.GetSemaphores());
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetSemaphores()->size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetSemaphores()->at(0).Name, "../name");
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetSemaphores()->at(0).ExportPrefix, "semaphores/001");
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetSemaphores()->at(1).Name, "name/with/slashes");
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetRateLimiterResources()->at(0).ResourcePath, "resource");
    }
}

} // namespace NKikimr::NBackup
