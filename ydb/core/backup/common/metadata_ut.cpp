#include "index_metadata.h"
#include "metadata.h"

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

} // namespace NKikimr::NBackup
