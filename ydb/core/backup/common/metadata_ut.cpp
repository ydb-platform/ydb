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

    Y_UNIT_TEST(OptionalIV) {
        const auto metadata = TMetadata::Deserialize(R"({"indexes": [
            {"export_prefix": "001", "impl_table_prefix": "encrypted/indexImplTable", "iv": "000102030405060708090A0B"},
            {"export_prefix": "plain/indexImplTable", "impl_table_prefix": "plain/indexImplTable"}
        ]})");

        for (const auto& value : {metadata, TMetadata::Deserialize(metadata.Serialize())}) {
            UNIT_ASSERT(value.GetIndexes());
            const auto& indexes = *value.GetIndexes();
            UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(indexes[0].ExportPrefix, "001");
            UNIT_ASSERT_VALUES_EQUAL(indexes[0].ImplTablePrefix, "encrypted/indexImplTable");
            UNIT_ASSERT(indexes[0].IV);
            UNIT_ASSERT(*indexes[0].IV == TEncryptionIV::FromHexString("000102030405060708090A0B"));
            UNIT_ASSERT_VALUES_EQUAL(indexes[1].ExportPrefix, "plain/indexImplTable");
            UNIT_ASSERT(!indexes[1].IV);
        }
    }

    Y_UNIT_TEST(InvalidIV) {
        // Reject malformed IVs before they reach Combine(), which asserts on their size.
        for (const TString iv : {"null", "123", "\"\"", "\"00\"", "\"000102030405060708090A0B0C\"", "\"GG0102030405060708090A0B\""}) {
            UNIT_ASSERT_EXCEPTION(TMetadata::Deserialize(
                TString(R"({"indexes": [{"export_prefix": "001", "impl_table_prefix": "index/indexImplTable", "iv":)")
                    + iv + "}]}"), yexception);
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
