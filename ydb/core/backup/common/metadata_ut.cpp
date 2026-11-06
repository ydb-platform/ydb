#include "metadata.h"
#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(TableChecksumMetadataTest) {
    Y_UNIT_TEST(RoundTrip) {
        const TTableChecksum checksum{ .Count = Max<ui64>(), .Hash = Max<ui64>() };
        const auto encoded = TMetadata::WithTableChecksum(TMetadata().Serialize(), checksum);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(encoded, &json));
        UNIT_ASSERT(json.Has("table_checksum"));
        UNIT_ASSERT_VALUES_EQUAL(json["table_checksum"]["version"].GetUInteger(), 1);
        const auto decoded = TMetadata::Deserialize(encoded, true);
        UNIT_ASSERT(decoded.GetTableChecksum());
        UNIT_ASSERT(*decoded.GetTableChecksum() == checksum);
        UNIT_ASSERT(TMetadata::Deserialize(decoded.Serialize(), true).GetTableChecksum() == decoded.GetTableChecksum());
        UNIT_ASSERT(!TMetadata::Deserialize(encoded).GetTableChecksum());
    }

    Y_UNIT_TEST(LegacyMetadata) {
        UNIT_ASSERT(!TMetadata::Deserialize(R"({"version":1})", true).GetTableChecksum());
    }

    Y_UNIT_TEST(DisabledIgnoresInvalidAndUnknownChecksums) {
        for (const TString value : { "null", "17", R"({"version":999})", R"({"version":0,"count":"1","hash":"2"})",
                 R"({"version":2,"count":"1","hash":"2"})", R"({"version":1,"count":"-1","hash":"2"})" }) {
            const TString json = TStringBuilder() << "{\"table_checksum\":" << value << "}";
            UNIT_ASSERT(!TMetadata::Deserialize(json).GetTableChecksum());
            UNIT_ASSERT_EXCEPTION(TMetadata::Deserialize(json, true), yexception);
        }
    }

    Y_UNIT_TEST(RejectsInvalidValues) {
        for (const TString value : { "-1", "", "18446744073709551616", "+1", "1.5", "1x" }) {
            const TString json = TStringBuilder()
                                 << R"({"table_checksum":{"version":1,"count":")" << value << R"(","hash":"0"}})";
            UNIT_ASSERT_EXCEPTION(TMetadata::Deserialize(json, true), yexception);
        }
    }

    Y_UNIT_TEST(EnrichmentPreservesAllMetadata) {
        const TString original = R"({"version":1,"full_backups":[{"snapshot_vts":[123,456]}],"extra":"keep"})";
        const auto json = TMetadata::WithTableChecksum(original, { .Count = 0, .Hash = 0 });
        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(json, &value));
        UNIT_ASSERT_VALUES_EQUAL(value["extra"].GetString(), "keep");
        UNIT_ASSERT_VALUES_EQUAL(value["full_backups"][0]["snapshot_vts"][0].GetUInteger(), 123);
        const auto metadata = TMetadata::Deserialize(json, true);
        UNIT_ASSERT(metadata.GetTableChecksum());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Hash, 0);
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
