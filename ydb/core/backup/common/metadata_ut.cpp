#include "metadata.h"
#include <util/string/builder.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(BackupMetadataTest) {
    Y_UNIT_TEST(SourceDatabase) {
        TMetadata metadata;
        UNIT_ASSERT(TMetadata::Deserialize(metadata.Serialize()).GetSourceDatabase().empty());
        UNIT_ASSERT(TMetadata::Deserialize(R"({"version": 1})").GetSourceDatabase().empty());
        metadata.SetSourceDatabase("/Root/tenant/database");
        UNIT_ASSERT_VALUES_EQUAL(TMetadata::Deserialize(metadata.Serialize()).GetSourceDatabase(), "/Root/tenant/database");
    }
}

Y_UNIT_TEST_SUITE(TableChecksumMetadataTest) {
    Y_UNIT_TEST(RoundTrip) {
        const TTableChecksum checksum{ .Count = Max<ui64>(), .Hash = Max<ui64>() };
        TMetadata metadata;
        metadata.SetRateLimiterResources(
            { { .ExportPrefix = "rate_limiter_resources/001", .ResourcePath = "resource" } });
        metadata.SetSemaphores({ { .ExportPrefix = "semaphores/001", .Name = "semaphore" } });
        const auto encoded = TMetadata::WithTableChecksum(metadata.Serialize(), checksum);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(encoded, &json));
        UNIT_ASSERT(json.Has("table_checksum"));
        UNIT_ASSERT_VALUES_EQUAL(json["table_checksum"]["version"].GetUInteger(), 1);
        const auto decoded = TMetadata::Deserialize(encoded, true);
        UNIT_ASSERT(decoded.GetTableChecksum());
        UNIT_ASSERT(*decoded.GetTableChecksum() == checksum);
        const auto roundTrip = TMetadata::Deserialize(decoded.Serialize(), true);
        UNIT_ASSERT(roundTrip.GetTableChecksum() == decoded.GetTableChecksum());
        UNIT_ASSERT(roundTrip.GetRateLimiterResources().has_value());
        UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetRateLimiterResources()->front().ResourcePath, "resource");
        UNIT_ASSERT(roundTrip.GetSemaphores().has_value());
        UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetSemaphores()->front().Name, "semaphore");
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
        }
    }

    Y_UNIT_TEST(UnsupportedVersionsAreIgnored) {
        for (const TString value : { R"({"version":0,"count":"1","hash":"2"})", R"({"version":999})",
                 R"({"version":2,"future_layout":[1,2,3]})", R"({"version":18446744073709551615})" }) {
            const TString json = TStringBuilder()
                                 << R"({"version":1,"permissions":1,"table_checksum":)" << value << "}";
            const auto metadata = TMetadata::Deserialize(json, true);
            UNIT_ASSERT(!metadata.GetTableChecksum().has_value());
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetVersion(), 1);
            UNIT_ASSERT(metadata.GetEnablePermissions());
            UNIT_ASSERT(!TMetadata::Deserialize(metadata.Serialize(), true).GetTableChecksum().has_value());
        }
    }

    Y_UNIT_TEST(RejectsMalformedChecksums) {
        for (const TString value : { "null", "17", "{}", R"({"version":"1"})", R"({"version":-1})",
                 R"({"version":1.5})", R"({"version":1})" }) {
            const TString json = TStringBuilder() << "{\"table_checksum\":" << value << "}";
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
