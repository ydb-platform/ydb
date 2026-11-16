#include "metadata.h"

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
