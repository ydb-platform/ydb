#include <ydb/library/workload/scheme/config.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdbWorkload::NScheme;

Y_UNIT_TEST_SUITE(SchemeWorkloadConfig) {
    Y_UNIT_TEST(MixedConfigurationAndPaths) {
        const auto config = ParseConfig(R"(
path: workload/nested
create_concurrency: 7
alter_concurrency: 3
tables: {count: 2, partitions: 64, alter_batch_size: 10}
topics: {count: 3, partitions: 16, alter_batch_size: 4}
)");
        UNIT_ASSERT_VALUES_EQUAL(config.CreateConcurrency, 7);
        UNIT_ASSERT_VALUES_EQUAL(config.AlterConcurrency, 3);
        UNIT_ASSERT_VALUES_EQUAL(config.ObjectCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(1).Path, "workload/nested/table_1");
        UNIT_ASSERT_VALUES_EQUAL(config.Object(2).Path, "workload/nested/topic_0");
        UNIT_ASSERT_VALUES_EQUAL(config.Object(4).Path, "workload/nested/topic_2");
        UNIT_ASSERT_VALUES_EQUAL(config.Object(1).AlterBatchSize, 10);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(2).Partitions, 16);
        UNIT_ASSERT_EXCEPTION(config.Object(5), yexception);
    }

    Y_UNIT_TEST(SingleGroupAndDefaults) {
        const auto config = ParseConfig("path: load\ntopics: {count: 1, partitions: 2}\n");
        UNIT_ASSERT(!config.Tables);
        UNIT_ASSERT_VALUES_EQUAL(config.CreateConcurrency, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.AlterConcurrency, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(0).AlterBatchSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(0).Path, "load/topic_0");
    }

    Y_UNIT_TEST(RejectsInvalidPaths) {
        for (const TString& path : {"", "/root", "a/", "a//b", ".", "..", "a/../b", "a/./b", "a`b", "a b"}) {
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: '" + path + "'\ntables: {count: 1, partitions: 1}"), std::exception);
        }
    }

    Y_UNIT_TEST(RejectsInvalidNumbers) {
        for (const TString& value : {"0", "-1", "1.5", "true", "4294967296", "null", "[]", "{}", "1e3"}) {
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ntables: {count: " + value + ", partitions: 1}"), std::exception);
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ntopics: {count: 1, partitions: " + value + "}"), std::exception);
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ncreate_concurrency: " + value + "\ntables: {count: 1, partitions: 1}"), std::exception);
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\nalter_concurrency: " + value + "\ntables: {count: 1, partitions: 1}"), std::exception);
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ntables: {count: 1, partitions: 1, alter_batch_size: " + value + "}"), std::exception);
        }
    }

    Y_UNIT_TEST(RejectsUnknownMissingDuplicateAndMultipleDocuments) {
        for (const TString& yaml : {
            "path: load",
            "tables: {count: 1, partitions: 1}",
            "path: load\ntables: {partitions: 1}",
            "path: load\ntopics: {count: 1}",
            "path: load\ntables: {count: 1, partitions: 1, typo: 2}",
            "path: load\ntables: {count: 1, partitions: 1}\nconcurrency: 2",
            "path: load\npath: other\ntables: {count: 1, partitions: 1}",
            "path: load\ntables: {count: 1, count: 2, partitions: 1}",
            "path: load\ntables: []",
            "path: load\ntables: {count: 1, partitions: 1}\n---\n{}",
        }) {
            UNIT_ASSERT_EXCEPTION(ParseConfig(yaml), std::exception);
        }
    }

    Y_UNIT_TEST(ObjectCountDoesNotOverflow) {
        const auto config = ParseConfig("path: load\ntables: {count: 4294967295, partitions: 1}\ntopics: {count: 1, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(config.ObjectCount(), 4294967296ULL);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(4294967295ULL).Path, "load/topic_0");
    }
}
