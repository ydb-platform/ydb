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
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObjectCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->InitialCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(config.Topics->InitialCount, 3);
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
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObject(0).Path, "load/topic_0");
    }

    Y_UNIT_TEST(InitialCountsAndObjectNames) {
        const auto config = ParseConfig("path: load\ntables: {count: 100, initial_count: 2, partitions: 1}\ntopics: {count: 50, initial_count: 1, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObjectCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObject(1).Path, "load/table_1");
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObject(2).Path, "load/topic_0");
        UNIT_ASSERT_EXCEPTION(config.InitialObject(3), yexception);
        const auto zero = ParseConfig("path: load\ntables: {count: 100, initial_count: 0, partitions: 1}\ntopics: {count: 50, initial_count: 0, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(zero.InitialObjectCount(), 0);
        UNIT_ASSERT_EXCEPTION(zero.InitialObject(0), yexception);
        const auto topics = ParseConfig("path: load\ntables: {count: 100, initial_count: 0, partitions: 1}\ntopics: {count: 50, initial_count: 1, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(topics.InitialObject(0).Path, "load/topic_0");
    }

    Y_UNIT_TEST(RejectsInvalidInitialCounts) {
        for (const TString& value : {"-1", "3", "1.5", "true", "4294967296", "null", "[]", "{}", "1e3"}) {
            for (const TString& group : {"tables", "topics"}) {
                UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\n" + group + ": {count: 2, partitions: 1, initial_count: " + value + "}"), std::exception);
            }
        }
        UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ntables: {count: 2, partitions: 1, initial_count: 0, initial_count: 1}"), std::exception);
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

    Y_UNIT_TEST(EnforcesCombinedWorkerLimit) {
        const TString yaml = "path: load\ntables: {count: 500, partitions: 1}\nalter_concurrency: 128\n";
        const auto config = ParseConfig(yaml + "create_concurrency: 128\n");
        UNIT_ASSERT_VALUES_EQUAL(config.CreateConcurrency, 128);
        UNIT_ASSERT_VALUES_EQUAL(config.AlterConcurrency, 128);
        for (const TString& concurrency : {"129", "100000", "4294967295"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseConfig(yaml + "create_concurrency: " + concurrency),
                yexception, "create_concurrency + alter_concurrency must not exceed 256");
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

    Y_UNIT_TEST(SelectsOperationsAndRejectsInvalidMaps) {
        const auto config = ParseConfig("path: load\noperations: {describe: 32, list: 2}\ntables: {count: 5, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(config.CreateConcurrency, 0);
        UNIT_ASSERT_VALUES_EQUAL(config.AlterConcurrency, 0);
        UNIT_ASSERT_VALUES_EQUAL(config.DescribeConcurrency, 32);
        UNIT_ASSERT_VALUES_EQUAL(config.ListConcurrency, 2);
        UNIT_ASSERT_VALUES_EQUAL(config.TotalConcurrency(), 34);
        for (const TString& operations : {"{}", "[]", "null", "{drop: 1}", "{describe: 0}", "{list: -1}",
                "{list: 1, list: 2}", "{create: 100, alter: 100, describe: 56, list: 1}", "{list: 4294967295}"}) {
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\ntables: {count: 1, partitions: 1}\noperations: " + operations), std::exception);
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseConfig("path: load\noperations: {list: 1}\ncreate_concurrency: 1"),
            yexception, "cannot be combined");
        UNIT_ASSERT_EXCEPTION(ParseConfig("path: load\noperations: {describe: 1}"), yexception);
        UNIT_ASSERT_VALUES_EQUAL(ParseConfig("path: load\noperations: {list: 256}").TotalConcurrency(), 256);
    }

    Y_UNIT_TEST(OptionsBuildConfigurationWithoutFile) {
        TConfigOverrides options;
        options.Path = "load";
        options.Tables = "3";
        options.Topics = "2";
        options.Operations = "create=4,alter=2,describe=8,list=1";
        const auto config = ParseConfig("{}", options);
        UNIT_ASSERT_VALUES_EQUAL(config.ObjectCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObjectCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->Partitions, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.Topics->Partitions, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->AlterBatchSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(config.TotalConcurrency(), 15);
        UNIT_ASSERT_VALUES_EQUAL(config.DescribeConcurrency, 8);
    }

    Y_UNIT_TEST(OptionsOverrideOnlyExplicitFieldsAndReplaceOperations) {
        const TString yaml = "path: load\ncreate_concurrency: 16\nalter_concurrency: 8\n"
            "tables: {count: 10, initial_count: 2, partitions: 4, alter_batch_size: 3}\n"
            "topics: {count: 5, partitions: 8}";
        TConfigOverrides options;
        options.Path = "other";
        options.Tables = "7";
        options.Operations = "describe=32";
        auto config = ParseConfig(yaml, options);
        UNIT_ASSERT_VALUES_EQUAL(config.Path, "other");
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->Count, 7);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->InitialCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->Partitions, 4);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->AlterBatchSize, 3);
        UNIT_ASSERT_VALUES_EQUAL(config.Topics->Count, 5);
        UNIT_ASSERT_VALUES_EQUAL(config.Topics->Partitions, 8);
        UNIT_ASSERT_VALUES_EQUAL(config.TotalConcurrency(), 32);
        UNIT_ASSERT_VALUES_EQUAL(config.CreateConcurrency + config.AlterConcurrency + config.ListConcurrency, 0);
        options.Partitions = "16";
        config = ParseConfig(yaml, options);
        UNIT_ASSERT_VALUES_EQUAL(config.Tables->Partitions, 16);
        UNIT_ASSERT_VALUES_EQUAL(config.Topics->Partitions, 16);
        options.Tables = "1";
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseConfig(yaml, options), yexception, "initial_count must not exceed count");
        options = {};
        options.Operations = "list=2";
        config = ParseConfig("path: load\noperations: {describe: 5}", options);
        UNIT_ASSERT_VALUES_EQUAL(config.TotalConcurrency(), 2);
        UNIT_ASSERT_VALUES_EQUAL(config.DescribeConcurrency, 0);
    }

    Y_UNIT_TEST(RejectsInvalidOptions) {
        TConfigOverrides options;
        for (const TString& operations : {"", "list", "list=", "=1", "list=1,", "list=1,list=2", "drop=1", "list=0", "list=1=2", "list=-1"}) {
            options.Operations = operations;
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load", options), std::exception);
        }
        options = {};
        options.Partitions = "2";
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseConfig("path: load\noperations: {list: 1}", options), yexception, "requires tables or topics");
        for (const TString& count : {"0", "-1", "4294967296", "foo"}) {
            options.Tables = count;
            UNIT_ASSERT_EXCEPTION(ParseConfig("path: load", options), std::exception);
        }
    }

    Y_UNIT_TEST(ObjectCountDoesNotOverflow) {
        const auto config = ParseConfig("path: load\ntables: {count: 4294967295, partitions: 1}\ntopics: {count: 1, partitions: 1}");
        UNIT_ASSERT_VALUES_EQUAL(config.ObjectCount(), 4294967296ULL);
        UNIT_ASSERT_VALUES_EQUAL(config.InitialObjectCount(), 4294967296ULL);
        UNIT_ASSERT_VALUES_EQUAL(config.Object(4294967295ULL).Path, "load/topic_0");
    }
}
