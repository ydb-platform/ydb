#include "normalize_path.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(NormalizePathTest) {
    Y_UNIT_TEST(TestNormalization) {
        UNIT_ASSERT(NormalizePath("/abc/./d") == "/abc/d");
        UNIT_ASSERT(NormalizePath("/abc/./def//abcd//tre///brr") == "/abc/def/abcd/tre/brr");
        UNIT_ASSERT(NormalizePath("/abc/../def") == "/abc/../def");
        UNIT_ASSERT(NormalizePath("/abc/def//") == "/abc/def");
        UNIT_ASSERT(NormalizePath("/abc/def/.") == "/abc/def");
        UNIT_ASSERT(NormalizePath("/abc/def/./") == "/abc/def");
    }

    TString AdjustPath(const TString& path, const TClientCommand::TConfig& config) {
        TString copyPath(path);
        NYdb::NConsoleClient::AdjustPath(copyPath, config);
        return copyPath;
    }

    TClientCommand::TConfig FakeConfig(const TString& database) {
        TClientCommand::TConfig config(0, nullptr);
        config.Database = database;
        return config;
    }

    Y_UNIT_TEST(TestAdjustment) {
        for (const TString database : {"/Root/mydb", "Root/mydb", "mydb"}) {
            const auto config = FakeConfig(database);
            for (const TString path : {"table", "Root/mydb/table", "Root/Root/mydb/table",
                "Root2/table", "mydb/table", "/Root/mydb/table", "/Root2/table", "."}) {
                UNIT_ASSERT_VALUES_EQUAL_C(AdjustPath(path, config), path, database << ": " << path);
            }
            UNIT_ASSERT_VALUES_EQUAL(AdjustPath("./table", config), "table");
            UNIT_ASSERT_VALUES_EQUAL(AdjustPath("./Root//mydb/./table/", config), "Root/mydb/table");
        }
    }

    Y_UNIT_TEST(TestAdjustmentWithExplicitDirectory) {
        for (const TString database : {"/Root/mydb", "Root/mydb", "mydb"}) {
            auto config = FakeConfig(database);
            config.Path = "current";
            UNIT_ASSERT_VALUES_EQUAL(AdjustPath("./table", config), "current/table");
            UNIT_ASSERT_VALUES_EQUAL(AdjustPath("/Root/mydb/table", config), "/Root/mydb/table");
            config.Path = "/Root/mydb/current";
            UNIT_ASSERT_VALUES_EQUAL(AdjustPath("table", config), "/Root/mydb/current/table");
        }
    }
}
