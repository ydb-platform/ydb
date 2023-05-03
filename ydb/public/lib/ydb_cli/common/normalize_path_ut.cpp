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
        UNIT_ASSERT(AdjustPath("abc", FakeConfig("/root/db")) == "/root/db/abc");
        UNIT_ASSERT(AdjustPath("./abc", FakeConfig("/root/db")) == "/root/db/abc");
        UNIT_ASSERT(AdjustPath("/root/db/abc", FakeConfig("/root/db")) == "/root/db/abc");

        UNIT_ASSERT_EXCEPTION(AdjustPath("/abc", FakeConfig("/root/db")), TMisuseException);
        UNIT_ASSERT_EXCEPTION(AdjustPath("/root/bd/abc", FakeConfig("/root/db")), TMisuseException);
    }
}

