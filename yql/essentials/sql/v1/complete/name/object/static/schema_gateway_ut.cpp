#include "schema_gateway.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(StaticSchemaGatewayTests) {

    ISchemaGateway::TPtr MakeStaticSchemaGatewayUT() {
        THashMap<TString, TVector<TFolderEntry>> fs = {
            {"/", {{"Folder", "local"},
                   {"Folder", "test"},
                   {"Folder", "prod"}}},
            {"/local/", {{"Table", "example"},
                         {"Table", "account"},
                         {"Table", "abacaba"}}},
            {"/test/", {{"Folder", "service"},
                        {"Table", "meta"}}},
            {"/test/service/", {{"Table", "example"}}},
        };
        return MakeStaticSchemaGateway(std::move(fs));
    }

    Y_UNIT_TEST(ListFolderBasic) {
        auto gateway = MakeStaticSchemaGatewayUT();
        {
            TVector<TFolderEntry> expected = {
                {"Folder", "local"},
                {"Folder", "test"},
                {"Folder", "prod"},
            };
            UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/"}).GetValueSync().Entries, expected);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Table", "example"},
                {"Table", "account"},
                {"Table", "abacaba"},
            };
            UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/local/"}).GetValueSync().Entries, expected);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Folder", "service"},
                {"Table", "meta"},
            };
            UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/test/"}).GetValueSync().Entries, expected);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Table", "example"}};
            UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/test/service/"}).GetValueSync().Entries, expected);
        }
    }

    Y_UNIT_TEST(ListFolderHint) {
        auto gateway = MakeStaticSchemaGatewayUT();
        {
            TVector<TFolderEntry> expected = {
                {"Folder", "local"},
            };
            auto actual = gateway->List({.Path = "/l"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 1);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Table", "account"},
                {"Table", "abacaba"},
            };
            auto actual = gateway->List({.Path = "/local/a"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 1);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Folder", "service"},
            };
            auto actual = gateway->List({.Path = "/test/service"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 7);
        }
    }

    Y_UNIT_TEST(ListFolderFilterByType) {
        auto gateway = MakeStaticSchemaGatewayUT();
        {
            TVector<TFolderEntry> expected = {
                {"Folder", "service"},
            };
            TListRequest request = {
                .Path = "/test/",
                .Filter = {
                    .Types = THashSet<TString>{"Folder"},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(gateway->List(request).GetValueSync().Entries, expected);
        }
        {
            TVector<TFolderEntry> expected = {
                {"Table", "meta"},
            };
            TListRequest request = {
                .Path = "/test/",
                .Filter = {
                    .Types = THashSet<TString>{"Table"},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(gateway->List(request).GetValueSync().Entries, expected);
        }
    }

    Y_UNIT_TEST(ListFolderLimit) {
        auto gateway = MakeStaticSchemaGatewayUT();
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 0}).GetValueSync().Entries.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 1}).GetValueSync().Entries.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 2}).GetValueSync().Entries.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 3}).GetValueSync().Entries.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 4}).GetValueSync().Entries.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(gateway->List({.Path = "/", .Limit = 5}).GetValueSync().Entries.size(), 3);
    }

} // Y_UNIT_TEST_SUITE(StaticSchemaGatewayTests)
