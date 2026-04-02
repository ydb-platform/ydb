#include "schema.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(StaticSchemaTests) {

ISchema::TPtr MakeStaticSchemaUT() {
    THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> fs = {
        {"", {{"/", {{.Type = "Folder", .Name = "local"},
                     {.Type = "Folder", .Name = "test"},
                     {.Type = "Folder", .Name = "prod"}}},
              {"/local/", {{.Type = "Table", .Name = "example"},
                           {.Type = "Table", .Name = "account"},
                           {.Type = "Table", .Name = "abacaba"}}},
              {"/test/", {{.Type = "Folder", .Name = "service"},
                          {.Type = "Table", .Name = "meta"}}},
              {"/test/service/", {{.Type = "Table", .Name = "example"}}}}},
    };
    return MakeSimpleSchema(MakeStaticSimpleSchema({.Folders = std::move(fs)}));
}

Y_UNIT_TEST(ListFolderBasic) {
    auto schema = MakeStaticSchemaUT();
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Folder", .Name = "local"},
            {.Type = "Folder", .Name = "test"},
            {.Type = "Folder", .Name = "prod"},
        };
        UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/"}).GetValueSync().Entries, expected);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Table", .Name = "example"},
            {.Type = "Table", .Name = "account"},
            {.Type = "Table", .Name = "abacaba"},
        };
        UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/local/"}).GetValueSync().Entries, expected);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Folder", .Name = "service"},
            {.Type = "Table", .Name = "meta"},
        };
        UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/test/"}).GetValueSync().Entries, expected);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Table", .Name = "example"}};
        UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/test/service/"}).GetValueSync().Entries, expected);
    }
}

Y_UNIT_TEST(ListFolderHint) {
    auto schema = MakeStaticSchemaUT();
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Folder", .Name = "local"},
        };
        auto actual = schema->List({.Path = "/l"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 1);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Table", .Name = "account"},
            {.Type = "Table", .Name = "abacaba"},
        };
        auto actual = schema->List({.Path = "/local/a"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 1);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Folder", .Name = "service"},
        };
        auto actual = schema->List({.Path = "/test/service"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Entries, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.NameHintLength, 7);
    }
}

Y_UNIT_TEST(ListFolderFilterByType) {
    auto schema = MakeStaticSchemaUT();
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Folder", .Name = "service"},
        };
        TListRequest request = {
            .Path = "/test/",
            .Filter = {
                .Types = THashSet<TString>{"Folder"},
            },
        };
        UNIT_ASSERT_VALUES_EQUAL(schema->List(request).GetValueSync().Entries, expected);
    }
    {
        TVector<TFolderEntry> expected = {
            {.Type = "Table", .Name = "meta"},
        };
        TListRequest request = {
            .Path = "/test/",
            .Filter = {
                .Types = THashSet<TString>{"Table"},
            },
        };
        UNIT_ASSERT_VALUES_EQUAL(schema->List(request).GetValueSync().Entries, expected);
    }
}

Y_UNIT_TEST(ListFolderLimit) {
    auto schema = MakeStaticSchemaUT();
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 0}).GetValueSync().Entries.size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 1}).GetValueSync().Entries.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 2}).GetValueSync().Entries.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 3}).GetValueSync().Entries.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 4}).GetValueSync().Entries.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(schema->List({.Path = "/", .Limit = 5}).GetValueSync().Entries.size(), 3);
}

} // Y_UNIT_TEST_SUITE(StaticSchemaTests)
