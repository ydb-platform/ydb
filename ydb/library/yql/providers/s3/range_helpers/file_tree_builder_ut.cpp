#include "file_tree_builder.h"
#include "path_list_reader.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NS3Details {

Y_UNIT_TEST_SUITE(S3FileTreeBuilderTest) {
    THashMap<TString, TString> MakeParams(const NS3::TRange& range) {
        TStringBuilder str;
        range.Save(&str.Out);
        THashMap<TString, TString> map;
        map[S3ProviderName] = str;
        return map;
    }

    Y_UNIT_TEST(Simple) {
        TFileTreeBuilder b;
        b.AddPath("name", 42);

        NS3::TRange range;
        b.Save(&range);

        UNIT_ASSERT_VALUES_EQUAL(range.PathsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).ChildrenSize(), 0);
    }

    Y_UNIT_TEST(Interesting) {
        TFileTreeBuilder b;
        b.AddPath("name", 42);
        b.AddPath("root/folder/file", 100500);
        b.AddPath("root2/file", 10);
        b.AddPath("root2", 42);
        b.AddPath("root/folder/other_file", 22);
        b.AddPath("root/file/", 12);

        NS3::TRange range;
        b.Save(&range);

        UNIT_ASSERT_VALUES_EQUAL(range.PathsSize(), 3);

        // name
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).ChildrenSize(), 0);

        // root
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetName(), "root");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).ChildrenSize(), 2);

        // root/file/
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetChildren(0).GetName(), "");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetChildren(0).GetSize(), 12);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetChildren(0).ChildrenSize(), 0);

        // root/folder
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetName(), "folder");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).ChildrenSize(), 2);

        // root/folder/file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).GetSize(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).ChildrenSize(), 0);

        // root/folder/other_file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).GetName(), "other_file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).GetSize(), 22);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).ChildrenSize(), 0);

        // root2
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetName(), "root2");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).ChildrenSize(), 1);

        // root2/file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetChildren(0).GetSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetChildren(0).ChildrenSize(), 0);
    }

    Y_UNIT_TEST(PassesFileWithZeroSize) {
        TFileTreeBuilder b;
        b.AddPath("name", 0);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ui64 startPathIndex = 0;
        ReadPathsList({}, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "name");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 0);
    }

    Y_UNIT_TEST(DeserializesManySlashes) {
        TFileTreeBuilder b;
        b.AddPath("a///b", 42);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ui64 startPathIndex = 0;
        ReadPathsList({}, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "a///b");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 42);
    }

    Y_UNIT_TEST(DeserializesTrailingSlash) {
        TFileTreeBuilder b;
        b.AddPath("root/name//", 3);
        b.AddPath("root/name/", 0);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ui64 startPathIndex = 0;
        ReadPathsList({}, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "root/name/");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 0);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[1]), "root/name//");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[1]), 3);
    }

    Y_UNIT_TEST(DeserializesLeadingSlash) {
        TFileTreeBuilder b;
        b.AddPath("/root/name", 3);
        b.AddPath("/", 42);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ui64 startPathIndex = 0;
        ReadPathsList({}, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "/");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 42);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[1]), "/root/name");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[1]), 3);
    }
}

} // namespace NYql::NS3Details
