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
        b.AddPath("name", 42, false);

        NS3::TRange range;
        b.Save(&range);

        UNIT_ASSERT_VALUES_EQUAL(range.PathsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).ChildrenSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetIsDirectory(), false);
    }

    Y_UNIT_TEST(Interesting) {
        TFileTreeBuilder b;
        b.AddPath("name", 42, false);
        b.AddPath("root/folder/file", 100500, false);
        b.AddPath("root2/file", 10, false);
        b.AddPath("root2", 42, false);
        b.AddPath("root/folder/other_file", 22, false);
        b.AddPath("root/file/", 0, true);

        NS3::TRange range;
        b.Save(&range);

        UNIT_ASSERT_VALUES_EQUAL(range.PathsSize(), 4);

        // name
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).ChildrenSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(0).GetIsDirectory(), false);

        // root/
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetName(), "root");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetIsDirectory(), true);

        // root/file/
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).GetIsDirectory(), true);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(0).ChildrenSize(), 0);

        // root/folder/
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetName(), "folder");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetIsDirectory(), true);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).ChildrenSize(), 2);

        // root/folder/file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).GetSize(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).GetIsDirectory(), false);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(0).ChildrenSize(), 0);

        // root/folder/other_file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).GetName(), "other_file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).GetSize(), 22);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).GetIsDirectory(), false);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(1).GetChildren(1).GetChildren(1).ChildrenSize(), 0);

        // root2
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetName(), "root2");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetSize(), 42);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).GetIsDirectory(), false);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(2).ChildrenSize(), 0);

        // root2/
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetName(), "root2");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetIsDirectory(), true);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).ChildrenSize(), 1);

        // root2/file
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetChildren(0).GetName(), "file");
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetChildren(0).GetSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetChildren(0).GetIsDirectory(), false);
        UNIT_ASSERT_VALUES_EQUAL(range.GetPaths(3).GetChildren(0).ChildrenSize(), 0);
    }

    Y_UNIT_TEST(PassesFileWithZeroSize) {
        TFileTreeBuilder b;
        b.AddPath("name", 0, false);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ReadPathsList({}, MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "name");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 0);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 0);
    }

    Y_UNIT_TEST(DeserializesManySlashes) {
        TFileTreeBuilder b;
        b.AddPath("a///b", 42, false);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ReadPathsList({}, MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "a///b");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 0);
    }

    Y_UNIT_TEST(DeserializesTrailingSlash) {
        TFileTreeBuilder b;
        b.AddPath("root/name//", 3, true);
        b.AddPath("root/name/", 0, true);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ReadPathsList({}, MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "root/name/");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 0);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 0);

        UNIT_ASSERT_VALUES_EQUAL(paths[1].Path, "root/name//");
        UNIT_ASSERT_VALUES_EQUAL(paths[1].Size, 3);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].PathIndex, 1);
    }

    Y_UNIT_TEST(DeserializesLeadingSlash) {
        TFileTreeBuilder b;
        b.AddPath("/root/name", 3, false);
        b.AddPath("/", 42, true);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ReadPathsList({}, MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "/");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 0);

        UNIT_ASSERT_VALUES_EQUAL(paths[1].Path, "/root/name");
        UNIT_ASSERT_VALUES_EQUAL(paths[1].Size, 3);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].PathIndex, 1);
    }

    Y_UNIT_TEST(DeserializesRootSlash) {
        TFileTreeBuilder b;
        b.AddPath("/root/name/", 3, true);
        b.AddPath("", 42, true);
        b.AddPath("//", 42, true);

        NS3::TRange range;
        b.Save(&range);

        TPathList paths;
        ReadPathsList({}, MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "//");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 0);

        UNIT_ASSERT_VALUES_EQUAL(paths[1].Path, "/root/name/");
        UNIT_ASSERT_VALUES_EQUAL(paths[1].Size, 3);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].PathIndex, 1);

        UNIT_ASSERT_VALUES_EQUAL(paths[2].Path, "");
        UNIT_ASSERT_VALUES_EQUAL(paths[2].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[2].IsDirectory, true);
        UNIT_ASSERT_VALUES_EQUAL(paths[2].PathIndex, 2);
    }
}

} // namespace NYql::NS3Details