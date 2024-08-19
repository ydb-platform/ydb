#include "path_list_reader.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NYql::NS3Details {

Y_UNIT_TEST_SUITE(PathListReaderTest) {
    THashMap<TString, TString> MakeParams(const NS3::TRange& range) {
        TStringBuilder str;
        range.Save(&str.Out);
        THashMap<TString, TString> map;
        map[S3ProviderName] = str;
        return map;
    }

    NYql::NS3::TRange::TPath* SetPath(NYql::NS3::TRange::TPath* path, const TString& name = {}, ui64 size = 0, bool read = false) {
        path->SetName(name);
        path->SetSize(size);
        path->SetRead(read);
        return path;
    }

    Y_UNIT_TEST(ReadsFilesListFromTreeParams) {
        NS3::TSource src;

        NS3::TRange range;
        range.SetStartPathIndex(42);
        range.AddDeprecatedPath("my/path"); // We shouldn't react on this

        {
            auto* root = SetPath(range.AddPaths(), "root", 1, true);
            {
                auto* folder = SetPath(root->AddChildren(), "folder");
                SetPath(folder->AddChildren(), "f1", 42, true);
                SetPath(folder->AddChildren(), "f2", 100500, true);
            }
            SetPath(root->AddChildren(), "f3", 0, true);
            SetPath(root->AddChildren(), "nothing"); // Shouldn't be processed.
        }
        {
            auto* root2 = SetPath(range.AddPaths(), "root2");
            SetPath(root2->AddChildren(), "f4", 42, true);
        }

        TPathList paths;
        ReadPathsList(MakeParams(range), {}, paths);

        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 5);

        UNIT_ASSERT_VALUES_EQUAL(paths[0].Path, "root");
        UNIT_ASSERT_VALUES_EQUAL(paths[0].Size, 1);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[0].PathIndex, 42);

        UNIT_ASSERT_VALUES_EQUAL(paths[1].Path, "root/folder/f1");
        UNIT_ASSERT_VALUES_EQUAL(paths[1].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[1].PathIndex, 43);

        UNIT_ASSERT_VALUES_EQUAL(paths[2].Path, "root/folder/f2");
        UNIT_ASSERT_VALUES_EQUAL(paths[2].Size, 100500);
        UNIT_ASSERT_VALUES_EQUAL(paths[2].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[2].PathIndex, 44);

        UNIT_ASSERT_VALUES_EQUAL(paths[3].Path, "root/f3");
        UNIT_ASSERT_VALUES_EQUAL(paths[3].Size, 0);
        UNIT_ASSERT_VALUES_EQUAL(paths[3].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[3].PathIndex, 45);

        UNIT_ASSERT_VALUES_EQUAL(paths[4].Path, "root2/f4");
        UNIT_ASSERT_VALUES_EQUAL(paths[4].Size, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths[4].IsDirectory, false);
        UNIT_ASSERT_VALUES_EQUAL(paths[4].PathIndex, 46);
    }
}

} // namespace NYql::NS3Details
