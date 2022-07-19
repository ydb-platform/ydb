#include "path_list_reader.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

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

    Y_UNIT_TEST(ReadsFilesListFromSourceSettings) {
        NS3::TSource src;
        {
            auto* p = src.AddDeprecatedPath();
            p->SetPath("my/path");
            p->SetSize(100500);
        }
        {
            auto* p = src.AddDeprecatedPath();
            p->SetPath("other/path");
            p->SetSize(1);
        }

        TPathList paths;
        ui64 startPathIndex = 42;
        ReadPathsList(src, {}, paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(startPathIndex, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "my/path");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 100500);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[1]), "other/path");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[1]), 1);
    }

    Y_UNIT_TEST(ReadsFilesListFromParamsAndSourceSettings) {
        NS3::TSource src;
        {
            auto* p = src.AddDeprecatedPath();
            p->SetPath("my/path");
            p->SetSize(100500);
        }
        {
            auto* p = src.AddDeprecatedPath();
            p->SetPath("other/path");
            p->SetSize(1);
        }

        NS3::TRange range;
        range.SetStartPathIndex(42);
        range.AddDeprecatedPath("my/path");

        TPathList paths;
        ui64 startPathIndex = 0;
        ReadPathsList(src, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(startPathIndex, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "my/path");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 100500);
    }

    NYql::NS3::TRange::TPath* SetPath(NYql::NS3::TRange::TPath* path, const TString& name = {}, ui64 size = 0, bool read = false) {
        path->SetName(name);
        path->SetSize(size);
        path->SetRead(read);
        return path;
    }

    Y_UNIT_TEST(ReadsFilesListFromTreeParams) {
        NS3::TSource src;
        {
            auto* p = src.AddDeprecatedPath();
            p->SetPath("my/path");
            p->SetSize(100500);
        }

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
        ui64 startPathIndex = 0;
        ReadPathsList(src, MakeParams(range), paths, startPathIndex);

        UNIT_ASSERT_VALUES_EQUAL(startPathIndex, 42);
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 5);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[0]), "root");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[0]), 1);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[1]), "root/folder/f1");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[1]), 42);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[2]), "root/folder/f2");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[2]), 100500);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[3]), "root/f3");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[3]), 0);

        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(paths[4]), "root2/f4");
        UNIT_ASSERT_VALUES_EQUAL(std::get<ui64>(paths[4]), 42);
    }
}

} // namespace NYql::NS3Details
