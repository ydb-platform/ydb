#include <ydb/public/lib/idx_test/idx_test.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NKikimr::NKqp;
using namespace NIdxTest;

struct TRunSettings {
    const bool PkOverlap;
    const bool IndexOverlap;
    const bool WithDataColumn;
    const bool UniqIndex;
};

static const TString TABLE_PATH = "Root/TestIdx";

static void RunTest(ui32 shardsCount, ui32 rowsCount, ui32 indexCount, const TRunSettings& settings) {
    bool pkOverlap = settings.PkOverlap;
    bool indexOverlap = settings.IndexOverlap;
    bool withDataColumn = settings.WithDataColumn;
    bool uniqIndex = settings.UniqIndex;

    TKikimrRunner kikimr(SyntaxV1Settings());

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint()));

    auto uploader = CreateUploader(driver, TABLE_PATH, TUploaderParams{shardsCount});

    static const TString keyColumnName = "key";
    auto builder = TTableBuilder()
        .AddNullableColumn(keyColumnName, EPrimitiveType::Uint64);

    TVector<TString> pks;
    pks.push_back(keyColumnName);

    TVector<TString> dataColumn;
    if (withDataColumn) {
        dataColumn.push_back("value");
    }

    for (ui32 i = 0; i < indexCount; i++) {
        TStringStream ss;
        ss << "index_" << i;
        if (uniqIndex && i == 0) {
            builder.AddNullableColumn(ss.Str(), EPrimitiveType::Int8);
        } else {
            builder.AddNullableColumn(ss.Str(), EPrimitiveType::Utf8);
        }


        if (!pkOverlap) {
            if (uniqIndex) {
                builder.AddUniqueSecondaryIndex(ss.Str() + "_name", TVector<TString>{ss.Str()}, dataColumn);
            } else {
                builder.AddSecondaryIndex(ss.Str() + "_name", TVector<TString>{ss.Str()}, dataColumn);
            }
        } else {
            if (uniqIndex) {
                builder.AddUniqueSecondaryIndex(ss.Str() + "_name", TVector<TString>{ss.Str(), keyColumnName}, dataColumn);
            } else {
                builder.AddSecondaryIndex(ss.Str() + "_name", TVector<TString>{ss.Str(), keyColumnName}, dataColumn);
            }
        }
        if (indexOverlap) {
            pks.push_back(ss.Str());
        }
    }
    builder.AddNullableColumn("value", EPrimitiveType::Uint32);

    builder.SetPrimaryKeyColumns(pks);
    uploader->Run(CreateDataProvider(rowsCount, shardsCount, builder.Build()));
    auto workLoader = CreateWorkLoader(driver);
    ui32 stms =
        IWorkLoader::LC_UPDATE |
        IWorkLoader::LC_UPSERT |
        IWorkLoader::LC_REPLACE |
        IWorkLoader::LC_INSERT |
        IWorkLoader::LC_UPDATE_ON |
        IWorkLoader::LC_DELETE_ON |
        IWorkLoader::LC_DELETE;
    workLoader->Run(TABLE_PATH, stms, IWorkLoader::TRunSettings{rowsCount, 5, 1});
    auto checker = CreateChecker(driver);
    checker->Run(TABLE_PATH);
    driver.Stop(true);
}

Y_UNIT_TEST_SUITE(YdbIndexTable) {
    Y_UNIT_TEST(MultiShardTableOneIndex) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 1, TRunSettings {
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = false,
            .UniqIndex = false
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneUniqIndex) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 2000, 1, TRunSettings {
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = false,
            .UniqIndex = true
        }));
    }

    Y_UNIT_TEST(MultiShardTableUniqAndNonUniqIndex) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 2000, 2, TRunSettings {
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = false,
            .UniqIndex = true
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneIndexDataColumn) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 1, TRunSettings{
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = true,
            .UniqIndex = false
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneUniqIndexDataColumn) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 2000, 1, TRunSettings {
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = true,
            .UniqIndex = true
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneIndexIndexOverlap) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 1, TRunSettings{
            .PkOverlap = false,
            .IndexOverlap = true,
            .WithDataColumn = false,
            .UniqIndex = false
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneIndexIndexOverlapDataColumn) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 1, TRunSettings{
            .PkOverlap = false,
            .IndexOverlap = true,
            .WithDataColumn = true,
            .UniqIndex = false
        }));
    }

    Y_UNIT_TEST(MultiShardTableOneIndexPkOverlap) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 1, TRunSettings{
            .PkOverlap = true,
            .IndexOverlap = false,
            .WithDataColumn = false,
            .UniqIndex = false
        }));
    }

    Y_UNIT_TEST(MultiShardTableTwoIndexes) {
        UNIT_ASSERT_NO_EXCEPTION(RunTest(10, 1000, 2, TRunSettings{
            .PkOverlap = false,
            .IndexOverlap = false,
            .WithDataColumn = false,
            .UniqIndex = false
        }));
    }

    void RunOnlineBuildTest(bool withDataColumn) {
        TKikimrRunner kikimr(SyntaxV1Settings());

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint()));

        auto uploader = CreateUploader(driver, TABLE_PATH, TUploaderParams{1});

        static const TString keyColumnName = "key";
        auto builder = TTableBuilder()
            .AddNullableColumn(keyColumnName, EPrimitiveType::Uint64)
            .AddNullableColumn("value", EPrimitiveType::Utf8)
            .AddSecondaryIndex("value", "value")
            .SetPrimaryKeyColumns({keyColumnName});

        if (withDataColumn) {
            builder.AddNullableColumn("payload", EPrimitiveType::Double);
        }

        try {
            uploader->Run(CreateDataProvider(5000, 1, builder.Build()));

            auto workLoader = CreateWorkLoader(driver);
            ui32 stms =
                IWorkLoader::LC_UPSERT |
                (withDataColumn ? IWorkLoader::LC_ALTER_ADD_INDEX_WITH_DATA_COLUMN : IWorkLoader::LC_ALTER_ADD_INDEX);
            workLoader->Run(TABLE_PATH, stms, IWorkLoader::TRunSettings{2000, 1, 1});
            auto checker = CreateChecker(driver);
            checker->Run(TABLE_PATH);
            driver.Stop(true);
        } catch (const std::exception& ex) {
            Cerr << ex.what() << Endl;
            Y_ABORT("test failed with exception");
        }
    }

    Y_UNIT_TEST(OnlineBuild) {
        RunOnlineBuildTest(false);
    }

    Y_UNIT_TEST(OnlineBuildWithDataColumn) {
        RunOnlineBuildTest(true);
    }
}
