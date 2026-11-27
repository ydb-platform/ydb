#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    bool IsRetryable(const EStatus& status) {
        return status == EStatus::OVERLOADED;
    }
}

Y_UNIT_TEST_SUITE(KqpLimitsOutOfSpace) {
    Y_UNIT_TEST(OutOfSpaceBulkUpsertFail) {
        TKikimrRunner kikimr(NFake::TStorage{
            .UseDisk = false,
            .SectorSize = 4096,
            .ChunkSize = 32_MB,
            .DiskSize = 8_GB
        });

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "LargeTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "DataText", Type: "String" }
            KeyColumnNames: ["Key"],
        )");

        auto client = kikimr.GetTableClient();

        const ui32 batchCount = 400;
        const ui32 dataTextSize = 1_MB;
        const ui32 rowsPerBatch = 30;

        auto session = client.CreateSession().GetValueSync().GetSession();

        bool failedToInsert = false;
        ui32 batchIdx = 0;
        ui32 cnt = 0;

        while (batchIdx < batchCount) {
            auto rowsBuilder = TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 0; i < rowsPerBatch; ++i) {
                TString dataText(dataTextSize, 'a' + RandomNumber<ui32>() % ('z' - 'a' + 1));
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(cnt++)
                    .AddMember("DataText")
                        .OptionalString(dataText)
                    .EndStruct();
            }
            rowsBuilder.EndList();

            auto result = client.BulkUpsert("/Root/LargeTable", rowsBuilder.Build()).ExtractValueSync();
            if (IsRetryable(result.GetStatus())) {
                continue;
            }
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAVAILABLE, result.GetIssues().ToString());
                failedToInsert = true;
                break;
            }
            ++batchIdx;
        }
        if (!failedToInsert) {
            UNIT_FAIL("Successfully inserted " << rowsPerBatch << " x " << batchCount << " lines, each of size " << dataTextSize << "bytes");
        }
    }

    Y_UNIT_TEST(OutOfSpaceYQLUpsertFail) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetStorage(NFake::TStorage{
                .UseDisk = false,
                .SectorSize = 4096,
                .ChunkSize = 32_MB,
                .DiskSize = 8_GB
                });
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000'000);

        TKikimrRunner kikimr(settings);

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "LargeTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "DataText", Type: "String" }
            KeyColumnNames: ["Key"],
        )");

        auto client = kikimr.GetTableClient();

        const ui32 batchCount = 400;
        const ui32 dataTextSize = 1_MB;
        const ui32 rowsPerBatch = 30;

        auto session = client.CreateSession().GetValueSync().GetSession();

        ui32 batchIdx = 0;
        ui32 cnt = 0;

        while (batchIdx < batchCount) {
            auto paramsBuilder = client.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            for (ui32 i = 0; i < rowsPerBatch; ++i) {
                TString dataText(dataTextSize, 'a' + RandomNumber<ui32>() % ('z' - 'a' + 1));
                rowsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(cnt++)
                    .AddMember("DataText")
                        .OptionalString(dataText)
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $rows AS List<Struct<Key: Uint64?, DataText: String?>>;

                UPSERT INTO `/Root/LargeTable`
                SELECT * FROM AS_TABLE($rows);
            )"), TTxControl::BeginTx().CommitTx(), paramsBuilder.Build()).ExtractValueSync();

            switch (result.GetStatus()) {
            case EStatus::SUCCESS:
            case EStatus::OVERLOADED:
            case EStatus::UNDETERMINED:
                continue;
            case EStatus::UNAVAILABLE:
                if (result.GetIssues().ToString().contains("out of space")
                    || result.GetIssues().ToString().contains("Disk space exhausted")
                    || result.GetIssues().ToString().contains("Kikimr cluster or one of its subsystems was unavailable.")) {
                    Cerr << "Got out of space. Successfully inserted " << rowsPerBatch << " x " << batchIdx << " lines, each of size " << dataTextSize << "bytes";
                    return;
                } else if (result.GetIssues().ToString().contains("WRONG_SHARD_STATE")
                        || result.GetIssues().ToString().contains("wrong shard state")
                        || result.GetIssues().ToString().contains("can't deliver message to tablet")) {
                    // shards are allowed to split
                    continue;
                }
                UNIT_ASSERT_C(false, "Unexpected UNAVAILABLE status" << result.GetIssues().ToString());
            default:
                UNIT_ASSERT_C(false, "Unexpected status" << result.GetStatus() << result.GetIssues().ToString());
            }

            ++batchIdx;
        }
        UNIT_FAIL("Out of space is expected");
    }
}

} // namespace NKqp
} // namespace NKikimr
