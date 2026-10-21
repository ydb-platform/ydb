#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

namespace {

constexpr ui64 MB = 1ull << 20;

enum EEvSkipLevelTest {
    EvExecuteSkipLevelTest = 1,
    EvSkipLevelTestExecuted,
};

struct TEvExecuteSkipLevelTest: NActors::TEventLocal<TEvExecuteSkipLevelTest, EvExecuteSkipLevelTest> {
    std::function<void()> Action;
};

struct TEvSkipLevelTestExecuted: NActors::TEventLocal<TEvSkipLevelTestExecuted, EvSkipLevelTestExecuted> {};

class TSkipLevelTestExecutor: public NActors::TActor<TSkipLevelTestExecutor> {
private:
    const NActors::TActorId ReplyTo;

    void Handle(TEvExecuteSkipLevelTest::TPtr ev) {
        ev->Get()->Action();
        Send(ReplyTo, new TEvSkipLevelTestExecuted());
        PassAway();
    }

public:
    explicit TSkipLevelTestExecutor(const NActors::TActorId replyTo)
        : TActor(&TThis::StateWork)
        , ReplyTo(replyTo)
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecuteSkipLevelTest, Handle);
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
        }
    }
};

void RunInActorContext(const std::function<void()>& action) {
    NActors::TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());

    const NActors::TActorId edge = runtime.AllocateEdgeActor();
    const NActors::TActorId executor = runtime.Register(new TSkipLevelTestExecutor(edge));

    auto* request = new TEvExecuteSkipLevelTest();
    request->Action = action;
    runtime.Send(new IEventHandle(executor, edge, request));

    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvSkipLevelTestExecuted>(handle);
}

std::shared_ptr<TPortionInfo> MakeTestPortionWithBlobBytes(const ui64 portionId, const ui64 blobBytes, const ui32 compactionLevel = 0) {
    const TInternalPathId pathId = TInternalPathId::FromRawValue(1);
    const TSnapshot snapshot(1, 1);
    TString serialized = NArrow::SerializeBatchNoCompression(NTest::MakePortionTestPKBatch(0, 100));
    TIndexInfo indexInfo = NTest::MakePortionTestIndexInfo();

    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(snapshot.GetPlanStep());
    metaProto.MutableRecordSnapshotMin()->SetTxId(snapshot.GetTxId());
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(snapshot.GetPlanStep());
    metaProto.MutableRecordSnapshotMax()->SetTxId(snapshot.GetTxId());
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(compactionLevel);
    metaProto.SetRecordsCount(100);
    metaProto.SetColumnRawBytes(blobBytes);
    metaProto.SetColumnBlobBytes(blobBytes);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(snapshot.GetPlanStep());
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(snapshot.GetTxId());

    TPortionMetaConstructor metaConstructor;
    TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));

    TCompactedPortionInfoConstructor constructor(pathId, portionId);
    constructor.SetSchemaVersion(1);
    constructor.SetAppearanceSnapshot(snapshot);
    constructor.MutableMeta() = metaConstructor;

    return constructor.Build();
}

NJson::TJsonValue MakePlannerJson(const std::optional<ui64> l0Skip, const std::optional<ui64> l1Skip) {
    auto makeZeroLevel = [](const std::optional<ui64> skip) {
        NJson::TJsonValue level;
        level["class_name"] = "Zero";
        level["expected_blobs_size"] = MB;
        level["portions_count_available"] = 100;
        level["portions_size_limit"] = 1ull << 40;
        level["portions_count_limit"] = 1000000;
        if (skip) {
            level["skip_level_min_blob_size"] = *skip;
        }
        return level;
    };

    NJson::TJsonValue json;
    auto& levels = json["levels"];
    levels.AppendValue(makeZeroLevel(l0Skip));
    levels.AppendValue(makeZeroLevel(l1Skip));

    NJson::TJsonValue oneLayer;
    oneLayer["class_name"] = "OneLayer";
    oneLayer["expected_portion_size"] = MB;
    oneLayer["size_limit_guarantee"] = 1ull << 40;
    oneLayer["bytes_limit_fraction"] = 1.0;
    levels.AppendValue(oneLayer);
    return json;
}

std::shared_ptr<IOptimizerPlanner> BuildPlannerFromJson(const NJson::TJsonValue& json) {
    auto ctor = IOptimizerPlannerConstructor::BuildDefault("lc-buckets");
    UNIT_ASSERT_C(ctor->DeserializeFromJson(json).IsSuccess(), "deserialize lc-buckets json");

    const TInternalPathId pathId = TInternalPathId::FromRawValue(1);
    const auto pkSchema = arrow::schema({ arrow::field("pk", arrow::uint64()) });
    IOptimizerPlannerConstructor::TBuildContext ctx(pathId, TTestStoragesManager::GetInstance(), pkSchema);

    const auto plannerConclusion = ctor->BuildPlanner(ctx);
    UNIT_ASSERT_C(plannerConclusion.IsSuccess(), plannerConclusion.GetErrorMessage());
    return plannerConclusion.GetResult();
}

ui32 AddPortionAndGetLevel(const std::shared_ptr<IOptimizerPlanner>& planner, const ui64 portionId, const ui64 blobBytes) {
    const auto portion = MakeTestPortionWithBlobBytes(portionId, blobBytes);
    planner->ModifyPortions({ portion }, {});
    return portion->GetCompactionLevel();
}

ui32 RunAddPortionTest(const NJson::TJsonValue& json, const ui64 portionId, const ui64 blobBytes) {
    ui32 level = 0;
    RunInActorContext([&] {
        const auto planner = BuildPlannerFromJson(json);
        level = AddPortionAndGetLevel(planner, portionId, blobBytes);
    });
    return level;
}

}   // namespace

Y_UNIT_TEST_SUITE(LCBucketsSkipLevelMinBlobSize) {
    Y_UNIT_TEST(SkipLevelMinBlobSizeNotConfigured) {
        UNIT_ASSERT_VALUES_EQUAL(RunAddPortionTest(MakePlannerJson(std::nullopt, std::nullopt), 1, 5 * MB), 0);
    }

    Y_UNIT_TEST(SkipLevelMinBlobSizePromotesToNextLevel) {
        UNIT_ASSERT_VALUES_EQUAL(RunAddPortionTest(MakePlannerJson(4 * MB, std::nullopt), 1, 5 * MB), 1);
    }

    Y_UNIT_TEST(SkipLevelMinBlobSizeBelowThreshold) {
        UNIT_ASSERT_VALUES_EQUAL(RunAddPortionTest(MakePlannerJson(4 * MB, std::nullopt), 1, 3 * MB), 0);
    }

    Y_UNIT_TEST(SkipLevelMinBlobSizeCascade) {
        ui32 firstLevel = 0;
        ui32 secondLevel = 0;
        const auto json = MakePlannerJson(4 * MB, 8 * MB);
        RunInActorContext([&] {
            const auto planner = BuildPlannerFromJson(json);
            firstLevel = AddPortionAndGetLevel(planner, 1, 10 * MB);
            secondLevel = AddPortionAndGetLevel(planner, 2, 5 * MB);
        });
        UNIT_ASSERT_VALUES_EQUAL(firstLevel, 2);
        UNIT_ASSERT_VALUES_EQUAL(secondLevel, 1);
    }

    Y_UNIT_TEST(SkipLevelMinBlobSizeJsonRoundTrip) {
        auto ctor = IOptimizerPlannerConstructor::BuildDefault("lc-buckets");
        const auto json = MakePlannerJson(4 * MB, 8 * MB);
        UNIT_ASSERT_C(ctor->DeserializeFromJson(json).IsSuccess(), "deserialize");

        NKikimrSchemeOp::TCompactionPlannerConstructorContainer proto;
        ctor->SerializeToProto(proto);
        UNIT_ASSERT(proto.HasLCBuckets());
        UNIT_ASSERT_VALUES_EQUAL(proto.GetLCBuckets().LevelsSize(), 3);
        UNIT_ASSERT(proto.GetLCBuckets().GetLevels(0).HasZeroLevel());
        UNIT_ASSERT(proto.GetLCBuckets().GetLevels(1).HasZeroLevel());
        UNIT_ASSERT_VALUES_EQUAL(proto.GetLCBuckets().GetLevels(0).GetZeroLevel().GetSkipLevelMinBlobSize(), 4 * MB);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetLCBuckets().GetLevels(1).GetZeroLevel().GetSkipLevelMinBlobSize(), 8 * MB);
        UNIT_ASSERT(!proto.GetLCBuckets().GetLevels(2).GetZeroLevel().HasSkipLevelMinBlobSize());
    }

    Y_UNIT_TEST(SkipLevelMinBlobSizeRejectsInvalidJson) {
        auto ctor = IOptimizerPlannerConstructor::BuildDefault("lc-buckets");
        NJson::TJsonValue json = MakePlannerJson(4 * MB, std::nullopt);
        json["levels"][0]["skip_level_min_blob_size"] = "not-a-number";
        UNIT_ASSERT(ctor->DeserializeFromJson(json).IsFail());
    }
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
