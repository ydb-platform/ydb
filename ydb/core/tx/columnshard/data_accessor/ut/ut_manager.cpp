#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/general_cache/usage/service.h>
#include <ydb/core/tx/general_cache/usage/events.h>
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>
#include <ydb/core/tx/columnshard/common/blob.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NDataAccessorControl;
using namespace NKikimr::NOlap::NGeneralCache;
using namespace NKikimr::NTxUT;

namespace {

// Test subscriber to capture results
class TTestSubscriber: public IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<const TAtomicCounter> AbortionFlag;
    TDataAccessorsResult Result;
    bool Finished = false;

    const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return AbortionFlag;
    }

    void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        Result = std::move(result);
        Finished = true;
    }

public:
    TTestSubscriber()
        : AbortionFlag(std::make_shared<TAtomicCounter>(0)) {
    }

    bool IsFinished() const {
        return Finished;
    }

    const TDataAccessorsResult& GetResult() const {
        return Result;
    }
};

// Mock cache service that intercepts TEvAskData and responds with test data
class TMockCacheService: public NActors::TActor<TMockCacheService> {
private:
    using TBase = NActors::TActor<TMockCacheService>;
    const bool WithRemovedAddresses;

public:
    TMockCacheService(bool withRemovedAddresses)
        : TBase(&TMockCacheService::StateWork)
        , WithRemovedAddresses(withRemovedAddresses) {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGeneralCache::NPublic::TEvents<TPortionsMetadataCachePolicy>::TEvAskData, Handle);
            default:
                break;
        }
    }

    void Handle(NKikimr::NGeneralCache::NPublic::TEvents<TPortionsMetadataCachePolicy>::TEvAskData::TPtr& ev, const NActors::TActorContext&) {
        auto callback = ev->Get()->ExtractCallback();
        auto addresses = ev->Get()->ExtractAddresses();

        THashMap<TGlobalPortionAddress, std::shared_ptr<TPortionDataAccessor>> objectAddresses;
        THashSet<TGlobalPortionAddress> removedAddresses;

        if (WithRemovedAddresses) {
            // Move all requested addresses to removedAddresses to simulate removed portions
            for (auto&& addr : addresses) {
                removedAddresses.emplace(std::move(addr));
            }
        }

        THashMap<TGlobalPortionAddress, TString> errors;
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<TPortionsMetadataCachePolicy> errorAddresses(std::move(errors));

        // This calls TActorAccessorsManager::TAdapterCallback::DoOnResultReady
        callback->OnResultReady(std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
    }
};

// Actor that calls TActorAccessorsManager::AskData within actor context
class TTestActor: public NActors::TActorBootstrapped<TTestActor> {
private:
    std::shared_ptr<TActorAccessorsManager> Manager;
    std::shared_ptr<TDataAccessorsRequest> Request;

public:
    TTestActor(std::shared_ptr<TActorAccessorsManager> manager, 
               std::shared_ptr<TDataAccessorsRequest> request)
        : Manager(std::move(manager))
        , Request(std::move(request)) {
    }

    void Bootstrap() {
        Manager->AskData(Request);
        PassAway();
    }
};

// Helper to create a test portion with minimal required fields
TPortionInfo::TConstPtr MakeTestPortion(TInternalPathId pathId, ui64 portionId) {
    // Create a minimal TIndexInfo for testing
    THashMap<ui32, NTable::TColumn> columns = {
        { 0, NTable::TColumn("pk", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "") }
    };
    
    std::vector<ui32> pkIds = { 0 };
    
    for (ui64 i = 0; i < pkIds.size(); ++i) {
        TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
    }

    TIndexInfo indexInfo = TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
    
    // Create a minimal schema with special columns
    const auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector(
        { std::make_shared<arrow::Field>("pk", arrow::uint64()) }));
    
    // Create a batch with special columns
    auto batch = NArrow::MakeEmptyBatch(schema, 1);
    
    // Serialize the batch to bytes for PrimaryKeyBorders
    TString serialized = NArrow::SerializeBatchNoCompression(batch);
    
    // Create the proto meta
    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(0);
    metaProto.MutableRecordSnapshotMin()->SetTxId(0);
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(0);
    metaProto.MutableRecordSnapshotMax()->SetTxId(0);
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(0);
    metaProto.SetRecordsCount(1);
    metaProto.SetColumnRawBytes(100);
    metaProto.SetColumnBlobBytes(100);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(0);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(0);
    
    // Load metadata from proto
    TPortionMetaConstructor metaConstructor;
    TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));
    
    // Use the constructor to set the private fields
    TCompactedPortionInfoConstructor constructor(pathId, portionId);
    constructor.SetSchemaVersion(1);
    constructor.SetAppearanceSnapshot(TSnapshot(0, 0));
    constructor.MutableMeta() = metaConstructor;
    
    return constructor.Build();
}

}  // namespace

Y_UNIT_TEST_SUITE(TActorAccessorsManagerTests) {

    Y_UNIT_TEST(AskDataHandlesNonEmptyRemovedAddresses) {
        // Test that TActorAccessorsManager handles non-empty removedAddresses without crashing
        // This verifies the fix where AFL_VERIFY(removedAddresses.empty()) was removed

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        // Register mock cache service that returns non-empty removedAddresses
        NActors::TActorId mockServiceId = NKikimr::NGeneralCache::TServiceOperator<TPortionsMetadataCachePolicy>::MakeServiceId(runtime.GetNodeId(0));
        runtime.RegisterService(mockServiceId, runtime.Register(new TMockCacheService(true)));

        NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

        auto manager = std::make_shared<TActorAccessorsManager>(tabletActorId);

        auto subscriber = std::make_shared<TTestSubscriber>();
        auto request = std::make_shared<TDataAccessorsRequest>(TPortionsMetadataCachePolicy::DefaultConsumer());
        request->RegisterSubscriber(subscriber);
        
        // Add a test portion so BuildAddresses returns non-empty
        auto testPortion = MakeTestPortion(TInternalPathId::FromRawValue(1), 1);
        request->AddPortion(testPortion);

        // Run test actor that calls AskData within actor context
        runtime.Register(new TTestActor(manager, request));

        // Wait for the response
        TDispatchOptions options;
        options.FinalEvents.emplace_back([&subscriber](IEventHandle&) {
            return subscriber->IsFinished();
        });
        runtime.DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT(subscriber->IsFinished());
        UNIT_ASSERT(subscriber->GetResult().HasRemovedData());
    }

    Y_UNIT_TEST(AskDataHandlesEmptyRemovedAddresses) {
        // Test that TActorAccessorsManager handles empty removedAddresses correctly

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        // Register mock cache service that returns empty removedAddresses
        NActors::TActorId mockServiceId = NKikimr::NGeneralCache::TServiceOperator<TPortionsMetadataCachePolicy>::MakeServiceId(runtime.GetNodeId(0));
        runtime.RegisterService(mockServiceId, runtime.Register(new TMockCacheService(false)));

        NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

        auto manager = std::make_shared<TActorAccessorsManager>(tabletActorId);

        auto subscriber = std::make_shared<TTestSubscriber>();
        auto request = std::make_shared<TDataAccessorsRequest>(TPortionsMetadataCachePolicy::DefaultConsumer());
        request->RegisterSubscriber(subscriber);
        
        // Add a test portion so BuildAddresses returns non-empty
        auto testPortion = MakeTestPortion(TInternalPathId::FromRawValue(1), 1);
        request->AddPortion(testPortion);

        runtime.Register(new TTestActor(manager, request));

        TDispatchOptions options;
        options.FinalEvents.emplace_back([&subscriber](IEventHandle&) {
            return subscriber->IsFinished();
        });
        runtime.DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT(subscriber->IsFinished());
        UNIT_ASSERT(!subscriber->GetResult().HasRemovedData());
    }
}