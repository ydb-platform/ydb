#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/adapter.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/object_key.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/read.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/wrappers/unavailable_storage.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/event.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

namespace {

class TTestRuntime: public TTestActorRuntimeBase {
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }

public:
    using TTestActorRuntimeBase::TTestActorRuntimeBase;
};

class TNotifyingUnavailableStorage: public NWrappers::NExternalStorage::TUnavailableExternalStorageOperator {
    TManualEvent& Dispatched;

public:
    explicit TNotifyingUnavailableStorage(TManualEvent& dispatched)
        : TUnavailableExternalStorageOperator("ServiceUnavailable", "not ready")
        , Dispatched(dispatched)
    {
    }

    void Execute(NWrappers::NExternalStorage::TEvGetObjectRequest::TPtr& ev) const override {
        TUnavailableExternalStorageOperator::Execute(ev);
        Dispatched.Signal();
    }
};

class TTestReadingAction: public IBlobsReadingAction {
private:
    std::atomic<ui32>& RetryCount;

protected:
    void DoStartReading(THashSet<TBlobRange>&&) override {
    }

    void DoRetryRead(const TBlobRange&) override {
        RetryCount.fetch_add(1);
    }

    THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const override {
        THashMap<TBlobRange, std::vector<TBlobRange>> result;
        for (auto&& r : ranges) {
            result[r] = { r };
        }
        return result;
    }

public:
    TTestReadingAction(const TString& storageId, std::atomic<ui32>& retryCount)
        : IBlobsReadingAction(storageId)
        , RetryCount(retryCount)
    {
    }
};

class TTestTask: public ITask {
private:
    bool DataReady = false;
    bool ErrorOccurred = false;
    TManualEvent Completed;

protected:
    void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>&) override {
        DataReady = true;
        Completed.Signal();
    }

    bool DoOnError(const TString&, const TBlobRange&, const IBlobsReadingAction::TErrorStatus&) override {
        ErrorOccurred = true;
        Completed.Signal();
        return false;
    }

public:
    TTestTask(const TReadActionsCollection& actions)
        : ITask(actions, "test_customer", "test_task")
    {
    }

    bool IsDataReady() const {
        return DataReady;
    }

    bool HasError() const {
        return ErrorOccurred;
    }

    bool WaitForCompletion(TDuration timeout) {
        return Completed.WaitT(timeout);
    }
};

TBlobRange MakeTestBlobRange(ui32 size = 100) {
    TLogoBlobID blobId(72075186224040201, 1, 1, 0, size, 0);
    return TBlobRange(TUnifiedBlobId(Max<ui32>(), blobId), 0, size);
}

struct TTestSetup {
    std::unique_ptr<TTestActorRuntimeBase> Runtime;
    std::shared_ptr<TTestTask> Task;
    std::atomic<ui32> RetryCount{ 0 };
    TActorId ActorId;
    TActorId Sender;
    TBlobRange BlobRange;
    TString StorageId = "test_tier";

    TTestSetup() {
        BlobRange = MakeTestBlobRange();
        Runtime = std::make_unique<TTestRuntime>();
        Runtime->Initialize();

        Runtime->SetScheduledEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration, TInstant&) {
            if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
                return false;
            }
            return true;
        });

        auto action = std::make_shared<TTestReadingAction>(StorageId, RetryCount);
        action->AddRange(BlobRange);

        auto storageCounters = std::make_shared<TStorageCounters>("test");
        auto consumerCounters = storageCounters->GetConsumerCounter(EConsumer::SCAN);
        action->SetCounters(consumerCounters->GetReadCounters());

        TReadActionsCollection actions({ action });
        Task = std::make_shared<TTestTask>(actions);

        ActorId = Runtime->Register(new TActor(Task));
        Runtime->EnableScheduleForActor(ActorId);
        Sender = Runtime->AllocateEdgeActor();

        Runtime->SimulateSleep(TDuration::Zero());
    }

    void SendRetriableError() {
        auto ev = std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeResult>(BlobRange, NKikimrProto::EReplyStatus::ERROR,
            "503 Please reduce your request rate.", "SlowDown, Please reduce your request rate.", false, StorageId, true);
        Runtime->Send(new IEventHandle(ActorId, Sender, ev.release()));
    }

    void SendNonRetriableError() {
        auto ev = std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeResult>(
            BlobRange, NKikimrProto::EReplyStatus::ERROR, "access denied", "AccessDenied, Access Denied", false, StorageId, false);
        Runtime->Send(new IEventHandle(ActorId, Sender, ev.release()));
    }

    void SendSuccess() {
        auto ev = std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeResult>(
            BlobRange, NKikimrProto::EReplyStatus::OK, TString(100, 'x'), TString{}, false, StorageId, false);
        Runtime->Send(new IEventHandle(ActorId, Sender, ev.release()));
    }

    void WaitFor(TDuration d) {
        Runtime->SimulateSleep(d);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TBlobReaderRetry) {
    Y_UNIT_TEST(TierRetryReportsSeparateActualAttempts) {
        using namespace NWrappers::NExternalStorage;
        TManualEvent firstDispatched;
        TTestRuntime runtime(1, true);
        runtime.Initialize();
        const TString storageId = "test_tier";
        const auto range = MakeTestBlobRange();
        auto holder = std::make_shared<NTier::TExternalStorageOperatorHolder>();
        auto errors = std::make_shared<NColumnShard::TErrorCollector>();
        auto unavailable = std::make_shared<TNotifyingUnavailableStorage>(firstDispatched);
        unavailable->InitReplyAdapter(std::make_shared<NTier::TRepliesAdapter>(errors, storageId));
        holder->Emplace(unavailable);
        auto action = std::make_shared<NTier::TReadingAction>(storageId, holder);
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        collector->BindScanId("tier-retry");
        auto parent = collector->Start(NScanStats::TStep::FetchColumns, "FETCH", "price");
        {
            NScanStats::TScope scope(parent);
            action->AddRange(range, {}, NKqpProto::TColumnShardStepDetails::TStorageRead::data);
        }
        auto counters = std::make_shared<TStorageCounters>("test");
        action->SetCounters(counters->GetConsumerCounter(EConsumer::SCAN)->GetReadCounters());
        auto task = std::make_shared<TTestTask>(TReadActionsCollection({ action }));
        runtime.Register(new TActor(task));
        UNIT_ASSERT(firstDispatched.WaitT(TDuration::Seconds(10)));

        Singleton<TFakeExternalStorage>()->SetSecretKey("test");
        auto storage = std::make_shared<TFakeExternalStorage>();
        storage->MutableBucket("bucket").PutObject(NTier::TObjectKey(storageId).Make(range.BlobId.GetLogoBlobId()), TString(100, 'x'));
        auto fake = std::make_shared<TFakeExternalStorageOperator>("bucket", "test", storage);
        fake->InitReplyAdapter(std::make_shared<NTier::TRepliesAdapter>(errors, storageId));
        holder->Emplace(fake);
        UNIT_ASSERT(task->WaitForCompletion(TDuration::Seconds(10)));
        UNIT_ASSERT(task->IsDataReady());
        UNIT_ASSERT(!task->HasError());
        parent->Finish();
        auto report = collector->Drain(true);
        UNIT_ASSERT(report->GetFinished());
        ui32 failed = 0;
        ui32 completed = 0;
        ui64 failedId = 0;
        ui64 completedId = 0;
        for (const auto& step : report->GetSteps()) {
            if (step.GetStepKind() != NScanStats::TStep::StorageRead) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(step.GetParentStepId(), parent->GetStepId());
            const auto& details = step.GetDetails().GetStorageRead();
            UNIT_ASSERT_VALUES_EQUAL(details.GetRequestedRangeBytes(), 100);
            if (step.GetOutcome() == NScanStats::TStep::failed) {
                ++failed;
                failedId = step.GetStepId();
                UNIT_ASSERT(!step.HasReadBytes());
                UNIT_ASSERT(!details.GetIsRetry());
            } else {
                ++completed;
                completedId = step.GetStepId();
                UNIT_ASSERT(details.GetIsRetry());
                UNIT_ASSERT_VALUES_EQUAL(step.GetReadBytes(), 100);
                UNIT_ASSERT(details.GetBackendKind() == NKqpProto::TColumnShardStepDetails::TStorageRead::MemoryOrFake);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(failed, 1);
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        UNIT_ASSERT(failedId && completedId && failedId != completedId);
    }

    Y_UNIT_TEST(RetriableErrorTriggersRetry) {
        TTestSetup setup;

        setup.SendRetriableError();
        setup.WaitFor(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(setup.RetryCount.load(), 1);
        UNIT_ASSERT(!setup.Task->HasError());
        UNIT_ASSERT(!setup.Task->IsDataReady());

        setup.SendSuccess();
        setup.WaitFor(TDuration::Zero());

        UNIT_ASSERT(setup.Task->IsDataReady());
        UNIT_ASSERT(!setup.Task->HasError());
    }

    Y_UNIT_TEST(NonRetriableErrorPropagatesImmediately) {
        TTestSetup setup;

        setup.SendNonRetriableError();
        setup.WaitFor(TDuration::Zero());

        UNIT_ASSERT(setup.Task->HasError());
        UNIT_ASSERT_VALUES_EQUAL(setup.RetryCount.load(), 0);
    }

    Y_UNIT_TEST(RetryExhaustion) {
        TTestSetup setup;

        for (ui32 i = 0; i < 15; ++i) {
            setup.SendRetriableError();
            setup.WaitFor(TDuration::Seconds(10));

            if (setup.Task->HasError()) {
                break;
            }
        }

        UNIT_ASSERT(setup.Task->HasError());
        UNIT_ASSERT(setup.RetryCount.load() > 0);
        UNIT_ASSERT(setup.RetryCount.load() <= 10);
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
