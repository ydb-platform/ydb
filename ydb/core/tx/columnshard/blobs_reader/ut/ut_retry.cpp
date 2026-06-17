#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

namespace {

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

protected:
    void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>&) override {
        DataReady = true;
    }

    bool DoOnError(const TString&, const TBlobRange&, const IBlobsReadingAction::TErrorStatus&) override {
        ErrorOccurred = true;
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
        Runtime = std::make_unique<TTestActorRuntimeBase>();
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
