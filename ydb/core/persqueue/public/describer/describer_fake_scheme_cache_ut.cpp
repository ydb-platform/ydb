#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

using namespace NActors;
using namespace NSchemeCache;

using TNavigate = TSchemeCacheNavigate;
using TFiller = std::function<void(ui32 /*requestIndex*/, TNavigate::TEntry&)>;

class TFakeSchemeCacheActor : public TActorBootstrapped<TFakeSchemeCacheActor> {
public:
    explicit TFakeSchemeCacheActor(TFiller filler)
        : Filler(std::move(filler))
    {
    }

    void Bootstrap() {
        Become(&TFakeSchemeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
    )

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        const ui32 index = RequestIndex++;
        auto request = std::move(ev->Get()->Request);
        for (auto& entry : request->ResultSet) {
            Filler(index, entry);
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    TFiller Filler;
    ui32 RequestIndex = 0;
};

void FillOkTopic(TNavigate::TEntry& entry, ui64 balancerTabletId) {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = TNavigate::EKind::KindTopic;
    auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
    pqInfo->Description.SetBalancerTabletID(balancerTabletId);
    entry.PQGroupInfo = pqInfo;
    entry.CreateStep = 1;
}

struct TDescribeEnv {
    TTestBasicRuntime Runtime;
    TActorId EdgeId;

    explicit TDescribeEnv(TFiller filler)
        : Runtime(1, false) // UseRealThreads=false — observers / fake actors see all mail
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetLogPriority(NKikimrServices::PQ_DESCRIBER, NLog::PRI_DEBUG);

        auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(std::move(filler)));
        Runtime.EnableScheduleForActor(schemeCacheId);
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        EdgeId = Runtime.AllocateEdgeActor();
    }

    TActorId StartDescribe(
        std::unordered_set<TString> topics,
        const NDescriber::TDescribeSettings& settings = {},
        const TString& databasePath = "/Root")
    {
        auto describerId = Runtime.Register(NDescriber::CreateDescriberActor(
            EdgeId,
            databasePath,
            std::move(topics),
            settings
        ));
        Runtime.EnableScheduleForActor(describerId);
        Runtime.DispatchEvents();
        return describerId;
    }

    THolder<NDescriber::TEvDescribeTopicsResponse> WaitResponse() {
        return Runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TDescriberFakeSchemeCacheTests) {

    Y_UNIT_TEST(AccessDeniedFromSchemeCache) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::AccessDenied;
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(IncompleteTopicBalancerZero) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate::TEntry& entry) {
            // KindTopic without balancer — incomplete create; triggers sync retry then NOT_FOUND.
            entry.Status = TNavigate::EStatus::Ok;
            entry.Kind = TNavigate::EKind::KindTopic;
            entry.PQGroupInfo = nullptr;
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(IncompleteTopicBalancerTabletIdZero) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/0);
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(UsedSyncVersionOnCacheMiss) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                UNIT_ASSERT(!entry.SyncVersion);
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            UNIT_ASSERT(entry.SyncVersion);
            FillOkTopic(entry, /*balancerTabletId=*/42);
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].RealPath, "/Root/topic1");
        UNIT_ASSERT(ev->Topics["/Root/topic1"].Info);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Info->Description.GetBalancerTabletID(), 42u);
    }

    Y_UNIT_TEST(UnknownErrorFromSchemeCache) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::LookupError;
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::UNKNOWN_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].RealPath, "/Root/topic1");
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(RootUnknownTriggersSyncThenNotFound) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                entry.Status = TNavigate::EStatus::RootUnknown;
                return;
            }
            entry.Status = TNavigate::EStatus::RootUnknown;
        });

        env.StartDescribe({"/Root/missing"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/missing"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/missing"].Status, NDescriber::EStatus::NOT_FOUND);
    }

}

} // namespace NKikimr::NPQ
