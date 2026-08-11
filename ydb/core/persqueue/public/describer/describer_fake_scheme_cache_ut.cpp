#include "describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

using namespace NActors;
using namespace NSchemeCache;

using TNavigate = TSchemeCacheNavigate;
using TFiller = std::function<void(ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry&)>;

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
            Filler(index, *request, entry);
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    TFiller Filler;
    ui32 RequestIndex = 0;
};

TString EntryPath(const TNavigate::TEntry& entry) {
    return CanonizePath(JoinPath(entry.Path));
}

void FillOkTopic(TNavigate::TEntry& entry, ui64 balancerTabletId) {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = TNavigate::EKind::KindTopic;
    auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
    pqInfo->Description.SetBalancerTabletID(balancerTabletId);
    entry.PQGroupInfo = pqInfo;
    entry.CreateStep = 1;
}

void FillCdcStream(TNavigate::TEntry& entry, const TString& streamName = "feed") {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = TNavigate::EKind::KindCdcStream;
    auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
    self->Info.SetName(streamName);
    entry.Self = self;
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

    void EnableFederationRoot(const TString& federationRoot = "/Root/Federation") {
        auto& appData = Runtime.GetAppData();
        appData.PQConfig.SetTopicsAreFirstClassCitizen(false);
        appData.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);
    }

    TActorId StartDescribe(
        absl::flat_hash_set<TString> topics,
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
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::AccessDenied;
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(IncompleteTopicBalancerZero) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
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
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/0);
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(IncompleteTopicThenSuccessOnSync) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                UNIT_ASSERT(!entry.SyncVersion);
                entry.Status = TNavigate::EStatus::Ok;
                entry.Kind = TNavigate::EKind::KindTopic;
                entry.PQGroupInfo = nullptr;
                return;
            }
            UNIT_ASSERT(entry.SyncVersion);
            FillOkTopic(entry, /*balancerTabletId=*/77);
        });

        env.StartDescribe({"/Root/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Info->Description.GetBalancerTabletID(), 77u);
    }

    Y_UNIT_TEST(UsedSyncVersionOnCacheMiss) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& /*request*/, TNavigate::TEntry& entry) {
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
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
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
        TDescribeEnv env([](ui32 requestIndex, TNavigate& /*request*/, TNavigate::TEntry& entry) {
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

    Y_UNIT_TEST(CdcThenStreamImplSuccess) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/table1/feed");
                FillCdcStream(entry, "feed");
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(requestIndex, 1u);
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/table1/feed/streamImpl");
            FillOkTopic(entry, /*balancerTabletId=*/99);
        });

        env.StartDescribe({"/Root/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("/Root/table1/feed"));
        auto& info = ev->Topics["/Root/table1/feed"];
        UNIT_ASSERT_VALUES_EQUAL(info.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(info.RealPath, "/Root/table1/feed/streamImpl");
        UNIT_ASSERT(info.CdcStream);
        UNIT_ASSERT_VALUES_EQUAL(info.CdcStreamName, "feed");
        UNIT_ASSERT_VALUES_EQUAL(info.Info->Description.GetBalancerTabletID(), 99u);
    }

    Y_UNIT_TEST(CdcThenStreamImplNotFound) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                FillCdcStream(entry);
                return;
            }
            // streamImpl miss → sync retry → still miss → NOT_FOUND
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });

        env.StartDescribe({"/Root/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/table1/feed"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(CdcWithEmptyDatabase) {
        // Regression: empty DatabaseName must still schedule streamImpl retry.
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT(request.DatabaseName.empty());
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/table1/feed");
                FillCdcStream(entry);
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/table1/feed/streamImpl");
            FillOkTopic(entry, /*balancerTabletId=*/55);
        });

        env.StartDescribe({"/Root/table1/feed"}, {}, /*databasePath=*/"");
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/table1/feed"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(ev->Topics["/Root/table1/feed"].CdcStream);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/table1/feed"].RealPath, "/Root/table1/feed/streamImpl");
    }

    Y_UNIT_TEST(CdcThenStreamImplAccessDenied) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                FillCdcStream(entry);
                return;
            }
            entry.Status = TNavigate::EStatus::AccessDenied;
        });

        env.StartDescribe({"/Root/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/table1/feed"].Status, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(FederationRetryAfterMiss) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            if (requestIndex == 0) {
                // Local resolve under /Root misses federation-style short path.
                UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            if (requestIndex == 1) {
                // Sync retry still misses.
                UNIT_ASSERT(entry.SyncVersion);
                UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            // Federation account DB request.
            UNIT_ASSERT_VALUES_EQUAL(requestIndex, 2u);
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT(!entry.SyncVersion);
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic1");
            FillOkTopic(entry, /*balancerTabletId=*/123);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic1"].RealPath, "/Root/Federation/account/topic1");
    }

    Y_UNIT_TEST(FederationSkippedForSingleComponentPath) {
        ui32 requests = 0;
        TDescribeEnv env([&](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            ++requests;
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });
        env.EnableFederationRoot("/Root/Federation");

        // No account/topic shape → ExtractFederationAccount fails → no Federation retry.
        env.StartDescribe({"topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(requests, 2u); // local + sync only
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(SetErrorResultPreservesSuccess) {
        // One topic succeeds locally; another misses into Federation and fails there.
        // Failure for the second must not erase the first SUCCESS.
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            const auto path = EntryPath(entry);
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
                if (path == "/Root/local") {
                    FillOkTopic(entry, /*balancerTabletId=*/1);
                } else {
                    entry.Status = TNavigate::EStatus::PathErrorUnknown;
                }
                return;
            }
            if (request.DatabaseName == "/Root") {
                // Sync retry for the missing federation topic only.
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"/Root/local", "account/missing"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/local"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/missing"].Status, NDescriber::EStatus::NOT_FOUND);
    }

}

} // namespace NKikimr::NPQ
