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

void FillOkTopic(TNavigate::TEntry& entry, ui64 balancerTabletId, TStringBuf federationAccount = {}) {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = TNavigate::EKind::KindTopic;
    auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
    pqInfo->Description.SetBalancerTabletID(balancerTabletId);
    auto& cfg = *pqInfo->Description.MutablePQTabletConfig();
    const TString path = EntryPath(entry);
    cfg.SetTopicPath(path);
    if (!entry.Path.empty()) {
        cfg.SetTopicName(entry.Path.back());
    }
    if (entry.Path.size() >= 2) {
        TVector<TString> dbParts(entry.Path.begin(), entry.Path.end() - 1);
        cfg.SetYdbDatabasePath(CanonizePath(JoinPath(dbParts)));
    }
    cfg.SetDC("dc1");
    cfg.SetLocalDC(true);
    if (federationAccount) {
        cfg.SetFederationAccount(TString(federationAccount));
    }
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
        UNIT_ASSERT(ev->Topics["/Root/topic1"].Names);
        UNIT_ASSERT_C(ev->Topics["/Root/topic1"].Names->IsValid(), ev->Topics["/Root/topic1"].Names->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Names->GetClientsideName(), "topic1");
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Names->GetPrimaryPath(), "/Root/topic1");
        UNIT_ASSERT(ev->Topics["/Root/topic1"].Names->GetAccount().empty());
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
        UNIT_ASSERT(info.Names);
        UNIT_ASSERT_C(info.Names->IsValid(), info.Names->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(info.Names->GetClientsideName(), "/Root/table1/feed");
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

    Y_UNIT_TEST(CdcStreamImplNotFoundSkipsFederation) {
        // Broken streamImpl must not be rewritten under another Federation database.
        ui32 requests = 0;
        TDescribeEnv env([&](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            requests = requestIndex + 1;
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/table1");
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/table1/feed");
                FillCdcStream(entry);
                return;
            }
            UNIT_ASSERT(EntryPath(entry).EndsWith("/streamImpl"));
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"/Root/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(requests, 3u); // feed + streamImpl + sync streamImpl
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

    Y_UNIT_TEST(FederationNavigateUsesAccountDatabase) {
        // ResolveName returns NavigateDatabase = account tenant; no miss→retry hop.
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic1");
            if (requestIndex == 0) {
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            UNIT_ASSERT(entry.SyncVersion);
            FillOkTopic(entry, /*balancerTabletId=*/123);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic1"].RealPath, "/Root/Federation/account/topic1");
    }

    Y_UNIT_TEST(FederationNavigateAbsoluteDatabasePrefixedPath) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic1");
            if (requestIndex == 0) {
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            FillOkTopic(entry, /*balancerTabletId=*/7);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"/Root/account/topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/account/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/account/topic1"].RealPath, "/Root/Federation/account/topic1");
    }

    Y_UNIT_TEST(FederationThenCdcKeepsAccountDatabase) {
        // CDC discovered under a Federation account DB must retry streamImpl with
        // the same AccountDatabase.
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed");
                FillCdcStream(entry, "feed");
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(requestIndex, 1u);
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed/streamImpl");
            FillOkTopic(entry, /*balancerTabletId=*/88);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("account/table1/feed"));
        auto& info = ev->Topics["account/table1/feed"];
        UNIT_ASSERT_VALUES_EQUAL(info.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(info.CdcStream);
        UNIT_ASSERT_VALUES_EQUAL(info.CdcStreamName, "feed");
        UNIT_ASSERT_VALUES_EQUAL(info.RealPath, "/Root/Federation/account/table1/feed/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(info.Info->Description.GetBalancerTabletID(), 88u);
    }

    Y_UNIT_TEST(FederationSkippedForSingleComponentPath) {
        ui32 requests = 0;
        TDescribeEnv env([&](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            ++requests;
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
            // Bare name stays under DatabasePath (NavigateDatabase = request DB).
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/topic1");
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"topic1"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(requests, 2u); // miss + sync only
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(SetErrorResultPreservesSuccess) {
        // Two topics in different NavigateDatabases: success for one and miss for
        // the other must not interfere.
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            const auto path = EntryPath(entry);
            if (request.DatabaseName == "/Root") {
                UNIT_ASSERT_VALUES_EQUAL(path, "/Root/local");
                FillOkTopic(entry, /*balancerTabletId=*/1);
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(path, "/Root/Federation/account/missing");
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"/Root/local", "account/missing"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/local"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/missing"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(LegacyRt3ResolvedUnderFederationRoot) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            FillOkTopic(entry, /*balancerTabletId=*/11);
        });
        env.EnableFederationRoot("/Root/Federation");

        UNIT_ASSERT(!env.Runtime.GetAppData().PQConfig.GetTopicsAreFirstClassCitizen());
        UNIT_ASSERT_VALUES_EQUAL(
            env.Runtime.GetAppData().PQConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot(),
            "/Root/Federation");

        env.StartDescribe({"rt3.dc1--account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("rt3.dc1--account--topic"));
        auto& info = ev->Topics["rt3.dc1--account--topic"];
        UNIT_ASSERT_VALUES_EQUAL(info.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(info.RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(LegacyRt3RemoteDcWithoutLocalDcSkipsMirror) {
        // Empty localDc/dc in ResolveName → no -mirrored-from- suffix.
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            FillOkTopic(entry, /*balancerTabletId=*/12);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.dc2--account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc2--account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc2--account--topic"].RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(LegacyShortNameResolved) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            FillOkTopic(entry, /*balancerTabletId=*/13);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account--topic"].RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(LegacyRt3CdcResolved) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed");
                FillCdcStream(entry, "feed");
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed/streamImpl");
            FillOkTopic(entry, /*balancerTabletId=*/14);
        });
        env.EnableFederationRoot("/Root/Federation");

        // Nested path in legacy form: @ → /
        env.StartDescribe({"rt3.dc1--account@table1--feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->Topics.contains("rt3.dc1--account@table1--feed"));
        auto& info = ev->Topics["rt3.dc1--account@table1--feed"];
        UNIT_ASSERT_VALUES_EQUAL(info.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(info.CdcStream);
        UNIT_ASSERT_VALUES_EQUAL(info.CdcStreamName, "feed");
        UNIT_ASSERT_VALUES_EQUAL(info.RealPath, "/Root/Federation/account/table1/feed/streamImpl");
    }

    Y_UNIT_TEST(LegacyNameResolveBadRequest) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& /*entry*/) {
            UNIT_FAIL("scheme cache must not be called for invalid legacy name");
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.bad"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.bad"].Status, NDescriber::EStatus::BAD_REQUEST);
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(FccKeepsLiteralLegacyLookingName) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/rt3.dc1--account--topic");
            FillOkTopic(entry, /*balancerTabletId=*/15);
        });
        auto& appData = env.Runtime.GetAppData();
        appData.PQConfig.SetTopicsAreFirstClassCitizen(true);
        appData.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbCommunal");

        env.StartDescribe({"rt3.dc1--account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].RealPath, "/Root/rt3.dc1--account--topic");
    }

    Y_UNIT_TEST(MultipleOriginalsSameResolvedPath) {
        // Two client names resolve to one SchemeCache path — both must get Result.
        ui32 navigateCalls = 0;
        TDescribeEnv env([&](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            ++navigateCalls;
            UNIT_ASSERT_VALUES_EQUAL(request.ResultSet.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            FillOkTopic(entry, /*balancerTabletId=*/16);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.dc1--account--topic", "account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(navigateCalls, 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].RealPath, "/Root/Federation/account/topic");
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account--topic"].RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(MultipleOriginalsFederationNavigate) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.ResultSet.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            if (requestIndex == 0) {
                entry.Status = TNavigate::EStatus::PathErrorUnknown;
                return;
            }
            UNIT_ASSERT(entry.SyncVersion);
            FillOkTopic(entry, /*balancerTabletId=*/17);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.dc1--account--topic", "account/topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].RealPath, "/Root/Federation/account/topic");
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic"].RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(MultipleOriginalsCdcFanOut) {
        TDescribeEnv env([](ui32 requestIndex, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.ResultSet.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            if (requestIndex == 0) {
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed");
                FillCdcStream(entry, "feed");
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/table1/feed/streamImpl");
            FillOkTopic(entry, /*balancerTabletId=*/18);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.dc1--account@table1--feed", "account/table1/feed"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        for (const auto* key : {"rt3.dc1--account@table1--feed", "account/table1/feed"}) {
            auto& info = ev->Topics[key];
            UNIT_ASSERT_VALUES_EQUAL(info.Status, NDescriber::EStatus::SUCCESS);
            UNIT_ASSERT(info.CdcStream);
            UNIT_ASSERT_VALUES_EQUAL(info.CdcStreamName, "feed");
            UNIT_ASSERT_VALUES_EQUAL(info.RealPath, "/Root/Federation/account/table1/feed/streamImpl");
        }
    }

    Y_UNIT_TEST(MixedBadRequestAndSuccess) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.ResultSet.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root/Federation/account");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/Federation/account/topic");
            FillOkTopic(entry, /*balancerTabletId=*/19);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.bad", "rt3.dc1--account--topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.bad"].Status, NDescriber::EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.dc1--account--topic"].RealPath, "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(OnlyBadRequestSkipsSchemeCache) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& /*entry*/) {
            UNIT_FAIL("scheme cache must not be called when all names fail ResolveName");
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"rt3.bad", "rt3.also-bad"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.bad"].Status, NDescriber::EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["rt3.also-bad"].Status, NDescriber::EStatus::BAD_REQUEST);
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(ModernPathUnderDatabase) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, "/Root");
            UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), "/Root/dir/topic");
            FillOkTopic(entry, /*balancerTabletId=*/20);
        });
        auto& appData = env.Runtime.GetAppData();
        appData.PQConfig.SetTopicsAreFirstClassCitizen(true);

        env.StartDescribe({"dir/topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["dir/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["dir/topic"].RealPath, "/Root/dir/topic");
    }

    Y_UNIT_TEST(PathErrorUnknownUnauthorizedWithSecurityObject) {
        // Sync miss with SecurityObject and no access → UNAUTHORIZED (not NOT_FOUND).
        auto token = MakeIntrusiveConst<NACLib::TUserToken>("user@staff", TVector<TString>{});
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
            entry.SecurityObject = MakeIntrusive<TSecurityObject>("root@builtin", TString{}, false);
        });

        env.StartDescribe(
            {"/Root/topic1"},
            NDescriber::TDescribeSettings{
                .UserToken = token,
                .AccessRights = NACLib::SelectRow,
                .ForceSyncVersion = true,
            });
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(NotTopicUnauthorizedWithoutDescribe) {
        auto token = MakeIntrusiveConst<NACLib::TUserToken>("user@staff", TVector<TString>{});
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::Ok;
            entry.Kind = TNavigate::EKind::KindPath;
            entry.SecurityObject = MakeIntrusive<TSecurityObject>("root@builtin", TString{}, false);
        });

        env.StartDescribe(
            {"/Root/dir"},
            NDescriber::TDescribeSettings{
                .UserToken = token,
                .AccessRights = NACLib::SelectRow,
            });
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/dir"].Status, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(TopicUnauthorizedWithDescribeAccess) {
        auto token = MakeIntrusiveConst<NACLib::TUserToken>("user@staff", TVector<TString>{});
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/5);
            NACLib::TSecurityObject aclObj("root@builtin", false);
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user@staff");
            aclObj.ApplyDiff(acl);
            entry.SecurityObject = MakeIntrusive<TSecurityObject>(
                "root@builtin", aclObj.GetACL().SerializeAsString(), false);
        });

        env.StartDescribe(
            {"/Root/topic1"},
            NDescriber::TDescribeSettings{
                .UserToken = token,
                .AccessRights = NACLib::SelectRow,
            });
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            ev->Topics["/Root/topic1"].Status,
            NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS);
    }

    Y_UNIT_TEST(AccessOrAllowsViaFakeSchemeCache) {
        auto token = MakeIntrusiveConst<NACLib::TUserToken>("user@staff", TVector<TString>{});
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/6);
            NACLib::TSecurityObject aclObj("root@builtin", false);
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user@staff");
            aclObj.ApplyDiff(acl);
            entry.SecurityObject = MakeIntrusive<TSecurityObject>(
                "root@builtin", aclObj.GetACL().SerializeAsString(), false);
        });

        env.StartDescribe(
            {"/Root/topic1"},
            NDescriber::TDescribeSettings{
                .UserToken = token,
                .AccessRights = NDescriber::TAccessRights(NACLib::SelectRow, NACLib::DescribeSchema),
            });
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
    }

    Y_UNIT_TEST(MultipleNavigateDatabasesBatched) {
        absl::flat_hash_set<TString> seenDatabases;
        TDescribeEnv env([&](ui32 /*requestIndex*/, TNavigate& request, TNavigate::TEntry& entry) {
            seenDatabases.insert(request.DatabaseName);
            FillOkTopic(entry, /*balancerTabletId=*/7);
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account1/topic", "account2/topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT(seenDatabases.contains("/Root/Federation/account1"));
        UNIT_ASSERT(seenDatabases.contains("/Root/Federation/account2"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account1/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account2/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account1/topic"].RealPath, "/Root/Federation/account1/topic");
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account2/topic"].RealPath, "/Root/Federation/account2/topic");
    }

    Y_UNIT_TEST(FederationSuccessFillsNamesFromConfig) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/21, "account");
        });
        env.EnableFederationRoot("/Root/Federation");
        env.Runtime.GetAppData().PQConfig.SetRoot("/Root/PQ");

        env.StartDescribe({"account/topic"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/topic"].Status, NDescriber::EStatus::SUCCESS);
        const auto& names = ev->Topics["account/topic"].Names;
        UNIT_ASSERT(names);
        UNIT_ASSERT_C(names->IsValid(), names->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(names->GetAccount(), "account");
        UNIT_ASSERT_VALUES_EQUAL(names->GetCluster(), "dc1");
        UNIT_ASSERT_VALUES_EQUAL(names->GetClientsideName(), "rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(names->GetFederationPath(), "account/topic");
        UNIT_ASSERT_VALUES_EQUAL(names->GetPrimaryPath(), "/Root/Federation/account/topic");
    }

    Y_UNIT_TEST(BadRequestLeavesNamesEmpty) {
        TDescribeEnv env([](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& /*entry*/) {
            UNIT_FAIL("scheme cache must not be queried for a malformed name");
        });
        env.EnableFederationRoot("/Root/Federation");

        env.StartDescribe({"account/"});
        auto ev = env.WaitResponse();

        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["account/"].Status, NDescriber::EStatus::BAD_REQUEST);
        UNIT_ASSERT(!ev->Topics["account/"].Names);
    }

    Y_UNIT_TEST(StoresDomainInfo) {
        const TPathId serverlessDomain{1, 2};
        const TPathId sharedDomain{1, 1};
        TDescribeEnv env([=](ui32 /*requestIndex*/, TNavigate& /*request*/, TNavigate::TEntry& entry) {
            FillOkTopic(entry, /*balancerTabletId=*/1);
            const TString path = EntryPath(entry);
            if (path == "/Root/serverless") {
                entry.DomainInfo = MakeIntrusive<TDomainInfo>(serverlessDomain, sharedDomain);
            } else if (path == "/Root/dedicated") {
                entry.DomainInfo = MakeIntrusive<TDomainInfo>(sharedDomain, sharedDomain);
            }
        });

        env.StartDescribe({"/Root/serverless", "/Root/dedicated", "/Root/none"});
        auto ev = env.WaitResponse();

        const auto& serverless = ev->Topics["/Root/serverless"];
        UNIT_ASSERT_VALUES_EQUAL(serverless.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(serverless.DomainInfo);
        UNIT_ASSERT(serverless.DomainInfo->DomainKey == serverlessDomain);
        UNIT_ASSERT(serverless.DomainInfo->ResourcesDomainKey == sharedDomain);
        UNIT_ASSERT(serverless.IsServerless());

        const auto& dedicated = ev->Topics["/Root/dedicated"];
        UNIT_ASSERT_VALUES_EQUAL(dedicated.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(dedicated.DomainInfo);
        UNIT_ASSERT(!dedicated.IsServerless());

        const auto& none = ev->Topics["/Root/none"];
        UNIT_ASSERT_VALUES_EQUAL(none.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(!none.DomainInfo);
        UNIT_ASSERT(!none.IsServerless());
    }

}

} // namespace NKikimr::NPQ
