#include "msgbus_server_pq_metacache.h"

#include <ydb/core/base/path.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NKikimr::NMsgBusProxy {
namespace {

using namespace NActors;
using namespace NSchemeCache;
using namespace NPqMetaCacheV2;

constexpr TStringBuf DbRoot = "/Root/accounts";

TString EntryPath(const TSchemeCacheNavigate::TEntry& entry) {
    return TString("/") + JoinPath(entry.Path);
}

class TScriptedSchemeCache : public TActorBootstrapped<TScriptedSchemeCache> {
public:
    explicit TScriptedSchemeCache(TVector<TVector<TString>>* requests)
        : Requests(requests)
    {
    }

    void Bootstrap() {
        Become(&TScriptedSchemeCache::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
    )

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        auto request = std::move(ev->Get()->Request);
        TVector<TString> paths;
        paths.reserve(request->ResultSet.size());
        for (auto& entry : request->ResultSet) {
            const TString path = EntryPath(entry);
            paths.push_back(path);
            if (path.Contains("topic-miss") && path.StartsWith(DbRoot)) {
                entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            } else if (path.Contains("topic-ok")) {
                entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            } else {
                entry.Status = TSchemeCacheNavigate::EStatus::PathErrorUnknown;
            }
        }
        Requests->push_back(std::move(paths));
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    TVector<TVector<TString>>* Requests;
};

Y_UNIT_TEST_SUITE(TPersQueueMetaCacheFallback) {

Y_UNIT_TEST(SkipsEntriesWithoutAccountWhenSchedulingFallback) {
    TVector<TVector<TString>> requests;
    TTestBasicRuntime runtime(1, false);
    runtime.Initialize(TAppPrepare().Unwrap());
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(TString(DbRoot));

    const auto schemeCacheId = runtime.Register(new TScriptedSchemeCache(&requests));
    const auto metaCacheId = runtime.Register(CreatePQMetaCache(schemeCacheId));
    runtime.DispatchEvents();

    NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
    auto noAccount = factory.MakeDiscoveryConverter("rt3.dc1--justtopic", {}, "dc1", "");
    auto resolved = factory.MakeDiscoveryConverter("account-ok/topic-ok", {}, "dc1", "");
    auto missing = factory.MakeDiscoveryConverter("account-miss/topic-miss", {}, "dc1", "");
    UNIT_ASSERT_C(noAccount->IsValid(), noAccount->GetReason());
    UNIT_ASSERT_C(resolved->IsValid(), resolved->GetReason());
    UNIT_ASSERT_C(missing->IsValid(), missing->GetReason());

    TVector<NPersQueue::TDiscoveryConverterPtr> topics{noAccount, resolved, missing};
    const auto edge = runtime.AllocateEdgeActor();
    runtime.Send(new IEventHandle(
        metaCacheId,
        edge,
        new TEvPqNewMetaCache::TEvDescribeTopicsRequest(topics, false)), 0, true);

    auto response = runtime.GrabEdgeEvent<TEvPqNewMetaCache::TEvDescribeTopicsResponse>(edge);
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Get()->Result);
    const auto& resultSet = response->Get()->Result->ResultSet;
    UNIT_ASSERT_VALUES_EQUAL(resultSet.size(), 3u);
    UNIT_ASSERT(resultSet[0].Status == TSchemeCacheNavigate::EStatus::PathErrorUnknown);
    UNIT_ASSERT(resultSet[1].Status == TSchemeCacheNavigate::EStatus::Ok);
    UNIT_ASSERT(resultSet[2].Status == TSchemeCacheNavigate::EStatus::Ok);

    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(requests[1].size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(requests[1][0], "/Root/accounts/account-miss/topic-miss");
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NMsgBusProxy
