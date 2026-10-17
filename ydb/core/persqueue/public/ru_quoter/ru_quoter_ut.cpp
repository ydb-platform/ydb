#include "ru_quoter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

using namespace NActors;
using namespace NSchemeCache;
using namespace NRuQuoter;

using TNavigate = TSchemeCacheNavigate;
using TFiller = std::function<void(TNavigate& /*request*/, TNavigate::TEntry*)>;

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
        auto request = std::move(ev->Get()->Request);
        TNavigate::TEntry* entry = request->ResultSet.empty() ? nullptr : &request->ResultSet.front();
        Filler(*request, entry);
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    TFiller Filler;
};

class THoldingSchemeCacheActor : public TActorBootstrapped<THoldingSchemeCacheActor> {
public:
    void Bootstrap() {
        Become(&THoldingSchemeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
    )

    ui32 RequestCount = 0;

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr&) {
        ++RequestCount;
    }
};

class TFakeMeteringActor : public TActor<TFakeMeteringActor> {
public:
    TFakeMeteringActor(TVector<TString>* records)
        : TActor(&TFakeMeteringActor::StateWork)
        , Records(records)
    {
    }

    STRICT_STFUNC(StateWork,
        hFunc(NMetering::TEvMetering::TEvWriteMeteringJson, Handle);
    )

private:
    void Handle(NMetering::TEvMetering::TEvWriteMeteringJson::TPtr& ev) {
        Records->push_back(ev->Get()->MeteringJson);
    }

    TVector<TString>* Records;
};

class TFakeQuoterService : public TActor<TFakeQuoterService> {
public:
    explicit TFakeQuoterService(TEvQuota::TEvClearance::EResult result, bool reply = true)
        : TActor(&TFakeQuoterService::StateWork)
        , Result(result)
        , Reply(reply)
    {
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvQuota::TEvRequest, Handle);
    )

private:
    void Handle(TEvQuota::TEvRequest::TPtr& ev) {
        ++RequestCount;
        if (Reply) {
            Send(ev->Sender, new TEvQuota::TEvClearance(Result), 0, ev->Cookie);
        }
    }

public:
    ui32 RequestCount = 0;

private:
    TEvQuota::TEvClearance::EResult Result;
    bool Reply;
};

void FillOkPath(TNavigate::TEntry& entry, const THashMap<TString, TString>& attrs = {}) {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = TNavigate::EKind::KindPath;
    entry.Attributes = attrs;
}

struct TQuoterEnv {
    TTestBasicRuntime Runtime;
    TActorId EdgeId;
    TVector<TString> Bills;
    TFakeQuoterService* QuoterService = nullptr;

    explicit TQuoterEnv(
        TFiller filler,
        TEvQuota::TEvClearance::EResult quotaResult = TEvQuota::TEvClearance::EResult::Success,
        bool replyQuota = true,
        bool enableRestartOnException = false)
        : Runtime(1, false)
    {
        TAppPrepare app;
        app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(enableRestartOnException);
        Runtime.Initialize(app.Unwrap());

        auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(std::move(filler)));
        Runtime.EnableScheduleForActor(schemeCacheId);
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        auto* quoter = new TFakeQuoterService(quotaResult, replyQuota);
        QuoterService = quoter;
        auto quoterId = Runtime.Register(quoter);
        Runtime.EnableScheduleForActor(quoterId);
        Runtime.RegisterService(MakeQuoterServiceID(), quoterId);

        auto meteringId = Runtime.Register(new TFakeMeteringActor(&Bills));
        Runtime.EnableScheduleForActor(meteringId);
        Runtime.RegisterService(NMetering::MakeMeteringServiceID(), meteringId);

        EdgeId = Runtime.AllocateEdgeActor();
    }

    TActorId Start(TRequestUnitsQuoterSettings settings) {
        auto id = Runtime.Register(CreateRequestUnitsQuoter(EdgeId, std::move(settings)));
        Runtime.EnableScheduleForActor(id);
        return id;
    }

    THolder<TEvChargeRequestUnitsResponse> WaitResponse(TDuration timeout = TDuration::Seconds(5)) {
        return Runtime.GrabEdgeEvent<TEvChargeRequestUnitsResponse>(timeout);
    }
};

struct THoldingQuoterEnv {
    TTestBasicRuntime Runtime;
    TActorId EdgeId;
    THoldingSchemeCacheActor* Cache = nullptr;
    TVector<TString> Bills;

    THoldingQuoterEnv()
        : Runtime(1, false)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());

        auto* cache = new THoldingSchemeCacheActor();
        Cache = cache;
        auto schemeCacheId = Runtime.Register(cache);
        Runtime.EnableScheduleForActor(schemeCacheId);
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        auto meteringId = Runtime.Register(new TFakeMeteringActor(&Bills));
        Runtime.EnableScheduleForActor(meteringId);
        Runtime.RegisterService(NMetering::MakeMeteringServiceID(), meteringId);

        EdgeId = Runtime.AllocateEdgeActor();
    }

    TActorId Start(TRequestUnitsQuoterSettings settings) {
        auto id = Runtime.Register(CreateRequestUnitsQuoter(EdgeId, std::move(settings)));
        Runtime.EnableScheduleForActor(id);
        return id;
    }

    THolder<TEvChargeRequestUnitsResponse> WaitResponse(TDuration timeout = TDuration::Seconds(5)) {
        return Runtime.GrabEdgeEvent<TEvChargeRequestUnitsResponse>(timeout);
    }
};

THashMap<TString, TString> MeteringAttrs() {
    return {
        {TString(CLOUD_ID_ATTR), "cloud"},
        {TString(FOLDER_ID_ATTR), "folder"},
        {TString(DATABASE_ID_ATTR), "database"},
    };
}

THashMap<TString, TString> RlAndMeteringAttrs() {
    auto attrs = MeteringAttrs();
    attrs[TString(RL_COORDINATION_NODE_ATTR)] = "/Root/ru";
    attrs[TString(RL_TOPIC_RESOURCE_ATTR)] = "ru-resource";
    return attrs;
}

TString MakeSerializedToken() {
    return NACLib::TUserToken("user@builtin", {}).SerializeAsString();
}

TRequestUnitsQuoterSettings MakeSettings(ui64 ru) {
    return {
        .Database = "/Root",
        .Ru = ru,
        .Token = MakeSerializedToken(),
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TRuQuoterTests) {

    Y_UNIT_TEST(ConvertAndDescription) {
        UNIT_ASSERT_VALUES_EQUAL(Convert(EStatus::Success), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Convert(EStatus::Throttled), Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(Convert(EStatus::UnknownError), Ydb::StatusIds::INTERNAL_ERROR);

        UNIT_ASSERT(Description(EStatus::Success).Contains("have been charged"));
        UNIT_ASSERT_VALUES_EQUAL(Description(EStatus::Throttled), "Request was throttled by the rate limiter");
        UNIT_ASSERT_VALUES_EQUAL(Description(EStatus::UnknownError), "Unexpected rate limiter wakeup");
    }

    Y_UNIT_TEST(ParseRlContextRequiresBothAttrs) {
        THashMap<TString, TString> attrs;
        UNIT_ASSERT(!ParseRlContext(attrs, "/Root", "token"));
        attrs[TString(RL_COORDINATION_NODE_ATTR)] = "/Root/ru";
        UNIT_ASSERT(!ParseRlContext(attrs, "/Root", "token"));
        attrs[TString(RL_TOPIC_RESOURCE_ATTR)] = "resource";
        auto ctx = ParseRlContext(attrs, "/Root", "token");
        UNIT_ASSERT(ctx.Defined());
        UNIT_ASSERT(*ctx);
    }

    Y_UNIT_TEST(ParseMeteringIdsIsCompleteOnlyWhenAllPresent) {
        THashMap<TString, TString> attrs;
        UNIT_ASSERT(!ParseMeteringIds(attrs).IsComplete());
        attrs[TString(CLOUD_ID_ATTR)] = "cloud";
        attrs[TString(FOLDER_ID_ATTR)] = "folder";
        UNIT_ASSERT(!ParseMeteringIds(attrs).IsComplete());
        attrs[TString(DATABASE_ID_ATTR)] = "database";
        UNIT_ASSERT(ParseMeteringIds(attrs).IsComplete());
    }

    Y_UNIT_TEST(RequestUnitsBillUsesYdsSchema) {
        const TMeteringIds ids{
            .CloudId = "cloud",
            .FolderId = "folder",
            .DatabaseId = "database",
        };
        const TString jsonLine = MakeRequestUnitsBill(ids, 7, TInstant::Seconds(1), "bill-id");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(jsonLine, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["schema"].GetString(), "yds.serverless.requests.v1");
        UNIT_ASSERT_VALUES_UNEQUAL(json["schema"].GetString(), "ydb.serverless.requests.v1");
        UNIT_ASSERT_VALUES_EQUAL(json["cloud_id"].GetString(), "cloud");
        UNIT_ASSERT_VALUES_EQUAL(json["folder_id"].GetString(), "folder");
        UNIT_ASSERT_VALUES_EQUAL(json["resource_id"].GetString(), "database");
        UNIT_ASSERT_VALUES_EQUAL(json["usage"]["quantity"].GetInteger(), 7);
        UNIT_ASSERT_VALUES_EQUAL(json["usage"]["unit"].GetString(), "request_unit");
    }

    Y_UNIT_TEST(ZeroRuRepliesOkWithoutNavigate) {
        ui32 navigates = 0;
        TQuoterEnv env([&](TNavigate&, TNavigate::TEntry*) {
            ++navigates;
        });

        env.Start(MakeSettings(0));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(ev->Message, Description(EStatus::Success));
        UNIT_ASSERT_VALUES_EQUAL(navigates, 0u);
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(NavigateNotOkSkipsQuotaAndBill) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            entry->Status = TNavigate::EStatus::PathErrorUnknown;
        });

        env.Start(MakeSettings(2));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT(env.Bills.empty());
        UNIT_ASSERT_VALUES_EQUAL(env.QuoterService->RequestCount, 0u);
    }

    Y_UNIT_TEST(NavigateEmptyResultSetSkipsQuotaAndBill) {
        TQuoterEnv env([](TNavigate& request, TNavigate::TEntry*) {
            request.ResultSet.clear();
        });

        env.Start(MakeSettings(2));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(OkWithoutRlWritesBillWhenIdsComplete) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            FillOkPath(*entry, MeteringAttrs());
        });

        env.Start(MakeSettings(7));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(env.QuoterService->RequestCount, 0u);
        UNIT_ASSERT_VALUES_EQUAL(env.Bills.size(), 1u);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(env.Bills[0], &json));
        UNIT_ASSERT_VALUES_EQUAL(json["schema"].GetString(), REQUEST_UNITS_SCHEMA);
        UNIT_ASSERT_VALUES_EQUAL(json["usage"]["quantity"].GetInteger(), 7);
    }

    Y_UNIT_TEST(OkWithoutCompleteMeteringIdsDoesNotBill) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            FillOkPath(*entry, {{TString(CLOUD_ID_ATTR), "cloud"}});
        });

        env.Start(MakeSettings(2));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(RlAllowedChargesAndBills) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            FillOkPath(*entry, RlAndMeteringAttrs());
        });

        env.Start(MakeSettings(3));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(env.QuoterService->RequestCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(env.Bills.size(), 1u);
    }

    Y_UNIT_TEST(RlDeadlineThrottlesWithoutBill) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            FillOkPath(*entry, RlAndMeteringAttrs());
        }, TEvQuota::TEvClearance::EResult::Deadline);

        env.Start(MakeSettings(3));
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Throttled);
        UNIT_ASSERT_VALUES_EQUAL(ev->Message, Description(EStatus::Throttled));
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(UnexpectedWakeupWhileWaitingNavigate) {
        THoldingQuoterEnv env;
        auto id = env.Start(MakeSettings(2));
        env.Runtime.Send(new IEventHandle(id, env.EdgeId, new TEvents::TEvWakeup(/*RecheckAcl*/ 4)), 0, true);
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::UnknownError);
        UNIT_ASSERT_VALUES_EQUAL(ev->Message, Description(EStatus::UnknownError));
        UNIT_ASSERT_VALUES_EQUAL(env.Cache->RequestCount, 1u);
    }

    Y_UNIT_TEST(RlAllowedWakeupWhileWaitingNavigate) {
        THoldingQuoterEnv env;
        auto id = env.Start(MakeSettings(2));
        env.Runtime.Send(new IEventHandle(id, env.EdgeId, new TEvents::TEvWakeup(/*RlAllowed*/ 2)), 0, true);
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(RlNoResourceWakeupWhileWaitingNavigate) {
        THoldingQuoterEnv env;
        auto id = env.Start(MakeSettings(2));
        env.Runtime.Send(new IEventHandle(id, env.EdgeId, new TEvents::TEvWakeup(/*RlNoResource*/ 3)), 0, true);
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Throttled);
    }

    Y_UNIT_TEST(UnhandledExceptionRepliesUnknownError) {
        TQuoterEnv env([](TNavigate&, TNavigate::TEntry* entry) {
            UNIT_ASSERT(entry);
            FillOkPath(*entry, RlAndMeteringAttrs());
        }, TEvQuota::TEvClearance::EResult::Success, /*replyQuota=*/false, /*enableRestartOnException=*/true);

        auto id = env.Start(MakeSettings(3));
        TDispatchOptions waitQuota;
        waitQuota.CustomFinalCondition = [&] {
            return env.QuoterService->RequestCount > 0;
        };
        env.Runtime.DispatchEvents(waitQuota);

        auto request = MakeHolder<TSchemeCacheNavigate>();
        request->ResultSet.emplace_back();
        FillOkPath(request->ResultSet.front(), RlAndMeteringAttrs());
        env.Runtime.Send(new IEventHandle(
            id,
            env.EdgeId,
            new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request))), 0, true);

        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::UnknownError);
        UNIT_ASSERT(ev->Message.Contains("Unhandled exception"));
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(PoisonWhileWaitingNavigateDoesNotReply) {
        THoldingQuoterEnv env;
        bool replied = false;
        auto observer = env.Runtime.AddObserver<TEvChargeRequestUnitsResponse>([&](auto&) {
            replied = true;
        });
        auto id = env.Start(MakeSettings(2));
        env.Runtime.Send(new IEventHandle(id, env.EdgeId, new TEvents::TEvPoison), 0, true);
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return env.Cache->RequestCount > 0;
        };
        env.Runtime.DispatchEvents(options);
        UNIT_ASSERT(!replied);
        UNIT_ASSERT_VALUES_EQUAL(env.Cache->RequestCount, 1u);
        Y_UNUSED(observer);
    }

    Y_UNIT_TEST(NavigateNullRequestSkipsQuotaAndBill) {
        THoldingQuoterEnv env;
        auto id = env.Start(MakeSettings(2));
        env.Runtime.Send(new IEventHandle(
            id,
            env.EdgeId,
            new TEvTxProxySchemeCache::TEvNavigateKeySetResult(TAutoPtr<TSchemeCacheNavigate>())), 0, true);
        auto ev = env.WaitResponse();
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Status, EStatus::Success);
        UNIT_ASSERT(env.Bills.empty());
    }

    Y_UNIT_TEST(TEvChargeRequestUnitsResponseDefaultCtor) {
        TEvChargeRequestUnitsResponse ev;
        UNIT_ASSERT_VALUES_EQUAL(ev.Status, EStatus::Success);
        UNIT_ASSERT(ev.Message.empty());
    }
}

} // namespace NKikimr::NPQ
