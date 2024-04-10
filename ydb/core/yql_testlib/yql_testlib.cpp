#include "yql_testlib.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/keyvalue/keyvalue.h>

using namespace NKikimr::NUdf;

namespace {

class TRepeat: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Repeat");
        return name;
    }

private:
    TUnboxedValue Run(
            const NKikimr::NUdf::IValueBuilder* valueBuilder,
            const NKikimr::NUdf::TUnboxedValuePod* args) const override
    {
        TString orig(args[0].AsStringRef());
        ui64 times = args[1].Get<ui64>();
        TString res = "";
        for (ui64 i = 0; i < times; i++) {
            res += orig;
        }
        return valueBuilder->NewString(res);
    }
};


class TSendPoisonPill: public TBoxedValue
{
    mutable NKikimr::TTestActorRuntime* Runtime;
    NThreading::TFuture<void> ResumeYqlExecution;

public:
    TSendPoisonPill(NKikimr::TTestActorRuntime* runtime, const NThreading::TFuture<void> resumeYqlExecution)
        : Runtime(runtime)
        , ResumeYqlExecution(resumeYqlExecution) {
    }

    static TStringRef Name() {
        static auto name = TStringRef::Of("SendPoisonPill");
        return name;
    }

private:
    NKikimr::NUdf::TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        YQL_ENSURE(Runtime->IsRealThreads());

        ui64 tabletID = args[0].Get<ui64>();

        auto sender = Runtime->AllocateEdgeActor();

        Runtime->SendToPipe(tabletID, sender, new NKikimr::TEvents::TEvPoisonPill);

        ResumeYqlExecution.Wait();

        ui64 result = 0;
        return TUnboxedValuePod(result);
    }
};

class TTestUDFs: public IUdfModule
{
    mutable NKikimr::TTestActorRuntime* Runtime;
    NThreading::TFuture<void> ResumeYqlExecution;

public:
    TTestUDFs(NKikimr::TTestActorRuntime* runtime, const NThreading::TFuture<void>& resumeYqlExecution)
        : Runtime(runtime)
        , ResumeYqlExecution(resumeYqlExecution) {
    }

    TStringRef Name() const {
        return TStringRef::Of("TestUDFs");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TRepeat::Name());
        sink.Add(TSendPoisonPill::Name());
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            Y_UNUSED(userType);
            Y_UNUSED(typeConfig);

            bool typesOnly = (flags & TFlags::TypesOnly);

            if (TRepeat::Name() == name) {

                builder.SimpleSignature<char*(char*, ui64)>();

                if (!typesOnly) {
                    builder.Implementation(new TRepeat);
                }
            }
            else if (TSendPoisonPill::Name() == name) {
                builder.SimpleSignature<ui64(ui64)>();

                if (!typesOnly) {
                    builder.Implementation(new TSendPoisonPill(Runtime, ResumeYqlExecution));
                }
            }
        } catch (...) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // unnamed namespace


namespace NKikimr {

namespace Tests {

void TYqlServer::Initialize() {
    ResumeYqlExecutionPromise = NThreading::NewPromise<void>();

    Runtime.Reset(new TTestActorRuntime(StaticNodes() + DynamicNodes(), true));

    Runtime->SetupMonitoring();
    Runtime->SetLogBackend(GetSettings().LogBackend);

    TAppPrepare app;

    SetupDomains(app);
    SetupChannelProfiles(app);

    app.AddHive(ChangeStateStorage(Hive, Settings->Domain));
    app.SetFnRegistry([this](const NKikimr::NScheme::TTypeRegistry& typeRegistry) -> NKikimr::NMiniKQL::IFunctionRegistry* {
            Y_UNUSED(typeRegistry);
            // register test UDFs
            auto freg = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();
            freg->AddModule("", "TestUDFs", new TTestUDFs(GetRuntime(), ResumeYqlExecutionPromise.GetFuture()));
            return freg.Release();
        }
    );

    SetupMessageBus(GetSettings().Port);

    SetupTabletServices(*Runtime, &app, StaticNodes() == 1 && Settings->EnableMockOnSingleNode, Settings->CustomDiskParams);

    CreateBootstrapTablets();
    SetupStorage();

    for (ui32 nodeIdx = 0; nodeIdx < GetSettings().NodeCount; ++nodeIdx) {
        SetupDomainLocalService(nodeIdx);
        SetupProxies(nodeIdx);
    }
    SetupLogging();
}


void MakeGatewaysConfig(const THashMap<TString, TString>& clusterMapping, NYql::TGatewaysConfig& gatewaysConfig) {
    for (auto& x : clusterMapping) {
        if (x.second == NYql::YtProviderName) {
            auto cluster = gatewaysConfig.MutableYt()->AddClusterMapping();
            cluster->SetName(x.first);
        }
        else if (x.second == NYql::KikimrProviderName) {
            auto cluster = gatewaysConfig.MutableKikimr()->AddClusterMapping();
            cluster->SetName(x.first);
        }
        else {
            ythrow yexception() << "Unknown system: " << x.second << " for cluster " << x.first;
        }
    }
}

void TYqlServer::ResumeYqlExecutionActor() {
    ResumeYqlExecutionPromise.SetValue();
}

} // namespace NTests

} // namespace NKikimr
