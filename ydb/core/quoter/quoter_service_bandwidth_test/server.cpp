#include "server.h"
#include "quota_requester.h"
#include <ydb/core/quoter/quoter_service.h>
#include <ydb/core/quoter/quoter_service_impl.h>

#include <ydb/core/kesus/tablet/events.h>

#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {

TTestServer::TTestServer(const TOptions &opts)
    : Opts(opts)
    , MsgBusPort(PortManager.GetPort())
    , ServerSettings(MakeIntrusive<Tests::TServerSettings>(MsgBusPort))
{
    SetupSettings();
    RunServer();
}

std::pair<TString, TString> TTestServer::GetKesusPathAndName(size_t i) {
    return {Tests::TestDomainName, TStringBuilder() << "Kesus_" << i};
}

TString TTestServer::GetKesusPath(size_t i) {
    return TStringBuilder() << "/" << Tests::TestDomainName << "/Kesus_" << i;
}

TString TTestServer::GetKesusRootResource() {
    return "Root";
}

TString TTestServer::GetKesusResource(size_t i) {
    return TStringBuilder() << GetKesusRootResource() << "/Resource_" << i;
}

void TTestServer::SetupSettings() {
    (*ServerSettings)
        .SetUseRealThreads(true);
}

void TTestServer::RunServer() {
    Server = MakeIntrusive<Tests::TServer>(ServerSettings, true);
    Client = MakeHolder<Tests::TClient>(*ServerSettings);

    Server->GetRuntime()->SetDispatchTimeout(TDuration::Minutes(10));

    Client->InitRootScheme();

    RegisterQuoterService();
    CreateKesusesAndResources();
}

void TTestServer::RunQuotaRequesters(TRequestStats& stats) {
    TTestActorRuntime* const runtime = Server->GetRuntime();
    const ui32 userPoolId = runtime->GetAppData().UserPoolId;
    //Cerr << "User pool: " << userPoolId << Endl;
    const ui32 nodeIndex = 0;
    size_t requesters = 0;
    for (size_t kesusIndex = 0; kesusIndex < Opts.KesusCount; ++kesusIndex) {
        for (size_t resIndex = 0; resIndex < Opts.ResourcePerKesus; ++resIndex) {
            runtime->Register(new TKesusQuotaRequester(Opts, stats, GetEdgeActor(), kesusIndex, resIndex), nodeIndex, userPoolId);
            ++requesters;
        }
    }
    for (size_t localResIndex = 0; localResIndex < Opts.LocalResourceCount; ++localResIndex) {
        runtime->Register(new TLocalResourceQuotaRequester(Opts, stats, GetEdgeActor(), localResIndex), nodeIndex, userPoolId);
        ++requesters;
    }

    while (requesters) {
        runtime->GrabEdgeEvent<TEvents::TEvWakeup>(GetEdgeActor())->Release();
        --requesters;
    }
}

void TTestServer::RegisterQuoterService() {
    TTestActorRuntime* const runtime = Server->GetRuntime();
    const ui32 systemPoolId = runtime->GetAppData().SystemPoolId;
    //Cerr << "System pool: " << systemPoolId << Endl;
    const ui32 nodeIndex = 0;
    const TActorId quoterServiceActorId = runtime->Register(CreateQuoterService(), nodeIndex, systemPoolId);
    runtime->RegisterService(MakeQuoterServiceID(), quoterServiceActorId);
}

void TTestServer::CreateKesusesAndResources() {
    for (size_t i = 0; i < Opts.KesusCount; ++i) {
        auto [parent, name] = GetKesusPathAndName(i);
        const NMsgBusProxy::EResponseStatus status = Client->CreateKesus(parent, name);
        Y_ABORT_UNLESS(status == NMsgBusProxy::MSTATUS_OK);

        // Create resources
        const ui64 tabletId = GetKesusTabletId(GetKesusPath(i));
        CreateKesusResource(tabletId, GetKesusRootResource(), static_cast<double>(Opts.RootResourceSpeedLimit));
        for (size_t r = 0; r < Opts.ResourcePerKesus; ++r) {
            CreateKesusResource(tabletId, GetKesusResource(r));
        }
    }
}

ui64 TTestServer::GetKesusTabletId(const TString& path) {
    TAutoPtr<NMsgBusProxy::TBusResponse> resp = Client->Ls(path);
    Y_ABORT_UNLESS(resp->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS);
    const auto& pathDesc = resp->Record.GetPathDescription();
    Y_ABORT_UNLESS(pathDesc.HasKesus());
    const ui64 tabletId = pathDesc.GetKesus().GetKesusTabletId();
    Y_ABORT_UNLESS(tabletId);
    return tabletId;
}

void TTestServer::CreateKesusResource(ui64 kesusTabletId, const TString& resourcePath, TMaybe<double> maxUnitsPerSecond) {
    TTestActorRuntime* const runtime = Server->GetRuntime();

    TAutoPtr<NKesus::TEvKesus::TEvAddQuoterResource> request(new NKesus::TEvKesus::TEvAddQuoterResource());
    request->Record.MutableResource()->SetResourcePath(resourcePath);
    auto* hdrrConfig = request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig(); // Create HDRR config
    if (maxUnitsPerSecond) {
        hdrrConfig->SetMaxUnitsPerSecond(*maxUnitsPerSecond);
    }

    TActorId sender = GetEdgeActor();
    ForwardToTablet(*runtime, kesusTabletId, sender, request.Release(), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvAddQuoterResourceResult>(handle);
    const NKikimrKesus::TEvAddQuoterResourceResult& record = handle->Get<NKesus::TEvKesus::TEvAddQuoterResourceResult>()->Record;
    Y_ABORT_UNLESS(record.GetError().GetStatus() == Ydb::StatusIds::SUCCESS);
}

void TTestServer::CreateKesusResource(const TString& kesusPath, const TString& resourcePath, TMaybe<double> maxUnitsPerSecond) {
    CreateKesusResource(GetKesusTabletId(kesusPath), resourcePath, maxUnitsPerSecond);
}

TActorId TTestServer::GetEdgeActor() {
    if (!EdgeActor) {
        EdgeActor = Server->GetRuntime()->AllocateEdgeActor(0);
    }
    return EdgeActor;
}


} // namespace NKikimr
