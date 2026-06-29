#include "events.h"
#include "nbs_dbg_like_load_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

#include <cinttypes>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/string/strip.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NNbsDbgLike {

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTabletHttp] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTabletHttp] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTabletHttp] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTabletHttp] " << stream)

namespace {

constexpr ui64 kLoadOwner = 0xB1610AD;
constexpr TDuration kHelperBaseTimeout = TDuration::Seconds(60);
constexpr ui32 kPipeRetryLimit = 3;
constexpr ui32 kDefaultChannelCount = 3;

std::vector<TString> SplitStoragePools(const TString& text) {
    std::vector<TString> out;
    TStringBuf rest(text);
    while (rest) {
        TStringBuf token = rest.NextTok('\n');
        token = StripString(token);
        if (token) {
            out.emplace_back(token);
        }
    }
    return out;
}

TString NbsTabletHtmlEscape(TStringBuf in) {
    TString out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '&':  out += "&amp;"; break;
            case '<':  out += "&lt;"; break;
            case '>':  out += "&gt;"; break;
            case '"':  out += "&quot;"; break;
            case '\'': out += "&#39;"; break;
            default:   out += c;
        }
    }
    return out;
}

struct TNbsHiveListAccumRow {
    NKikimrHive::TTabletInfo Hive;
    TString PoolCell = "-";
    TString NumDbgCell = "-";
};

class TNbsLoadTabletListPageActor : public TActorBootstrapped<TNbsLoadTabletListPageActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE_TABLET;
    }

    TNbsLoadTabletListPageActor(TActorId parent, ui32 httpRequestId)
        : Parent(std::move(parent))
        , HttpRequestId(httpRequestId)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(45), new TEvents::TEvWakeup);
        ResolveTenantDomainAndProceed();
    }

private:
    bool IsRootDomain(const TString& databaseName) const {
        const auto* domainsInfo = AppData()->DomainsInfo.Get();
        if (!domainsInfo || !domainsInfo->Domain) {
            return false;
        }
        const TString rootDomainName = "/" + domainsInfo->Domain->Name;
        return databaseName == rootDomainName || databaseName == domainsInfo->Domain->Name;
    }

    void ResolveTenantDomainAndProceed() {
        const auto* domainsInfo = AppData()->DomainsInfo.Get();
        if (!domainsInfo || !domainsInfo->Domain) {
            return FinishError("domain info is unavailable");
        }
        TenantName = AppData()->TenantName;
        if (TenantName.empty() || IsRootDomain(TenantName)) {
            if (!domainsInfo->HiveTabletId) {
                return FinishError("root domain does not have Hive configured");
            }
            HiveTabletId = *domainsInfo->HiveTabletId;
            DomainSchemeShard = domainsInfo->Domain->SchemeRoot;
            DomainPathId = 1;
            FilterByObjectDomain = false;
            OpenHivePipe();
            return;
        }
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = TenantName;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = SplitPath(TenantName);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void OpenHivePipe() {
        NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}};
        HivePipe = Register(NTabletPipe::CreateClient(SelfId(), HiveTabletId, pipeConfig));
    }

    void SendHiveListRequest() {
        auto req = std::make_unique<TEvHive::TEvRequestHiveInfo>();
        auto& rec = req->Record;
        rec.SetTabletType(NKikimrTabletBase::TTabletTypes::NbsLoadTablet);
        if (FilterByObjectDomain) {
            rec.MutableFilterTabletsByObjectDomain()->SetSchemeShard(DomainSchemeShard);
            rec.MutableFilterTabletsByObjectDomain()->SetPathId(DomainPathId);
        }
        NTabletPipe::SendData(SelfId(), HivePipe, req.release());
    }

    static TString BuildStateLabel(const NKikimrHive::TTabletInfo& t) {
        TStringStream s;
        s << NHive::ETabletStateName(static_cast<NHive::ETabletState>(t.GetState()))
          << " / " << NKikimrHive::ETabletVolatileState_Name(t.GetVolatileState());
        return s.Str();
    }

    static bool IsLeaderRow(const NKikimrHive::TTabletInfo& t) {
        return !t.HasFollowerID() || t.GetFollowerID() == 0;
    }

    static bool ShouldQuerySummary(const NKikimrHive::TTabletInfo& t) {
        const auto v = t.GetVolatileState();
        if (v == NKikimrHive::TABLET_VOLATILE_STATE_RUNNING
            || v == NKikimrHive::TABLET_VOLATILE_STATE_STARTING) {
            return true;
        }
        if (v == NKikimrHive::TABLET_VOLATILE_STATE_UNKNOWN && t.GetNodeID() != 0) {
            return true;
        }
        return false;
    }

    void FinishError(const TString& msg) {
        TStringStream html;
        html << "<div class='alert alert-danger'>" << NbsTabletHtmlEscape(msg) << "</div>";
        SendDone(html.Str());
    }

    void SendDone(const TString& html) {
        if (HivePipe) {
            NTabletPipe::CloseClient(SelfId(), HivePipe);
            HivePipe = {};
        }
        if (SummaryPipe) {
            NTabletPipe::CloseClient(SelfId(), SummaryPipe);
            SummaryPipe = {};
        }
        auto ev = std::make_unique<TEvLoad::TEvNbsTabletListPageReady>();
        ev->HttpRequestId = HttpRequestId;
        ev->HtmlFragment = html;
        Send(Parent, ev.release());
        PassAway();
    }

    void FinalizeTableHtml() {
        Sort(Accum.begin(), Accum.end(), [](const TNbsHiveListAccumRow& a, const TNbsHiveListAccumRow& b) {
            const ui64 oa = a.Hive.GetTabletOwner().GetOwnerIdx();
            const ui64 ob = b.Hive.GetTabletOwner().GetOwnerIdx();
            return oa < ob;
        });

        TStringStream str;
        str << "<div id='nbs-tablet-list-container'>";
        str << "<div class='panel panel-default' style='margin-top:16px'>";
        str << "<div class='panel-heading'><strong>NbsLoad tablets (from Hive)</strong></div>";
        str << "<div class='panel-body'>";
        if (Accum.empty()) {
            str << "<p class='text-muted'>No NbsLoadTablet records in this Hive scope.</p>";
        } else {
            str << "<table class='table table-condensed table-bordered'>";
            str << "<thead><tr>"
                   "<th>TabletId</th><th>Owner index</th><th>NodeId</th><th>TabletState</th>"
                   "<th>PoolName</th><th>NumDirectBlockGroups</th><th>Actions</th>"
                   "</tr></thead><tbody>";
            for (const auto& row : Accum) {
                const ui64 tid = row.Hive.GetTabletID();
                const ui64 ownerIdx = row.Hive.HasTabletOwner()
                    ? row.Hive.GetTabletOwner().GetOwnerIdx()
                    : 0;
                const ui32 nodeId = row.Hive.GetNodeID();
                const TString stateLabel = BuildStateLabel(row.Hive);
                str << "<tr>";
                str << "<td><a href='/tablets?TabletID=" << tid << "'>" << tid << "</a></td>";
                str << "<td>" << ownerIdx << "</td>";
                str << "<td>" << nodeId << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(stateLabel) << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(row.PoolCell) << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(row.NumDbgCell) << "</td>";
                str << "<td>";
                str << "<button type='button' class='btn btn-xs btn-primary' "
                       "onClick='nbsTabletPrepareRun(\"" << tid << "\"," << nodeId << ")'>Run</button> ";
                str << "<button type='button' class='btn btn-xs btn-danger' "
                       "onClick='nbsTabletDeleteOwner(" << ownerIdx << ")'>Delete</button>";
                str << "</td>";
                str << "</tr>";
            }
            str << "</tbody></table>";
            str << "<script>(function(){"
                   "var rows=[";
            bool firstRow = true;
            for (const auto& row : Accum) {
                const ui64 tid = row.Hive.GetTabletID();
                const ui64 ownerIdx = row.Hive.HasTabletOwner()
                    ? row.Hive.GetTabletOwner().GetOwnerIdx()
                    : 0;
                const ui32 nodeId = row.Hive.GetNodeID();
                if (!firstRow) {
                    str << ",";
                }
                firstRow = false;
                str << "{tid:\"" << tid << "\",oi:" << ownerIdx << ",nid:" << nodeId << "}";
            }
            str << "];"
                   "var maxOi=0;"
                   "window.nbsTabletNodeMap=window.nbsTabletNodeMap||{};"
                   "rows.forEach(function(r){"
                   "if(r.oi>maxOi)maxOi=r.oi;"
                   "window.nbsTabletNodeMap[String(r.tid)]=r.nid;"
                   "});"
                   "var ownerInput=document.getElementById('nbs-tablet-owner-idx');"
                   "if(ownerInput)ownerInput.value=maxOi+1;"
                   "})();</script>";
        }
        str << "</div></div>";
        str << "</div>"; // nbs-tablet-list-container
        SendDone(str.Str());
    }

    void ProceedSummary() {
        while (SummaryIndex < Accum.size()) {
            if (!ShouldQuerySummary(Accum[SummaryIndex].Hive)) {
                ++SummaryIndex;
                continue;
            }
            SummaryTabletId = Accum[SummaryIndex].Hive.GetTabletID();
            NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}};
            SummaryPipe = Register(NTabletPipe::CreateClient(SelfId(), SummaryTabletId, pipeConfig));
            return;
        }
        FinalizeTableHtml();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate)
        hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed)
        hFunc(TEvHive::TEvResponseHiveInfo, HandleHiveInfo)
        hFunc(TEvLoad::TEvNbsLoadTabletGetSummaryResult, HandleSummaryResult)
        hFunc(TEvents::TEvWakeup, HandleTimeout)
    )

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        if (request->ResultSet.empty()) {
            return FinishError("failed to resolve tenant path");
        }
        const auto& entry = request->ResultSet.front();
        if (request->ErrorCount > 0) {
            return FinishError("failed to resolve tenant path (scheme cache error)");
        }
        auto domainInfo = entry.DomainInfo;
        if (!domainInfo || !domainInfo->Params.HasHive()) {
            return FinishError("resolved tenant does not have Hive configured");
        }
        HiveTabletId = domainInfo->Params.GetHive();
        DomainSchemeShard = domainInfo->DomainKey.OwnerId;
        DomainPathId = domainInfo->DomainKey.LocalPathId;
        FilterByObjectDomain = true;
        OpenHivePipe();
    }

    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Sender == HivePipe) {
            if (ev->Get()->Status != NKikimrProto::OK) {
                return FinishError("hive pipe connect failed");
            }
            SendHiveListRequest();
            return;
        }
        if (ev->Sender == SummaryPipe) {
            if (ev->Get()->Status != NKikimrProto::OK) {
                NTabletPipe::CloseClient(SelfId(), SummaryPipe);
                SummaryPipe = {};
                ++SummaryIndex;
                ProceedSummary();
                return;
            }
            auto req = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummary>();
            NTabletPipe::SendData(SelfId(), SummaryPipe, req.release());
            return;
        }
    }

    void HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Sender == HivePipe) {
            return FinishError("hive pipe lost");
        }
        if (ev->Sender == SummaryPipe) {
            SummaryPipe = {};
            ++SummaryIndex;
            ProceedSummary();
        }
    }

    void HandleHiveInfo(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.HasForwardRequest()) {
            return FinishError("Hive forwarded RequestHiveInfo (unexpected for load list)");
        }
        Accum.clear();
        for (const auto& t : rec.GetTablets()) {
            if (!IsLeaderRow(t)) {
                continue;
            }
            if (t.GetTabletType() != NKikimrTabletBase::TTabletTypes::NbsLoadTablet) {
                continue;
            }
            TNbsHiveListAccumRow row;
            row.Hive = t;
            Accum.push_back(std::move(row));
        }
        SummaryIndex = 0;
        ProceedSummary();
    }

    void HandleSummaryResult(TEvLoad::TEvNbsLoadTabletGetSummaryResult::TPtr& ev) {
        const auto& r = ev->Get()->Record;
        if (SummaryIndex < Accum.size() && Accum[SummaryIndex].Hive.GetTabletID() == SummaryTabletId) {
            if (r.GetStatus() == NBSLT_OK) {
                TStringStream pools;
                pools << r.GetDDiskPoolName();
                if (r.HasPersistentBufferDDiskPoolName()
                    && r.GetPersistentBufferDDiskPoolName() != r.GetDDiskPoolName()) {
                    pools << " / " << r.GetPersistentBufferDDiskPoolName();
                }
                Accum[SummaryIndex].PoolCell = pools.Str();
                Accum[SummaryIndex].NumDbgCell = ToString(r.GetNumDirectBlockGroups());
            }
        }
        if (SummaryPipe) {
            NTabletPipe::CloseClient(SelfId(), SummaryPipe);
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        FinishError("tablet list request timed out");
    }

    TActorId Parent;
    ui32 HttpRequestId = 0;

    TString TenantName;
    ui64 HiveTabletId = 0;
    ui64 DomainSchemeShard = 0;
    ui64 DomainPathId = 0;
    bool FilterByObjectDomain = false;

    TActorId HivePipe;
    TActorId SummaryPipe;
    std::vector<TNbsHiveListAccumRow> Accum;
    ui32 SummaryIndex = 0;
    ui64 SummaryTabletId = 0;
};

// Helper actor that runs the Hive→Tablet round-trip for a single
// tablet_create / tablet_delete HTTP POST and replies with
// TEvHttpInfoRes when done.
//
// Wire flows (Hive replies to TEvLookupTablet with TEvCreateTabletReply, see
// hive_impl.cpp:2236-2245):
//   Create:
//     self -> SchemeCache: resolve current tenant (AppData()->TenantName)
//     self -> Hive: TEvCreateTablet{Owner, OwnerIdx, NbsLoadTablet, bindings, AllowedDomains}
//     Hive -> self: TEvCreateTabletReply{tabletId, status=OK|ALREADY}
//     Hive -> self: TEvTabletCreationResult{status=OK} when tablet up
//     self -> Tablet: pipe + TEvNbsLoadTabletAllocateGroups{AllocConfig}
//     Tablet -> self: TEvNbsLoadTabletAllocateGroupsResult{status}
//     self -> origin: HTTP body
//   Delete:
//     self -> Hive: TEvLookupTablet{Owner, OwnerIdx}
//     Hive -> self: TEvCreateTabletReply{OK, tabletId} (or NODATA -> 404)
//     self -> Tablet: pipe + TEvNbsLoadTabletDelete
//     Tablet -> self: TEvNbsLoadTabletDeleteResult
//     self -> Hive: TEvDeleteTablet
//     Hive -> self: TEvDeleteTabletReply
//     self -> origin: HTTP body
class TNbsLoadTabletRequestActor : public TActorBootstrapped<TNbsLoadTabletRequestActor> {
public:
    using EOp = ENbsLoadTabletOp;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE_TABLET;
    }

    TNbsLoadTabletRequestActor(EOp op, ui64 ownerIdx, TString configText,
        TActorId origin, ui32 subRequestId, TString storagePoolsText)
        : Op(op)
        , OwnerIdx(ownerIdx)
        , ConfigText(std::move(configText))
        , Origin(origin)
        , SubRequestId(subRequestId)
        , StoragePoolNames(SplitStoragePools(storagePoolsText))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        const TDuration timeout = kHelperBaseTimeout;
        Schedule(timeout, new TEvents::TEvWakeup);
        LOG_I("Bootstrap Op# " << OpName(Op)
            << " OwnerIdx# " << OwnerIdx
            << " Timeout# " << timeout
            << " Origin# " << Origin);
        ResolveTenantDomainAndProceed();
    }

private:
    bool IsRootDomain(const TString& databaseName) const {
        const auto* domainsInfo = AppData()->DomainsInfo.Get();
        if (!domainsInfo || !domainsInfo->Domain) {
            return false;
        }
        const TString rootDomainName = "/" + domainsInfo->Domain->Name;
        return databaseName == rootDomainName || databaseName == domainsInfo->Domain->Name;
    }

    void ResolveTenantDomainAndProceed() {
        const auto* domainsInfo = AppData()->DomainsInfo.Get();
        if (!domainsInfo || !domainsInfo->Domain) {
            return ReplyError(500, "domain info is unavailable");
        }

        TenantName = AppData()->TenantName;
        if (TenantName.empty() || IsRootDomain(TenantName)) {
            if (!domainsInfo->HiveTabletId) {
                return ReplyError(409, "root domain does not have Hive configured");
            }
            HiveTabletId = *domainsInfo->HiveTabletId;
            DomainSchemeShard = domainsInfo->Domain->SchemeRoot;
            DomainPathId = 1;
            TenantStoragePools.clear();
            TenantStoragePools.reserve(domainsInfo->Domain->StoragePoolTypes.size());
            for (const auto& [poolName, _] : domainsInfo->Domain->StoragePoolTypes) {
                TenantStoragePools.push_back(poolName);
            }
            LOG_D("Resolved root domain for Op# " << OpName(Op)
                << " Tenant# " << (TenantName.empty() ? "<root>" : TenantName)
                << " HiveTabletId# " << HiveTabletId
                << " Domain# " << DomainSchemeShard << ":" << DomainPathId);
            OpenHivePipeAndSendRequest();
            return;
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = TenantName;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = SplitPath(TenantName);

        LOG_D("Resolving tenant domain Op# " << OpName(Op)
            << " Tenant# " << TenantName);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        if (request->ResultSet.empty()) {
            return ReplyError(404, "failed to resolve tenant path");
        }

        const auto& entry = request->ResultSet.front();
        if (request->ErrorCount > 0) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    return ReplyError(403, "access denied to tenant path");
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    return ReplyError(404, "tenant path does not exist");
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    return ReplyError(503, "failed to lookup tenant path");
                default:
                    return ReplyError(500, "failed to resolve tenant path");
            }
        }

        auto domainInfo = entry.DomainInfo;
        if (!domainInfo || !domainInfo->Params.HasHive()) {
            return ReplyError(500, "resolved tenant does not have Hive configured");
        }

        HiveTabletId = domainInfo->Params.GetHive();
        DomainSchemeShard = domainInfo->DomainKey.OwnerId;
        DomainPathId = domainInfo->DomainKey.LocalPathId;
        TenantStoragePools.clear();
        if (entry.DomainDescription && entry.DomainDescription->Description.StoragePoolsSize() > 0) {
            TenantStoragePools.reserve(entry.DomainDescription->Description.StoragePoolsSize());
            for (const auto& pool : entry.DomainDescription->Description.GetStoragePools()) {
                TenantStoragePools.push_back(pool.GetName());
            }
        }
        LOG_D("Resolved tenant domain for Op# " << OpName(Op)
            << " Tenant# " << TenantName
            << " HiveTabletId# " << HiveTabletId
            << " Domain# " << DomainSchemeShard << ":" << DomainPathId);
        OpenHivePipeAndSendRequest();
    }

    void OpenHivePipeAndSendRequest() {
        NTabletPipe::TClientConfig pipeConfig{
            .RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}};
        HivePipe = Register(NTabletPipe::CreateClient(SelfId(), HiveTabletId, pipeConfig));
        LOG_D("Opened Hive pipe Op# " << OpName(Op)
            << " Tenant# " << (TenantName.empty() ? "<root>" : TenantName)
            << " HiveTabletId# " << HiveTabletId
            << " HivePipe# " << HivePipe);

        switch (Op) {
            case EOp::Create:
                SendCreate();
                break;
            case EOp::Delete:
                SendLookup();
                break;
        }
    }

    void SendCreate() {
        LOG_D("Send create to Hive OwnerIdx# " << OwnerIdx
            << " Tenant# " << (TenantName.empty() ? "<root>" : TenantName)
            << " Domain# " << DomainSchemeShard << ":" << DomainPathId);
        auto req = std::make_unique<TEvHive::TEvCreateTablet>();
        auto& rec = req->Record;
        rec.SetOwner(kLoadOwner);
        rec.SetOwnerIdx(OwnerIdx);
        rec.SetTabletType(NKikimrTabletBase::TTabletTypes::NbsLoadTablet);
        rec.SetChannelsProfile(0);
        auto* domain = rec.AddAllowedDomains();
        domain->SetSchemeShard(DomainSchemeShard);
        domain->SetPathId(DomainPathId);
        std::vector<TString> effectivePoolNames = StoragePoolNames;
        if (effectivePoolNames.empty() && !TenantStoragePools.empty()) {
            for (ui32 i = 0; i < kDefaultChannelCount; ++i) {
                effectivePoolNames.push_back(TenantStoragePools[i % TenantStoragePools.size()]);
            }
            LOG_D("Using tenant storage pools for create OwnerIdx# " << OwnerIdx
                << " Pools# " << JoinSeq(",", effectivePoolNames));
        }
        if (effectivePoolNames.empty()) {
            // Last-resort fallback: let Hive pick default bindings.
            rec.AddBindedChannels()->SetStoragePoolName("");
        } else {
            for (const auto& name : effectivePoolNames) {
                rec.AddBindedChannels()->SetStoragePoolName(name);
            }
        }
        WaitingForTabletCreation = true;
        NTabletPipe::SendData(SelfId(), HivePipe, req.release());
    }

    void SendLookup() {
        LOG_D("Send lookup to Hive Op# " << OpName(Op)
            << " OwnerIdx# " << OwnerIdx
            << " Tenant# " << (TenantName.empty() ? "<root>" : TenantName)
            << " Domain# " << DomainSchemeShard << ":" << DomainPathId);
        auto req = std::make_unique<TEvHive::TEvLookupTablet>();
        auto& rec = req->Record;
        rec.SetOwner(kLoadOwner);
        rec.SetOwnerIdx(OwnerIdx);
        NTabletPipe::SendData(SelfId(), HivePipe, req.release());
    }

    void Handle(TEvHive::TEvCreateTabletReply::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        const auto status = rec.GetStatus();
        LOG_D("Hive reply Op# " << OpName(Op)
            << " Status# " << NKikimrProto::EReplyStatus_Name(status)
            << " TabletId# " << rec.GetTabletID());

        // For Lookup (Run/Delete), Hive replies NODATA when the OwnerIdx is
        // unknown (hive_impl.cpp:2236-2245).
        if (Op != EOp::Create && status == NKikimrProto::NODATA) {
            return ReplyError(404, "tablet not found in Hive");
        }
        if (status != NKikimrProto::OK && status != NKikimrProto::ALREADY) {
            return ReplyError(500, TStringBuilder() << "Hive replied "
                << NKikimrProto::EReplyStatus_Name(status));
        }
        TabletId = rec.GetTabletID();

        if (Op == EOp::Create) {
            if (status == NKikimrProto::ALREADY) {
                // Tablet was previously created by Hive but we don't know whether
                // its TEvNbsLoadTabletAllocateGroups succeeded. Bypass TEvTabletCreationResult
                // (Hive only sends that on first boot) and try the idempotent
                // tablet handler directly. Phase 1.2 spec §23.10 case 1.
                WaitingForTabletCreation = false;
                LOG_N("Tablet already exists OwnerIdx# " << OwnerIdx
                    << " TabletId# " << TabletId);
                OpenTabletPipe();
                return;
            }
            // In practice Hive can reply OK without sending TEvTabletCreationResult
            // back to this helper actor (e.g. routing nuances), so proceed
            // immediately with the idempotent tablet Create request.
            WaitingForTabletCreation = false;
            LOG_D("Hive create OK, opening tablet pipe immediately TabletId# " << TabletId);
            OpenTabletPipe();
            return;
        }

        // Delete: lookup succeeded, the tablet exists; open the pipe.
        OpenTabletPipe();
    }

    void Handle(TEvHive::TEvTabletCreationResult::TPtr& ev) {
        if (Op != EOp::Create || !WaitingForTabletCreation) {
            LOG_D("Ignoring late tablet creation result Op# " << OpName(Op)
                << " WaitingForTabletCreation# " << (WaitingForTabletCreation ? "true" : "false"));
            return;
        }
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NKikimrProto::OK) {
            return ReplyError(500, TStringBuilder() << "Tablet creation failed: "
                << NKikimrProto::EReplyStatus_Name(rec.GetStatus()));
        }
        WaitingForTabletCreation = false;
        LOG_N("Tablet created successfully OwnerIdx# " << OwnerIdx
            << " TabletId# " << TabletId);
        OpenTabletPipe();
    }

    void OpenTabletPipe() {
        NTabletPipe::TClientConfig pipeConfig{
            .RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}};
        TabletPipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId, pipeConfig));
        LOG_D("Opened tablet pipe Op# " << OpName(Op)
            << " TabletId# " << TabletId
            << " TabletPipe# " << TabletPipe);
        switch (Op) {
            case EOp::Create: {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroups>();
                NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig cfg;
                if (!google::protobuf::TextFormat::ParseFromString(ConfigText, &cfg)) {
                    return ReplyError(400, "failed to parse AllocConfig text-proto");
                }
                *ev->Record.MutableAllocConfig() = std::move(cfg);
                LOG_D("Dispatch tablet create request TabletId# " << TabletId);
                NTabletPipe::SendData(SelfId(), TabletPipe, ev.release());
                break;
            }
            case EOp::Delete: {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletDelete>();
                LOG_D("Dispatch tablet delete request TabletId# " << TabletId);
                NTabletPipe::SendData(SelfId(), TabletPipe, ev.release());
                break;
            }
        }
    }

    void Handle(TEvLoad::TEvNbsLoadTabletAllocateGroupsResult::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() == NBSLT_OK) {
            LOG_N("Tablet create completed TabletId# " << TabletId);
            return ReplyOk("Tablet created");
        }
        if (rec.GetStatus() == NBSLT_ALREADY_INITIALIZED) {
            LOG_N("Tablet already initialized TabletId# " << TabletId);
            return ReplyAlready("Tablet was already initialized");
        }
        ReplyError(409, TStringBuilder()
            << "tablet status " << static_cast<ui32>(rec.GetStatus())
            << " " << rec.GetErrorReason());
    }

    void Handle(TEvLoad::TEvNbsLoadTabletDeleteResult::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NBSLT_OK) {
            return ReplyError(409, TStringBuilder()
                << "tablet status " << static_cast<ui32>(rec.GetStatus())
                << " " << rec.GetErrorReason());
        }
        // Now ask Hive to delete the tablet.
        LOG_D("Tablet delete acknowledged, requesting Hive delete TabletId# " << TabletId);
        auto del = std::make_unique<TEvHive::TEvDeleteTablet>();
        auto& rec2 = del->Record;
        rec2.SetShardOwnerId(kLoadOwner);
        rec2.AddShardLocalIdx(OwnerIdx);
        NTabletPipe::SendData(SelfId(), HivePipe, del.release());
    }

    void Handle(TEvHive::TEvDeleteTabletReply::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NKikimrProto::OK && rec.GetStatus() != NKikimrProto::ALREADY) {
            return ReplyError(500, TStringBuilder() << "Hive delete failed: "
                << NKikimrProto::EReplyStatus_Name(rec.GetStatus()));
        }
        LOG_N("Hive delete completed OwnerIdx# " << OwnerIdx
            << " TabletId# " << TabletId);
        ReplyOk("Tablet deleted");
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ReplyError(503, TStringBuilder() << "pipe to tablet "
                << ev->Get()->TabletId << " failed: "
                << NKikimrProto::EReplyStatus_Name(ev->Get()->Status));
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {}

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        LOG_E("Operation timeout Op# " << OpName(Op)
            << " OwnerIdx# " << OwnerIdx
            << " TabletId# " << TabletId);
        ReplyError(504, "operation timeout");
    }

    static TString FormatBytes(ui64 bytes) {
        constexpr ui64 KiB = 1024;
        constexpr ui64 MiB = 1024 * KiB;
        constexpr ui64 GiB = 1024 * MiB;
        if (bytes >= GiB) return Sprintf("%.2f GiB", static_cast<double>(bytes) / GiB);
        if (bytes >= MiB) return Sprintf("%.2f MiB", static_cast<double>(bytes) / MiB);
        if (bytes >= KiB) return Sprintf("%.2f KiB", static_cast<double>(bytes) / KiB);
        return Sprintf("%" PRIu64 " B", bytes);
    }

    static TString FormatUs(ui64 us) {
        if (us == 0) return TString("-");
        if (us >= 1000000ULL) return Sprintf("%.3f s", us / 1e6);
        if (us >= 1000ULL)    return Sprintf("%.3f ms", us / 1e3);
        return Sprintf("%" PRIu64 " us", us);
    }

    // ----- HTML helpers ---------------------------------------------------

    static TString HtmlEscape(TStringBuf in) {
        TString out;
        out.reserve(in.size());
        for (char c : in) {
            switch (c) {
                case '&':  out += "&amp;"; break;
                case '<':  out += "&lt;"; break;
                case '>':  out += "&gt;"; break;
                case '"':  out += "&quot;"; break;
                case '\'': out += "&#39;"; break;
                default:   out += c;
            }
        }
        return out;
    }

    void ReplyOk(const TString& message) {
        TStringStream html;
        html << "<div class='alert alert-success'>"
             << "<strong>" << HtmlEscape(message) << "</strong>";
        if (TabletId) html << " &nbsp; TabletId=" << TabletId;
        html << "</div>";
        ReplyHtml(200, html.Str());
    }

    void ReplyAlready(const TString& message) {
        TStringStream html;
        html << "<div class='alert alert-warning'>"
             << "<strong>" << HtmlEscape(message) << "</strong>";
        if (TabletId) html << " &nbsp; TabletId=" << TabletId;
        html << "</div>";
        ReplyHtml(409, html.Str());
    }

    void ReplyError(ui32 httpStatus, const TString& msg) {
        LOG_E("Reply error Op# " << OpName(Op)
            << " HttpStatus# " << httpStatus
            << " Reason# " << msg);
        TStringStream html;
        html << "<div class='alert alert-danger'>"
             << "<strong>Error " << httpStatus << "</strong> &nbsp; "
             << HtmlEscape(msg);
        if (TabletId) html << " &nbsp; (TabletId=" << TabletId << ")";
        html << "</div>";
        ReplyHtml(httpStatus, html.Str());
    }

    void ReplyHtml(ui32 httpStatus, const TString& body) {
        TStringStream s;
        if (httpStatus == 200) {
            s << NMonitoring::HTTPOKHTML;
        } else {
            s << "HTTP/1.1 " << httpStatus << " ERR\r\n"
              << "Content-Type: text/html; charset=utf-8\r\n"
              << "Connection: Close\r\n\r\n";
        }
        s << body;
        Send(Origin, new NMon::TEvHttpInfoRes(
            s.Str(), SubRequestId, NMon::IEvHttpInfoRes::EContentType::Custom));
        Cleanup();
    }

    void Cleanup() {
        LOG_D("Cleanup Op# " << OpName(Op)
            << " TabletId# " << TabletId
            << " HivePipe# " << HivePipe
            << " TabletPipe# " << TabletPipe);
        if (HivePipe) {
            NTabletPipe::CloseClient(SelfId(), HivePipe);
            HivePipe = TActorId();
        }
        if (TabletPipe) {
            NTabletPipe::CloseClient(SelfId(), TabletPipe);
            TabletPipe = TActorId();
        }
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
        hFunc(TEvHive::TEvCreateTabletReply, Handle)
        hFunc(TEvHive::TEvTabletCreationResult, Handle)
        hFunc(TEvHive::TEvDeleteTabletReply, Handle)
        hFunc(TEvLoad::TEvNbsLoadTabletAllocateGroupsResult, Handle)
        hFunc(TEvLoad::TEvNbsLoadTabletDeleteResult, Handle)
        hFunc(TEvTabletPipe::TEvClientConnected, Handle)
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
        hFunc(TEvents::TEvWakeup, HandleTimeout)
    )

private:
    static const char* OpName(EOp op) {
        switch (op) {
            case EOp::Create: return "create";
            case EOp::Delete: return "delete";
        }
        return "unknown";
    }

    const EOp Op;
    const ui64 OwnerIdx;
    const TString ConfigText;
    const TActorId Origin;
    const ui32 SubRequestId;
    const std::vector<TString> StoragePoolNames;

    TActorId HivePipe;
    TActorId TabletPipe;
    TString TenantName;
    ui64 HiveTabletId = 0;
    ui64 DomainSchemeShard = 0;
    ui64 DomainPathId = 0;
    std::vector<TString> TenantStoragePools;
    ui64 TabletId = 0;
    bool WaitingForTabletCreation = false;
};

} // anonymous namespace

NActors::IActor* CreateNbsDbgLikeLoadTabletHttpRequest(
    ENbsLoadTabletOp op, ui64 ownerIdx, TString configText,
    NActors::TActorId origin, ui32 subRequestId, TString storagePoolsText)
{
    return new TNbsLoadTabletRequestActor(
        op, ownerIdx, std::move(configText),
        origin, subRequestId, std::move(storagePoolsText));
}

NActors::IActor* CreateNbsLoadTabletListPageActor(
    NActors::TActorId parent, ui32 httpRequestId, ui32 /*subRequestId*/)
{
    return new TNbsLoadTabletListPageActor(std::move(parent), httpRequestId);
}

void RenderTabletForm(IOutputStream& str, const TString& nbsTabletListHtml) {
    // Status messages (validation errors, "Sending..." stubs) use textContent
    // so they are HTML-escaped. The dedicated #nbs-tablet-result div receives
    // the server-rendered HTML response body verbatim.
    str << R"___(
        <script>
            function nbsTabletStatus(text) {
                $("#nbs-tablet-status").text(text);
            }

            function nbsTabletResultClear() {
                $("#nbs-tablet-result").empty();
            }

            function nbsTabletResultHtml(html) {
                $("#nbs-tablet-result").html(html);
            }

            function nbsTabletTrim(text) {
                return String(text || "").trim();
            }

            function nbsTabletEscapeProto(text) {
                return String(text || "")
                    .replace(/\\/g, "\\\\")
                    .replace(/"/g, "\\\"");
            }

            function nbsRunScaleLsns(inst, factor) {
                const inp = $("#nbs-" + inst + "-max-inflight-lsns");
                const v = parseInt(inp.val(), 10);
                if (!isNaN(v) && v >= 1) {
                    inp.val(factor >= 1 ? v * factor : Math.max(1, Math.floor(v * factor)));
                }
            }

            function nbsRunDisableReplicationChanged(inst, cb) {
                const readRatio = $("#nbs-" + inst + "-read-ratio");
                if (cb.checked) {
                    readRatio.val("0").prop("disabled", true);
                } else {
                    readRatio.prop("disabled", false);
                }
            }

            function nbsTabletFieldInt(fieldId, fieldLabel, minValue) {
                const raw = nbsTabletTrim($("#" + fieldId).val());
                if (raw === "") {
                    return { ok: false, error: fieldLabel + " is required" };
                }
                if (!/^[0-9]+$/.test(raw)) {
                    return { ok: false, error: fieldLabel + " must be a non-negative integer" };
                }
                const value = Number(raw);
                if (value < minValue) {
                    return { ok: false, error: fieldLabel + " must be >= " + minValue };
                }
                return { ok: true, value: value };
            }

            function nbsTabletReadPools() {
                const lines = $("#nbs-tablet-create-storage-pools").val().split(/\r?\n/);
                const out = [];
                for (let i = 0; i < lines.length; ++i) {
                    const line = nbsTabletTrim(lines[i]);
                    if (line.length > 0) {
                        out.push(line);
                    }
                }
                return out;
            }

            function nbsTabletBuildAllocConfigProto() {
                // The storage namespace owner (AllocConfig.TabletId) is forced to
                // the load tablet's own TabletID() server-side, so it is not set here.
                const numDbg = nbsTabletFieldInt("nbs-tablet-create-num-dbg", "NumDirectBlockGroups", 1);
                if (!numDbg.ok) {
                    return numDbg;
                }
                const hostsPerDbg = nbsTabletFieldInt("nbs-tablet-create-hosts-per-dbg", "HostsPerDbg", 3);
                if (!hostsPerDbg.ok) {
                    return hostsPerDbg;
                }
                if (hostsPerDbg.value > 5) {
                    return { ok: false, error: "HostsPerDbg must be <= 5" };
                }
                const targetVchunks = nbsTabletFieldInt("nbs-tablet-create-target-vchunks", "TargetNumVChunks", 1);
                if (!targetVchunks.ok) {
                    return targetVchunks;
                }
                const vchunkSize = nbsTabletFieldInt("nbs-tablet-create-vchunk-size", "VChunkSizeBytes", 4096);
                if (!vchunkSize.ok) {
                    return vchunkSize;
                }
                if (vchunkSize.value % 4096 !== 0) {
                    return { ok: false, error: "VChunkSizeBytes must be a multiple of 4096" };
                }

                const ddiskPoolName = nbsTabletTrim($("#nbs-tablet-create-ddisk-pool").val());
                const pbPoolName = nbsTabletTrim($("#nbs-tablet-create-pb-pool").val());
                const nonDefaultOnly = $("#nbs-tablet-create-non-default").is(":checked");
                const pools = nbsTabletReadPools();
                const lines = [];

                function addLine(line) {
                    lines.push(line);
                }

                function addString(name, value, def) {
                    if (!nonDefaultOnly || value !== def) {
                        addLine(name + ': "' + nbsTabletEscapeProto(value) + '"');
                    }
                }

                function addNumber(name, value, def, always) {
                    if (always || !nonDefaultOnly || value !== def) {
                        addLine(name + ": " + value);
                    }
                }

                addString("DDiskPoolName", ddiskPoolName || "ddp1", "ddp1");
                addString("PersistentBufferDDiskPoolName", pbPoolName || "ddp1", "ddp1");
                addNumber("NumDirectBlockGroups", numDbg.value, 1, false);
                addNumber("HostsPerDbg", hostsPerDbg.value, 5, false);
                addNumber("TargetNumVChunks", targetVchunks.value, 1, false);
                addNumber("VChunkSizeBytes", vchunkSize.value, 134217728, false);

                for (let i = 0; i < pools.length; ++i) {
                    addLine('TabletStoragePools: "' + nbsTabletEscapeProto(pools[i]) + '"');
                }

                return {
                    ok: true,
                    proto: lines.join("\n"),
                    storagePoolsText: pools.join("\n")
                };
            }

            function nbsTabletRefreshPreview() {
                const alloc = nbsTabletBuildAllocConfigProto();
                $("#nbs-tablet-create-proto-preview").val(alloc.ok ? alloc.proto : ("# " + alloc.error));
            }

            function nbsTabletPostRaw(mode, owner, cfg, pools, onSuccess) {
                nbsTabletStatus("HTTP request in flight (mode=" + mode + ")...");
                nbsTabletResultClear();
                $.ajax({
                    url: "",
                    data: { mode: mode, owner_idx: owner, config: cfg, storage_pools: pools },
                    method: "POST",
                    contentType: "application/x-protobuf-text",
                    complete: function(xhr) {
                        nbsTabletStatus("HTTP " + xhr.status + " (mode=" + mode + ")");
                        const ct = (xhr.getResponseHeader("Content-Type") || "").toLowerCase();
                        const body = xhr.responseText || "";
                        if (ct.indexOf("text/html") >= 0) {
                            nbsTabletResultHtml(body);
                        } else {
                            // Fallback: render any non-HTML response as a <pre>
                            // block so users still see something readable.
                            const pre = document.createElement("pre");
                            pre.textContent = body;
                            $("#nbs-tablet-result").empty().append(pre);
                        }
                        if (xhr.status >= 200 && xhr.status < 300 && typeof onSuccess === "function") {
                            onSuccess();
                        }
                    }
                });
            }

            function nbsTabletCreate() {
                const owner = nbsTabletTrim($("#nbs-tablet-owner-idx").val());
                if (!/^[0-9]+$/.test(owner) || Number(owner) < 1) {
                    nbsTabletStatus("Create validation error: Owner index must be >= 1");
                    return;
                }
                const alloc = nbsTabletBuildAllocConfigProto();
                if (!alloc.ok) {
                    nbsTabletStatus("Create validation error: " + alloc.error);
                    return;
                }
                nbsTabletPostRaw("tablet_create", owner, alloc.proto, alloc.storagePoolsText, nbsTabletRefreshList);
            }

            function nbsTabletDeleteOwner(ownerIdx) {
                if (!confirm("Delete NbsLoadTablet for owner index " + ownerIdx + "?")) {
                    return;
                }
                nbsTabletStatus("HTTP request in flight (tablet_delete)...");
                nbsTabletResultClear();
                $.ajax({
                    url: "",
                    data: {
                        mode: "tablet_delete",
                        owner_idx: String(ownerIdx),
                        config: "",
                        storage_pools: ""
                    },
                    method: "POST",
                    contentType: "application/x-protobuf-text",
                    complete: function(xhr) {
                        nbsTabletStatus("HTTP " + xhr.status + " (tablet_delete)");
                        const ct = (xhr.getResponseHeader("Content-Type") || "").toLowerCase();
                        const body = xhr.responseText || "";
                        if (ct.indexOf("text/html") >= 0) {
                            nbsTabletResultHtml(body);
                        } else {
                            const pre = document.createElement("pre");
                            pre.textContent = body;
                            $("#nbs-tablet-result").empty().append(pre);
                        }
                        if (xhr.status >= 200 && xhr.status < 300) {
                            nbsTabletRefreshList();
                        }
                    }
                });
            }

            $(document).on("input change", ".nbs-tablet-builder", function() {
                nbsTabletRefreshPreview();
            });

            $(function() {
                nbsTabletRefreshPreview();
            });

            // ── Run workload (independent per-tablet cards) ──────────────────
            //
            // Each tablet gets its own run card with a full parameter form plus
            // its own status line, progress bar, result tables and plots. Every
            // element id inside a card is scoped by a per-tablet instance id, so
            // several cards can run sweeps simultaneously and independently.

            window.nbsRunCards = window.nbsRunCards || {};

            function nbsRunNodeForTablet(tabletId) {
                var map = window.nbsTabletNodeMap || {};
                var nid = map[String(tabletId)];
                return (typeof nid === "number" && isFinite(nid)) ? nid : 0;
            }

            function nbsRunInst(tabletId) {
                return "rt" + String(tabletId).replace(/[^0-9a-zA-Z]/g, "");
            }

            // Scoped accessor bundle for a single card's elements/fields.
            function nbsRunCtx(inst) {
                const reg = window.nbsRunCards[inst] || {};
                return {
                    inst: inst,
                    reg: reg,
                    tabletId: reg.tabletId,
                    nodeId: reg.nodeId,
                    id: function(suffix) { return "nbs-" + inst + "-" + suffix; },
                    el: function(suffix) { return document.getElementById("nbs-" + inst + "-" + suffix); },
                    $f: function(suffix) { return $("#nbs-" + inst + "-" + suffix); },
                    val: function(suffix) { return nbsTabletTrim($("#nbs-" + inst + "-" + suffix).val()); },
                    checked: function(suffix) { return $("#nbs-" + inst + "-" + suffix).is(":checked"); }
                };
            }

            function nbsCardStatus(ctx, text) {
                const el = ctx.el("status");
                if (el) { el.textContent = text; }
            }

            function nbsRunCloseCard(inst) {
                const reg = window.nbsRunCards[inst];
                if (reg && reg.running &&
                    !confirm("A sweep is still running for this tablet. Close anyway?")) {
                    return;
                }
                const card = document.getElementById("nbs-" + inst + "-card");
                if (card && card.parentNode) {
                    card.parentNode.removeChild(card);
                }
                delete window.nbsRunCards[inst];
            }

            // Shared workload parameter form rows (used by both the per-tablet
            // card and the combined multi-tablet card). All ids are scoped by
            // the card prefix p ("nbs-<inst>-"); inst is needed for the inline
            // button handlers.
            function nbsRunFormFieldsHtml(p, inst) {
                return "" +
                    "<div class='row'>" +
                      "<div class='col-sm-4'><div class='form-group'><label>Tag (0 = auto-assign):</label>" +
                        "<input id='" + p + "tag' class='form-control' type='number' min='0' step='1' value='0' /></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>DurationSeconds:</label>" +
                        "<input id='" + p + "duration' class='form-control' type='number' min='0' step='1' value='60' /></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>Measure after (warmup s):</label>" +
                        "<input id='" + p + "delay-before' class='form-control' type='number' min='0' step='1' value='15' /></div></div>" +
                    "</div>" +
                    "<div class='row'>" +
                      "<div class='col-sm-4'><div class='form-group'><label>Trials per InFlight:</label>" +
                        "<input id='" + p + "trials' class='form-control' type='number' min='1' step='2' value='1' />" +
                        "<p class='help-block'>1 = single run. Values &gt; 1 must be odd.</p></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>MaxInFlight (single run):</label>" +
                        "<input id='" + p + "max-inflight' class='form-control' type='number' min='1' step='1' value='32' /></div></div>" +
                    "</div>" +
                    "<div class='row'>" +
                      "<div class='col-sm-4'><div class='form-group'><label>InFlightFrom (sweep start):</label>" +
                        "<input id='" + p + "inflight-from' class='form-control' type='number' min='1' step='1' placeholder='e.g. 1' /></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>InFlightTo (sweep end):</label>" +
                        "<input id='" + p + "inflight-to' class='form-control' type='number' min='1' step='1' placeholder='e.g. 128' />" +
                        "<p class='help-block'>If both set, sweeps MaxInFlight x2 per step.</p></div></div>" +
                    "</div>" +
                    "<div class='row'>" +
                      "<div class='col-sm-4'><div class='form-group'><label>ReadRatioPct (0-100):</label>" +
                        "<input id='" + p + "read-ratio' class='form-control' type='number' min='0' max='100' step='1' value='0' /></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>ReadWriteSizeKiB:</label>" +
                        "<input id='" + p + "size-kib' class='form-control' type='number' min='4' step='4' value='4' /></div></div>" +
                      "<div class='col-sm-4'><div class='form-group'><label>NumDirectBlockGroupsToUse (0=all):</label>" +
                        "<input id='" + p + "num-dbg' class='form-control' type='number' min='0' step='1' value='0' /></div></div>" +
                    "</div>" +
                    "<div class='row'>" +
                      "<div class='col-sm-4'><div class='form-group'><label>MaxInflightLsns:</label>" +
                        "<div class='input-group'>" +
                          "<span class='input-group-btn'><button type='button' class='btn btn-default' onclick='nbsRunScaleLsns(\"" + inst + "\",0.5)' title='Halve'>&divide;2</button></span>" +
                          "<input id='" + p + "max-inflight-lsns' class='form-control' type='number' min='1' step='1' value='4096' />" +
                          "<span class='input-group-btn'><button type='button' class='btn btn-default' onclick='nbsRunScaleLsns(\"" + inst + "\",2)' title='Double'>&times;2</button></span>" +
                        "</div></div></div>" +
                    "</div>" +
                    "<div class='form-group'><div class='checkbox'><label>" +
                      "<input id='" + p + "sequential' type='checkbox' /> Sequential (round-robin address space instead of random)" +
                    "</label></div></div>" +
                    "<div class='form-group'><div class='checkbox'><label>" +
                      "<input id='" + p + "disable-replication' type='checkbox' onchange='nbsRunDisableReplicationChanged(\"" + inst + "\", this)' /> Disable replication (single-PB write, skip DDisk flush)" +
                    "</label></div></div>";
            }

            // Common result containers (progress, status is rendered separately).
            function nbsRunResultContainersHtml(p) {
                return "" +
                    "<div id='" + p + "progress' style='display:none'></div>" +
                    "<div id='" + p + "result-table'></div>" +
                    "<div id='" + p + "median-table' style='margin-top:16px;display:none'></div>" +
                    "<div id='" + p + "iops-plot' style='margin-top:16px'></div>" +
                    "<div id='" + p + "latency-plot' style='margin-top:16px'></div>";
            }

            // Builds the full per-tablet run-card markup with instance-scoped ids.
            function nbsRunCardHtml(inst, tabletId, nodeId) {
                const p = "nbs-" + inst + "-";
                return "" +
                "<div id='" + p + "card' class='panel panel-primary' style='margin-top:16px'>" +
                  "<div class='panel-heading' style='display:flex;justify-content:space-between;align-items:center'>" +
                    "<strong>Run workload &mdash; tablet " + tabletId + " (node " + nodeId + ")</strong>" +
                    "<button type='button' class='btn btn-xs btn-default' onclick='nbsRunCloseCard(\"" + inst + "\")'>Close</button>" +
                  "</div>" +
                  "<div class='panel-body'>" +
                    nbsRunFormFieldsHtml(p, inst) +
                    "<div class='form-group'>" +
                      "<button type='button' id='" + p + "run-btn' onclick='nbsTabletSweepInst(\"" + inst + "\")' class='btn btn-primary'>Run</button> " +
                      "<span id='" + p + "status' class='text-muted' style='margin-left:10px'></span>" +
                    "</div>" +
                    nbsRunResultContainersHtml(p) +
                  "</div>" +
                "</div>";
            }

            // Builds the singleton combined multi-tablet card. It targets several
            // tablets at once (each load actor runs on its tablet's node) and shows
            // both the aggregated result and a per-tablet breakdown.
            function nbsMultiCardHtml(inst) {
                const p = "nbs-" + inst + "-";
                return "" +
                "<div id='" + p + "card' class='panel panel-info' style='margin-top:16px'>" +
                  "<div class='panel-heading' style='display:flex;justify-content:space-between;align-items:center'>" +
                    "<strong>Run multi-tablet load (combined)</strong>" +
                    "<button type='button' class='btn btn-xs btn-default' onclick='nbsRunCloseCard(\"" + inst + "\")'>Close</button>" +
                  "</div>" +
                  "<div class='panel-body'>" +
                    "<div class='form-group'>" +
                      "<label>Target tablet ids (comma/space separated):</label>" +
                      "<input id='" + p + "targets' class='form-control' type='text' placeholder='e.g. 72075186224037888, 72075186224037889' />" +
                      "<p class='help-block'>Each tablet's node is resolved from the list above; its load actor runs on that node. The results below combine all tablets, plus a per-tablet breakdown.</p>" +
                    "</div>" +
                    nbsRunFormFieldsHtml(p, inst) +
                    "<div class='form-group'>" +
                      "<button type='button' id='" + p + "run-btn' onclick='nbsTabletSweepInst(\"" + inst + "\")' class='btn btn-primary'>Run</button> " +
                      "<span id='" + p + "status' class='text-muted' style='margin-left:10px'></span>" +
                    "</div>" +
                    "<div id='" + p + "progress' style='display:none'></div>" +
                    "<div id='" + p + "pertablet-tables' style='margin-top:16px;display:none'></div>" +
                    "<div id='" + p + "pertablet-plot' style='margin-top:16px;display:none'></div>" +
                    "<div id='" + p + "result-table'></div>" +
                    "<div id='" + p + "median-table' style='margin-top:16px;display:none'></div>" +
                    "<div id='" + p + "iops-plot' style='margin-top:16px'></div>" +
                    "<div id='" + p + "latency-plot' style='margin-top:16px'></div>" +
                  "</div>" +
                "</div>";
            }

            // Parses the multi-tablet card's target field into a deduplicated list
            // of {tid, nid} (nid resolved from the rendered tablet list).
            function nbsRunCollectTargets(ctx) {
                const raw = ctx.val("targets");
                const seen = {};
                const out = [];
                raw.split(/[\s,]+/).forEach(function(tok) {
                    tok = nbsTabletTrim(tok);
                    if (tok === "" || !/^[0-9]+$/.test(tok) || seen[tok]) {
                        return;
                    }
                    seen[tok] = true;
                    out.push({ tid: tok, nid: nbsRunNodeForTablet(tok) });
                });
                return out;
            }

            // Opens (or focuses) the singleton combined multi-tablet card.
            function nbsTabletOpenMulti() {
                const inst = "multi";
                let card = document.getElementById("nbs-" + inst + "-card");
                if (!card) {
                    window.nbsRunCards[inst] = { multi: true, running: false };
                    const container = document.getElementById("nbs-run-cards");
                    const wrap = document.createElement("div");
                    wrap.innerHTML = nbsMultiCardHtml(inst);
                    card = wrap.firstElementChild;
                    container.insertBefore(card, container.firstChild);
                }
                const field = document.getElementById("nbs-" + inst + "-targets");
                if (field && nbsTabletTrim(field.value) === "") {
                    field.value = Object.keys(window.nbsTabletNodeMap || {}).join(", ");
                }
                card.scrollIntoView({behavior: "smooth", block: "start"});
            }

            // Opens (or focuses) the dedicated run card for the given tablet.
            function nbsTabletPrepareRun(tabletId, nodeId) {
                if (typeof nodeId === "number" && isFinite(nodeId)) {
                    window.nbsTabletNodeMap = window.nbsTabletNodeMap || {};
                    window.nbsTabletNodeMap[String(tabletId)] = nodeId;
                } else {
                    nodeId = nbsRunNodeForTablet(tabletId);
                }
                const inst = nbsRunInst(tabletId);
                let card = document.getElementById("nbs-" + inst + "-card");
                if (!card) {
                    window.nbsRunCards[inst] = {
                        tabletId: String(tabletId), nodeId: nodeId, running: false
                    };
                    const container = document.getElementById("nbs-run-cards");
                    const wrap = document.createElement("div");
                    wrap.innerHTML = nbsRunCardHtml(inst, tabletId, nodeId);
                    card = wrap.firstElementChild;
                    container.appendChild(card);
                } else {
                    window.nbsRunCards[inst].tabletId = String(tabletId);
                    window.nbsRunCards[inst].nodeId = nodeId;
                }
                card.scrollIntoView({behavior: "smooth", block: "start"});
            }

            function nbsTabletRunOnce(ctx, maxInFlight, targetsCsv) {
                return new Promise(function(resolve, reject) {
                    const disableRepl = ctx.checked("disable-replication");
                    const params = {
                        mode:                    "tablet_run",
                        tag:                     ctx.val("tag") || "0",
                        duration_seconds:        ctx.val("duration") || "0",
                        delay_before_seconds:    ctx.val("delay-before") || "15",
                        max_in_flight:           String(maxInFlight),
                        read_ratio_pct:          disableRepl ? "0" : (ctx.val("read-ratio") || "0"),
                        read_write_size_kib:     ctx.val("size-kib") || "4",
                        sequential:              ctx.checked("sequential") ? "1" : "0",
                        num_dbg_to_use:          ctx.val("num-dbg") || "0",
                        max_inflight_lsns:       ctx.val("max-inflight-lsns") || "4096",
                        disable_replication:     disableRepl ? "1" : "0"
                    };
                    // Route every run (single- and multi-tablet) through the
                    // targets path so the load actor starts on the tablet's own
                    // node (co-located), avoiding the cross-node hop. Falls back
                    // to nid=0 (local) only if the node is unknown.
                    params.targets = targetsCsv ||
                        (String(ctx.tabletId) + ":" + (Number(ctx.nodeId) || 0));
                    $.ajax({
                        url: "",
                        data: params,
                        method: "POST",
                        contentType: "application/x-protobuf-text",
                        dataType: "json",
                        success: function(result) {
                            if (result.status === "ok") {
                                resolve({uuid: result.uuid, tag: result.tag});
                            } else {
                                reject(new Error("Start failed: " + result.status));
                            }
                        },
                        error: function(xhr) {
                            reject(new Error("HTTP " + xhr.status + ": " + (xhr.responseText || "")));
                        }
                    });
                });
            }

            function nbsTabletPollResult(uuid, durationSec, progressEl) {
                return new Promise(function(resolve, reject) {
                    const startTime = Date.now();
                    let progressTimer = null;
                    let pollTimer = null;
                    let timeoutId = null;

                    if (progressEl) {
                        const bar = progressEl.querySelector(".progress-bar");
                        progressTimer = setInterval(function() {
                            const elapsed = (Date.now() - startTime) / 1000;
                            if (durationSec > 0) {
                                const pct = Math.min(100, elapsed / durationSec * 100);
                                if (bar) {
                                    bar.style.width = pct.toFixed(1) + "%";
                                    bar.textContent = Math.floor(pct) + "%";
                                }
                            }
                        }, 1000);
                    }

                    function cleanup() {
                        if (progressTimer !== null) {
                            clearInterval(progressTimer);
                            progressTimer = null;
                        }
                        if (pollTimer !== null) {
                            clearTimeout(pollTimer);
                            pollTimer = null;
                        }
                        if (timeoutId !== null) {
                            clearTimeout(timeoutId);
                            timeoutId = null;
                        }
                        if (progressEl) {
                            progressEl.style.display = "none";
                        }
                    }

                    const pollTimeoutMs = durationSec * 1000 + 30_000;
                    timeoutId = setTimeout(function() {
                        cleanup();
                        reject(new Error("poll timeout after " + Math.round(pollTimeoutMs / 1000) + "s"));
                    }, pollTimeoutMs);

                    let pollCount = 0;
                    function poll() {
                        pollCount++;
                        $.ajax({
                            url: window.location.pathname,
                            data: {mode: "results"},
                            headers: {Accept: "application/json"},
                            method: "GET",
                            dataType: "json",
                            success: function(results) {
                                if (!Array.isArray(results)) { scheduleNext(); return; }
                                for (let i = 0; i < results.length; i++) {
                                    if (results[i].uuid === uuid) {
                                        cleanup();
                                        const nodes = results[i].nodes;
                                        const jr = (nodes && nodes.length > 0) ? nodes[0] : {};
                                        resolve(jr);
                                        return;
                                    }
                                }
                                scheduleNext();
                            },
                            error: function() { scheduleNext(); }
                        });
                    }

                    function scheduleNext() {
                        pollTimer = setTimeout(poll, 2000);
                    }

                    // First poll after a short delay to let the run register.
                    setTimeout(poll, 1500);
                });
            }

            function nbsTabletBuildSweepValues(fromVal, toVal, singleVal) {
                fromVal = parseInt(fromVal) || 0;
                toVal   = parseInt(toVal)   || 0;
                if (fromVal > 0 && toVal >= fromVal) {
                    const vals = [];
                    for (let v = fromVal; v <= toVal; v = v * 2) {
                        vals.push(v);
                    }
                    return vals;
                }
                return [parseInt(singleVal) || 32];
            }

            function nbsTabletFmt2(v) {
                return (typeof v === "number" && isFinite(v)) ? v.toFixed(2) : "-";
            }

            function nbsTabletFmtUs(v) {
                if (typeof v !== "number" || !isFinite(v)) {
                    return "-";
                }
                return String(Math.round(v));
            }

            function nbsTabletParseTrials(ctx) {
                const raw = ctx.val("trials");
                if (raw === "") {
                    return { ok: true, trials: 1 };
                }
                if (!/^[0-9]+$/.test(raw)) {
                    return { ok: false, error: "Trials per InFlight must be a positive integer" };
                }
                const trials = parseInt(raw, 10);
                if (trials < 1) {
                    return { ok: false, error: "Trials per InFlight must be >= 1" };
                }
                if (trials > 1 && (trials % 2) === 0) {
                    return { ok: false, error: "Trials per InFlight must be odd when > 1" };
                }
                return { ok: true, trials: trials };
            }

            function nbsTabletMedianByWriteIops(runs) {
                if (!runs || runs.length === 0) {
                    return null;
                }
                const sorted = runs.slice().sort(function(a, b) {
                    return (a.write_rps || 0) - (b.write_rps || 0);
                });
                return sorted[Math.floor(sorted.length / 2)];
            }

            function nbsTabletRenderTableSkeleton(ctx, sweepValues, showReads) {
                const tbl = ctx.el("result-table");
                if (!tbl) {
                    return;
                }

                let html = "<table class='table table-condensed table-bordered' style='margin-top:12px'>";
                html += "<thead><tr>";
                if (showReads) {
                    html += "<th rowspan='2' style='vertical-align:middle'>MaxInFlight</th>";
                    html += "<th rowspan='2' style='vertical-align:middle'>Direction</th>";
                } else {
                    html += "<th style='vertical-align:middle'>MaxInFlight</th>";
                    html += "<th style='vertical-align:middle'>Direction</th>";
                }
                html += "<th>IOPS</th><th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                html += "</tr></thead>";
                html += "<tbody>";
                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    html += "<tr id='" + ctx.id("sweep-w-" + i) + "'>";
                    if (showReads) {
                        html += "<td rowspan='2' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                    } else {
                        html += "<td style='font-weight:bold'>" + v + "</td>";
                    }
                    html += "<td>Writes</td>";
                    html += "<td id='" + ctx.id("sw-wiops-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sw-wp50-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sw-wp95-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sw-wp99-" + i) + "'>&#9711;</td>";
                    html += "</tr>";
                    if (showReads) {
                        html += "<tr id='" + ctx.id("sweep-r-" + i) + "'>";
                        html += "<td>Reads</td>";
                        html += "<td id='" + ctx.id("sw-riops-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-rp50-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-rp95-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-rp99-" + i) + "'>&#9711;</td>";
                        html += "</tr>";
                    }
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderMultiTrialSkeleton(ctx, sweepValues, trials, showReads) {
                const tbl = ctx.el("result-table");
                if (!tbl) {
                    return;
                }

                let html = "<table class='table table-condensed table-bordered' style='margin-top:12px'>";
                html += "<thead><tr>";
                html += "<th style='vertical-align:middle'>MaxInFlight</th>";
                html += "<th style='vertical-align:middle'>Trial</th>";
                html += "<th style='vertical-align:middle'>Direction</th>";
                html += "<th>IOPS</th><th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                html += "</tr></thead><tbody>";
                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    const rowSpan = showReads ? 2 * trials : trials;
                    for (let t = 0; t < trials; t++) {
                        html += "<tr id='" + ctx.id("sweep-w-" + i + "-" + t) + "'>";
                        if (t === 0) {
                            html += "<td rowspan='" + rowSpan +
                                "' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                        }
                        html += "<td rowspan='" + (showReads ? "2" : "1") +
                            "' style='vertical-align:middle'>" + (t + 1) + "</td>";
                        html += "<td>Writes</td>";
                        html += "<td id='" + ctx.id("sw-wiops-" + i + "-" + t) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-wp50-" + i + "-" + t) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-wp95-" + i + "-" + t) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sw-wp99-" + i + "-" + t) + "'>&#9711;</td>";
                        html += "</tr>";
                        if (showReads) {
                            html += "<tr id='" + ctx.id("sweep-r-" + i + "-" + t) + "'>";
                            html += "<td>Reads</td>";
                            html += "<td id='" + ctx.id("sw-riops-" + i + "-" + t) + "'>&#9711;</td>";
                            html += "<td id='" + ctx.id("sw-rp50-" + i + "-" + t) + "'>&#9711;</td>";
                            html += "<td id='" + ctx.id("sw-rp95-" + i + "-" + t) + "'>&#9711;</td>";
                            html += "<td id='" + ctx.id("sw-rp99-" + i + "-" + t) + "'>&#9711;</td>";
                            html += "</tr>";
                        }
                    }
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderMedianSkeleton(ctx, sweepValues, showReads) {
                const tbl = ctx.el("median-table");
                if (!tbl) {
                    return;
                }

                let html = "<h4 style='margin-top:0'>Median trial (by write IOPS)</h4>";
                html += "<table class='table table-condensed table-bordered'>";
                html += "<thead><tr>";
                if (showReads) {
                    html += "<th rowspan='2' style='vertical-align:middle'>MaxInFlight</th>";
                    html += "<th rowspan='2' style='vertical-align:middle'>Direction</th>";
                } else {
                    html += "<th style='vertical-align:middle'>MaxInFlight</th>";
                    html += "<th style='vertical-align:middle'>Direction</th>";
                }
                html += "<th>IOPS</th><th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                html += "</tr></thead><tbody>";
                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    html += "<tr id='" + ctx.id("median-w-" + i) + "'>";
                    if (showReads) {
                        html += "<td rowspan='2' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                    } else {
                        html += "<td style='font-weight:bold'>" + v + "</td>";
                    }
                    html += "<td>Writes</td>";
                    html += "<td id='" + ctx.id("sm-wiops-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sm-wp50-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sm-wp95-" + i) + "'>&#9711;</td>";
                    html += "<td id='" + ctx.id("sm-wp99-" + i) + "'>&#9711;</td>";
                    html += "</tr>";
                    if (showReads) {
                        html += "<tr id='" + ctx.id("median-r-" + i) + "'>";
                        html += "<td>Reads</td>";
                        html += "<td id='" + ctx.id("sm-riops-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sm-rp50-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sm-rp95-" + i) + "'>&#9711;</td>";
                        html += "<td id='" + ctx.id("sm-rp99-" + i) + "'>&#9711;</td>";
                        html += "</tr>";
                    }
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderDetailSkeleton(ctx, sweepValues, trials, showReads) {
                if (trials > 1) {
                    nbsTabletRenderMultiTrialSkeleton(ctx, sweepValues, trials, showReads);
                } else {
                    nbsTabletRenderTableSkeleton(ctx, sweepValues, showReads);
                }
            }

            function nbsTabletFillMetricsCells(ctx, kind, idx, suffix, jr) {
                function set(suff, val) {
                    const el = ctx.el(suff);
                    if (el) {
                        el.textContent = val;
                    }
                }
                const sfx = suffix !== undefined ? ("-" + suffix) : "";
                const wIops = jr.write_rps !== undefined ? Math.round(jr.write_rps) : "-";
                const rIops = jr.read_rps !== undefined ? Math.round(jr.read_rps) : "-";
                set(kind + "-wiops-" + idx + sfx, wIops);
                set(kind + "-wp50-" + idx + sfx, nbsTabletFmtUs(jr.write_p50));
                set(kind + "-wp95-" + idx + sfx, nbsTabletFmtUs(jr.write_p95));
                set(kind + "-wp99-" + idx + sfx, nbsTabletFmtUs(jr.write_p99));
                set(kind + "-riops-" + idx + sfx, rIops);
                set(kind + "-rp50-" + idx + sfx, nbsTabletFmtUs(jr.read_p50));
                set(kind + "-rp95-" + idx + sfx, nbsTabletFmtUs(jr.read_p95));
                set(kind + "-rp99-" + idx + sfx, nbsTabletFmtUs(jr.read_p99));
            }

            function nbsTabletSetMetricsError(ctx, kind, idx, suffix, msg) {
                function set(suff, val) {
                    const el = ctx.el(suff);
                    if (el) {
                        el.textContent = val;
                    }
                }
                const sfx = suffix !== undefined ? ("-" + suffix) : "";
                set(kind + "-wiops-" + idx + sfx, "ERR");
                set(kind + "-wp50-" + idx + sfx, msg || "ERR");
                set(kind + "-wp95-" + idx + sfx, "");
                set(kind + "-wp99-" + idx + sfx, "");
                set(kind + "-riops-" + idx + sfx, "ERR");
                set(kind + "-rp50-" + idx + sfx, "");
                set(kind + "-rp95-" + idx + sfx, "");
                set(kind + "-rp99-" + idx + sfx, "");
            }

            function nbsTabletFillTableColumn(ctx, idx, jr) {
                nbsTabletFillMetricsCells(ctx, "sw", idx, undefined, jr);
            }

            function nbsTabletFillTrialRows(ctx, idx, trialIdx, jr) {
                nbsTabletFillMetricsCells(ctx, "sw", idx, trialIdx, jr);
            }

            function nbsTabletFillMedianRow(ctx, idx, jr) {
                nbsTabletFillMetricsCells(ctx, "sm", idx, undefined, jr);
            }

            function nbsTabletSetColumnError(ctx, idx, msg) {
                nbsTabletSetMetricsError(ctx, "sw", idx, undefined, msg);
            }

            function nbsTabletSetTrialError(ctx, idx, trialIdx, msg) {
                nbsTabletSetMetricsError(ctx, "sw", idx, trialIdx, msg);
            }

            function nbsTabletSetMedianError(ctx, idx, msg) {
                nbsTabletSetMetricsError(ctx, "sm", idx, undefined, msg);
            }

            function nbsTabletIopsStats(values) {
                if (!values || values.length === 0) {
                    return null;
                }
                const sorted = values.slice().sort(function(a, b) {
                    return a - b;
                });
                return {
                    min: sorted[0],
                    max: sorted[sorted.length - 1],
                    median: sorted[Math.floor(sorted.length / 2)]
                };
            }

            function nbsTabletRenderIopsPlot(ctx, sweepValues, plotData) {
                const plotDiv = ctx.el("iops-plot");
                if (!plotDiv || !sweepValues || sweepValues.length === 0) {
                    return;
                }

                const writeColor = "#337ab7";
                const readColor = "#f0ad4e";
                const n = sweepValues.length;
                const marginLeft = 60;
                const marginRight = 20;
                const marginTop = 40;
                const marginBottom = 50;
                const width = 800;
                const height = 320;
                const plotW = width - marginLeft - marginRight;
                const plotH = height - marginTop - marginBottom;
                const baseY = height - marginBottom;

                function xAt(i) {
                    if (n <= 1) {
                        return marginLeft + plotW / 2;
                    }
                    return marginLeft + (i / (n - 1)) * plotW;
                }

                function yAt(v, yMax) {
                    if (yMax <= 0) {
                        return baseY;
                    }
                    return baseY - (v / yMax) * plotH;
                }

                let yMax = 1;
                let showReads = false;
                for (let i = 0; i < plotData.length; i++) {
                    const p = plotData[i];
                    const ws = nbsTabletIopsStats(p.writes);
                    const rs = nbsTabletIopsStats(p.reads);
                    if (ws) {
                        yMax = Math.max(yMax, ws.max);
                    }
                    if (rs && rs.max > 0) {
                        showReads = true;
                        yMax = Math.max(yMax, rs.max);
                    }
                }
                yMax = Math.ceil(yMax * 1.1);

                let svg = "<h4 style='margin-top:0'>IOPS vs MaxInFlight</h4>";
                svg += "<svg viewBox='0 0 " + width + " " + height +
                    "' width='100%' style='max-width:800px;background:#fff' " +
                    "xmlns='http://www.w3.org/2000/svg'>";

                svg += "<line x1='" + marginLeft + "' y1='" + marginTop +
                    "' x2='" + marginLeft + "' y2='" + baseY +
                    "' stroke='#ccc' stroke-width='1'/>";
                svg += "<line x1='" + marginLeft + "' y1='" + baseY +
                    "' x2='" + (width - marginRight) + "' y2='" + baseY +
                    "' stroke='#ccc' stroke-width='1'/>";

                const yTicks = 5;
                for (let t = 0; t <= yTicks; t++) {
                    const val = Math.round((yMax * t) / yTicks);
                    const y = yAt(val, yMax);
                    svg += "<line x1='" + marginLeft + "' y1='" + y +
                        "' x2='" + (width - marginRight) + "' y2='" + y +
                        "' stroke='#eee' stroke-width='1'/>";
                    svg += "<text x='" + (marginLeft - 6) + "' y='" + (y + 4) +
                        "' text-anchor='end' font-size='10' fill='#666'>" + val + "</text>";
                }

                for (let i = 0; i < n; i++) {
                    const x = xAt(i);
                    svg += "<text x='" + x + "' y='" + (baseY + 16) +
                        "' text-anchor='middle' font-size='11' fill='#333'>" +
                        sweepValues[i] + "</text>";
                }

                const writeMedians = [];
                const readMedians = [];

                for (let i = 0; i < n; i++) {
                    const x = xAt(i);
                    const p = plotData[i] || { writes: [], reads: [] };
                    const ws = nbsTabletIopsStats(p.writes);
                    if (ws) {
                        const yMin = yAt(ws.min, yMax);
                        const yMaxP = yAt(ws.max, yMax);
                        const yMed = yAt(ws.median, yMax);
                        svg += "<line x1='" + x + "' y1='" + yMin +
                            "' x2='" + x + "' y2='" + yMaxP +
                            "' stroke='" + writeColor + "' stroke-width='2'/>";
                        svg += "<circle cx='" + x + "' cy='" + yMed +
                            "' r='4' fill='" + writeColor + "'/>";
                        writeMedians.push(x + "," + yMed);
                    }
                    if (showReads) {
                        const rs = nbsTabletIopsStats(p.reads);
                        if (rs && rs.max > 0) {
                            const yMinR = yAt(rs.min, yMax);
                            const yMaxR = yAt(rs.max, yMax);
                            const yMedR = yAt(rs.median, yMax);
                            svg += "<line x1='" + x + "' y1='" + yMinR +
                                "' x2='" + x + "' y2='" + yMaxR +
                                "' stroke='" + readColor + "' stroke-width='2'/>";
                            svg += "<circle cx='" + x + "' cy='" + yMedR +
                                "' r='4' fill='" + readColor + "'/>";
                            readMedians.push(x + "," + yMedR);
                        }
                    }
                }

                if (writeMedians.length >= 2) {
                    svg += "<polyline points='" + writeMedians.join(" ") +
                        "' fill='none' stroke='" + writeColor +
                        "' stroke-width='1.5' stroke-dasharray='4,3'/>";
                }
                if (showReads && readMedians.length >= 2) {
                    svg += "<polyline points='" + readMedians.join(" ") +
                        "' fill='none' stroke='" + readColor +
                        "' stroke-width='1.5' stroke-dasharray='4,3'/>";
                }

                const legX = width - marginRight - 90;
                svg += "<line x1='" + legX + "' y1='28' x2='" + (legX + 20) +
                    "' y2='28' stroke='" + writeColor + "' stroke-width='2'/>";
                svg += "<circle cx='" + (legX + 10) + "' cy='28' r='3' fill='" +
                    writeColor + "'/>";
                svg += "<text x='" + (legX + 26) + "' y='32' font-size='11'>" +
                    "Writes</text>";
                if (showReads) {
                    svg += "<line x1='" + legX + "' y1='46' x2='" + (legX + 20) +
                        "' y2='46' stroke='" + readColor + "' stroke-width='2'/>";
                    svg += "<circle cx='" + (legX + 10) + "' cy='46' r='3' fill='" +
                        readColor + "'/>";
                    svg += "<text x='" + (legX + 26) + "' y='50' font-size='11'>" +
                        "Reads</text>";
                }

                svg += "</svg>";
                plotDiv.innerHTML = svg;
            }

            function nbsTabletLatMedian(arr) {
                if (!arr || arr.length === 0) {
                    return null;
                }
                const sorted = arr.slice().sort(function(a, b) { return a - b; });
                return sorted[Math.floor(sorted.length / 2)];
            }

            function nbsTabletRenderLatencyPlot(ctx, sweepValues, plotData) {
                const plotDiv = ctx.el("latency-plot");
                if (!plotDiv || !sweepValues || sweepValues.length === 0) {
                    return;
                }

                const writeColor = "#337ab7";
                const readColor  = "#f0ad4e";
                const n = sweepValues.length;
                const marginLeft   = 70;
                const marginRight  = 20;
                const marginTop    = 56;
                const marginBottom = 50;
                const width  = 800;
                const height = 320;
                const plotW  = width  - marginLeft - marginRight;
                const plotH  = height - marginTop  - marginBottom;
                const baseY  = height - marginBottom;

                function xAt(i) {
                    if (n <= 1) {
                        return marginLeft + plotW / 2;
                    }
                    return marginLeft + (i / (n - 1)) * plotW;
                }

                function yAt(v, yMax) {
                    if (yMax <= 0) {
                        return baseY;
                    }
                    return baseY - (v / yMax) * plotH;
                }

                let yMax = 1;
                let hasReads = false;
                for (let i = 0; i < plotData.length; i++) {
                    const p = plotData[i];
                    if (p.writeLat) {
                        const v = nbsTabletLatMedian(p.writeLat.p99);
                        if (v) {
                            yMax = Math.max(yMax, v);
                        }
                    }
                    if (p.readLat && p.readLat.p99 && p.readLat.p99.length > 0) {
                        const v = nbsTabletLatMedian(p.readLat.p99);
                        if (v && v > 0) {
                            hasReads = true;
                            yMax = Math.max(yMax, v);
                        }
                    }
                }
                if (yMax <= 1) {
                    return;
                }
                yMax = Math.ceil(yMax * 1.1);

                let svg = "<h4 style='margin-top:0'>Latency (us) vs MaxInFlight</h4>";
                svg += "<svg viewBox='0 0 " + width + " " + height +
                    "' width='100%' style='max-width:800px;background:#fff' " +
                    "xmlns='http://www.w3.org/2000/svg'>";

                svg += "<line x1='" + marginLeft + "' y1='" + marginTop +
                    "' x2='" + marginLeft + "' y2='" + baseY +
                    "' stroke='#ccc' stroke-width='1'/>";
                svg += "<line x1='" + marginLeft + "' y1='" + baseY +
                    "' x2='" + (width - marginRight) + "' y2='" + baseY +
                    "' stroke='#ccc' stroke-width='1'/>";

                const yTicks = 5;
                for (let t = 0; t <= yTicks; t++) {
                    const val = Math.round((yMax * t) / yTicks);
                    const y = yAt(val, yMax);
                    svg += "<line x1='" + marginLeft + "' y1='" + y +
                        "' x2='" + (width - marginRight) + "' y2='" + y +
                        "' stroke='#eee' stroke-width='1'/>";
                    svg += "<text x='" + (marginLeft - 6) + "' y='" + (y + 4) +
                        "' text-anchor='end' font-size='10' fill='#666'>" + val + "</text>";
                }

                for (let i = 0; i < n; i++) {
                    const x = xAt(i);
                    svg += "<text x='" + x + "' y='" + (baseY + 16) +
                        "' text-anchor='middle' font-size='11' fill='#333'>" +
                        sweepValues[i] + "</text>";
                }

                const pctLines = [
                    { pct: "p50", dash: "4,3",  strokeW: "1.5" },
                    { pct: "p95", dash: "2,2",  strokeW: "1.5" },
                    { pct: "p99", dash: "none", strokeW: "2"   },
                ];
                const bands = [
                    { lat: "writeLat", color: writeColor },
                ];
                if (hasReads) {
                    bands.push({ lat: "readLat", color: readColor });
                }

                for (const band of bands) {
                    for (const pl of pctLines) {
                        const pts = [];
                        for (let i = 0; i < n; i++) {
                            const x = xAt(i);
                            const p = plotData[i];
                            const latArr = p[band.lat] && p[band.lat][pl.pct];
                            const med = nbsTabletLatMedian(latArr);
                            if (med == null) {
                                continue;
                            }
                            const yv = yAt(med, yMax);
                            svg += "<circle cx='" + x + "' cy='" + yv +
                                "' r='3' fill='" + band.color + "' opacity='0.75'/>";
                            pts.push(x + "," + yv);
                        }
                        if (pts.length >= 2) {
                            svg += "<polyline points='" + pts.join(" ") +
                                "' fill='none' stroke='" + band.color +
                                "' stroke-width='" + pl.strokeW + "'" +
                                (pl.dash !== "none" ? " stroke-dasharray='" + pl.dash + "'" : "") +
                                "/>";
                        }
                    }
                }

                const legItems = [
                    { color: writeColor, label: "Write p50", dash: "4,3"  },
                    { color: writeColor, label: "Write p95", dash: "2,2"  },
                    { color: writeColor, label: "Write p99", dash: "none" },
                ];
                if (hasReads) {
                    legItems.push(
                        { color: readColor, label: "Read p50", dash: "4,3"  },
                        { color: readColor, label: "Read p95", dash: "2,2"  },
                        { color: readColor, label: "Read p99", dash: "none" }
                    );
                }
                const legColW = 90;
                const legCols = 3;
                const legStartX = marginLeft + 4;
                const legStartY = 14;
                for (let li = 0; li < legItems.length; li++) {
                    const lx = legStartX + (li % legCols) * legColW;
                    const ly = legStartY + Math.floor(li / legCols) * 18;
                    const item = legItems[li];
                    svg += "<line x1='" + lx + "' y1='" + ly +
                        "' x2='" + (lx + 20) + "' y2='" + ly +
                        "' stroke='" + item.color + "' stroke-width='2'" +
                        (item.dash !== "none" ? " stroke-dasharray='" + item.dash + "'" : "") +
                        "/>";
                    svg += "<text x='" + (lx + 24) + "' y='" + (ly + 4) +
                        "' font-size='10' fill='#333'>" + item.label + "</text>";
                }

                svg += "</svg>";
                plotDiv.innerHTML = svg;
            }

            function nbsTabletRefreshList() {
                $.get(window.location.pathname + "?mode=tablet_list", function(html) {
                    $("#nbs-tablet-list-container").html(html);
                });
            }

            function nbsTabletPalette(i) {
                const colors = ["#337ab7", "#5cb85c", "#f0ad4e", "#d9534f",
                    "#9b59b6", "#1abc9c", "#e67e22", "#34495e", "#16a085",
                    "#c0392b", "#2980b9", "#8e44ad"];
                return colors[i % colors.length];
            }

            // perTabletData[tid][i] is the array of per-tablet jr objects (one
            // per trial) for sweep value i. Renders one median table per tablet
            // into the multi-tablet card's pertablet-tables container.
            function nbsTabletRenderPerTabletTables(ctx, sweepValues, tabletOrder, tabletNode, perTabletData, showReads) {
                const div = ctx.el("pertablet-tables");
                if (!div) {
                    return;
                }
                if (!tabletOrder || tabletOrder.length === 0) {
                    div.style.display = "none";
                    div.innerHTML = "";
                    return;
                }
                let html = "<h4 style='margin-top:0'>Per-tablet results (median by write IOPS)</h4>";
                tabletOrder.forEach(function(tid) {
                    html += "<div style='margin-bottom:14px'>";
                    html += "<strong>Tablet " + tid + " (node " +
                        (tabletNode[tid] || 0) + ")</strong>";
                    html += "<table class='table table-condensed table-bordered' style='margin-top:6px'>";
                    html += "<thead><tr>";
                    html += "<th>MaxInFlight</th><th>Direction</th><th>IOPS</th>" +
                        "<th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                    html += "</tr></thead><tbody>";
                    for (let i = 0; i < sweepValues.length; i++) {
                        const trials = (perTabletData[tid] && perTabletData[tid][i])
                            ? perTabletData[tid][i] : [];
                        const m = nbsTabletMedianByWriteIops(trials);
                        const wIops = (m && m.write_rps != null) ? Math.round(m.write_rps) : "-";
                        const rIops = (m && m.read_rps != null) ? Math.round(m.read_rps) : "-";
                        const rowspan = showReads ? "2" : "1";
                        html += "<tr>";
                        html += "<td rowspan='" + rowspan +
                            "' style='vertical-align:middle;font-weight:bold'>" + sweepValues[i] + "</td>";
                        html += "<td>Writes</td>";
                        html += "<td>" + wIops + "</td>";
                        html += "<td>" + nbsTabletFmtUs(m ? m.write_p50 : undefined) + "</td>";
                        html += "<td>" + nbsTabletFmtUs(m ? m.write_p95 : undefined) + "</td>";
                        html += "<td>" + nbsTabletFmtUs(m ? m.write_p99 : undefined) + "</td>";
                        html += "</tr>";
                        if (showReads) {
                            html += "<tr>";
                            html += "<td>Reads</td>";
                            html += "<td>" + rIops + "</td>";
                            html += "<td>" + nbsTabletFmtUs(m ? m.read_p50 : undefined) + "</td>";
                            html += "<td>" + nbsTabletFmtUs(m ? m.read_p95 : undefined) + "</td>";
                            html += "<td>" + nbsTabletFmtUs(m ? m.read_p99 : undefined) + "</td>";
                            html += "</tr>";
                        }
                    }
                    html += "</tbody></table></div>";
                });
                div.innerHTML = html;
                div.style.display = "";
            }

            // One write-IOPS line per tablet (median across trials) over the
            // sweep; read lines (dashed, same color) added when reads are on.
            function nbsTabletRenderPerTabletPlot(ctx, sweepValues, tabletOrder, tabletNode, perTabletData, showReads) {
                const div = ctx.el("pertablet-plot");
                if (!div) {
                    return;
                }
                if (!tabletOrder || tabletOrder.length === 0 || sweepValues.length === 0) {
                    div.style.display = "none";
                    div.innerHTML = "";
                    return;
                }

                const n = sweepValues.length;
                const marginLeft = 60, marginRight = 140, marginTop = 40, marginBottom = 50;
                const width = 800, height = 320;
                const plotW = width - marginLeft - marginRight;
                const plotH = height - marginTop - marginBottom;
                const baseY = height - marginBottom;

                function xAt(i) {
                    return n <= 1 ? marginLeft + plotW / 2 : marginLeft + (i / (n - 1)) * plotW;
                }
                function yAt(v, yMax) {
                    return yMax <= 0 ? baseY : baseY - (v / yMax) * plotH;
                }
                function medianOf(arr) {
                    if (!arr || arr.length === 0) {
                        return null;
                    }
                    const s = arr.slice().sort(function(a, b) { return a - b; });
                    return s[Math.floor(s.length / 2)];
                }

                // Median write/read IOPS per tablet per sweep value.
                const series = [];
                tabletOrder.forEach(function(tid) {
                    const wr = [], rd = [];
                    for (let i = 0; i < n; i++) {
                        const trials = (perTabletData[tid] && perTabletData[tid][i])
                            ? perTabletData[tid][i] : [];
                        wr.push(medianOf(trials.map(function(j) { return Math.round(j.write_rps || 0); })));
                        rd.push(medianOf(trials.map(function(j) { return Math.round(j.read_rps || 0); })));
                    }
                    series.push({ tid: tid, writes: wr, reads: rd });
                });

                let yMax = 1;
                series.forEach(function(s) {
                    s.writes.forEach(function(v) { if (v != null) yMax = Math.max(yMax, v); });
                    if (showReads) {
                        s.reads.forEach(function(v) { if (v != null) yMax = Math.max(yMax, v); });
                    }
                });
                yMax = Math.ceil(yMax * 1.1);

                let svg = "<h4 style='margin-top:0'>Per-tablet write IOPS vs MaxInFlight</h4>";
                svg += "<svg viewBox='0 0 " + width + " " + height +
                    "' width='100%' style='max-width:800px;background:#fff' " +
                    "xmlns='http://www.w3.org/2000/svg'>";
                svg += "<line x1='" + marginLeft + "' y1='" + marginTop + "' x2='" + marginLeft +
                    "' y2='" + baseY + "' stroke='#ccc' stroke-width='1'/>";
                svg += "<line x1='" + marginLeft + "' y1='" + baseY + "' x2='" + (width - marginRight) +
                    "' y2='" + baseY + "' stroke='#ccc' stroke-width='1'/>";

                const yTicks = 5;
                for (let t = 0; t <= yTicks; t++) {
                    const val = Math.round((yMax * t) / yTicks);
                    const y = yAt(val, yMax);
                    svg += "<line x1='" + marginLeft + "' y1='" + y + "' x2='" + (width - marginRight) +
                        "' y2='" + y + "' stroke='#eee' stroke-width='1'/>";
                    svg += "<text x='" + (marginLeft - 6) + "' y='" + (y + 4) +
                        "' text-anchor='end' font-size='10' fill='#666'>" + val + "</text>";
                }
                for (let i = 0; i < n; i++) {
                    svg += "<text x='" + xAt(i) + "' y='" + (baseY + 16) +
                        "' text-anchor='middle' font-size='11' fill='#333'>" + sweepValues[i] + "</text>";
                }

                series.forEach(function(s, si) {
                    const color = nbsTabletPalette(si);
                    const wpts = [];
                    for (let i = 0; i < n; i++) {
                        if (s.writes[i] == null) {
                            continue;
                        }
                        const x = xAt(i), y = yAt(s.writes[i], yMax);
                        wpts.push(x + "," + y);
                        svg += "<circle cx='" + x + "' cy='" + y + "' r='3' fill='" + color + "'/>";
                    }
                    if (wpts.length >= 2) {
                        svg += "<polyline points='" + wpts.join(" ") +
                            "' fill='none' stroke='" + color + "' stroke-width='2'/>";
                    }
                    if (showReads) {
                        const rpts = [];
                        for (let i = 0; i < n; i++) {
                            if (s.reads[i] == null) {
                                continue;
                            }
                            const x = xAt(i), y = yAt(s.reads[i], yMax);
                            rpts.push(x + "," + y);
                        }
                        if (rpts.length >= 2) {
                            svg += "<polyline points='" + rpts.join(" ") +
                                "' fill='none' stroke='" + color +
                                "' stroke-width='1.5' stroke-dasharray='4,3'/>";
                        }
                    }
                    const ly = marginTop + si * 16;
                    svg += "<line x1='" + (width - marginRight + 10) + "' y1='" + ly + "' x2='" +
                        (width - marginRight + 30) + "' y2='" + ly + "' stroke='" + color + "' stroke-width='2'/>";
                    svg += "<text x='" + (width - marginRight + 34) + "' y='" + (ly + 4) +
                        "' font-size='10' fill='#333'>" + s.tid + "</text>";
                });

                if (showReads) {
                    svg += "<text x='" + (width - marginRight + 10) + "' y='" +
                        (marginTop + series.length * 16 + 12) +
                        "' font-size='10' fill='#666'>solid=write dashed=read</text>";
                }

                svg += "</svg>";
                div.innerHTML = svg;
                div.style.display = "";
            }

            // Entry point wired to a card's Run button; guards against starting
            // a second sweep on a card that is already running.
            function nbsTabletSweepInst(inst) {
                const ctx = nbsRunCtx(inst);
                const multi = !!(ctx.reg && ctx.reg.multi);
                if (!multi && !ctx.tabletId) {
                    nbsCardStatus(ctx, "Run error: missing tablet id");
                    return;
                }
                if (ctx.reg && ctx.reg.running) {
                    return;
                }
                nbsTabletSweep(ctx);
            }

            async function nbsTabletSweep(ctx) {
                const multi = !!(ctx.reg && ctx.reg.multi);
                let targets = [];
                let targetsCsv = "";
                if (multi) {
                    targets = nbsRunCollectTargets(ctx);
                    if (targets.length === 0) {
                        nbsCardStatus(ctx, "Run error: add at least one target tablet");
                        return;
                    }
                    targetsCsv = targets.map(function(t) {
                        return t.tid + ":" + t.nid;
                    }).join(",");
                }

                const trialsParsed = nbsTabletParseTrials(ctx);
                if (!trialsParsed.ok) {
                    nbsCardStatus(ctx, "Run error: " + trialsParsed.error);
                    return;
                }
                const trials = trialsParsed.trials;

                const fromVal = ctx.val("inflight-from");
                const toVal   = ctx.val("inflight-to");
                const single  = ctx.val("max-inflight") || "32";
                const sweepValues = nbsTabletBuildSweepValues(fromVal, toVal, single);

                const durationSec = parseInt(ctx.val("duration")) || 0;
                const delayBeforeSec = parseInt(ctx.val("delay-before")) || 0;
                const disableReplication = ctx.checked("disable-replication");
                const readRatioPct = disableReplication ? 0 : (parseInt(
                    ctx.val("read-ratio") || "0") || 0);
                const showReads = readRatioPct > 0;

                nbsTabletRenderDetailSkeleton(ctx, sweepValues, trials, showReads);
                const medianDiv = ctx.el("median-table");
                if (medianDiv) {
                    if (trials > 1) {
                        medianDiv.style.display = "";
                        nbsTabletRenderMedianSkeleton(ctx, sweepValues, showReads);
                    } else {
                        medianDiv.style.display = "none";
                        medianDiv.innerHTML = "";
                    }
                }

                const iopsDiv = ctx.el("iops-plot");
                if (iopsDiv) {
                    iopsDiv.innerHTML = "";
                }
                const latencyDiv = ctx.el("latency-plot");
                if (latencyDiv) {
                    latencyDiv.innerHTML = "";
                }

                // Per-tablet accumulation (multi-tablet card only).
                const perTabletData = {};
                const tabletOrder = [];
                const tabletNode = {};
                if (multi) {
                    targets.forEach(function(t) {
                        tabletOrder.push(t.tid);
                        tabletNode[t.tid] = t.nid;
                        perTabletData[t.tid] = [];
                        for (let i = 0; i < sweepValues.length; i++) {
                            perTabletData[t.tid].push([]);
                        }
                    });
                }

                if (ctx.reg) {
                    ctx.reg.running = true;
                }
                const runBtn = ctx.el("run-btn");
                if (runBtn) {
                    runBtn.disabled = true;
                }

                let statusMsg = "Starting sweep over " + sweepValues.length + " value(s)";
                if (trials > 1) {
                    statusMsg += ", " + trials + " trials each";
                }
                statusMsg += "…";
                nbsCardStatus(ctx, statusMsg);

                const progressDiv = ctx.el("progress");
                const plotData = [];

                try {
                    for (let i = 0; i < sweepValues.length; i++) {
                        const v = sweepValues[i];
                        const trialResults = [];
                        plotData.push({ inflight: v, writes: [], reads: [],
                            writeLat: { p50: [], p95: [], p99: [] },
                            readLat:  { p50: [], p95: [], p99: [] } });

                        for (let t = 0; t < trials; t++) {
                            let statusLine = "MaxInFlight=" + v + " (" + (i + 1) + "/" +
                                sweepValues.length + ")";
                            if (trials > 1) {
                                statusLine += " trial " + (t + 1) + "/" + trials;
                            }
                            statusLine += "...";
                            nbsCardStatus(ctx, statusLine);

                            if (progressDiv) {
                                progressDiv.innerHTML =
                                    "<div class='progress' style='margin-top:8px'>" +
                                    "<div class='progress-bar progress-bar-striped active' " +
                                    "role='progressbar' style='width:0%;min-width:2em'>0%</div>" +
                                    "</div>" +
                                    "<small class='text-muted'>" + statusLine + "</small>";
                                progressDiv.style.display = "";
                            }

                            try {
                                const runInfo = await nbsTabletRunOnce(
                                    ctx, v, multi ? targetsCsv : null);
                                const jr = await nbsTabletPollResult(
                                    runInfo.uuid, durationSec + delayBeforeSec, progressDiv);
                                trialResults.push(jr);
                                if (multi && jr && Array.isArray(jr.tablets)) {
                                    jr.tablets.forEach(function(entry) {
                                        const tid = String(entry.tablet_id);
                                        if (perTabletData[tid] && perTabletData[tid][i]) {
                                            perTabletData[tid][i].push(entry);
                                        }
                                    });
                                }
                                plotData[i].writes.push(Math.round(jr.write_rps || 0));
                                plotData[i].reads.push(Math.round(jr.read_rps || 0));
                                if (jr.write_p50 != null) {
                                    plotData[i].writeLat.p50.push(jr.write_p50);
                                    plotData[i].writeLat.p95.push(jr.write_p95);
                                    plotData[i].writeLat.p99.push(jr.write_p99);
                                }
                                if (jr.read_p50 != null && (jr.read_p50 || jr.read_p95 || jr.read_p99)) {
                                    plotData[i].readLat.p50.push(jr.read_p50);
                                    plotData[i].readLat.p95.push(jr.read_p95);
                                    plotData[i].readLat.p99.push(jr.read_p99);
                                }
                                if (trials > 1) {
                                    nbsTabletFillTrialRows(ctx, i, t, jr);
                                } else {
                                    nbsTabletFillTableColumn(ctx, i, jr);
                                }
                            } catch (e) {
                                if (trials > 1) {
                                    nbsTabletSetTrialError(ctx, i, t, String(e));
                                } else {
                                    nbsTabletSetColumnError(ctx, i, String(e));
                                }
                            }
                            if (progressDiv) {
                                progressDiv.style.display = "none";
                            }
                        }

                        if (trials > 1) {
                            const medianJr = nbsTabletMedianByWriteIops(trialResults);
                            if (medianJr) {
                                nbsTabletFillMedianRow(ctx, i, medianJr);
                            } else {
                                nbsTabletSetMedianError(ctx, i, "ERR");
                            }
                        }
                    }
                    if (multi) {
                        nbsTabletRenderPerTabletTables(
                            ctx, sweepValues, tabletOrder, tabletNode, perTabletData, showReads);
                        nbsTabletRenderPerTabletPlot(
                            ctx, sweepValues, tabletOrder, tabletNode, perTabletData, showReads);
                    }
                    nbsTabletRenderIopsPlot(ctx, sweepValues, plotData);
                    nbsTabletRenderLatencyPlot(ctx, sweepValues, plotData);
                    nbsCardStatus(ctx, "Sweep complete.");
                } finally {
                    if (ctx.reg) {
                        ctx.reg.running = false;
                    }
                    if (runBtn) {
                        runBtn.disabled = false;
                    }
                    if (progressDiv) {
                        progressDiv.style.display = "none";
                    }
                }
            }
        </script>
    )___";
    HTML(str) {
        FORM() {
            str << R"___(
                <div class='row'>
                    <div class='col-md-12'>
                        <div class='panel panel-default'>
                            <div class='panel-heading'>
                                <a data-toggle='collapse' href='#nbs-create-body' style='cursor:pointer'>
                                    <strong>Create tablet</strong>
                                </a>
                            </div>
                            <div id='nbs-create-body' class='panel-body collapse'>
                                <div class='form-group'>
                                    <label for='nbs-tablet-owner-idx'>Owner index:</label>
                                    <input id='nbs-tablet-owner-idx' name='owner_idx' type='number' min='1' step='1' value='1' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-ddisk-pool'>DDiskPoolName:</label>
                                    <input id='nbs-tablet-create-ddisk-pool' class='form-control nbs-tablet-builder' type='text' value='ddp1' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-pb-pool'>PersistentBufferDDiskPoolName:</label>
                                    <input id='nbs-tablet-create-pb-pool' class='form-control nbs-tablet-builder' type='text' value='ddp1' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-num-dbg'>NumDirectBlockGroups:</label>
                                    <input id='nbs-tablet-create-num-dbg' class='form-control nbs-tablet-builder' type='number' min='1' step='1' value='1' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-hosts-per-dbg'>HostsPerDbg:</label>
                                    <input id='nbs-tablet-create-hosts-per-dbg' class='form-control nbs-tablet-builder' type='number' min='3' max='5' step='1' value='5' />
                                    <p class='help-block'>Must match DDisk pool geometry (NumFailDomainsPerFailRealm).</p>
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-target-vchunks'>TargetNumVChunks:</label>
                                    <input id='nbs-tablet-create-target-vchunks' class='form-control nbs-tablet-builder' type='number' min='1' step='1' value='1' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-vchunk-size'>VChunkSizeBytes:</label>
                                    <input id='nbs-tablet-create-vchunk-size' class='form-control nbs-tablet-builder' type='number' min='4096' step='4096' value='134217728' />
                                </div>
                                <div class='form-group'>
                                    <label for='nbs-tablet-create-storage-pools'>TabletStoragePools / Hive bindings (one per line, optional):</label>
                                    <textarea id='nbs-tablet-create-storage-pools' class='form-control nbs-tablet-builder' rows='3'></textarea>
                                    <p class='help-block'>Leave empty to auto-use current tenant storage pools.</p>
                                </div>
                                <details class='form-group'>
                                    <summary>Advanced</summary>
                                    <div class='checkbox'>
                                        <label>
                                            <input id='nbs-tablet-create-non-default' class='nbs-tablet-builder' type='checkbox' />
                                            Include only non-default fields in generated AllocConfig
                                        </label>
                                    </div>
                                    <label for='nbs-tablet-create-proto-preview'>Generated AllocConfig (preview):</label>
                                    <textarea id='nbs-tablet-create-proto-preview' class='form-control' rows='10' readonly='readonly'></textarea>
                                </details>
                                <div class='form-group'>
                                    <button type='button' onClick='nbsTabletCreate()' class='btn btn-default'>Create</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )___";
            str << nbsTabletListHtml;
            // ── Run workload cards ──────────────────────────────────────────
            // One independent run card is created per tablet (via the per-row
            // "Run" button); each card has its own form, progress and results
            // and can run a sweep simultaneously with the others.
            str << R"___(
                <div style='margin-top:16px'>
                    <button type='button' class='btn btn-info' onclick='nbsTabletOpenMulti()'>Run multi-tablet load (combined)</button>
                    <span class='text-muted' style='margin-left:8px'>Loads several tablets at once and shows combined + per-tablet results. Use the per-row Run for an isolated single-tablet card.</span>
                </div>
                <div id='nbs-run-cards'></div>
            )___";
            DIV_CLASS("form-group") {
                str << "<p id='nbs-tablet-status'></p>";
                str << "<div id='nbs-tablet-result'></div>";
            }
        }
    }
}

} // namespace NKikimr::NNbsDbgLike
