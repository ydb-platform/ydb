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
                   "<th>TabletId</th><th>Owner index</th><th>TabletState</th>"
                   "<th>PoolName</th><th>NumDirectBlockGroups</th><th>Actions</th>"
                   "</tr></thead><tbody>";
            for (const auto& row : Accum) {
                const ui64 tid = row.Hive.GetTabletID();
                const ui64 ownerIdx = row.Hive.HasTabletOwner()
                    ? row.Hive.GetTabletOwner().GetOwnerIdx()
                    : 0;
                const TString stateLabel = BuildStateLabel(row.Hive);
                str << "<tr>";
                str << "<td><a href='/tablets?TabletID=" << tid << "'>" << tid << "</a></td>";
                str << "<td>" << ownerIdx << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(stateLabel) << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(row.PoolCell) << "</td>";
                str << "<td>" << NbsTabletHtmlEscape(row.NumDbgCell) << "</td>";
                str << "<td>";
                str << "<button type='button' class='btn btn-xs btn-primary' "
                       "onClick='nbsTabletPrepareRun(\"" << tid << "\")'>Run</button> ";
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
                if (!firstRow) {
                    str << ",";
                }
                firstRow = false;
                str << "{tid:\"" << tid << "\",oi:" << ownerIdx << "}";
            }
            str << "];"
                   "var maxOi=0;"
                   "rows.forEach(function(r){if(r.oi>maxOi)maxOi=r.oi;});"
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

            function nbsRunDisableReplicationChanged(cb) {
                const readRatio = $("#nbs-run-read-ratio");
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
                const tabletId = nbsTabletFieldInt("nbs-tablet-create-tablet-id", "TabletId", 1);
                if (!tabletId.ok) {
                    return tabletId;
                }
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

                addNumber("TabletId", tabletId.value, 0, true);
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

            // ── Run workload / sweep ────────────────────────────────────────

            function nbsTabletPrepareRun(tabletId) {
                var inp = document.getElementById("nbs-tablet-run-tablet-id");
                if (inp) inp.value = tabletId;
                var lbl = document.getElementById("nbs-run-target-label");
                if (lbl) lbl.textContent = "Target tablet: " + tabletId;
                var panel = document.getElementById("nbs-run-panel");
                if (panel) {
                    panel.style.display = "";
                    panel.scrollIntoView({behavior: "smooth", block: "start"});
                }
            }

            function nbsTabletRunOnce(tabletId, maxInFlight) {
                return new Promise(function(resolve, reject) {
                    const params = {
                        mode:                    "tablet_run",
                        tablet_id:               String(tabletId),
                        tag:                     nbsTabletTrim($("#nbs-run-tag").val()) || "0",
                        duration_seconds:        nbsTabletTrim($("#nbs-run-duration").val()) || "0",
                        delay_before_seconds:    nbsTabletTrim($("#nbs-run-delay-before").val()) || "15",
                        max_in_flight:           String(maxInFlight),
                        read_ratio_pct:          $("#nbs-run-disable-replication").is(":checked") ? "0" : (nbsTabletTrim($("#nbs-run-read-ratio").val()) || "0"),
                        read_write_size_kib:     nbsTabletTrim($("#nbs-run-size-kib").val()) || "4",
                        sequential:              $("#nbs-run-sequential").is(":checked") ? "1" : "0",
                        num_dbg_to_use:          nbsTabletTrim($("#nbs-run-num-dbg").val()) || "0",
                        disable_replication:     $("#nbs-run-disable-replication").is(":checked") ? "1" : "0"
                    };
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

            function nbsTabletParseTrials() {
                const raw = nbsTabletTrim($("#nbs-run-trials").val());
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

            function nbsTabletRenderTableSkeleton(sweepValues) {
                const tbl = document.getElementById("nbs-run-result-table");
                if (!tbl) {
                    return;
                }

                let html = "<table class='table table-condensed table-bordered' style='margin-top:12px'>";
                html += "<thead><tr>";
                html += "<th rowspan='2' style='vertical-align:middle'>MaxInFlight</th>";
                html += "<th rowspan='2' style='vertical-align:middle'>Direction</th>";
                html += "<th>IOPS</th><th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                html += "</tr></thead>";
                html += "<tbody>";
                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    html += "<tr id='nbs-sweep-w-" + i + "'>";
                    html += "<td rowspan='2' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                    html += "<td>Writes</td>";
                    html += "<td id='nbs-sw-wiops-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-wp50-"  + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-wp95-"  + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-wp99-"  + i + "'>&#9711;</td>";
                    html += "</tr>";
                    html += "<tr id='nbs-sweep-r-" + i + "'>";
                    html += "<td>Reads</td>";
                    html += "<td id='nbs-sw-riops-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-rp50-"  + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-rp95-"  + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sw-rp99-"  + i + "'>&#9711;</td>";
                    html += "</tr>";
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderMultiTrialSkeleton(sweepValues, trials) {
                const tbl = document.getElementById("nbs-run-result-table");
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
                    const rowSpan = 2 * trials;
                    for (let t = 0; t < trials; t++) {
                        html += "<tr id='nbs-sweep-w-" + i + "-" + t + "'>";
                        if (t === 0) {
                            html += "<td rowspan='" + rowSpan +
                                "' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                        }
                        html += "<td rowspan='2' style='vertical-align:middle'>" + (t + 1) + "</td>";
                        html += "<td>Writes</td>";
                        html += "<td id='nbs-sw-wiops-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-wp50-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-wp95-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-wp99-" + i + "-" + t + "'>&#9711;</td>";
                        html += "</tr>";
                        html += "<tr id='nbs-sweep-r-" + i + "-" + t + "'>";
                        html += "<td>Reads</td>";
                        html += "<td id='nbs-sw-riops-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-rp50-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-rp95-" + i + "-" + t + "'>&#9711;</td>";
                        html += "<td id='nbs-sw-rp99-" + i + "-" + t + "'>&#9711;</td>";
                        html += "</tr>";
                    }
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderMedianSkeleton(sweepValues) {
                const tbl = document.getElementById("nbs-run-median-table");
                if (!tbl) {
                    return;
                }

                let html = "<h4 style='margin-top:0'>Median trial (by write IOPS)</h4>";
                html += "<table class='table table-condensed table-bordered'>";
                html += "<thead><tr>";
                html += "<th rowspan='2' style='vertical-align:middle'>MaxInFlight</th>";
                html += "<th rowspan='2' style='vertical-align:middle'>Direction</th>";
                html += "<th>IOPS</th><th>p50 us</th><th>p95 us</th><th>p99 us</th>";
                html += "</tr></thead><tbody>";
                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    html += "<tr id='nbs-median-w-" + i + "'>";
                    html += "<td rowspan='2' style='vertical-align:middle;font-weight:bold'>" + v + "</td>";
                    html += "<td>Writes</td>";
                    html += "<td id='nbs-sm-wiops-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-wp50-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-wp95-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-wp99-" + i + "'>&#9711;</td>";
                    html += "</tr>";
                    html += "<tr id='nbs-median-r-" + i + "'>";
                    html += "<td>Reads</td>";
                    html += "<td id='nbs-sm-riops-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-rp50-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-rp95-" + i + "'>&#9711;</td>";
                    html += "<td id='nbs-sm-rp99-" + i + "'>&#9711;</td>";
                    html += "</tr>";
                }
                html += "</tbody></table>";
                tbl.innerHTML = html;
            }

            function nbsTabletRenderDetailSkeleton(sweepValues, trials) {
                if (trials > 1) {
                    nbsTabletRenderMultiTrialSkeleton(sweepValues, trials);
                } else {
                    nbsTabletRenderTableSkeleton(sweepValues);
                }
            }

            function nbsTabletFillMetricsCells(prefix, idx, suffix, jr) {
                function set(id, val) {
                    const el = document.getElementById(id);
                    if (el) {
                        el.textContent = val;
                    }
                }
                const sfx = suffix !== undefined ? ("-" + suffix) : "";
                const wIops = jr.write_rps !== undefined ? Math.round(jr.write_rps) : "-";
                const rIops = jr.read_rps !== undefined ? Math.round(jr.read_rps) : "-";
                set(prefix + "wiops-" + idx + sfx, wIops);
                set(prefix + "wp50-" + idx + sfx, nbsTabletFmtUs(jr.write_p50));
                set(prefix + "wp95-" + idx + sfx, nbsTabletFmtUs(jr.write_p95));
                set(prefix + "wp99-" + idx + sfx, nbsTabletFmtUs(jr.write_p99));
                set(prefix + "riops-" + idx + sfx, rIops);
                set(prefix + "rp50-" + idx + sfx, nbsTabletFmtUs(jr.read_p50));
                set(prefix + "rp95-" + idx + sfx, nbsTabletFmtUs(jr.read_p95));
                set(prefix + "rp99-" + idx + sfx, nbsTabletFmtUs(jr.read_p99));
            }

            function nbsTabletSetMetricsError(prefix, idx, suffix, msg) {
                function set(id, val) {
                    const el = document.getElementById(id);
                    if (el) {
                        el.textContent = val;
                    }
                }
                const sfx = suffix !== undefined ? ("-" + suffix) : "";
                set(prefix + "wiops-" + idx + sfx, "ERR");
                set(prefix + "wp50-" + idx + sfx, msg || "ERR");
                set(prefix + "wp95-" + idx + sfx, "");
                set(prefix + "wp99-" + idx + sfx, "");
                set(prefix + "riops-" + idx + sfx, "ERR");
                set(prefix + "rp50-" + idx + sfx, "");
                set(prefix + "rp95-" + idx + sfx, "");
                set(prefix + "rp99-" + idx + sfx, "");
            }

            function nbsTabletFillTableColumn(idx, jr) {
                nbsTabletFillMetricsCells("nbs-sw-", idx, undefined, jr);
            }

            function nbsTabletFillTrialRows(idx, trialIdx, jr) {
                nbsTabletFillMetricsCells("nbs-sw-", idx, trialIdx, jr);
            }

            function nbsTabletFillMedianRow(idx, jr) {
                nbsTabletFillMetricsCells("nbs-sm-", idx, undefined, jr);
            }

            function nbsTabletSetColumnError(idx, msg) {
                nbsTabletSetMetricsError("nbs-sw-", idx, undefined, msg);
            }

            function nbsTabletSetTrialError(idx, trialIdx, msg) {
                nbsTabletSetMetricsError("nbs-sw-", idx, trialIdx, msg);
            }

            function nbsTabletSetMedianError(idx, msg) {
                nbsTabletSetMetricsError("nbs-sm-", idx, undefined, msg);
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

            function nbsTabletRenderIopsPlot(sweepValues, plotData) {
                const plotDiv = document.getElementById("nbs-run-iops-plot");
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

            function nbsTabletRefreshList() {
                $.get(window.location.pathname + "?mode=tablet_list", function(html) {
                    $("#nbs-tablet-list-container").html(html);
                });
            }

            async function nbsTabletSweep() {
                const tabletId = nbsTabletTrim(
                    $("#nbs-tablet-run-tablet-id").val());
                if (!tabletId) {
                    nbsTabletStatus("Run error: select a target tablet first");
                    return;
                }
                const trialsParsed = nbsTabletParseTrials();
                if (!trialsParsed.ok) {
                    nbsTabletStatus("Run error: " + trialsParsed.error);
                    return;
                }
                const trials = trialsParsed.trials;

                const fromVal = nbsTabletTrim($("#nbs-run-inflight-from").val());
                const toVal   = nbsTabletTrim($("#nbs-run-inflight-to").val());
                const single  = nbsTabletTrim($("#nbs-run-max-inflight").val()) || "32";
                const sweepValues = nbsTabletBuildSweepValues(fromVal, toVal, single);

                const durationSec = parseInt(
                    nbsTabletTrim($("#nbs-run-duration").val())) || 0;
                const delayBeforeSec = parseInt(
                    nbsTabletTrim($("#nbs-run-delay-before").val())) || 0;

                nbsTabletRenderDetailSkeleton(sweepValues, trials);
                const medianDiv = document.getElementById("nbs-run-median-table");
                if (medianDiv) {
                    if (trials > 1) {
                        medianDiv.style.display = "";
                        nbsTabletRenderMedianSkeleton(sweepValues);
                    } else {
                        medianDiv.style.display = "none";
                        medianDiv.innerHTML = "";
                    }
                }

                const plotDiv = document.getElementById("nbs-run-iops-plot");
                if (plotDiv) {
                    plotDiv.innerHTML = "";
                }

                let statusMsg = "Starting sweep over " + sweepValues.length + " value(s)";
                if (trials > 1) {
                    statusMsg += ", " + trials + " trials each";
                }
                statusMsg += "…";
                nbsTabletStatus(statusMsg);

                const progressDiv = document.getElementById("nbs-run-progress");
                const plotData = [];

                for (let i = 0; i < sweepValues.length; i++) {
                    const v = sweepValues[i];
                    const trialResults = [];
                    plotData.push({ inflight: v, writes: [], reads: [] });

                    for (let t = 0; t < trials; t++) {
                        let statusLine = "MaxInFlight=" + v + " (" + (i + 1) + "/" +
                            sweepValues.length + ")";
                        if (trials > 1) {
                            statusLine += " trial " + (t + 1) + "/" + trials;
                        }
                        statusLine += "...";
                        nbsTabletStatus(statusLine);

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
                            const runInfo = await nbsTabletRunOnce(tabletId, v);
                            const jr = await nbsTabletPollResult(
                                runInfo.uuid, durationSec + delayBeforeSec, progressDiv);
                            trialResults.push(jr);
                            plotData[i].writes.push(Math.round(jr.write_rps || 0));
                            plotData[i].reads.push(Math.round(jr.read_rps || 0));
                            if (trials > 1) {
                                nbsTabletFillTrialRows(i, t, jr);
                            } else {
                                nbsTabletFillTableColumn(i, jr);
                            }
                        } catch (e) {
                            if (trials > 1) {
                                nbsTabletSetTrialError(i, t, String(e));
                            } else {
                                nbsTabletSetColumnError(i, String(e));
                            }
                        }
                        if (progressDiv) {
                            progressDiv.style.display = "none";
                        }
                    }

                    if (trials > 1) {
                        const medianJr = nbsTabletMedianByWriteIops(trialResults);
                        if (medianJr) {
                            nbsTabletFillMedianRow(i, medianJr);
                        } else {
                            nbsTabletSetMedianError(i, "ERR");
                        }
                    }
                }
                nbsTabletRenderIopsPlot(sweepValues, plotData);
                nbsTabletStatus("Sweep complete.");
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
                                    <div class='form-group'>
                                        <label for='nbs-tablet-create-tablet-id'>BSC TabletId:</label>
                                        <input id='nbs-tablet-create-tablet-id' class='form-control nbs-tablet-builder' type='number' min='1' step='1' value='9000' />
                                    </div>
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
            // ── Run workload panel ──────────────────────────────────────────
            str << R"___(
                <div id='nbs-run-panel' class='row' style='margin-top:16px;display:none'>
                    <div class='col-md-12'>
                        <div class='panel panel-primary'>
                            <div class='panel-heading'><strong>Run workload</strong></div>
                            <div class='panel-body'>
                                <input type='hidden' id='nbs-tablet-run-tablet-id'>
                                <div id='nbs-run-target-label' class='alert alert-info' style='margin-bottom:8px'></div>
                                <div class='row'>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-tag'>Tag (0 = auto-assign):</label>
                                            <input id='nbs-run-tag' class='form-control' type='number' min='0' step='1' value='0' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-duration'>DurationSeconds:</label>
                                            <input id='nbs-run-duration' class='form-control' type='number' min='0' step='1' value='60' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-delay-before'>Measure after (warmup s):</label>
                                            <input id='nbs-run-delay-before' class='form-control' type='number' min='0' step='1' value='15' />
                                        </div>
                                    </div>
                                </div>
                                <div class='row'>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-trials'>Trials per InFlight:</label>
                                            <input id='nbs-run-trials' class='form-control' type='number' min='1' step='2' value='1' />
                                            <p class='help-block'>1 = single run. Values &gt; 1 must be odd; each InFlight is run that many times.</p>
                                        </div>
                                    </div>
                                </div>
                                <div class='row'>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-max-inflight'>MaxInFlight (single run):</label>
                                            <input id='nbs-run-max-inflight' class='form-control' type='number' min='1' step='1' value='32' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-inflight-from'>InFlightFrom (sweep start):</label>
                                            <input id='nbs-run-inflight-from' class='form-control' type='number' min='1' step='1' placeholder='e.g. 1' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-inflight-to'>InFlightTo (sweep end):</label>
                                            <input id='nbs-run-inflight-to' class='form-control' type='number' min='1' step='1' placeholder='e.g. 128' />
                                            <p class='help-block'>If both From/To set, sweeps MaxInFlight x2 per step (From should be a power of 2; otherwise the last value may be below To).</p>
                                        </div>
                                    </div>
                                </div>
                                <div class='row'>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-read-ratio'>ReadRatioPct (0-100):</label>
                                            <input id='nbs-run-read-ratio' class='form-control' type='number' min='0' max='100' step='1' value='0' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-size-kib'>ReadWriteSizeKiB:</label>
                                            <input id='nbs-run-size-kib' class='form-control' type='number' min='4' step='4' value='4' />
                                        </div>
                                    </div>
                                    <div class='col-sm-4'>
                                        <div class='form-group'>
                                            <label for='nbs-run-num-dbg'>NumDirectBlockGroupsToUse (0=all):</label>
                                            <input id='nbs-run-num-dbg' class='form-control' type='number' min='0' step='1' value='0' />
                                        </div>
                                    </div>
                                </div>
                                <div class='form-group'>
                                    <div class='checkbox'>
                                        <label>
                                            <input id='nbs-run-sequential' type='checkbox' />
                                            Sequential (round-robin address space instead of random)
                                        </label>
                                    </div>
                                </div>
                                <div class='form-group'>
                                    <div class='checkbox'>
                                        <label>
                                            <input id='nbs-run-disable-replication' type='checkbox' onchange='nbsRunDisableReplicationChanged(this)' />
                                            Disable replication (single-PB write, skip DDisk flush)
                                        </label>
                                    </div>
                                </div>
                                <div class='form-group'>
                                    <button type='button' onClick='nbsTabletSweep()' class='btn btn-primary'>Run</button>
                                </div>
                                <div id='nbs-run-progress' style='display:none'></div>
                                <div id='nbs-run-result-table'></div>
                                <div id='nbs-run-median-table' style='margin-top:16px;display:none'></div>
                                <div id='nbs-run-iops-plot' style='margin-top:16px'></div>
                            </div>
                        </div>
                    </div>
                </div>
            )___";
            DIV_CLASS("form-group") {
                str << "<p id='nbs-tablet-status'></p>";
                str << "<div id='nbs-tablet-result'></div>";
            }
        }
    }
}

} // namespace NKikimr::NNbsDbgLike
