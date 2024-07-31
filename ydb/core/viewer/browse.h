#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "browse_events.h"
#include "viewer.h"
#include "wb_aggregate.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TBrowse : public TActorBootstrapped<TBrowse> {
    static const bool WithRetry = false;
    using TBase = TActorBootstrapped<TBrowse>;
    const IViewer* Viewer;
    TActorId Owner;
    ui32 Requests = 0;
    ui32 Responses = 0;
    bool Final = false;
    TString Path;
    NKikimrViewer::TBrowseInfo BrowseInfo;
    NKikimrViewer::TMetaInfo MetaInfo;
    TString CurrentPath; // TStringBuf?
    NKikimrViewer::EObjectType CurrentType;
    THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    THashSet<TActorId> Handlers;
    TActorId TxProxy = MakeTxProxyID();

public:
    IViewer::TBrowseContext BrowseContext;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TBrowse(const IViewer* viewer, const TActorId& owner, const TString& path, const TString& userToken)
        : Viewer(viewer)
        , Owner(owner)
        , Path(path)
        , BrowseContext(owner, userToken)
    {}

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }
        return clientConfig;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static NTabletPipe::TClientConfig clientConfig = InitPipeClientConfig();
        return clientConfig;
    }

    static NKikimrViewer::EObjectType GetPathTypeFromSchemeShardType(NKikimrSchemeOp::EPathType type) {
        switch (type) {
        case NKikimrSchemeOp::EPathType::EPathTypeDir:
        case NKikimrSchemeOp::EPathType::EPathTypeColumnStore: // TODO
            return NKikimrViewer::EObjectType::Directory;
        case NKikimrSchemeOp::EPathType::EPathTypeRtmrVolume:
            return NKikimrViewer::EObjectType::RtmrVolume;
        case NKikimrSchemeOp::EPathType::EPathTypeTable:
        case NKikimrSchemeOp::EPathType::EPathTypeColumnTable: // TODO
            return NKikimrViewer::EObjectType::Table;
        case NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup:
            return NKikimrViewer::EObjectType::Topic;
        case NKikimrSchemeOp::EPathType::EPathTypeSubDomain:
            return NKikimrViewer::EObjectType::SubDomain;
        case NKikimrSchemeOp::EPathType::EPathTypeBlockStoreVolume:
            return NKikimrViewer::EObjectType::BlockStoreVolume;
        case NKikimrSchemeOp::EPathType::EPathTypeFileStore:
            return NKikimrViewer::EObjectType::FileStore;
        case NKikimrSchemeOp::EPathType::EPathTypeKesus:
            return NKikimrViewer::EObjectType::Kesus;
        case NKikimrSchemeOp::EPathType::EPathTypeSolomonVolume:
            return NKikimrViewer::EObjectType::SolomonVolume;
        case NKikimrSchemeOp::EPathType::EPathTypeCdcStream:
            return NKikimrViewer::EObjectType::CdcStream;
        case NKikimrSchemeOp::EPathType::EPathTypeSequence:
            return NKikimrViewer::EObjectType::Sequence;
        case NKikimrSchemeOp::EPathType::EPathTypeReplication:
            return NKikimrViewer::EObjectType::Replication;
        case NKikimrSchemeOp::EPathType::EPathTypeBlobDepot:
            return NKikimrViewer::EObjectType::BlobDepot;
        case NKikimrSchemeOp::EPathType::EPathTypeExternalTable:
            return NKikimrViewer::EObjectType::ExternalTable;
        case NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource:
            return NKikimrViewer::EObjectType::ExternalDataSource;
        case NKikimrSchemeOp::EPathType::EPathTypeView:
            return NKikimrViewer::EObjectType::View;
        case NKikimrSchemeOp::EPathType::EPathTypeResourcePool:
            return NKikimrViewer::EObjectType::ResourcePool;
        case NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain:
        case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
        case NKikimrSchemeOp::EPathType::EPathTypeInvalid:
            Y_UNREACHABLE();
        }
        return NKikimrViewer::EObjectType::Unknown;
    }

    TString GetNextName() const {
        TString nextName;
        if (CurrentPath.size() < Path.size()) {
            auto pos = CurrentPath.size();
            if (Path[pos] == '/') {
                ++pos;
            }
            if (CurrentType == NKikimrViewer::RtmrTables) {
                nextName = Path.substr(pos);
            } else {
                auto end_pos = pos;
                while (end_pos < Path.size() && Path[end_pos] != '/') {
                    ++end_pos;
                }

                nextName = Path.substr(pos, end_pos - pos);
            }
        }
        return nextName;
    }

    void FillCommonData(NKikimrViewer::TBrowseInfo& browseInfo, NKikimrViewer::TMetaInfo& metaInfo) {
        browseInfo.SetPath(BrowseContext.Path);
        browseInfo.SetName(BrowseContext.GetMyName());
        browseInfo.SetType(BrowseContext.GetMyType());
        NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
        pbCommon.SetPath(BrowseContext.Path);
        pbCommon.SetType(BrowseContext.GetMyType());
        if (DescribeResult != nullptr) {
            const auto& pbRecord(DescribeResult->GetRecord());
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasSelf()) {
                    const auto& pbSelf(pbPathDescription.GetSelf());
                    pbCommon.SetOwner(pbSelf.GetOwner());
                }
                if (pbPathDescription.GetSelf().HasACL()) {
                    NACLib::TACL acl(pbPathDescription.GetSelf().GetACL());
                    auto& aces(acl.GetACE());
                    for (auto it = aces.begin(); it != aces.end(); ++it) {
                        const NACLibProto::TACE& ace = *it;
                        auto& pbAce = *pbCommon.AddACL();
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Deny) {
                            pbAce.SetAccessType("Deny");
                        } else
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Allow) {
                            pbAce.SetAccessType("Allow");
                        }
                        auto ar = ace.GetAccessRight();
                        if ((ar & NACLib::EAccessRights::SelectRow) != 0) {
                            pbAce.AddAccessRights("SelectRow");
                        }
                        if ((ar & NACLib::EAccessRights::UpdateRow) != 0) {
                            pbAce.AddAccessRights("UpdateRow");
                        }
                        if ((ar & NACLib::EAccessRights::EraseRow) != 0) {
                            pbAce.AddAccessRights("EraseRow");
                        }
                        if ((ar & NACLib::EAccessRights::ReadAttributes) != 0) {
                            pbAce.AddAccessRights("ReadAttributes");
                        }
                        if ((ar & NACLib::EAccessRights::WriteAttributes) != 0) {
                            pbAce.AddAccessRights("WriteAttributes");
                        }
                        if ((ar & NACLib::EAccessRights::CreateDirectory) != 0) {
                            pbAce.AddAccessRights("CreateDirectory");
                        }
                        if ((ar & NACLib::EAccessRights::CreateTable) != 0) {
                            pbAce.AddAccessRights("CreateTable");
                        }
                        if ((ar & NACLib::EAccessRights::CreateQueue) != 0) {
                            pbAce.AddAccessRights("CreateQueue");
                        }
                        if ((ar & NACLib::EAccessRights::RemoveSchema) != 0) {
                            pbAce.AddAccessRights("RemoveSchema");
                        }
                        if ((ar & NACLib::EAccessRights::DescribeSchema) != 0) {
                            pbAce.AddAccessRights("DescribeSchema");
                        }
                        if ((ar & NACLib::EAccessRights::AlterSchema) != 0) {
                            pbAce.AddAccessRights("AlterSchema");
                        }
                        pbAce.SetSubject(ace.GetSID());
                        auto inht = ace.GetInheritanceType();
                        if ((inht & NACLib::EInheritanceType::InheritObject) != 0) {
                            pbAce.AddInheritanceType("InheritObject");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritContainer) != 0) {
                            pbAce.AddInheritanceType("InheritContainer");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritOnly) != 0) {
                            pbAce.AddInheritanceType("InheritOnly");
                        }
                    }
                }
            }
        }
    }

    void StepOnPath(const TActorContext& ctx, const TString& currentName) {
        switch (CurrentType) {
        case NKikimrViewer::EObjectType::Unknown:
        case NKikimrViewer::EObjectType::Root:
        case NKikimrViewer::EObjectType::Directory:
        case NKikimrViewer::EObjectType::Table:
            {
                const auto* domainsInfo = AppData(ctx)->DomainsInfo.Get();
                if (!domainsInfo || !TxProxy) {
                    break;
                }
                bool hasTxAllocators = false;
                if (const auto& domain = domainsInfo->Domain) {
                    if (!domain->TxAllocators.empty()) {
                        hasTxAllocators = true;
                        break;
                    }
                }
                if (!hasTxAllocators) {
                    break;
                }
                THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
                if (!BrowseContext.UserToken.empty()) {
                    request->Record.SetUserToken(BrowseContext.UserToken);
                }
                NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
                record->SetPath(CurrentPath);
                ctx.Send(TxProxy, request.Release(), IEventHandle::FlagTrackDelivery);
                ++Requests;
                ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(TxProxy, TEvTxUserProxy::EvNavigate));
            }
            break;
        default:
            break;
        };

        BrowseContext.Path = CurrentPath;
        BrowseContext.Paths.emplace_back(IViewer::TBrowseContext::TPathEntry({CurrentType, currentName}));

        auto handlers = Viewer->GetVirtualHandlers(CurrentType, CurrentPath);
        for (const auto* handler : handlers) {
            if (handler->BrowseHandler) {
                TActorId browseActor = ctx.RegisterWithSameMailbox(handler->BrowseHandler(ctx.SelfID, BrowseContext));
                Handlers.insert(browseActor);
                ++Requests;
            }
        }

        if (Requests == Responses) {
            return ReplyAndDie(ctx);
        }
    }

    void NextStep(const TActorContext& ctx) {
        TString currentName = GetNextName();
        if (currentName.empty()) {
            return ReplyAndDie(ctx);
        }
        for (const auto& pbChild : BrowseInfo.GetChildren()) {
            if (pbChild.GetName() == currentName) {
                if (!CurrentPath.EndsWith('/')) {
                    CurrentPath += '/';
                }
                CurrentPath += currentName;
                CurrentType = pbChild.GetType();
                Final = pbChild.GetFinal();
                BrowseInfo.Clear();
                MetaInfo.Clear();
                DescribeResult.Reset();
                return StepOnPath(ctx, currentName);
            }
        }
        return HandleBadRequest(ctx, "The path is not found");
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx) {
        DescribeResult.Reset(ev->Release());
        ++Responses;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(TxProxy, TEvTxUserProxy::EvNavigate));
        const auto& pbRecord(DescribeResult->GetRecord());
        if (pbRecord.GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasSelf()) {
                    const auto& pbChildren(pbPathDescription.GetChildren());
                    for (const auto& pbSrcChild : pbChildren) {
                        auto& pbDstChild = *BrowseInfo.AddChildren();
                        pbDstChild.SetType(GetPathTypeFromSchemeShardType(pbSrcChild.GetPathType()));
                        pbDstChild.SetName(pbSrcChild.GetName());
                    }
                }
            }
        } else {
            return HandleBadRequest(ctx, "Error getting schema information");
        }
        if (Responses == Requests) {
            NextStep(ctx);
        }
    }

    void Handle(NViewerEvents::TEvBrowseResponse::TPtr &ev, const TActorContext &ctx) {
        BrowseInfo.MergeFrom(ev->Get()->BrowseInfo);
        MetaInfo.MergeFrom(ev->Get()->MetaInfo);
        ++Responses;
        Handlers.erase(ev->Sender);
        if (Final) {
            // TODO(xenoxeno): it could be a little bit more effective
            BrowseInfo.ClearChildren();
        }
        if (Responses == Requests) {
            NextStep(ctx);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        if (Path.empty()) {
            HandleBadRequest(ctx, "The path is empty");
            return;
        }
        if (!Path.StartsWith('/')) {
            HandleBadRequest(ctx, "The path should start from /");
            return;
        }
        CurrentPath = Path.substr(0, 1);
        CurrentType = NKikimrViewer::EObjectType::Root;
        StepOnPath(ctx, "/");
        Become(&TThis::StateWork);
    }

    void Die(const TActorContext& ctx) override {
        for (const TActorId& actor : Handlers) {
            ctx.Send(actor, new TEvents::TEvPoisonPill());
        }
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(NViewerEvents::TEvBrowseResponse, Handle);
            CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
        }
    }

    void ReplyAndDie(const TActorContext &ctx) {
        FillCommonData(BrowseInfo, MetaInfo);
        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(BrowseInfo), std::move(MetaInfo)));
        Die(ctx);
    }

    void HandleBadRequest(const TActorContext& ctx, const TString& error) {
        ctx.Send(Owner, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandlePoisonPill(const TActorContext& ctx) {
        Die(ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->SourceType == TEvTxUserProxy::EvNavigate) {
            ++Responses;
            ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(TxProxy, TEvTxUserProxy::EvNavigate));
            TxProxy = TActorId(); // do not query this TxProxy any more
            if (Responses == Requests) {
                NextStep(ctx);
            }
        }
    }
};

class TBrowseTabletsCommon : public TActorBootstrapped<TBrowseTabletsCommon> {
protected:
    static const bool WithRetry = false;
    using TBase = TActorBootstrapped<TBrowseTabletsCommon>;
    TActorId Owner;
    THashMap<TTabletId, TActorId> PipeClients;
    TSet<TString> Consumers;
    TVector<ui64> Tablets;
    ui32 Requests = 0;
    ui32 Responses = 0;
    THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TString Path;
    IViewer::TBrowseContext BrowseContext;
    TMap<TTabletId, THolder<TEvTablet::TEvGetCountersResponse>> TabletCountersResults;
    THashMap<ui32, THolder<NKikimrBlobStorage::TEvResponseBSControllerInfo::TBSGroupInfo>> GroupInfo;
    THashSet<ui32> ErasureSpecies;
    THashSet<ui64> PDiskCategories;
    THashSet<ui64> VDiskCategories;
    THashSet<TNodeId> Nodes;
    THashSet<ui32> PDisks;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TBrowseTabletsCommon(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : Owner(owner)
        , Path(browseContext.Path)
        , BrowseContext(browseContext)
    {}

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }
        return clientConfig;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static NTabletPipe::TClientConfig clientConfig = InitPipeClientConfig();
        return clientConfig;
    }

    const TActorId& GetTabletPipe(TTabletId tabletId, const TActorContext& ctx) {
        auto it = PipeClients.find(tabletId);
        if (it != PipeClients.end()) {
            return it->second;
        }
        TActorId pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, GetPipeClientConfig()));
        return PipeClients.emplace(tabletId, pipeClient).first->second;
    }

    void Handle(TEvTablet::TEvGetCountersResponse::TPtr &ev, const TActorContext &ctx) {
        TabletCountersResults.emplace(ev->Cookie, ev->Release());
        ++Responses;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(ev->Sender, ev->Cookie, TEvTablet::EvGetCounters));
        if (Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvHive::TEvChannelInfo::TPtr &ev, const TActorContext &ctx) {
        THolder<TEvHive::TEvChannelInfo> lookupResult = ev->Release();
        for (const auto& channelInfo : lookupResult->Record.GetChannelInfo()) {
            for (const auto& historyInfo : channelInfo.GetHistory()) {
                ui32 groupId = historyInfo.GetGroupID();
                if (GroupInfo.emplace(groupId, nullptr).second) {
                    TTabletId bscTabletId = MakeBSControllerID();
                    TActorId pipeClient = GetTabletPipe(bscTabletId, ctx);
                    NTabletPipe::SendData(ctx, pipeClient, new TEvBlobStorage::TEvRequestControllerInfo(groupId));
                    ++Requests;
                    ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(pipeClient, TEvBlobStorage::EvRequestControllerInfo));
                }
            }
        }
        ++Responses;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(ev->Sender, TEvHive::EvLookupChannelInfo));
        if (Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvBlobStorage::TEvResponseControllerInfo::TPtr &ev, const TActorContext &ctx) {
        THolder<TEvBlobStorage::TEvResponseControllerInfo> bsResult = ev->Release();
        auto& bsGroupInfo = *bsResult->Record.MutableBSGroupInfo();

        while (!bsGroupInfo.empty()) {
            THolder<NKikimrBlobStorage::TEvResponseBSControllerInfo::TBSGroupInfo> groupInfo(bsGroupInfo.ReleaseLast());
            ErasureSpecies.insert(groupInfo->GetErasureSpecies());
            for (const auto& vDiskInfo : groupInfo->GetVDiskInfo()) {
                VDiskCategories.insert(vDiskInfo.GetVDiskCategory());
                PDiskCategories.insert(vDiskInfo.GetPDiskCategory());
                Nodes.insert(vDiskInfo.GetNodeId());
                PDisks.insert(vDiskInfo.GetPDiskId());
            }
            GroupInfo[groupInfo->GetGroupId()] = std::move(groupInfo);
        }
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(ev->Sender, TEvBlobStorage::EvRequestControllerInfo));
        if (++Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    static ui64 GetCounterValue(
            const ::google::protobuf::RepeatedPtrField<NKikimrTabletBase::TTabletSimpleCounter>& counters,
            const TString& name) {
        for (const NKikimrTabletBase::TTabletSimpleCounter& counter : counters) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        return 0;
    }

    void Die(const TActorContext& ctx) override {
        for (const auto& pr : PipeClients) {
            NTabletPipe::CloseClient(ctx, pr.second);
        }
        TBase::Die(ctx);
    }

    void HandleBadRequest(const TActorContext& ctx, const TString& error) {
        ctx.Send(Owner, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleServerError(const TActorContext& ctx, const TString& error) {
        ctx.Send(Owner, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 500 Internal Server Error\r\n\r\n") + error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandlePoisonPill(const TActorContext& ctx) {
        Die(ctx);
    }

    void FillCommonData(NKikimrViewer::TBrowseInfo& browseInfo, NKikimrViewer::TMetaInfo& metaInfo) {
        browseInfo.SetPath(BrowseContext.Path);
        browseInfo.SetName(BrowseContext.GetMyName());
        browseInfo.SetType(BrowseContext.GetMyType());
        NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
        pbCommon.SetPath(BrowseContext.Path);
        pbCommon.SetType(BrowseContext.GetMyType());
        if (DescribeResult != nullptr) {
            const auto& pbRecord(DescribeResult->GetRecord());
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasSelf()) {
                    const auto& pbSelf(pbPathDescription.GetSelf());
                    pbCommon.SetOwner(pbSelf.GetOwner());
                }
                if (pbPathDescription.GetSelf().HasACL()) {
                    NACLib::TACL acl(pbPathDescription.GetSelf().GetACL());
                    auto& aces(acl.GetACE());
                    for (auto it = aces.begin(); it != aces.end(); ++it) {
                        const NACLibProto::TACE& ace = *it;
                        auto& pbAce = *pbCommon.AddACL();
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Deny) {
                            pbAce.SetAccessType("Deny");
                        } else
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Allow) {
                            pbAce.SetAccessType("Allow");
                        }
                        auto ar = ace.GetAccessRight();
                        if ((ar & NACLib::EAccessRights::SelectRow) != 0) {
                            pbAce.AddAccessRights("SelectRow");
                        }
                        if ((ar & NACLib::EAccessRights::UpdateRow) != 0) {
                            pbAce.AddAccessRights("UpdateRow");
                        }
                        if ((ar & NACLib::EAccessRights::EraseRow) != 0) {
                            pbAce.AddAccessRights("EraseRow");
                        }
                        if ((ar & NACLib::EAccessRights::ReadAttributes) != 0) {
                            pbAce.AddAccessRights("ReadAttributes");
                        }
                        if ((ar & NACLib::EAccessRights::WriteAttributes) != 0) {
                            pbAce.AddAccessRights("WriteAttributes");
                        }
                        if ((ar & NACLib::EAccessRights::CreateDirectory) != 0) {
                            pbAce.AddAccessRights("CreateDirectory");
                        }
                        if ((ar & NACLib::EAccessRights::CreateTable) != 0) {
                            pbAce.AddAccessRights("CreateTable");
                        }
                        if ((ar & NACLib::EAccessRights::CreateQueue) != 0) {
                            pbAce.AddAccessRights("CreateQueue");
                        }
                        if (ar == NACLib::EAccessRights::GenericList) {
                            pbAce.SetAccessRule("List");
                        }
                        if (ar == NACLib::EAccessRights::GenericRead) {
                            pbAce.SetAccessRule("Read");
                        }
                        if (ar == NACLib::EAccessRights::GenericWrite) {
                            pbAce.SetAccessRule("Write");
                        }
                        if (ar == NACLib::EAccessRights::GenericFull) {
                            pbAce.SetAccessRule("Full");
                        }
                        if (ar == NACLib::EAccessRights::GenericFullLegacy) {
                            pbAce.SetAccessRule("FullLegacy");
                        }
                        pbAce.SetSubject(ace.GetSID());
                        auto inht = ace.GetInheritanceType();
                        if ((inht & NACLib::EInheritanceType::InheritObject) != 0) {
                            pbAce.AddInheritanceType("InheritObject");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritContainer) != 0) {
                            pbAce.AddInheritanceType("InheritContainer");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritOnly) != 0) {
                            pbAce.AddInheritanceType("InheritOnly");
                        }
                    }
                }
            }
        }
        pbCommon.SetTablets(Tablets.size());

        THolder<TEvTablet::TEvGetCountersResponse> response = AggregateWhiteboardResponses(TabletCountersResults);

        ui64 dataSize = 0;
        dataSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "LogRedoMemory");
        dataSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "DbIndexBytes");
        dataSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "DbDataBytes");
        dataSize += GetCounterValue(response->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters(), "KV/RecordBytes");
        dataSize += GetCounterValue(response->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters(), "KV/TrashBytes");
        pbCommon.SetDataSize(dataSize);

        ui64 memSize = 0;
        memSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "DbWarmBytes"); // not cache
        memSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "CacheFreshSize"); // cache
        memSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "CacheStagingSize"); // cache
        memSize += GetCounterValue(response->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters(), "CacheWarmSize"); // cache
        pbCommon.SetMemorySize(memSize);

        for (auto erasure : ErasureSpecies) {
            pbCommon.AddErasureSpecies(TErasureType::ErasureSpeciesName(erasure));
        }

        for (auto pDiskKind : PDiskCategories) {
            pbCommon.AddPDiskKind(TPDiskCategory(pDiskKind).TypeStrShort());
        }

        for (auto vDiskKind : VDiskCategories) {
            pbCommon.AddVDiskKind(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(static_cast<NKikimrBlobStorage::TVDiskKind::EVDiskKind>(vDiskKind)));
        }

        for (auto nodeId : Nodes) {
            pbCommon.AddNodes(nodeId);
        }

        for (auto pDiskId : PDisks) {
            pbCommon.AddDisks(pDiskId);
        }

        metaInfo.MutableCounters()->CopyFrom(response->Record.GetTabletCounters());
    }

    virtual void Bootstrap(const TActorContext& ctx) = 0;
    virtual void ReplyAndDie(const TActorContext &ctx) = 0;
};

}
}
