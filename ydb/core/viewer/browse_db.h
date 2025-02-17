#pragma once
#include "browse.h"
#include "viewer.h"
#include "wb_aggregate.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::NViewerDB {

using namespace NViewer;
using namespace NActors;

class TBrowseTable : public TBrowseTabletsCommon {
protected:
    static const bool WithRetry = false;
    using TThis = TBrowseTable;
    using TBase = TBrowseTabletsCommon;
    TActorId TxProxy = MakeTxProxyID();

public:
    TBrowseTable(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : TBrowseTabletsCommon(owner, browseContext)
    {}

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx) {
        DescribeResult.Reset(ev->Release());
        ++Responses;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(TxProxy, TEvTxUserProxy::EvNavigate));
        const auto& pbRecord(DescribeResult->GetRecord());
        if (pbRecord.GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                TVector<ui64> tablets;
                tablets.reserve(pbPathDescription.TablePartitionsSize());
                for (const auto& partition : pbPathDescription.GetTablePartitions()) {
                    tablets.emplace_back(partition.GetDatashardId());
                }
                if (!tablets.empty()) {
                    Sort(tablets);
                    tablets.erase(std::unique(tablets.begin(), tablets.end()), tablets.end());
                    SendTabletRequests(tablets, ctx);
                    std::copy(tablets.begin(), tablets.end(), std::back_inserter(Tablets));
                }
            }
        } else {
            switch (DescribeResult->GetRecord().GetStatus()) {
                case NKikimrScheme::EStatus::StatusPathDoesNotExist:
                    return HandleBadRequest(ctx, "The path is not found");
                default:
                    return HandleBadRequest(ctx, "Error getting schema information");
            }
        }
        if (Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    void SendTabletRequests(const TVector<TTabletId>& tablets, const TActorContext& ctx) {
        TDomainsInfo* domainsInfo = AppData(ctx)->DomainsInfo.Get();
        for (auto tabletId : tablets) {
            TActorId pipeClient = GetTabletPipe(tabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvTablet::TEvGetCounters(), tabletId);
            ++Requests;
            ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(TxProxy, tabletId, TEvTablet::EvGetCounters));
            ui64 hiveTabletId = domainsInfo->GetHive();
            pipeClient = GetTabletPipe(hiveTabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvHive::TEvLookupChannelInfo(tabletId), tabletId);
            ++Requests;
            ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(pipeClient, TEvHive::EvLookupChannelInfo));
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvTablet::TEvGetCountersResponse, TBase::Handle);
            HFunc(TEvHive::TEvChannelInfo, TBase::Handle);
            HFunc(TEvBlobStorage::TEvResponseControllerInfo, TBase::Handle);
            CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
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

    virtual void Bootstrap(const TActorContext& ctx) override {
        THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
        if (!BrowseContext.UserToken.empty()) {
            request->Record.SetUserToken(BrowseContext.UserToken);
        }
        NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
        record->SetPath(Path);
        record->SetBackupInfo(true);
        request->Record.SetUserToken(BrowseContext.UserToken);
        ctx.Send(TxProxy, request.Release());
        ++Requests;
        UnsafeBecome(&TThis::StateWork);
    }

    virtual void ReplyAndDie(const TActorContext &ctx) override {
        NKikimrViewer::TBrowseInfo browseInfo;
        NKikimrViewer::TMetaInfo metaInfo;
        NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
        FillCommonData(browseInfo, metaInfo);
        if (DescribeResult != nullptr) {
            const auto& pbRecord(DescribeResult->GetRecord());
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasSelf()) {
                    const auto& pbSelf(pbPathDescription.GetSelf());
                    pbCommon.SetOwner(pbSelf.GetOwner());
                    if (pbSelf.GetCreateStep() != 0) {
                        pbCommon.SetCreateTime(pbSelf.GetCreateStep());
                    }
                }
                if (pbPathDescription.HasTable()) {
                    const auto& pbTable(pbPathDescription.GetTable());
                    if (pbTable.HasUniformPartitionsCount()) {
                        pbCommon.SetPartitions(pbTable.GetUniformPartitionsCount());
                    }
                    if (pbTable.SplitBoundarySize() > 0) {
                        pbCommon.SetPartitions(pbTable.SplitBoundarySize() + 1);
                    }
                    if (pbTable.ColumnsSize() > 0) {
                        NKikimrViewer::TMetaTableInfo& pbMetaTable = *metaInfo.MutableTable();
                        for (const auto& column : pbTable.GetColumns()) {
                            auto& pbSchema = *pbMetaTable.AddSchema();
                            pbSchema.SetName(column.GetName());
                            pbSchema.SetType(column.GetType());
                            for (ui32 keyId : pbTable.GetKeyColumnIds()) {
                                if (keyId == column.GetId()) {
                                    pbSchema.SetKey(true);
                                    break;
                                }
                            }
                        }
                    }
                }
                if (pbPathDescription.HasTableStats()) {
                    const auto& pbTableStats(pbPathDescription.GetTableStats());
                    pbCommon.SetRowCount(pbTableStats.GetRowCount());
                    pbCommon.SetAccessTime(pbTableStats.GetLastAccessTime());
                    pbCommon.SetUpdateTime(pbTableStats.GetLastUpdateTime());
                    pbCommon.SetImmediateTxCompleted(pbTableStats.GetImmediateTxCompleted());
                    pbCommon.SetPlannedTxCompleted(pbTableStats.GetPlannedTxCompleted());
                    pbCommon.SetTxRejectedByOverload(pbTableStats.GetTxRejectedByOverload());
                    pbCommon.SetTxRejectedBySpace(pbTableStats.GetTxRejectedBySpace());

                    pbCommon.SetRowUpdates(pbTableStats.GetRowUpdates());
                    pbCommon.SetRowDeletes(pbTableStats.GetRowDeletes());
                    pbCommon.SetRowReads(pbTableStats.GetRowReads());
                    pbCommon.SetRangeReads(pbTableStats.GetRangeReads());
                    pbCommon.SetRangeReadRows(pbTableStats.GetRangeReadRows());
                }
                if (pbPathDescription.HasTabletMetrics()) {
                    const auto& pbTabletMetrics(pbPathDescription.GetTabletMetrics());
                    auto& pbResources(*pbCommon.MutableResources());
                    pbResources.SetCPU(pbTabletMetrics.GetCPU());
                    pbResources.SetMemory(pbTabletMetrics.GetMemory());
                    pbResources.SetNetwork(pbTabletMetrics.GetNetwork());
                    pbResources.SetStorage(pbTabletMetrics.GetStorage());
                    pbResources.SetReadThroughput(pbTabletMetrics.GetReadThroughput());
                    pbResources.SetWriteThroughput(pbTabletMetrics.GetWriteThroughput());
                }
                pbCommon.MutableBackup()->MutableLastResults()->CopyFrom(pbPathDescription.GetLastBackupResult());
                pbCommon.MutableBackup()->MutableProgress()->CopyFrom(pbPathDescription.GetBackupProgress());
            }
        }

        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(browseInfo), std::move(metaInfo)));
        Die(ctx);
    }
};

}
