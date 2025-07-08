#include "kmeans_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {
using namespace NKMeans;

/*
 * TRecomputeKMeansScan recomputes K-means cluster centroids.
 * It assigns each data points to the nearest centroid and recalculates the centroid as a mean of all data points assigned to it.
 * It scans either a MAIN or BUILD table shard, does not upload rows anywhere and just returns the new clusters.
 *
 * Request:
 * - The client sends TEvRecomputeKMeansRequest with:
 *   - Parent: ID of the scanned cluster
 *     - If Parent=0, the entire table shard is scanned
 *   - Clusters: list of centroids to which input rows will be reassigned
 *   - The embedding (vector) column name to be used for K-means
 *
 * Execution Flow:
 * - TRecomputeKMeansScan scans the relevant input shard range
 * - For each input row:
 *   - The closest cluster (from the provided centroids) is determined
 *   - The row is aggregated in memory to the cluster's centroid
 * - Mean value is calculated for each centroid and returned in the response
 */

class TRecomputeKMeansScan: public TActor<TRecomputeKMeansScan>, public NTable::IScan {
protected:
    const ui64 TabletId = 0;
    const ui64 BuildId = 0;
    const TAutoPtr<TEvDataShard::TEvRecomputeKMeansResponse> Response;
    const TActorId ResponseActorId;

    TTags ScanTags;
    ui32 EmbeddingPos = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    IDriver* Driver = nullptr;
    NYql::TIssues Issues;

    TLead Lead;

    std::unique_ptr<IClusters> Clusters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::RECOMPUTE_KMEANS_SCAN_ACTOR;
    }

    TRecomputeKMeansScan(ui64 tabletId, const TUserTable& table, TLead&& lead,
        const NKikimrTxDataShard::TEvRecomputeKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvRecomputeKMeansResponse>&& response,
        std::unique_ptr<IClusters>&& clusters)
        : TActor(&TThis::StateWork)
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Response(std::move(response))
        , ResponseActorId(responseActorId)
        , Lead(std::move(lead))
        , Clusters(std::move(clusters))
    {
        LOG_I("Create " << Debug());

        const auto& embedding = request.GetEmbeddingColumn();
        NTable::TTag embeddingTag;
        ui32 dataPos = 0;
        ScanTags = MakeScanTags(table, embedding, {}, EmbeddingPos, dataPos, embeddingTag);
        Lead.SetTags(ScanTags);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        if (abort != EAbort::None) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
            FillResponse();
        }

        NYql::IssuesToMessage(Issues, record.MutableIssues());

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << ToShortDebugString(Response->Record));
        } else {
            LOG_E("Failed " << Debug() << " " << ToShortDebugString(Response->Record));
        }
        Send(ResponseActorId, Response.Release());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept final
    {
        out << Debug();
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
    try {
        LOG_T("Seek " << seq << " " << Debug());

        lead = Lead;

        return EScan::Feed;
    } catch (const std::exception& exc) {
        Issues.AddIssue(exc.what());
        return EScan::Final;
    }
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final
    {
    try {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        Feed(key, *row);

        return EScan::Feed;
    } catch (const std::exception& exc) {
        Issues.AddIssue(exc.what());
        return EScan::Final;
    }
    }

    EScan Exhausted() noexcept final
    {
    try {
        LOG_T("Exhausted " << Debug());

        return EScan::Final;
    } catch (const std::exception& exc) {
        Issues.AddIssue(exc.what());
        return EScan::Final;
    }
    }

protected:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() 
                    << " event: " << ev->ToString() << " " << Debug());
        }
    }

    TString Debug() const
    {
        return TStringBuilder() << "TRecomputeKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " " << Clusters->Debug();
    }

    void Feed(TArrayRef<const TCell>, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            Clusters->AggregateToCluster(*pos, row.at(EmbeddingPos).AsRef());
        }
    }

    void FillResponse() noexcept
    {
    try{
        auto& record = Response->Record;
        Clusters->RecomputeClusters();
        const auto& clusters = Clusters->GetClusters();
        const auto& sizes = Clusters->GetNextClusterSizes();
        for (size_t i = 0; i < clusters.size(); i++) {
            record.AddClusters(clusters[i]);
            record.AddClusterSizes(sizes[i]);
        }
        record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
    } catch (const std::exception& exc) {
        Issues.AddIssue(exc.what());
    }
    }
};

class TDataShard::TTxHandleSafeRecomputeKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeRecomputeKMeansScan(TDataShard* self, TEvDataShard::TEvRecomputeKMeansRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) final
    {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) final
    {
    }

private:
    TEvDataShard::TEvRecomputeKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvRecomputeKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeRecomputeKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvRecomputeKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvRecomputeKMeansResponse>();
        response->Record.SetId(id);
        response->Record.SetTabletId(TabletID());
        response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
        response->Record.SetRequestSeqNoRound(seqNo.Round);

        LOG_N("Starting TRecomputeKMeansScan TabletId: " << TabletID()
            << " " << ToShortDebugString(request)
            << " row version " << rowVersion);

        // Note: it's very unlikely that we have volatile txs before this snapshot
        if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
            VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
            return;
        }

        auto badRequest = [&](const TString& error) {
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        };
        auto trySendBadRequest = [&] {
            if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
                LOG_E("Rejecting TRecomputeKMeansScan bad request TabletId: " << TabletID()
                    << " " << ToShortDebugString(request)
                    << " with response " << ToShortDebugString(response->Record));
                ctx.Send(ev->Sender, std::move(response));
                return true;
            } else {
                return false;
            }
        };

        // 1. Validating table and path existence
        if (request.GetTabletId() != TabletID()) {
            badRequest(TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << TabletID());
        }
        if (!IsStateActive()) {
            badRequest(TStringBuilder() << "Shard " << TabletID() << " is " << State << " and not ready for requests");
        }
        const auto pathId = TPathId::FromProto(request.GetPathId());
        const auto* userTableIt = GetUserTables().FindPtr(pathId.LocalPathId);
        if (!userTableIt) {
            badRequest(TStringBuilder() << "Unknown table id: " << pathId.LocalPathId);
        }
        if (trySendBadRequest()) {
            return;
        }
        const auto& userTable = **userTableIt;

        // 2. Validating request fields
        if (request.HasSnapshotStep() || request.HasSnapshotTxId()) {
            const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
            if (!SnapshotManager.FindAvailable(snapshotKey)) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                    << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
            }
        }


        const auto parent = request.GetParent();
        NTable::TLead lead;
        if (parent == 0) {
            lead.To({}, NTable::ESeek::Lower);
        } else {
            TCell from, to;
            const auto range = CreateRangeFrom(userTable, request.GetParent(), from, to);
            if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
                badRequest(TStringBuilder() << " requested range doesn't intersect with table range");
            }
            lead = CreateLeadFrom(range);
        }

        auto tags = GetAllTags(userTable);
        if (!tags.contains(request.GetEmbeddingColumn())) {
            badRequest(TStringBuilder() << "Unknown embedding column: " << request.GetEmbeddingColumn());
        }

        // 3. Validating vector index settings
        TString error;
        auto clusters = NKikimr::NKMeans::CreateClusters(request.GetSettings(), 0, error);
        if (!clusters) {
            badRequest(error);
        } else if (request.ClustersSize() < 1) {
            badRequest("Should be requested for at least one cluster");
        } else if (!clusters->SetClusters(TVector<TString>{request.GetClusters().begin(), request.GetClusters().end()})) {
            badRequest("Clusters have invalid format");
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TRecomputeKMeansScan(
            TabletID(), userTable, std::move(lead), request, ev->Sender, std::move(response), std::move(clusters)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvRecomputeKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TRecomputeKMeansScan");
    }
}

}
