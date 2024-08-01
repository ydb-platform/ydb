#include "datashard_impl.h"
#include "scan_common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr::NDataShard {

static std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings) {
    auto types = GetAllTypes(tableInfo);

    auto result = std::make_shared<TTypes>();
    result->reserve(checkingNotNullSettings.columnSize());

    for (size_t i = 0; i < checkingNotNullSettings.columnSize(); i++) {
        const auto& colName = checkingNotNullSettings.column(i).GetColumnName();
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, types.at(colName));
        result->emplace_back(colName, type);
    }

    return result;
}

bool CheckNotNullConstraint(const TConstArrayRef<TCell>& cells) {
    for (const auto& cell : cells) {
        if (cell.IsNull()) {
            return false;
        }
    }

    return true;
}

class TCheckColumnScan final: public TActor<TCheckColumnScan>, public NTable::IScan {
private:
    const ui64 BuildIndexId;
    const TString TargetTable;
    const TBuildIndexRecord::TSeqNo SeqNo;
    const ui64 DataShardId;
    const TActorId DatashardActorId;
    const TActorId SchemeShardActorID;
    const TUserTable& TableInfo;
    const NKikimrIndexBuilder::TCheckingNotNullSettings& CheckingNotNullSettings;

    IDriver* Driver = nullptr;

    enum class ECheckingNotNullStatus {
        None,
        Ok,
        NullFound
    } CheckingNotNullStatus = ECheckingNotNullStatus::None;

public:
    TCheckColumnScan(ui64 buildIndexId,
                     const TString& targetTable,
                     const TScanRecord::TSeqNo& seqNo,
                     ui64 dataShardId,
                     const TActorId& datashardActorId,
                     const TActorId& schemeshardActorId,
                     const TUserTable& tableInfo,
                     const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings
    )
    : BuildIndexId(buildIndexId)
    , TargetTable(targetTable)
    , SeqNo(seqNo)
    , DataShardId(dataShardId)
    , DatashardActorId(datashardActorId)
    , SchemeShardActorID(schemeshardActorId)
    , TableInfo(tableInfo)
    , CheckingNotNullSettings(checkingNotNullSettings)
    {
        Y_ABORT_UNLESS(checkingNotNullSettings.size() == 0);
    }

    ~TBuildIndexScan() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) {
        auto selfActorId = TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        auto ctx = TActivationContext::AsActorContext().MakeFor(selfActorId);

        Driver = driver;

        return { EScan::Feed, { } };
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) {
        if (!CheckNotNullConstraint(rowCells)) {
            CheckingNotNullStatus = ECheckingNotNullStatus::NullFound;
            return EScan::Final;
        } else {
            CheckingNotNullStatus = ECheckingNotNullStatus::Ok;
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

        TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress
            = new TEvDataShard::TEvBuildIndexProgressResponse;
        progress->Record.SetBuildIndexId(BuildIndexId);
        progress->Record.SetTabletId(DataShardId);
        progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
        progress->Record.SetRequestSeqNoRound(SeqNo.Round);

        Y_ABORT_UNLESS(CheckingNotNullStatus == ECheckingNotNullStatus::None);

        switch (CheckingNotNullStatus) {
            case ECheckingNotNullStatus::NullFound:
                progress->Record.SetStatus(NKikimrTxDataShard::TEvCheckConstraintProgressResponse::CHECKING_NOT_NULL_ERROR);
                progress->Record.AddIssues(NYql::TIssue("Column contains null value, so not-null constraint was not set."));
                break;
            case ECheckingNotNullStatus::Ok:
                progress->Record.SetStatus(NKikimrTxDataShard::TEvCheckConstraintProgressResponse::DONE);
                break;
            default:
                break;
        }

        ctx.Send(SchemeShardActorID, progress.Release());;

        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept override {
        out << Debug();
    }

    TString Debug() const {

    }    
};

TAutoPtr<NTable::IScan> CreateCheckConstraintScan(
    ui64 buildIndexId,
    TString targetTable,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings,
    const TUserTable& tableInfo
)
{
    return new TCheckColumnScan(
        buildIndexId,
        targetTable,
        seqNo,
        dataShardId,
        progressActorId,
        range,
        tableInfo,
        checkingNotNullSettings
    );
}

class TDataShard::TTxHandleSafeCheckConstraintScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeCheckConstraintScan(TDataShard* self, TEvDataShard::TEvCheckConstraintCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvCheckConstraintCreateRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvCheckConstraintCreateRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeBuildIndexScan(this, std::move(ev)));
}

// void TDataShard::HandleSafe(TEvDataShard::TEvCheckConstraintCreateRequest::TPtr& ev, const TActorContext& ctx) {
//     const auto& record = ev->Get()->Record;

//     // Note: it's very unlikely that we have volatile txs before this snapshot
//     if (VolatileTxManager.HasVolatileTxsAtSnapshot(TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId()))) {
//         VolatileTxManager.AttachWaitingSnapshotEvent(
//             TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId()),
//             std::unique_ptr<IEventHandle>(ev.Release()));
//         return;
//     }

//     auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
//     response->Record.SetBuildIndexId(record.GetBuildIndexId());
//     response->Record.SetTabletId(TabletID());
//     response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::ACCEPTED);

//     TBuildIndexRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
//     response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
//     response->Record.SetRequestSeqNoRound(seqNo.Round);

//     auto badRequest = [&] (const TString& error) {
//         response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::BAD_REQUEST);
//         auto issue = response->Record.AddIssues();
//         issue->set_severity(NYql::TSeverityIds::S_ERROR);
//         issue->set_message(error);
//     };

//     const ui64 buildIndexId = record.GetBuildIndexId();
//     const ui64 shardId = record.GetTabletId();
//     const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());

//     if (shardId != TabletID()) {
//         badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
//         badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     TUserTable::TCPtr userTable = GetUserTables().at(tableId.PathId.LocalPathId);


//     if (const auto* recCard = BuildIndexManager.Get(buildIndexId)) {
//         if (recCard->SeqNo == seqNo) {
//             // do no start one more scan
//             ctx.Send(ev->Sender, std::move(response));
//             return;
//         }

//         CancelScan(userTable->LocalTid, recCard->ScanId);
//         BuildIndexManager.Drop(buildIndexId);
//     }

//     TSerializedTableRange requestedRange;
//     requestedRange.Load(record.GetKeyRange());

//     auto scanRange = Intersect(userTable->KeyColumnTypes, requestedRange.ToTableRange(), userTable->Range.ToTableRange());

//     if (scanRange.IsEmptyRange(userTable->KeyColumnTypes)) {
//         badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
//                                     << " requestedRange: " << DebugPrintRange(userTable->KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
//                                     << " tableRange: " << DebugPrintRange(userTable->KeyColumnTypes, userTable->Range.ToTableRange(), *AppData()->TypeRegistry)
//                                     << " scanRange: " << DebugPrintRange(userTable->KeyColumnTypes, scanRange, *AppData()->TypeRegistry) );
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     const TVector<TString> targetIndexColumns(record.GetIndexColumns().begin(), record.GetIndexColumns().end());
//     const TVector<TString> targetDataColumns(record.GetDataColumns().begin(), record.GetDataColumns().end());

//     if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
//         badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     const TSnapshotKey snapshotKey(tableId.PathId, record.GetSnapshotStep(), record.GetSnapshotTxId());
//     const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
//     if (!snapshot) {
//         badRequest(TStringBuilder()
//                    << "no snapshot has been found"
//                    << " , path id is " <<tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
//                    << " , snapshot step is " <<  snapshotKey.Step
//                    << " , snapshot tx is " <<  snapshotKey.TxId);
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     if (!IsStateActive()) {
//         badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
//         ctx.Send(ev->Sender, std::move(response));
//         return;
//     }

//     TScanOptions scanOpts;
//     scanOpts.SetSnapshotRowVersion(TRowVersion(snapshotKey.Step, snapshotKey.TxId));
//     scanOpts.SetResourceBroker("build_index", 10);

//     NKikimrIndexBuilder::TColumnBuildSettings columnsToBuild;
//     columnsToBuild.Swap(ev->Get()->Record.MutableColumnBuildSettings());

//     NKikimrIndexBuilder::TCheckingNotNullSettings columnsToCheckingNotNull;
//     columnsToCheckingNotNull.Swap(ev->Get()->Record.MutableCheckingNotNullSettings());

//     const auto scanId = QueueScan(userTable->LocalTid,
//                             CreateBuildIndexScan(buildIndexId,
//                                                  record.GetTargetName(),
//                                                  seqNo,
//                                                  shardId,
//                                                  ctx.SelfID,
//                                                  ev->Sender,
//                                                  requestedRange,
//                                                  targetIndexColumns,
//                                                  targetDataColumns,
//                                                  std::move(columnsToBuild),
//                                                  std::move(columnsToCheckingNotNull),
//                                                  userTable),
//                             ev->Cookie,
//                             scanOpts);

//     TBuildIndexRecord recCard = {scanId, seqNo};

//     BuildIndexManager.Set(buildIndexId, recCard);

//     ctx.Send(ev->Sender, std::move(response));
// }


} // namespace NKikimr::NDataShard
