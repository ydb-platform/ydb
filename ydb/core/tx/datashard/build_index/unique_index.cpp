#include "common_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/string/builder.h>

namespace NKikimr::NDataShard {

/*
 * TEvValidateUniqueIndexRequest executes a "local" (single-shard) unique index validation job across
 * a single unique index impl table shard. Such a job is a part of bigger process of building unique index on existing table.
 * This job is done on every unique index shard after its filling from existing main table data to validate
 * received data if it is unique. After such a validation on every single unique index shard, Scheme Shard does
 * cross shard validation in order to ensure that first/last keys in single shards don't intercept.
 *
 * If we have a main table with primary key columns (key1, key2), index keys columns (ikey1, ikey2),
 * then the index impl table has columns (ikey1, ikey2, key1, key2). This scan is supposed to scan columns (ikey1, ikey2).
 *
 * Request:
 * - The client sends TEvValidateUniqueIndexRequest to every unique index shard after filling all the data from the table with:
 *   - Names of target index columns to validate uniqueness.
 *
 * Execution Flow:
 * - A TValidateUniqueIndexScan iterates over the whole table shard in key order and compares every pair of consecutive keys.
 *   If these key are not the same, the processing succeedes.
 *   The comparison is performed with NULL-semantics, meaning that NULL values are not considered equal (the result of their comparison is also NULL).
 *   As a result of the scan datashard returns two tuples: the first and the last index keys. These keys are supposed to take part in cross shard validation.
 */

class TValidateUniqueIndexScan: public NTable::IScan {
public:
    TValidateUniqueIndexScan(
        ui64 buildIndexId,
        const TUserTable& tableInfo,
        TProtoColumnsCRef targetIndexColumns,
        const TScanRecord::TSeqNo& seqNo,
        ui64 dataShardId,
        NActors::TActorId responseActorId)
        : BuildIndexId(buildIndexId)
        , SeqNo(seqNo)
        , DataShardId(dataShardId)
        , ResponseActorId(responseActorId)
        , ScanTags(BuildTags(tableInfo, targetIndexColumns))
        , IndexColumnNames(targetIndexColumns.begin(), targetIndexColumns.end())
    {
        LOG_I("Create " << Debug());
    }

    TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme> scheme) override {
        Scheme = std::move(scheme);
        MakeTypeInfos();

        LOG_I("Prepare " << Debug());
        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) override {
        LOG_T("Seek " << seq << " " << Debug());

        lead.To(ScanTags, {}, NTable::ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
        // LOG_T("Feed " << Debug());

        if (row.Size() != ScanTags.size()) {
            return FinishValidation(NKikimrIndexBuilder::EBuildStatus::ABORTED, TStringBuilder() << "Row size mismatch: expected " << ScanTags.size() << ", got " << row.Size());
        }

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        TArrayRef<const TCell> rowCells = *row;
        if (!FirstIndexKey) {
            FirstIndexKey = TSerializedCellVec(rowCells);
            LastIndexKey = TSerializedCellVec(rowCells);
            return EScan::Feed;
        }

        if (TypedCellVectorsEqualWithNullSemantics(rowCells.data(), LastIndexKey.GetCells().data(), IndexColumnTypeInfos.data(), IndexColumnTypeInfos.size()) == ETriBool::True) {
            return FinishValidation(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, DuplicateKeyErrorMessage());
        }
        LastIndexKey = TSerializedCellVec(rowCells);
        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(const std::exception& ex) override {
        Issues.AddIssue(TStringBuilder() << "Scan failed: " << ex.what());
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        if (status == EStatus::Exception) {
            StatusCode = NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR;
        } else if (status != EStatus::Done) {
            StatusCode = NKikimrIndexBuilder::EBuildStatus::ABORTED;
        } else if (StatusCode == NKikimrIndexBuilder::EBuildStatus::INVALID) {
            StatusCode = NKikimrIndexBuilder::EBuildStatus::DONE;
        }

        auto response = std::make_unique<TEvDataShard::TEvValidateUniqueIndexResponse>();
        FillScanResponseCommonFields(*response, BuildIndexId, DataShardId, SeqNo);
        auto& rec = response->Record;
        rec.SetStatus(StatusCode);
        rec.MutableMeteringStats()->SetReadRows(ReadRows);
        rec.MutableMeteringStats()->SetReadBytes(ReadBytes);
        if (Issues) {
            NYql::IssuesToMessage(Issues, rec.MutableIssues());
        }
        if (FirstIndexKey) {
            rec.SetFirstIndexKey(FirstIndexKey.GetBuffer());
        }
        if (LastIndexKey) {
            rec.SetLastIndexKey(LastIndexKey.GetBuffer());
        }

        if (rec.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << ToShortDebugString(rec));
        } else {
            LOG_E("Failed " << Debug() << " " << ToShortDebugString(rec));
        }

        TActivationContext::Send(ResponseActorId, std::move(response));
        return this;
    }

    EScan Exhausted() override {
        LOG_T("Exhausted " << Debug());

        return EScan::Final;
    }

    void Describe(IOutputStream& out) const override {
        out << "TValidateUniqueIndexScan Id: " << BuildIndexId
            << " Status: " << StatusCode << " Issues: " << Issues.ToOneLineString();
    }

private:
    TString DuplicateKeyErrorMessage() const {
        TStringBuilder res;
        res << "Duplicate key found: (";
        for (size_t i = 0; i < IndexColumnNames.size(); ++i) {
            if (i > 0) {
                res << ", ";
            }
            res << IndexColumnNames[i] << "=";
            DbgPrintValue(res, LastIndexKey.GetCells()[i], IndexColumnTypeInfos[i].ToTypeInfo());
        }
        res << ")";
        return std::move(res);
    }

    EScan FinishValidation(NKikimrIndexBuilder::EBuildStatus statusCode = NKikimrIndexBuilder::EBuildStatus::DONE, TString error = {}) {
        StatusCode = statusCode;
        Issues.AddIssue(error);
        return EScan::Final;
    }

    TString Debug() const {
        TStringBuilder res;
        Describe(res.Out);
        return std::move(res);
    }

    void MakeTypeInfos() {
        IndexColumnTypeInfos.reserve(ScanTags.size());
        for (NTable::TTag tag : ScanTags) {
            const NTable::TColInfo* colInfo = Scheme->ColInfo(tag);
            Y_ENSURE(colInfo, "Column info not found for tag " << tag);
            IndexColumnTypeInfos.emplace_back(colInfo->TypeInfo);
        }
    }

private:
    const ui64 BuildIndexId;
    const TScanRecord::TSeqNo SeqNo;
    const ui64 DataShardId;
    const NActors::TActorId ResponseActorId;

    TTags ScanTags;
    std::vector<TString> IndexColumnNames;
    TIntrusiveConstPtr<TScheme> Scheme;
    std::vector<NScheme::TTypeInfoOrder> IndexColumnTypeInfos;

    // Results
    NKikimrIndexBuilder::EBuildStatus StatusCode = NKikimrIndexBuilder::EBuildStatus::INVALID;
    NYql::TIssues Issues;
    TSerializedCellVec FirstIndexKey;
    TSerializedCellVec LastIndexKey;
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
};

TAutoPtr<NTable::IScan> CreateValidateUniqueIndexScan(
    ui64 buildIndexId,
    const TUserTable& tableInfo,
    TProtoColumnsCRef targetIndexColumns,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    NActors::TActorId senderActorId)
{
    return new TValidateUniqueIndexScan(buildIndexId, tableInfo, targetIndexColumns, seqNo, dataShardId, senderActorId);
}

class TDataShard::TTxHandleSafeValidateUniqueIndexScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeValidateUniqueIndexScan(TDataShard* self, TEvDataShard::TEvValidateUniqueIndexRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvValidateUniqueIndexRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvValidateUniqueIndexRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeValidateUniqueIndexScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvValidateUniqueIndexRequest::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;

    const ui64 id = request.GetId();
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvValidateUniqueIndexResponse>();
        FillScanResponseCommonFields(*response, request.GetId(), TabletID(), seqNo);

        LOG_N("Starting TValidateUniqueIndexScan TabletId: " << TabletID()
            << " " << request.ShortDebugString());

        auto badRequest = [&](const TString& error) {
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        };
        auto trySendBadRequest = [&] {
            if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
                LOG_E("Rejecting TValidateUniqueIndexScan bad request TabletId: " << TabletID()
                    << " " << request.ShortDebugString()
                    << " with response " << ToShortDebugString(response->Record));
                ctx.Send(ev->Sender, std::move(response));
                return true;
            } else {
                return false;
            }
        };

        const auto tableId = TTableId(request.GetOwnerId(), request.GetPathId());
        if (request.GetTabletId() != TabletID()) {
            badRequest(TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << TabletID());
        }
        if (!IsStateActive()) {
            badRequest(TStringBuilder() << "Shard " << TabletID() << " is " << State << " and not ready for requests");
        }
        if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
            badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        }
        if (request.GetIndexColumns().empty()) {
            badRequest("Empty index columns list");
        }

        if (trySendBadRequest()) {
            return;
        }

        const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);
        auto tags = GetAllTags(userTable);
        for (auto column : request.GetIndexColumns()) {
            if (!tags.contains(column)) {
                badRequest(TStringBuilder() << "Unknown index column: " << column);
            }
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = CreateValidateUniqueIndexScan(
            id,
            userTable,
            request.GetIndexColumns(),
            seqNo,
            request.GetTabletId(),
            ev->Sender);

        // We don't need a specific row version here, because KQP level forbids table modification during unique index building.
        StartScan(this, std::move(scan), id, seqNo, std::nullopt, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvValidateUniqueIndexResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TValidateUniqueIndexScan");
    }
}

} // namespace NKikimr::NDataShard
