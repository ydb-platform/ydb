#include "common_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard {
using namespace NTableIndex::NFulltext;
using namespace NKikimr::NFulltext;

/*
 * TBuildFulltextDictScan aggregates rows from the fulltext posting table and does two things:
 * 1) calculates token frequencies, i.e. the number of documents containing each token.
 * 2) compacts fulltext segments generated during initial build process for compact index formats
 *    (FulltextCompact/FulltextCompactRelevance/JsonCompact).
 *
 * For simple index formats:
 * - This scan takes the indexImplTable and writes output to indexImplDictTable.
 * - Source columns: __ydb_token, <PK columns>, __ydb_freq
 * For compact index formats:
 * - This scan takes the indexImplTable0build and writes output to indexImplTable and indexImplDictTable.
 * - Source columns: __ydb_token, __ydb_generation, __ydb_max_id, __ydb_added, __ydb_segment
 * - Destination columns: __ydb_token, __ydb_generation, __ydb_max_id, __ydb_added, __ydb_segment
 * - All source segments are expected to be unique with __ydb_generation = MAX
 * - Output __ydb_generation is always MAX
 * For both:
 * - indexImplDictTable destination columns: __ydb_token, __ydb_freq
 *
 * Request:
 * - The client sends TEvBuildFulltextDictRequest with:
 *   - Name of the target dictionary table
 *   - Name of the target compacted posting table
 *   - Index type
 *   - SkipFirstToken, SkipLastToken
 *
 * Execution Flow:
 * - TBuildFulltextDictScan scans the whole input shard
 * - If SkipFirstToken is specified in the request:
 *   - The first __ydb_token and the number of rows with it are copied to the response protobuf.
 * - If SkipLastToken is specified in the request:
 *   - The last __ydb_token and the number of rows with it are copied to the response protobuf.
 * - For all other input rows, namely, for every token:
 *   - Total token frequency is calculated from the source table rows.
 *   - The token is inserted into DictTable along with the calculated number.
 *   - For compact index formats, all segments are merged so that the number of document IDs in
 *     each of the resulting segments doesn't exceed MaxSegmentDocuments and inserted into PostingTable.
 */

class TBuildFulltextDictScan: public TActor<TBuildFulltextDictScan>, public IActorExceptionHandler, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    EState UploadState;

    IDriver* Driver = nullptr;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TTags ScanTags;
    TBatchRowsUploader Uploader;
    bool KeyIs32 = false;
    bool Signed = false;

    TBufferData* DictBuf = nullptr;
    TBufferData* PostingBuf = nullptr;

    bool SkipFirstToken = false;
    bool SkipLastToken = false;
    TString FirstToken;
    TString LastToken;
    ui64 FirstTokenRows = 0;
    ui64 LastTokenRows = 0;

    // Number of leading prefix key columns in the posting table key [prefix..., token, max_id, gen].
    // Zero for non-prefixed indexes. Used to locate the token and to group segments per (prefix, token).
    ui32 NumPrefixColumns = 0;
    // Current group's key cells [prefix..., token] and its serialized form for group boundary detection.
    TOwnedCellVec LastGroupKey;
    TString LastGroupKeySerialized;

    bool WithFreq = false;
    ui64 MaxSegmentDocuments = 0;
    TDeltaWriter Delta;

    ui32 RetryCount = 0;

    const TIndexBuildScanSettings ScanSettings;

    TUploadStatus UploadStatus;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvBuildFulltextDictResponse> Response;

    bool IsExhausted = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::BUILD_FULLTEXT_DICTIONARY;
    }

    TBuildFulltextDictScan(ui64 tabletId, const TUserTable& table,
        const NKikimrTxDataShard::TEvBuildFulltextDictRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvBuildFulltextDictResponse>&& response)
        : TActor(&TThis::StateWork)
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Uploader(request.GetDatabaseName(), request.GetScanSettings())
        , SkipFirstToken(request.GetSkipFirstToken())
        , SkipLastToken(request.GetSkipLastToken())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId(responseActorId)
        , Response(std::move(response))
    {
        YDB_LOG_INFO("Scan actor created",
            {"debug", Debug()});

        NumPrefixColumns = request.PrefixColumnsSize();

        auto types = GetAllTypes(table);
        auto addType = [&](auto& uploadTypes, const auto& column) {
            auto typeInfo = types.at(column);
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(typeInfo, type);
            uploadTypes->emplace_back(column, type);
        };

        if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance)
        {
            auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
            addType(uploadTypes, TokenColumn);
            {
                Ydb::Type type;
                type.set_type_id(DocCountType);
                uploadTypes->emplace_back(FreqColumn, type);
            }
            DictBuf = Uploader.AddDestination(request.GetDictTableName(), uploadTypes);
        }

        if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance)
        {
            // For compact formats the key is [prefix..., token, __ydb_generation, __ydb_max_id];
            // __ydb_max_id (the doc-id) determines the integer key encoding. For non-prefixed indexes
            // this is at(2) (token at 0), preserving the previous behavior.
            auto keyTypeId = table.KeyColumnTypes.at(NumPrefixColumns + 2).GetTypeId();
            KeyIs32 = (keyTypeId == NScheme::NTypeIds::Uint32 || keyTypeId == NScheme::NTypeIds::Int32);
            Signed = (keyTypeId == NScheme::NTypeIds::Int64 || keyTypeId == NScheme::NTypeIds::Int32);

            auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
            // Posting key is [prefix..., token, max_id, gen]; the prefix columns lead the key.
            for (const auto& prefixColumn : request.GetPrefixColumns()) {
                addType(uploadTypes, prefixColumn);
            }
            addType(uploadTypes, TokenColumn);
            {
                Ydb::Type type;
                type.set_type_id(NTableIndex::NFulltext::GenType);
                uploadTypes->emplace_back(GenColumn, type);
            }
            addType(uploadTypes, MaxIdColumn);
            addType(uploadTypes, AddedColumn);
            addType(uploadTypes, SegmentColumn);
            PostingBuf = Uploader.AddDestination(request.GetPostingTableName(), std::move(uploadTypes));

            WithFreq = (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance);
            Delta.Reset(WithFreq, Signed);
            MaxSegmentDocuments = request.GetMaxSegmentDocuments();
            if (!MaxSegmentDocuments) {
                MaxSegmentDocuments = gFulltextMaxSegment;
            }

            auto tags = GetAllTags(table);
            ScanTags.push_back(tags.at("__ydb_added"));
            ScanTags.push_back(tags.at("__ydb_segment"));
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        YDB_LOG_INFO("Scan actor prepared",
            {"debug", Debug()});

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(const std::exception& exc) final
    {
        Uploader.AddIssue(exc);
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        record.SetFirstToken(FirstToken);
        record.SetLastToken(LastToken);
        record.SetFirstTokenRows(FirstTokenRows);
        record.SetLastTokenRows(LastTokenRows);

        Uploader.Finish(record, status);

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            YDB_LOG_NOTICE("Scan completed successfully",
                {"debug", Debug()},
                {"responseRecord", Response->Record.ShortDebugString()});
        } else {
            YDB_LOG_ERROR("Scan failed",
                {"debug", Debug()},
                {"responseRecord", Response->Record.ShortDebugString()});
        }
        Send(ResponseActorId, Response.Release());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    bool OnUnhandledException(const std::exception& exc) final
    {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void Describe(IOutputStream& out) const final
    {
        out << Debug();
    }

    EScan PageFault() final
    {
        YDB_LOG_TRACE("Page fault",
            {"debug", Debug()});
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        YDB_LOG_TRACE("Seek",
            {"seekSequence", seq},
            {"debug", Debug()});

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead.To(ScanTags, {}, NTable::ESeek::Lower);

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        YDB_LOG_TRACE("Scan range exhausted",
            {"debug", Debug()});

        IsExhausted = true;
        if (LastTokenRows > 0) {
            FinishToken(true);
        }

        // call Seek to wait uploads
        return EScan::Reset;
    }

protected:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                YDB_LOG_ERROR("Unexpected event in scan actor",
                    {"eventType", ev->GetTypeRewrite()},
                    {"eventDetails", ev->ToString()},
                    {"debug", Debug()});
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        YDB_LOG_DEBUG("Retrying row upload",
            {"debug", Debug()});

        Uploader.RetryUpload();
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        YDB_LOG_DEBUG("Received row upload response",
            {"debug", Debug()},
            {"senderActorId", ev->Sender});

        if (!Driver) {
            return;
        }

        Uploader.Handle(ev);

        if (Uploader.GetUploadStatus().IsSuccess()) {
            Driver->Touch(EScan::Feed);
            return;
        }

        if (auto retryAfter = Uploader.GetRetryAfter(); retryAfter) {
            YDB_LOG_NOTICE("Row upload failed with retriable error",
                {"debug", Debug()},
                {"uploadStatus", Uploader.GetUploadStatus()});
            ctx.Schedule(*retryAfter, new TEvents::TEvWakeup());
            return;
        }

        YDB_LOG_NOTICE("Row upload failed, aborting scan",
            {"debug", Debug()},
            {"uploadStatus", Uploader.GetUploadStatus()});

        Driver->Touch(EScan::Final);
    }

    TString Debug() const
    {
        return TStringBuilder() << "TBuildFulltextDictScan TabletId: " << TabletId << " Id: " << BuildId
            << " SkipFirstToken: " << SkipFirstToken << " SkipLastToken: " << SkipLastToken
            << " " << Uploader.Debug();
    }

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        // Segments are grouped and compacted per (prefix..., token). The token sits at
        // key[NumPrefixColumns]; the prefix cells precede it. With prefix columns as the leading
        // sort key, the same token may appear under different prefixes (non-adjacent), so the group
        // boundary must be detected on the whole [prefix..., token] key, not on the token alone.
        auto groupCells = key.Slice(0, NumPrefixColumns + 1);
        TString groupSerialized = TSerializedCellVec::Serialize(groupCells);
        if (LastGroupKeySerialized != groupSerialized) {
            if (LastTokenRows > 0) {
                FinishToken(false);
            }
            LastGroupKey = TOwnedCellVec(groupCells);
            LastGroupKeySerialized = std::move(groupSerialized);
            LastToken = TString(groupCells.at(NumPrefixColumns).AsBuf());
        }
        if (!PostingBuf) {
            LastTokenRows++;
        } else {
            // During initial compaction, we know that all segments have added=true
            // and all their ranges are unique, so we can just concatenate all lists by token
            Y_ENSURE(row[0].AsValue<bool>());
            TConstArrayRef<ui8> inBuf((ui8*)row[1].AsBuf().data(), row[1].AsBuf().size());
            TDeltaReader rdr(inBuf, WithFreq, Signed);
            ui64 docId = 0;
            ui32 freq = 0;
            while (rdr.Read(docId, freq)) {
                if (Delta.GetCount() >= MaxSegmentDocuments) {
                    UploadSegment();
                }
                LastTokenRows++;
                Delta.Add(docId, freq);
            }
        }
    }

    void UploadSegment()
    {
        auto buf = Delta.GetBuf();
        if (buf.size()) {
            auto maxId = Delta.GetMaxId();
            // Key is [prefix..., token, __ydb_max_id, __ydb_generation]; LastGroupKey holds
            // [prefix..., token] (just the token for non-prefixed indexes).
            TVector<TCell> uploadKey;
            uploadKey.reserve(LastGroupKey.size() + 2);
            for (const auto& cell : LastGroupKey) {
                uploadKey.push_back(cell);
            }
            uploadKey.push_back(TCell::Make(std::numeric_limits<NTableIndex::NFulltext::TGen>::max()));
            uploadKey.push_back(KeyIs32 ? TCell::Make((ui32)maxId) : TCell::Make(maxId));
            TVector<TCell> uploadValue = {
                TCell::Make(true),
                TCell((const char*)buf.data(), buf.size()),
            };
            PostingBuf->AddRow(uploadKey, uploadValue);
        }
        Delta.Reset(WithFreq, Signed);
    }

    void FinishToken(bool last)
    {
        UploadSegment();
        if (last && SkipLastToken) {
            return;
        }
        if (SkipFirstToken && !FirstToken) {
            FirstToken = LastToken;
            FirstTokenRows = LastTokenRows;
        } else if (DictBuf) {
            TVector<TCell> pk = {TCell(LastToken)};
            TVector<TCell> freq = {TCell::Make(LastTokenRows)};
            DictBuf->AddRow(pk, freq, pk);
        }
        LastToken.clear();
        LastGroupKey = TOwnedCellVec();
        LastGroupKeySerialized.clear();
        LastTokenRows = 0;
    }
};

class TDataShard::TTxHandleSafeBuildFulltextDictScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildFulltextDictScan(TDataShard* self, TEvDataShard::TEvBuildFulltextDictRequest::TPtr&& ev)
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
    TEvDataShard::TEvBuildFulltextDictRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildFulltextDictRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeBuildFulltextDictScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildFulltextDictRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvBuildFulltextDictResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        YDB_LOG_NOTICE("Starting fulltext dictionary build scan",
            {"tabletId", TabletID()},
            {"request", request.ShortDebugString()},
            {"rowVersion", rowVersion});

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
                YDB_LOG_ERROR("Rejecting invalid fulltext dictionary build scan request",
                    {"tabletId", TabletID()},
                    {"request", request.ShortDebugString()},
                    {"responseRecord", response->Record.ShortDebugString()});
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

        // 3. Validating fulltext index settings
        if (request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance &&
            request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::FulltextCompact &&
            request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance &&
            request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            badRequest(TStringBuilder() << "Unsupported index type");
        }

        if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance) {
            if (!request.GetPostingTableName()) {
                badRequest(TStringBuilder() << "Empty output posting table name");
            }
        } else if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance) {
            if (request.GetPostingTableName()) {
                badRequest(TStringBuilder() << "Output posting table name is set for a non-compact index");
            }
        }

        if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance) {
            if (!request.GetDictTableName()) {
                badRequest(TStringBuilder() << "Empty output dictionary table name");
            }
        } else if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            if (request.GetDictTableName()) {
                badRequest(TStringBuilder() << "Output dict table name is set for a plain index");
            }
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TBuildFulltextDictScan(
            TabletID(), userTable, request, ev->Sender, std::move(response)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, request.GetReadShadowData() ? userTable.ShadowTid : userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvBuildFulltextDictResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TBuildFulltextDictScan");
    }
}

}
