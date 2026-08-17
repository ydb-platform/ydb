#include "common_helper.h"
#include "../datashard_impl.h"
#include "../range_ops.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/library/json_index/json_index.h>
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
 * TBuildFulltextIndexScan scans the source document table and calculates the token posting table.
 *
 * This scan takes the main table and writes output to indexImplTable.
 *
 * Source columns: <PK columns>, <text column>, <data columns>, <prefix columns>
 * Destination columns with a FulltextPlain index: <prefix columns>, __ydb_token, <PK columns>, <data columns>
 * Destination columns with a FulltextRelevance index: <prefix columns>, __ydb_token, <PK columns>, __ydb_freq
 * Destination columns with a FulltextCompact/FulltextCompactRelevance/JsonCompact index:
 *   __ydb_token, __ydb_generation (always max), __ydb_max_id, __ydb_added (always true), __ydb_segment
 *
 * Request:
 * - The client sends TEvBuildFulltextIndexRequest with:
 *   - Name of the target table
 *   - Fulltext index settings
 *   - Data columns
 *
 * Execution Flow:
 * - TBuildFulltextIndexScan scans the whole input shard
 * - Extracts tokens from the text column using tokenizers set in the index settings
 * - When the index has FulltextRelevance type, it also calculates __ydb_freq for each token
 *   as the number of its occurrences in the document
 * - Tokens are inserted into the index table with their __ydb_freqs if required
 */

struct TTokenState {
    TString Token;
    // Segments are bucketed per (prefix, token): a delta segment is a doc-id run within a single
    // prefix, so different prefix values must never share a segment. BucketKey uniquely identifies
    // the (prefix, token) bucket; Prefix holds the leading key cells prepended to the posting key.
    TString BucketKey;
    TOwnedCellVec Prefix;
    TDeltaWriter Segment;
};

struct TTokenStateLess {
    bool operator()(const TTokenState* a, const TTokenState* b) const {
        return a->Segment.GetCount() > b->Segment.GetCount() || a->Segment.GetCount() == b->Segment.GetCount() && a->BucketKey < b->BucketKey;
    }
};

class TBuildFulltextIndexScan: public TActor<TBuildFulltextIndexScan>, public IActorExceptionHandler, public NTable::IScan {
    IDriver* Driver = nullptr;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 JsonErrors = 0;

    ui64 DocCount = 0;
    ui64 TotalDocLength = 0;

    // Diagnostic counters: how many entries the scan added to each destination
    // buffer (cumulative, never reset on flush). These must match what
    // eventually lands in the impl tables — when they don't, the docs build
    // lost rows somewhere on the upload path.
    ui64 PostingEntriesAdded = 0;
    ui64 DocsEntriesAdded = 0;

    TTags ScanTags;
    TString TextColumn;
    Ydb::Table::FulltextIndexSettings::Analyzers TextAnalyzers;
    bool IsBinaryJson = false;
    bool UseRowIdAsDocId = false;
    size_t RowIdRowIndex = 0; // position of __ydb_row_id in the row returned by the scan

    // Prefix (leading) key columns of the posting table. The posting key is
    // [prefix..., __ydb_token, doc_id...]. PrefixRowIndices are their positions
    // in the row returned by the scan.
    TVector<TString> PrefixColumns;
    TVector<size_t> PrefixRowIndices;

    TBatchRowsUploader Uploader;
    TBufferData* UploadBuf = nullptr;
    TBufferData* DocsBuf = nullptr;

    bool Compact = false;
    bool WithRelevance = false;
    THashMap<TString, TTokenState> TokenBuf;
    TSet<TTokenState*, TTokenStateLess> TokensBySize;
    ui64 BufferedBytes = 0;
    ui64 EmptyTokenBytes = 0;

    ui64 MaxBatchBytes = 0;
    ui64 MaxSegmentDocuments = 0;

    TVector<NScheme::TTypeInfo> KeyTypes;
    NScheme::TTypeId KeyTypeId;
    bool Signed = false;
    TSerializedTableRange RequestedRange;
    TSerializedTableRange TableRange;
    TSerializedCellVec LastProcessedKey;
    TSerializedCellVec LastAckedKey;

    const NKikimrTxDataShard::TEvBuildFulltextIndexRequest Request;
    const TActorId ResponseActorId;
    const TAutoPtr<TEvDataShard::TEvBuildFulltextIndexResponse> Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::BUILD_FULLTEXT_INDEX;
    }

    TBuildFulltextIndexScan(ui64 tabletId, const TUserTable& table, NKikimrTxDataShard::TEvBuildFulltextIndexRequest request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvBuildFulltextIndexResponse>&& response)
        : TActor{&TThis::StateWork}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Uploader(request.GetDatabaseName(), request.GetScanSettings())
        , MaxBatchBytes(request.GetScanSettings().GetMaxBatchBytes())
        , KeyTypes(table.KeyColumnTypes)
        , KeyTypeId(table.KeyColumnTypes[0].GetTypeId())
        , TableRange(table.Range)
        , Request(std::move(request))
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
    {
        if (Request.HasKeyRange()) {
            RequestedRange.Load(Request.GetKeyRange());
        } else {
            RequestedRange = TableRange;
        }

        YDB_LOG_INFO("Scan actor created",
            {"debug", Debug()});

        Y_ENSURE(Request.settings().columns().size() == 1);
        TextColumn = Request.settings().columns().at(0).column();
        TextAnalyzers = Request.settings().columns().at(0).analyzers();
        UseRowIdAsDocId = Request.GetUseRowIdAsDocId();
        Signed = (KeyTypeId == NScheme::NTypeIds::Int64 || KeyTypeId == NScheme::NTypeIds::Int32);

        auto tags = GetAllTags(table);
        auto types = GetAllTypes(table);

        const auto& textType = types.at(TextColumn);
        if (textType.GetTypeId() == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
            IsBinaryJson = true;
        }

        {
            ScanTags.push_back(tags.at(TextColumn));

            for (auto dataColumn : Request.GetDataColumns()) {
                if (dataColumn != TextColumn) {
                    ScanTags.push_back(tags.at(dataColumn));
                }
            }

            if (UseRowIdAsDocId) {
                RowIdRowIndex = ScanTags.size();
                ScanTags.push_back(tags.at(NKikimr::NTableIndex::NFulltext::RowIdColumn));
            }

            PrefixColumns.assign(Request.GetPrefixColumns().begin(), Request.GetPrefixColumns().end());
            for (const auto& prefixColumn : PrefixColumns) {
                PrefixRowIndices.push_back(ScanTags.size());
                ScanTags.push_back(tags.at(prefixColumn));
            }
        }

        auto addType = [&](auto& uploadTypes, const auto& column) {
            auto it = types.find(column);
            if (it != types.end()) {
                Ydb::Type type;
                NScheme::ProtoFromTypeInfo(it->second, type);
                uploadTypes->emplace_back(it->first, type);
            }
        };

        MakeTokenTypes(table, types, addType);

        if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompact ||
            Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance ||
            Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            Compact = true;
        }

        if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance ||
            Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance) {
            WithRelevance = true;
            MakeDocsTypes(table, addType);
        }

        MaxSegmentDocuments = gFulltextMaxSegment;
    }

    // Emit the doc-id key columns for an upload destination: a single __ydb_row_id (UINT64) when the
    // fulltext index uses rowid-as-docid, otherwise the base table primary-key columns.
    void AddDocIdColumns(std::shared_ptr<NTxProxy::TUploadTypes>& uploadTypes, const TUserTable& table,
        const std::function<void(std::shared_ptr<NTxProxy::TUploadTypes>&, const TString&)>& addType)
    {
        if (UseRowIdAsDocId) {
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT64);
            uploadTypes->emplace_back(NKikimr::NTableIndex::NFulltext::RowIdColumn, type);
        } else {
            for (const auto& column : table.KeyColumnIds) {
                addType(uploadTypes, table.Columns.at(column).Name);
            }
        }
    }

    void MakeTokenTypes(const TUserTable& table, const TColumnsTypes& types, std::function<void(std::shared_ptr<NTxProxy::TUploadTypes>& uploadTypes, const TString& column)> addType)
    {
        auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();

        // Leading prefix key columns come first in the posting key.
        for (const auto& prefixColumn : PrefixColumns) {
            addType(uploadTypes, prefixColumn);
        }

        if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::Json ||
            Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            Ydb::Type type;
            type.set_type_id(Ydb::Type::STRING);
            uploadTypes->emplace_back(TokenColumn, type);
        } else {
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(types.at(TextColumn), type);
            uploadTypes->emplace_back(TokenColumn, type);
        }

        switch (Request.GetIndexType()) {
        case NKikimrTxDataShard::EFulltextIndexType::FulltextCompact:
        case NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance:
        case NKikimrTxDataShard::EFulltextIndexType::JsonCompact:
            {
                Ydb::Type type;
                type.set_type_id(NTableIndex::NFulltext::GenType);
                uploadTypes->emplace_back(GenColumn, type);
            }
            {
                Ydb::Type type;
                NScheme::ProtoFromTypeInfo(KeyTypes.at(0), type);
                uploadTypes->emplace_back(MaxIdColumn, type);
            }
            {
                Ydb::Type type;
                type.set_type_id(Ydb::Type::BOOL);
                uploadTypes->emplace_back(AddedColumn, type);
            }
            {
                Ydb::Type type;
                type.set_type_id(Ydb::Type::STRING);
                uploadTypes->emplace_back(SegmentColumn, type);
            }
            break;
        case NKikimrTxDataShard::EFulltextIndexType::FulltextPlain:
        case NKikimrTxDataShard::EFulltextIndexType::Json:
            AddDocIdColumns(uploadTypes, table, addType);
            for (auto dataColumn : Request.GetDataColumns()) {
                addType(uploadTypes, dataColumn);
            }
            break;
        case NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance:
            AddDocIdColumns(uploadTypes, table, addType);
            {
                Ydb::Type type;
                type.set_type_id(TokenCountType);
                uploadTypes->emplace_back(FreqColumn, type);
            }
            break;
        }

        UploadBuf = Uploader.AddDestination(Request.GetIndexName(), std::move(uploadTypes));
    }

    void MakeDocsTypes(const TUserTable& table, std::function<void(std::shared_ptr<NTxProxy::TUploadTypes>& uploadTypes, const TString& column)> addType)
    {
        auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
        AddDocIdColumns(uploadTypes, table, addType);
        for (auto dataColumn : Request.GetDataColumns()) {
            addType(uploadTypes, dataColumn);
        }
        {
            Ydb::Type type;
            type.set_type_id(TokenCountType);
            uploadTypes->emplace_back(DocLengthColumn, type);
        }
        DocsBuf = Uploader.AddDestination(Request.GetDocsTableName(), std::move(uploadTypes));
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

    EScan Seek(TLead& lead, ui64 seq) final
    {
        YDB_LOG_TRACE("Seek",
            {"seekSequence", seq},
            {"debug", Debug()});

        if (seq) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        // Effective doc-id key cells: either the table PK or the single __ydb_row_id cell.
        TArrayRef<const TCell> docIdKey = key;
        TCell rowIdCell;
        if (UseRowIdAsDocId) {
            rowIdCell = row.Get(RowIdRowIndex);
            docIdKey = TArrayRef<const TCell>(&rowIdCell, 1);
        }

        // Leading prefix key cells of the posting table.
        TVector<TCell> prefixCells(::Reserve(PrefixRowIndices.size()));
        for (size_t prefixIndex : PrefixRowIndices) {
            prefixCells.push_back(row.Get(prefixIndex));
        }

        TVector<TString> tokens;
        if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::Json ||
            Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            if (IsBinaryJson) {
                tokens = NJsonIndex::TokenizeBinaryJson(row.Get(0).AsBuf());
            } else {
                TString error;
                tokens = NJsonIndex::TokenizeJson(row.Get(0).AsBuf(), error);
                if (error != "") {
                    JsonErrors++;
                }
            }
        } else {
            tokens = Analyze(row.Get(0).AsBuf(), TextAnalyzers);
        }
        if (Compact) {
            Y_ENSURE(key.size() == 1);
            ui64 docId;
            if (KeyTypeId == NScheme::NTypeIds::Uint64 || KeyTypeId == NScheme::NTypeIds::Int64) {
                docId = key[0].AsValue<ui64>();
            } else if (KeyTypeId == NScheme::NTypeIds::Int32) {
                docId = (ui64)(i64)key[0].AsValue<i32>(); // sign-extend
            } else {
                docId = key[0].AsValue<ui32>();
            }
            std::sort(tokens.begin(), tokens.end());
            ui32 freq = 1;
            for (size_t i = 0; i < tokens.size(); i++) {
                if (i < tokens.size() - 1 && tokens[i] == tokens[i + 1]) {
                    freq++;
                    continue;
                }
                TString bucketKey = MakeCompactBucketKey(prefixCells, tokens[i]);
                auto& state = TokenBuf[bucketKey];
                if (state.BucketKey.empty()) {
                    state.Token = tokens[i];
                    state.BucketKey = bucketKey;
                    state.Prefix = TOwnedCellVec(prefixCells);
                    state.Segment.Reset(WithRelevance, Signed);
                    BufferedBytes += bucketKey.size(); // count bucket-key sizes
                } else if (!state.Segment.GetCount()) {
                    EmptyTokenBytes -= bucketKey.size();
                    state.Segment.Reset(WithRelevance, Signed);
                }
                TokensBySize.erase(&state);
                BufferedBytes -= state.Segment.GetBuf().size();
                state.Segment.Add(docId, freq);
                BufferedBytes += state.Segment.GetBuf().size();
                TokensBySize.insert(&state);
                freq = 1;
                if (state.Segment.GetCount() >= MaxSegmentDocuments) {
                    FlushToken(state);
                }
            }
            if (BufferedBytes >= MaxBatchBytes) {
                // flush the most frequent tokens
                while (BufferedBytes >= MaxBatchBytes/2) {
                    auto mostFreqIt = TokensBySize.begin();
                    Y_ENSURE(mostFreqIt != TokensBySize.end());
                    if (!(*mostFreqIt)->Segment.GetCount()) {
                        break;
                    }
                    FlushToken(TokenBuf.at((*mostFreqIt)->BucketKey));
                }
            }
            if (EmptyTokenBytes >= MaxBatchBytes/3) {
                FlushAllTokens();
            }
            if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance) {
                UploadDocRow(key, row, tokens.size());
            }
        } else if (Request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance) {
            LastProcessedKey = TSerializedCellVec(key);
            // Snapshot the source key BEFORE the uploads so that any flush they trigger records this
            // key as the per-destination boundary; GetMinFlushedKey() then yields a checkpoint that is
            // safe across the asymmetrically-filling posting and docs buffers.
            Uploader.SetCurrentSourceKey(LastProcessedKey);
            UploadFulltextRelevance(prefixCells, docIdKey, tokens);
            UploadDocRow(docIdKey, row, tokens.size());
        } else {
            LastProcessedKey = TSerializedCellVec(key);
            Uploader.SetCurrentSourceKey(LastProcessedKey);
            UploadFulltextPlain(prefixCells, docIdKey, row, tokens);
        }

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    void UploadFulltextRelevance(TArrayRef<const TCell> prefix, TArrayRef<const TCell> key, const TVector<TString>& tokens)
    {
        THashMap<TString, ui32> tokenFreq;
        for (const auto& token : tokens) {
            tokenFreq[token]++;
        }

        TVector<TCell> uploadKey(::Reserve(prefix.size() + key.size() + 1));
        TVector<TCell> uploadValue(::Reserve(1));

        for (const auto& [token, freq] : tokenFreq) {
            uploadKey.clear();
            uploadKey.insert(uploadKey.end(), prefix.begin(), prefix.end());
            uploadKey.push_back(TCell(token));
            uploadKey.insert(uploadKey.end(), key.begin(), key.end());

            uploadValue.clear();
            uploadValue.push_back(TCell::Make(freq));

            UploadBuf->AddRow(uploadKey, uploadValue);
            ++PostingEntriesAdded;
        }
    }

    void UploadDocRow(TArrayRef<const TCell> key, const TRow& row, ui32 totalTokens)
    {
        TVector<TCell> uploadValue(::Reserve(Request.GetDataColumns().size()));
        uploadValue.clear();

        // Include data columns in indexImplDocsTable
        size_t index = 1; // skip text column
        for (auto dataColumn : Request.GetDataColumns()) {
            if (dataColumn != TextColumn) {
                uploadValue.push_back(row.Get(index++));
            } else {
                uploadValue.push_back(row.Get(0));
            }
        }
        // Document length column
        uploadValue.push_back(TCell::Make(totalTokens));
        DocsBuf->AddRow(key, uploadValue);
        ++DocsEntriesAdded;

        DocCount++;
        TotalDocLength += totalTokens;
    }

    void UploadFulltextPlain(TArrayRef<const TCell> prefix, TArrayRef<const TCell> key, const TRow& row, const TVector<TString>& tokens)
    {
        TVector<TCell> uploadKey(::Reserve(prefix.size() + key.size() + 1));
        TVector<TCell> uploadValue(::Reserve(Request.GetDataColumns().size()));

        for (const auto& token : tokens) {
            uploadKey.clear();
            uploadKey.insert(uploadKey.end(), prefix.begin(), prefix.end());
            uploadKey.push_back(TCell(token));
            uploadKey.insert(uploadKey.end(), key.begin(), key.end());

            uploadValue.clear();
            // Include data columns in every posting row (poor, but anyway)
            size_t index = 1; // skip text column
            for (auto dataColumn : Request.GetDataColumns()) {
                if (dataColumn != TextColumn) {
                    uploadValue.push_back(row.Get(index++));
                } else {
                    uploadValue.push_back(row.Get(0));
                }
            }

            UploadBuf->AddRow(uploadKey, uploadValue);
            ++PostingEntriesAdded;
        }
    }

    // Unique map key for the (prefix, token) segment bucket. Without prefix it is the raw token
    // (preserving the non-prefixed behavior); with prefix it is the serialized [prefix..., token] key.
    static TString MakeCompactBucketKey(TArrayRef<const TCell> prefix, const TString& token)
    {
        if (prefix.empty()) {
            return token;
        }
        TVector<TCell> cells(::Reserve(prefix.size() + 1));
        cells.insert(cells.end(), prefix.begin(), prefix.end());
        cells.push_back(TCell(token.data(), token.size()));
        return TSerializedCellVec::Serialize(cells);
    }

    void FlushToken(TTokenState& state)
    {
        if (!state.Segment.GetCount()) {
            return;
        }
        auto segment = state.Segment.GetBuf();
        TVector<TCell> uploadKey(::Reserve(state.Prefix.size() + 3));
        uploadKey.insert(uploadKey.end(), state.Prefix.begin(), state.Prefix.end());
        uploadKey.push_back(TCell(state.Token));
        uploadKey.push_back(TCell::Make(std::numeric_limits<NTableIndex::NFulltext::TGen>::max()));
        if (KeyTypeId == NScheme::NTypeIds::Uint64 || KeyTypeId == NScheme::NTypeIds::Int64) {
            uploadKey.push_back(TCell::Make(state.Segment.GetMaxId()));
        } else {
            uploadKey.push_back(TCell::Make((ui32)state.Segment.GetMaxId()));
        }
        TVector<TCell> uploadValue(::Reserve(2));
        uploadValue.push_back(TCell::Make(true));
        uploadValue.push_back(TCell((const char*)segment.data(), segment.size()));
        UploadBuf->AddRow(uploadKey, uploadValue);
        TokensBySize.erase(&state);
        BufferedBytes -= state.Segment.GetBuf().size();
        EmptyTokenBytes += state.BucketKey.size();
        state.Segment = TDeltaWriter();
        TokensBySize.insert(&state);
    }

    void FlushAllTokens()
    {
        if (!TokenBuf.size()) {
            return;
        }
        for (auto& [token, state] : TokenBuf) {
            FlushToken(state);
        }
        BufferedBytes = 0;
        EmptyTokenBytes = 0;
        TokenBuf.clear();
        TokensBySize.clear();
    }

    EScan PageFault() final
    {
        YDB_LOG_TRACE("Page fault",
            {"debug", Debug()});
        return EScan::Feed;
    }

    EScan Exhausted() final
    {
        FlushAllTokens();

        if (JsonErrors > 0) {
            YDB_LOG_WARN("Invalid JSON in scanned rows",
                {"invalidJsonRowCount", JsonErrors},
                {"debug", Debug()});
        }
        YDB_LOG_DEBUG("Posting scan range exhausted",
            {"readRowCount", ReadRows},
            {"docCount", DocCount},
            {"postingEntriesAdded", PostingEntriesAdded},
            {"docEntriesAdded", DocsEntriesAdded},
            {"debug", Debug()});

        // call Seek to wait uploads
        return EScan::Reset;
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
        record.SetDocCount(DocCount);
        record.SetTotalDocLength(TotalDocLength);

        if (LastAckedKey.GetBuffer()) {
            record.SetLastKeyAck(LastAckedKey.GetBuffer());
        }

        Uploader.Finish(record, status);

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            YDB_LOG_NOTICE("Posting scan completed successfully",
                {"debug", Debug()},
                {"postingEntriesAdded", PostingEntriesAdded},
                {"docEntriesAdded", DocsEntriesAdded},
                {"responseRecord", Response->Record.ShortDebugString()});
        } else {
            YDB_LOG_ERROR("Posting scan failed",
                {"debug", Debug()},
                {"postingEntriesAdded", PostingEntriesAdded},
                {"docEntriesAdded", DocsEntriesAdded},
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
        YDB_LOG_DEBUG("Received row upload response for posting entries",
            {"debug", Debug()},
            {"postingEntriesAdded", PostingEntriesAdded},
            {"docEntriesAdded", DocsEntriesAdded},
            {"senderActorId", ev->Sender});

        if (!Driver) {
            return;
        }

        bool batchUploaded = Uploader.Handle(ev);

        if (Uploader.GetUploadStatus().IsSuccess()) {
            if (batchUploaded) {
                // Use the cross-destination safe checkpoint, not the scan's
                // LastProcessedKey. In relevance mode the posting buffer
                // fills much faster than docs; advancing LastAckedKey on
                // every posting flush would leave docs entries for those
                // same rows still buffered. On shard restart the SchemeShard
                // would resume from LastAckedKey-exclusive and the buffered
                // docs entries would be permanently lost.
                if (auto safeKey = Uploader.GetMinFlushedKey();
                    safeKey && safeKey->GetBuffer() &&
                    safeKey->GetBuffer() != LastAckedKey.GetBuffer()) {
                    LastAckedKey = std::move(*safeKey);

                    auto progress = MakeHolder<TEvDataShard::TEvBuildFulltextIndexResponse>();
                    auto& record = progress->Record;
                    record.SetId(BuildId);
                    record.SetTabletId(TabletId);
                    record.SetRequestSeqNoGeneration(Request.GetSeqNoGeneration());
                    record.SetRequestSeqNoRound(Request.GetSeqNoRound());
                    record.SetStatus(NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
                    record.SetLastKeyAck(LastAckedKey.GetBuffer());
                    Send(ResponseActorId, progress.Release());
                }
            }
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
        return TStringBuilder() << "TBuildFulltextIndexScan TabletId: " << TabletId << " Id: " << BuildId
            << ", last acked key: " << DebugPrintPoint(KeyTypes, LastAckedKey.GetCells(), *AppData()->TypeRegistry)
            << " " << Uploader.Debug();
    }
};

class TDataShard::TTxHandleSafeBuildFulltextIndexScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildFulltextIndexScan(TDataShard* self, TEvDataShard::TEvBuildFulltextIndexRequest::TPtr&& ev)
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
    TEvDataShard::TEvBuildFulltextIndexRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildFulltextIndexRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeBuildFulltextIndexScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildFulltextIndexRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvBuildFulltextIndexResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        YDB_LOG_NOTICE("Starting fulltext index build scan",
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
                YDB_LOG_ERROR("Rejecting invalid fulltext index build scan request",
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
        // The snapshot is optional: live-table scans (plain/relevance/compact over the main table)
        // always carry one, but the compact rowid-mode posting fill scans the transient seq-keyed
        // row-id source table - a build impl table that is immutable once the prepass completed and so
        // needs no snapshot (mirrors the dictionary scan over the 0build impl table, fulltext_dict.cpp).
        if (request.HasSnapshotStep() || request.HasSnapshotTxId()) {
            const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
            if (!SnapshotManager.FindAvailable(snapshotKey)) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                    << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
            }
        }

        if (!request.GetIndexName()) {
            badRequest(TStringBuilder() << "Empty index table name");
        }

        auto tags = GetAllTags(userTable);
        auto types = GetAllTypes(userTable);
        for (auto column : request.GetSettings().columns()) {
            if (!tags.contains(column.column())) {
                badRequest(TStringBuilder() << "Unknown key column: " << column.column());
            } else if (types.at(column.column()).GetTypeId() == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
                if (request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::Json &&
                    request.GetIndexType() != NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
                    badRequest("Indexing binary JSON requires JSON index type");
                }
            }
        }
        for (auto dataColumn : request.GetDataColumns()) {
            if (!tags.contains(dataColumn)) {
                badRequest(TStringBuilder() << "Unknown data column: " << dataColumn);
            }
        }

        if (request.GetUseRowIdAsDocId() && !tags.contains(NKikimr::NTableIndex::NFulltext::RowIdColumn)) {
            badRequest(TStringBuilder() << "UseRowIdAsDocId requested but column '"
                << NKikimr::NTableIndex::NFulltext::RowIdColumn << "' not found in table");
        }

        for (const auto& prefixColumn : request.GetPrefixColumns()) {
            if (!tags.contains(prefixColumn)) {
                badRequest(TStringBuilder() << "Unknown prefix column: " << prefixColumn);
            }
        }

        if (request.GetPrefixColumns().size() &&
            (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::Json ||
             request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact)) {
            badRequest(TStringBuilder() << "Prefix columns are not supported for JSON indexes");
        }

        // Compact builds scan a table with a single integer key holding the doc id: either the main
        // table (single-integer-PK case) or, for an arbitrary-PK main table, the row-id source table
        // built by the rowid-mode prepass (a generic secondary-index build keyed by __ydb_row_id).
        if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompact ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextCompactRelevance ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            if (userTable.KeyColumnTypes.size() != 1) {
                badRequest(TStringBuilder() << "Source table must have a single key column");
            } else {
                auto keyTypeId = userTable.KeyColumnTypes[0].GetTypeId();
                if (keyTypeId != NScheme::NTypeIds::Uint64 &&
                    keyTypeId != NScheme::NTypeIds::Int64 &&
                    keyTypeId != NScheme::NTypeIds::Uint32 &&
                    keyTypeId != NScheme::NTypeIds::Int32) {
                    badRequest(TStringBuilder() << "Source table must have a single uint64/uint32/int64/int32 key column");
                }
            }
        }

        if (trySendBadRequest()) {
            return;
        }

        // 3. Validating fulltext index settings
        if (!request.HasSettings()) {
            badRequest(TStringBuilder() << "Missing fulltext index settings");
        } else if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::Json ||
            request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::JsonCompact) {
            if (!request.GetSettings().columns_size()) {
                badRequest(TStringBuilder() << "JSON columns should be set");
            }
        } else {
            TString error;
            if (!NKikimr::NFulltext::ValidateSettings(request.GetSettings(), error)) {
                badRequest(error);
            }
            if (request.GetIndexType() == NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance &&
                !request.GetDocsTableName()) {
                badRequest(TStringBuilder() << "Empty index documents table name");
            }
        }

        if (trySendBadRequest()) {
            return;
        }

        // 4. Creating scan
        TAutoPtr<NTable::IScan> scan = new TBuildFulltextIndexScan(TabletID(), userTable,
            request, ev->Sender, std::move(response));

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvBuildFulltextIndexResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TBuildFulltextIndexScan");
    }
}

}
