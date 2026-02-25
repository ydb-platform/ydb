#include "kqp_full_text_source.h"

#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <util/generic/intrlist.h>
#include <util/string/vector.h>
#include <util/generic/queue.h>
#include <util/generic/algorithm.h>

#include <util/string/escape.h>

#include <cmath>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NTableIndex::NFulltext;

static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(10);

// replace with parameters from settings
constexpr double K1_FACTOR_DEFAULT = 1.2;
constexpr double B_FACTOR_DEFAULT = 0.75;
constexpr double EPSILON = 1e-6;
constexpr i32 RELEVANCE_COLUMN_MARKER = -1;
constexpr double NGRAM_IMBALANCE_FACTOR = 10;

class TDocId;

template <typename T>
class TTableReader : public TAtomicRefCount<T> {
    TIntrusivePtr<TKqpCounters> Counters;
    TTableId TableId;
    TString TablePath;
    IKqpGateway::TKqpSnapshot Snapshot;
    TString LogPrefix;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    bool UseArrowFormat = false;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;
    TVector<i32> ResultColumnIds;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> PartitionInfo;

public:

    TTableReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : Counters(counters)
        , TableId(tableId)
        , TablePath(tablePath)
        , Snapshot(snapshot)
        , LogPrefix(logPrefix)
        , KeyColumnTypes(keyColumnTypes)
        , ResultColumnTypes(resultColumnTypes)
        , ResultColumnIds(resultColumnIds)
    {}

    void SetPartitionInfo(const THolder<TKeyDesc>& keyDesc) {
        YQL_ENSURE(keyDesc->TableId == TableId, "Table ID mismatch");
        PartitionInfo = keyDesc->Partitioning;
    }

    TString GetTablePath() const {
        return TablePath;
    }

    ui64 GetReadRows() const {
        return ReadRows;
    }

    ui64 GetReadBytes() const {
        return ReadBytes;
    }

    const TTableId GetTableId() const {
        return TableId;
    }

    bool GetUseArrowFormat() const {
        return UseArrowFormat;
    }

    void SetUseArrowFormat(bool useArrowFormat) {
        UseArrowFormat = useArrowFormat;
    }

    const TConstArrayRef<NScheme::TTypeInfo> GetKeyColumnTypes() const {
        return KeyColumnTypes;
    }

    const TConstArrayRef<NScheme::TTypeInfo> GetResultColumnTypes() const {
        return ResultColumnTypes;
    }

    void RecvStats(ui64 rowCount, ui64 rowBytes) {
        ReadRows += rowCount;
        ReadBytes += rowBytes;
    }

    template <typename TableRange>
    std::unique_ptr<TEvDataShard::TEvRead> GetReadRequest(ui64 readId, const TableRange& range) {
        return GetReadRequest(readId, std::deque<TableRange>{range});
    }

    template <typename TableRange>
    std::unique_ptr<TEvDataShard::TEvRead> GetReadRequest(ui64 readId, const std::deque<TableRange>& ranges) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (size_t i = 0; i < ResultColumnIds.size(); i++) {
            record.AddColumns(ResultColumnIds[i]);
        }

        for(const auto& range: ranges) {
            if (range.Point) {
                request->Keys.emplace_back(TSerializedCellVec(range.From));
            } else {
                YQL_ENSURE(!range.Point);
                if (range.To.size() < KeyColumnTypes.size()) {
                    // absent cells mean infinity => in prefix notation `To` should be inclusive
                    request->Ranges.emplace_back(TSerializedTableRange(range.From, range.InclusiveFrom, range.To, true));
                } else {
                    request->Ranges.emplace_back(TSerializedTableRange(range));
                }
            }
        }

        Counters->CreatedIterators->Inc();

        if (Snapshot.IsValid()) {
            record.MutableSnapshot()->SetStep(Snapshot.Step);
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
        }

        auto defaultSettings = GetDefaultReadSettings()->Record;
        auto MaxRowsDefaultQuota = defaultSettings.GetMaxRows();
        auto MaxBytesDefaultQuota = defaultSettings.GetMaxBytes();

        record.SetMaxRows(MaxRowsDefaultQuota);
        record.SetMaxBytes(MaxBytesDefaultQuota);
        record.SetResultFormat(UseArrowFormat ? NKikimrDataEvents::FORMAT_ARROW : NKikimrDataEvents::FORMAT_CELLVEC);

        return request;
    }

    void AddResolvePartitioningRequest(std::unique_ptr<NSchemeCache::TSchemeCacheRequest>& request) {
        auto keyColumnTypes = KeyColumnTypes;

        TVector<TCell> minusInf(keyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(
            TableId, range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Counters->IteratorsShardResolve->Inc();
    }

    std::vector<std::pair<ui64, TTableRange>> GetRangePartitioning(const TTableRange& range) {

        YQL_ENSURE(PartitionInfo);

        // Binary search of the index to start with.
        size_t idxStart = 0;
        size_t idxFinish = PartitionInfo->size();
        while ((idxFinish - idxStart) > 1) {
            size_t idxCur = (idxFinish + idxStart) / 2;
            const auto& partCur = (*PartitionInfo)[idxCur].Range->EndKeyPrefix.GetCells();
            YQL_ENSURE(partCur.size() <= KeyColumnTypes.size());
            int cmp = CompareTypedCellVectors(partCur.data(), range.From.data(), KeyColumnTypes.data(),
                                            std::min(partCur.size(), range.From.size()));
            if (cmp < 0) {
                idxStart = idxCur;
            } else {
                idxFinish = idxCur;
            }
        }

        std::vector<TCell> minusInf(KeyColumnTypes.size());

        std::vector<std::pair<ui64, TTableRange>> rangePartition;
        for (size_t idx = idxStart; idx < PartitionInfo->size(); ++idx) {
            TTableRange partitionRange{
                idx == 0 ? minusInf : (*PartitionInfo)[idx - 1].Range->EndKeyPrefix.GetCells(),
                idx == 0 ? true : !(*PartitionInfo)[idx - 1].Range->IsInclusive,
                (*PartitionInfo)[idx].Range->EndKeyPrefix.GetCells(),
                (*PartitionInfo)[idx].Range->IsInclusive
            };

            if (range.Point) {
                int intersection = ComparePointAndRange(
                    range.From,
                    partitionRange,
                    KeyColumnTypes,
                    KeyColumnTypes);

                if (intersection == 0) {
                    rangePartition.emplace_back((*PartitionInfo)[idx].ShardId, range);
                } else if (intersection < 0) {
                    break;
                }
            } else {
                int intersection = CompareRanges(range, partitionRange, KeyColumnTypes);

                if (intersection == 0) {
                    auto rangeIntersection = Intersect(KeyColumnTypes, range, partitionRange);
                    rangePartition.emplace_back((*PartitionInfo)[idx].ShardId, rangeIntersection);
                } else if (intersection < 0) {
                    break;
                }
            }
        }

        return rangePartition;
    }
};

class TQueryCtx : public TAtomicRefCount<TQueryCtx> {
    const ui64 DocCount = 0;
    const double AvgDL = 1.0;
    std::vector<double> IDFValues;
    double K1Factor = K1_FACTOR_DEFAULT;
    double BFactor = B_FACTOR_DEFAULT;
    const TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    const EDefaultOperator DefaultOperator;
    ui32 MinimumShouldMatch;

public:
    TQueryCtx(size_t wordCount, ui64 totalDocLength, ui64 docCount,
        EDefaultOperator defaultOperator,
        ui32 minimumShouldMatch,
        const TVector<std::pair<i32, NScheme::TTypeInfo>> resultCellIndices)
        : DocCount(docCount)
        , AvgDL(docCount > 0 ? static_cast<double>(totalDocLength) / docCount : 1.0)
        , IDFValues(wordCount, 0.0)
        , ResultCellIndices(resultCellIndices)
        , DefaultOperator(defaultOperator)
        , MinimumShouldMatch(minimumShouldMatch)
    {
    }

    void SetBFactor(double bFactor) {
        BFactor = bFactor;
    }

    void SetK1Factor(double k1Factor) {
        K1Factor = k1Factor;
    }

    const TVector<std::pair<i32, NScheme::TTypeInfo>>& GetResultCellIndices() const {
        return ResultCellIndices;
    }

    void AddIDFValue(size_t wordIndex, ui64 docFreq) {
        YQL_ENSURE(wordIndex < IDFValues.size());
        IDFValues[wordIndex] = std::log((static_cast<double>(DocCount) - static_cast<double>(docFreq) + 0.5) / (static_cast<double>(docFreq) + 0.5) + 1);
    }

    double GetK1Factor() const {
        return K1Factor;
    }

    size_t GetWordCount() const {
        return IDFValues.size();
    }

    double GetIDFValue(ui64 wordIndex) const {
        YQL_ENSURE(wordIndex < IDFValues.size());
        return IDFValues[wordIndex];
    }

    double GetBFactor() const {
        return BFactor;
    }

    EDefaultOperator GetDefaultOperator() const {
        return DefaultOperator;
    }

    ui32 GetMinimumShouldMatch() const {
        return MinimumShouldMatch;
    }

    void ReduceMinimumShouldMatch(ui32 minShouldMatch) {
        if (MinimumShouldMatch > minShouldMatch) {
            MinimumShouldMatch = minShouldMatch;
        }
    }

    double GetAvgDL() const {
        return AvgDL;
    }
};

class TIndexTableImplReader;

TTableId FromProto(const ::NKqpProto::TKqpPhyTableId & proto) {
    return TTableId(
        (ui64)proto.GetOwnerId(),
        (ui64)proto.GetTableId(),
        proto.GetSysView(),
        (ui64)proto.GetVersion()
    );
}

class TDocumentInfo : public TSimpleRefCount<TDocumentInfo> {
    TOwnedCellVec KeyCells;
    TOwnedCellVec RowCells;
    ui64 DocumentLength = std::numeric_limits<ui64>::max();

public:
    using TPtr = TIntrusivePtr<TDocumentInfo>;
    const ui64 DocumentNumId = 0;
    std::vector<ui32> TokenFrequencies;

    TDocumentInfo(ui64 documentNumId)
        : DocumentNumId(documentNumId)
    {
        TVector<TCell> cells{TCell::Make<ui64>(DocumentNumId)};
        KeyCells = TOwnedCellVec(cells);
    }

    void SetDocumentLength(ui64 documentLength) {
        DocumentLength = documentLength;
    }

    bool HasDocumentLength() const {
        return DocumentLength != std::numeric_limits<ui64>::max();
    }

    double GetBM25Score(const TQueryCtx& queryCtx) const {
        double score = 0;
        const double avgDocLength = queryCtx.GetAvgDL();
        const double k1Factor = queryCtx.GetK1Factor();
        const double bFactor = queryCtx.GetBFactor();
        const double documentFactor = k1Factor * (1 - bFactor + bFactor * static_cast<double>(DocumentLength) / avgDocLength);
        for(size_t i = 0; i < TokenFrequencies.size(); ++i) {
            double docFreq = static_cast<double>(TokenFrequencies[i]);
            double idf = queryCtx.GetIDFValue(i);
            double tf = docFreq / (docFreq + documentFactor);
            score += idf * tf;
        }
        return score;
    }

    void AddRow(const TConstArrayRef<TCell>& row) {
        RowCells = TOwnedCellVec(row);
    }

    TCell GetResultCell(const size_t idx) const {
        return RowCells.at(idx);
    }

    NUdf::TUnboxedValue GetRow(const TQueryCtx& queryCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory, i64& computeBytes) const {
        NUdf::TUnboxedValue* rowItems = nullptr;
        auto row = holderFactory.CreateDirectArrayHolder(
            queryCtx.GetResultCellIndices().size(), rowItems);

        for(size_t i = 0; i < queryCtx.GetResultCellIndices().size(); ++i) {
            const auto& [cellIndex, cellType] = queryCtx.GetResultCellIndices()[i];
            if (cellIndex == RELEVANCE_COLUMN_MARKER) {
                double score = GetBM25Score(queryCtx);
                rowItems[i] = NUdf::TUnboxedValuePod(score);
                computeBytes += 8;
                continue;
            }

            if (cellIndex == 0) {
                rowItems[i] = NMiniKQL::GetCellValue(TCell::Make(DocumentNumId), cellType);
                computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], cellType).AllocatedBytes;
                continue;
            }

            rowItems[i] = NMiniKQL::GetCellValue(RowCells[cellIndex], cellType);
            computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], cellType).AllocatedBytes;
        }

        return row;
    }

    TTableRange GetPoint() const {
        return TTableRange(KeyCells);
    }
};

class TL1DocumentInfo : public TSimpleRefCount<TL1DocumentInfo> {
public:
    using TPtr = TIntrusivePtr<TL1DocumentInfo>;
    TDocumentInfo::TPtr Document;
    TString Word;
    TOwnedCellVec IndexKey;

    TL1DocumentInfo(TDocumentInfo::TPtr& document, TString word)
        : Document(document)
        , Word(word)
    {
        TCell tokenCell(Word.data(), Word.size());
        TVector<TCell> point = TVector<TCell>{tokenCell, TCell::Make<ui64>(document->DocumentNumId)};
        IndexKey = TOwnedCellVec(point);
    }

    TTableRange GetPoint() const {
        return TTableRange(IndexKey);
    }
};


class TDocId {
public:
    size_t WordIndex;
    ui64 DocId = 0;

    struct TCompare {
        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TCompare(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        bool operator()(const TDocId& key1, const TDocId& key2) const {
            return key1.DocId > key2.DocId;
        }
    };

    struct TEquals {
        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TEquals(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        bool operator()(const TDocId& key1, const TDocId& key2) const {
            return key1.DocId == key2.DocId;
        }
    };

    explicit TDocId(size_t wordIndex, ui64 docId)
        : WordIndex(wordIndex)
        , DocId(docId)
    {}
};

class TIndexTableImplReader : public TTableReader<TIndexTableImplReader> {
    i32 FrequencyColumnIndex = 0;

public:
    TIndexTableImplReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        i32 frequencyColumnIndex)
        : TTableReader(counters, tableId, tablePath,  snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , FrequencyColumnIndex(frequencyColumnIndex)
    {}

    static TIntrusivePtr<TIndexTableImplReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings,
        bool withRelevance)
    {
        YQL_ENSURE(settings->GetIndexTables().size() >= 1);
        auto& info = settings->GetIndexTables(settings->GetIndexTables().size() - 1);
        YQL_ENSURE(info.GetTable().GetPath().EndsWith(NTableIndex::ImplTable));
        auto& columns = info.GetColumns();
        auto& keyColumns = info.GetKeyColumns();

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultColumnTypes;
        TVector<i32> resultColumnIds;

        for (const auto& keyColumn : keyColumns) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                keyColumn.GetTypeId(), keyColumn.GetTypeInfo()));

            if (keyColumn.GetName() == TokenColumn) {
                // dont request token column because it's not a part of document id
                continue;
            }

            resultColumnTypes.push_back(keyColumnTypes.back());
            resultColumnIds.push_back(keyColumn.GetId());
        }

        bool useArrowFormat = false;

        // arrow format is only supported when key is a single Uint64 column
        // doc_id (Uint64), [freq (Uint32)]
        if (resultColumnTypes.size() == 1 && resultColumnTypes[0].GetTypeId() == NScheme::NTypeIds::Uint64) {
            useArrowFormat = true;
        }

        YQL_ENSURE(useArrowFormat);

        i32 freqColumnIndex = -1;
        if (withRelevance) {
            NScheme::TTypeInfo freqColumnType;
            for (const auto& column: columns) {
                if (column.GetName() == FreqColumn) {
                    freqColumnIndex = column.GetId();
                    freqColumnType = NScheme::TypeInfoFromProto(
                        column.GetTypeId(), column.GetTypeInfo());
                }
            }
            YQL_ENSURE(freqColumnIndex != -1);
            resultColumnTypes.push_back(freqColumnType);
            resultColumnIds.push_back(freqColumnIndex);
            freqColumnIndex = resultColumnTypes.size()-1;
        }

        TIntrusivePtr<TIndexTableImplReader> reader = MakeIntrusive<TIndexTableImplReader>(
            counters, FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix,
            keyColumnTypes, resultColumnTypes, resultColumnIds, freqColumnIndex);
        reader->SetUseArrowFormat(useArrowFormat);
        return reader;
    }

    ui32 GetFrequencyColumnIndex() const {
        return FrequencyColumnIndex;
    }
};

class ITokenStream {
protected:
    std::deque<std::unique_ptr<TEvDataShard::TEvReadResult>> PendingReadResults;
    bool ReadFinished = false;
    i64 UnprocessedDocumentPos = 0;
    ui64 UnprocessedDocumentCount = 0;
    ui64 TokenIndex = 0;
    ui64 Bytes = 0;
    ui64 Rows = 0;

public:
    virtual ~ITokenStream() = default;

    ITokenStream(ui64 tokenIndex)
        : TokenIndex(tokenIndex)
    {}

    void SetReadFinished() {
        ReadFinished = true;
    }

    std::pair<ui64, ui64> GetStats() const {
        return {Rows, Bytes};
    }

    virtual void AddResult(std::unique_ptr<TEvDataShard::TEvReadResult> result) {
        if (result->GetRowsCount() == 0) {
            return;
        }

        UnprocessedDocumentCount += result->GetRowsCount();
        PendingReadResults.push_back(std::move(result));
    }

    virtual bool MoveToNext() {
        YQL_ENSURE(!PendingReadResults.empty());
        UnprocessedDocumentPos++;
        UnprocessedDocumentCount--;
        if (UnprocessedDocumentPos == static_cast<i64>(PendingReadResults.front()->GetRowsCount())) {
            PendingReadResults.pop_front();
            UnprocessedDocumentPos = 0;
        }

        return UnprocessedDocumentCount > 0;
    }

    ui32 GetUnprocessedDocumentCount() const {
        return UnprocessedDocumentCount;
    }

    virtual ui32 GetLeastDocFrequency() const = 0;
    virtual ui64 GetLeastDocId() = 0;

    bool IsEof() const {
        return UnprocessedDocumentCount == 0 && ReadFinished;
    }
};

class TArrowTokenStream : public ITokenStream {
    std::deque<std::shared_ptr<arrow::UInt64Array>> PendingDocumentIds;
    std::deque<std::shared_ptr<arrow::UInt32Array>> PendingDocumentFrequencies;
public:
    TArrowTokenStream(ui64 tokenIndex)
        : ITokenStream(tokenIndex)
    {
    }

    void AddResult(std::unique_ptr<TEvDataShard::TEvReadResult> result) override {
        if (result->GetRowsCount() == 0) {
            return;
        }

        YQL_ENSURE(result->Record.GetResultFormat() == NKikimrDataEvents::EDataFormat::FORMAT_ARROW);
        auto batch = result->GetArrowBatch();
        YQL_ENSURE(batch && batch->num_columns() >= 1);
        auto docIds = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        YQL_ENSURE(docIds);
        YQL_ENSURE(docIds->length() == static_cast<int64_t>(result->GetRowsCount()));
        Rows += docIds->length();
        Bytes += docIds->length() * sizeof(ui64);
        if (batch->num_columns() > 1) {
            auto array = batch->column(1);
            auto freq_array = std::static_pointer_cast<arrow::UInt32Array>(array);
            YQL_ENSURE(freq_array);
            YQL_ENSURE(freq_array->length() == docIds->length());
            Bytes += freq_array->length() * sizeof(ui32);
            PendingDocumentFrequencies.emplace_back(std::move(freq_array));
        }
        PendingDocumentIds.emplace_back(std::move(docIds));
        ITokenStream::AddResult(std::move(result));
    }

    bool MoveToNext() override {
        YQL_ENSURE(!PendingDocumentIds.empty());
        if (UnprocessedDocumentPos + 1 == static_cast<i64>(PendingDocumentIds.front()->length())) {
            if (!PendingDocumentFrequencies.empty()) {
                PendingDocumentFrequencies.pop_front();
            }

            PendingDocumentIds.pop_front();
        }

        return ITokenStream::MoveToNext();
    }

    ui32 GetLeastDocFrequency() const override {
        YQL_ENSURE(!PendingDocumentFrequencies.empty());
        return PendingDocumentFrequencies.front()->Value(UnprocessedDocumentPos);
    }

    ui64 GetLeastDocId() override {
        YQL_ENSURE(!PendingDocumentIds.empty());
        return PendingDocumentIds.front()->Value(UnprocessedDocumentPos);
    }
};



class TWordReadState : public TSimpleRefCount<TWordReadState> {
public:
    ui64 WordIndex;
    TString Word;
    bool PendingRead = false;
    TIntrusivePtr<TIndexTableImplReader> Reader;
    std::deque<std::pair<ui64, TOwnedTableRange>> RangesToRead;
    ui32 Frequency = 0;

    TCell TokenCell;
    TOwnedTableRange WordKeyCells;
    bool L1 = true;
    bool L2 = false;
    ui32 L2StreamIndex = 0;

    using TPtr = TIntrusivePtr<TWordReadState>;

    explicit TWordReadState(ui64 wordIndex, const TString& word, const TIntrusivePtr<TIndexTableImplReader>& reader)
        : WordIndex(wordIndex)
        , Word(word)
        , Reader(reader)
        , TokenCell(Word.data(), Word.size())
        , WordKeyCells(TOwnedTableRange(TVector<TCell>{TokenCell}))
    {
        BuildRangesToRead();
    }

    TOwnedTableRange GetWordKeyCells() const {
        return WordKeyCells;
    }

    TTableRange GetPoint() const {
        return TTableRange(WordKeyCells);
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> BuildNextRangeToRead(ui64 readId) {
        YQL_ENSURE(!RangesToRead.empty());
        auto [shardId, range] = RangesToRead.front();
        RangesToRead.pop_front();
        return std::make_pair(shardId, Reader->GetReadRequest(readId, range));
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> ScheduleNextRead(ui64 readId) {
        if (!RangesToRead.empty()) {
            return BuildNextRangeToRead(readId);
        }

        return std::make_pair(0, nullptr);
    }

    void BuildRangesToRead() {
        TCell tokenCell(Word.data(), Word.size());
        std::vector <TCell> fromCells(Reader->GetKeyColumnTypes().size() - 1);
        fromCells.insert(fromCells.begin(), tokenCell);

        std::vector <TCell> toCells = {tokenCell};
        auto range = TTableRange(fromCells, true, toCells, false);

        auto rangePartition = Reader->GetRangePartitioning(range);
        for(const auto& [shardId, range] : rangePartition) {
            RangesToRead.emplace_back(shardId, range);
        }
    }
};

class IMergeAlgorithm {
protected:
    std::vector<std::unique_ptr<ITokenStream>> Streams;
    ui64 TokenCount;
    ui64 MinShouldMatch;
    TDocId::TEquals DocIdEquals;
    TDocId::TCompare DocIdCompare;
    const bool WithFrequencies;
    ui64 FinishedTokens = 0;
    std::vector<ui32> MatchedTokens;

public:

    IMergeAlgorithm(std::vector<std::unique_ptr<ITokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : Streams(std::move(streams))
        , TokenCount(Streams.size())
        , MinShouldMatch(minShouldMatch)
        , DocIdEquals(TDocId::TEquals(keyColumnTypes))
        , DocIdCompare(TDocId::TCompare(keyColumnTypes))
        , WithFrequencies(withFrequencies)
    {
    }

    virtual void AddResult(ui64 tokenIndex, std::unique_ptr<TEvDataShard::TEvReadResult> msg) = 0;

    void FinishTokenStream(ui64 tokenIndex) {
        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        stream->SetReadFinished();
        if (stream->IsEof()) {
            FinishedTokens++;
        }
    }

    bool Done() const {
        return FinishedTokens == Streams.size();
    }

    std::pair<ui64, ui64> GetStats() const {
        std::pair<ui64, ui64> total = {0, 0};
        for(const auto& stream: Streams) {
            auto stats = stream->GetStats();
            total.first += stats.first;
            total.second += stats.second;
        }

        return total;
    }

    virtual std::vector<TDocumentInfo::TPtr> FindMatches() = 0;
    virtual ~IMergeAlgorithm() = default;
};


class TAndOptimizedMergeAlgorithm : public IMergeAlgorithm {
    std::deque<ui32> ReadyStreams;

public:
    TAndOptimizedMergeAlgorithm(std::vector<std::unique_ptr<ITokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : IMergeAlgorithm(std::move(streams), minShouldMatch, withFrequencies, keyColumnTypes)
    {
        YQL_ENSURE(Streams.size() == TokenCount, "Misuse of TAndOptimizedMatchAlgo: minShouldMatch must be equal to tokenCount");
    }

    void AddResult(ui64 tokenIndex, std::unique_ptr<TEvDataShard::TEvReadResult> msg) override {
        if (msg->GetRowsCount() == 0) {
            return;
        }

        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        bool wasEmpty = stream->GetUnprocessedDocumentCount() == 0;
        stream->AddResult(std::move(msg));
        if (wasEmpty) {
            ReadyStreams.push_back(tokenIndex);
        }
    }

    void AdvanceStreams() {
        for(ui32 tokenIndex : MatchedTokens) {
            YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
            auto& stream = Streams[tokenIndex];
            if (bool hasMore = stream->MoveToNext(); hasMore) {
                ReadyStreams.push_back(tokenIndex);
            }

            if (stream->IsEof()) {
                FinishedTokens++;
            }
        }
        MatchedTokens.clear();
    }

    int JumpToBest(ui64& candidate) {
        ui32 tokenIndex = ReadyStreams.front();
        ReadyStreams.pop_front();

        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        do {
            ui64 leastDocId = stream->GetLeastDocId();
            if (leastDocId == candidate) {
                MatchedTokens.push_back(tokenIndex);
                return 0;
            } else if (leastDocId > candidate) {
                AdvanceStreams();
                MatchedTokens.push_back(tokenIndex);
                candidate = std::move(leastDocId);
                return 1;
            }

            if (bool hasMore = stream->MoveToNext(); !hasMore) {
                if (stream->IsEof()) {
                    FinishedTokens++;
                }
                return -1;
            }

        } while (true);
    }

    std::vector<TDocumentInfo::TPtr> FindMatches() override {
        if (FinishedTokens > 0)
            return std::vector<TDocumentInfo::TPtr>();

        std::vector<TDocumentInfo::TPtr> matches;
        while (MatchedTokens.size() + ReadyStreams.size() == TokenCount) {
            YQL_ENSURE(ReadyStreams.size() > 0, "ReadyStreams must be non-empty");
            if (MatchedTokens.empty()) {
                MatchedTokens.push_back(ReadyStreams.front());
                ReadyStreams.pop_front();
            }

            YQL_ENSURE(MatchedTokens.back() < Streams.size(), "Matched token index out of bounds");
            ui64 candidate = Streams[MatchedTokens.back()]->GetLeastDocId();

            while (!ReadyStreams.empty()) {
                int compareSign = JumpToBest(candidate);
                if (compareSign < 0) {
                    break;
                } else if (compareSign >= 0) {
                    continue;
                }
            }

            if (MatchedTokens.size() < TokenCount || FinishedTokens > 0) {
                break;
            }

            auto match = MakeIntrusive<TDocumentInfo>(candidate);
            if (WithFrequencies) {
                match->TokenFrequencies.resize(TokenCount, 0);
                for (ui32 tokenIndex : MatchedTokens) {
                    YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                    match->TokenFrequencies[tokenIndex] = Streams[tokenIndex]->GetLeastDocFrequency();
                }
            }

            AdvanceStreams();

            matches.push_back(std::move(match));
        }

        return matches;
    }
};

class TDefaultMergeAlgorithm : public IMergeAlgorithm {
    std::priority_queue<TDocId, TStackVec<TDocId, 64>, TDocId::TCompare> MergeQueue;

public:
    TDefaultMergeAlgorithm(std::vector<std::unique_ptr<ITokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : IMergeAlgorithm(std::move(streams), minShouldMatch, withFrequencies, keyColumnTypes)
        , MergeQueue(DocIdCompare)
    {
    }

    void AddResult(ui64 tokenIndex, std::unique_ptr<TEvDataShard::TEvReadResult> msg) override {
        if (msg->GetRowsCount() == 0) {
            return;
        }

        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        bool wasEmpty = stream->GetUnprocessedDocumentCount() == 0;
        stream->AddResult(std::move(msg));
        if (wasEmpty) {
            MergeQueue.push(TDocId(tokenIndex, stream->GetLeastDocId()));
        }
    }

    std::vector<TDocumentInfo::TPtr> FindMatches() override {
        std::vector<TDocumentInfo::TPtr> matches;
        std::vector<size_t> matchedTokens;
        while(!MergeQueue.empty() && MergeQueue.size() + FinishedTokens == TokenCount) {
            if (MergeQueue.size() < MinShouldMatch) {
                break;
            }

            TDocId doc = std::move(MergeQueue.top());
            matchedTokens.clear();
            matchedTokens.push_back(doc.WordIndex);

            MergeQueue.pop();
            while(!MergeQueue.empty() && DocIdEquals(doc, MergeQueue.top())) {
                matchedTokens.push_back(MergeQueue.top().WordIndex);
                MergeQueue.pop();
            }

            if (matchedTokens.size() >= MinShouldMatch) {
                auto match = MakeIntrusive<TDocumentInfo>(doc.DocId);
                if (WithFrequencies) {
                    match->TokenFrequencies.resize(TokenCount, 0);
                    for (ui32 tokenIndex : matchedTokens) {
                        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                        match->TokenFrequencies[tokenIndex] = Streams[tokenIndex]->GetLeastDocFrequency();
                    }
                }
                matches.push_back(std::move(match));
            }

            for (ui32 tokenIndex : matchedTokens) {
                YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                auto& token = Streams[tokenIndex];
                if (bool hasMore = token->MoveToNext(); hasMore) {
                    MergeQueue.push(TDocId(tokenIndex, token->GetLeastDocId()));
                }

                if (token->IsEof()) {
                    FinishedTokens++;
                }
            }
        }

        return matches;
    }
};

class TDocsTableReader : public TTableReader<TDocsTableReader> {
    NKqpProto::EKqpFullTextIndexType IndexType;
public:
    TDocsTableReader(const NKqpProto::EKqpFullTextIndexType& indexType,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , IndexType(indexType)
    {}

    static TIntrusivePtr<TDocsTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings,
        bool withRelevance)
    {
        if (!withRelevance) {
            return nullptr;
        }

        YQL_ENSURE(settings->GetIndexTables().size() >= 2);
        auto& info = settings->GetIndexTables(1);
        YQL_ENSURE(info.GetTable().GetPath().EndsWith(DocsTable));
        auto& columns = info.GetColumns();
        auto& keyColumns = info.GetKeyColumns();

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultKeyColumnTypes;
        TVector<i32> resultKeyColumnIds;

        i32 docLengthColumnIndex = -1;
        NScheme::TTypeInfo docLengthColumnType;
        for (const auto& column : columns) {
            if (column.GetName() == DocLengthColumn) {
                docLengthColumnIndex = column.GetId();
                docLengthColumnType = NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo());
            }
        }

        for (const auto& column : keyColumns) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            resultKeyColumnTypes.push_back(keyColumnTypes.back());
            resultKeyColumnIds.push_back(column.GetId());
        }

        bool useArrowFormat = false;

        // arrow format is only supported when key is a single Uint64 column
        // doc_id (Uint64), [freq (Uint32)]
        if (resultKeyColumnTypes.size() == 1 && resultKeyColumnTypes[0].GetTypeId() == NScheme::NTypeIds::Uint64) {
            useArrowFormat = true;
        }

        YQL_ENSURE(useArrowFormat);

        YQL_ENSURE(docLengthColumnIndex != -1);
        resultKeyColumnTypes.push_back(docLengthColumnType);
        resultKeyColumnIds.push_back(docLengthColumnIndex);

        auto reader = MakeIntrusive<TDocsTableReader>(
            settings->GetIndexType(), counters,
            FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
        reader->SetUseArrowFormat(useArrowFormat);
        return reader;
    }

    ui64 GetDocumentLength(const TConstArrayRef<TCell>& row) const {
        switch (IndexType) {
            case NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui32>();
            default:
                return 0;
        }
    }
};

class TStatsTableReader : public TTableReader<TStatsTableReader> {
    NKqpProto::EKqpFullTextIndexType IndexType;
public:
    TStatsTableReader(const NKqpProto::EKqpFullTextIndexType& indexType,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , IndexType(indexType)
    {}

    static TIntrusivePtr<TStatsTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings,
        bool withRelevance)
    {
        if (!withRelevance) {
            return nullptr;
        }

        YQL_ENSURE(settings->GetIndexTables().size() >= 3);
        auto& info = settings->GetIndexTables(2);
        YQL_ENSURE(info.GetTable().GetPath().EndsWith(StatsTable));
        auto& columns = info.GetColumns();
        auto& keyColumns = info.GetKeyColumns();

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultKeyColumnTypes;
        TVector<i32> resultKeyColumnIds;

        i32 statsColumnIndex = -1;
        NScheme::TTypeInfo statsColumnType;

        i32 sumDocLengthColumnIndex = -1;
        NScheme::TTypeInfo sumDocLengthColumnType;

        for (const auto& column : columns) {

            if (column.GetName() == DocCountColumn) {
                statsColumnIndex = column.GetId();
                statsColumnType = NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo());
            }

            if (column.GetName() == SumDocLengthColumn) {
                sumDocLengthColumnIndex = column.GetId();
                sumDocLengthColumnType = NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo());
            }
        }

        for (const auto& column : keyColumns) {
            NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo());
            keyColumnTypes.push_back(typeInfo);
        }

        YQL_ENSURE(statsColumnIndex != -1);
        resultKeyColumnTypes.push_back(statsColumnType);
        resultKeyColumnIds.push_back(statsColumnIndex);

        YQL_ENSURE(sumDocLengthColumnIndex != -1);
        resultKeyColumnTypes.push_back(sumDocLengthColumnType);
        resultKeyColumnIds.push_back(sumDocLengthColumnIndex);

        return MakeIntrusive<TStatsTableReader>(
            settings->GetIndexType(), counters,
            FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    ui64 GetDocCount(const TConstArrayRef<TCell>& row) const {
        switch (IndexType) {
            case NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance:
                return row[0].AsValue<ui64>();
            default:
                return 0;
        }
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> GetTotalStatsRequest(ui64 readId) {
        TCell tokenCell = TCell::Make<ui32>(0);
        std::vector <TCell> fromCells;
        fromCells.insert(fromCells.begin(), tokenCell);

        TCell maxCell = TCell::Make<ui32>(std::numeric_limits<ui32>::max());
        std::vector <TCell> toCells;
        toCells.insert(toCells.begin(), maxCell);

        bool fromInclusive = true;
        bool toInclusive = false;
        auto tcellVector = TTableRange(fromCells, fromInclusive, toCells, toInclusive);

        auto partitioning = GetRangePartitioning(tcellVector);
        YQL_ENSURE(partitioning.size() == 1);
        auto [shardId, range] = partitioning[0];
        return std::make_pair(shardId, GetReadRequest(readId, range));
    }

    ui64 GetSumDocLength(const TConstArrayRef<TCell>& row) const {
        switch (IndexType) {
            case NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance:
                return row[1].AsValue<ui64>();
            default:
                return 0;
        }
    }
};

class TDictTableReader : public TTableReader<TDictTableReader> {
public:
    TDictTableReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
    {}

    static TIntrusivePtr<TDictTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        if (settings->GetIndexType() != NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance) {
            return nullptr;
        }

        YQL_ENSURE(settings->GetIndexTables().size() >= 1);
        auto& info = settings->GetIndexTables(0);
        YQL_ENSURE(info.GetTable().GetPath().EndsWith(DictTable));
        auto& columns = info.GetColumns();
        auto& keyColumns = info.GetKeyColumns();

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultKeyColumnTypes;
        TVector<i32> resultKeyColumnIds;

        i32 freqColumnIndex = -1;
        NScheme::TTypeInfo freqColumnType;
        for (const auto& column : columns) {
            if (column.GetName() == FreqColumn) {
                freqColumnIndex = column.GetId();
                freqColumnType = NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo());
            }
        }

        for (const auto& keyColumn : keyColumns) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                keyColumn.GetTypeId(), keyColumn.GetTypeInfo()));
            resultKeyColumnTypes.push_back(keyColumnTypes.back());
            resultKeyColumnIds.push_back(keyColumn.GetId());
        }

        YQL_ENSURE(freqColumnIndex != -1);
        resultKeyColumnTypes.push_back(freqColumnType);
        resultKeyColumnIds.push_back(freqColumnIndex);

        return MakeIntrusive<TDictTableReader>(counters, FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix,
            keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    TStringBuf GetWord(const TConstArrayRef<TCell>& row) const {
        return row[0].AsBuf();
    }

    ui64 GetWordFrequency(const TConstArrayRef<TCell>& row) const {
        return row[GetResultColumnTypes().size() - 1].AsValue<ui64>();
    }
};

enum EReadKind : ui32 {
    EReadKind_Word = 0,
    EReadKind_WordStats = 1,
    EReadKind_DocumentStats = 2,
    EReadKind_Document = 3,
    EReadKind_TotalStats = 4,
    EReadKind_Word_L2 = 5,
};

struct TReadInfo {
    ui64 ReadKind;
    ui64 Cookie;
    ui64 ShardId;
    ui64 LastSeqNo = 0;
};

class TMainTableReader : public TTableReader<TMainTableReader> {
    bool WithRelevance;
public:
    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    bool MainTableCovered;
    i32 SearchColumnIdx;

    TMainTableReader(
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        const TVector<std::pair<i32, NScheme::TTypeInfo>>& resultCellIndices,
        bool mainTableCovered,
        bool withRelevance,
        i32 searchColumnIdx)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , WithRelevance(withRelevance)
        , ResultCellIndices(resultCellIndices)
        , MainTableCovered(mainTableCovered)
        , SearchColumnIdx(searchColumnIdx)
    {}

    static TIntrusivePtr<TMainTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultColumnTypes;
        TVector<i32> resultColumnIds;
        i32 searchColumnIdx = -1;
        bool withRelevance = false;

        YQL_ENSURE(settings->GetQuerySettings().ColumnsSize() == 1);
        const TStringBuf searchColumnName = settings->GetQuerySettings().GetColumns(0).GetName();

        THashMap<TString, std::pair<i32, NScheme::TTypeInfo>> keyColumns;
        for (const auto& column : settings->GetKeyColumns()) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            if (column.GetName() == searchColumnName) {
                searchColumnIdx = resultColumnIds.size();
            }

            resultColumnTypes.push_back(keyColumnTypes.back());
            keyColumns.insert({column.GetName(), {resultColumnIds.size(), keyColumnTypes.back()}});
            resultColumnIds.push_back(column.GetId());
        }

        TVector<std::pair<i32, NScheme::TTypeInfo>> resultCellIndices;
        resultCellIndices.reserve(settings->GetColumns().size());
        bool mainTableCovered = true;
        for(i32 i = 0; i < settings->GetColumns().size(); i++) {
            const auto& column = settings->GetColumns(i);
            if (column.GetName() == FullTextRelevanceColumn) {
                resultCellIndices.emplace_back(RELEVANCE_COLUMN_MARKER, NScheme::TTypeInfo());
                withRelevance = true;
                continue;
            }

            if (keyColumns.contains(column.GetName())) {
                resultCellIndices.push_back(keyColumns[column.GetName()]);
                continue;
            }

            mainTableCovered = false;
            if (column.GetName() == searchColumnName) {
                searchColumnIdx = resultColumnIds.size();
            }

            resultColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            resultCellIndices.emplace_back(resultColumnIds.size(), resultColumnTypes.back());
            resultColumnIds.push_back(column.GetId());
        }

        if (searchColumnIdx == -1) {
            for(const auto& column: settings->GetQuerySettings().GetColumns()) {
                bool needPostfilter = false;
                if (column.GetName() == searchColumnName) {
                    for(const auto& analyzer : settings->GetIndexDescription().GetSettings().columns()) {
                        if (analyzer.column() == column.GetName()) {
                            if (analyzer.analyzers().use_filter_ngram() || analyzer.analyzers().use_filter_edge_ngram()) {
                                needPostfilter = true;
                                break;
                            }
                        }
                    }
                }

                if (needPostfilter) {
                    mainTableCovered = false;
                    resultColumnTypes.push_back(
                        NScheme::TypeInfoFromProto(column.GetTypeId(), column.GetTypeInfo()));
                    searchColumnIdx = resultColumnIds.size();
                    resultColumnIds.push_back(column.GetId());
                }
            }
        }

        withRelevance = withRelevance && settings->GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance;

        return MakeIntrusive<TMainTableReader>(counters, FromProto(settings->GetTable()), settings->GetTable().GetPath(),
            snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds, resultCellIndices,
            mainTableCovered, withRelevance, searchColumnIdx);
    }

    bool GetWithRelevance() {
        return WithRelevance;
    }
};



class TReadsState {
    struct TShardInfo {
        absl::flat_hash_set<ui64> ReadIds;
        ui64 Retries = 0;
        bool IsScheduledRetry = false;
    };

    absl::flat_hash_map<ui64, TReadInfo> Reads;
    absl::flat_hash_map<ui64, TShardInfo> ReadsByShardId;
    absl::flat_hash_set<ui64> PipesCreated;
    const TIntrusivePtr<TKqpCounters> Counters;
    TActorId SelfId;
    const TString LogPrefix;
    ui64 NextReadId = 1;


    void AddRead(ui64 readId, TReadInfo readInfo) {
        Reads[readId] = readInfo;
        ReadsByShardId[readInfo.ShardId].ReadIds.insert(readId);
    }

public:

    explicit TReadsState(const TIntrusivePtr<TKqpCounters>& counters, const TString& logPrefix)
        : Counters(counters)
        , LogPrefix(logPrefix)
    {}

    ui64 GetNextReadId() {
        return NextReadId++;
    }

    void SetSelfId(const TActorId& selfId) {
        SelfId = selfId;
    }

    const absl::flat_hash_map<ui64, TReadInfo>& GetReads() const {
        return Reads;
    }

    void AckRead(ui64 readId) {
        auto readIt = FindPtr(readId);
        if (readIt == nullptr) {
            return;
        }
        auto& readInfo = *readIt;
        ui64 shardId = readInfo.ShardId;
        auto request = GetDefaultReadAckSettings();
        request->Record.SetReadId(readId);
        request->Record.SetSeqNo(readInfo.LastSeqNo);
        Counters->SentIteratorAcks->Inc();

        CA_LOG_D("Sending ack for read #" << readId << " seqno = " << readInfo.LastSeqNo);

        bool newPipe = PipesCreated.insert(shardId).second;
        TlsActivationContext->Send(new NActors::IEventHandle(
            NKikimr::MakePipePerNodeCacheID(false),
            SelfId,
            new TEvPipeCache::TEvForward(request.Release(), shardId, TEvPipeCache::TEvForwardOptions{
                .AutoConnect = newPipe,
                .Subscribe = newPipe}),
            IEventHandle::FlagTrackDelivery));
    }

    void SendEvRead(ui64 shardId, std::unique_ptr<TEvDataShard::TEvRead>& request, TReadInfo readInfo) {
        auto& record = request->Record;
        auto readId = request->Record.GetReadId();
        const bool needToCreatePipe = PipesCreated.insert(shardId).second;

        CA_LOG_D(TStringBuilder() << "Send EvRead (full text source) to shardId=" << shardId
            << ", readId = " << record.GetReadId()
            << ", snapshot=(txid=" << record.GetSnapshot().GetTxId() << ", step=" << record.GetSnapshot().GetStep() << ")"
            << ", lockTxId=" << record.GetLockTxId()
            << ", lockNodeId=" << record.GetLockNodeId());

        TlsActivationContext->Send(new NActors::IEventHandle(
            NKikimr::MakePipePerNodeCacheID(false),
            SelfId,
            new TEvPipeCache::TEvForward(
                request.release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery,
            readId));

        AddRead(readId, readInfo);
    }

    TDuration GetDelay(ui64 shardId, bool allowInstantRetry) {
        auto it = ReadsByShardId.find(shardId);
        if (it == ReadsByShardId.end()) {
            return TDuration::Zero();
        }

        auto& shardInfo = it->second;
        ++shardInfo.Retries;
        return CalcDelay(shardInfo.Retries, allowInstantRetry);
    }

    bool CheckShardRetriesExeeded(ui64 shardId, ui64 maxRetries) {
        auto it = ReadsByShardId.find(shardId);
        if (it == ReadsByShardId.end()) {
            return false;
        }
        return it->second.Retries > maxRetries;
    }

    bool Empty() const {
        return Reads.empty();
    }

    TReadInfo* FindPtr(ui64 readId) {
        auto it = Reads.find(readId);
        if (it == Reads.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void UntrackPipe(ui64 shardId) {
        PipesCreated.erase(shardId);
    }

    TShardInfo* FindShardPtr(ui64 shardId) {
        auto it = ReadsByShardId.find(shardId);
        if (it == ReadsByShardId.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void RemoveRead(ui64 readId) {
        auto it = Reads.find(readId);
        if (it != Reads.end()) {
            ui64 shardId = it->second.ShardId;
            Reads.erase(it);

            auto& shardReads = ReadsByShardId[shardId];
            shardReads.ReadIds.erase(readId);
            if (shardReads.ReadIds.empty()) {
                ReadsByShardId.erase(shardId);
            }
        }
    }
};

template <typename TItem>
class TReadItemsQueue {
    struct TSentReadItems {
        std::deque<TItem> Items;
        std::deque<TOwnedTableRange> Points;
        ui64 ShardId = 0;
        ui64 ReadId = 0;
        bool Finished = false;

        TItem& GetItem() {
            YQL_ENSURE(!Items.empty());
            return Items.front();
        }

        void PopItem() {
            Items.pop_front();
            Points.pop_front();
        }

        bool Empty() const {
            return Items.empty() && Points.empty();
        }
    };

    void Enqueue(ui64 readId, TSentReadItems&& items) {
        auto [it, inserted] = Queue.emplace(readId, std::move(items));
        YQL_ENSURE(inserted);
        YQL_ENSURE(it->second.ReadId == readId);
    }

    struct TPendingSequentialRead {
        std::deque<TItem> Items;
        std::deque<TOwnedTableRange> Points;
        ui64 ShardId = 0;
    };

public:
    absl::flat_hash_map<ui64, TSentReadItems> Queue;
    const TActorId SelfId;
    TReadsState& ReadsState;
    absl::flat_hash_map<ui64, ui64> InflightSequentialReadId; // cookie -> current in-flight readId
    absl::flat_hash_map<ui64, std::deque<TPendingSequentialRead>> PendingSequential;

    explicit TReadItemsQueue(const TActorId& selfId, TReadsState& readsState)
        : SelfId(selfId)
        , ReadsState(readsState)
    {}

    void ClearReadItems(TSentReadItems& items) {
        items.Items.clear();
        items.Points.clear();
    }

    void UpdateReadStatus(ui64 readId, TSentReadItems& items, bool finished) {
        YQL_ENSURE(items.ReadId == readId);
        if (finished) {
            YQL_ENSURE(items.Empty(), "Read items are not empty while read is finished");
        }

        if (finished) {
            Queue.erase(readId);
            ReadsState.RemoveRead(readId);
        }
    }

    template <typename TReader>
    void SendSequentialGroup(TReader* reader, EReadKind readKind, ui64 cookie, TPendingSequentialRead&& group) {
        ui64 readId = ReadsState.GetNextReadId();
        auto evRead = reader->GetReadRequest(readId, group.Points);
        YQL_ENSURE(evRead);

        InflightSequentialReadId[cookie] = readId;
        TSentReadItems sentItems = TSentReadItems{
            .Items = std::move(group.Items), .Points = std::move(group.Points),
            .ShardId = group.ShardId, .ReadId = readId};
        ReadsState.SendEvRead(sentItems.ShardId, evRead, TReadInfo{.ReadKind = readKind, .Cookie = cookie, .ShardId = sentItems.ShardId});
        Enqueue(readId, std::move(sentItems));
    }

    template <typename TReader, typename TCollection>
    void Sequential(TReader* reader, EReadKind readKind, const TCollection& infos, ui64 cookie) {
        std::deque<TPendingSequentialRead> shardGroups;

        for (auto& info : infos) {
            auto ranges = reader->GetRangePartitioning(info->GetPoint());
            YQL_ENSURE(ranges.size() == 1);
            ui64 shardId = ranges[0].first;

            if (shardGroups.empty() || shardGroups.back().ShardId != shardId) {
                shardGroups.emplace_back();
                shardGroups.back().ShardId = shardId;
            }

            shardGroups.back().Points.emplace_back(ranges[0].second);
            shardGroups.back().Items.emplace_back(info);
        }

        if (shardGroups.empty()) return;

        bool hasInflightReads = InflightSequentialReadId.contains(cookie)
            || HasPendingSequentialReads(cookie);

        size_t startIdx = 0;
        if (!hasInflightReads) {
            // No in-flight reads for this cookie, send the first group immediately
            SendSequentialGroup(reader, readKind, cookie, std::move(shardGroups.front()));
            startIdx = 1;
        }

        // Store remaining (or all, if reads are in-flight) groups as pending
        if (startIdx < shardGroups.size()) {
            auto& pendingQueue = PendingSequential[cookie];
            for (size_t i = startIdx; i < shardGroups.size(); ++i) {
                pendingQueue.push_back(std::move(shardGroups[i]));
            }
        }
    }

    template <typename TReader>
    bool SendNextSequentialRead(TReader* reader, EReadKind readKind, ui64 cookie) {
        auto it = PendingSequential.find(cookie);
        if (it == PendingSequential.end() || it->second.empty()) {
            return false;
        }

        auto group = std::move(it->second.front());
        it->second.pop_front();
        if (it->second.empty()) {
            PendingSequential.erase(it);
        }

        SendSequentialGroup(reader, readKind, cookie, std::move(group));
        return true;
    }

    template <typename TReader>
    void RetrySequential(TReader* reader, EReadKind readKind, ui64 readId, ui64 cookie) {
        auto queueIt = Queue.find(readId);
        YQL_ENSURE(queueIt != Queue.end());
        auto& readItems = queueIt->second;

        // Create new read for the same items on the same shard
        ui64 newReadId = ReadsState.GetNextReadId();
        auto evRead = reader->GetReadRequest(newReadId, readItems.Points);
        YQL_ENSURE(evRead);

        // Update in-flight readId for this cookie
        auto cookieIt = InflightSequentialReadId.find(cookie);
        YQL_ENSURE(cookieIt != InflightSequentialReadId.end() && cookieIt->second == readId);
        cookieIt->second = newReadId;

        TSentReadItems newItems;
        newItems.ShardId = readItems.ShardId;
        newItems.ReadId = newReadId;
        newItems.Items = std::move(readItems.Items);
        newItems.Points = std::move(readItems.Points);

        ui64 shardId = newItems.ShardId;
        ReadsState.SendEvRead(shardId, evRead, TReadInfo{.ReadKind = readKind, .Cookie = cookie, .ShardId = shardId});

        // Remove old, add new
        ReadsState.RemoveRead(readId);
        Queue.erase(queueIt);
        Enqueue(newReadId, std::move(newItems));
    }

    bool HasPendingSequentialReads(ui64 cookie) const {
        auto it = PendingSequential.find(cookie);
        return it != PendingSequential.end() && !it->second.empty();
    }

    bool HasInflightOrPendingSequential(ui64 cookie) const {
        return InflightSequentialReadId.contains(cookie) || HasPendingSequentialReads(cookie);
    }

    bool HasPendingSequential() const {
        return !PendingSequential.empty() || !InflightSequentialReadId.empty();
    }

    void ClearInflightSequentialRead(ui64 cookie) {
        InflightSequentialReadId.erase(cookie);
    }

    template <typename TReader, typename TCollection>
    void Enqueue(TReader* reader, EReadKind readKind, const TCollection& infos) {
        absl::flat_hash_map<ui64, TReadItemsQueue<TItem>::TSentReadItems> inflightItems;
        std::deque<ui64> scheduledReads;
        for (auto& info : infos) {
            auto ranges = reader->GetRangePartitioning(info->GetPoint());
            YQL_ENSURE(ranges.size() == 1);
            auto& shardItems = inflightItems[ranges[0].first];
            if (shardItems.ShardId == 0) {
                shardItems.ShardId = ranges[0].first;
            }
            YQL_ENSURE(shardItems.ShardId == ranges[0].first);
            if (shardItems.ReadId == 0) {
                shardItems.ReadId = ReadsState.GetNextReadId();
            }

            shardItems.Points.emplace_back(ranges[0].second);
            shardItems.Items.emplace_back(info);
        }

        for(auto& [shardId, inflightItem] : inflightItems) {
            auto evRead = reader->GetReadRequest(inflightItem.ReadId, inflightItem.Points);
            YQL_ENSURE(evRead);
            ReadsState.SendEvRead(shardId, evRead, TReadInfo{.ReadKind = readKind, .Cookie = inflightItem.ReadId, .ShardId = shardId});
            Enqueue(inflightItem.ReadId, std::move(inflightItem));
        }
    }

    template <typename TReader>
    void Retry(TReader* reader, EReadKind readKind, ui64 readId) {
        auto& readItems = GetReadItems(readId).Items;
        Enqueue(reader, readKind, readItems);

        ReadsState.RemoveRead(readId);
        Queue.erase(readId);
    }

    TSentReadItems& GetReadItems(ui64 readId) {
        auto it = Queue.find(readId);
        YQL_ENSURE(it != Queue.end());
        return it->second;
    }
};

class TFullTextMatchSource : public TActorBootstrapped<TFullTextMatchSource>, public NYql::NDq::IDqComputeActorAsyncInput, public NActors::IActorExceptionHandler {
private:

    struct TEvPrivate {
        enum EEv {
            EvSchemeCacheRequestTimeout,
            EvRetryRead
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
        };

        struct TEvRetryRead : public TEventLocal<TEvRetryRead, EvRetryRead> {
            explicit TEvRetryRead(ui64 shardId)
                : ShardId(shardId) {
            }

            const ui64 ShardId;
        };
    };


    const NKikimrKqp::TKqpFullTextSourceSettings* Settings;
    TIntrusivePtr<NActors::TProtoArenaHolder> Arena;
    const NActors::TActorId ComputeActorId;
    const ui64 InputIndex;
    // const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;

    const NKikimrSchemeOp::TFulltextIndexDescription& IndexDescription;

    bool MainTableCovered = false;

    TString Database;
    TString LogPrefix;
    TDqAsyncStats IngressStats;

    IKqpGateway::TKqpSnapshot Snapshot;

    TActorId SchemeCacheRequestTimeoutTimer;
    TDuration SchemeCacheRequestTimeout;


    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    i64 Limit = -1;

    struct TTopKDocumentInfo {
        double Score;
        TDocumentInfo::TPtr DocumentInfo;

        bool operator<(const TTopKDocumentInfo& other) const {
            return Score > other.Score;
        }
    };

    TIntrusivePtr<TQueryCtx> QueryCtx;

    std::vector<TTopKDocumentInfo> TopKQueue;

    bool ResolveInProgress = true;
    bool PendingNotify = false;

    i32 SearchColumnIdx = -1;
    ui64 ProducedItemsCount = 0;
    std::deque<TDocumentInfo::TPtr> ResultQueue;
    std::deque<TDocumentInfo::TPtr> L1MergedDocuments;
    bool IsNgram = false;

    TActorId PipeCacheId;

    ui64 DocCount = 0;
    ui64 SumDocLength = 0;

    TIntrusivePtr<TMainTableReader> MainTableReader;
    TIntrusivePtr<TIndexTableImplReader> IndexTableReader;
    TIntrusivePtr<TDocsTableReader> DocsTableReader;
    TIntrusivePtr<TDictTableReader> DictTableReader;
    TIntrusivePtr<TStatsTableReader> StatsTableReader;

    TReadsState ReadsState;
    TReadItemsQueue<TDocumentInfo::TPtr> DocsReadingQueue;
    TReadItemsQueue<TL1DocumentInfo::TPtr> L2ReadingQueue;
    TReadItemsQueue<TWordReadState::TPtr> WordsReadingQueue;
    TVector<TWordReadState::TPtr> Words; // Tokenized words from expression

    std::unique_ptr<IMergeAlgorithm> L1MergeAlgo;
    std::unique_ptr<IMergeAlgorithm> L2MergeAlgo;
    // Helper to bind allocator
    TGuard<NMiniKQL::TScopedAlloc> BindAllocator() {
        return TGuard<NMiniKQL::TScopedAlloc>(*Alloc);
    }

    bool ExtractAndTokenizeExpression() {
        YQL_ENSURE(Settings->GetQuerySettings().GetQuery().size() > 0, "Expected non-empty query");

        // Get the first expression (assuming single expression for now)
        const auto& expr = Settings->GetQuerySettings().GetQuery();
        YQL_ENSURE(Settings->GetQuerySettings().GetColumns().size() == 1);

        for(const auto& column : Settings->GetQuerySettings().GetColumns()) {

            for(const auto& analyzer : Settings->GetIndexDescription().GetSettings().columns()) {
                if (analyzer.analyzers().use_filter_ngram() || analyzer.analyzers().use_filter_edge_ngram()) {
                    IsNgram = true;
                }

                if (analyzer.column() == column.GetName()) {
                    size_t wordIndex = 0;
                    for (const TString& query: NFulltext::BuildSearchTerms(expr, analyzer.analyzers())) {
                        YQL_ENSURE(IndexTableReader);
                        Words.emplace_back(MakeIntrusive<TWordReadState>(wordIndex++, query, IndexTableReader));
                    }
                }
            }
        }

        if (Words.empty()) {
            RuntimeError("No search terms were extracted from the query", NYql::NDqProto::StatusIds::BAD_REQUEST);
            return false;
        }

        return true;
    }

    void FetchDocumentDetails(std::vector<TDocumentInfo::TPtr>& docInfos) {
        if (Limit > 0 && ProducedItemsCount + ResultQueue.size() >= static_cast<ui64>(Limit)) {
            return;
        }

        if (docInfos.empty()) {
            NotifyCA();
            return;
        }

        if (MainTableReader->GetWithRelevance()) {
            if (!docInfos[0]->HasDocumentLength()) {
                DocsReadingQueue.Enqueue(DocsTableReader.Get(), EReadKind_DocumentStats, docInfos);
                return;
            }
        }

        if (MainTableCovered) {
            for(auto& doc: docInfos) {
                ResultQueue.emplace_back(std::move(doc));
            }
            NotifyCA();
            return;
        }

        DocsReadingQueue.Enqueue(MainTableReader.Get(), EReadKind_Document, docInfos);
    }

    bool ContinueWordRead(TWordReadState::TPtr word) {
        ui64 readId = ReadsState.GetNextReadId();
        auto [shardId, ev] = word->ScheduleNextRead(readId);
        if (ev) {
            ReadsState.SendEvRead(shardId, ev, TReadInfo{EReadKind_Word, word->WordIndex, shardId});
            return true;
        }

        return false;
    }

    void EnrichWordInfo() {
        WordsReadingQueue.Enqueue(DictTableReader.Get(), EReadKind_WordStats, Words);
    }

    void StartWordReads() {
        bool needL2Layer = false;
        TString explain;
        EDefaultOperator defaultOperator = DefaultOperatorFromString(Settings->GetDefaultOperator(), explain);
        if (!explain.empty()) {
            RuntimeError(explain, NYql::NDqProto::StatusIds::BAD_REQUEST);
            return;
        }

        if (IsNgram || MainTableReader->GetWithRelevance()) {
            // Queries often contain 'imbalanced' ngrams. I.e. some ngrams
            // are really frequent and others aren't, like one with 5.5 million
            // documents and other with 400 documents. In such cases we can
            // only leave the second one because we anyway postfilter documents.
            // The only concern is to not make ourselves postfilter too many
            // documents... So it's just a heuristic which we can control with
            // the NGRAM_IMBALANCE_FACTOR parameter.
            TVector<size_t> byFreq;
            for (size_t i = 0; i < Words.size(); i++) {
                byFreq.push_back(i);
            }
            std::sort(byFreq.begin(), byFreq.end(), [&](const size_t a, const size_t b) {
                return Words[a]->Frequency < Words[b]->Frequency;
            });
            size_t bestTokenLimit = byFreq.size();
            for (size_t i = 1; i < byFreq.size(); i++) {
                if (Words[byFreq[i]]->Frequency > NGRAM_IMBALANCE_FACTOR * Words[byFreq[0]]->Frequency) {
                    bestTokenLimit = i;
                    break;
                }
            }
            if (IsNgram && bestTokenLimit < Words.size()) {
                CA_LOG_I("Selecting " << bestTokenLimit << " balanced ngrams out of " << Words.size()
                    << " (imbalance: " << Words[byFreq[0]]->Frequency << " vs " << Words[byFreq[bestTokenLimit]]->Frequency << ")");
                TVector<TWordReadState::TPtr> newWords;
                for (size_t i = 0; i < bestTokenLimit; i++) {
                    newWords.emplace_back(std::move(Words[byFreq[i]]));
                    newWords[i]->WordIndex = i;
                }
                std::swap(Words, newWords);
            } else if (MainTableReader->GetWithRelevance() && bestTokenLimit < Words.size() && defaultOperator == EDefaultOperator::And) {
                CA_LOG_I("Selecting " << bestTokenLimit << " balanced tokens out of " << Words.size()
                    << " (imbalance: " << Words[byFreq[0]]->Frequency << " vs " << Words[byFreq[bestTokenLimit]]->Frequency << ")");

                needL2Layer = true;
                TVector<TWordReadState::TPtr> newWords;
                for (size_t i = 0; i < Words.size(); i++) {
                    newWords.emplace_back(std::move(Words[byFreq[i]]));
                    newWords[i]->WordIndex = i;
                    if (i >= bestTokenLimit) {
                        newWords.back()->L2 = true;
                        newWords.back()->L1 = false;
                    }
                }

                std::swap(Words, newWords);
            }
        }

        ui32 minimumShouldMatch = MinimumShouldMatchFromString(Words.size(), defaultOperator, Settings->GetMinimumShouldMatch(), explain);
        if (!explain.empty()) {
            RuntimeError(explain, NYql::NDqProto::StatusIds::BAD_REQUEST);
            return;
        }

        QueryCtx = MakeIntrusive<TQueryCtx>(
            Words.size(), SumDocLength, DocCount, defaultOperator, minimumShouldMatch, ResultCellIndices);

        if (DictTableReader) {
            for (auto& word: Words) {
                QueryCtx->AddIDFValue(word->WordIndex, word->Frequency);
            }
        }

        YQL_ENSURE(IndexTableReader->GetUseArrowFormat());
        std::vector<std::unique_ptr<ITokenStream>> l1streams;
        std::vector<std::unique_ptr<ITokenStream>> l2streams;

        for (size_t i = 0; i < Words.size(); ++i) {
            auto& wordInfo = Words[i];
            if (wordInfo->L1) {
                l1streams.emplace_back(std::make_unique<TArrowTokenStream>(i));
            } else {
                int idx = l2streams.size();
                wordInfo->L2StreamIndex = idx;
                l2streams.emplace_back(std::make_unique<TArrowTokenStream>(idx));
            }
        }

        if (needL2Layer) {
            YQL_ENSURE(l2streams.size() > 0);
            YQL_ENSURE(l1streams.size() > 0);
        } else {
            YQL_ENSURE(l1streams.size() > 0);
            YQL_ENSURE(l2streams.size() == 0);
        }

        if (l2streams.size() > 0) {
            YQL_ENSURE(defaultOperator == EDefaultOperator::And);

            L2MergeAlgo = std::make_unique<TAndOptimizedMergeAlgorithm>(
                std::move(l2streams),
                minimumShouldMatch,
                MainTableReader->GetWithRelevance(),
                MainTableReader->GetKeyColumnTypes()
            );
        }

        if (defaultOperator == EDefaultOperator::And) {
            L1MergeAlgo = std::make_unique<TAndOptimizedMergeAlgorithm>(
                std::move(l1streams),
                minimumShouldMatch,
                MainTableReader->GetWithRelevance(),
                MainTableReader->GetKeyColumnTypes()
            );
        } else {
            L1MergeAlgo = std::make_unique<TDefaultMergeAlgorithm>(
                std::move(l1streams),
                minimumShouldMatch,
                MainTableReader->GetWithRelevance(),
                MainTableReader->GetKeyColumnTypes()
            );
        }

        if (Settings->HasBFactor() && Settings->GetBFactor() > EPSILON) {
            QueryCtx->SetBFactor(Settings->GetBFactor());
        }

        if (Settings->HasK1Factor() && Settings->GetK1Factor() > EPSILON) {
            QueryCtx->SetK1Factor(Settings->GetK1Factor());
        }

        for (auto& word : Words) {
            if (word->L1) {
                ContinueWordRead(word);
            }
        }
    }

    void PrepareTableReaders() {
        ResultCellIndices = MainTableReader->ResultCellIndices;
        SearchColumnIdx = MainTableReader->SearchColumnIdx;
        MainTableCovered = MainTableReader->MainTableCovered;

        auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        MainTableReader->AddResolvePartitioningRequest(request);
        IndexTableReader->AddResolvePartitioningRequest(request);
        if (Settings->GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance) {
            DictTableReader->AddResolvePartitioningRequest(request);
            if (DocsTableReader) {
                DocsTableReader->AddResolvePartitioningRequest(request);
            }
            if (StatsTableReader) {
                StatsTableReader->AddResolvePartitioningRequest(request);
            }
        }

        YQL_ENSURE(request->ResultSet.size() >= 1, "Expected at least one table to resolve partitioning");
        ResolveTablePartitioning(std::move(request));
    }

    void ResolveTablePartitioning(std::unique_ptr<NSchemeCache::TSchemeCacheRequest>&& request) {
        auto resolveRequest = std::make_unique<TEvTxProxySchemeCache::TEvResolveKeySet>(request.release());
        Send(MakeSchemeCacheID(), resolveRequest.release());
    }


public:
    TFullTextMatchSource(const NKikimrKqp::TKqpFullTextSourceSettings* settings,
        TIntrusivePtr<NActors::TProtoArenaHolder> arena,
        const NActors::TActorId& computeActorId,
        ui64 inputIndex,
        NYql::NDq::TCollectStatsLevel statsLevel,
        NYql::NDq::TTxId txId,
        ui64 taskId,
        const NKikimr::NMiniKQL::TTypeEnvironment& ,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NWilson::TTraceId&,
        TIntrusivePtr<TKqpCounters> counters)
        : Settings(settings)
        , Arena(arena)
        , ComputeActorId(computeActorId)
        , InputIndex(inputIndex)
        , HolderFactory(holderFactory)
        , Alloc(alloc)
        , Counters(counters)
        , IndexDescription(Settings->GetIndexDescription())
        , Database(Settings->GetDatabase())
        , LogPrefix(TStringBuilder() << "TxId: " << txId << ", task: " << taskId << ", CA Id " << computeActorId << ". ")
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , PipeCacheId(NKikimr::MakePipePerNodeCacheID(false))
        , MainTableReader(TMainTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , IndexTableReader(TIndexTableImplReader::FromSettings(Counters, Snapshot, LogPrefix, Settings, MainTableReader->GetWithRelevance()))
        , DocsTableReader(TDocsTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings, MainTableReader->GetWithRelevance()))
        , DictTableReader(TDictTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , StatsTableReader(TStatsTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings, MainTableReader->GetWithRelevance()))
        , ReadsState(Counters, LogPrefix)
        , DocsReadingQueue(SelfId(), ReadsState)
        , L2ReadingQueue(SelfId(), ReadsState)
        , WordsReadingQueue(SelfId(), ReadsState)
    {
        Y_ABORT_UNLESS(Arena);
        Y_ABORT_UNLESS(Settings->GetArena() == Arena->Get());

        IngressStats.Level = statsLevel;

        if (Settings->HasLimit() && Settings->GetLimit() > 0) {
            Limit = Settings->GetLimit();
        }

        if (Settings->HasSnapshot()) {
            Snapshot = IKqpGateway::TKqpSnapshot(
                Settings->GetSnapshot().GetStep(),
                Settings->GetSnapshot().GetTxId());
        }
    }

    void Bootstrap() {
        ReadsState.SetSelfId(this->SelfId());
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TFullTextMatchSource::StateWork);
        PrepareTableReaders();
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueBatch& resultBatch,
        TMaybe<TInstant>&,
        bool& finished,
        i64 freeSpace) override
    {
        YQL_ENSURE(!resultBatch.IsWide(), "Wide stream is not supported");

        PendingNotify = false;
        auto guard = BindAllocator();
        i64 computeBytes = 0;

        while(!ResultQueue.empty()) {
            TDocumentInfo& documentInfo = ResultQueue.front().GetRef();
            ProducedItemsCount++;

            auto row = documentInfo.GetRow(QueryCtx.GetRef(), HolderFactory, computeBytes);
            resultBatch.emplace_back(std::move(row));

            ResultQueue.pop_front();
            if (Limit > 0 && ProducedItemsCount >= static_cast<ui64>(Limit)) {
                break;
            }

            if (computeBytes > freeSpace) {
                break;
            }
        }

        if (!ResultQueue.empty()) {
            NotifyCA();
        }

        finished = IsFinished();
        return computeBytes;
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    void LoadState(const TSourceState&) override {}

    void PassAway() override {
        {
            for (auto& [id, state] : ReadsState.GetReads()) {
                auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
                cancel->Record.SetReadId(id);
                Send(PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
            }
        }
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));

        TBase::PassAway();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode) {
        NYql::TIssues issues;
        issues.AddIssue(NYql::TIssue(message));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

    bool IsFinished() {
        return ReadsState.Empty() && !ResolveInProgress && ResultQueue.empty()
                && !L2ReadingQueue.HasPendingSequential() ||
            ProducedItemsCount >= static_cast<ui64>(Limit);
    }

    void NotifyCA() {
        if (!PendingNotify && (!ResultQueue.empty() || IsFinished())) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
            PendingNotify = true;
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleReadResult);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
            hFunc(TEvPrivate::TEvRetryRead, HandleRetryRead);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
        }
    }

    bool OnUnhandledException(const std::exception_ptr& exception) override {
        try {
            if (exception) {
                std::rethrow_exception(exception);
            }
        } catch (const std::exception& ex) {
            RuntimeError(ex.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return true;
        } catch (const TMemoryLimitExceededException& ex) {
            RuntimeError("Memory limit exceeded at full text source", NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
            return true;
        }

        return false;
    }

    bool OnUnhandledException(const std::exception&) override {
        return false;
    }

    void HandleRetryRead(TEvPrivate::TEvRetryRead::TPtr& ev) {
        DoRetryRead(ev->Get()->ShardId);
    }

    void RetryWordRead(ui64 wordIndex, ui64 readId) {
        YQL_ENSURE(wordIndex < Words.size(), "Word index out of bounds");
        auto& word = Words[wordIndex];
        word->BuildRangesToRead();
        ContinueWordRead(word);
        ReadsState.RemoveRead(readId);
    }

    void RetryTotalStatsRead(ui64 readId) {
        ReadsState.RemoveRead(readId);
        ReadTotalStats();
    }

    void DoRetryRead(ui64 shardId) {
        auto shardIt = ReadsState.FindShardPtr(shardId);
        YQL_ENSURE(shardIt != nullptr);

        shardIt->IsScheduledRetry = false;

        std::vector<ui64> readsToRetry(shardIt->ReadIds.begin(), shardIt->ReadIds.end());

        for (ui64 readId : readsToRetry) {
            auto readIt = ReadsState.FindPtr(readId);
            if (readIt == nullptr) {
                continue;
            }

            auto readInfo = *readIt;

            switch (readInfo.ReadKind) {
                case EReadKind_DocumentStats:
                    DocsReadingQueue.Retry(DocsTableReader.Get(), EReadKind_DocumentStats, readId);
                    break;
                case EReadKind_Document:
                    DocsReadingQueue.Retry(MainTableReader.Get(), EReadKind_Document, readId);
                    break;
                case EReadKind_Word:
                    RetryWordRead(readInfo.Cookie, readId);
                    break;
                case EReadKind_WordStats:
                    WordsReadingQueue.Retry(DictTableReader.Get(), EReadKind_WordStats, readId);
                    break;
                case EReadKind_TotalStats:
                    RetryTotalStatsRead(readId);
                    break;
            }
        }
    }

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_E("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        ui64 shardId = ev->Get()->TabletId;
        ReadsState.UntrackPipe(shardId);
        auto shardIt = ReadsState.FindShardPtr(shardId);
        if (shardIt == nullptr) {
            // No reads for this shard, ignore
            return;
        }

        if (shardIt->IsScheduledRetry) {
            return;
        }

        Counters->IteratorDeliveryProblems->Inc();
        auto delay = ReadsState.GetDelay(shardId, true);

        auto maxRetries = MaxShardRetries();
        if (ReadsState.CheckShardRetriesExeeded(shardId, maxRetries)) {
            RuntimeError(TStringBuilder() << "Max shard retries ("<< maxRetries << ") exeeded for shard " << shardId, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }

        if (delay > TDuration::Zero() ) {
            shardIt->IsScheduledRetry = true;
            TlsActivationContext->Schedule(delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(shardId)));
        } else {
            DoRetryRead(shardId);
        }
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        CA_LOG_D("TEvResolveKeySetResult was received for table.");
        ResolveInProgress = false;

        if (ev->Get()->Request->ErrorCount > 0) {
            for(const auto& entry : ev->Get()->Request->ResultSet) {
                CA_LOG_E("Table " << entry.KeyDescription->TableId << " error status: " << entry.Status);
            }

            TString errorMsg = TStringBuilder() << "Failed to get partitioning for table. ";
            return RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() >= 2, "Expected at least 2 tables for fulltext index");
        MainTableReader->SetPartitionInfo(resultSet[0].KeyDescription);
        IndexTableReader->SetPartitionInfo(resultSet[1].KeyDescription);
        if (Settings->GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance) {
            size_t count = 3 + (DocsTableReader ? 1 : 0) + (StatsTableReader ? 1 : 0);
            YQL_ENSURE(resultSet.size() == count, "Expected " << count << " tables for flat relevance index");
            DictTableReader->SetPartitionInfo(resultSet[2].KeyDescription);
            if (DocsTableReader) {
                DocsTableReader->SetPartitionInfo(resultSet[3].KeyDescription);
            }
            if (StatsTableReader) {
                StatsTableReader->SetPartitionInfo(resultSet[3 + (DocsTableReader ? 1 : 0)].KeyDescription);
            }
        }

        if (ExtractAndTokenizeExpression()) {
            if (StatsTableReader || DictTableReader) {
                if (StatsTableReader) {
                    ReadTotalStats();
                }
                if (DictTableReader) {
                    EnrichWordInfo();
                }
            } else {
                StartWordReads();
            }
        }
    }

    void ReadTotalStats() {
        ui64 readId = ReadsState.GetNextReadId();
        auto [shardId, request] = StatsTableReader->GetTotalStatsRequest(readId);
        ReadsState.SendEvRead(shardId, request, TReadInfo{.ReadKind = EReadKind_TotalStats, .Cookie = readId, .ShardId = shardId});
    }

    void DocumentDetailsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 readId, bool finished) {
        auto& readItems = DocsReadingQueue.GetReadItems(readId);

        ui64 rows = 0;
        ui64 bytes = 0;
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            if (Limit > 0 && ProducedItemsCount + ResultQueue.size() >= static_cast<ui64>(Limit)) {
                DocsReadingQueue.ClearReadItems(readItems);
                break;
            }

            const auto& row = msg.GetCells(i);
            for(const auto& cell: row) {
                bytes += std::max(cell.Size(), (ui32)8);
            }

            rows++;
            auto& doc = readItems.GetItem();
            YQL_ENSURE(doc->DocumentNumId == row.at(0).AsValue<ui64>(), "detected out of order document reading");
            doc->AddRow(row);
            ResultQueue.push_back(std::move(doc));
            readItems.PopItem();
        }

        MainTableReader->RecvStats(rows, bytes);

        DocsReadingQueue.UpdateReadStatus(readId, readItems, finished);

        // Notify about possibly empty result
        NotifyCA();
    }

    void ProcessTopKQueue() {
        std::vector<TDocumentInfo::TPtr> documentInfos;
        documentInfos.reserve(TopKQueue.size());

        while (!TopKQueue.empty()) {
            auto&[score, documentInfo] = TopKQueue.back();
            documentInfos.emplace_back(std::move(documentInfo));
            TopKQueue.pop_back();
        }

        FetchDocumentDetails(documentInfos);
    }

    void DocumentStatsResultArrow(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 readId, bool finished) {

        auto& readItems = DocsReadingQueue.GetReadItems(readId);
        std::vector<TDocumentInfo::TPtr> documentInfos;
        auto batch = msg.GetArrowBatch();
        size_t rows = msg.GetRowsCount();
        YQL_ENSURE(batch);
        YQL_ENSURE(batch->columns().size() >= 2);
        auto docIds = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        YQL_ENSURE(docIds);
        auto freq_array = std::static_pointer_cast<arrow::UInt32Array>(batch->column(1));
        YQL_ENSURE(freq_array);
        YQL_ENSURE(freq_array->length() == docIds->length());
        YQL_ENSURE((i64)rows == freq_array->length());

        DocsTableReader->RecvStats(rows, rows * (sizeof(ui64) + sizeof(ui32)));

        for(size_t i = 0; i < rows; ++i) {
            TDocumentInfo::TPtr doc = readItems.GetItem();
            YQL_ENSURE(doc->DocumentNumId == docIds->Value(i), "detected out of order document reading");
            doc->SetDocumentLength(freq_array->Value(i));

            if (Limit > 0) {
                TopKQueue.emplace_back(doc->GetBM25Score(QueryCtx.GetRef()), std::move(doc));
                std::push_heap(TopKQueue.begin(), TopKQueue.end());
                if (TopKQueue.size() > (size_t)Limit) {
                    std::pop_heap(TopKQueue.begin(), TopKQueue.end());
                    TopKQueue.pop_back();
                }

            } else {
                documentInfos.emplace_back(std::move(doc));
            }

            readItems.PopItem();
        }

        DocsReadingQueue.UpdateReadStatus(readId, readItems, finished);

        FetchDocumentDetails(documentInfos);

        if (ReadsState.Empty()) {
            ProcessTopKQueue();
        }
    }

    void DocumentStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 readId, bool finished) {
        DocumentStatsResultArrow(msg, readId, finished);
    }

    void HandleTotalStatsResult(TEvDataShard::TEvReadResult& msg) {
        size_t rows = msg.GetRowsCount();
        size_t bytes = 0;
        for (size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            for(const auto& cell: row) {
                bytes += cell.Size();
            }

            DocCount = StatsTableReader->GetDocCount(row);
            SumDocLength = StatsTableReader->GetSumDocLength(row);
        }

        StatsTableReader->RecvStats(rows, bytes);

        if (ReadsState.Empty()) {
            StartWordReads();
        }
    }

    void WordStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 readId, bool finished) {
        auto& readItems = WordsReadingQueue.GetReadItems(readId);

        ui64 rows = 0;
        ui64 bytes = 0;
        for (size_t i = 0; i < msg.GetRowsCount(); i++) {
            const auto& row = msg.GetCells(i);
            const auto& wordBuf = DictTableReader->GetWord(row);

            while (!readItems.Empty() && readItems.GetItem()->Word != wordBuf) {
                readItems.GetItem()->Frequency = 0;
                readItems.PopItem();
            }

            rows++;
            bytes += sizeof(ui32) + wordBuf.size();
            YQL_ENSURE(!readItems.Empty(), "Word not found in read items");
            auto& word = readItems.GetItem();
            word->Frequency = DictTableReader->GetWordFrequency(row);
            readItems.PopItem();
        }

        DictTableReader->RecvStats(rows, bytes);

        if (finished) {
            while (!readItems.Empty()) {
                readItems.GetItem()->Frequency = 0;
                readItems.PopItem();
            }
        }

        WordsReadingQueue.UpdateReadStatus(readId, readItems, finished);

        if (ReadsState.Empty()) {
            StartWordReads();
        }
    }

    void L2WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult> msg, ui64 recReadId, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& wordInfo = Words[wordIndex];
        YQL_ENSURE(wordInfo->L2);

        // With one-at-a-time sequential reads, results arrive in order
        // and can be fed directly to L2MergeAlgo without buffering
        L2MergeAlgo->AddResult(wordInfo->L2StreamIndex, std::move(msg));

        if (finished) {
            // Cleanup the finished read
            auto& readItems = L2ReadingQueue.GetReadItems(recReadId);
            L2ReadingQueue.ClearReadItems(readItems);
            L2ReadingQueue.UpdateReadStatus(recReadId, readItems, true);

            L2ReadingQueue.ClearInflightSequentialRead(wordIndex);

            // Send next sequential read for this word if any
            if (!L2ReadingQueue.SendNextSequentialRead(IndexTableReader.Get(), EReadKind_Word_L2, wordIndex)) {
                // No more pending reads for this word; if L1 is also done, finish the stream
                if (L1MergeAlgo->Done()) {
                    L2MergeAlgo->FinishTokenStream(wordInfo->L2StreamIndex);
                }
            }
        }

        std::vector<TDocumentInfo::TPtr> matches = L2MergeAlgo->FindMatches();
        if (!matches.empty()) {
            MergeL2MatchFrequencies(matches);
            FetchDocumentDetails(matches);
        }
        NotifyCA();
    }

    void ScheduleL2Read(std::vector<TDocumentInfo::TPtr>& matches) {
        for(int i = Words.size() - 1; i >= 0; i--) {
            auto& word = Words[i];
            if (word->L1) {
                continue;
            }

            std::vector<TL1DocumentInfo::TPtr> remappedMatches;
            remappedMatches.reserve(matches.size());
            for(auto& match: matches) {
                remappedMatches.emplace_back(MakeIntrusive<TL1DocumentInfo>(match, word->Word));
            }

            L2ReadingQueue.Sequential(IndexTableReader.Get(), EReadKind_Word_L2, remappedMatches, i);
        }

        L1MergedDocuments.insert(L1MergedDocuments.end(), matches.begin(), matches.end());
    }

    void TryFinishL2Streams() {
        YQL_ENSURE(L2MergeAlgo);
        YQL_ENSURE(L1MergeAlgo->Done());

        for (size_t i = 0; i < Words.size(); ++i) {
            if (!Words[i]->L2) {
                continue;
            }
            if (!L2ReadingQueue.HasInflightOrPendingSequential(i)) {
                L2MergeAlgo->FinishTokenStream(Words[i]->L2StreamIndex);
            }
        }

        // Drain any remaining L2 matches
        std::vector<TDocumentInfo::TPtr> matches = L2MergeAlgo->FindMatches();
        if (!matches.empty()) {
            MergeL2MatchFrequencies(matches);
            FetchDocumentDetails(matches);
        }
    }

    void MergeL2MatchFrequencies(std::vector<TDocumentInfo::TPtr>& matches) {
        for (auto& match : matches) {
            while (!L1MergedDocuments.empty() &&
                   L1MergedDocuments.front()->DocumentNumId != match->DocumentNumId) {
                L1MergedDocuments.pop_front();
            }

            YQL_ENSURE(!L1MergedDocuments.empty(), "L2 match has no corresponding L1 document");
            auto& l1Doc = L1MergedDocuments.front();

            std::vector<ui32> combined(Words.size(), 0);
            for (size_t wi = 0; wi < Words.size(); ++wi) {
                if (Words[wi]->L1 && wi < l1Doc->TokenFrequencies.size()) {
                    combined[wi] = l1Doc->TokenFrequencies[wi];
                }
            }
            for (size_t wi = 0; wi < Words.size(); ++wi) {
                if (Words[wi]->L2 && Words[wi]->L2StreamIndex < match->TokenFrequencies.size()) {
                    combined[wi] = match->TokenFrequencies[Words[wi]->L2StreamIndex];
                }
            }
            match->TokenFrequencies = std::move(combined);
            L1MergedDocuments.pop_front();
        }
    }

    void L1WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult> msg, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& incomingWordInfo = Words[wordIndex];
        YQL_ENSURE(incomingWordInfo->L1);

        L1MergeAlgo->AddResult(wordIndex, std::move(msg));

        if (finished) {
            if (!ContinueWordRead(incomingWordInfo)) {
                L1MergeAlgo->FinishTokenStream(wordIndex);
            }
        }

        std::vector<TDocumentInfo::TPtr> matches = L1MergeAlgo->FindMatches();
        if (L2MergeAlgo) {
            ScheduleL2Read(matches);
            // When L1 is fully done, finish any L2 streams that have no pending reads
            if (L1MergeAlgo->Done()) {
                TryFinishL2Streams();
            }
        } else {
            FetchDocumentDetails(matches);
        }

        NotifyCA();
    }

    template<typename TReader>
    void ExportTableReaderStats(NDqProto::TDqTaskStats* stats, const TIntrusivePtr<TReader>& reader) {
        NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == reader->GetTablePath()) {
                tableStats = table;
            }
        }

        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(reader->GetTablePath());
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + reader->GetReadRows());
        tableStats->SetReadBytes(tableStats->GetReadBytes() + reader->GetReadBytes());
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (last) {
            if (L1MergeAlgo) {
                auto [rows, bytes] = L1MergeAlgo->GetStats();
                IndexTableReader->RecvStats(rows, bytes);
            }

            if (L2MergeAlgo) {
                auto [rows, bytes] = L2MergeAlgo->GetStats();
                IndexTableReader->RecvStats(rows, bytes);
            }

            ExportTableReaderStats(stats, MainTableReader);
            ExportTableReaderStats(stats, IndexTableReader);
            if (DocsTableReader) {
                ExportTableReaderStats(stats, DocsTableReader);
            }
            if (StatsTableReader) {
                ExportTableReaderStats(stats, StatsTableReader);
            }
            if (DictTableReader) {
                ExportTableReaderStats(stats, DictTableReader);
            }
        }
    }

    void HandleReadResult(TEvDataShard::TEvReadResult::TPtr& ev) {
        ui64 readId = ev->Get()->Record.GetReadId();
        auto& record = ev->Get()->Record;

        auto it = ReadsState.FindPtr(readId);
        if (it == nullptr) {
            return;
        }

        auto& readInfo = *it;

        CA_LOG_D("Recv TEvReadResult (full text source)"
            << ", Cookie=" << readInfo.Cookie
            << ", ReadKind=" << (ui32)readInfo.ReadKind
            << ", ShardId=" << readInfo.ShardId
            << ", ReadId=" << record.GetReadId()
            << ", SeqNo=" << record.GetSeqNo()
            << ", Status=" << Ydb::StatusIds::StatusCode_Name(record.GetStatus().GetCode())
            << ", Finished=" << record.GetFinished()
            << ", RowCount=" << record.GetRowCount()
            << ", ResultFormat=" << NKikimrDataEvents::EDataFormat_Name(record.GetResultFormat())
            << ", TxLocks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", BrokenTxLocks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetBrokenTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            // add retry logic a bit later
            RuntimeError("Read result status is not success", NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        auto& msg = *ev->Get();
        ui64 cookie = readInfo.Cookie;
        auto readKind = readInfo.ReadKind;
        YQL_ENSURE(readInfo.LastSeqNo < record.GetSeqNo());
        readInfo.LastSeqNo = record.GetSeqNo();

        if (record.GetFinished()) {
            ReadsState.RemoveRead(readId);
        } else {
            ReadsState.AckRead(readId);
        }

        switch (readKind) {
            case EReadKind_Document:
                DocumentDetailsResult(msg, cookie, record.GetFinished());
                break;
            case EReadKind_DocumentStats:
                DocumentStatsResult(msg, cookie, record.GetFinished());
                break;
            case EReadKind_WordStats:
                WordStatsResult(msg, cookie, record.GetFinished());
                break;
            case EReadKind_Word:
                L1WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult>(ev->Release().Release()), cookie, record.GetFinished());
                break;
            case EReadKind_Word_L2:
                L2WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult>(ev->Release().Release()), readId, cookie, record.GetFinished());
                break;
            case EReadKind_TotalStats:
                HandleTotalStatsResult(msg);
                break;
        }
    }

private:
    using TBase = TActorBootstrapped<TFullTextMatchSource>;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpFullTextSource(const NKikimrKqp::TKqpFullTextSourceSettings* settings,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena, // Arena for settings
    const NActors::TActorId& computeActorId,
    ui64 inputIndex,
    NYql::NDq::TCollectStatsLevel statsLevel,
    NYql::NDq::TTxId txId,
    ui64 taskId,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
    const NWilson::TTraceId& traceId,
    TIntrusivePtr<TKqpCounters> counters)
{
    auto* actor = new TFullTextMatchSource(settings, arena, computeActorId, inputIndex, statsLevel, txId, taskId, typeEnv, holderFactory, alloc, traceId, counters);
    return std::make_pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(actor, actor);
}

void RegisterKqpFullTextSource(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSource<NKikimrKqp::TKqpFullTextSourceSettings>(
        TString(NYql::KqpFullTextSourceName),
        [counters] (const NKikimrKqp::TKqpFullTextSourceSettings* settings, NYql::NDq::TDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateKqpFullTextSource(settings, args.Arena, args.ComputeActorId, args.InputIndex, args.StatsLevel,
        args.TxId, args.TaskId, args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters);
        });
}

}

