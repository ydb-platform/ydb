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

#include <library/cpp/regex/pire/pire.h>
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

namespace NKikimr {
namespace NKqp {

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

class TDocId;

namespace {

TString WildcardToRegex(const TStringBuf wildcardPattern) {
    static const TStringBuf special = R"(^$.\+?()|{}[])";
    TStringBuilder builder;
    for (char c : wildcardPattern) {
        if (c == '*') {
            builder << '.';
        } else if (special.find(c) != TStringBuf::npos) {
            builder << '\\';
        }
        builder << c;
    }
    return builder;
}

}

template <typename T>
class TTableReader : public TAtomicRefCount<T> {
    TIntrusivePtr<TKqpCounters> Counters;
    TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;
    TString LogPrefix;

    bool UseArrowFormat = false;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;
    TVector<i32> ResultColumnIds;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> PartitionInfo;
    absl::flat_hash_map<ui64, std::deque<TOwnedTableRange>> RangesToRead;

public:

    TTableReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : Counters(counters)
        , TableId(tableId)
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


    template <typename TableRange>
    std::unique_ptr<TEvDataShard::TEvRead> GetReadRequest(ui64 readId, ui64 shardId, const TableRange& range) {
        return GetReadRequest(readId, shardId, std::deque<TableRange>{range});
    }

    template <typename TableRange>
    std::unique_ptr<TEvDataShard::TEvRead> GetReadRequest(ui64 readId, ui64 shardId, const std::deque<TableRange>& ranges) {
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

        CA_LOG_E(TStringBuilder() << "Send EvRead (full text source) to shardId=" << shardId
            << ", readId = " << record.GetReadId()
            << ", snapshot=(txid=" << record.GetSnapshot().GetTxId() << ", step=" << record.GetSnapshot().GetStep() << ")"
            << ", lockTxId=" << record.GetLockTxId()
            << ", lockNodeId=" << record.GetLockNodeId());

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

    void StageRangeToRead(const TTableRange& range) {
        auto requests = GetRangePartitioning(range);
        for(const auto& [shardId, range] : requests) {
            RangesToRead[shardId].emplace_back(std::move(range));
        }
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> GetNextReadRequest(ui64 nextReadId) {
        if (RangesToRead.empty()) {
            return {0, nullptr};
        }

        auto [shardId, ranges] = *RangesToRead.begin();
        auto request = GetReadRequest(nextReadId, shardId, ranges);
        RangesToRead.erase(shardId);
        return {shardId, std::move(request)};
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
    const EQueryMode QueryMode;
    const ui32 MinimumShouldMatch;

public:
    TQueryCtx(size_t wordCount, ui64 totalDocLength, ui64 docCount,
        EQueryMode queryMode,
        ui32 minimumShouldMatch,
        const TVector<std::pair<i32, NScheme::TTypeInfo>> resultCellIndices)
        : DocCount(docCount)
        , AvgDL(docCount > 0 ? static_cast<double>(totalDocLength) / docCount : 1.0)
        , IDFValues(wordCount, 0.0)
        , ResultCellIndices(resultCellIndices)
        , QueryMode(queryMode)
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

    EQueryMode GetQueryMode() const {
        return QueryMode;
    }

    ui32 GetMinimumShouldMatch() const {
        return MinimumShouldMatch;
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

class TDocumentInfo : public TAtomicRefCount<TDocumentInfo> {
    TOwnedCellVec KeyCells;
    TOwnedCellVec RowCells;
    std::vector<ui32> WordFrequencies;
    ui64 DocumentLength = std::numeric_limits<ui64>::max();
public:
    TDocumentInfo(TOwnedCellVec&& keyCells)
        : KeyCells(std::move(keyCells))
    {}

    void SetWordFrequencies(std::vector<ui32>&& wordFrequencies) {
        WordFrequencies = std::move(wordFrequencies);
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
        for(size_t i = 0; i < WordFrequencies.size(); ++i) {
            double docFreq = static_cast<double>(WordFrequencies[i]);
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

            if (cellIndex < (i32)KeyCells.size()) {
                rowItems[i] = NMiniKQL::GetCellValue(KeyCells[cellIndex], cellType);
                computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], cellType).AllocatedBytes;
                continue;
            }

            rowItems[i] = NMiniKQL::GetCellValue(RowCells[cellIndex], cellType);
            computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], cellType).AllocatedBytes;
        }

        return row;
    }

    ui64 GetRowStorageSize() const {
        ui64 rowStorageSize = 0;
        for(size_t i = 0; i < KeyCells.size(); ++i) {
            rowStorageSize += KeyCells[i].Size();
        }
        return rowStorageSize;
    }

    const TOwnedCellVec& GetDocumentId() const {
        return KeyCells;
    }
};

class TDocId {
public:
    size_t WordIndex;
    ui64 DocId = 0;
    const TCell* Document;

    struct TCompareSign {
        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TCompareSign(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        int operator()(const TDocId& key1, const TDocId& key2) const {
            if (key1.Document == nullptr) {
                if (key1.DocId > key2.DocId)
                    return 1;
                if (key1.DocId < key2.DocId)
                    return -1;
                return 0;
            }
            YQL_ENSURE(key1.Document);
            YQL_ENSURE(key2.Document);
            return CompareTypedCellVectors(key1.Document, key2.Document, DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size());
        }
    };

    struct TCompare {
        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TCompare(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        bool operator()(const TDocId& key1, const TDocId& key2) const {
            if (key1.Document == nullptr)
                return key1.DocId > key2.DocId;
            YQL_ENSURE(key1.Document);
            YQL_ENSURE(key2.Document);
            return CompareTypedCellVectors(key1.Document, key2.Document, DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size()) > 0;
        }
    };

    struct TEquals {
        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TEquals(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        bool operator()(const TDocId& key1, const TDocId& key2) const {
            if (key1.Document == nullptr)
                return key1.DocId == key2.DocId;
            YQL_ENSURE(key1.Document);
            YQL_ENSURE(key2.Document);
            return CompareTypedCellVectors(key1.Document, key2.Document, DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size()) == 0;
        }
    };

    struct TKeyGetter {

        TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

        TKeyGetter(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
            : DocumentKeyColumnTypes(documentKeyColumnTypes)
        {}

        TOwnedCellVec operator()(const TDocId& docId) const {
            if (docId.Document == nullptr) {
                TVector<TCell> cells = {TCell::Make(docId.DocId)};
                return TOwnedCellVec(TConstArrayRef<TCell>(cells.data(), cells.size()));
            }
            YQL_ENSURE(docId.Document);
            return TOwnedCellVec(TConstArrayRef<TCell>(docId.Document, DocumentKeyColumnTypes.size()));
        }
    };

    explicit TDocId(size_t wordIndex, ui64 docId, const TCell* document)
        : WordIndex(wordIndex)
        , DocId(docId)
        , Document(document)
    {}
};


class TIndexTableImplReader : public TTableReader<TIndexTableImplReader> {
    ui32 FrequencyColumnIndex = 0;

public:
    TIndexTableImplReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        ui32 frequencyColumnIndex)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , FrequencyColumnIndex(frequencyColumnIndex)
    {}

    static TIntrusivePtr<TIndexTableImplReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        const auto& indexDescription = settings->GetIndexDescription();
        YQL_ENSURE(settings->GetIndexTables().size() >= 1);
        auto& info = settings->GetIndexTables(settings->GetIndexTables().size() - 1);
        YQL_ENSURE(info.GetTable().GetPath().EndsWith(NTableIndex::ImplTable));
        auto& columns = info.GetColumns();
        auto& keyColumns = info.GetKeyColumns();

        i32 freqColumnIndex = -1;
        NScheme::TTypeInfo freqColumnType;
        for (const auto& column: columns) {
            if (column.GetName() == FreqColumn) {
                freqColumnIndex = column.GetId();
                freqColumnType = NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo());
            }
        }

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

        ui32 frequencyColumnIndex = std::numeric_limits<ui32>::max();
        if (indexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(freqColumnIndex != -1);
            frequencyColumnIndex = resultColumnTypes.size();
            resultColumnTypes.push_back(freqColumnType);
            resultColumnIds.push_back(freqColumnIndex);
        }

        bool useArrowFormat = false;

        // arrow format is supported only key consist of the single column of type Uint64
        // doc_id (Uint64), [freq (Uint32)]
        if (resultColumnTypes.size() == 1 + (freqColumnIndex != -1) && (resultColumnTypes[0].GetTypeId() == NScheme::NTypeIds::Uint64)) {
            useArrowFormat = true;
        }

        TIntrusivePtr<TIndexTableImplReader> reader = MakeIntrusive<TIndexTableImplReader>(
            counters, FromProto(info.GetTable()), snapshot, logPrefix,
            keyColumnTypes, resultColumnTypes, resultColumnIds, frequencyColumnIndex);
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

public:
    virtual ~ITokenStream() = default;

    ITokenStream(ui64 tokenIndex)
        : TokenIndex(tokenIndex)
    {}

    void SetReadFinished() {
        ReadFinished = true;
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
    virtual TDocId GetLeastDocId() = 0;

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
        if (batch->num_columns() > 1) {
            auto array = batch->column(1);
            auto freq_array = std::static_pointer_cast<arrow::UInt32Array>(array);
            YQL_ENSURE(freq_array);
            YQL_ENSURE(freq_array->length() == docIds->length());
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

    TDocId GetLeastDocId() override {
        YQL_ENSURE(!PendingDocumentIds.empty());
        return TDocId(TokenIndex, PendingDocumentIds.front()->Value(UnprocessedDocumentPos), nullptr);
    }
};

class TCellVecTokenStream : public ITokenStream {
    ui32 FrequencyColumnIndex = 0;
public:
    TCellVecTokenStream(ui64 tokenIndex, ui32 frequencyColumnIndex)
        : ITokenStream(tokenIndex)
        , FrequencyColumnIndex(frequencyColumnIndex)
    {
    }

    ui32 GetLeastDocFrequency() const override {
        YQL_ENSURE(!PendingReadResults.empty());
        const auto& cells = PendingReadResults.front()->GetCells(UnprocessedDocumentPos);
        YQL_ENSURE(FrequencyColumnIndex < cells.size());
        return cells[FrequencyColumnIndex].AsValue<ui32>();
    }

    TDocId GetLeastDocId() override {
        YQL_ENSURE(!PendingReadResults.empty());
        return TDocId(TokenIndex, 0, PendingReadResults.front()->GetCells(UnprocessedDocumentPos).data());
    }
};


class TWordReadState {
public:
    ui64 WordIndex;
    TString Word;
    bool PendingRead = false;
    TIntrusivePtr<TIndexTableImplReader> Reader;
    std::deque<std::pair<ui64, TOwnedTableRange>> RangesToRead;
    ui32 Frequency = 0;

    explicit TWordReadState(ui64 wordIndex, const TString& word, const TIntrusivePtr<TIndexTableImplReader>& reader)
        : WordIndex(wordIndex)
        , Word(word)
        , Reader(reader)
    {
        BuildRangesToRead();
    }

    TOwnedTableRange GetWordKeyCells() const {
        TCell tokenCell(Word.data(), Word.size());
        TVector <TCell> fromCells{tokenCell};
        return TOwnedTableRange(fromCells);
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> BuildNextRangeToRead(ui64 readId) {
        YQL_ENSURE(!RangesToRead.empty());
        auto [shardId, range] = RangesToRead.front();
        RangesToRead.pop_front();
        return std::make_pair(shardId, Reader->GetReadRequest(readId, shardId, range));
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
    TDocId::TKeyGetter KeyGetter;
    const bool WithFrequencies;
    ui64 FinishedTokens = 0;
    std::vector<ui32> MatchedTokens;

public:
    struct TMatch {
        TDocId DocId;
        std::vector<ui32> Frequencies;

        explicit TMatch(TDocId&& docId)
            : DocId(std::move(docId))
        {
        }
    };

    IMergeAlgorithm(std::vector<std::unique_ptr<ITokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : Streams(std::move(streams))
        , TokenCount(Streams.size())
        , MinShouldMatch(minShouldMatch)
        , DocIdEquals(TDocId::TEquals(keyColumnTypes))
        , DocIdCompare(TDocId::TCompare(keyColumnTypes))
        , KeyGetter(TDocId::TKeyGetter(keyColumnTypes))
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

    virtual std::vector<TMatch> FindMatches() = 0;
    virtual ~IMergeAlgorithm() = default;
};

class TAndOptimizedMergeAlgorithm : public IMergeAlgorithm {
    std::deque<ui32> ReadyStreams;
    TDocId::TCompareSign DocIdCompareSign;

public:
    TAndOptimizedMergeAlgorithm(std::vector<std::unique_ptr<ITokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : IMergeAlgorithm(std::move(streams), minShouldMatch, withFrequencies, keyColumnTypes)
        , DocIdCompareSign(TDocId::TCompareSign(keyColumnTypes))
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

    int JumpToBest(TDocId& candidate) {
        ui32 tokenIndex = ReadyStreams.front();
        ReadyStreams.pop_front();

        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        do {
            auto leastDocId = stream->GetLeastDocId();
            int compareSign = DocIdCompareSign(leastDocId, candidate);
            if (compareSign == 0) {
                MatchedTokens.push_back(tokenIndex);
                return 0;
            } else if (compareSign > 0) {
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

    std::vector<TMatch> FindMatches() override {
        if (FinishedTokens > 0)
            return std::vector<TMatch>();

        std::vector<TMatch> matches;
        while (MatchedTokens.size() + ReadyStreams.size() == TokenCount) {
            YQL_ENSURE(ReadyStreams.size() > 0, "ReadyStreams must be non-empty");
            if (MatchedTokens.empty()) {
                MatchedTokens.push_back(ReadyStreams.front());
                ReadyStreams.pop_front();
            }

            YQL_ENSURE(MatchedTokens.back() < Streams.size(), "Matched token index out of bounds");
            TDocId candidate = Streams[MatchedTokens.back()]->GetLeastDocId();

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

            auto match = TMatch(std::move(candidate));
            if (WithFrequencies) {
                match.Frequencies.resize(TokenCount, 0);
                for (ui32 tokenIndex : MatchedTokens) {
                    YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                    match.Frequencies[tokenIndex] = Streams[tokenIndex]->GetLeastDocFrequency();
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
            MergeQueue.push(stream->GetLeastDocId());
        }
    }

    std::vector<TMatch> FindMatches() override {
        std::vector<TMatch> matches;
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
                auto match = TMatch(std::move(doc));
                if (WithFrequencies) {
                    match.Frequencies.resize(TokenCount, 0);
                    for (ui32 tokenIndex : matchedTokens) {
                        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                        match.Frequencies[tokenIndex] = Streams[tokenIndex]->GetLeastDocFrequency();
                    }
                }
                matches.push_back(std::move(match));
            }

            for (ui32 tokenIndex : matchedTokens) {
                YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
                auto& token = Streams[tokenIndex];
                if (bool hasMore = token->MoveToNext(); hasMore) {
                    MergeQueue.push(token->GetLeastDocId());
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
    Ydb::Table::FulltextIndexSettings::Layout Layout;
public:
    TDocsTableReader(const Ydb::Table::FulltextIndexSettings::Layout& layout,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , Layout(layout)
    {}

    static TIntrusivePtr<TDocsTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        const auto& indexDescription = settings->GetIndexDescription();
        if (indexDescription.GetSettings().layout() != Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
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

        YQL_ENSURE(docLengthColumnIndex != -1);
        resultKeyColumnTypes.push_back(docLengthColumnType);
        resultKeyColumnIds.push_back(docLengthColumnIndex);

        return MakeIntrusive<TDocsTableReader>(
            indexDescription.GetSettings().layout(), counters,
            FromProto(info.GetTable()), snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    ui64 GetDocumentLength(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui32>();
            default:
                return 0;
        }
    }
};

class TStatsTableReader : public TTableReader<TStatsTableReader> {
    Ydb::Table::FulltextIndexSettings::Layout Layout;
public:
    TStatsTableReader(const Ydb::Table::FulltextIndexSettings::Layout& layout,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , Layout(layout)
    {}

    static TIntrusivePtr<TStatsTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        const auto& indexDescription = settings->GetIndexDescription();
        if (indexDescription.GetSettings().layout() != Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
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
            indexDescription.GetSettings().layout(), counters,
            FromProto(info.GetTable()), snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    ui64 GetDocCount(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
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
        return std::make_pair(shardId, GetReadRequest(readId, shardId, range));
    }

    ui64 GetSumDocLength(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[1].AsValue<ui64>();
            default:
                return 0;
        }
    }
};

class TDictTableReader : public TTableReader<TDictTableReader> {
    Ydb::Table::FulltextIndexSettings::Layout Layout;
public:
    TDictTableReader(const Ydb::Table::FulltextIndexSettings::Layout& layout,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , Layout(layout)
    {}

    static TIntrusivePtr<TDictTableReader> FromSettings(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrKqp::TKqpFullTextSourceSettings* settings)
    {
        const auto& indexDescription = settings->GetIndexDescription();
        if (indexDescription.GetSettings().layout() != Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
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

        return MakeIntrusive<TDictTableReader>(
            indexDescription.GetSettings().layout(), counters,
            FromProto(info.GetTable()), snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    ui64 GetWordFrequency(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui64>();
            default:
                return 0;
        }
    }
};

enum EReadKind : ui32 {
    EReadKind_Word = 0,
    EReadKind_WordStats = 1,
    EReadKind_DocumentStats = 2,
    EReadKind_Document = 3,
    EReadKind_TotalStats = 4,
};

struct TReadInfo {
    ui64 ReadKind;
    ui64 Cookie;
    ui64 ShardId;
    ui64 LastSeqNo = 0;
};

class TMainTableReader : public TTableReader<TMainTableReader> {
public:
    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    bool MainTableCovered;
    i32 SearchColumnIdx;

    TMainTableReader(
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        const TVector<std::pair<i32, NScheme::TTypeInfo>>& resultCellIndices,
        bool mainTableCovered,
        i32 searchColumnIdx)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
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

        return MakeIntrusive<TMainTableReader>(counters, FromProto(settings->GetTable()),
            snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds, resultCellIndices, mainTableCovered, searchColumnIdx);
    }
};

class TFullTextContainsSource : public TActorBootstrapped<TFullTextContainsSource>, public NYql::NDq::IDqComputeActorAsyncInput, public NActors::IActorExceptionHandler {
private:

    struct TEvPrivate {
        enum EEv {
            EvSchemeCacheRequestTimeout
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
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
    absl::flat_hash_set<ui64> PipesCreated;

    const NKikimrSchemeOp::TFulltextIndexDescription& IndexDescription;

    ui64 ReadBytes = 0;
    ui64 ReadRows = 0;

    bool MainTableCovered = false;

    TString Database;
    TString LogPrefix;
    TDqAsyncStats IngressStats;

    ui64 NextReadId = 0;

    IKqpGateway::TKqpSnapshot Snapshot;

    TActorId SchemeCacheRequestTimeoutTimer;
    TDuration SchemeCacheRequestTimeout;

    TVector<TWordReadState> Words; // Tokenized words from expression
    absl::flat_hash_map<ui64, TReadInfo> Reads;

    absl::flat_hash_map<TConstArrayRef<TCell>, TIntrusivePtr<TDocumentInfo>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> DocumentInfos;

    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    i64 Limit = -1;

    struct TTopKDocumentInfo {
        double Score;
        TIntrusivePtr<TDocumentInfo> DocumentInfo;

        bool operator<(const TTopKDocumentInfo& other) const {
            return Score > other.Score;
        }
    };

    TIntrusivePtr<TQueryCtx> QueryCtx;

    std::priority_queue<
        TTopKDocumentInfo,
        TVector<TTopKDocumentInfo>
    > TopKQueue;

    bool ResolveInProgress = true;
    bool PendingNotify = false;

    i32 SearchColumnIdx = -1;
    TVector<std::function<bool(TStringBuf)>> PostfilterMatchers;
    ui64 ProducedItemsCount = 0;
    std::deque<TIntrusivePtr<TDocumentInfo>> ResultQueue;

    TActorId PipeCacheId;

    ui64 DocCount = 0;
    ui64 SumDocLength = 0;

    TIntrusivePtr<TIndexTableImplReader> IndexTableReader;
    TIntrusivePtr<TMainTableReader> MainTableReader;
    TIntrusivePtr<TDocsTableReader> DocsTableReader;
    TIntrusivePtr<TDictTableReader> DictTableReader;
    TIntrusivePtr<TStatsTableReader> StatsTableReader;
    TDocId::TCompare DocumentKeyCompare;
    TDocId::TEquals DocumentIdPointerEquals;
    TDocId::TKeyGetter KeyGetter;

    std::unique_ptr<IMergeAlgorithm> MergeAlgo;
    // Helper to bind allocator
    TGuard<NMiniKQL::TScopedAlloc> BindAllocator() {
        return TGuard<NMiniKQL::TScopedAlloc>(*Alloc);
    }

    void GeneratePostfilterMatchers(const Ydb::Table::FulltextIndexSettings::Analyzers& analyzers, const TStringBuf query) {
        if (!analyzers.use_filter_ngram() && !analyzers.use_filter_edge_ngram()) {
            return;
        }

        const auto analyzersForQuery = NFulltext::GetAnalyzersForQuery(analyzers);

        for (const TString& queryToken : NFulltext::Analyze(TString(query), analyzersForQuery, '*')) {
            const TString pattern = WildcardToRegex(queryToken);
            TVector<wchar32> ucs4Pattern;
            NPire::NEncodings::Utf8().FromLocal(
                pattern.data(),
                pattern.data() + pattern.size(),
                std::back_inserter(ucs4Pattern));

            auto regex = NPire::TLexer(ucs4Pattern.begin(), ucs4Pattern.end())
                .SetEncoding(NPire::NEncodings::Utf8())
                .Parse().Compile<NPire::TScanner>();

            PostfilterMatchers.push_back([regex=std::move(regex)](const TStringBuf str) {
                return Pire::Matches(regex, str);
            });
        }
    }

    bool ExtractAndTokenizeExpression() {
        YQL_ENSURE(Settings->GetQuerySettings().GetQuery().size() > 0, "Expected non-empty query");

        // Get the first expression (assuming single expression for now)
        const auto& expr = Settings->GetQuerySettings().GetQuery();
        YQL_ENSURE(Settings->GetQuerySettings().GetColumns().size() == 1);

        for(const auto& column : Settings->GetQuerySettings().GetColumns()) {

            for(const auto& analyzer : Settings->GetIndexDescription().GetSettings().columns()) {

                if (analyzer.column() == column.GetName()) {
                    size_t wordIndex = 0;
                    for (const TString& query: NFulltext::BuildSearchTerms(expr, analyzer.analyzers())) {
                        YQL_ENSURE(IndexTableReader);
                        Words.emplace_back(TWordReadState(wordIndex++, query, IndexTableReader));
                    }

                    GeneratePostfilterMatchers(analyzer.analyzers(), expr);
                }
            }
        }

        if (Words.empty()) {
            NotifyCA();
        }

        return !Words.empty();
    }

    template <typename T>
    void InvokeReads(TTableReader<T>* reader, EReadKind readKind, ui64 cookie) {
        while (true) {
            ui64 readId = NextReadId++;
            auto [shardId, request] = reader->GetNextReadRequest(readId);
            if (!request) {
                break;
            }
            SendEvRead(shardId, request);
            Reads[readId] = TReadInfo{.ReadKind = readKind, .Cookie = cookie, .ShardId = shardId};
        }
    }

    void FetchDocumentDetails(TIntrusivePtr<TDocumentInfo> docInfo) {
        if (Limit > 0 && ProducedItemsCount + ResultQueue.size() >= static_cast<ui64>(Limit)) {
            return;
        }

        if (DocsTableReader && !docInfo->HasDocumentLength()) {
            DocsTableReader->StageRangeToRead(TTableRange(docInfo->GetDocumentId()));
            return;
        }

        if (MainTableCovered) {
            YQL_ENSURE(PostfilterMatchers.empty());
            ResultQueue.push_back(docInfo);
            NotifyCA();
            return;
        }

        MainTableReader->StageRangeToRead(TTableRange(docInfo->GetDocumentId()));
    }

    bool ContinueWordRead(TWordReadState& word) {
        ui64 readId = NextReadId++;
        auto [shardId, ev] = word.ScheduleNextRead(readId);
        if (ev) {
            SendEvRead(shardId, ev);
            Reads[readId] = TReadInfo{EReadKind_Word, word.WordIndex, shardId};
            return true;
        }

        return false;
    }

    void EnrichWordInfo(TWordReadState& word) {
        DictTableReader->StageRangeToRead(word.GetWordKeyCells());
        InvokeReads<TDictTableReader>(DictTableReader.Get(), EReadKind_WordStats, word.WordIndex);
    }

    void StartWordReads() {
        TString explain;
        EQueryMode queryMode = QueryModeFromString(Settings->GetQueryMode(), explain);
        if (!explain.empty()) {
            RuntimeError(explain, NYql::NDqProto::StatusIds::BAD_REQUEST);
            return;
        }

        ui32 minimumShouldMatch = MinimumShouldMatchFromString(Words.size(), queryMode, Settings->GetMinimumShouldMatch(), explain);
        if (!explain.empty()) {
            RuntimeError(explain, NYql::NDqProto::StatusIds::BAD_REQUEST);
            return;
        }

        QueryCtx = MakeIntrusive<TQueryCtx>(
            Words.size(), SumDocLength, DocCount, queryMode, minimumShouldMatch, ResultCellIndices);

        bool useArrowFormat = IndexTableReader->GetUseArrowFormat();
        std::vector<std::unique_ptr<ITokenStream>> streams;
        for (size_t i = 0; i < Words.size(); ++i) {
            if (useArrowFormat) {
                streams.emplace_back(std::move(std::make_unique<TArrowTokenStream>(i)));
            } else {
                streams.emplace_back(std::move(std::make_unique<TCellVecTokenStream>(i, IndexTableReader->GetFrequencyColumnIndex())));
            }
        }

        if (queryMode == EQueryMode::And) {
            MergeAlgo = std::make_unique<TAndOptimizedMergeAlgorithm>(
                std::move(streams),
                minimumShouldMatch,
                IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE,
                MainTableReader->GetKeyColumnTypes()
            );
        } else {
            MergeAlgo = std::make_unique<TDefaultMergeAlgorithm>(
                std::move(streams),
                minimumShouldMatch,
                IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE,
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
            if (IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
                EnrichWordInfo(word);
            } else {
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
        if (IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            DictTableReader->AddResolvePartitioningRequest(request);
            DocsTableReader->AddResolvePartitioningRequest(request);
            StatsTableReader->AddResolvePartitioningRequest(request);
        }

        YQL_ENSURE(request->ResultSet.size() >= 1, "Expected at least one table to resolve partitioning");
        ResolveTablePartitioning(std::move(request));
    }

    void ResolveTablePartitioning(std::unique_ptr<NSchemeCache::TSchemeCacheRequest>&& request) {
        auto resolveRequest = std::make_unique<TEvTxProxySchemeCache::TEvResolveKeySet>(request.release());
        Send(MakeSchemeCacheID(), resolveRequest.release());
    }

    void AckRead(ui64 readId) {
        auto readIt = Reads.find(readId);
        if (readIt == Reads.end()) {
            return;
        }
        auto& readInfo = readIt->second;
        ui64 shardId = readInfo.ShardId;
        auto request = GetDefaultReadAckSettings();
        request->Record.SetReadId(readId);
        request->Record.SetSeqNo(readInfo.LastSeqNo);
        Counters->SentIteratorAcks->Inc();

        CA_LOG_D("sending ack for read #" << readId << " seqno = " << readInfo.LastSeqNo);

        bool newPipe = PipesCreated.insert(shardId).second;
        Send(PipeCacheId, new TEvPipeCache::TEvForward(request.Release(), shardId, TEvPipeCache::TEvForwardOptions{
                .AutoConnect = newPipe,
                .Subscribe = newPipe}),
            IEventHandle::FlagTrackDelivery);
    }

    void SendEvRead(ui64 shardId, std::unique_ptr<TEvDataShard::TEvRead>& request) {
        auto readId = request->Record.GetReadId();
        const bool needToCreatePipe = PipesCreated.insert(shardId).second;
        Send(NKikimr::MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(
                request.release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery,
            readId);
    }

public:
    TFullTextContainsSource(const NKikimrKqp::TKqpFullTextSourceSettings* settings,
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
        , IndexTableReader(TIndexTableImplReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , MainTableReader(TMainTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , DocsTableReader(TDocsTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , DictTableReader(TDictTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , StatsTableReader(TStatsTableReader::FromSettings(Counters, Snapshot, LogPrefix, Settings))
        , DocumentKeyCompare(MainTableReader->GetKeyColumnTypes())
        , DocumentIdPointerEquals(MainTableReader->GetKeyColumnTypes())
        , KeyGetter(MainTableReader->GetKeyColumnTypes())
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
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TFullTextContainsSource::StateWork);
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
            TIntrusivePtr<TDocumentInfo> documentInfo = ResultQueue.front();
            ResultQueue.pop_front();
            ProducedItemsCount++;

            auto row = documentInfo->GetRow(QueryCtx.GetRef(), HolderFactory, computeBytes);
            resultBatch.emplace_back(std::move(row));
            ReadBytes += documentInfo->GetRowStorageSize();
            ReadRows++;

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
            for (auto& [id, state] : Reads) {
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
        return Reads.empty() && !ResolveInProgress && ResultQueue.empty() ||
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

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        // implement retry logic a bit later
        CA_LOG_D("TEvDeliveryProblem was received");
        RuntimeError("Read result status is not success", NYql::NDqProto::StatusIds::UNAVAILABLE);
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
        if (IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(resultSet.size() == 5, "Expected 5 tables for flat relevance index");
            DictTableReader->SetPartitionInfo(resultSet[2].KeyDescription);
            DocsTableReader->SetPartitionInfo(resultSet[3].KeyDescription);
            StatsTableReader->SetPartitionInfo(resultSet[4].KeyDescription);
            if (ExtractAndTokenizeExpression()) {
                ReadTotalStats();
            }
        } else {
            if (ExtractAndTokenizeExpression()) {
                StartWordReads();
            }
        }
    }

    void ReadTotalStats() {
        ui64 readId = NextReadId++;
        auto [shardId, request] = StatsTableReader->GetTotalStatsRequest(readId);
        Reads[readId] = TReadInfo{.ReadKind = EReadKind_TotalStats, .Cookie = readId, .ShardId = shardId};
        SendEvRead(shardId, request);
    }

    bool Postfilter(const TDocumentInfo& documentInfo) const {
        auto analyzers = IndexDescription.GetSettings().columns(0).analyzers();
        // Prevent splitting tokens into ngrams
        analyzers.set_use_filter_ngram(false);
        analyzers.set_use_filter_edge_ngram(false);

        for (const auto& matcher : PostfilterMatchers) {
            YQL_ENSURE(SearchColumnIdx != -1);
            const TString searchColumnValue(documentInfo.GetResultCell(SearchColumnIdx).AsBuf()); // TODO: don't copy

            bool found = false;
            for (const auto& valueToken : NFulltext::Analyze(searchColumnValue, analyzers)) {
                if (matcher(valueToken)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                return false;
            }
        }

        return true;
    }

    void DocumentDetailsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            const auto& doc = DocumentInfos.at(GetDocumentId(row));
            doc->AddRow(row);
            if (PostfilterMatchers.empty() || Postfilter(*doc)) {
                ResultQueue.push_back(doc);
            }

            if (Limit > 0 && ProducedItemsCount + ResultQueue.size() >= static_cast<ui64>(Limit)) {
                NotifyCA();
                return;
            }
        }

        // Notify about possibly empty result
        NotifyCA();
    }

    void ProcessTopKQueue() {
        while (!TopKQueue.empty()) {
            auto [score, documentInfo] = TopKQueue.top();
            TopKQueue.pop();
            FetchDocumentDetails(documentInfo);
        }
    }

    TConstArrayRef<TCell> GetDocumentId(const TConstArrayRef<TCell>& row) const {
        return row.subspan(0, DocumentKeyCompare.DocumentKeyColumnTypes.size());
    }

    void DocumentStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            auto& doc = DocumentInfos.at(GetDocumentId(row));
            doc->SetDocumentLength(DocsTableReader->GetDocumentLength(row));

            if (Limit > 0) {
                TopKQueue.push({doc->GetBM25Score(QueryCtx.GetRef()), doc});
                if (TopKQueue.size() > (size_t)Limit) {
                    TopKQueue.pop();
                }

            } else {
                FetchDocumentDetails(doc);
            }
        }

        if (Reads.empty()) {
            ProcessTopKQueue();
        }

        InvokeReads<TMainTableReader>(MainTableReader.Get(), EReadKind_Document, 0);
    }

    void HandleTotalStatsResult(TEvDataShard::TEvReadResult& msg) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            DocCount = StatsTableReader->GetDocCount(row);
            SumDocLength = StatsTableReader->GetSumDocLength(row);
        }

        StartWordReads();
    }

    void WordStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 wordIndex) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            auto& word = Words[wordIndex];
            word.Frequency = DictTableReader->GetWordFrequency(row);
            QueryCtx->AddIDFValue(wordIndex, word.Frequency);
            ContinueWordRead(word);
        }
    }

    void WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult> msg, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& incomingWordInfo = Words[wordIndex];

        MergeAlgo->AddResult(wordIndex, std::move(msg));

        if (finished) {
            if (!ContinueWordRead(incomingWordInfo)) {
                MergeAlgo->FinishTokenStream(wordIndex);
            }
        }

        std::vector<IMergeAlgorithm::TMatch> matches = MergeAlgo->FindMatches();

        for(auto& match : matches) {
            auto doc = MakeIntrusive<TDocumentInfo>(std::move(KeyGetter(match.DocId)));
            if (!match.Frequencies.empty()) {
                doc->SetWordFrequencies(std::move(match.Frequencies));
            }

            DocumentInfos.emplace(doc->GetDocumentId(), doc);

            FetchDocumentDetails(doc);
        }

        InvokeReads<TMainTableReader>(MainTableReader.Get(), EReadKind_Document, 0);
        if (DocsTableReader) {
            InvokeReads<TDocsTableReader>(DocsTableReader.Get(), EReadKind_DocumentStats, 0);
        }

        NotifyCA();
    }

    void HandleReadResult(TEvDataShard::TEvReadResult::TPtr& ev) {
        ui64 readId = ev->Get()->Record.GetReadId();
        auto& record = ev->Get()->Record;

        auto it = Reads.find(readId);
        if (it == Reads.end()) {
            return;
        }

        CA_LOG_E("Recv TEvReadResult (full text source)"
            << ", Cookie=" << it->second.Cookie
            << ", ReadKind=" << (ui32)it->second.ReadKind
            << ", ShardId=" << it->second.ShardId
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
        ui64 cookie = it->second.Cookie;
        auto readKind = it->second.ReadKind;
        YQL_ENSURE(it->second.LastSeqNo < record.GetSeqNo());
        it->second.LastSeqNo = record.GetSeqNo();

        if (record.GetFinished()) {
            Reads.erase(readId);
        } else {
            AckRead(readId);
        }

        switch (readKind) {
            case EReadKind_Document:
                DocumentDetailsResult(msg, cookie);
                break;
            case EReadKind_DocumentStats:
                DocumentStatsResult(msg);
                break;
            case EReadKind_WordStats:
                WordStatsResult(msg, cookie);
                break;
            case EReadKind_Word:
                WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult>(ev->Release().Release()), cookie, record.GetFinished());
                break;
            case EReadKind_TotalStats:
                HandleTotalStatsResult(msg);
                break;
        }
    }

private:
    using TBase = TActorBootstrapped<TFullTextContainsSource>;
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
    auto* actor = new TFullTextContainsSource(settings, arena, computeActorId, inputIndex, statsLevel, txId, taskId, typeEnv, holderFactory, alloc, traceId, counters);
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
}
