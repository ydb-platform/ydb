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
using TDocumentId = const TConstArrayRef<TCell>;

// replace with parameters from settings
constexpr double K1_FACTOR_DEFAULT = 1.2;
constexpr double B_FACTOR_DEFAULT = 0.75;
constexpr double EPSILON = 1e-6;
constexpr i32 RELEVANCE_COLUMN_MARKER = -1;

class TDocumentIdPointer;

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

        record.SetMaxRows(MaxRowsDefaultQuota * 100);
        record.SetMaxBytes(MaxBytesDefaultQuota);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        CA_LOG_E(TStringBuilder() << "Send EvRead (stream lookup) to shardId=" << shardId
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

        auto& [shardId, ranges] = *RangesToRead.begin();
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
    friend class TDocumentIdPointer;

    TIntrusivePtr<TQueryCtx> QueryCtx;
    TOwnedCellVec KeyCells;
    TOwnedCellVec RowCells;
    TDocumentId DocumentId;
    std::vector<ui64> WordFrequencies;
    ui64 DocumentLength = std::numeric_limits<ui64>::max();
public:

    TDocumentInfo(TIntrusivePtr<TQueryCtx> queryCtx, TConstArrayRef<TCell> keyCells)
        : QueryCtx(queryCtx)
        , KeyCells(std::move(keyCells))
        , DocumentId(KeyCells)
    {}

    void SetWordFrequencies(std::vector<ui64>&& wordFrequencies) {
        WordFrequencies = std::move(wordFrequencies);
    }

    void SetDocumentLength(ui64 documentLength) {
        DocumentLength = documentLength;
    }

    bool HasDocumentLength() const {
        return DocumentLength != std::numeric_limits<ui64>::max();
    }

    double GetBM25Score() const {
        double score = 0;
        const double avgDocLength = QueryCtx->GetAvgDL();
        const double k1Factor = QueryCtx->GetK1Factor();
        const double bFactor = QueryCtx->GetBFactor();
        const double documentFactor = k1Factor * (1 - bFactor + bFactor * static_cast<double>(DocumentLength) / avgDocLength);
        for(size_t i = 0; i < WordFrequencies.size(); ++i) {
            double docFreq = static_cast<double>(WordFrequencies[i]);
            double idf = QueryCtx->GetIDFValue(i);
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

    NUdf::TUnboxedValue GetRow(const NKikimr::NMiniKQL::THolderFactory& holderFactory, i64& computeBytes) const {
        NUdf::TUnboxedValue* rowItems = nullptr;
        auto row = holderFactory.CreateDirectArrayHolder(
            QueryCtx->GetResultCellIndices().size(), rowItems);

        for(size_t i = 0; i < QueryCtx->GetResultCellIndices().size(); ++i) {
            const auto& [cellIndex, cellType] = QueryCtx->GetResultCellIndices()[i];
            if (cellIndex == RELEVANCE_COLUMN_MARKER) {
                double score = GetBM25Score();
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

class TDocumentIdPointer {
public:
    size_t WordIndex;
    const TCell* Document;

    explicit TDocumentIdPointer(size_t wordIndex, const TCell* document)
        : WordIndex(wordIndex)
        , Document(document)
    {}
};

struct TDocumentKeyCompare {
    TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

    TDocumentKeyCompare(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
        : DocumentKeyColumnTypes(documentKeyColumnTypes)
    {}

    bool operator()(const TDocumentIdPointer& key1, const TDocumentIdPointer& key2) const {
        return CompareTypedCellVectors(key1.Document, key2.Document, DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size()) > 0;
    }
};

struct TDocumentIdPointerEquals {
public:
    TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

    TDocumentIdPointerEquals(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
        : DocumentKeyColumnTypes(documentKeyColumnTypes)
    {}

    bool operator()(const TDocumentIdPointer& key1, const TDocumentIdPointer& key2) const {
        return CompareTypedCellVectors(key1.Document, key2.Document, DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size()) == 0;
    }
};

class TWordReadState {
public:
    ui64 WordIndex;
    TString Word;
    bool PendingRead = false;
    std::deque<std::unique_ptr<TEvDataShard::TEvReadResult>> PendingReadResults;
    // pending ranges
    TIntrusivePtr<TTableReader<TIndexTableImplReader>> Reader;
    std::deque<std::pair<ui64, TOwnedTableRange>> RangesToRead;
    ui32 Frequency = 0;
    ui32 UnprocessedDocumentPos = 0;

    explicit TWordReadState(ui64 wordIndex, const TString& word, const TIntrusivePtr<TTableReader<TIndexTableImplReader>>& reader)
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
        PendingRead = true;
        return std::make_pair(shardId, Reader->GetReadRequest(readId, shardId, range));
    }

    bool FinishedRead() const {
        return RangesToRead.empty() && !PendingRead && PendingReadResults.empty();
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> ScheduleNextRead(ui64 readId) {
        if (!RangesToRead.empty()) {
            return BuildNextRangeToRead(readId);
        }

        PendingRead = false;
        return std::make_pair(0, nullptr);
    }

    TDocumentIdPointer GetNextDocumentIdPointer() const {
        YQL_ENSURE(!PendingReadResults.empty());
        auto& result = PendingReadResults.front();
        return TDocumentIdPointer(WordIndex, result->GetCells(UnprocessedDocumentPos).data());
    }

    void BuildRangesToRead() {
        TCell tokenCell(Word.data(), Word.size());
        std::vector <TCell> fromCells(Reader->GetKeyColumnTypes().size() - 1);
        fromCells.insert(fromCells.begin(), tokenCell);

        std::vector <TCell> toCells;
        toCells.insert(toCells.begin(), tokenCell);

        bool fromInclusive = true;
        bool toInclusive = false;
        auto range = TTableRange(fromCells, fromInclusive, toCells, toInclusive);

        auto rangePartition = Reader->GetRangePartitioning(range);
        for(const auto& [shardId, range] : rangePartition) {
            RangesToRead.emplace_back(shardId, range);
        }
    }
};


class TIndexTableImplReader : public TTableReader<TIndexTableImplReader> {
    Ydb::Table::FulltextIndexSettings::Layout Layout;

public:
    TIndexTableImplReader(const Ydb::Table::FulltextIndexSettings::Layout& layout, const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, snapshot, logPrefix, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , Layout(layout)
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

        if (indexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(freqColumnIndex != -1);
            resultColumnTypes.push_back(freqColumnType);
            resultColumnIds.push_back(freqColumnIndex);
        }

        return MakeIntrusive<TIndexTableImplReader>(
            indexDescription.GetSettings().layout(), counters, FromProto(info.GetTable()), snapshot, logPrefix,
            keyColumnTypes, resultColumnTypes, resultColumnIds);
    }

    ui32 GetFrequency(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui32>();
            default:
                return 1;
        }
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

    absl::flat_hash_map<TDocumentId, TIntrusivePtr<TDocumentInfo>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> DocumentInfos;

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
    ui64 FinishedWords = 0;

    TActorId PipeCacheId;

    ui64 DocCount = 0;
    ui64 SumDocLength = 0;

    TIntrusivePtr<TIndexTableImplReader> IndexTableReader;
    TIntrusivePtr<TMainTableReader> MainTableReader;
    TIntrusivePtr<TDocsTableReader> DocsTableReader;
    TIntrusivePtr<TDictTableReader> DictTableReader;
    TIntrusivePtr<TStatsTableReader> StatsTableReader;
    TDocumentKeyCompare DocumentKeyCompare;
    TDocumentIdPointerEquals DocumentIdPointerEquals;
    std::priority_queue<TDocumentIdPointer, TStackVec<TDocumentIdPointer, 64>, TDocumentKeyCompare> MergeQueue;

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

            for(const auto& analyzer : IndexDescription.GetSettings().columns()) {

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

    void ContinueWordRead(TWordReadState& word) {
        ui64 readId = NextReadId++;
        auto [shardId, ev] = word.ScheduleNextRead(readId);
        if (ev) {
            SendEvRead(shardId, ev);
            Reads[readId] = TReadInfo{EReadKind_Word, word.WordIndex, shardId};
        }
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

    void AckRead(ui64 readId, ui64 seqno) {
        auto& readInfo = Reads[readId];
        ui64 shardId = readInfo.ShardId;
        auto request = GetDefaultReadAckSettings();
        request->Record.SetReadId(readId);
        request->Record.SetSeqNo(seqno);
        Counters->SentIteratorAcks->Inc();

        CA_LOG_D("sending ack for read #" << readId << " seqno = " << seqno);

        bool newPipe = PipesCreated.insert(shardId).second;
        Send(PipeCacheId, new TEvPipeCache::TEvForward(request.Release(), shardId, TEvPipeCache::TEvForwardOptions{
                .AutoConnect = newPipe,
                .Subscribe = newPipe}),
            IEventHandle::FlagTrackDelivery);
    }

    void SendEvRead(ui64 shardId, std::unique_ptr<TEvDataShard::TEvRead>& request) {
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
            0);
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
        // , TypeEnv(typeEnv)
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
        , MergeQueue(DocumentKeyCompare)
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

            auto row = documentInfo->GetRow(HolderFactory, computeBytes);
            resultBatch.emplace_back(std::move(row));
            ReadBytes += documentInfo->GetRowStorageSize();
            ReadRows++;

            if (Limit > 0 && ProducedItemsCount >= static_cast<ui64>(Limit)) {
                finished = true;
                break;
            }

            if (computeBytes > freeSpace) {
                break;
            }
        }

        if (!ResultQueue.empty()) {
            NotifyCA();
        }

        finished = Reads.empty() && !ResolveInProgress && ResultQueue.empty();
        return computeBytes;
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    void LoadState(const TSourceState&) override {}

    void PassAway() override {
        TBase::PassAway();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode) {
        NYql::TIssues issues;
        issues.AddIssue(NYql::TIssue(message));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

    void NotifyCA() {
        bool finished = Reads.empty() && !ResolveInProgress && ResultQueue.empty();
        if (!PendingNotify && (finished || !ResultQueue.empty())) {
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
                return;
            }
        }

        NotifyCA();
    }

    void ProcessTopKQueue() {
        while (!TopKQueue.empty()) {
            auto [score, documentInfo] = TopKQueue.top();
            TopKQueue.pop();
            FetchDocumentDetails(documentInfo);
        }
    }

    TDocumentId GetDocumentId(const TConstArrayRef<TCell>& row) const {
        return row.subspan(0, DocumentKeyCompare.DocumentKeyColumnTypes.size());
    }

    void DocumentStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            auto& doc = DocumentInfos.at(GetDocumentId(row));
            doc->SetDocumentLength(DocsTableReader->GetDocumentLength(row));

            if (Limit > 0) {
                TopKQueue.push({doc->GetBM25Score(), doc});
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
        if (msg->GetRowsCount() > 0) {
            incomingWordInfo.PendingReadResults.emplace_back(std::move(msg));
            if (incomingWordInfo.PendingReadResults.size() == 1) {
                MergeQueue.push(incomingWordInfo.GetNextDocumentIdPointer());
            }
        }

        if (finished) {
            ContinueWordRead(incomingWordInfo);
            // if no pending reads and everithing is already merged and
            // processed then word is finished.
            if (incomingWordInfo.FinishedRead()) {
                FinishedWords++;
            }
        }

        TStackVec<ui32, 64> matchedWords;
        const size_t minShouldMatch = QueryCtx->GetMinimumShouldMatch();
        while(!MergeQueue.empty() && MergeQueue.size() + FinishedWords == Words.size()) {
            if (MergeQueue.size() < minShouldMatch) {
                break;
            }

            TDocumentIdPointer documentIdPointer = std::move(MergeQueue.top());
            matchedWords.clear();
            matchedWords.push_back(documentIdPointer.WordIndex);

            MergeQueue.pop();
            ui32 matchCount = 1;
            while(!MergeQueue.empty() && DocumentIdPointerEquals(documentIdPointer, MergeQueue.top())) {
                matchCount++;
                matchedWords.push_back(MergeQueue.top().WordIndex);
                MergeQueue.pop();
            }

            if (matchCount >= minShouldMatch) {
                auto docInfo = MakeIntrusive<TDocumentInfo>(QueryCtx, TConstArrayRef<TCell>(documentIdPointer.Document, DocumentKeyCompare.DocumentKeyColumnTypes.size()));
                auto [it, inserted] = DocumentInfos.emplace(docInfo->GetDocumentId(), std::move(docInfo));
                YQL_ENSURE(inserted);
                if (DocsTableReader) {
                    std::vector<ui64> wordFrequencies(Words.size(), 0);
                    for (auto wordIndex : matchedWords) {
                        auto& wordInfo = Words[wordIndex];
                        const auto& payload = wordInfo.PendingReadResults.front()->GetCells(wordInfo.UnprocessedDocumentPos);
                        wordFrequencies[wordIndex] = IndexTableReader->GetFrequency(payload);
                    }
                    it->second->SetWordFrequencies(std::move(wordFrequencies));
                }

                FetchDocumentDetails(it->second);
            }

            for (auto wordIndex : matchedWords) {
                auto& wordInfo = Words[wordIndex];
                wordInfo.UnprocessedDocumentPos++;
                if (wordInfo.UnprocessedDocumentPos == wordInfo.PendingReadResults.front()->GetRowsCount()) {
                    wordInfo.PendingReadResults.pop_front();
                    wordInfo.UnprocessedDocumentPos = 0;
                }

                if (!wordInfo.PendingReadResults.empty()) {
                    MergeQueue.push(wordInfo.GetNextDocumentIdPointer());
                }

                if (wordInfo.FinishedRead()) {
                    FinishedWords++;
                }
            }
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

        if (record.GetFinished()) {
            Reads.erase(readId);
        } else {
            AckRead(readId, record.GetSeqNo());
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
