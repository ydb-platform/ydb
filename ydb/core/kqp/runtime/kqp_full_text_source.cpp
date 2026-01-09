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

class TTableReader : public TAtomicRefCount<TTableReader> {
    TIntrusivePtr<TKqpCounters> Counters;
    TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;
    TString LogPrefix;

    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;
    TVector<i32> ResultColumnIds;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> PartitionInfo;

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

    std::unique_ptr<TEvDataShard::TEvRead> GetReadRequest(ui64 readId, ui64 shardId, const TOwnedTableRange& range) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (size_t i = 0; i < ResultColumnIds.size(); i++) {
            record.AddColumns(ResultColumnIds[i]);
        }

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

    std::vector<std::pair<ui64, TOwnedTableRange>> GetRangePartitioning(const TOwnedTableRange& range) {

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

        std::vector<std::pair<ui64, TOwnedTableRange>> rangePartition;
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
    const TVector<i32> ResultCellIndices;

public:
    TQueryCtx(size_t wordCount, ui64 totalDocLength, ui64 docCount, const TVector<i32>& resultCellIndices)
        : DocCount(docCount)
        , AvgDL(docCount > 0 ? static_cast<double>(totalDocLength) / docCount : 1.0)
        , IDFValues(wordCount, 0.0)
        , ResultCellIndices(resultCellIndices)
    {
    }

    void SetBFactor(double bFactor) {
        BFactor = bFactor;
    }

    void SetK1Factor(double k1Factor) {
        K1Factor = k1Factor;
    }

    const TVector<i32>& GetResultCellIndices() const {
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

    double GetAvgDL() const {
        return AvgDL;
    }
};

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
    const TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;
    TIntrusivePtr<TTableReader> MainTableReader;
    TIntrusivePtr<TTableReader> DocsTableReader;
    TDocumentId DocumentId;
    std::vector<ui64> ContainingWords;
    size_t NumContainingWords = 0;
    ui64 DocumentLength = 0;


public:
    ui64 DocumentNumId = 0;

    TDocumentInfo(TIntrusivePtr<TQueryCtx> queryCtx, TOwnedCellVec&& keyCells,
        const TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes, TIntrusivePtr<TTableReader> mainTableReader,
        TIntrusivePtr<TTableReader> docsTableReader)
        : QueryCtx(queryCtx)
        , KeyCells(std::move(keyCells))
        , DocumentKeyColumnTypes(documentKeyColumnTypes)
        , MainTableReader(mainTableReader)
        , DocsTableReader(docsTableReader)
        , DocumentId(KeyCells)
        , ContainingWords(QueryCtx->GetWordCount())
    {}

    bool AllWordsContained() const {
        return NumContainingWords == ContainingWords.size();
    }

    void AddContainingWord(size_t wordIndex, ui64 docFreq) {
        if (!ContainingWords[wordIndex]) {
            ContainingWords[wordIndex] = docFreq;
            NumContainingWords++;
        }
    }

    void SetDocumentLength(ui64 documentLength) {
        DocumentLength = documentLength;
    }

    const TConstArrayRef<NScheme::TTypeInfo> GetDocumentKeyColumnTypes() const {
        return DocumentKeyColumnTypes;
    }

    double GetBM25Score() const {
        double score = 0;
        const double avgDocLength = QueryCtx->GetAvgDL();
        const double k1Factor = QueryCtx->GetK1Factor();
        const double bFactor = QueryCtx->GetBFactor();
        const double documentFactor = k1Factor * (1 - bFactor + bFactor * static_cast<double>(DocumentLength) / avgDocLength);
        for(size_t i = 0; i < ContainingWords.size(); ++i) {
            double docFreq = static_cast<double>(ContainingWords[i]);
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

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> GetReadDocumentStatsRequest(ui64 readId) {
        auto point = TOwnedTableRange(DocumentId);
        YQL_ENSURE(point.Point);
        auto requests = DocsTableReader->GetRangePartitioning(point);
        YQL_ENSURE(requests.size() == 1);
        auto [shardId, range] = requests[0];
        return std::make_pair(shardId, DocsTableReader->GetReadRequest(readId, shardId, range));
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> GetReadRequest(ui64 readId) {
        auto point = TOwnedTableRange(DocumentId);
        YQL_ENSURE(point.Point);
        auto requests = MainTableReader->GetRangePartitioning(point);
        YQL_ENSURE(requests.size() == 1);
        auto [shardId, range] = requests[0];
        return std::make_pair(shardId, MainTableReader->GetReadRequest(readId, shardId, range));
    }

    NUdf::TUnboxedValue GetRow(const NKikimr::NMiniKQL::THolderFactory& holderFactory, i64& computeBytes) const {
        NUdf::TUnboxedValue* rowItems = nullptr;
        const auto& resultRowTypes = MainTableReader->GetResultColumnTypes();
        auto row = holderFactory.CreateDirectArrayHolder(
            QueryCtx->GetResultCellIndices().size(), rowItems);

        for(size_t i = 0; i < QueryCtx->GetResultCellIndices().size(); ++i) {
            const auto cellIndex = QueryCtx->GetResultCellIndices()[i];
            if (cellIndex == RELEVANCE_COLUMN_MARKER) {
                double score = GetBM25Score();
                rowItems[i] = NUdf::TUnboxedValuePod(score);
                computeBytes += 8;
                continue;
            }

            if (cellIndex < (i32)KeyCells.size()) {
                rowItems[i] = NMiniKQL::GetCellValue(KeyCells[cellIndex], resultRowTypes[cellIndex]);
                computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], resultRowTypes[cellIndex]).AllocatedBytes;
                continue;
            }

            rowItems[i] = NMiniKQL::GetCellValue(RowCells[cellIndex], resultRowTypes[cellIndex]);
            computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], resultRowTypes[cellIndex]).AllocatedBytes;
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

    TDocumentId GetDocumentId() const {
        return DocumentId;
    }
};

class TDocumentIdPointer {
public:
    bool Finished = false;
    size_t WordIndex;
    TIntrusivePtr<TDocumentInfo> DocumentInfo;

    explicit TDocumentIdPointer(bool finished, size_t wordIndex, TIntrusivePtr<TDocumentInfo> documentInfo)
        : Finished(finished)
        , WordIndex(wordIndex)
        , DocumentInfo(documentInfo)
    {
    }

    bool operator<(const TDocumentIdPointer& other) const {
        if (Finished != other.Finished) {
            return Finished < other.Finished;
        }

        YQL_ENSURE(Finished == other.Finished);
        if (Finished) {
            return false;
        }

        YQL_ENSURE(DocumentInfo);
        YQL_ENSURE(other.DocumentInfo);

        int cmp = CompareTypedCellVectors(
            DocumentInfo->GetDocumentId().data(), other.DocumentInfo->GetDocumentId().data(),
            other.DocumentInfo->GetDocumentKeyColumnTypes().data(),
            other.DocumentInfo->GetDocumentKeyColumnTypes().size());
        return cmp > 0;
    }
};

class TWordReadState {
public:
    ui64 WordIndex;
    TString Word;
    bool PendingRead = false;
    std::deque<TIntrusivePtr<TDocumentInfo>> PendingDocuments;
    // pending ranges
    TIntrusivePtr<TTableReader> Reader;
    std::deque<std::pair<ui64, TOwnedTableRange>> RangesToRead;
    ui32 Frequency = 0;

    explicit TWordReadState(ui64 wordIndex, const TString& word, const TIntrusivePtr<TTableReader>& reader)
        : WordIndex(wordIndex)
        , Word(word)
        , Reader(reader)
    {
        BuildRangesToRead();
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> BuildNextRangeToRead(ui64 readId) {
        YQL_ENSURE(!RangesToRead.empty());
        auto [shardId, range] = RangesToRead.front();
        RangesToRead.pop_front();
        PendingRead = true;
        return std::make_pair(shardId, Reader->GetReadRequest(readId, shardId, range));
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> ScheduleNextRead(ui64 readId) {
        if (!RangesToRead.empty()) {
            return BuildNextRangeToRead(readId);
        }

        PendingRead = false;
        return std::make_pair(0, nullptr);
    }

    void BuildRangesToRead() {
        TCell tokenCell(Word.data(), Word.size());
        std::vector <TCell> fromCells(Reader->GetKeyColumnTypes().size() - 1);
        fromCells.insert(fromCells.begin(), tokenCell);

        std::vector <TCell> toCells;
        toCells.insert(toCells.begin(), tokenCell);

        bool fromInclusive = true;
        bool toInclusive = false;
        auto range = TOwnedTableRange(fromCells, fromInclusive, toCells, toInclusive);

        auto rangePartition = Reader->GetRangePartitioning(range);
        for(const auto& [shardId, range] : rangePartition) {
            RangesToRead.emplace_back(shardId, range);
        }
    }

    bool HasDocumentIdPointer() const {
        if (PendingDocuments.empty() && RangesToRead.empty() && !PendingRead) {
            return true;
        }

        return !PendingDocuments.empty();
    }

    TDocumentIdPointer GetDocumentIdPointer() const {
        if (PendingDocuments.empty() && RangesToRead.empty() && !PendingRead) {
            return TDocumentIdPointer(true, WordIndex, nullptr);
        }

        YQL_ENSURE(!PendingDocuments.empty());
        return TDocumentIdPointer(false, WordIndex, PendingDocuments.front());
    }
};

class TIndexTableImplReader : public TTableReader {
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

    static TIntrusivePtr<TIndexTableImplReader> FromNavigateRequest(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrSchemeOp::TFulltextIndexDescription& indexDescription,
        const TTableId& tableId,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& columns)
    {

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
        TVector<NScheme::TTypeInfo> resultKeyColumnTypes;
        TVector<i32> resultKeyColumnIds;

        for (const auto& keyColumn : keyColumns) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                keyColumn.GetTypeId(), keyColumn.GetTypeInfo()));

            if (keyColumn.GetName() == TokenColumn) {
                // dont request token column because it's not a part of document id
                continue;
            }

            resultKeyColumnTypes.push_back(keyColumnTypes.back());
            resultKeyColumnIds.push_back(keyColumn.GetId());
        }

        if (indexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(freqColumnIndex != -1);
            resultKeyColumnTypes.push_back(freqColumnType);
            resultKeyColumnIds.push_back(freqColumnIndex);
        }

        return MakeIntrusive<TIndexTableImplReader>(
            indexDescription.GetSettings().layout(), counters, tableId, snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    const TConstArrayRef<TCell> GetDocumentId(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row.subspan(0, GetResultColumnTypes().size() - 1);
            default:
                return row;
        }
    }

    const TConstArrayRef<NScheme::TTypeInfo> GetDocumentKeyColumnTypes() const {
        const auto returnColumnTypes = GetResultColumnTypes();
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return returnColumnTypes.subspan(0, returnColumnTypes.size() - 1);
            default:
                return returnColumnTypes;
        }
    }

    ui32 GetFrequency(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui32>();
            default:
                return 1;
        }
    }

    std::pair<ui32, TIntrusivePtr<TDocumentInfo>> BuildDocumentInfo(TIntrusivePtr<TQueryCtx> queryCtx, TIntrusivePtr<TTableReader> mainTableReader, TIntrusivePtr<TTableReader> docsTableReader, const TConstArrayRef<TCell>& row) {
        if (Layout == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(row.size() == GetResultColumnTypes().size());
            // at least it contains document id (which is at least 1 column) and frequency (which is at least 1 column)
            YQL_ENSURE(row.size() >= 2);
        }

        TConstArrayRef<TCell> docId = GetDocumentId( row);
        ui32 freq = GetFrequency(row);
        TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes = GetDocumentKeyColumnTypes();
        auto docInfo = MakeIntrusive<TDocumentInfo>(
            std::move(queryCtx), TOwnedCellVec(docId), documentKeyColumnTypes, mainTableReader, docsTableReader);

        return std::make_pair(freq, std::move(docInfo));
    }
};

class TDocsTableReader : public TTableReader {
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

    static TIntrusivePtr<TDocsTableReader> FromNavigateRequest(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrSchemeOp::TFulltextIndexDescription& indexDescription,
        const TTableId& tableId,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& columns)
    {
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
            tableId, snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    ui64 GetDocumentLength(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui32>();
            default:
                return 0;
        }
    }

    const TConstArrayRef<TCell> GetDocumentId(const TConstArrayRef<TCell>& row) const {
        return row.subspan(0, GetResultColumnTypes().size() - 1);
    }
};

class TStatsTableReader : public TTableReader {
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

    static TIntrusivePtr<TStatsTableReader> FromNavigateRequest(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrSchemeOp::TFulltextIndexDescription& indexDescription,
        const TTableId& tableId,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& columns)
    {
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
            tableId, snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
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
        auto tcellVector = TOwnedTableRange(fromCells, fromInclusive, toCells, toInclusive);

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

class TDictTableReader : public TTableReader {
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

    static TIntrusivePtr<TDictTableReader> FromNavigateRequest(
        const TIntrusivePtr<TKqpCounters>& counters,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const NKikimrSchemeOp::TFulltextIndexDescription& indexDescription,
        const TTableId& tableId,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TKqpColumnMetadataProto>& columns)
    {
        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultKeyColumnTypes;
        TVector<i32> resultKeyColumnIds;

        i32 freqColumnIndex = -1;
        NScheme::TTypeInfo freqColumnType;
        for (const auto& column :columns) {
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
            tableId, snapshot, logPrefix, keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    std::pair<ui64, std::unique_ptr<TEvDataShard::TEvRead>> GetWordReadRequest(ui64 readId, TString token) {
        TVector<TCell> keyCells;
        TCell cell(token.data(), token.size());
        keyCells.insert(keyCells.end(), cell);
        TOwnedTableRange range(keyCells);
        YQL_ENSURE(range.Point);
        auto points = GetRangePartitioning(range);
        YQL_ENSURE(points.size() == 1);
        auto [shardId, point] = points[0];
        return std::make_pair(shardId, GetReadRequest(readId, shardId, point));
    }

    ui64 GetWordFrequency(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return row[GetResultColumnTypes().size() - 1].AsValue<ui64>();
            default:
                return 0;
        }
    }

    TString GetToken(const TConstArrayRef<TCell>& row) const {
        switch (Layout) {
            case Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE:
                return TString(row[GetResultColumnTypes().size() - 1].AsBuf());
            default:
                return "";
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

class TFullTextContainsSource : public TActorBootstrapped<TFullTextContainsSource>, public NYql::NDq::IDqComputeActorAsyncInput {
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

    ui64 DocumentNumId = 0;
    ui64 NextReadId = 0;

    TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;

    TActorId SchemeCacheRequestTimeoutTimer;
    TDuration SchemeCacheRequestTimeout;

    TIntrusivePtr<TIndexTableImplReader> IndexTableReader;
    TIntrusivePtr<TTableReader> MainTableReader;
    TIntrusivePtr<TDocsTableReader> DocsTableReader;
    TIntrusivePtr<TDictTableReader> DictTableReader;
    TIntrusivePtr<TStatsTableReader> StatsTableReader;

    TVector<TWordReadState> Words; // Tokenized words from expression
    absl::flat_hash_map<ui64, TReadInfo> Reads;

    absl::flat_hash_map<TDocumentId, TIntrusivePtr<TDocumentInfo>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> DocumentInfos;

    TVector<i32> ResultCellIndices;
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

    std::priority_queue<TDocumentIdPointer, TVector<TDocumentIdPointer>> MergeQueue;
    std::deque<TIntrusivePtr<TDocumentInfo>> ResultQueue;
    TActorId PipeCacheId;

    ui64 DocCount = 0;
    ui64 SumDocLength = 0;

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

    void ExtractAndTokenizeExpression() {
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
    }

    void FetchDocumentStats(TIntrusivePtr<TDocumentInfo> docInfo) {
        ui64 readId = NextReadId++;
        auto [shardId, request] = docInfo->GetReadDocumentStatsRequest(readId);
        SendEvRead(shardId, request);
        Reads[readId] = TReadInfo{EReadKind_DocumentStats, docInfo->DocumentNumId, shardId};
    }

    void FetchDocumentDetails(TIntrusivePtr<TDocumentInfo> docInfo) {
        if (MainTableCovered) {
            YQL_ENSURE(PostfilterMatchers.empty());
            ResultQueue.push_back(docInfo);
            NotifyCA();
            return;
        }

        ui64 readId = NextReadId++;
        auto [shardId, request] = docInfo->GetReadRequest(readId);
        SendEvRead(shardId, request);
        Reads[readId] = TReadInfo{EReadKind_Document, docInfo->DocumentNumId, shardId};
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
        ui64 readId = NextReadId++;
        auto [shardId, ev] = DictTableReader->GetWordReadRequest(readId, word.Word);
        if (ev) {
            SendEvRead(shardId, ev);
            Reads[readId] = TReadInfo{EReadKind_WordStats, word.WordIndex, shardId};
        }
    }

    void StartWordReads() {
        QueryCtx = MakeIntrusive<TQueryCtx>(
            Words.size(), SumDocLength, DocCount, ResultCellIndices);

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
        TVector<NScheme::TTypeInfo> keyColumnTypes;
        TVector<NScheme::TTypeInfo> resultColumnTypes;
        TVector<i32> resultColumnIds;

        YQL_ENSURE(Settings->GetQuerySettings().ColumnsSize() == 1);
        const TStringBuf searchColumnName = Settings->GetQuerySettings().GetColumns(0).GetName();

        THashMap<TString, i32> keyColumns;
        for (const auto& column : Settings->GetKeyColumns()) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            if (column.GetName() == searchColumnName) {
                SearchColumnIdx = resultColumnIds.size();
            }

            resultColumnTypes.push_back(keyColumnTypes.back());
            keyColumns.insert({column.GetName(), resultColumnIds.size()});
            resultColumnIds.push_back(column.GetId());
        }

        ResultCellIndices.resize(Settings->GetColumns().size(), -1);
        MainTableCovered = true;
        for(i32 i = 0; i < Settings->GetColumns().size(); i++) {
            const auto& column = Settings->GetColumns(i);
            if (column.GetName() == "_yql_full_text_relevance") {
                ResultCellIndices[i] = RELEVANCE_COLUMN_MARKER;
                continue;
            }

            if (keyColumns.contains(column.GetName())) {
                ResultCellIndices[i] = keyColumns[column.GetName()];
                continue;
            }

            MainTableCovered = false;
            if (column.GetName() == searchColumnName) {
                SearchColumnIdx = resultColumnIds.size();
            }

            resultColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            ResultCellIndices[i] = resultColumnIds.size();
            resultColumnIds.push_back(column.GetId());
        }

        auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;

        MainTableReader = MakeIntrusive<TTableReader>(
            Counters, TableId, Snapshot, LogPrefix,
            keyColumnTypes, resultColumnTypes, resultColumnIds);
        MainTableReader->AddResolvePartitioningRequest(request);

        YQL_ENSURE(Settings->GetIndexTables().size() >= 1);

        {
            auto& info = Settings->GetIndexTables(Settings->GetIndexTables().size() - 1);
            YQL_ENSURE(info.GetTable().GetPath().EndsWith(NTableIndex::ImplTable));
            IndexTableReader = TIndexTableImplReader::FromNavigateRequest(Counters, Snapshot, LogPrefix, IndexDescription, FromProto(info.GetTable()), info.GetKeyColumns(), info.GetColumns());
            IndexTableReader->AddResolvePartitioningRequest(request);
        }

        if (IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
            YQL_ENSURE(Settings->GetIndexTables().size() >= 4);
            {
                auto&info = Settings->GetIndexTables(0);
                YQL_ENSURE(info.GetTable().GetPath().EndsWith(DictTable));
                DictTableReader = TDictTableReader::FromNavigateRequest(Counters, Snapshot, LogPrefix, IndexDescription, FromProto(info.GetTable()), info.GetKeyColumns(), info.GetColumns());
                DictTableReader->AddResolvePartitioningRequest(request);
            }

            {
                auto& info = Settings->GetIndexTables(1);
                YQL_ENSURE(info.GetTable().GetPath().EndsWith(DocsTable));
                DocsTableReader = TDocsTableReader::FromNavigateRequest(Counters, Snapshot, LogPrefix, IndexDescription, FromProto(info.GetTable()), info.GetKeyColumns(), info.GetColumns());
                DocsTableReader->AddResolvePartitioningRequest(request);
            }

            {
                auto& info = Settings->GetIndexTables(2);
                YQL_ENSURE(info.GetTable().GetPath().EndsWith(StatsTable));
                StatsTableReader = TStatsTableReader::FromNavigateRequest(
                    Counters, Snapshot, LogPrefix, IndexDescription, FromProto(info.GetTable()), info.GetKeyColumns(), info.GetColumns());
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
    {
        Y_ABORT_UNLESS(Arena);
        Y_ABORT_UNLESS(Settings->GetArena() == Arena->Get());

        IngressStats.Level = statsLevel;

        if (Settings->HasLimit() && Settings->GetLimit() > 0) {
            Limit = Settings->GetLimit();
        }

        TableId = FromProto(Settings->GetTable());
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

            auto row = documentInfo->GetRow(HolderFactory, computeBytes);
            resultBatch.emplace_back(std::move(row));
            ReadBytes += documentInfo->GetRowStorageSize();
            ReadRows++;

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
            ExtractAndTokenizeExpression();
            ReadTotalStats();
        } else {
            ExtractAndTokenizeExpression();
            StartWordReads();
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
            auto kct = MainTableReader->GetKeyColumnTypes();
            TConstArrayRef<TCell> documentId = row.subspan(0, kct.size());
            auto it = DocumentInfos.find(documentId);
            YQL_ENSURE(it != DocumentInfos.end());

            const TIntrusivePtr<NKikimr::NKqp::TDocumentInfo> documentInfoPtr = it->second;
            CA_LOG_E("Adding row info about docnumid: " << documentInfoPtr->DocumentNumId);
            documentInfoPtr->AddRow(row);
            if (PostfilterMatchers.empty() || Postfilter(*documentInfoPtr)) {
                ResultQueue.push_back(documentInfoPtr);
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

    void DocumentStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64) {
        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            auto it = DocumentInfos.find(DocsTableReader->GetDocumentId(row));
            YQL_ENSURE(it != DocumentInfos.end());
            auto& doc = it->second;
            doc->SetDocumentLength(DocsTableReader->GetDocumentLength(row));

            if (Limit > 0) {
                TopKQueue.push({doc->GetBM25Score(), doc});
                if (TopKQueue.size() > (size_t)Limit) {
                    TopKQueue.pop();
                }

                if (Reads.empty()) {
                    ProcessTopKQueue();
                }

            } else {
                FetchDocumentDetails(doc);
            }
        }
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
            CA_LOG_E("Setting word frequency for word: " << wordIndex << ", frequency: " << word.Frequency);
            ContinueWordRead(word);
        }
    }

    void WordResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& wordInfo = Words[wordIndex];

        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);
            YQL_ENSURE(IndexTableReader);
            auto [freq, docInfo] = IndexTableReader->BuildDocumentInfo(QueryCtx, MainTableReader, DocsTableReader, row);
            const auto docId = docInfo->GetDocumentId();
            auto [it, success] = DocumentInfos.emplace(docId, std::move(docInfo));
            if (success) {
                it->second->DocumentNumId = DocumentNumId++;
            }

            CA_LOG_E("Adding containing word: " << wordIndex << ", freq: " << freq);
            it->second->AddContainingWord(wordIndex, freq);
            wordInfo.PendingDocuments.push_back(it->second);
            if (wordInfo.HasDocumentIdPointer() && wordInfo.PendingDocuments.size() == 1) {
                MergeQueue.push(wordInfo.GetDocumentIdPointer());
            }
        }

        while (MergeQueue.size() == Words.size()) {
            CA_LOG_E("MergeQueue size = " << MergeQueue.size());

            const auto& documentIdPointer = MergeQueue.top();
            if (documentIdPointer.Finished) {
                break;
            }

            YQL_ENSURE(documentIdPointer.DocumentInfo);
            TIntrusivePtr<TDocumentInfo> documentInfo = documentIdPointer.DocumentInfo;
            if (documentInfo->AllWordsContained()) {
                if (IndexDescription.GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
                    FetchDocumentStats(documentInfo);
                } else {
                    FetchDocumentDetails(documentInfo);
                }
            }

            std::vector<ui64> wordIndexes;
            while (!MergeQueue.empty() && MergeQueue.top().DocumentInfo->DocumentNumId == documentInfo->DocumentNumId) {
                auto wordIndex = MergeQueue.top().WordIndex;
                wordIndexes.push_back(wordIndex);
                YQL_ENSURE(wordIndex < Words.size());
                auto& word = Words[wordIndex];
                word.PendingDocuments.pop_front();
                MergeQueue.pop();
            }

            for(auto wordIndex : wordIndexes) {
                auto& word = Words[wordIndex];
                if (word.HasDocumentIdPointer()) {
                    MergeQueue.push(word.GetDocumentIdPointer());
                }
            }
        }

        NotifyCA();

        if (finished) {
            ContinueWordRead(wordInfo);
        }
    }

    void HandleReadResult(TEvDataShard::TEvReadResult::TPtr& ev) {
        // Find which word read this result belongs to
        ui64 readId = ev->Get()->Record.GetReadId();
        auto& record = ev->Get()->Record;
        // TODO: Map readId to word index and process result

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
                DocumentStatsResult(msg, cookie);
                break;
            case EReadKind_WordStats:
                WordStatsResult(msg, cookie);
                break;
            case EReadKind_Word:
                WordResult(msg, cookie, record.GetFinished());
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
