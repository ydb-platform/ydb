/**
 * TFullTextSource -- async input source actor for KQP compute actors.
 *
 * This file implements the full-text search query execution pipeline within YDB's
 * KQP (KiKiMR Query Processor). TFullTextSource is registered as an
 * IDqComputeActorAsyncInput source and is driven by a parent compute actor that
 * pulls rows via GetAsyncInputData().
 *
 * === High-level pipeline ===
 *
 *  1. Bootstrap: resolve table partitioning for all involved tables via SchemeCache.
 *  2. Tokenize the user's search query using the analyzer configured on the index.
 *  3. (Relevance mode) Read aggregate statistics (doc count, total doc length) from
 *     the stats table, and per-token document frequencies from the dict table.
 *  4. Issue range reads against the posting (index impl) table to stream sorted
 *     doc_id lists for each query token.
 *  5. Merge posting lists using one of two algorithms:
 *       - TAndOptimizedMergeAlgorithm -- leapfrog/galloping merge for AND semantics.
 *       - TDefaultMergeAlgorithm       -- min-heap merge for OR / minimum_should_match.
 *  6. (Relevance mode with imbalanced tokens) Use a two-layer (L1/L2) approach:
 *     L1 merges only the rarest tokens, then L2 verifies candidates against the
 *     remaining frequent tokens via point lookups, avoiding full scans of large
 *     posting lists.
 *  7. For matched documents, optionally read document lengths from the docs table
 *     and compute BM25 scores.  When a LIMIT is specified, maintain a TopK buffer
 *     with amortized O(1) nth_element compaction so only the highest-scoring
 *     documents survive.
 *  8. Fetch full row data from the main table (unless "covered" -- all requested
 *     columns are already available from the index key).
 *  9. Stream result rows to the compute actor via ResultQueue / NotifyCA().
 *
 * === Tables involved (relevance index) ===
 *
 *  - Main table: the user's original table (fetches full rows for matched doc_ids).
 *  - Posting table (indexImplTable): key=(token, doc_id), value=[freq].
 *      Stores the inverted index.  Read in Arrow FORMAT_ARROW batches.
 *  - Dict table (indexImplDictTable): key=(token), value=(document_frequency).
 *      Provides per-token DF values for IDF computation.
 *  - Docs table (indexImplDocsTable): key=(doc_id), value=(doc_length).
 *      Provides per-document lengths for BM25 normalization.
 *  - Stats table (indexImplStatsTable): key=(partition_id),
 *      value=(doc_count, sum_doc_length).  Global corpus statistics.
 *
 *  For a plain (non-relevance) index only the main table and posting table are used.
 *
 * === Communication model ===
 *
 *  TFullTextSource is an NActors actor (TActorBootstrapped).  It sends TEvRead
 *  requests to datashards via the PipeCache and receives TEvReadResult responses.
 *  Shard partitioning is resolved at runtime through TEvResolveKeySet to the
 *  SchemeCache.  Retries use exponential backoff with configurable limits.
 */

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

// BM25 tuning parameters (Okapi BM25).
// K1 controls term-frequency saturation: higher K1 means raw TF matters more.
// B controls document-length normalization: B=1 fully normalizes, B=0 ignores length.
constexpr double K1_FACTOR_DEFAULT = 1.2;
constexpr double B_FACTOR_DEFAULT = 0.75;

// Threshold below which floating-point settings are treated as "not set".
constexpr double EPSILON = 1e-6;

// Sentinel column index used in ResultCellIndices to indicate that the column
// should be filled with the computed BM25 relevance score rather than a table cell.
constexpr i32 RELEVANCE_COLUMN_MARKER = -1;

// When token document-frequencies differ by more than this factor, the rarer tokens
// are used for L1 merge and the frequent ones are deferred to L2 point lookups.
// For n-gram queries the frequent n-grams are dropped entirely because n-gram
// results are post-filtered anyway.
constexpr double NGRAM_IMBALANCE_FACTOR = 10;

class TDocId;

/**
 * TTableReader<T> -- CRTP base for partition-aware table reading via TEvRead.
 *
 * Each derived reader (TIndexTableImplReader, TDocsTableReader, etc.) inherits
 * this base to get:
 *   - Schema storage: key column types, result column types/ids.
 *   - Partitioning resolution: AddResolvePartitioningRequest() requests the full
 *     key range from the SchemeCache; SetPartitionInfo() stores the result.
 *   - Request building: GetReadRequest() constructs a TEvDataShard::TEvRead with
 *     the correct table id, columns, snapshot, and quota settings.
 *   - Range splitting: GetRangePartitioning() maps a logical TTableRange to
 *     (shardId, subrange) pairs by binary-searching the partition boundaries.
 *   - Stats tracking: ReadRows / ReadBytes accumulation.
 *
 * The Arrow format flag (UseArrowFormat) is set when the result schema is a single
 * Uint64 column (doc_id), enabling columnar batch transfer from the datashard.
 */
template <typename T>
class TTableReader : public TAtomicRefCount<T> {
    TIntrusivePtr<TKqpCounters> Counters;
    TTableId TableId;
    TString TablePath;
    IKqpGateway::TKqpSnapshot Snapshot;
    TString LogPrefix;
    TString Database;
    TString PoolId;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    bool UseArrowFormat = false;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;
    TVector<i32> ResultColumnIds;
    TPartitioning::TCPtr PartitionInfo;

public:

    TTableReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : Counters(counters)
        , TableId(tableId)
        , TablePath(tablePath)
        , Snapshot(snapshot)
        , LogPrefix(logPrefix)
        , Database(database)
        , PoolId(poolId)
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

    // Build a TEvRead protobuf request addressed to the posting/docs/dict/main table.
    // Point lookups go into request->Keys; range scans go into request->Ranges.
    // When the `To` key is a prefix (fewer cells than the full key), the range
    // endpoint is treated as inclusive to capture all rows sharing that prefix.
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

        if (!PoolId.empty()) {
            record.SetDatabaseId(Database);
            record.SetPoolId(PoolId);
        }

        return request;
    }

    // Append a full-range key descriptor for this table to a SchemeCache batch
    // request.  The SchemeCache returns partition boundaries that are later stored
    // via SetPartitionInfo().
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

    // Map a logical TTableRange to a list of (shardId, intersectedRange) pairs.
    // Uses binary search on PartitionInfo boundaries to find the first partition
    // that may overlap, then walks forward collecting intersections until the
    // range is fully covered.
    std::vector<TPartitioning::TIntersection> GetRangePartitioning(const TTableRange& range) {
        return PartitionInfo->GetIntersectionWithRange(KeyColumnTypes, range);
    }
};

/**
 * TQueryCtx -- per-query BM25 scoring context.
 *
 * Holds corpus-level statistics needed by the BM25 formula:
 *   - DocCount:  total number of documents in the corpus (from stats table).
 *   - AvgDL:     average document length = SumDocLength / DocCount.
 *   - IDFValues: one IDF value per query token, computed as:
 *                  IDF(t) = ln( (N - df(t) + 0.5) / (df(t) + 0.5) + 1 )
 *                where N = DocCount, df(t) = document frequency of token t.
 *   - K1Factor / BFactor: BM25 tuning knobs (overridable per query).
 *
 * Also stores the result column mapping (ResultCellIndices) and the boolean
 * query semantics (DefaultOperator = AND/OR, MinimumShouldMatch).
 */
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

/**
 * TDocumentInfo -- represents a single matched document throughout the pipeline.
 *
 * Lifecycle:
 *   1. Created by a merge algorithm with just DocumentNumId (the synthetic
 *      Uint64 doc_id from the posting table).
 *   2. TokenFrequencies[] are populated if relevance scoring is needed --
 *      one entry per query token giving the term frequency in this document.
 *   3. DocumentLength is set from the docs table (for BM25 normalization).
 *   4. RowCells are filled either from the main table read (AddRow) or are
 *      already available when the query is "covered" by the index key columns.
 *   5. GetRow() materializes the final NUdf::TUnboxedValue row, substituting
 *      the BM25 score for any RELEVANCE_COLUMN_MARKER column.
 *
 * KeyCells stores (doc_id) as a serialized cell vector, used to build point
 * read requests against the main/docs tables.
 */
class TDocumentInfo : public TSimpleRefCount<TDocumentInfo> {
    TOwnedCellVec KeyCells;
    TOwnedCellVec RowCells;
    ui64 DocumentLength = std::numeric_limits<ui64>::max();
    bool Covered = false;

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

    // Compute the BM25 score for this document:
    //   score = SUM_over_tokens[ IDF(t) * tf(t,d) / (tf(t,d) + K1*(1-B+B*|d|/avgdl)) ]
    // where tf(t,d) = TokenFrequencies[t], |d| = DocumentLength.
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
        Covered = true;
    }

    TCell GetResultCell(const size_t idx) const {
        return RowCells.at(idx);
    }

    // A document is "covered" when all columns the user requested are available
    // from the index key alone, so no main-table read is needed.
    bool IsCovered(bool external) const {
        return external || Covered;
    }

    // Materialize the final result row as an NUdf::TUnboxedValue array.
    // Columns are filled according to ResultCellIndices:
    //   - RELEVANCE_COLUMN_MARKER -> BM25 score (double)
    //   - index 0 -> DocumentNumId (the synthetic doc_id)
    //   - other indices -> cells from RowCells (fetched from main table)
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

/**
 * TL1DocumentInfo -- wraps a TDocumentInfo with the token word for L2 verification.
 *
 * When a document matches in the L1 merge (rarest tokens), we need to verify it
 * against each frequent (L2) token by doing a point lookup in the posting table
 * for the key (token, doc_id).  TL1DocumentInfo stores that composite key in
 * IndexKey so it can be used with TReadItemsQueue::Sequential().
 */
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


/**
 * TDocId -- lightweight (word_index, doc_id) pair used in the merge priority queue.
 *
 * TCompare orders by doc_id descending so that std::priority_queue yields the
 * *smallest* doc_id first (min-heap).  TEquals checks doc_id equality across
 * different token streams for match detection.
 */
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

/**
 * TIndexTableImplReader -- reader for the posting (index impl) table.
 *
 * The posting table has key = (token, doc_id) and an optional freq column
 * (present for relevance indexes).  This reader is configured from settings
 * to request only doc_id (and optionally freq) columns, skipping the token
 * column since it is already known from the query.  Arrow batch format is
 * used for efficient columnar transfer of Uint64 doc_ids.
 */
class TIndexTableImplReader : public TTableReader<TIndexTableImplReader> {
    i32 FrequencyColumnIndex = 0;

public:
    TIndexTableImplReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        i32 frequencyColumnIndex)
        : TTableReader(counters, tableId, tablePath,  snapshot, logPrefix, database, poolId, keyColumnTypes, resultColumnTypes, resultColumnIds)
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
            settings->GetDatabase(),settings->GetPoolId(),
            keyColumnTypes, resultColumnTypes, resultColumnIds, freqColumnIndex);
        reader->SetUseArrowFormat(useArrowFormat);
        return reader;
    }

    ui32 GetFrequencyColumnIndex() const {
        return FrequencyColumnIndex;
    }
};

/**
 * TArrowTokenStream -- streaming buffer for one token's posting list.
 *
 * Each token in the search query has its own TArrowTokenStream.  As
 * TEvReadResult batches arrive from the datashard (in Arrow format), they are
 * appended to the pending deques.  The merge algorithm consumes entries one at
 * a time via GetLeastDocId() / MoveToNext().
 *
 * The stream tracks:
 *   - PendingDocumentIds: deque of Arrow UInt64 arrays (doc_id batches).
 *   - PendingDocumentFrequencies: matching deque of UInt32 arrays (term freq).
 *   - UnprocessedDocumentPos/Count: cursor within the current front batch.
 *   - MaxKey: the largest doc_id seen so far (used as a resume key for
 *     continued reads when partitions span multiple TEvRead responses).
 *   - ReadFinished: set when the last TEvReadResult for this token arrives.
 *
 * IsEof() is true only when all batches have been consumed AND no more reads
 * are pending.
 */
class TArrowTokenStream {
    std::deque<std::shared_ptr<arrow::UInt64Array>> PendingDocumentIds;
    std::deque<std::shared_ptr<arrow::UInt32Array>> PendingDocumentFrequencies;
    bool ReadFinished = false;
    i64 UnprocessedDocumentPos = 0;
    ui64 UnprocessedDocumentCount = 0;
    ui64 Bytes = 0;
    ui64 Rows = 0;
    ui64 MaxKey = 0;

public:
    TArrowTokenStream(ui64 tokenIndex)
    {
        Y_UNUSED(tokenIndex);
    }

    bool IsEof() const {
        return UnprocessedDocumentCount == 0 && ReadFinished;
    }

    void SetReadFinished() {
        ReadFinished = true;
    }

    ui64 GetMaxKey() const {
        return MaxKey;
    }

    std::pair<ui64, ui64> GetStats() const {
        return {Rows, Bytes};
    }

    ui32 GetUnprocessedDocumentCount() const {
        return UnprocessedDocumentCount;
    }

    void AddResult(std::unique_ptr<TEvDataShard::TEvReadResult> result) {
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
        YQL_ENSURE(MaxKey < docIds->Value(0) || MaxKey == 0);
        MaxKey = docIds->Value(docIds->length() - 1);
        if (batch->num_columns() > 1) {
            auto array = batch->column(1);
            auto freq_array = std::static_pointer_cast<arrow::UInt32Array>(array);
            YQL_ENSURE(freq_array);
            YQL_ENSURE(freq_array->length() == docIds->length());
            Bytes += freq_array->length() * sizeof(ui32);
            PendingDocumentFrequencies.emplace_back(std::move(freq_array));
        }
        PendingDocumentIds.emplace_back(std::move(docIds));
        UnprocessedDocumentCount += result->GetRowsCount();
    }

    bool MoveToNext() {
        YQL_ENSURE(!PendingDocumentIds.empty());
        UnprocessedDocumentPos++;
        UnprocessedDocumentCount--;

        if (UnprocessedDocumentPos == static_cast<i64>(PendingDocumentIds.front()->length())) {
            if (!PendingDocumentFrequencies.empty()) {
                PendingDocumentFrequencies.pop_front();
            }

            PendingDocumentIds.pop_front();
            UnprocessedDocumentPos = 0;
        }

        return UnprocessedDocumentCount > 0;
    }

    ui32 GetLeastDocFrequency() const {
        YQL_ENSURE(!PendingDocumentFrequencies.empty());
        return PendingDocumentFrequencies.front()->Value(UnprocessedDocumentPos);
    }

    ui64 GetLeastDocId() {
        YQL_ENSURE(!PendingDocumentIds.empty());
        return PendingDocumentIds.front()->Value(UnprocessedDocumentPos);
    }
};

/**
 * TWordReadState -- per-query-token read state for the posting table.
 *
 * Each token extracted from the search query gets a TWordReadState that
 * manages its range reads against the posting table.  The posting table key
 * is (token, doc_id), so reading all documents for a token means scanning the
 * range [token, 0] .. [token, +inf).  Because the table is partitioned across
 * shards, BuildRangesToRead() splits this range using GetRangePartitioning()
 * into per-shard sub-ranges stored in RangesToRead.
 *
 * Key fields:
 *   - WordIndex: position of this token in the Words[] vector (also the stream
 *     index for the merge algorithm).
 *   - Word: the tokenized search term string.
 *   - L1 / L2: flags controlling which merge layer this token participates in.
 *     L1 tokens participate in the primary merge; L2 tokens are verified via
 *     point lookups against L1-matched candidates.
 *   - StartReadKeyFrom: resume point after a completed shard read; set to
 *     maxDocId+1 so the next BuildRangesToRead() continues from where we left off.
 *   - Frequency: document frequency from the dict table (used for IDF and
 *     imbalance detection).
 */
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
    ui64 StartReadKeyFrom = 0;

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
        RangesToRead.clear();

        TCell tokenCell(Word.data(), Word.size());
        std::vector<TCell> fromCells = {tokenCell, TCell::Make<ui64>(StartReadKeyFrom)};
        std::vector<TCell> toCells = {tokenCell};
        auto range = TTableRange(fromCells, true, toCells, false /*toInclusive*/);

        auto rangePartition = Reader->GetRangePartitioning(range);
        for(const auto& [shardId, range] : rangePartition) {
            RangesToRead.emplace_back(shardId, std::move(range));
        }
    }
};

/**
 * IMergeAlgorithm -- base interface for merging sorted posting lists.
 *
 * Subclasses consume TArrowTokenStream entries (one stream per query token)
 * and emit batches of TDocumentInfo for documents that satisfy the match
 * criteria (AND / OR with minimum_should_match).
 *
 * Key state:
 *   - Streams[]: one TArrowTokenStream per L1 (or L2) token.
 *   - MinShouldMatch: minimum number of tokens a document must match.
 *   - FinishedTokens: count of streams that have been fully consumed.
 *   - WithFrequencies: whether to extract term frequencies for BM25 scoring.
 *
 * The merge is incremental: FindMatches() returns whatever matches can be
 * determined from the data currently buffered.  New data is fed via AddResult().
 * Done() returns true when no more matches can ever be produced.
 */
class IMergeAlgorithm {
protected:
    std::vector<std::unique_ptr<TArrowTokenStream>> Streams;
    ui64 TokenCount;
    ui64 MinShouldMatch;
    TDocId::TEquals DocIdEquals;
    TDocId::TCompare DocIdCompare;
    const bool WithFrequencies;
    ui64 FinishedTokens = 0;
    std::vector<ui32> MatchedTokens;

public:

    IMergeAlgorithm(std::vector<std::unique_ptr<TArrowTokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
        : Streams(std::move(streams))
        , TokenCount(Streams.size())
        , MinShouldMatch(minShouldMatch)
        , DocIdEquals(TDocId::TEquals(keyColumnTypes))
        , DocIdCompare(TDocId::TCompare(keyColumnTypes))
        , WithFrequencies(withFrequencies)
    {
    }

    virtual void AddResult(ui64 tokenIndex, std::unique_ptr<TEvDataShard::TEvReadResult> msg) = 0;

    ui64 GetMaxTokenKey(ui64 tokenIndex) {
        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        return stream->GetMaxKey();
    }

    void FinishTokenStream(ui64 tokenIndex) {
        YQL_ENSURE(tokenIndex < Streams.size(), "Token index out of bounds");
        auto& stream = Streams[tokenIndex];
        stream->SetReadFinished();
        if (stream->IsEof()) {
            FinishedTokens++;
        }
    }

    virtual bool Done() const = 0;

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


/**
 * TAndOptimizedMergeAlgorithm -- leapfrog merge for strict AND semantics.
 *
 * All tokens must match (MinShouldMatch == TokenCount).  This algorithm avoids
 * the overhead of a full priority queue by maintaining a simple ReadyStreams
 * deque.  The core loop picks a candidate doc_id and "jumps" each other stream
 * to that candidate:
 *
 *   1. Take the front stream's least doc_id as candidate.
 *   2. For each remaining ready stream, call JumpToBest():
 *      - If stream's current doc_id == candidate: match (return 0).
 *      - If stream's current doc_id > candidate: the old candidate is
 *        impossible; adopt the new (larger) doc_id as candidate and restart
 *        matching from scratch (return 1).
 *      - If stream is exhausted trying to reach candidate: this stream
 *        needs more data (return -1).
 *   3. When all streams agree on the same doc_id, emit a match.
 *
 * Done() is true as soon as ANY stream is fully exhausted, since AND requires
 * all streams.
 *
 * The "ready" concept: a stream is "ready" when it has buffered data to read.
 * Streams without data are implicitly waiting for the next TEvReadResult.
 */
class TAndOptimizedMergeAlgorithm : public IMergeAlgorithm {
    std::deque<ui32> ReadyStreams;

public:
    TAndOptimizedMergeAlgorithm(std::vector<std::unique_ptr<TArrowTokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
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

    bool Done() const override {
        return FinishedTokens > 0;
    }

    // Move all currently matched streams past their current entry.
    // If a stream runs out of buffered data it leaves ReadyStreams (waits for
    // more TEvReadResult); if it's fully exhausted, FinishedTokens increments.
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

    // Advance a single stream until its doc_id >= candidate.
    // Returns: 0 = exact match, 1 = found larger doc_id (candidate updated),
    //         -1 = stream needs more data.
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
        if (FinishedTokens > 0) {
            return std::vector<TDocumentInfo::TPtr>();
        }

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

/**
 * TDefaultMergeAlgorithm -- min-heap merge for OR / minimum_should_match.
 *
 * Maintains a priority queue (min-heap by doc_id) containing the current
 * entry from each active stream.  FindMatches() repeatedly:
 *
 *   1. Pop the smallest doc_id.
 *   2. Pop all other entries with the same doc_id (they come from different
 *      token streams matching the same document).
 *   3. If the number of matching streams >= MinShouldMatch, emit a match.
 *   4. Advance each consumed stream and re-insert into the heap.
 *
 * Done() is true when the number of non-exhausted streams drops below
 * MinShouldMatch (no future document can satisfy the threshold).
 */
class TDefaultMergeAlgorithm : public IMergeAlgorithm {
    std::priority_queue<TDocId, TStackVec<TDocId, 64>, TDocId::TCompare> MergeQueue;

public:
    TDefaultMergeAlgorithm(std::vector<std::unique_ptr<TArrowTokenStream>>&& streams, ui64 minShouldMatch, bool withFrequencies, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes)
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

    virtual bool Done() const override {
        return Streams.size() - FinishedTokens < MinShouldMatch;
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

/**
 * TDocsTableReader -- reader for the docs table (indexImplDocsTable).
 *
 * Schema: key = (doc_id), value = (doc_length).
 * Used in relevance mode to fetch per-document lengths needed by BM25's
 * length normalization factor: K1 * (1 - B + B * |d| / avgdl).
 *
 * Reads use Arrow format for batch efficiency (doc_id Uint64, length Uint32).
 */
class TDocsTableReader : public TTableReader<TDocsTableReader> {
    NKqpProto::EKqpFullTextIndexType IndexType;
public:
    TDocsTableReader(const NKqpProto::EKqpFullTextIndexType& indexType,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, database, poolId, keyColumnTypes, resultColumnTypes, resultColumnIds)
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
            FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix,
                settings->GetDatabase(), settings->GetPoolId(),
                keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
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

/**
 * TStatsTableReader -- reader for the stats table (indexImplStatsTable).
 *
 * Schema: key = (partition_id Uint32), value = (doc_count Uint64, sum_doc_length Uint64).
 * Stores corpus-wide aggregate statistics.  Read once at startup (via
 * GetTotalStatsRequest) to populate DocCount and SumDocLength in TQueryCtx
 * for BM25's average document length computation.
 */
class TStatsTableReader : public TTableReader<TStatsTableReader> {
    NKqpProto::EKqpFullTextIndexType IndexType;
public:
    TStatsTableReader(const NKqpProto::EKqpFullTextIndexType& indexType,
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, database, poolId, keyColumnTypes, resultColumnTypes, resultColumnIds)
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
            FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix,
                settings->GetDatabase(), settings->GetPoolId(),
                keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
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

/**
 * TDictTableReader -- reader for the dictionary table (indexImplDictTable).
 *
 * Schema: key = (token), value = (document_frequency).
 * Stores how many documents contain each token.  Read during the enrichment
 * phase (EnrichWordInfo) to populate TWordReadState::Frequency for each query
 * token, which is then used for:
 *   - IDF computation in TQueryCtx::AddIDFValue().
 *   - Token frequency imbalance detection in StartWordReads().
 */
class TDictTableReader : public TTableReader<TDictTableReader> {
public:
    TDictTableReader(const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, database, poolId, keyColumnTypes, resultColumnTypes, resultColumnIds)
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

        return MakeIntrusive<TDictTableReader>(
            counters, FromProto(info.GetTable()), info.GetTable().GetPath(), snapshot, logPrefix,
            settings->GetDatabase(), settings->GetPoolId(),
            keyColumnTypes, resultKeyColumnTypes, resultKeyColumnIds);
    }

    TStringBuf GetWord(const TConstArrayRef<TCell>& row) const {
        return row[0].AsBuf();
    }

    ui64 GetWordFrequency(const TConstArrayRef<TCell>& row) const {
        return row[GetResultColumnTypes().size() - 1].AsValue<ui64>();
    }
};

// Discriminator for in-flight TEvRead requests so HandleReadResult can
// dispatch the response to the correct processing path.
enum EReadKind : ui32 {
    EReadKind_Word = 0,           // L1 posting list range scan
    EReadKind_WordStats = 1,      // Dict table: per-token document frequency
    EReadKind_DocumentStats = 2,  // Docs table: per-document length
    EReadKind_Document = 3,       // Main table: full row data for matched docs
    EReadKind_TotalStats = 4,     // Stats table: corpus-wide aggregates
    EReadKind_Word_L2 = 5,        // L2 posting list point lookups
};

// Metadata stored for each in-flight read so we can route the response.
// Cookie semantics depend on ReadKind: for EReadKind_Word it's the word index;
// for EReadKind_Word_L2 it's the word index; for others it may be a read id.
struct TReadInfo {
    ui64 ReadKind;
    ui64 Cookie;
    ui64 ShardId;
    ui64 LastSeqNo = 0;  // Last acknowledged sequence number (for flow control)
};

/**
 * TMainTableReader -- reader for the user's original table.
 *
 * After the merge identifies matching document IDs, this reader fetches the
 * full row data that the user's SELECT requested.  FromSettings() analyzes
 * the requested columns to determine:
 *
 *   - ResultCellIndices: mapping from output column position to either a table
 *     cell index or RELEVANCE_COLUMN_MARKER for the BM25 score column.
 *   - MainTableCovered: true if all requested columns are key columns of the
 *     main table (so no actual read is needed -- the data is already known
 *     from the doc_id).
 *   - WithRelevance: true if the user requested __ydb_full_text_relevance and
 *     the index type supports it.
 */
class TMainTableReader : public TTableReader<TMainTableReader> {
    bool WithRelevance;
public:
    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    bool MainTableCovered;

    TMainTableReader(
        const TIntrusivePtr<TKqpCounters>& counters,
        const TTableId& tableId,
        const TString& tablePath,
        const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& logPrefix,
        const TString& database,
        const TString& poolId,
        const TVector<NScheme::TTypeInfo>& keyColumnTypes,
        const TVector<NScheme::TTypeInfo>& resultColumnTypes,
        const TVector<i32>& resultColumnIds,
        const TVector<std::pair<i32, NScheme::TTypeInfo>>& resultCellIndices,
        bool mainTableCovered,
        bool withRelevance)
        : TTableReader(counters, tableId, tablePath, snapshot, logPrefix, database, poolId, keyColumnTypes, resultColumnTypes, resultColumnIds)
        , WithRelevance(withRelevance)
        , ResultCellIndices(resultCellIndices)
        , MainTableCovered(mainTableCovered)
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
        bool withRelevance = false;

        YQL_ENSURE(settings->GetQuerySettings().ColumnsSize() == 1);
        const TStringBuf searchColumnName = settings->GetQuerySettings().GetColumns(0).GetName();

        THashMap<TString, std::pair<i32, NScheme::TTypeInfo>> keyColumns;
        for (const auto& column : settings->GetKeyColumns()) {
            keyColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));

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
            resultColumnTypes.push_back(NScheme::TypeInfoFromProto(
                column.GetTypeId(), column.GetTypeInfo()));
            resultCellIndices.emplace_back(resultColumnIds.size(), resultColumnTypes.back());
            resultColumnIds.push_back(column.GetId());
        }

        withRelevance = withRelevance && settings->GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextRelevance;

        return MakeIntrusive<TMainTableReader>(counters, FromProto(settings->GetTable()), settings->GetTable().GetPath(),
            snapshot, logPrefix, settings->GetDatabase(), settings->GetPoolId(), keyColumnTypes, resultColumnTypes, resultColumnIds,
            resultCellIndices, mainTableCovered, withRelevance);
    }

    bool GetWithRelevance() {
        return WithRelevance;
    }
};

/**
 * TReadsState -- centralized tracker for all in-flight TEvRead requests.
 *
 * Responsibilities:
 *   - Allocates monotonically increasing ReadIds.
 *   - Maps ReadId -> TReadInfo (kind, cookie, shard, last seqno).
 *   - Groups reads by shard (ReadsByShardId) for pipe management and retry
 *     coordination.
 *   - Tracks which shard pipes have been created (PipesCreated) to avoid
 *     redundant pipe creation through PipeCache.
 *   - Sends TEvRead and TEvReadAck through the per-node PipeCache.
 *   - Manages per-shard retry counts with exponential backoff (GetDelay /
 *     CalcDelay from kqp_read_iterator_common.h).
 *
 * The "Empty" state (no active reads, no resolve in progress) is used by the
 * main actor to detect pipeline phase transitions.
 */
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

    // Send a TEvReadAck to the datashard to replenish the read's row/byte quota.
    // Called when a non-final TEvReadResult arrives (i.e., more data is expected).
    // Also reduces the retry counter on successful ack (halves if > 3).
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
        auto shardInfoIt = ReadsByShardId.find(shardId);
        if (shardInfoIt != ReadsByShardId.end()) {
            if (shardInfoIt->second.Retries > 3) {
                shardInfoIt->second.Retries = std::max(shardInfoIt->second.Retries >> 1, (ui64)1);
            }
        }

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

    // Send a TEvRead request to the target shard via PipeCache, tracking
    // the read in our internal maps. Creates a pipe on first contact with a shard.
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

    void RemoveRead(ui64 readId, bool reduceRetries = false) {
        auto it = Reads.find(readId);
        if (it != Reads.end()) {
            ui64 shardId = it->second.ShardId;
            Reads.erase(it);

            auto& shardReads = ReadsByShardId[shardId];
            shardReads.ReadIds.erase(readId);
            if (reduceRetries && shardReads.Retries > 3) {
                shardReads.Retries = std::max(shardReads.Retries >> 1, (ui64)1);
            }

            if (shardReads.ReadIds.empty()) {
                ReadsByShardId.erase(shardId);
            }
        }
    }
};

/**
 * TReadItemsQueue<TItem> -- manages batched point-read requests and correlates
 * responses with the items that triggered them.
 *
 * Used for three kinds of reads:
 *   - DocsReadingQueue (TItem = TDocumentInfo::TPtr): reads from docs table
 *     (document lengths) and main table (full rows).
 *   - L2ReadingQueue (TItem = TL1DocumentInfo::TPtr): L2 point lookups in the
 *     posting table to verify candidates.
 *   - WordsReadingQueue (TItem = TWordReadState::TPtr): dict table lookups for
 *     per-token document frequencies.
 *
 * Key concepts:
 *   - TSentReadItems: a group of items sent in a single TEvRead to one shard.
 *     Items are consumed in order as TEvReadResult rows arrive, using
 *     GetItem()/PopItem().
 *   - TPendingSequentialRead: for L2 reads, items must be sent to the same
 *     shard sequentially (one batch at a time) because the merge algorithm
 *     needs results in doc_id order.  SentItemsPrefixSize tracks how many
 *     items from the front are currently in-flight.
 *   - Enqueue(): groups items by shard (using GetRangePartitioning on each
 *     item's point key), assigns ReadIds, and sends TEvRead requests.
 *   - Sequential(): appends items to a per-cookie pending queue and sends
 *     only the prefix that maps to a single shard.
 */
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
        ui64 SentItemsPrefixSize = 0;

        bool HasSentItems() {
            return SentItemsPrefixSize > 0;
        }

        bool Empty() const {
            return Items.empty();
        }

        const TItem& GetItem() const {
            return Items.front();
        }

        void PopItem() {
            YQL_ENSURE(SentItemsPrefixSize > 0);
            SentItemsPrefixSize--;
            Items.pop_front();
        }
    };

    absl::flat_hash_map<ui64, TPendingSequentialRead> PendingSequential;
    absl::flat_hash_map<ui64, TSentReadItems> Queue;
    const TActorId SelfId;
    TReadsState& ReadsState;

public:

    explicit TReadItemsQueue(const TActorId& selfId, TReadsState& readsState)
        : SelfId(selfId)
        , ReadsState(readsState)
    {}

    void ClearReadItems(TSentReadItems& items) {
        items.Items.clear();
        items.Points.clear();
    }

    TPendingSequentialRead& GetSequentialSchedule(ui64 cookie) {
        return PendingSequential[cookie];
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

    template <typename TReader, typename TCollection>
    void Sequential(TReader* reader, EReadKind readKind, const TCollection& infos, ui64 cookie) {
        auto& schedule = PendingSequential[cookie];
        schedule.Items.insert(schedule.Items.end(), infos.begin(), infos.end());
        if (schedule.SentItemsPrefixSize == 0) {
            schedule.SentItemsPrefixSize = Enqueue(reader, readKind, schedule.Items, cookie, false /*allowMultipleShards*/);
        }
    }

    template <typename TReader>
    bool SendNextSequentialRead(TReader* reader, EReadKind readKind, ui64 cookie) {
        auto& schedule = PendingSequential[cookie];
        schedule.SentItemsPrefixSize = Enqueue(reader, readKind, schedule.Items, cookie, false /*allowMultipleShards*/);
        return schedule.SentItemsPrefixSize > 0;
    }

    template <typename TReader, typename TCollection>
    size_t Enqueue(TReader* reader, EReadKind readKind, const TCollection& infos, ui64 cookie, bool allowMultipleShards = true) {
        absl::flat_hash_map<ui64, TReadItemsQueue<TItem>::TSentReadItems> inflightItems;
        ui64 lastShard = 0;
        size_t prefixSize = 0;
        for (auto& info : infos) {
            auto ranges = reader->GetRangePartitioning(info->GetPoint());
            YQL_ENSURE(ranges.size() == 1);
            if (ranges[0].ShardId != lastShard && lastShard != 0 && !allowMultipleShards) {
                break;
            }

            prefixSize++;
            lastShard = ranges[0].ShardId;
            auto& shardItems = inflightItems[ranges[0].ShardId];
            if (shardItems.ShardId == 0) {
                shardItems.ShardId = ranges[0].ShardId;
            }
            YQL_ENSURE(shardItems.ShardId == ranges[0].ShardId);
            if (shardItems.ReadId == 0) {
                shardItems.ReadId = ReadsState.GetNextReadId();
            }

            shardItems.Points.emplace_back(TOwnedTableRange(ranges[0].TableRange));
            shardItems.Items.emplace_back(info);
        }

        for(auto& [shardId, inflightItem] : inflightItems) {
            auto evRead = reader->GetReadRequest(inflightItem.ReadId, inflightItem.Points);
            YQL_ENSURE(evRead);
            ReadsState.SendEvRead(shardId, evRead, TReadInfo{.ReadKind = readKind, .Cookie = cookie, .ShardId = shardId});
            auto readId = inflightItem.ReadId;
            Enqueue(readId, std::move(inflightItem));
        }

        return prefixSize;
    }

    template <typename TReader>
    void Retry(TReader* reader, EReadKind readKind, ui64 readId) {
        auto& readItems = GetReadItems(readId).Items;
        Enqueue(reader, readKind, readItems, 0);

        ReadsState.RemoveRead(readId);
        Queue.erase(readId);
    }

    template <typename TReader>
    void RetrySequential(TReader* reader, EReadKind readKind, ui64 readId, ui64 cookie) {
        auto& schedule = PendingSequential[cookie];
        schedule.SentItemsPrefixSize = 0;

        ReadsState.RemoveRead(readId);
        Queue.erase(readId);

        SendNextSequentialRead(reader, readKind, cookie);
    }

    TSentReadItems& GetReadItems(ui64 readId) {
        auto it = Queue.find(readId);
        YQL_ENSURE(it != Queue.end());
        return it->second;
    }
};

/**
 * TFullTextSource -- the main actor implementing full-text search as an
 * IDqComputeActorAsyncInput source.
 *
 * === Actor lifecycle ===
 *
 *   Bootstrap() -> StateWork (handles TEvReadResult, TEvResolveKeySetResult,
 *                              TEvDeliveryProblem, retry timers)
 *
 * === Pipeline phases (driven by event handlers, not a state enum) ===
 *
 *   Phase 1 - Resolve: PrepareTableReaders() sends a batch SchemeCache resolve
 *     for all involved tables.  HandleResolve() receives partition info.
 *
 *   Phase 2 - Tokenize & enrich: ExtractAndTokenizeExpression() splits the
 *     search query into tokens.  If relevance mode, ReadTotalStats() and
 *     EnrichWordInfo() issue reads against the stats and dict tables.
 *
 *   Phase 3 - Merge: StartWordReads() configures the L1 (and optionally L2)
 *     merge algorithms and kicks off posting list reads.  L1WordResult()
 *     feeds data into L1MergeAlgo; matched documents flow to either
 *     ScheduleL2Read() or FetchDocumentDetails().
 *
 *   Phase 4 - Fetch: FetchDocumentDetails() reads document lengths (if BM25),
 *     computes scores, maintains a TopK buffer with amortized O(1) nth_element
 *     compaction (if LIMIT), and finally reads full rows from the main table
 *     (unless covered).
 *
 *   Phase 5 - Deliver: Matched rows are pushed to ResultQueue and the compute
 *     actor is notified via TEvNewAsyncInputDataArrived.  The compute actor
 *     pulls rows through GetAsyncInputData().
 *
 * === Pull interface ===
 *
 *   GetAsyncInputData() is called by the compute actor when it has free buffer
 *   space.  It drains ResultQueue up to freeSpace bytes or the LIMIT count.
 *
 * === Error handling ===
 *
 *   - TEvDeliveryProblem (pipe broken): schedule shard-level retry.
 *   - TEvReadResult with error status: per-status retry or abort.
 *   - Retries use exponential backoff with a per-shard retry counter.
 *   - Unhandled exceptions are caught by IActorExceptionHandler and reported
 *     as INTERNAL_ERROR to the compute actor.
 */
class TFullTextSource : public TActorBootstrapped<TFullTextSource>, public NYql::NDq::IDqComputeActorAsyncInput, public NActors::IActorExceptionHandler {
private:

    struct TEvPrivate {
        enum EEv {
            EvSchemeCacheRequestTimeout,
            EvRetryRead,
            EvRetrySingleRead
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
        };

        struct TEvRetryRead : public TEventLocal<TEvRetryRead, EvRetryRead> {
            explicit TEvRetryRead(ui64 shardId)
                : ShardId(shardId) {
            }

            const ui64 ShardId;
        };

        struct TEvRetrySingleRead : public TEventLocal<TEvRetrySingleRead, EvRetrySingleRead> {
            explicit TEvRetrySingleRead(ui64 readId)
                : ReadId(readId) {
            }

            const ui64 ReadId;
        };
    };


    const NKikimrKqp::TKqpFullTextSourceSettings* Settings;  // Protobuf settings (lives in Arena)
    TIntrusivePtr<NActors::TProtoArenaHolder> Arena;         // Protobuf arena owning Settings
    const NActors::TActorId ComputeActorId;                  // Parent compute actor to notify
    const ui64 InputIndex;                                   // Source index within the compute actor
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;  // Factory for NUdf row values
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;  // Memory allocator for MiniKQL values
    TIntrusivePtr<TKqpCounters> Counters;

    const NKikimrSchemeOp::TFulltextIndexDescription& IndexDescription;

    // True when all user-requested columns are available from the index key
    // alone, so no main table read is required for matched documents.
    bool MainTableCovered = false;

    TString Database;
    TString LogPrefix;
    TDqAsyncStats IngressStats;

    // MVCC snapshot for consistent reads across all tables.
    IKqpGateway::TKqpSnapshot Snapshot;

    TActorId SchemeCacheRequestTimeoutTimer;
    TDuration SchemeCacheRequestTimeout;

    // Mapping from output column position to (table_cell_index, type).
    // RELEVANCE_COLUMN_MARKER indicates BM25 score column.
    TVector<std::pair<i32, NScheme::TTypeInfo>> ResultCellIndices;
    i64 Limit = -1;  // User-specified LIMIT; -1 means unlimited.

    // Element for TopK selection when LIMIT + relevance are active.
    struct TTopKDocumentInfo {
        double Score;
        TDocumentInfo::TPtr DocumentInfo;
    };

    TIntrusivePtr<TQueryCtx> QueryCtx;  // BM25 scoring context (created after stats are loaded)

    // TopK buffer for selecting the highest-scoring documents when LIMIT is set.
    // Uses an amortized O(1) approach: documents are appended to the buffer,
    // and when the buffer exceeds threshold, std::nth_element is used to compact
    // it down to the top Limit entries. This avoids O(log K) per-insert heap overhead.
    std::vector<TTopKDocumentInfo> TopKQueue;

    // Compact TopKQueue down to at most Limit entries by keeping only the
    // highest-scoring documents.  Uses std::nth_element (O(n) average).
    void CompactTopK(size_t threshold) {
        if (TopKQueue.size() >= threshold) {
            auto nth = TopKQueue.begin() + Limit;
            std::nth_element(TopKQueue.begin(), nth, TopKQueue.end(),
                [](const TTopKDocumentInfo& a, const TTopKDocumentInfo& b) {
                    return a.Score > b.Score;
                });
            TopKQueue.resize(Limit);
        }
    }

    bool ResolveInProgress = true;   // True while SchemeCache resolve is pending
    bool PendingNotify = false;      // True when a TEvNewAsyncInputDataArrived is in flight

    ui64 ProducedItemsCount = 0;                          // Rows already delivered to compute actor
    std::deque<TDocumentInfo::TPtr> ResultQueue;           // Ready-to-deliver document rows
    std::deque<TDocumentInfo::TPtr> L1MergedDocuments;     // L1-matched docs awaiting L2 verification
    bool IsNgram = false;                                  // True if the index uses n-gram tokenization

    TActorId PipeCacheId;  // Per-node pipe cache for datashard communication

    // Corpus-wide statistics from the stats table.
    ui64 DocCount = 0;
    ui64 SumDocLength = 0;

    // Table readers -- one per involved table.  Null readers indicate that the
    // table is not needed (e.g., dict/docs/stats are null for plain indexes).
    TIntrusivePtr<TMainTableReader> MainTableReader;
    TIntrusivePtr<TIndexTableImplReader> IndexTableReader;
    TIntrusivePtr<TDocsTableReader> DocsTableReader;
    TIntrusivePtr<TDictTableReader> DictTableReader;
    TIntrusivePtr<TStatsTableReader> StatsTableReader;

    // Read infrastructure.
    TReadsState ReadsState;                                // Tracks all in-flight reads
    TReadItemsQueue<TDocumentInfo::TPtr> DocsReadingQueue; // Docs table + main table reads
    TReadItemsQueue<TL1DocumentInfo::TPtr> L2ReadingQueue; // L2 posting list point lookups
    TReadItemsQueue<TWordReadState::TPtr> WordsReadingQueue; // Dict table lookups
    TVector<TWordReadState::TPtr> Words;                   // Tokenized query terms

    // Merge algorithms: L1 handles the primary merge (all or rare tokens),
    // L2 (optional) handles verification of frequent tokens.
    std::unique_ptr<IMergeAlgorithm> L1MergeAlgo;
    std::unique_ptr<IMergeAlgorithm> L2MergeAlgo;
    // Helper to bind allocator
    TGuard<NMiniKQL::TScopedAlloc> BindAllocator() {
        return TGuard<NMiniKQL::TScopedAlloc>(*Alloc);
    }

    // Parse the search query string and tokenize it using the analyzer
    // configured on the fulltext index (same analyzer used at index build time).
    // Each resulting token becomes a TWordReadState entry in Words[].
    // Returns false if no tokens were extracted (reports BAD_REQUEST error).
    bool ExtractAndTokenizeExpression() {
        YQL_ENSURE(Settings->GetQuerySettings().GetColumns().size() == 1);

        if (Settings->GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextJson) {
            // For JSON index, tokens are pre-compiled at query compile time
            YQL_ENSURE(Settings->GetQuerySettings().TokensSize() > 0, "Expected non-empty tokens");
            YQL_ENSURE(IndexTableReader, "Index table reader is not initialized");

            size_t wordIndex = 0;
            for (const TString& token : Settings->GetQuerySettings().GetTokens()) {
                Words.emplace_back(MakeIntrusive<TWordReadState>(wordIndex++, token, IndexTableReader));
            }
        } else {
            YQL_ENSURE(Settings->GetQuerySettings().GetQuery().size() > 0, "Expected non-empty query");
            const auto& expr = Settings->GetQuerySettings().GetQuery();

            for (const auto& column : Settings->GetQuerySettings().GetColumns()) {
                for (const auto& analyzer : Settings->GetIndexDescription().GetSettings().columns()) {
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
        }

        if (Words.empty()) {
            RuntimeError("No search terms were extracted from the query", NYql::NDqProto::StatusIds::BAD_REQUEST);
            return false;
        }

        return true;
    }

    // Route matched documents through the remaining pipeline stages:
    //   1. If relevance mode and documents lack doc_length -> read from docs table.
    //   2. If LIMIT + relevance -> insert into TopK buffer; compact with nth_element when buffer is full; drain when merge is done.
    //   3. If documents are "covered" -> push directly to ResultQueue.
    //   4. Otherwise -> enqueue main table reads for full row data.
    void FetchDocumentDetails(std::vector<TDocumentInfo::TPtr>& docInfos) {
        if (Limit > 0 && ProducedItemsCount + ResultQueue.size() >= static_cast<ui64>(Limit)) {
            return;
        }

        if (MainTableReader->GetWithRelevance()) {
            if (!docInfos.empty() && !docInfos[0]->HasDocumentLength()) {
                DocsReadingQueue.Enqueue(DocsTableReader.Get(), EReadKind_DocumentStats, docInfos, 0);
                return;
            }
        }

        if (Limit > 0 && MainTableReader->GetWithRelevance()) {
            for(auto& doc: docInfos) {
                TopKQueue.emplace_back(doc->GetBM25Score(QueryCtx.GetRef()), std::move(doc));
            }
            CompactTopK(static_cast<size_t>(Limit) * 2);
            docInfos.clear();
        }

        if (Limit > 0 && !TopKQueue.empty() && L1MergeAlgo->Done() && (!L2MergeAlgo || L2MergeAlgo->Done())) {
            YQL_ENSURE(docInfos.empty());
            CompactTopK(static_cast<size_t>(Limit) + 1);
            docInfos.reserve(TopKQueue.size());
            for (auto& [score, documentInfo] : TopKQueue) {
                docInfos.emplace_back(std::move(documentInfo));
            }
            TopKQueue.clear();
        }

        if (!docInfos.empty() && docInfos.back()->IsCovered(MainTableCovered)) {
            for(auto& doc: docInfos) {
                ResultQueue.emplace_back(std::move(doc));
            }
            docInfos.clear();
            NotifyCA();
            return;
        }

        DocsReadingQueue.Enqueue(MainTableReader.Get(), EReadKind_Document, docInfos, 0);
        if (ReadsState.Empty()) {
            NotifyCA();
        }
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

    // Read per-token document frequencies from the dict table.
    // Results are stored in TWordReadState::Frequency and later used for
    // IDF computation and token imbalance detection.
    void EnrichWordInfo() {
        WordsReadingQueue.Enqueue(DictTableReader.Get(), EReadKind_WordStats, Words, 0);
    }

    // Configure merge algorithms and begin posting list reads.
    //
    // This is the central orchestration point called after stats/dict reads complete.
    // Steps:
    //   1. Detect token frequency imbalance.  For n-gram queries, drop the
    //      most frequent n-grams.  For relevance AND queries, split tokens
    //      into L1 (rare, full scan) and L2 (frequent, point lookups).
    //   2. Compute MinimumShouldMatch from the query operator and settings.
    //   3. Create TQueryCtx with IDF values from dict table frequencies.
    //   4. Build TArrowTokenStream instances and instantiate the appropriate
    //      merge algorithm:
    //        - AND -> TAndOptimizedMergeAlgorithm (leapfrog)
    //        - OR  -> TDefaultMergeAlgorithm (min-heap)
    //   5. Apply user-overridden BM25 K1/B factors.
    //   6. Issue initial posting list reads for all L1 tokens.
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
        std::vector<std::unique_ptr<TArrowTokenStream>> l1streams;
        std::vector<std::unique_ptr<TArrowTokenStream>> l2streams;

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

    // Initiate SchemeCache partition resolution for all tables.
    // Called once from Bootstrap().  The response is handled by HandleResolve().
    void PrepareTableReaders() {
        ResultCellIndices = MainTableReader->ResultCellIndices;
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
    TFullTextSource(const NKikimrKqp::TKqpFullTextSourceSettings* settings,
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
        Become(&TFullTextSource::StateWork);
        PrepareTableReaders();
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    // Pull interface called by the compute actor.
    // Drains ResultQueue into resultBatch up to freeSpace bytes or LIMIT rows.
    // Sets finished=true when all reads are done and the queue is empty.
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

    // Cancel all in-flight reads and disconnect all pipes on actor destruction.
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
        return (ReadsState.Empty() && !ResolveInProgress && ResultQueue.empty() && TopKQueue.empty()) || (Limit > 0 && ProducedItemsCount >= static_cast<ui64>(Limit));
    }

    // Send TEvNewAsyncInputDataArrived to the compute actor so it calls
    // GetAsyncInputData().  Coalesced: only one notification in flight at a time.
    void NotifyCA() {
        if (!PendingNotify && (!ResultQueue.empty() || IsFinished())) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
            PendingNotify = true;
        }
    }

    // Main event dispatch.  All actor work happens in this state.
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleReadResult);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
            hFunc(TEvPrivate::TEvRetryRead, HandleRetryRead);
            hFunc(TEvPrivate::TEvRetrySingleRead, HandleRetrySingleRead);
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

    void HandleRetrySingleRead(TEvPrivate::TEvRetrySingleRead::TPtr& ev) {
        DoRetrySingleRead(ev->Get()->ReadId);
    }

    void RetryWordRead(ui64 wordIndex, ui64 readId) {
        YQL_ENSURE(wordIndex < Words.size(), "Word index out of bounds");
        auto& word = Words[wordIndex];
        word->BuildRangesToRead();
        ReadsState.RemoveRead(readId);
        ContinueWordRead(word);
    }

    void RetryTotalStatsRead(ui64 readId) {
        ReadsState.RemoveRead(readId);
        ReadTotalStats();
    }

    // Re-issue a single read by its ReadId, dispatching to the appropriate
    // retry path based on the read's EReadKind.
    void DoRetrySingleRead(ui64 readId) {
        auto readIt = ReadsState.FindPtr(readId);
        if (readIt == nullptr) {
            return;
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
            case EReadKind_Word_L2:
                L2ReadingQueue.RetrySequential(IndexTableReader.Get(), EReadKind_Word_L2, readId, readInfo.Cookie);
                break;
            case EReadKind_TotalStats:
                RetryTotalStatsRead(readId);
                break;
        }
    }

    void DoRetryRead(ui64 shardId) {
        auto shardIt = ReadsState.FindShardPtr(shardId);
        YQL_ENSURE(shardIt != nullptr);

        shardIt->IsScheduledRetry = false;

        std::vector<ui64> readsToRetry(shardIt->ReadIds.begin(), shardIt->ReadIds.end());

        for (ui64 readId : readsToRetry) {
            DoRetrySingleRead(readId);
        }
    }

    // Schedule a retry for all reads on a given shard.
    // Checks max retry limit, computes backoff delay, and either retries
    // immediately or schedules a TEvRetryRead timer event.
    bool ScheduleShardRetry(ui64 shardId, bool allowInstantRetry, NYql::NDqProto::StatusIds::StatusCode errorStatus) {
        auto shardIt = ReadsState.FindShardPtr(shardId);
        if (shardIt == nullptr) {
            return false;
        }

        if (shardIt->IsScheduledRetry) {
            return true;
        }

        auto maxRetries = MaxShardRetries();
        if (ReadsState.CheckShardRetriesExeeded(shardId, maxRetries)) {
            RuntimeError(TStringBuilder() << "Max shard retries ("<< maxRetries << ") exceeded for shard " << shardId, errorStatus);
            return false;
        }

        auto delay = ReadsState.GetDelay(shardId, allowInstantRetry);
        if (delay > TDuration::Zero()) {
            shardIt->IsScheduledRetry = true;
            TlsActivationContext->Schedule(delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(shardId)));
        } else {
            DoRetryRead(shardId);
        }
        return true;
    }

    // Handle broken pipe to a datashard tablet.
    // Resets pipe tracking and schedules a retry for all reads on that shard.
    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_E("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        ui64 shardId = ev->Get()->TabletId;
        ReadsState.UntrackPipe(shardId);

        Counters->IteratorDeliveryProblems->Inc();
        ScheduleShardRetry(shardId, true, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
    }

    // Phase 1 completion: SchemeCache returns partition boundaries for all tables.
    // Stores partition info in each reader, then kicks off Phase 2:
    //   - Tokenize the search query.
    //   - If relevance mode: read stats + dict tables, then proceed to StartWordReads.
    //   - If plain mode: go directly to StartWordReads.
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

    // Process main table read results: match each returned row to the
    // corresponding TDocumentInfo (by doc_id order), store the row cells,
    // and push completed documents into ResultQueue for delivery.
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

    // Process docs table read results (Arrow format): extract doc_id and
    // doc_length, set each document's length for BM25 scoring, then continue
    // to FetchDocumentDetails for the next pipeline step.
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
            readItems.PopItem();
            documentInfos.emplace_back(doc);
        }

        DocsReadingQueue.UpdateReadStatus(readId, readItems, finished);

        FetchDocumentDetails(documentInfos);
    }

    void DocumentStatsResult(NKikimr::TEvDataShard::TEvReadResult &msg, ui64 readId, bool finished) {
        DocumentStatsResultArrow(msg, readId, finished);
    }

    // Process stats table result: extract DocCount and SumDocLength.
    // When all prerequisite reads (stats + dict) are done, proceed to StartWordReads.
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

    // Process dict table results: match returned (token, doc_frequency) rows to
    // TWordReadState entries.  Words not found in the dict get Frequency=0.
    // When all prerequisite reads are done, proceed to StartWordReads.
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

    // Process L2 posting list point-lookup results for a frequent token.
    // Feeds results into L2MergeAlgo, advances the sequential read schedule,
    // and runs L2 merge to find documents that pass both L1 and L2 checks.
    void L2WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult> msg, ui64 recReadId, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& wordInfo = Words[wordIndex];
        YQL_ENSURE(wordInfo->L2);

        L2MergeAlgo->AddResult(wordInfo->L2StreamIndex, std::move(msg));
        auto& readItems = L2ReadingQueue.GetReadItems(recReadId);
        ui64 maxKeyBarrier = L2MergeAlgo->GetMaxTokenKey(wordInfo->L2StreamIndex);
        while(!readItems.Empty() && readItems.GetItem()->Document->DocumentNumId <= maxKeyBarrier) {
            readItems.PopItem();
        }

        auto& schedule = L2ReadingQueue.GetSequentialSchedule(wordIndex);
        while(schedule.HasSentItems() && schedule.GetItem()->Document->DocumentNumId <= maxKeyBarrier) {
            schedule.PopItem();
        }

        if (finished) {
            L2ReadingQueue.ClearReadItems(readItems);
            // drop tail of items - these items doesn't exists in the database
            while(schedule.HasSentItems()) {
                schedule.PopItem();
            }
        }

        L2ReadingQueue.UpdateReadStatus(recReadId, readItems, finished);
        if (finished) {
            L2ReadingQueue.SendNextSequentialRead(IndexTableReader.Get(), EReadKind_Word_L2, wordIndex);
            if (L2ReadingQueue.GetSequentialSchedule(wordIndex).Empty() && L1MergeAlgo->Done()) {
                L2MergeAlgo->FinishTokenStream(wordInfo->L2StreamIndex);
            }
        }

        std::vector<TDocumentInfo::TPtr> matches = L2MergeAlgo->FindMatches();
        MergeL2MatchFrequencies(matches);
        FetchDocumentDetails(matches);
    }

    // For each L1-matched document, schedule point lookups in the posting table
    // for every L2 (frequent) token.  Uses Sequential() to ensure reads go to
    // one shard at a time, preserving doc_id order for the L2 merge.
    // Saves L1 matches in L1MergedDocuments so their token frequencies can be
    // merged with L2 results later.
    void ScheduleL2Read(std::vector<TDocumentInfo::TPtr>& l1matched) {
        for(int i = Words.size() - 1; i >= 0; i--) {
            auto& word = Words[i];
            if (word->L1) {
                continue;
            }

            std::vector<TL1DocumentInfo::TPtr> remappedMatches;
            remappedMatches.reserve(l1matched.size());
            for(auto& match: l1matched) {
                remappedMatches.emplace_back(MakeIntrusive<TL1DocumentInfo>(match, word->Word));
            }

            L2ReadingQueue.Sequential(IndexTableReader.Get(), EReadKind_Word_L2, remappedMatches, i);
            if (L2ReadingQueue.GetSequentialSchedule(i).Empty() && L1MergeAlgo->Done()) {
                L2MergeAlgo->FinishTokenStream(Words[i]->L2StreamIndex);
            }
        }

        L1MergedDocuments.insert(L1MergedDocuments.end(), l1matched.begin(), l1matched.end());

        std::vector<TDocumentInfo::TPtr> matches = L2MergeAlgo->FindMatches();
        CA_LOG_E("L2Merge done: " << L2MergeAlgo->Done());
        MergeL2MatchFrequencies(matches);
        FetchDocumentDetails(matches);
    }

    // Combine token frequencies from L1 and L2 matches into a single vector
    // covering all query tokens.  L1 frequencies come from L1MergedDocuments,
    // L2 frequencies come from the L2 merge result.  The combined vector is
    // stored in the match's TokenFrequencies for BM25 scoring.
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

    // Process L1 posting list read results for a token.
    // Feeds data into L1MergeAlgo, updates the resume key (StartReadKeyFrom),
    // and when the current shard is exhausted, tries to continue from the next
    // shard partition.  Runs L1 merge and routes matches to either L2 verification
    // (if L2MergeAlgo exists) or directly to FetchDocumentDetails.
    void L1WordResult(std::unique_ptr<NKikimr::TEvDataShard::TEvReadResult> msg, ui64 wordIndex, bool finished) {
        YQL_ENSURE(wordIndex < Words.size());
        auto& incomingWordInfo = Words[wordIndex];
        YQL_ENSURE(incomingWordInfo->L1);

        L1MergeAlgo->AddResult(wordIndex, std::move(msg));
        incomingWordInfo->StartReadKeyFrom = L1MergeAlgo->GetMaxTokenKey(wordIndex) + 1;

        if (finished) {
            if (!ContinueWordRead(incomingWordInfo)) {
                L1MergeAlgo->FinishTokenStream(wordIndex);
            }
        }

        std::vector<TDocumentInfo::TPtr> matches = L1MergeAlgo->FindMatches();
        if (L2MergeAlgo) {
            ScheduleL2Read(matches);
        } else {
            FetchDocumentDetails(matches);
        }
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

    void ScheduleReadRetry(ui64 readId, ui64 shardId, bool allowInstantRetry, NYql::NDqProto::StatusIds::StatusCode errorStatus) {
        auto maxRetries = MaxShardRetries();
        if (ReadsState.CheckShardRetriesExeeded(shardId, maxRetries)) {
            RuntimeError(TStringBuilder() << "Max retries (" << maxRetries << ") exceeded for read " << readId
                << " on shard " << shardId, errorStatus);
            return;
        }

        auto delay = ReadsState.GetDelay(shardId, allowInstantRetry);
        if (delay > TDuration::Zero()) {
            TlsActivationContext->Schedule(delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetrySingleRead(readId)));
        } else {
            DoRetrySingleRead(readId);
        }
    }

    // Handle error statuses in TEvReadResult.
    // OVERLOADED / INTERNAL_ERROR -> schedule retry with backoff.
    // NOT_FOUND (shard moved/split) -> reset pipe and retry.
    // Other statuses -> abort with ABORTED status.
    void HandleReadResultError(ui64 readId, const TReadInfo& readInfo, const NKikimrTxDataShard::TEvReadResult& record) {
        ui64 shardId = readInfo.ShardId;
        auto statusCode = record.GetStatus().GetCode();

        CA_LOG_W("Read result error, ReadId=" << readId
            << ", ShardId=" << shardId
            << ", Status=" << Ydb::StatusIds::StatusCode_Name(statusCode));

        switch (statusCode) {
            case Ydb::StatusIds::OVERLOADED: {
                ScheduleReadRetry(readId, shardId, false, NYql::NDqProto::StatusIds::OVERLOADED);
                return;
            }
            case Ydb::StatusIds::INTERNAL_ERROR: {
                ScheduleReadRetry(readId, shardId, true, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                return;
            }
            case Ydb::StatusIds::NOT_FOUND: {
                ReadsState.UntrackPipe(shardId);
                ScheduleReadRetry(readId, shardId, true, NYql::NDqProto::StatusIds::UNAVAILABLE);
                return;
            }
            default: {
                RuntimeError(TStringBuilder() << "Read request aborted, status: "
                    << Ydb::StatusIds::StatusCode_Name(statusCode), NYql::NDqProto::StatusIds::ABORTED);
                return;
            }
        }
    }

    // Central TEvReadResult dispatcher.
    // Validates the read id, logs diagnostics, handles errors, sends acks for
    // non-final results, then dispatches to the appropriate handler based on
    // the read's EReadKind.
    void HandleReadResult(TEvDataShard::TEvReadResult::TPtr& ev) {
        ui64 readId = ev->Get()->Record.GetReadId();
        auto& record = ev->Get()->Record;

        auto it = ReadsState.FindPtr(readId);
        if (it == nullptr) {
            return;
        }

        auto& readInfo = *it;

        CA_LOG_E("Recv TEvReadResult (full text source)"
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
            HandleReadResultError(readId, readInfo, record);
            return;
        }

        auto& msg = *ev->Get();
        ui64 cookie = readInfo.Cookie;
        auto readKind = readInfo.ReadKind;
        YQL_ENSURE(readInfo.LastSeqNo < record.GetSeqNo());
        readInfo.LastSeqNo = record.GetSeqNo();

        if (record.GetFinished()) {
            ReadsState.RemoveRead(readId, true);
        } else {
            ReadsState.AckRead(readId);
        }

        switch (readKind) {
            case EReadKind_Document:
                DocumentDetailsResult(msg, readId, record.GetFinished());
                break;
            case EReadKind_DocumentStats:
                DocumentStatsResult(msg, readId, record.GetFinished());
                break;
            case EReadKind_WordStats:
                WordStatsResult(msg, readId, record.GetFinished());
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
    using TBase = TActorBootstrapped<TFullTextSource>;
};

// Factory function: creates a TFullTextSource actor and returns both the
// IDqComputeActorAsyncInput interface pointer and the IActor pointer (they
// are the same object, but the compute actor needs both to register the
// source and to register the actor in the actor system).
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
    auto* actor = new TFullTextSource(settings, arena, computeActorId, inputIndex, statsLevel, txId, taskId, typeEnv, holderFactory, alloc, traceId, counters);
    return std::make_pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(actor, actor);
}

// Register the full-text search source type with the DQ async I/O factory.
// Called once at system startup.  The factory uses the protobuf settings type
// (TKqpFullTextSourceSettings) as the discriminator to route source creation
// requests to CreateKqpFullTextSource.
void RegisterKqpFullTextSource(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSource<NKikimrKqp::TKqpFullTextSourceSettings>(
        TString(NYql::KqpFullTextSourceName),
        [counters] (const NKikimrKqp::TKqpFullTextSourceSettings* settings, NYql::NDq::TDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateKqpFullTextSource(settings, args.Arena, args.ComputeActorId, args.InputIndex, args.StatsLevel,
        args.TxId, args.TaskId, args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters);
        });
}

}

