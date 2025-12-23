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

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NTableIndex::NFulltext;

// Structure to hold a row from one word's read stream for k-way merge
static constexpr TDuration SCHEME_CACHE_REQUEST_TIMEOUT = TDuration::Seconds(10);
using TDocumentId = const TConstArrayRef<TCell>;

class TDocumentIdPointer;

std::vector<std::pair<ui64, TOwnedTableRange>> GetRangePartitioning(const std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>& partitionInfo,
    const std::vector<NScheme::TTypeInfo>& keyColumnTypes, const TOwnedTableRange& range) {

    YQL_ENSURE(partitionInfo);

    // Binary search of the index to start with.
    size_t idxStart = 0;
    size_t idxFinish = partitionInfo->size();
    while ((idxFinish - idxStart) > 1) {
        size_t idxCur = (idxFinish + idxStart) / 2;
        const auto& partCur = (*partitionInfo)[idxCur].Range->EndKeyPrefix.GetCells();
        YQL_ENSURE(partCur.size() <= keyColumnTypes.size());
        int cmp = CompareTypedCellVectors(partCur.data(), range.From.data(), keyColumnTypes.data(),
                                          std::min(partCur.size(), range.From.size()));
        if (cmp < 0) {
            idxStart = idxCur;
        } else {
            idxFinish = idxCur;
        }
    }

    std::vector<TCell> minusInf(keyColumnTypes.size());

    std::vector<std::pair<ui64, TOwnedTableRange>> rangePartition;
    for (size_t idx = idxStart; idx < partitionInfo->size(); ++idx) {
        TTableRange partitionRange{
            idx == 0 ? minusInf : (*partitionInfo)[idx - 1].Range->EndKeyPrefix.GetCells(),
            idx == 0 ? true : !(*partitionInfo)[idx - 1].Range->IsInclusive,
            (*partitionInfo)[idx].Range->EndKeyPrefix.GetCells(),
            (*partitionInfo)[idx].Range->IsInclusive
        };

        if (range.Point) {
            int intersection = ComparePointAndRange(
                range.From,
                partitionRange,
                keyColumnTypes,
                keyColumnTypes);

            if (intersection == 0) {
                rangePartition.emplace_back((*partitionInfo)[idx].ShardId, range);
            } else if (intersection < 0) {
                break;
            }
        } else {
            int intersection = CompareRanges(range, partitionRange, keyColumnTypes);

            if (intersection == 0) {
                auto rangeIntersection = Intersect(keyColumnTypes, range, partitionRange);
                rangePartition.emplace_back((*partitionInfo)[idx].ShardId, rangeIntersection);
            } else if (intersection < 0) {
                break;
            }
        }
    }

    return rangePartition;
}

class TDocumentInfo : public TAtomicRefCount<TDocumentInfo> {
    friend class TDocumentIdPointer;

    TOwnedCellVec KeyCells;
    const TVector<NScheme::TTypeInfo>& DocumentKeyColumnTypes;
    TDocumentId DocumentId;
    std::vector<bool> ContainingWords;
    size_t NumContainingWords = 0;

public:
    ui64 DocumentNumId = 0;

    TDocumentInfo(TOwnedCellVec&& keyCells, const TVector<NScheme::TTypeInfo>& documentKeyColumnTypes, size_t numWords)
        : KeyCells(std::move(keyCells))
        , DocumentKeyColumnTypes(documentKeyColumnTypes)
        , DocumentId(KeyCells)
        , ContainingWords(numWords, false)
    {}

    bool AllWordsContained() const {
        return NumContainingWords == ContainingWords.size();
    }

    void AddContainingWord(size_t wordIndex) {
        if (!ContainingWords[wordIndex]) {
            ContainingWords[wordIndex] = true;
            NumContainingWords++;
        }
    }

    NUdf::TUnboxedValue GetRow(const NKikimr::NMiniKQL::THolderFactory& holderFactory, i64& computeBytes) const {
        NUdf::TUnboxedValue* rowItems = nullptr;
        auto row = holderFactory.CreateDirectArrayHolder(
            DocumentKeyColumnTypes.size(), rowItems);
        for(size_t i = 0; i < KeyCells.size(); ++i) {
            rowItems[i] = NMiniKQL::GetCellValue(KeyCells[i], DocumentKeyColumnTypes[i]);
            computeBytes += NMiniKQL::GetUnboxedValueSize(rowItems[i], DocumentKeyColumnTypes[i]).AllocatedBytes;
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


// Comparator for k-way merge priority queue
struct TDocumentIdComparator {
    TConstArrayRef<NScheme::TTypeInfo> DocumentKeyColumnTypes;

    TDocumentIdComparator(TConstArrayRef<NScheme::TTypeInfo> documentKeyColumnTypes)
        : DocumentKeyColumnTypes(documentKeyColumnTypes)
    {}

    bool operator()(const TDocumentId& a, const TDocumentId& b) const {
        // Compare keys - we want smallest key first (min-heap)
        int cmp = CompareTypedCellVectors(
            a.data(), b.data(), DocumentKeyColumnTypes.data(), DocumentKeyColumnTypes.size());
        return cmp > 0;
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
    {}

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
            other.DocumentInfo->DocumentKeyColumnTypes.data(), other.DocumentInfo->DocumentKeyColumnTypes.size());
        return cmp < 0;
    }
};

class TWordReadState {
public:
    ui64 WordIndex;
    TString Word;
    bool Finished = false;
    std::deque<TIntrusivePtr<TDocumentInfo>> PendingDocuments;
    // pending ranges
    std::deque<std::pair<ui64, TOwnedTableRange>> RangesToRead;

    explicit TWordReadState(ui64 wordIndex, const TString& word)
        : WordIndex(wordIndex)
        , Word(word)
    {}

    bool HasDocumentIdPointer() const {
        if (Finished) {
            return true;
        }

        return !PendingDocuments.empty();
    }

    TDocumentIdPointer GetDocumentIdPointer() const {
        if (Finished) {
            return TDocumentIdPointer(true, WordIndex, nullptr);
        }

        YQL_ENSURE(!PendingDocuments.empty());
        return TDocumentIdPointer(false, WordIndex, PendingDocuments.front());
    }
};

struct TReadInfo {
    ui64 WordIndex;
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

    ui64 ReadBytes = 0;
    ui64 ReadRows = 0;

    TString Database;
    TString LogPrefix;
    TDqAsyncStats IngressStats;

    ui64 DocumentNumId = 0;
    ui64 NextReadId = 0;

    TTableId TableId;
    IKqpGateway::TKqpSnapshot Snapshot;

    TActorId SchemeCacheRequestTimeoutTimer;
    TDuration SchemeCacheRequestTimeout;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;

    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> DocumentKeyColumnTypes;
    TVector<i32> DocumentKeyColumnIds;
    TVector<TWordReadState> Words; // Tokenized words from expression
    absl::flat_hash_map<ui64, TReadInfo> Reads;

    absl::flat_hash_map<TDocumentId, TIntrusivePtr<TDocumentInfo>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> DocumentInfos;
    size_t ActiveWordReads = 0;
    bool ResolveInProgress = true;
    bool NavigateIndexInProgress = false;

    // K-way merge state
    std::priority_queue<TDocumentIdPointer, TVector<TDocumentIdPointer>> MergeQueue;
    std::deque<TIntrusivePtr<TDocumentInfo>> ResultQueue;
    TActorId PipeCacheId;

    // Helper to bind allocator
    TGuard<NMiniKQL::TScopedAlloc> BindAllocator() {
        return TGuard<NMiniKQL::TScopedAlloc>(*Alloc);
    }

    void ExtractAndTokenizeExpression(const NKikimrSchemeOp::TFulltextIndexDescription& indexInfo) {
        YQL_ENSURE(Settings->GetQuerySettings().GetQuery().size() > 0, "Expected non-empty query");

        // Get the first expression (assuming single expression for now)
        const auto& expr = Settings->GetQuerySettings().GetQuery();
        YQL_ENSURE( Settings->GetQuerySettings().GetColumns().size() == 1);



        for(const auto& column : Settings->GetQuerySettings().GetColumns()) {

            for(const auto& analyzer : indexInfo.GetSettings().columns()) {

                if (analyzer.column() == column.GetName()) {
                    size_t wordIndex = 0;
                    for(TString query: NFulltext::Analyze(expr, analyzer.analyzers())) {
                        Words.emplace_back(TWordReadState(wordIndex++, query));
                    }
                }
            }
        }

        YQL_ENSURE(!Words.empty(), "Expression must produce at least one word after tokenization");
    }

    void StartWordReads() {
        // Initialize read states for each word

        for (auto& word : Words) {
            StartReadToken(word.WordIndex);
            ScheduleNextRead(word.WordIndex);
        }

        ActiveWordReads = Words.size();
    }

    void ScheduleNextRead(size_t wordIndex) {
        auto& wordState = Words[wordIndex];
        if (!wordState.RangesToRead.empty()) {
            auto [shardId, range] = wordState.RangesToRead.front();
            StartReadRange(shardId, wordIndex, range);
            wordState.RangesToRead.pop_front();
        }
    }

    void StartReadToken(size_t wordIndex) {
        auto& wordState = Words[wordIndex];

        TCell tokenCell(wordState.Word.data(), wordState.Word.size());
        std::vector <TCell> fromCells(DocumentKeyColumnTypes.size());
        fromCells.insert(fromCells.begin(), tokenCell);

        std::vector <TCell> toCells;
        toCells.insert(toCells.begin(), tokenCell);

        bool fromInclusive = true;
        bool toInclusive = false;

        auto partitions = GetRangePartitioning(Partitioning, KeyColumnTypes,
            TOwnedTableRange(fromCells, fromInclusive, toCells, toInclusive)
        );

        wordState.RangesToRead.insert(wordState.RangesToRead.begin(), partitions.begin(), partitions.end());
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            TString errorMsg = TStringBuilder() << "Failed to get partitioning for table. ";
            RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        if (NavigateIndexInProgress) {
            auto& resultSet = ev->Get()->Request->ResultSet;
            const auto& entry = resultSet[0];

            TableId = resultSet[0].TableId;
            TMap<i32, ui32> keyPositionToIndex;

            for (const auto& [index, columnInfo] : entry.Columns) {
                if (columnInfo.KeyOrder != -1) {
                    AFL_ENSURE(columnInfo.KeyOrder >= 0);
                    keyPositionToIndex[columnInfo.KeyOrder] = index;
                }
            }

            for (const auto& [_, index] : keyPositionToIndex) {
                const auto columnInfo = entry.Columns.FindPtr(index);
                YQL_ENSURE(columnInfo);

                KeyColumnTypes.push_back(columnInfo->PType);
                if (columnInfo->Name == TokenColumn) {
                    // dont request token column because it's not a part of document id
                    continue;
                }

                DocumentKeyColumnTypes.push_back(columnInfo->PType);
                DocumentKeyColumnIds.push_back(columnInfo->Id);
            }

            AFL_ENSURE(!keyPositionToIndex.empty());

            ResolveIndexTable();
            return;
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");

        TString indexImplTable;
        TString tablePath = NKikimr::JoinPath(resultSet[0].Path);

        for(const auto& entry : resultSet) {

            for(const auto& index : entry.Indexes) {
                if (index.GetName() == Settings->GetIndex()) {

                    ExtractAndTokenizeExpression(index.GetFulltextIndexDescription());

                    TStringBuilder indexInfo;
                    indexInfo << "Index " << index.GetName() << " for table " << index.ShortUtf8DebugString() << " has impl tables: ";
                    NYql::TIndexDescription indexDescription(index);

                    for(const auto& implTable : indexDescription.GetImplTables()) {
                        indexImplTable = TStringBuilder() << tablePath << "/" << index.GetName() << "/" << implTable;
                    }

                    TString indexInfoStr = indexInfo;
                }
            }
        }

        if (indexImplTable.empty()) {
            RuntimeError(TStringBuilder() << "Expected index " << Settings->GetIndex() << " for table " << tablePath, NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        NavigateIndexTableImpl(indexImplTable);
    }

    void NavigateIndexTableImpl(const TString& indexImplTable) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(indexImplTable);
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        NavigateIndexInProgress = true;
    }

    void NavigateIndexTable() {

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
       // SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
       //     new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout()));
    }

    void ResolveIndexTable() {

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;

        auto keyColumnTypes = KeyColumnTypes;

        TVector<TCell> minusInf(keyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));

        Counters->IteratorsShardResolve->Inc();

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        SchemeCacheRequestTimeoutTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), SchemeCacheRequestTimeout,
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvSchemeCacheRequestTimeout()));
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

    void StartReadRange(ui64 shardId, ui64 wordIndex, const TOwnedTableRange& range) {
        ui64 readId = NextReadId++;
        Reads[readId] = TReadInfo{wordIndex, shardId};
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (size_t i = 0; i < DocumentKeyColumnIds.size(); i++) {
            record.AddColumns(DocumentKeyColumnIds[i]);
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
        , Database(Settings->GetDatabase())
        , LogPrefix(TStringBuilder() << "TxId: " << txId << ", task: " << taskId << ", CA Id " << computeActorId << ". ")
        , SchemeCacheRequestTimeout(SCHEME_CACHE_REQUEST_TIMEOUT)
        , PipeCacheId(NKikimr::MakePipePerNodeCacheID(false))
    {
        Y_ABORT_UNLESS(Arena);
        Y_ABORT_UNLESS(Settings->GetArena() == Arena->Get());

        IngressStats.Level = statsLevel;

        TableId = TTableId(
            (ui64)Settings->GetTable().GetOwnerId(),
            (ui64)Settings->GetTable().GetTableId(),
            Settings->GetTable().GetSysView(),
            (ui64)Settings->GetTable().GetVersion()
        );

        if (Settings->HasSnapshot()) {
            Snapshot = IKqpGateway::TKqpSnapshot(
                Settings->GetSnapshot().GetStep(),
                Settings->GetSnapshot().GetTxId());
        }

        // Extract key column types
        //KeyColumnTypes.reserve(Settings->GetColumns().size());
        //for (const auto& keyColumn : Settings->GetColumns()) {
        //    NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
        //    KeyColumnTypes.push_back(typeInfo);
        //}

        // Extract and tokenize expression
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TFullTextContainsSource::StateWork);
        // ResolveIndexTable();
        NavigateIndexTable();
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
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleReadResult);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
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
            TString errorMsg = TStringBuilder() << "Failed to get partitioning for table. ";
            return RuntimeError(errorMsg, NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1, "Expected one result for range [NULL, +inf)");
        Partitioning = resultSet[0].KeyDescription->Partitioning;

        StartWordReads();
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

        auto wordIndex = it->second.WordIndex;
        YQL_ENSURE(wordIndex < Words.size());
        auto& wordInfo = Words[wordIndex];

        CA_LOG_E("Recv TEvReadResult (full text source)"
            << ", WordIndex=" << wordIndex
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

        if (record.GetFinished()) {
            Reads.erase(readId);
            ScheduleNextRead(wordIndex);
        } else {
            AckRead(readId, record.GetSeqNo());
        }

        auto& msg = *ev->Get();

        for(size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& row = msg.GetCells(i);

            auto docInfo = MakeIntrusive<TDocumentInfo>(TOwnedCellVec(row), DocumentKeyColumnTypes, Words.size());
            auto [it, success] = DocumentInfos.emplace(row, std::move(docInfo));
            if (success) {
                it->second->DocumentNumId = DocumentNumId++;
            }

            it->second->AddContainingWord(wordIndex);
            wordInfo.PendingDocuments.push_back(it->second);
            if (wordInfo.HasDocumentIdPointer() && wordInfo.PendingDocuments.size() == 1) {
                MergeQueue.push(wordInfo.GetDocumentIdPointer());
            }
        }


        while (MergeQueue.size() == Words.size()) {

            CA_LOG_E("MergeQueue size = " << MergeQueue.size());

            const auto& documentIdPointer = MergeQueue.top();
            TIntrusivePtr<TDocumentInfo> documentInfo = documentIdPointer.DocumentInfo;
            if (documentInfo->AllWordsContained()) {
                ResultQueue.push_back(documentInfo);
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

        CA_LOG_E("ResultQueue size = " << ResultQueue.size());

        if (!ResultQueue.empty()) {
            NotifyCA();
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