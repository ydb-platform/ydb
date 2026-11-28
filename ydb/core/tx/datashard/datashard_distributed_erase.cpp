#include "datashard_distributed_erase.h"
#include "datashard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/datashard/build_index/vector_resolve_actor.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <yql/essentials/public/udf/udf_types.h>

#include <util/generic/bitmap.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/string/builder.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

class TDistEraser: public TActorBootstrapped<TDistEraser> {
    using TEvResponse = NKikimrTxDataShard::TEvEraseRowsResponse;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResolve = NSchemeCache::TSchemeCacheRequest;
    using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySet;

    class TTableInfo {
        template <typename T, typename TExtractor>
        static TVector<T> MakeKeyColumnSmth(const TNavigate::TEntry& entry, TExtractor extractor) {
            TVector<T> result;

            for (const auto& [_, column] : entry.Columns) {
                if (column.KeyOrder < 0) {
                    continue;
                }

                if (result.size() <= static_cast<ui32>(column.KeyOrder)) {
                    result.resize(column.KeyOrder + 1);
                }

                result[column.KeyOrder] = extractor(column);
            }

            return result;
        }

        static TKeyMap MakeSelfKeyMap(const TNavigate::TEntry& entry) {
            return MakeKeyColumnSmth<std::pair<ui32, ui32>>(entry, [](auto column) {
                return std::make_pair(column.Id, column.Id);
            });
        }

        static TVector<NScheme::TTypeInfo> MakeKeyColumnTypes(const TNavigate::TEntry& entry) {
            return MakeKeyColumnSmth<NScheme::TTypeInfo>(entry, [](auto column) {
                return column.PType;
            });
        }

        static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
            TVector<TCell> fromValues(keyColumnsCount);
            TVector<TCell> toValues;
            return TSerializedTableRange(fromValues, true, toValues, false);
        }

        static THolder<TKeyDesc> MakeKeyDesc(const TNavigate::TEntry& entry) {
            const TVector<NScheme::TTypeInfo> keyColumnTypes = MakeKeyColumnTypes(entry);
            return MakeHolder<TKeyDesc>(
                entry.TableId,
                GetFullRange(keyColumnTypes.size()).ToTableRange(),
                TKeyDesc::ERowOperation::Erase,
                keyColumnTypes,
                TVector<TKeyDesc::TColumnOp>()
            );
        }

    public:
        explicit TTableInfo(const TNavigate::TEntry& entry, const TKeyMap& keyMap)
            : KeyMap(keyMap ? keyMap : MakeSelfKeyMap(entry))
            , KeyDesc(MakeKeyDesc(entry))
        {
        }

        const TKeyMap& GetKeyMap() const {
            return KeyMap;
        }

        THolder<TKeyDesc> TakeKeyDesc() {
            return std::move(KeyDesc);
        }

        void SetKeyDesc(THolder<TKeyDesc> keyDesc) {
            KeyDesc = std::move(keyDesc);
        }

        const TKeyDesc* GetKeyDesc() const {
            return KeyDesc.Get();
        }

    public:
        uintptr_t ToUserDataHandle() {
            return reinterpret_cast<uintptr_t>(this);
        }

        static TTableInfo& FromUserDataHandle(uintptr_t userData) {
            return *reinterpret_cast<TTableInfo*>(userData);
        }

    private:
        const TKeyMap KeyMap;
        THolder<TKeyDesc> KeyDesc;

    }; // TTableInfo

    class TShardKeys {
    public:
        void Add(TString&& key, size_t row) {
            Keys.emplace_back(std::move(key));
            PresentRows.Set(row);
        }

        // Adds a key that does not correspond to its own main table row, e.g. a
        // posting table key of a vector index: one row may resolve to several
        // clusters and thus to several keys.
        void AddMapped(TString&& key, ui32 mainRow) {
            const size_t keyIndex = Keys.size();
            Keys.emplace_back(std::move(key));
            PresentRows.Set(keyIndex);
            MainRowIndexes.push_back(mainRow);
        }

        TVector<TString>& GetKeys() {
            return Keys;
        }

        const TDynBitMap& GetPresentRows() const {
            return PresentRows;
        }

        const TVector<ui32>& GetMainRowIndexes() const {
            return MainRowIndexes;
        }

    private:
        TVector<TString> Keys;
        TDynBitMap PresentRows;
        TVector<ui32> MainRowIndexes;
    }; // TShardKeys

    // Resolution of main table rows into posting clusters for one vector index.
    struct TVectorResolveState {
        size_t Index = 0;
        NActors::TActorId ResolverId;
        ui64 RequestId = 0;
        bool InFlight = false;
        TVector<TString> Vectors;
        TVector<TSerializedCellVec> Prefixes;
        TVector<TVector<NTableIndex::NKMeans::TClusterId>> Clusters;
        // Main table key cells in the posting key order, per request row.
        TVector<TSerializedCellVec> MainKeys;
    };

    void Reply(TEvResponse::EStatus status = TEvResponse::OK, const TString& error = TString()) {
        if (status == TEvResponse::OK) {
            YDB_LOG_DEBUG("[DistEraser] Reply",
                {"selfId", SelfId()},
                {"txId", TxId},
                {"status", status},
                {"error", error});
        } else {
            YDB_LOG_ERROR("[DistEraser] Reply",
                {"selfId", SelfId()},
                {"txId", TxId},
                {"status", status},
                {"error", error});
        }

        auto response = MakeHolder<TEvDataShard::TEvEraseRowsResponse>();
        auto& record = response->Record;
        record.SetStatus(status);
        record.SetErrorDescription(error);

        Send(ReplyTo, std::move(response));
        PassAway();
    }

    void SchemeError(const TString& error) {
        Reply(TEvResponse::SCHEME_ERROR, error);
    }

    void BadRequest(const TString& error) {
        Reply(TEvResponse::BAD_REQUEST, error);
    }

    void ExecError(const TString& error) {
        Reply(TEvResponse::EXEC_ERROR, error);
    }

    void CoordinatorDeclined(const TString& error) {
        Reply(TEvResponse::COORDINATOR_DECLINED, error);
    }

    void CoordinatorUnknown(const TString& error) {
        Reply(TEvResponse::COORDINATOR_UNKNOWN, error);
    }

    void ShardUnknown(const TString& error) {
        Reply(TEvResponse::SHARD_UNKNOWN, error);
    }

    void DeliveryProblem() {
        Reply(TEvResponse::UNKNOWN, "Unexpected delivery problem");
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result, const TStringBuf marker) {
        if (result) {
            return true;
        }

        SchemeError(TStringBuilder() << "Empty result from scheme cache at '" << marker << "'");
        return false;
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, const TStringBuf marker, ui32 expected) {
        if (result->ResultSet.size() == expected) {
            return true;
        }

        SchemeError(TStringBuilder() << "Entries count mismatch at '" << marker << "'"
            << ": expected# " << expected
            << ", actual# " << result->ResultSet.size()
            << ", result# " << result->ToString(*AppData()->TypeRegistry));
        return false;
    }

    static bool IsSucceeded(TNavigate::EStatus status) {
        return status == TNavigate::EStatus::Ok;
    }

    static bool IsSucceeded(TResolve::EStatus status) {
        return status == TResolve::EStatus::OkData;
    }

    static TTableId GetTableId(const TNavigate::TEntry& entry) {
        return entry.TableId;
    }

    static TTableId GetTableId(const TResolve::TEntry& entry) {
        return entry.KeyDescription->TableId;
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry, const TStringBuf marker) {
        if (IsSucceeded(entry.Status)) {
            return true;
        }

        SchemeError(TStringBuilder() << "Failed to resolve table at '" << marker << "'"
            << ": tableId# " << GetTableId(entry)
            << ", status# " << entry.Status);
        return false;
    }

    static TNavigate::TEntry MakeNavigateEntry(const TTableId& tableId) {
        TNavigate::TEntry entry;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.TableId = tableId;
        entry.Operation = TNavigate::OpTable;
        entry.ShowPrivatePath = true;
        return entry;
    }

    void AddTableInfo(const TNavigate::TEntry& entry, const TKeyMap& keyMap = {}) {
        Y_DEBUG_ABORT_UNLESS(!TableInfos.contains(entry.TableId));
        TableInfos.emplace(entry.TableId, TTableInfo(entry, keyMap));
    }

    static ui64 GetShardId(const TTableRange& range, const TKeyDesc* keyDesc) {
        Y_ENSURE(range.Point);
        Y_ENSURE(!keyDesc->GetPartitions().empty());

        TVector<TKeyDesc::TPartitionInfo>::const_iterator it = LowerBound(
            keyDesc->GetPartitions().begin(), keyDesc->GetPartitions().end(), true,
            [&](const TKeyDesc::TPartitionInfo& partition, bool) {
                const int compares = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, keyDesc->KeyColumnTypes
                );

                return (compares < 0);
            }
        );

        Y_ENSURE(it != keyDesc->GetPartitions().end());
        return it->ShardId;
    }

    void CancelProposal(ui64 exceptShardId = 0) {
        for (const ui64 shardId : Shards) {
            if (shardId == exceptShardId) {
                continue;
            }

            YDB_LOG_DEBUG("[DistEraser] Cancel proposal",
                {"selfId", SelfId()},
                {"txId", TxId},
                {"shard", shardId});

            auto cancel = MakeHolder<TEvDataShard::TEvCancelTransactionProposal>(TxId);
            Send(LeaderPipeCache, new TEvPipeCache::TEvForward(cancel.Release(), shardId, false));
        }
    }

    /// Allocate tx id

    void AllocateTxId() {
        YDB_LOG_DEBUG("[DistEraser] AllocateTxId",
            {"selfId", SelfId()});

        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StatePrepareBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        YDB_LOG_DEBUG("[DistEraser] Handle TEvTxUserProxy::TEvAllocateTxIdResult",
            {"selfId", SelfId()});

        TxId = ev->Get()->TxId;
        LeaderPipeCache = ev->Get()->Services.LeaderPipeCache;
        Mon = ev->Get()->TxProxyMon;

        ResolveTables();
    }

    /// Resolve tables

    void ResolveTables() {
        YDB_LOG_DEBUG("[DistEraser] Resolve tables",
            {"selfId", SelfId()},
            {"txId", TxId});

        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = DatabaseName;
        request->ResultSet.emplace_back(MakeNavigateEntry(MainTableId));
        for (const auto& [tableId, _] : Indexes) {
            request->ResultSet.emplace_back(MakeNavigateEntry(tableId));
        }
        for (const auto& index : VectorIndexes) {
            request->ResultSet.emplace_back(MakeNavigateEntry(index.TableId));
        }

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveTables);
    }

    STATEFN(StateResolveTables) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return StatePrepareBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        const TStringBuf marker = "ResolveTables";

        YDB_LOG_DEBUG("[DistEraser] Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"selfId", SelfId()},
            {"request", (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!CheckNotEmpty(request, marker)) {
            return;
        }

        if (!CheckEntriesCount(request, marker, 1 /* main */ + Indexes.size() + VectorIndexes.size())) {
            return;
        }

        // main
        {
            const auto& entry = request->ResultSet.at(0);
            if (!CheckEntrySucceeded(entry, marker)) {
                return;
            }

            if (entry.TableId.PathId != MainTableId.PathId) {
                return SchemeError(TStringBuilder() << "Main table's path id mismatch"
                    << ": expected# " << MainTableId.PathId
                    << ", actual# " << entry.TableId.PathId
                    << ", entry# " << entry.ToString());
            }

            if (entry.TableId.SchemaVersion != MainTableId.SchemaVersion) {
                return SchemeError(TStringBuilder() << "Main table's schema version mismatch"
                    << ": expected# " << MainTableId.SchemaVersion
                    << ", actual# " << entry.TableId.SchemaVersion
                    << ", entry# " << entry.ToString());
            }

            ui32 nIndexes = 0;
            for (const auto& index : entry.Indexes) {
                if (index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                    continue;
                }

                const auto tableId = TTableId(index.GetPathOwnerId(), index.GetLocalPathId(), index.GetSchemaVersion());
                switch (index.GetState()) {
                case NKikimrSchemeOp::EIndexStateReady:
                case NKikimrSchemeOp::EIndexStateWriteOnly:
                case NKikimrSchemeOp::EIndexStateNotReady:
                    break;
                case NKikimrSchemeOp::EIndexStateInvalid:
                    return SchemeError(TStringBuilder() << "Invalid index state"
                        << ": tableId# " << tableId
                        << ", name# " << index.GetName());
                default:
                    return SchemeError(TStringBuilder() << "Unknown index state"
                        << ": tableId# " << tableId
                        << ", name# " << index.GetName()
                        << ", state# " << static_cast<ui32>(index.GetState()));
                }

                // Keep in sync with TSchemeShard::TTxRunConditionalErase::FillVectorIndexes:
                // a vector index is only erased once its level table is complete
                // (Ready or WriteOnly), not while it is still being built.
                if (index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree
                    && index.GetState() != NKikimrSchemeOp::EIndexStateReady
                    && index.GetState() != NKikimrSchemeOp::EIndexStateWriteOnly)
                {
                    continue;
                }

                // Indexes that do not support TTL are not erased here.
                if (!NTableIndex::DoesIndexSupportTTL(index.GetType())) {
                    continue;
                }

                ++nIndexes;
            }

            if (nIndexes != Indexes.size() + VectorIndexes.size()) {
                return SchemeError(TStringBuilder() << "Indexes count mismatch"
                    << ": expected# " << Indexes.size() + VectorIndexes.size()
                    << ", actual# " << nIndexes
                    << ", entry# " << entry.ToString());
            }

            AddTableInfo(entry);
        }

        // indexes
        THashSet<TTableId> indexesSeen;

        for (ui32 i = 1; i < request->ResultSet.size(); ++i) {
            const auto& entry = request->ResultSet.at(i);
            if (!CheckEntrySucceeded(entry, marker)) {
                return;
            }

            if (!indexesSeen.insert(entry.TableId).second) {
                return SchemeError(TStringBuilder() << "Duplicate index"
                    << ": tableId# " << entry.TableId
                    << ", path# " << JoinPath(entry.Path));
            }

            if (auto it = Indexes.find(entry.TableId); it != Indexes.end()) {
                AddTableInfo(entry, it->second);
            } else if (auto it = VectorIndexByTableId.find(entry.TableId); it != VectorIndexByTableId.end()) {
                AddTableInfo(entry, VectorIndexes.at(it->second).KeyMap);
            } else {
                return SchemeError(TStringBuilder() << "Unknown index"
                    << " (probably schema version mismatch)"
                    << ": tableId# " << entry.TableId
                    << ", path# " << JoinPath(entry.Path));
            }
        }

        if (indexesSeen.size() != Indexes.size() + VectorIndexes.size()) {
            return SchemeError(TStringBuilder() << "Incomplete indexes"
                << ": expected# " << Indexes.size() + VectorIndexes.size()
                << ", actual# " << indexesSeen.size());
        }

        NSchemeCache::TDomainInfo::TPtr domainInfo;
        for (const auto& entry : request->ResultSet) {
            if (!entry.DomainInfo) {
                return SchemeError(TStringBuilder() << "Empty domain info"
                    << ": entry# " << entry.ToString());
            }

            if (!domainInfo) {
                domainInfo = entry.DomainInfo;
                continue;
            }

            if (domainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                return SchemeError(TStringBuilder() << "Failed locality check"
                    << ": expected# " << domainInfo->DomainKey
                    << ", actual# " << entry.DomainInfo->DomainKey
                    << ", entry# " << entry.ToString());
            }
        }

        Y_ENSURE(TxId);
        SelectedCoordinator = domainInfo->Coordinators.Select(TxId);

        ResolveKeys();
    }

    /// Resolve keys

    void ResolveKeys() {
        YDB_LOG_DEBUG("[DistEraser] Resolve keys",
            {"selfId", SelfId()},
            {"txId", TxId});

        Y_ENSURE(!TableInfos.empty());

        auto request = MakeHolder<TResolve>();
        request->DatabaseName = DatabaseName;

        for (auto& [_, info] : TableInfos) {
            auto& entry = request->ResultSet.emplace_back(info.TakeKeyDesc());
            entry.Access = NACLib::EAccessRights::EraseRow;
            entry.UserData = info.ToUserDataHandle();
        }

        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        Become(&TThis::StateResolveKeys);

        ResolvingKeys = true;
    }

    STATEFN(StateResolveKeys) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
        default:
            return StatePrepareBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        ResolvingKeys = false;

        if (Cancelled) {
            return PassAway();
        }

        const auto& request = ev->Get()->Request;
        const TStringBuf marker = "ResolveKeys";

        YDB_LOG_DEBUG("[DistEraser] Handle TEvTxProxySchemeCache::TEvResolveKeySetResult",
            {"selfId", SelfId()},
            {"request", (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!CheckNotEmpty(request, marker)) {
            return;
        }

        if (!CheckEntriesCount(request, marker, TableInfos.size())) {
            return;
        }

        for (ui32 i = 0; i < request->ResultSet.size(); ++i) {
            auto& entry = request->ResultSet.at(i);
            if (!CheckEntrySucceeded(entry, marker)) {
                return;
            }

            if (entry.KeyDescription->GetPartitions().empty()) {
                return SchemeError(TStringBuilder() << "Empty partitions list"
                    << ": entry# " << entry.ToString(*AppData()->TypeRegistry));
            }

            auto& info = TTableInfo::FromUserDataHandle(entry.UserData);
            info.SetKeyDesc(std::move(entry.KeyDescription));
        }

        if (Request) {
            Handle(Request);
        } else {
            Become(&TThis::StateAwaitRequest);
        }
    }

    /// Process request

    STATEFN(StateAwaitRequest) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvEraseRowsRequest, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    void Store(TEvDataShard::TEvEraseRowsRequest::TPtr& ev) {
        YDB_LOG_DEBUG("[DistEraser] Store TEvDataShard::TEvEraseRowsRequest",
            {"selfId", SelfId()});
        Request = ev;
    }

    void Handle(TEvDataShard::TEvEraseRowsRequest::TPtr& ev) {
        YDB_LOG_DEBUG("[DistEraser] Handle TEvDataShard::TEvEraseRowsRequest",
            {"selfId", SelfId()});

        Request = ev;

        const auto& record = ev->Get()->Record;

        switch (record.GetConditionCase()) {
        case NKikimrTxDataShard::TEvEraseRowsRequest::kExpiration:
            break;
        default:
            return BadRequest(TStringBuilder() << "Unknown condition"
                << ": condition# " << static_cast<ui32>(record.GetConditionCase()));
        }

        if (!record.KeyColumnsSize()) {
            return Reply(TEvResponse::OK);
        }

        THashMap<ui32, ui32> keyColumnIdToIdx;
        for (ui32 i = 0; i < record.KeyColumnIdsSize(); ++i) {
            Y_DEBUG_ABORT_UNLESS(!keyColumnIdToIdx.contains(record.GetKeyColumnIds(i)));
            keyColumnIdToIdx.emplace(record.GetKeyColumnIds(i), i);
        }

        TVector<ui32> indexColumnIds;
        {
            Y_ENSURE(TableInfos.contains(MainTableId));
            const auto& mainTableInfo = TableInfos.at(MainTableId);

            THashSet<ui32> mainTableKeys;
            for (const auto& [_, id] : mainTableInfo.GetKeyMap()) {
                Y_DEBUG_ABORT_UNLESS(!mainTableKeys.contains(id));
                mainTableKeys.insert(id);
            }

            THashSet<ui32> seen;
            auto addIndexColumn = [&](ui32 id) {
                if (!mainTableKeys.contains(id) && seen.insert(id).second) {
                    indexColumnIds.push_back(id);
                }
            };

            for (const auto& id : record.GetKeyColumnIds()) {
                addIndexColumn(id);
            }

            // Vector index columns are needed both to check that the row has not
            // changed since the scan and to resolve the row into posting clusters.
            for (const auto& index : VectorIndexes) {
                addIndexColumn(index.EmbeddingColumnId);
                for (const ui32 id : index.PrefixColumnIds) {
                    addIndexColumn(id);
                }
            }

            for (const ui32 id : indexColumnIds) {
                if (!keyColumnIdToIdx.contains(id)) {
                    return BadRequest(TStringBuilder() << "Index column is absent"
                        << ": columnId# " << id);
                }
            }
        }

        Keys.clear();
        VectorResolveStates.assign(VectorIndexes.size(), TVectorResolveState());
        for (size_t i = 0; i < VectorIndexes.size(); ++i) {
            VectorResolveStates[i].Index = i;
        }

        TVector<TString> indexColumnValues(Reserve(record.KeyColumnsSize()));
        for (ui32 i = 0; i < record.KeyColumnsSize(); ++i) {
            const auto& serializedKey = record.GetKeyColumns(i);

            TSerializedCellVec keyCells;
            if (!TSerializedCellVec::TryParse(serializedKey, keyCells)) {
                return BadRequest(TStringBuilder() << "Cannot parse key"
                    << ": serialized# " << serializedKey);
            }

            for (const auto& [tableId, info] : TableInfos) {
                if (VectorIndexByTableId.contains(tableId)) {
                    continue;
                }

                TVector<TCell> cells(Reserve(info.GetKeyMap().size()));
                for (const auto& [_, id] : info.GetKeyMap()) {
                    if (!keyColumnIdToIdx.contains(id)) {
                        return BadRequest(TStringBuilder() << "Key column is absent"
                            << ": tableId# " << tableId
                            << ", columnId# " << id);
                    }

                    cells.push_back(keyCells.GetCells()[keyColumnIdToIdx.at(id)]);
                }

                const ui64 shardId = GetShardId(TTableRange(cells), info.GetKeyDesc());

                auto it = Keys[tableId].find(shardId);
                if (it == Keys[tableId].end()) {
                    it = Keys[tableId].emplace(shardId, TShardKeys()).first;
                }

                it->second.Add(TSerializedCellVec::Serialize(cells), i);
            }

            for (auto& state : VectorResolveStates) {
                const auto& index = VectorIndexes[state.Index];

                TVector<TCell> mainCells(Reserve(index.KeyMap.size()));
                for (const auto& [_, id] : index.KeyMap) {
                    if (!keyColumnIdToIdx.contains(id)) {
                        return BadRequest(TStringBuilder() << "Key column is absent"
                            << ": tableId# " << index.TableId
                            << ", columnId# " << id);
                    }

                    mainCells.push_back(keyCells.GetCells()[keyColumnIdToIdx.at(id)]);
                }
                state.MainKeys.emplace_back(mainCells);

                const TCell& embedding = keyCells.GetCells()[keyColumnIdToIdx.at(index.EmbeddingColumnId)];
                if (embedding.IsNull() || !embedding.Size()) {
                    state.Vectors.emplace_back();
                } else {
                    state.Vectors.emplace_back(embedding.Data(), embedding.Size());
                }

                if (!index.PrefixColumnIds.empty()) {
                    TVector<TCell> prefixCells(Reserve(index.PrefixColumnIds.size()));
                    for (const ui32 id : index.PrefixColumnIds) {
                        prefixCells.push_back(keyCells.GetCells()[keyColumnIdToIdx.at(id)]);
                    }
                    state.Prefixes.emplace_back(prefixCells);
                }
            }

            TVector<TCell> indexCells(Reserve(indexColumnIds.size()));
            for (const ui32 id : indexColumnIds) {
                indexCells.push_back(keyCells.GetCells()[keyColumnIdToIdx.at(id)]);
            }
            indexColumnValues.push_back(TSerializedCellVec::Serialize(indexCells));
        }

        Y_ENSURE(Keys.contains(MainTableId));
        if (Keys.at(MainTableId).size() > 1) {
            return ExecError(TStringBuilder() << "Too many main table's shards"
                << ": tableId# " << MainTableId
                << ", expected# " << 1
                << ", actual# " << Keys.at(MainTableId).size());
        }

        IndexColumnIds = std::move(indexColumnIds);
        IndexColumnValues = std::move(indexColumnValues);

        if (VectorResolveStates.empty()) {
            return ProposeKeys();
        }

        return StartVectorResolve();
    }

    void ProposeKeys() {
        const auto& record = Request->Get()->Record;

        auto getDependents = [this]() {
            THashSet<ui64> dependents;

            for (const auto& [tableId, shards] : Keys) {
                if (tableId != MainTableId) {
                    for (const auto& [shardId, _] : shards) {
                        Y_DEBUG_ABORT_UNLESS(!dependents.contains(shardId));
                        dependents.insert(shardId);
                    }
                }
            }

            return dependents;
        };

        for (auto& [tableId, data] : Keys) {
            Y_ENSURE(TableInfos.contains(tableId));
            const auto& keyMap = TableInfos.at(tableId).GetKeyMap();
            const auto vectorIt = VectorIndexByTableId.find(tableId);

            for (auto& kv : data) {
                const ui64 shardId = kv.first;
                auto& shardKeys = kv.second;

                NKikimrTxDataShard::TDistributedEraseTransaction tx;

                auto& request = *tx.MutableEraseRowsRequest();
                request.SetTableId(tableId.PathId.LocalPathId);
                request.SetSchemaVersion(tableId.SchemaVersion);

                if (vectorIt != VectorIndexByTableId.end()) {
                    request.AddKeyColumnIds(VectorIndexes.at(vectorIt->second).ParentColumnId);
                }

                for (const auto& [id, _] : keyMap) {
                    request.AddKeyColumnIds(id);
                }

                Y_ENSURE(shardKeys.GetKeys().size() == shardKeys.GetPresentRows().Count(), "Rows count mismatch"
                    << ": expected# " << shardKeys.GetKeys().size()
                    << ", actual# " << shardKeys.GetPresentRows().Count());

                for (TString& key : shardKeys.GetKeys()) {
                    request.AddKeyColumns(std::move(key));
                }

                switch (record.GetConditionCase()) {
                case NKikimrTxDataShard::TEvEraseRowsRequest::kExpiration:
                    request.MutableExpiration()->CopyFrom(record.GetExpiration());
                    break;
                default:
                    Y_ENSURE(false, "Unknown condition: " << static_cast<ui32>(record.GetConditionCase()));
                }

                if (tableId == MainTableId) {
                    for (const ui64 shardId : getDependents()) {
                        auto& dependents = *tx.AddDependents();
                        dependents.SetShardId(shardId);
                    }

                    for (const auto& id : IndexColumnIds) {
                        tx.AddIndexColumnIds(id);
                    }

                    for (auto& value : IndexColumnValues) {
                        tx.AddIndexColumns(std::move(value));
                    }
                } else {
                    Y_ENSURE(Keys.contains(MainTableId));

                    auto& dependency = *tx.AddDependencies();
                    dependency.SetShardId(Keys.at(MainTableId).begin()->first);
                    dependency.SetPresentRows(SerializeBitMap(shardKeys.GetPresentRows()));
                    for (const ui32 mainRow : shardKeys.GetMainRowIndexes()) {
                        dependency.AddMainRowIndexes(mainRow);
                    }
                }

                YDB_LOG_DEBUG("[DistEraser] Propose tx",
                    {"selfId", SelfId()},
                    {"txId", TxId},
                    {"shard", shardId},
                    {"keys", request.KeyColumnsSize()},
                    {"dependents", tx.DependentsSize()},
                    {"dependencies", tx.DependenciesSize()});

                auto propose = MakeHolder<TEvDataShard::TEvProposeTransaction>(
                    NKikimrTxDataShard::TX_KIND_DISTRIBUTED_ERASE, SelfId(), TxId, tx.SerializeAsString()
                );

                Send(LeaderPipeCache, new TEvPipeCache::TEvForward(propose.Release(), shardId, true));

                Y_DEBUG_ABORT_UNLESS(!Shards.contains(shardId));
                Shards.insert(shardId);

                Y_DEBUG_ABORT_UNLESS(!PendingPrepare.contains(shardId));
                PendingPrepare.insert(shardId);
            }
        }

        Become(&TThis::StatePropose);
    }

    void StartVectorResolve() {
        VectorResolvesRemaining = 0;

        for (const auto& index : VectorIndexes) {
            const auto& info = TableInfos.at(index.TableId);
            if (info.GetKeyMap().size() + 1 != info.GetKeyDesc()->KeyColumnTypes.size()) {
                return ExecError(TStringBuilder() << "Unexpected vector index key layout"
                    << ": tableId# " << index.TableId
                    << ", mappedKeys# " << info.GetKeyMap().size()
                    << ", keyColumns# " << info.GetKeyDesc()->KeyColumnTypes.size());
            }
        }

        for (auto& state : VectorResolveStates) {
            const auto& index = VectorIndexes[state.Index];

            bool hasVectors = false;
            for (const auto& vector : state.Vectors) {
                if (!vector.empty()) {
                    hasVectors = true;
                    break;
                }
            }

            state.Clusters.resize(state.MainKeys.size());
            if (!hasVectors) {
                continue;
            }

            NBuildIndex::TVectorResolveSettings settings;
            settings.DatabaseName = DatabaseName;
            settings.LevelTablePath = index.LevelTablePath;
            settings.PrefixTablePath = index.PrefixTablePath;
            settings.PrefixColumns = index.PrefixColumnNames;
            settings.IndexSettings = index.Settings;
            settings.OverlapClusters = index.OverlapClusters;
            settings.OverlapRatio = index.OverlapRatio;
            // TTL conditional erase has no snapshot: the main table is scanned
            // at the current state and the erase is re-validated by the main
            // transaction. Snapshot in the resolver only applies to level table
            // reads, while the prefix table is always read from its latest
            // version (it is mutable: new prefixes are inserted on demand), so
            // a snapshot would not make prefix resolution consistent.
            settings.Snapshot = Nothing();

            state.ResolverId = RegisterWithSameMailbox(NBuildIndex::CreateVectorResolveActor(settings));
            state.RequestId = NextVectorResolveRequestId++;

            auto request = MakeHolder<NBuildIndex::TEvVectorResolveRequest>();
            request->RequestId = state.RequestId;
            request->Vectors = state.Vectors;
            request->Prefixes = state.Prefixes;

            YDB_LOG_DEBUG("[DistEraser] Resolve vector index",
                {"selfId", SelfId()},
                {"tableId", index.TableId.PathId.LocalPathId},
                {"rows", state.Vectors.size()});

            state.InFlight = true;
            ++VectorResolvesRemaining;
            Send(state.ResolverId, request.Release());
        }

        if (!VectorResolvesRemaining) {
            BuildVectorKeys();
            return ProposeKeys();
        }

        Become(&TThis::StateResolveVectors);
    }

    void HandleVectorResolve(NBuildIndex::TEvVectorResolveResponse::TPtr& ev) {
        if (Cancelled) {
            return;
        }

        auto* msg = ev->Get();

        TVectorResolveState* state = nullptr;
        for (auto& candidate : VectorResolveStates) {
            if (candidate.InFlight && candidate.RequestId == msg->RequestId) {
                state = &candidate;
                break;
            }
        }

        if (!state) {
            return;
        }

        state->InFlight = false;
        if (!msg->Success) {
            return ExecError(TStringBuilder() << "Cannot resolve vector index rows"
                << ": index# " << VectorIndexes[state->Index].TableId
                << ", issues# " << msg->Issues.ToString());
        }

        if (msg->Clusters.size() != state->MainKeys.size()) {
            return ExecError(TStringBuilder() << "Vector index rows count mismatch"
                << ": index# " << VectorIndexes[state->Index].TableId
                << ", expected# " << state->MainKeys.size()
                << ", actual# " << msg->Clusters.size());
        }

        state->Clusters = std::move(msg->Clusters);
        if (--VectorResolvesRemaining) {
            return;
        }

        BuildVectorKeys();
        return ProposeKeys();
    }

    void BuildVectorKeys() {
        for (const auto& state : VectorResolveStates) {
            const auto& index = VectorIndexes[state.Index];
            const auto& info = TableInfos.at(index.TableId);

            Y_ENSURE(state.Clusters.size() == state.MainKeys.size());

            for (size_t row = 0; row < state.Clusters.size(); ++row) {
                const auto& mainKey = state.MainKeys[row];
                for (const auto cluster : state.Clusters[row]) {
                    TVector<TCell> cells(Reserve(1 + mainKey.GetCells().size()));
                    cells.push_back(TCell::Make(cluster));
                    for (const auto& cell : mainKey.GetCells()) {
                        cells.push_back(cell);
                    }

                    const ui64 shardId = GetShardId(TTableRange(cells), info.GetKeyDesc());

                    auto it = Keys[index.TableId].find(shardId);
                    if (it == Keys[index.TableId].end()) {
                        it = Keys[index.TableId].emplace(shardId, TShardKeys()).first;
                    }

                    it->second.AddMapped(TSerializedCellVec::Serialize(cells), static_cast<ui32>(row));
                }
            }
        }
    }

    /// Propose

    STATEFN(StatePropose) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvProposeTransactionResult, HandlePropose);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePropose);
        default:
            return StateExecuteBase(ev);
        }
    }

    void HandlePropose(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        const auto* msg = ev->Get();

        const ui64 shardId = msg->GetOrigin();
        if (!PendingPrepare.contains(shardId)) {
            return;
        }

        const auto status = msg->GetStatus();
        YDB_LOG_DEBUG("[DistEraser] HandlePropose TEvDataShard::TEvProposeTransactionResult",
            {"selfId", SelfId()},
            {"txId", TxId},
            {"shard", shardId},
            {"status", static_cast<ui32>(status)});

        auto error = [&](TEvResponse::EStatus code, const TStringBuf header) {
            return Reply(code, TStringBuilder() << header
                << ": reason# " << msg->GetError()
                << ", txId# " << TxId
                << ", shard# " << shardId);
        };

        switch (status) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                Mon->TxResultPrepared->Inc();

                const auto& record = msg->Record;

                AggrMinStep = Max(AggrMinStep, record.GetMinStep());
                AggrMaxStep = Min(AggrMaxStep, record.GetMaxStep());

                const auto& domainCoordinators = record.GetDomainCoordinators();
                const auto privateCoordinators = TVector<ui64>(domainCoordinators.begin(), domainCoordinators.end());
                const ui64 privateCoordinator = TCoordinators(privateCoordinators).Select(TxId);

                if (!SelectedCoordinator) {
                    SelectedCoordinator = privateCoordinator;
                }

                if (!SelectedCoordinator || SelectedCoordinator != privateCoordinator) {
                    CancelProposal();
                    Mon->TxResultAborted->Inc();
                    return CoordinatorUnknown(TStringBuilder() << "Unable to choose coordinator"
                        << ": txId# " << TxId
                        << ", shard# " << shardId
                        << ", selectCoordinator# " << SelectedCoordinator
                        << ", privateCoordinator# " << privateCoordinator);
                }

                PendingPrepare.erase(shardId);
                if (PendingPrepare) {
                    return;
                }

                return RegisterPlan();
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE:
                CancelProposal();
                Mon->TxResultComplete->Inc();
                return ExecError(TStringBuilder() << "Unexpected COMPLETE result"
                    << ": txId# " << TxId
                    << ", shard# " << shardId);
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                CancelProposal(shardId);
                Mon->TxResultError->Inc();
                return error(TEvResponse::SHARD_NOT_AVAILABLE, TStringBuf("Not available"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                Mon->TxResultAborted->Inc();
                return error(TEvResponse::SHARD_ABORTED, TStringBuf("Aborted"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER:
                CancelProposal(shardId);
                Mon->TxResultShardTryLater->Inc();
                return error(TEvResponse::SHARD_TRY_LATER, TStringBuf("Try later"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                CancelProposal(shardId);
                Mon->TxResultShardOverloaded->Inc();
                return error(TEvResponse::SHARD_OVERLOADED, TStringBuf("Overloaded"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR:
                Mon->TxResultExecError->Inc();
                return error(TEvResponse::SHARD_EXEC_ERROR, TStringBuf("Execution error"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                Mon->TxResultResultUnavailable->Inc();
                return error(TEvResponse::SHARD_RESULT_UNAVAILABLE, TStringBuf("Result unavailable"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                Mon->TxResultCancelled->Inc();
                return error(TEvResponse::SHARD_CANCELLED, TStringBuf("Cancelled"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST:
                Mon->TxResultCancelled->Inc();
                return error(TEvResponse::SHARD_CANCELLED, TStringBuf("Bad request"));
            default:
                CancelProposal();
                Mon->TxResultFatal->Inc();
                return ShardUnknown(TStringBuilder() << "Unexpected status"
                    << ": reason# " << msg->GetError()
                    << ", txId# " << TxId
                    << ", shard# " << shardId);
        }
    }

    void HandlePropose(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const auto* msg = ev->Get();

        const ui64 shardId = msg->TabletId;
        if (!PendingPrepare.contains(shardId)) {
            return;
        }

        CancelProposal(shardId);
        Mon->ClientConnectedError->Inc();

        if (msg->NotDelivered) {
            return Reply(TEvResponse::SHARD_NOT_AVAILABLE, TStringBuilder() << "Could not deliver program"
                << ": txId# " << TxId
                << ", shard# " << shardId);
        } else {
            return ShardUnknown(TStringBuilder() << "Tx state unknown"
                << ": reason# " << "lost pipe while waiting for reply (propose)"
                << ", txId# " << TxId
                << ", shard# " << shardId);
        }
    }

    /// Plan

    void RegisterPlan() {
        Y_ENSURE(SelectedCoordinator);

        YDB_LOG_DEBUG("[DistEraser] Register plan",
            {"selfId", SelfId()},
            {"txId", TxId},
            {"minStep", AggrMinStep},
            {"maxStep", AggrMaxStep});

        auto propose = MakeHolder<TEvTxProxy::TEvProposeTransaction>(
            SelectedCoordinator, TxId, 0, AggrMinStep, AggrMaxStep);

        auto& affectedSet = *propose->Record.MutableTransaction()->MutableAffectedSet();
        affectedSet.Reserve(Shards.size());

        for (const ui64 shardId : Shards) {
            auto& x = *affectedSet.Add();
            x.SetTabletId(shardId);
            x.SetFlags(1 << 1 /* AffectedWrite */);

            Y_DEBUG_ABORT_UNLESS(!PendingResult.contains(shardId));
            PendingResult.insert(shardId);
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(propose.Release(), SelectedCoordinator, true));
        Become(&TThis::StatePlan);
    }

    STATEFN(StatePlan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandlePlan);
            hFunc(TEvDataShard::TEvProposeTransactionResult, HandlePlan);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePlan);
        default:
            return StateExecuteBase(ev);
        }
    }

    void HandlePlan(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto status = ev->Get()->GetStatus();
        YDB_LOG_DEBUG("[DistEraser] Handle TEvTxProxy::TEvProposeTransactionStatus",
            {"selfId", SelfId()},
            {"txId", TxId},
            {"status", static_cast<ui32>(status)});

        switch (status) {
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
            Mon->ClientTxStatusAccepted->Inc();
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
            Mon->ClientTxStatusProcessed->Inc();
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
            Mon->ClientTxStatusConfirmed->Inc();
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
            Mon->ClientTxStatusPlanned->Inc();
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting: // TODO: retry
            CancelProposal();
            [[fallthrough]];
        default:
            Mon->ClientTxStatusCoordinatorDeclined->Inc();
            return CoordinatorDeclined(TStringBuilder() << "Tx failed to plan"
                << ": reason# " << "declined by coodinator"
                << ", txId# " << TxId
                << ", status# " << static_cast<ui32>(status));
        }
    }

    void HandlePlan(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        const auto* msg = ev->Get();

        const ui64 shardId = msg->GetOrigin();
        if (!PendingResult.contains(shardId)) {
            return;
        }

        const auto status = msg->GetStatus();
        YDB_LOG_DEBUG("[DistEraser] HandlePlan TEvDataShard::TEvProposeTransactionResult",
            {"selfId", SelfId()},
            {"txId", TxId},
            {"shard", shardId},
            {"status", static_cast<ui32>(status)});

        auto error = [&](TEvResponse::EStatus code, const TStringBuf header) {
            return Reply(code, TStringBuilder() << header
                << ": reason# " << msg->GetError()
                << ", txId# " << TxId
                << ", shard# " << shardId);
        };

        switch (status) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                Mon->PlanClientTxResultComplete->Inc();

                PendingResult.erase(shardId);
                if (PendingResult) {
                    return;
                }

                return Reply();
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                Mon->PlanClientTxResultAborted->Inc();
                return error(TEvResponse::SHARD_ABORTED, TStringBuf("Aborted"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                Mon->PlanClientTxResultResultUnavailable->Inc();
                return error(TEvResponse::SHARD_RESULT_UNAVAILABLE, TStringBuf("Result unavailable"));
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                Mon->PlanClientTxResultCancelled->Inc();
                return error(TEvResponse::SHARD_CANCELLED, TStringBuf("Cancelled"));
            default:
                Mon->PlanClientTxResultExecError->Inc();
                return error(TEvResponse::SHARD_EXEC_ERROR, TStringBuf("Unexpected status"));
        }
    }

    void HandlePlan(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const auto* msg = ev->Get();

        if (SelectedCoordinator == msg->TabletId) {
            if (msg->NotDelivered) {
                Mon->PlanCoordinatorDeclined->Inc();
                return CoordinatorDeclined(TStringBuilder() << "Tx failed to plan"
                    << ": reason# " << "not delivered to coordinator"
                    << ", txId# " << TxId
                    << ", coordinator# " << msg->TabletId);
            } else {
                Mon->PlanClientDestroyed->Inc();
                return CoordinatorUnknown(TStringBuilder() << "Tx state unknown"
                    << ": reason# " << "delivery problem to coordinator"
                    << ", txId# " << TxId
                    << ", coordinator# " << msg->TabletId);
            }
        }

        if (PendingResult.contains(msg->TabletId)) {
            Mon->ClientConnectedError->Inc();
            return ShardUnknown(TStringBuilder() << "Tx state unknown"
                << ": reason# " << "lost pipe while waiting for reply (plan)"
                << ", txId# " << TxId
                << ", shard# " << msg->TabletId);
        }
    }

    void PassAway() override {
        Cancelled = true;

        for (auto& state : VectorResolveStates) {
            if (state.ResolverId) {
                Send(std::exchange(state.ResolverId, TActorId()), new TEvents::TEvPoisonPill());
            }
        }

        if (ResolvingKeys) {
            return;
        }

        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DISTRIBUTED_ERASE_ROWS_ACTOR;
    }

    TDistEraser(const TActorId& replyTo, const TString& databaseName, const TTableId& mainTableId,
            const TIndexes& indexes, const TVectorIndexes& vectorIndexes)
        : ReplyTo(replyTo)
        , DatabaseName(databaseName)
        , MainTableId(mainTableId)
        , Indexes(indexes)
        , VectorIndexes(vectorIndexes)
        , Cancelled(false)
        , ResolvingKeys(false)
        , TxId(0)
        , SelectedCoordinator(0)
        , AggrMinStep(0)
        , AggrMaxStep(Max<ui64>())
    {
        for (size_t i = 0; i < VectorIndexes.size(); ++i) {
            Y_DEBUG_ABORT_UNLESS(!VectorIndexByTableId.contains(VectorIndexes[i].TableId));
            VectorIndexByTableId.emplace(VectorIndexes[i].TableId, i);
        }
    }

    void Bootstrap() {
        AllocateTxId();
    }

    STATEFN(StatePrepareBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvEraseRowsRequest, Store);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateExecuteBase) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvUndelivered::EventType, DeliveryProblem);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateResolveVectors) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NBuildIndex::TEvVectorResolveResponse, HandleVectorResolve);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        default:
            return StatePrepareBase(ev);
        }
    }

private:
    const TActorId ReplyTo;
    const TString DatabaseName;
    const TTableId MainTableId;
    const TIndexes Indexes;
    const TVectorIndexes VectorIndexes;
    THashMap<TTableId, size_t> VectorIndexByTableId;

    bool Cancelled;
    bool ResolvingKeys;

    ui64 TxId;
    TActorId LeaderPipeCache;
    TIntrusivePtr<NTxProxy::TTxProxyMon> Mon;

    THashMap<TTableId, TTableInfo> TableInfos;
    TEvDataShard::TEvEraseRowsRequest::TPtr Request;

    // Built while handling the request, completed by the vector resolution.
    THashMap<TTableId, THashMap<ui64, TShardKeys>> Keys;
    TVector<ui32> IndexColumnIds;
    TVector<TString> IndexColumnValues;
    TVector<TVectorResolveState> VectorResolveStates;
    size_t VectorResolvesRemaining = 0;
    ui64 NextVectorResolveRequestId = 1;

    THashSet<ui64> Shards;
    THashSet<ui64> PendingPrepare;
    THashSet<ui64> PendingResult;

    ui64 SelectedCoordinator;
    ui64 AggrMinStep;
    ui64 AggrMaxStep;

}; // TDistEraser

IActor* CreateDistributedEraser(const TActorId& replyTo, const TString& databaseName,
    const TTableId& mainTableId, const TIndexes& indexes, const TVectorIndexes& vectorIndexes) {
    return new TDistEraser(replyTo, databaseName, mainTableId, indexes, vectorIndexes);
}

} // NDataShard
} // NKikimr

#undef YDB_LOG_THIS_FILE_COMPONENT
