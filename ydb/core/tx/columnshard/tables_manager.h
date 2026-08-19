#pragma once

#include "columnshard_schema.h"

#include "blobs_action/abstract/storages_manager.h"
#include "data_accessor/manager.h"
#include "engines/column_engine.h"
#include "engines/metadata_accessor.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>

#include <ydb/library/accessor/accessor.h>

#include <util/digest/numeric.h>

namespace NKikimr::NColumnShard {

template <class TVersionData>
class TVersionedSchema {
private:
    TMap<NOlap::TSnapshot, ui64> Versions;
    TMap<ui64, TVersionData> VersionsById;
    TMap<ui64, NOlap::TSnapshot> MinVersionById;

public:
    bool IsEmpty() const {
        return VersionsById.empty();
    }

    const TMap<ui64, TVersionData>& GetVersionsById() const {
        return VersionsById;
    }

    TMap<ui64, TVersionData>& MutableVersionsById() {
        return VersionsById;
    }

    NOlap::TSnapshot GetMinVersionForId(const ui64 sVersion) const {
        auto it = MinVersionById.find(sVersion);
        Y_ABORT_UNLESS(it != MinVersionById.end());
        return it->second;
    }

    void AddVersion(const NOlap::TSnapshot& snapshot, const TVersionData& versionInfo) {
        ui64 ssVersion = 0;
        if (versionInfo.HasSchema()) {
            ssVersion = versionInfo.GetSchema().GetVersion();
        }
        VersionsById.emplace(ssVersion, versionInfo);
        Y_ABORT_UNLESS(Versions.emplace(snapshot, ssVersion).second);

        auto it = MinVersionById.find(ssVersion);
        if (it == MinVersionById.end()) {
            MinVersionById.emplace(ssVersion, snapshot);
        } else {
            it->second = std::min(snapshot, it->second);
        }
    }
};

class TSchemaPreset: public TVersionedSchema<NKikimrTxColumnShard::TSchemaPresetVersionInfo> {
public:
    using TSchemaPresetVersionInfo = NKikimrTxColumnShard::TSchemaPresetVersionInfo;
    ui32 Id = 0;
    TString Name;

public:
    bool IsStandaloneTable() const {
        return Id == 0;
    }

    const TString& GetName() const {
        return Name;
    }

    ui32 GetId() const {
        return Id;
    }

    void Deserialize(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto);

    template <class TRow>
    bool InitFromDB(const TRow& rowset) {
        Id = rowset.template GetValue<Schema::SchemaPresetInfo::Id>();
        if (!IsStandaloneTable()) {
            Name = rowset.template GetValue<Schema::SchemaPresetInfo::Name>();
        }
        Y_ABORT_UNLESS(!Id || Name == "default", "Unsupported preset at load time");
        return true;
    }
};

class TTableInfo {
    struct TPathInfo {
        std::optional<NOlap::TSnapshot> DropVersion;
        std::optional<NOlap::TSnapshot> CopyVersion;
        std::optional<TString> LastCompletedBackupTransaction;
        bool IsReadOnly = false;
    };

    TInternalPathId InternalPathId;
    std::map<TSchemeShardLocalPathId, TPathInfo> SchemeShardLocalPathIds;   // path ids the tables is known as at SchemeShard
    YDB_READONLY_DEF(TSet<NOlap::TSnapshot>, Versions);

public:
    bool IsEmpty() const {
        return Versions.empty();
    }

    const TInternalPathId& GetInternalPathId() const {
        return InternalPathId;
    }

    bool CanBeUsedAt(const NOlap::TSnapshot& snapshot) const {
        if (Versions.empty()) {
            return false;
        }
        const NOlap::TSnapshot minVersion = *Versions.begin();
        for (const auto& [_, pathInfo] : SchemeShardLocalPathIds) {
            const NOlap::TSnapshot appearVersion = pathInfo.CopyVersion.value_or(minVersion);
            if (snapshot < appearVersion) {
                continue;
            }
            if (!pathInfo.DropVersion || snapshot < *pathInfo.DropVersion) {
                return true;
            }
        }
        return false;
    }

    std::set<TUnifiedPathId> GetPathIds() const {
        std::set<NColumnShard::TUnifiedPathId> paths;
        for (const auto& [schemeShardLocalPathId, _] : SchemeShardLocalPathIds) {
            paths.insert(NColumnShard::TUnifiedPathId::BuildValid(InternalPathId, schemeShardLocalPathId));
        }
        return paths;
    }

    const std::optional<NOlap::TSnapshot> GetDropVersionOptional() const {
        for (const auto& [schemeShardLocalPathId, pathInfo] : SchemeShardLocalPathIds) {
            if (!pathInfo.DropVersion) {
                return std::nullopt;
            }
        }
        std::optional<NOlap::TSnapshot> dropVersion;
        for (const auto& [schemeShardLocalPathId, pathInfo] : SchemeShardLocalPathIds) {
            if (!dropVersion || *dropVersion < *pathInfo.DropVersion) {
                dropVersion = pathInfo.DropVersion;
            }
        }
        AFL_VERIFY(dropVersion);
        return *dropVersion;
    }

    bool HasSchemeShardLocalPathId(const TSchemeShardLocalPathId& schemeShardLocalPathId) const {
        return SchemeShardLocalPathIds.contains(schemeShardLocalPathId);
    }

    // Path-local drop version. Caller must ensure HasSchemeShardLocalPathId; nullopt means the path is live.
    std::optional<NOlap::TSnapshot> GetPathDropVersionOptional(const TSchemeShardLocalPathId& schemeShardLocalPathId) const {
        const auto it = SchemeShardLocalPathIds.find(schemeShardLocalPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        return it->second.DropVersion;
    }

    void Merge(TTableInfo&& other) {
        AFL_VERIFY(InternalPathId == other.InternalPathId);
        Versions.insert(other.Versions.begin(), other.Versions.end());
        for (auto&& [schemeShardLocalPathId, pathInfo] : other.SchemeShardLocalPathIds) {
            SchemeShardLocalPathIds[schemeShardLocalPathId] = std::move(pathInfo);   // override
        }
    }

    void Remove(const TSchemeShardLocalPathId schemeShardLocalPathId) {
        SchemeShardLocalPathIds.erase(schemeShardLocalPathId);
    }

    const NOlap::TSnapshot GetDropVersionVerified() const {
        auto dropVersion = GetDropVersionOptional();
        AFL_VERIFY(dropVersion);
        return *dropVersion;
    }

    void SetDropVersion(const TSchemeShardLocalPathId& schemeShardLocalPathId, const NOlap::TSnapshot& version) {
        auto it = SchemeShardLocalPathIds.find(schemeShardLocalPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        auto& pathInfo = it->second;
        AFL_VERIFY(!pathInfo.DropVersion)("exists", pathInfo.DropVersion->DebugString())("version", version.DebugString());
        pathInfo.DropVersion = version;
    }

    bool IsReadOnly(const TSchemeShardLocalPathId& schemeShardLocalPathId) const {
        auto it = SchemeShardLocalPathIds.find(schemeShardLocalPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        return it->second.IsReadOnly;
    }

    std::optional<NOlap::TSnapshot> GetCopyVersionOptional(const TSchemeShardLocalPathId& schemeShardLocalPathId) const {
        const auto it = SchemeShardLocalPathIds.find(schemeShardLocalPathId);
        if (it == SchemeShardLocalPathIds.end()) {
            return std::nullopt;
        }
        if (it->second.DropVersion) {
            return std::nullopt;
        }
        return it->second.CopyVersion;
    }

    void SetCopyVersion(const TSchemeShardLocalPathId& schemeShardLocalPathId, const NOlap::TSnapshot& version) {
        auto& pathInfo = SchemeShardLocalPathIds[schemeShardLocalPathId];
        AFL_VERIFY(!pathInfo.CopyVersion)("exists", pathInfo.CopyVersion->DebugString())("version", version.DebugString());
        pathInfo.CopyVersion = version;
    }

    void SetReadOnly(const TSchemeShardLocalPathId& schemeShardLocalPathId, const bool isReadOnly) {
        auto& pathInfo = SchemeShardLocalPathIds[schemeShardLocalPathId];
        AFL_VERIFY(!pathInfo.IsReadOnly)("exists", pathInfo.IsReadOnly)("version", isReadOnly);
        pathInfo.IsReadOnly = isReadOnly;
    }

    void SetLastCompletedBackupTransaction(const TSchemeShardLocalPathId& schemeShardLocalPathId, TString serializedBackupTx) {
        auto it = SchemeShardLocalPathIds.find(schemeShardLocalPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        it->second.LastCompletedBackupTransaction = std::move(serializedBackupTx);
    }

    void AddVersion(const NOlap::TSnapshot& snapshot) {
        Versions.insert(snapshot);
    }

    void CollectReadOnlyTablesSnapshots(TSet<NOlap::TSnapshot>& target) const {
        for (const auto& [_, pathInfo] : SchemeShardLocalPathIds) {
            if (pathInfo.CopyVersion && !pathInfo.DropVersion) {
                target.insert(*pathInfo.CopyVersion);
            }
        }
    }

    void RenameTableSchemeShardLocalPathId(
        NIceDb::TNiceDb& db, const TSchemeShardLocalPathId oldPathId, const TSchemeShardLocalPathId newPathId) {
        auto it = SchemeShardLocalPathIds.find(oldPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        const auto& pathInfo = it->second;
        if (!pathInfo.IsReadOnly) {   // v0 can't be read-only. backward compatibility
            Schema::SaveTableSchemeShardLocalPathId(db, InternalPathId, newPathId);
        }
        Schema::RenameTableSchemeShardLocalPathIdV1(db, InternalPathId, oldPathId, newPathId, pathInfo.DropVersion, pathInfo.CopyVersion,
            pathInfo.LastCompletedBackupTransaction, pathInfo.IsReadOnly);
        AFL_VERIFY(SchemeShardLocalPathIds
                       .insert({newPathId, TPathInfo{pathInfo.DropVersion, pathInfo.CopyVersion,
                                              pathInfo.LastCompletedBackupTransaction, pathInfo.IsReadOnly}})
                       .second);
        SchemeShardLocalPathIds.erase(oldPathId);
    }

    void CopySchemeShardLocalPathId(NIceDb::TNiceDb& db, const TSchemeShardLocalPathId srcSchemeShardLocalPathId,
        const TSchemeShardLocalPathId dstSchemeShardLocalPathId, const NOlap::TSnapshot& copyVersion) {
        auto it = SchemeShardLocalPathIds.find(srcSchemeShardLocalPathId);
        AFL_VERIFY(it != SchemeShardLocalPathIds.end());
        const auto dstIt = SchemeShardLocalPathIds.find(dstSchemeShardLocalPathId);
        if (dstIt == SchemeShardLocalPathIds.end()) {
            Schema::CopySchemeShardLocalPathIdV1(
                db, InternalPathId, dstSchemeShardLocalPathId, it->second.DropVersion, copyVersion, std::nullopt, true);
            AFL_VERIFY(SchemeShardLocalPathIds
                           .insert({dstSchemeShardLocalPathId, TPathInfo{it->second.DropVersion, copyVersion, std::nullopt, true}})
                           .second);
            return;
        }
        AFL_VERIFY(dstIt->second.CopyVersion == copyVersion)("expected", copyVersion.DebugString())(
            "actual", dstIt->second.CopyVersion->DebugString());
        AFL_VERIFY(dstIt->second.IsReadOnly);
    }

    bool IsDropped(const std::optional<NOlap::TSnapshot>& minReadSnapshot = std::nullopt) const {
        auto dropVersion = GetDropVersionOptional();
        if (!dropVersion) {
            return false;
        }
        if (!minReadSnapshot) {
            return true;
        }
        // Exclusive drop boundary, same as CanBeUsedAt / ResolveInternalPathIdForSnapshot:
        // a read at exactly the drop/truncate snapshot must not see the dropped generation.
        return *dropVersion <= *minReadSnapshot;
    }

    TTableInfo(const std::set<TUnifiedPathId>& unifiedPathIds) {
        AFL_VERIFY(unifiedPathIds.size());
        for (const auto& unifiedPathId : unifiedPathIds) {
            AFL_VERIFY(!InternalPathId || InternalPathId == unifiedPathId.InternalPathId);
            InternalPathId = unifiedPathId.InternalPathId;
            AFL_VERIFY(SchemeShardLocalPathIds.insert({unifiedPathId.SchemeShardLocalPathId, {}}).second);
        }
        AFL_VERIFY(SchemeShardLocalPathIds.size());
    }

    template <class TRow>
    static TTableInfo InitFromDB(const TRow& rowset) {
        const auto internalPathId = TInternalPathId::FromRawValue(rowset.template GetValue<Schema::TableInfo::PathId>());
        AFL_VERIFY(internalPathId);
        const auto& schemeShardLocalPathId =
            TSchemeShardLocalPathId::FromRawValue(rowset.template HaveValue<Schema::TableInfo::SchemeShardLocalPathId>()
                                                      ? rowset.template GetValue<Schema::TableInfo::SchemeShardLocalPathId>()
                                                      : internalPathId.GetRawValue());
        AFL_VERIFY(schemeShardLocalPathId);
        TTableInfo result({ TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId) });
        if (rowset.template HaveValue<Schema::TableInfo::DropStep>() && rowset.template HaveValue<Schema::TableInfo::DropTxId>()) {
            result.SetDropVersion(schemeShardLocalPathId, NOlap::TSnapshot(rowset.template GetValue<Schema::TableInfo::DropStep>(),
                                                              rowset.template GetValue<Schema::TableInfo::DropTxId>()));
        }
        return result;
    }

    template <class TRow>
    static TTableInfo InitFromDBV1(const TRow& rowset) {
        const auto internalPathId = TInternalPathId::FromRawValue(rowset.template GetValue<Schema::TableInfoV1::PathId>());
        AFL_VERIFY(internalPathId);
        const auto schemeShardLocalPathId =
            TSchemeShardLocalPathId::FromRawValue(rowset.template GetValue<Schema::TableInfoV1::SchemeShardLocalPathId>());
        AFL_VERIFY(schemeShardLocalPathId);
        TTableInfo result({ TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId) });
        if (rowset.template HaveValue<Schema::TableInfoV1::DropStep>() && rowset.template HaveValue<Schema::TableInfoV1::DropTxId>()) {
            result.SetDropVersion(schemeShardLocalPathId, NOlap::TSnapshot(rowset.template GetValue<Schema::TableInfoV1::DropStep>(),
                                                              rowset.template GetValue<Schema::TableInfoV1::DropTxId>()));
        }
        if (rowset.template HaveValue<Schema::TableInfoV1::CopyStep>() && rowset.template HaveValue<Schema::TableInfoV1::CopyTxId>()) {
            result.SetCopyVersion(schemeShardLocalPathId, NOlap::TSnapshot(rowset.template GetValue<Schema::TableInfoV1::CopyStep>(),
                                                              rowset.template GetValue<Schema::TableInfoV1::CopyTxId>()));
        }
        if (rowset.template HaveValue<Schema::TableInfoV1::LastCompletedBackupTransaction>()) {
            result.SchemeShardLocalPathIds[schemeShardLocalPathId].LastCompletedBackupTransaction =
                rowset.template GetValue<Schema::TableInfoV1::LastCompletedBackupTransaction>();
        }
        if (rowset.template HaveValue<Schema::TableInfoV1::IsReadOnly>()) {
            result.SetReadOnly(schemeShardLocalPathId, rowset.template GetValue<Schema::TableInfoV1::IsReadOnly>());
        }
        return result;
    }
};

class TTtlVersions {
private:
    THashMap<TInternalPathId, std::map<NOlap::TSnapshot, std::optional<NOlap::TTiering>>> Ttl;
    // Raw TTL settings proto kept alongside the deserialized TTiering, so that the exact original
    // settings (column unit, tiers, etc.) can be replayed onto a new path id on TRUNCATE without a
    // lossy TTiering->proto round trip (TTiering has no serialize-to-proto).
    THashMap<TInternalPathId, std::map<NOlap::TSnapshot, NKikimrSchemeOp::TColumnDataLifeCycle>> TtlProtos;

    void AddVersion(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot, std::optional<NOlap::TTiering> ttl) {
        auto [it, inserted] = Ttl[pathId].emplace(snapshot, ttl);
        AFL_VERIFY(inserted || it->second == ttl)("snapshot", snapshot);
    }

public:
    void AddVersionFromProto(
        const TInternalPathId pathId, const NOlap::TSnapshot& snapshot, const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings) {
        std::optional<NOlap::TTiering> ttlVersion;
        if (ttlSettings.HasEnabled()) {
            NOlap::TTiering deserializedTtl;
            AFL_VERIFY(deserializedTtl.DeserializeFromProto(ttlSettings.GetEnabled()).IsSuccess());
            ttlVersion.emplace(std::move(deserializedTtl));
        }
        AddVersion(pathId, snapshot, ttlVersion);
        // Keep TtlProtos in lockstep with Ttl: both maps must always carry the same (pathId, snapshot)
        // versions, otherwise GetTableTtl and GetTableTtlProto could disagree. Mirror the same
        // idempotency invariant enforced by AddVersion above.
        auto [it, inserted] = TtlProtos[pathId].emplace(snapshot, ttlSettings);
        AFL_VERIFY(inserted || it->second.SerializeAsString() == ttlSettings.SerializeAsString())("snapshot", snapshot);
    }

    std::optional<NOlap::TTiering> GetTableTtl(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        auto findTable = Ttl.FindPtr(pathId);
        if (!findTable) {
            return std::nullopt;
        }
        const auto findTtl = findTable->upper_bound(snapshot);
        if (findTtl == findTable->begin()) {
            return std::nullopt;
        }
        return std::prev(findTtl)->second;
    }

    // Returns the raw TTL settings proto effective at `snapshot`, if the table ever had TTL settings.
    // Used by TRUNCATE to carry the table's lifecycle settings over to the freshly generated path id.
    std::optional<NKikimrSchemeOp::TColumnDataLifeCycle> GetTableTtlProto(
        const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        auto findTable = TtlProtos.FindPtr(pathId);
        if (!findTable) {
            return std::nullopt;
        }
        const auto findTtl = findTable->upper_bound(snapshot);
        if (findTtl == findTable->begin()) {
            return std::nullopt;
        }
        return std::prev(findTtl)->second;
    }

    ui64 GetMemoryUsage() const {
        ui64 memory = 0;
        for (const auto& [_, ttlVersions] : Ttl) {
            memory += ttlVersions.size() * sizeof(NOlap::TTiering);
        }
        for (const auto& [_, ttlProtoVersions] : TtlProtos) {
            // Use serialized size for accurate accounting; sizeof only counts the fixed header.
            for (const auto& [_, proto] : ttlProtoVersions) {
                memory += proto.ByteSizeLong() + sizeof(NKikimrSchemeOp::TColumnDataLifeCycle);
            }
        }
        return memory;
    }

    // Removes all TTL history for `pathId`. Called when the path is fully cleaned up
    // so that Ttl and TtlProtos do not accumulate entries for dropped generations.
    void RemovePathId(const TInternalPathId pathId) {
        Ttl.erase(pathId);
        TtlProtos.erase(pathId);
    }
};

// TGenerationIndex owns the "live" and "all generations" mappings for SchemeShardLocalPathId →
// InternalPathId.  It encapsulates the invariant that Live and All stay consistent: every SetLive
// inserts into All; ForgetLive/ForgetGeneration erase from both; Rename moves the full history.
class TGenerationIndex {
private:
    THashMap<TSchemeShardLocalPathId, TInternalPathId> Live;
    THashMap<TSchemeShardLocalPathId, THashSet<TInternalPathId>> All;

public:
    // Set (or replace) the live generation for @p ss.  If @p isDropped is true the generation is
    // dropped and must NOT overwrite an existing live entry (protects against loading a dropped
    // generation over a live one during recovery).
    void SetLive(TSchemeShardLocalPathId ss, TInternalPathId id, bool isDropped) {
        if (isDropped) {
            // Only insert if there is no live mapping yet (recovery ordering guard).
            Live.emplace(ss, id);
        } else {
            Live[ss] = id;
        }
        All[ss].insert(id);
    }

    // Remove @p id from the live mapping for @p ss only when it still points to @p id.
    // Returns true if the live entry was actually erased.
    bool ForgetLiveIfMatches(TSchemeShardLocalPathId ss, TInternalPathId id) {
        auto it = Live.find(ss);
        if (it != Live.end() && it->second == id) {
            Live.erase(it);
            return true;
        }
        return false;
    }

    // Unconditionally erase @p ss from live (used by fence operations).
    void ForgetLive(TSchemeShardLocalPathId ss) {
        Live.erase(ss);
    }

    // Remove @p id from the generation history for @p ss.  If the history becomes empty, the key
    // is removed entirely.
    void ForgetGeneration(TSchemeShardLocalPathId ss, TInternalPathId id) {
        auto it = All.find(ss);
        if (it != All.end()) {
            it->second.erase(id);
            if (it->second.empty()) {
                All.erase(it);
            }
        }
    }

    // Move the entire generation history from @p fromSs to @p toSs and update the live mapping.
    void Rename(TSchemeShardLocalPathId fromSs, TSchemeShardLocalPathId toSs) {
        // Move live mapping.
        auto itLive = Live.find(fromSs);
        if (itLive != Live.end()) {
            Live[toSs] = itLive->second;
            Live.erase(itLive);
        }
        // Move full history.
        auto itAll = All.find(fromSs);
        if (itAll != All.end()) {
            All[toSs] = std::move(itAll->second);
            All.erase(itAll);
        }
    }

    // Insert a single generation into the history (without making it live).  Used by CopyTable
    // and lazy-populate during TruncateTablePropose.
    void AddToHistory(TSchemeShardLocalPathId ss, TInternalPathId id) {
        All[ss].insert(id);
    }

    // Resolve the current live generation.
    std::optional<TInternalPathId> ResolveLive(TSchemeShardLocalPathId ss) const {
        const auto* it = Live.FindPtr(ss);
        return it ? std::optional<TInternalPathId>(*it) : std::nullopt;
    }

    // Return a pointer to the full generation set for @p ss (or nullptr if none).
    const THashSet<TInternalPathId>* Generations(TSchemeShardLocalPathId ss) const {
        return All.FindPtr(ss);
    }

    // Resolve the best generation for a time-travel read at @p readSnapshot.
    // The caller provides callbacks for table-dependent checks (membership + drop version).
    // Returns the best matching generation, or std::nullopt if none found in history.
    template <class TMemberCheck, class TDropVersionGet>
    std::optional<TInternalPathId> ResolveForSnapshot(
        TSchemeShardLocalPathId ss,
        const NOlap::TSnapshot& readSnapshot,
        TMemberCheck isMember,
        TDropVersionGet getDropVersion) const {
        const auto* generations = Generations(ss);
        if (!generations) {
            return std::nullopt;
        }
        std::optional<TInternalPathId> best;
        std::optional<NOlap::TSnapshot> bestDrop;
        std::optional<TInternalPathId> live;
        for (const auto& internalPathId : *generations) {
            if (!isMember(internalPathId, ss)) {
                continue;
            }
            const auto dropVersion = getDropVersion(internalPathId, ss);
            if (!dropVersion) {
                live = internalPathId;
                continue;
            }
            if (*dropVersion <= readSnapshot) {
                continue;
            }
            if (!bestDrop || *dropVersion < *bestDrop) {
                bestDrop = dropVersion;
                best = internalPathId;
            }
        }
        if (best) {
            return best;
        }
        return live;
    }

    // Accessors for iteration / bulk operations that need direct map access.
    const THashMap<TSchemeShardLocalPathId, TInternalPathId>& GetLive() const { return Live; }
    const THashMap<TSchemeShardLocalPathId, THashSet<TInternalPathId>>& GetAll() const { return All; }
};

// Lightweight fence map shared by Move/Copy/Truncate schema operations.
// Stores "SS-path → InternalPathId" for paths that are fenced during an in-flight
// schema transaction. The fence blocks new writes (path resolves as unknown) until
// the operation completes on the plan/progress step.
class TPendingOpFence {
private:
    THashMap<TSchemeShardLocalPathId, TInternalPathId> Fenced;

public:
    // Record that @p ss is fenced with @p id.  Returns true on first insert;
    // if already fenced, verifies the id matches and returns false (idempotent).
    bool Propose(TSchemeShardLocalPathId ss, TInternalPathId id) {
        auto [it, inserted] = Fenced.emplace(ss, id);
        if (!inserted) {
            AFL_VERIFY(it->second == id)("ss", ss)("expected", id)("actual", it->second);
            return false;
        }
        return true;
    }

    // Return the fenced InternalPathId for @p ss, or std::nullopt if not fenced.
    std::optional<TInternalPathId> Get(TSchemeShardLocalPathId ss) const {
        const auto* p = Fenced.FindPtr(ss);
        return p ? std::optional<TInternalPathId>(*p) : std::nullopt;
    }

    // Remove the fence entry on successful plan/progress.  Returns true if present.
    bool Complete(TSchemeShardLocalPathId ss) {
        return Fenced.erase(ss);
    }

    // Remove the fence entry on abort (same effect as Complete, but semantically distinct).
    bool Abort(TSchemeShardLocalPathId ss) {
        return Fenced.erase(ss);
    }

    // Direct map access (for AFL_VERIFY-based erase patterns in progress methods).
    const TInternalPathId* FindPtr(TSchemeShardLocalPathId ss) const {
        return Fenced.FindPtr(ss);
    }

    // Erase with AFL_VERIFY semantics (used by progress methods that expect the entry).
    void Erase(TSchemeShardLocalPathId ss) {
        AFL_VERIFY(Fenced.erase(ss));
    }

    bool empty() const { return Fenced.empty(); }
};

class TTablesManager: public NOlap::IPathIdTranslator {
private:
    THashMap<TInternalPathId, TTableInfo> Tables;
    TGenerationIndex GenerationIndex;
    TPendingOpFence Renaming;   // Fence for MoveTable (propose → progress)
    TPendingOpFence Copying;    // Fence for CopyTable (propose → progress)
    TPendingOpFence Truncating; // Fence for TruncateTable (propose → plan)
    THashSet<ui32> SchemaPresetsIds;
    THashMap<ui32, NKikimrSchemeOp::TColumnTableSchema> ActualSchemaForPreset;
    std::map<NOlap::TSnapshot, THashSet<TInternalPathId>> PathsToDrop;
    TSet<NOlap::TSnapshot> ReadOnlyTablesSnapshots;
    TTtlVersions Ttl;
    std::unique_ptr<NOlap::IColumnEngine> PrimaryIndex;
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    NOlap::NDataAccessorControl::TDataAccessorsManagerContainer DataAccessorsManager;
    std::unique_ptr<TTableLoadTimeCounters> LoadTimeCounters;
    YDB_READONLY_DEF(NBackgroundTasks::TControlInterfaceContainer<NOlap::TSchemaObjectsCache>, SchemaObjectsCache);
    std::shared_ptr<TPortionIndexStats> PortionsStats;
    ui64 TabletId = 0;
    bool GenerateInternalPathId;
    std::optional<TUnifiedPathId> TabletPathId;
    TInternalPathId MaxInternalPathId;

    void RegisterReadOnlyTableSnapshot(const NOlap::TSnapshot& version);
    void RebuildReadOnlyTablesSnapshots();

    // Allocates the next free internal path id by advancing MaxInternalPathId. Only valid when the
    // tablet owns internal-path-id generation (GenerateInternalPathId). Shared by table creation and
    // TRUNCATE so the "+1" increment lives in exactly one place.
    TInternalPathId GenerateNextInternalPathId();

    friend class TTxInit;

public:   //IPathIdTranslator
    virtual std::optional<std::set<NColumnShard::TSchemeShardLocalPathId>> ResolveSchemeShardLocalPathIdsOptional(
        const TInternalPathId internalPathId) const override;
    virtual std::optional<TInternalPathId> ResolveInternalPathIdOptional(
        const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const override;
    virtual std::optional<NOlap::TSnapshot> GetCopyVersionOptional(
        const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId) const override;
    virtual std::vector<NOlap::TSnapshot> GetReadOnlyTablesSnapshots() const override;

public:
    std::optional<TInternalPathId> ResolveInternalPathIdForSnapshot(const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId,
        const NOlap::TSnapshot& readSnapshot, const bool withTabletPathId) const;

    TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<TPortionIndexStats>& portionsStats, const ui64 tabletId);

    TConclusion<std::shared_ptr<NOlap::ITableMetadataAccessor>> BuildTableMetadataAccessor(
        const TString& tablePath, const TSchemeShardLocalPathId externalPathId, const NOlap::TSnapshot& readSnapshot);
    TConclusion<std::shared_ptr<NOlap::ITableMetadataAccessor>> BuildTableMetadataAccessor(const TString& tablePath,
        const TInternalPathId internalPathId, const TSchemeShardLocalPathId externalPathId,
        const std::optional<NOlap::TSnapshot>& readSnapshot = std::nullopt);

    class TSchemaAddress {
    private:
        YDB_READONLY(ui32, PresetId, 0);
        YDB_READONLY(NOlap::TSnapshot, Snapshot, NOlap::TSnapshot::Zero());

    public:
        TString DebugString() const {
            return TStringBuilder() << PresetId << "," << Snapshot.DebugString();
        }

        TSchemaAddress(const ui32 presetId, const NOlap::TSnapshot& snapshot)
            : PresetId(presetId)
            , Snapshot(snapshot)
        {
        }

        explicit operator size_t() const {
            return CombineHashes<size_t>((size_t)PresetId, (size_t)Snapshot);
        }

        bool operator==(const TSchemaAddress& item) const {
            return std::tie(PresetId, Snapshot) == std::tie(item.PresetId, item.Snapshot);
        }

        bool operator<(const TSchemaAddress& item) const {
            AFL_VERIFY(PresetId == item.PresetId);
            return Snapshot < item.Snapshot;
        }
    };

    class TSchemasChain {
    private:
        YDB_READONLY_DEF(std::set<TSchemaAddress>, ToRemove);
        TSchemaAddress Finish;

    public:
        const TSchemaAddress& GetFinish() const {
            return Finish;
        }

        void FillAddressesTo(std::set<TSchemaAddress>& addresses) const {
            addresses.insert(ToRemove.begin(), ToRemove.end());
            addresses.emplace(Finish);
        }

        TSchemasChain(const std::set<TSchemaAddress>& toRemove, const TSchemaAddress& finish)
            : ToRemove(toRemove)
            , Finish(finish)
        {
            AFL_VERIFY(toRemove.size());
            AFL_VERIFY(*ToRemove.rbegin() < Finish);
        }
    };

    std::vector<TSchemasChain> ExtractSchemasToClean() const;

    std::optional<TUnifiedPathId> GetTabletPathIdOptional() const {
        return TabletPathId;
    }

    TUnifiedPathId GetTabletPathIdVerified() const {
        AFL_VERIFY(TabletPathId.has_value());
        AFL_VERIFY(TabletPathId->InternalPathId.IsValid());
        AFL_VERIFY(TabletPathId->SchemeShardLocalPathId.IsValid());
        return *TabletPathId;
    }

    const std::unique_ptr<TTableLoadTimeCounters>& GetLoadTimeCounters() const {
        return LoadTimeCounters;
    }

    bool TryFinalizeDropPathOnExecute(NTable::TDatabase& dbTable, const TInternalPathId pathId) const;
    bool TryFinalizeDropPathOnComplete(const TInternalPathId pathId);

    THashMap<TInternalPathId, NOlap::TTiering> GetTtl(const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        THashMap<TInternalPathId, NOlap::TTiering> ttl;
        for (const auto& [pathId, info] : Tables) {
            if (info.IsDropped(snapshot)) {
                continue;
            }
            if (auto tableTtl = Ttl.GetTableTtl(pathId, snapshot)) {
                ttl.emplace(pathId, std::move(*tableTtl));
            }
        }
        return ttl;
    }

    std::optional<NOlap::TTiering> GetTableTtl(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        return Ttl.GetTableTtl(pathId, snapshot);
    }

    // Returns the raw TTL settings proto effective for `pathId` at `snapshot`. Used by TRUNCATE to
    // replay the truncated table's lifecycle settings onto the freshly generated internal path id.
    std::optional<NKikimrSchemeOp::TColumnDataLifeCycle> GetTableTtlProto(
        const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        return Ttl.GetTableTtlProto(pathId, snapshot);
    }

    const std::map<NOlap::TSnapshot, THashSet<TInternalPathId>>& GetPathsToDrop() const {
        return PathsToDrop;
    }

    const THashMap<TInternalPathId, TTableInfo>& GetTables() const {
        return Tables;
    }

    const THashSet<ui32>& GetSchemaPresets() const {
        return SchemaPresetsIds;
    }

    // Tables belonging to a column store carry a non-standalone schema preset (id != 0), whereas
    // standalone column tables use an inline schema (their only preset, if any, is the id-0
    // placeholder registered on load). So a non-zero preset id means this tablet backs a column store.
    bool IsStoreTablet() const {
        for (const ui32 presetId : SchemaPresetsIds) {
            if (presetId != 0) {
                return true;
            }
        }
        return false;
    }

    bool HasPrimaryIndex() const {
        return !!PrimaryIndex;
    }

    void MoveTablePropose(const TSchemeShardLocalPathId schemeShardLocalPathId);
    void MoveTableProgress(
        NIceDb::TNiceDb& db, const TSchemeShardLocalPathId oldSchemeShardLocalPathId, const TSchemeShardLocalPathId newSchemeShardLocalPathId);

    void CopyTablePropose(const TSchemeShardLocalPathId srcSchemeShardLocalPathId);
    void CopyTablePlanStep(NIceDb::TNiceDb& db, const NOlap::TSnapshot& version, const TSchemeShardLocalPathId srcSchemeShardLocalPathId,
        const TSchemeShardLocalPathId dstSchemeShardLocalPathId);
    void CopyTableProgress(NIceDb::TNiceDb& db, const NOlap::TSnapshot& version, const TSchemeShardLocalPathId srcSchemeShardLocalPathId,
        const TSchemeShardLocalPathId dstSchemeShardLocalPathId);

    // Fence the path for TRUNCATE on propose: remove from GenerationIndex.Live so new writes and
    // CommitWriteLock fail with "unknown table" (same pattern as MoveTablePropose). The old
    // InternalPathId is kept in Truncating fence until TruncateTable runs on plan.
    void TruncateTablePropose(const TSchemeShardLocalPathId schemeShardLocalPathId);
    // Returns the InternalPathId fenced by TruncateTablePropose, if any.
    std::optional<TInternalPathId> GetTruncatingInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId) const;

    NOlap::TSnapshot ResolveReadSnapshot(const TSchemeShardLocalPathId schemeShardLocalPathId, const NOlap::TSnapshot& requestSnapshot) const;

    void AddTableInfo(const NKikimr::NColumnShard::TUnifiedPathId unifiedPathId, TTableInfo&& tableInfo);

    NOlap::IColumnEngine& MutablePrimaryIndex() const {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    const NOlap::TIndexInfo& GetIndexInfo(const NOlap::TSnapshot& version) const {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return PrimaryIndex->GetVersionedIndex().GetSchemaVerified(version)->GetIndexInfo();
    }

    const std::unique_ptr<NOlap::IColumnEngine>& GetPrimaryIndex() const {
        return PrimaryIndex;
    }

    const NOlap::IColumnEngine& GetPrimaryIndexSafe() const {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    template <class TIndex>
    TIndex& MutablePrimaryIndexAsVerified() const {
        AFL_VERIFY(!!PrimaryIndex);
        auto result = dynamic_cast<TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return *result;
    }

    template <class TIndex>
    const TIndex& GetPrimaryIndexAsVerified() const {
        AFL_VERIFY(!!PrimaryIndex);
        auto result = dynamic_cast<const TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return *result;
    }

    template <class TIndex>
    const TIndex* GetPrimaryIndexAsOptional() const {
        if (!PrimaryIndex) {
            return nullptr;
        }
        auto result = dynamic_cast<const TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return result;
    }

    template <class TIndex>
    TIndex* MutablePrimaryIndexAsOptional() const {
        if (!PrimaryIndex) {
            return nullptr;
        }
        auto result = dynamic_cast<TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return result;
    }

    bool InitFromDB(NIceDb::TNiceDb& db);
    void Init(NIceDb::TNiceDb& db, const TSchemeShardLocalPathId tabletSchemeShardLocalPathId, const TTabletStorageInfo* info);
    bool InitFromDB(NIceDb::TNiceDb& db, const TTabletStorageInfo* info);

    const TTableInfo& GetTable(const TInternalPathId pathId, const bool withDeleted = false) const;

    void SetLastCompletedBackupTransaction(const TSchemeShardLocalPathId schemeShardLocalPathId, TString serializedBackupTx) {
        const auto internalPathId = ResolveInternalPathIdVerified(schemeShardLocalPathId, false);
        auto* table = Tables.FindPtr(internalPathId);
        AFL_VERIFY(table);
        table->SetLastCompletedBackupTransaction(schemeShardLocalPathId, std::move(serializedBackupTx));
    }

    ui64 GetMemoryUsage() const;
    TInternalPathId GetOrCreateInternalPathId(const TSchemeShardLocalPathId schemShardLocalPathId);

    bool IsGenerateInternalPathId() const {
        return GenerateInternalPathId;
    }

    // Loads the latest persisted TTableVersionInfo for `pathId` (used by TRUNCATE to copy schema preset
    // / version adj onto the freshly allocated internal path id).
    std::optional<NKikimrTxColumnShard::TTableVersionInfo> LoadLastTableVersionInfo(const TInternalPathId pathId, NIceDb::TNiceDb& db) const;
    THashMap<TSchemeShardLocalPathId, TInternalPathId> ResolveInternalPathIds(
        const TSchemeShardLocalPathId from, const TSchemeShardLocalPathId to) const;
    bool HasTable(const TInternalPathId pathId, const bool withDeleted = false,
        const std::optional<NOlap::TSnapshot> minReadSnapshot = std::nullopt) const;
    bool IsReadyForStartWrite(const TInternalPathId pathId, const bool withDeleted) const;
    bool IsReadyForFinishWrite(const TInternalPathId pathId, const NOlap::TSnapshot& minReadSnapshot) const;
    bool HasPreset(const ui32 presetId) const;

    void DropTable(const TSchemeShardLocalPathId schemeShardLocalPathId, const TInternalPathId pathId, const NOlap::TSnapshot& version,
        NIceDb::TNiceDb& db);
    TInternalPathId TruncateTable(const TSchemeShardLocalPathId schemeShardLocalPathId, const TInternalPathId pathId,
        const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);
    void DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);

    void RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db);
    bool RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db);

    void AddSchemaVersion(
        const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db);
    void AddTableVersion(const TInternalPathId pathId, const NOlap::TSnapshot& version,
        const NKikimrTxColumnShard::TTableVersionInfo& versionInfo, const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema,
        NIceDb::TNiceDb& db);
    bool FillMonitoringReport(NTabletFlatExecutor::TTransactionContext& txc, NJson::TJsonValue& json);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateAddShardingInfoTx(TColumnShard& owner,
        const NColumnShard::TSchemeShardLocalPathId pathId, const ui64 versionId,
        const NSharding::TGranuleShardingLogicContainer& tabletShardingLogic) const;
};

}   // namespace NKikimr::NColumnShard
