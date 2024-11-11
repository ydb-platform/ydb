#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/abstract/schema.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NOlap {

class IDbWrapper;

class TGranuleShardingInfo {
private:
    YDB_READONLY_DEF(NSharding::TGranuleShardingLogicContainer, ShardingInfo);
    YDB_READONLY(TSnapshot, SinceSnapshot, TSnapshot::Zero());
    YDB_READONLY(ui64, SnapshotVersion, 0);
    YDB_READONLY(ui64, PathId, 0);

public:
    TGranuleShardingInfo(const NSharding::TGranuleShardingLogicContainer& shardingInfo, const TSnapshot& sinceSnapshot, const ui64 version, const ui64 pathId)
        : ShardingInfo(shardingInfo)
        , SinceSnapshot(sinceSnapshot)
        , SnapshotVersion(version)
        , PathId(pathId) {
        AFL_VERIFY(!!ShardingInfo);
    }
};

class TVersionedIndex {
    THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>> ShardingInfo;
    std::map<TSnapshot, ISchema::TPtr> Snapshots;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    std::map<ui64, ISchema::TPtr> SnapshotByVersion;
    ui64 LastSchemaVersion = 0;
    std::optional<ui64> SchemeVersionForActualization;
    ISchema::TPtr SchemeForActualization;

public:
    ISchema::TPtr GetLastCriticalSchema() const {
        return SchemeForActualization;
    }

    ISchema::TPtr GetLastCriticalSchemaDef(const ISchema::TPtr defaultSchema) const {
        auto result = GetLastCriticalSchema();
        return result ? result : defaultSchema;
    }

    std::optional<TGranuleShardingInfo> GetShardingInfoOptional(const ui64 pathId, const TSnapshot& ss) const {
        auto it = ShardingInfo.find(pathId);
        if (it == ShardingInfo.end() || it->second.empty()) {
            return std::nullopt;
        } else {
            auto itSS = it->second.upper_bound(ss);
            if (itSS == it->second.end()) {
                return it->second.rbegin()->second;
            } else if (itSS == it->second.begin()) {
                return std::nullopt;
            } else {
                --itSS;
                return itSS->second;
            }
        }
    }

    std::optional<TGranuleShardingInfo> GetShardingInfoActual(const ui64 pathId) const;

    void AddShardingInfo(const TGranuleShardingInfo& shardingInfo) {
        AFL_VERIFY(ShardingInfo[shardingInfo.GetPathId()].emplace(shardingInfo.GetSinceSnapshot(), shardingInfo).second);
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : Snapshots) {
            sb << i.first << ":" << i.second->DebugString() << ";";
        }
        return sb;
    }

    ISchema::TPtr GetSchema(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        return it == SnapshotByVersion.end() ? nullptr : it->second;
    }

    ISchema::TPtr GetSchemaVerified(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        Y_ABORT_UNLESS(it != SnapshotByVersion.end(), "no schema for version %lu", version);
        return it->second;
    }

    ISchema::TPtr GetSchema(const TSnapshot& version) const {
        for (auto it = Snapshots.rbegin(); it != Snapshots.rend(); ++it) {
            if (it->first <= version) {
                return it->second;
            }
        }
        Y_ABORT_UNLESS(!Snapshots.empty());
        return Snapshots.begin()->second;
    }

    ISchema::TPtr GetLastSchemaBeforeOrEqualSnapshotOptional(const ui64 version) const {
        ISchema::TPtr res = nullptr;
        for (auto it = SnapshotByVersion.rbegin(); it != SnapshotByVersion.rend(); ++it) {
            if (it->first <= version) {
                res = it->second;
                break;
            }
        }
        return res;
    }

    ISchema::TPtr GetLastSchema() const {
        Y_ABORT_UNLESS(!Snapshots.empty());
        return Snapshots.rbegin()->second;
    }

    bool IsEmpty() const {
        return Snapshots.empty();
    }

    const std::shared_ptr<arrow::Schema>& GetPrimaryKey() const noexcept {
        return PrimaryKey;
    }

    const TIndexInfo* AddIndex(const TSnapshot& snapshot, TIndexInfo&& indexInfo);

    bool LoadShardingInfo(IDbWrapper& db);
};
}   // namespace NKikimr::NOlap
