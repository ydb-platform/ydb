#pragma once

#include <ydb/core/tx/columnshard/common/log.h>

namespace NKikimr::NColumnShard {

using TVersionToKey = THashMap<ui64, std::vector<NKikimr::NOlap::TSchemaKey>>;

class TInsertKey {
public:
    ui64 PlanStep;
    ui64 TxId;
    ui64 PathId;
    TString DedupId;
    ui8 RecType;

public:
    TInsertKey() = default;

    TInsertKey(ui64 planStep, ui64 txId, ui64 pathId, const TString& dedupId, ui8 recType)
        : PlanStep(planStep)
        , TxId(txId)
        , PathId(pathId)
        , DedupId(dedupId)
        , RecType(recType)
    {
    }

    bool operator==(const TInsertKey& other) const {
        return (PlanStep == other.PlanStep) && (TxId == other.TxId) && (PathId == other.PathId) && (DedupId == other.DedupId) && (RecType == other.RecType);
    }

    ui64 Hash() const {
        return CombineHashes(PlanStep, CombineHashes(TxId, CombineHashes(PathId, CombineHashes(hash<TString>()(DedupId), (ui64)RecType))));
    }
};

class TVersionCounts {
private:
    struct Hasher {
        inline size_t operator()(const TInsertKey& key) const noexcept {
            return key.Hash();
        }
    };

private:
    THashMap<TInsertKey, ui64, Hasher> InsertVersions;
    THashMap<std::pair<ui64, ui64>, ui64> PortionVersions;
    THashMap<ui64, ui32> VersionCounts;

public:
    template<class Key, class Versions>
    void VersionAddRef(Versions& versions, const Key& portion, ui64 version) {
        ui64& curVer = versions[portion];
        if (curVer != 0) {// Portion is already in the local database, no need to increase ref count
            TEMPLOG("Schema version is already written");
            AFL_VERIFY(version == curVer);
            return;
        }
        curVer = version;
        ui32& refCount = VersionCounts[version];
        refCount++;
        TEMPLOG("Ref count of schema version " << version << " changed from " << refCount - 1 << " to " << refCount << " this " << (ui64)this);
    }

    template<class Key, class Versions>
    ui32 VersionRemoveRef(Versions& versions, const Key& portion, ui64 version) {
        auto iter = versions.find(portion);
        if (iter == versions.end()) { //Portion is already removed from local databae, no need to decrease ref count
            return (ui32)-1;
        }
        versions.erase(iter);
        ui32& refCount = VersionCounts[version];
        AFL_VERIFY(refCount > 0);
        TEMPLOG("Ref count of schema version " << version << " changed from " << refCount << " to " << refCount - 1 << " this " << (ui64)this);
        return --refCount;
    }

    void VersionAddRef(ui64 portion, ui64 pathId, ui64 version) {
        VersionAddRef(PortionVersions, std::pair<ui64, ui64>(portion, pathId), version);
    }

    ui32 VersionRemoveRef(ui64 portion, ui64 pathId, ui64 version) {
        return VersionRemoveRef(PortionVersions, std::pair<ui64, ui64>(portion, pathId), version);
    }

    void VersionAddRef(const TInsertKey& key, ui64 version) {
        VersionAddRef(InsertVersions, key, version);
    }

    ui32 VersionRemoveRef(const TInsertKey& key, ui64 version) {
        return VersionRemoveRef(InsertVersions, key, version);
    }

};

}
