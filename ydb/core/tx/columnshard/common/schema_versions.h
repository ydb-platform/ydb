#pragma once

#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TVersionCounters {
public:
    class TSchemaKey {
    private:
        YDB_READONLY_DEF(ui64, PlanStep);
        YDB_READONLY_DEF(ui64, TxId);
        YDB_READONLY_DEF(ui32, Id);

    public:
        TSchemaKey() = default;

        TSchemaKey(const ui32 id, const ui64 planStep, const ui64 txId)
            : PlanStep(planStep)
            , TxId(txId)
            , Id(id)
        {
        }
    };

    using TVersionToKey = THashMap<ui64, std::vector<TSchemaKey>>;

private:
    TVersionToKey VersionToKey;
    THashSet<ui64> VersionsToErase;
    THashMap<ui64, ui32> VersionCounters;

public:
    TVersionToKey& GetVersionToKey() {
        return VersionToKey;
    }

    void VersionAddRef(const ui64 version, const ui32 source = 0) {
        ui32& count = VersionCounters[version];
        if (count == 0) {
            VersionsToErase.erase(version);
        }
        count++;
        Y_UNUSED(count);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_addref")("source", source)("version", version)("ref_count", count);
    }

    ui32 VersionRemoveRef(const ui64 version, const ui32 source = 0) {
        ui32& count = VersionCounters[version];
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_removref")("source", source)("version", version)("ref_count", count - 1);
        AFL_VERIFY(count > 0);
        if (--count == 0) {
            VersionsToErase.insert(version);
            VersionCounters.erase(version);
        }
        return count;
    }

    bool HasUnusedSchemaVersions() const {
        return !VersionsToErase.empty();
    }

    THashSet<ui64>& GetVersionsToErase() {
        return VersionsToErase;
    }

    THashMap<ui64, ui32>& GetVersionCounters() {
        return VersionCounters;
    }
};

}