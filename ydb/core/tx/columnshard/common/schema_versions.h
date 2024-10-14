#pragma once

#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TVersionCounts {
public:

    class TSchemaKey {
    public:
        ui64 PlanStep;
        ui64 TxId;
        ui32 Id;

    public:
        TSchemaKey() = default;

        TSchemaKey(ui32 id, ui64 planStep, ui64 txId)
            : PlanStep(planStep)
            , TxId(txId)
            , Id(id)
        {
        }
    };

    using TVersionToKey = THashMap<ui64, std::vector<TSchemaKey>>;

public:
    TVersionToKey VersionToKey;
    THashSet<ui64> VersionsToErase;

private:
    THashMap<ui64, ui32> VersionCounters;

public:
    void VersionAddRef(ui64 version, ui32 source = 0) {
        ui32& count = VersionCounters[version];
        if (count == 0) {
            VersionsToErase.erase(version);
        }
        count++;
        Y_UNUSED(count);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_addref")("source", source)("version", version)("ref_count", count);
    }

    ui32 VersionRemoveRef(ui64 version, ui32 source = 0) {
        ui32& count = VersionCounters[version];
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_removref")("source", source)("version", version)("ref_count", count - 1);
        AFL_VERIFY(count > 0);
        if (--count == 0) {
            VersionsToErase.insert(version);
            VersionCounters.erase(version);
        }
        return count;
    }

    bool IsEmpty(ui64 lastVersion) const {
        return VersionsToErase.empty() || ((VersionsToErase.size() == 1) && (*VersionsToErase.begin() == lastVersion));
    }

    template<class Processor>
    void EnumerateVersionsToErase(Processor&& processor) const {
        for (ui64 version: VersionsToErase) {
            auto iter = VersionToKey.find(version);
            AFL_VERIFY(iter != VersionToKey.end());
            for (auto& key: iter->second) {
                processor(version, key);
            }
        }
    }
};

}