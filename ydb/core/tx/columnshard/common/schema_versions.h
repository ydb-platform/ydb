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
    THashMap<ui64, ui32> VersionCounts;

public:
    void VersionAddRef(ui64 version) {
        ui32 count = ++VersionCounts[version];
        Y_UNUSED(count);
        if (TlsActivationContext != nullptr) { // Can be null in tests
            LOG_S_DEBUG("VersionAddRef, version " <<  version << " ref_count " << count << " this " << (ui64)this);
        }
    }

    ui32 VersionRemoveRef(ui64 version) {
        ui32& count = VersionCounts[version];
        if (TlsActivationContext != nullptr) { // Can be null in tests
            LOG_S_DEBUG("VersionRemoveRef, version " <<  version << " ref_count " << count - 1 << " this " << (ui64)this);
        }
        if (--count == 0) {
            VersionsToErase.insert(version);
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