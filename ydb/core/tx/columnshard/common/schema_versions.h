#pragma once

#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TVersionCounters {
public:
    class TSchemaKey {
    private:
        YDB_READONLY_DEF(ui32, Id);
        YDB_READONLY_DEF(ui64, PlanStep);
        YDB_READONLY_DEF(ui64, TxId);

    public:
        TSchemaKey() = default;

        TSchemaKey(const ui32 id, const ui64 planStep, const ui64 txId)
            : Id(id)
            , PlanStep(planStep)
            , TxId(txId)
        {
        }

        constexpr auto operator<=>(const TSchemaKey&) const noexcept = default;
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

    void VersionAddRef(const ui64 version, const char* source = "portions") {
        ui32& count = VersionCounters[version];
        if (count == 0) {
            VersionsToErase.erase(version);
        }
        count++;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_addref")("source", source)("version", version)("ref_count", count);
    }

    void VersionRemoveRef(const ui64 version, const char* source = "portions") {
        ui32& count = VersionCounters[version];
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "version_removref")("source", source)("version", version)("ref_count", count - 1);
        AFL_VERIFY(count > 0);
        if (--count == 0) {
            AFL_VERIFY(VersionsToErase.insert(version).second);
            AFL_VERIFY(VersionCounters.erase(version));
        }
        return;
    }

    bool HasUnusedSchemaVersions() const {
        return !VersionsToErase.empty();
    }

    THashSet<ui64> ExtractVersionsToErase() {
        THashSet<ui64> result = std::move(VersionsToErase);
        VersionsToErase.clear();
        return result;
    }
};

}