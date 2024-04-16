#pragma once

#include <ydb/library/accessor/accessor.h>

#include <ydb/library/actors/core/log.h>

#include <util/system/types.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <set>

namespace NKikimr::NOlap {

class TEntityGroups {
private:
    THashMap<TString, std::set<ui32>> GroupEntities;
    THashSet<ui32> UsedEntityIds;
    YDB_READONLY_DEF(TString, DefaultGroupName);
public:
    TEntityGroups(const TString& defaultGroupName)
        : DefaultGroupName(defaultGroupName) {

    }

    bool IsEmpty() const {
        return GroupEntities.empty();
    }

    void Add(const ui32 entityId, const TString& groupName) {
        AFL_VERIFY(UsedEntityIds.emplace(entityId).second);
        AFL_VERIFY(GroupEntities[groupName].emplace(entityId).second);
    }

    THashMap<TString, std::set<ui32>>::const_iterator begin() const {
        return GroupEntities.begin();
    }

    THashMap<TString, std::set<ui32>>::const_iterator end() const {
        return GroupEntities.end();
    }
};

class TSplitSettings {
private:
    static const inline i64 DefaultMaxBlobSize = 8 * 1024 * 1024;
    static const inline i64 DefaultMinBlobSize = 4 * 1024 * 1024;
    static const inline i64 DefaultMinRecordsCount = 10000;
    static const inline i64 DefaultMaxPortionSize = 6 * DefaultMaxBlobSize;
    YDB_ACCESSOR(i64, MaxBlobSize, DefaultMaxBlobSize);
    YDB_ACCESSOR(i64, MinBlobSize, DefaultMinBlobSize);
    YDB_ACCESSOR(i64, MinRecordsCount, DefaultMinRecordsCount);
    YDB_ACCESSOR(i64, MaxPortionSize, DefaultMaxPortionSize);
public:
    ui64 GetExpectedRecordsCountOnPage() const {
        return 1.5 * MinRecordsCount;
    }

    ui64 GetExpectedUnpackColumnChunkRawSize() const {
        return (ui64)50 * 1024 * 1024;
    }

    ui64 GetExpectedPortionSize() const {
        return MaxPortionSize;
    }
};
}
