#pragma once

#include "index_info.h"

#include <ydb/core/tx/columnshard/engines/portions/index_chunk.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/accessor.h>

#include <ydb/library/accessor/accessor.h>

#include <util/system/types.h>

namespace NKikimr::NOlap {

class TColumnIndexProperties {
private:
    YDB_READONLY_DEF(bool, InheritPortionStorage);

public:
    TColumnIndexProperties(const TIndexChunk& chunk)
        : InheritPortionStorage(chunk.GetInheritPortionStorage())
    {
    }
    TColumnIndexProperties(const NIndexes::TIndexMetaContainer& chunk)
        : InheritPortionStorage(chunk->GetInheritPortionStorage())
    {
    }

    bool operator==(const TColumnIndexProperties& other) const {
        return InheritPortionStorage == other.InheritPortionStorage;
    }
};

class TFreshColumnIndexAccessor: public IColumnIndexAccessor {
private:
    THashMap<ui32, TColumnIndexProperties> IndexProperties;

public:
    TFreshColumnIndexAccessor(const TIndexInfo& indexInfo) {
        for (const auto& [id, index] : indexInfo.GetIndexes()) {
            AFL_VERIFY(IndexProperties.emplace(id, TColumnIndexProperties(index)).second);
        }
    }

    bool IsIndexInheritPortionStorage(const ui64 indexId) const override {
        auto* findProperties = IndexProperties.FindPtr(indexId);
        AFL_VERIFY(findProperties)("index", indexId);
        return findProperties->GetInheritPortionStorage();
    }
};
}   // namespace NKikimr::NOlap
