#pragma once

#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NTest {

class TTestPathIdTranslator: public IPathIdTranslator {
private:
    THashMap<NColumnShard::TInternalPathId, std::set<NColumnShard::TSchemeShardLocalPathId>> InternalToSchemeShardLocal;

public:
    void Add(const NColumnShard::TInternalPathId internalPathId, std::set<NColumnShard::TSchemeShardLocalPathId> schemeShardLocalPathIds) {
        InternalToSchemeShardLocal[internalPathId] = std::move(schemeShardLocalPathIds);
    }

    std::optional<std::set<NColumnShard::TSchemeShardLocalPathId>> ResolveSchemeShardLocalPathIdsOptional(
        const NColumnShard::TInternalPathId internalPathId) const override {
        if (const auto* p = InternalToSchemeShardLocal.FindPtr(internalPathId)) {
            return *p;
        }
        return std::nullopt;
    }

    std::optional<NColumnShard::TInternalPathId> ResolveInternalPathIdOptional(
        const NColumnShard::TSchemeShardLocalPathId, const bool) const override {
        return std::nullopt;
    }
};

}   // namespace NKikimr::NOlap::NTest
