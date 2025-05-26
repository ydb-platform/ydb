#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

class TPathIdTranslator {
    ui64 Offset = 0;
    THashMap<TInternalPathId, TSchemeShardLocalPathId> InternalToSchemeShardLocal;
    THashMap<TSchemeShardLocalPathId, TInternalPathId> SchemeShardLocalToInternal;
public:
    TPathIdTranslator();
    TInternalPathId CreateNewInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId);
    void RemovePathId(const TInternalPathId internalPathId);
    std::optional <TSchemeShardLocalPathId> GetSchemeShardLocalPathId(const TInternalPathId internalPathId) const;
    TSchemeShardLocalPathId GetSchemeShardLocalPathIdVerified(const TInternalPathId internalPathId) const;
    std::optional<TInternalPathId> GetInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId) const;
    TInternalPathId GetInternalPathIdVerified(const TSchemeShardLocalPathId schemeShardLocalPathId) const;
};

} //namespace NKikimr::NColumnShard
