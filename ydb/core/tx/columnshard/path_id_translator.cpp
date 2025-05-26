#include <ydb/core/tx/columnshard/path_id_translator.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

namespace {

ui64 RandomOffsetForTests() {
    if (false) { //(NYDBTest::TControllers::GetColumnShardController()->UseRandomOffsetForInternalPathIds())
        return RandomNumber(1000);
    } else {
        return 0;
    }
}

} //namespace

TPathIdTranslator::TPathIdTranslator()
    : Offset(RandomOffsetForTests()) {
}

TInternalPathId TPathIdTranslator::CreateNewInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId) {
    const auto& newInternalPathId = TInternalPathId::FromRawValue(schemeShardLocalPathId.GetRawValue() + Offset);
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_BACKGROUND)
        ("scheme_shard_local_path_id", schemeShardLocalPathId)
        ("internal_path_id", newInternalPathId)
    ;
    AFL_VERIFY(SchemeShardLocalToInternal.emplace(schemeShardLocalPathId, newInternalPathId).second);
    AFL_VERIFY(InternalToSchemeShardLocal.emplace(newInternalPathId, schemeShardLocalPathId).second);
    return newInternalPathId;
}

void TPathIdTranslator::RemovePathId(const TInternalPathId internalPathId) {
    const auto* schemeShardLocalPathId = InternalToSchemeShardLocal.FindPtr(internalPathId);
    AFL_VERIFY(schemeShardLocalPathId)("internal_path_id", internalPathId);
    AFL_VERIFY(InternalToSchemeShardLocal.erase(internalPathId));
    AFL_VERIFY(SchemeShardLocalToInternal.erase(*schemeShardLocalPathId));
}
std::optional <TSchemeShardLocalPathId> TPathIdTranslator::GetSchemeShardLocalPathId(const TInternalPathId pathId) const {
    if (const auto* p = InternalToSchemeShardLocal.FindPtr(pathId)) {
        return *p;
    }
    return {};
}

TSchemeShardLocalPathId TPathIdTranslator::GetSchemeShardLocalPathIdVerified(const TInternalPathId internalPathId) const {
    const auto& result = GetSchemeShardLocalPathId(internalPathId);
    AFL_VERIFY(result.has_value())("internal_path_id", internalPathId);;
    return *result;
}

std::optional<TInternalPathId> TPathIdTranslator::GetInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId) const {
    if (const auto* p = SchemeShardLocalToInternal.FindPtr(schemeShardLocalPathId)) {
        return *p;
    }
    return {};
}

TInternalPathId TPathIdTranslator::GetInternalPathIdVerified(const TSchemeShardLocalPathId schemeShardLocalPathId) const {
    const auto& result = GetInternalPathId(schemeShardLocalPathId);
    AFL_VERIFY(result.has_value())("scheme_shard_local_path_id", schemeShardLocalPathId);
    return *result;
}

} //namespace NKikimr::NColumnShard
