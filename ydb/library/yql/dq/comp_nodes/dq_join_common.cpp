#include "dq_join_common.h"
#include <yql/essentials/minikql/mkql_node_cast.h>
namespace NKikimr::NMiniKQL {

TKeyTypes KeyTypesFromColumns(const std::vector<TType*>& types, const std::vector<ui32>& keyIndexes) {
    TKeyTypes kt;
    std::ranges::copy(keyIndexes | std::views::transform([&types](ui32 typeIndex) {
                          const TType* type = types[typeIndex];
                          MKQL_ENSURE(type->IsData(), "exepected data type");
                          return std::pair{*AS_TYPE(TDataType, type)->GetDataSlot(), false};
                      }), std::back_inserter(kt));
    return kt;
}

TTypedJoinKind TypifyJoinKind(EJoinKind kind) {
    switch (kind) {
    case EJoinKind::LeftOnly:
        return TJoinKindTag<EJoinKind::LeftOnly>{};
    case EJoinKind::Inner:
        return TJoinKindTag<EJoinKind::Inner>{};
    case EJoinKind::RightOnly:
        return TJoinKindTag<EJoinKind::RightOnly>{};
    case EJoinKind::Left:
        return TJoinKindTag<EJoinKind::Left>{};
    case EJoinKind::Right:
        return TJoinKindTag<EJoinKind::Right>{};
    case EJoinKind::Exclusion:
        return TJoinKindTag<EJoinKind::Exclusion>{};
    case EJoinKind::Full:
        return TJoinKindTag<EJoinKind::Full>{};
    case EJoinKind::LeftSemi:
        return TJoinKindTag<EJoinKind::LeftSemi>{};
    case EJoinKind::RightSemi:
        return TJoinKindTag<EJoinKind::RightSemi>{};
    default:
        MKQL_ENSURE(false, "unsupported join kind");
    }
}
} // namespace NKikimr::NMiniKQL