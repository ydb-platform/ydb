#include "dq_join_common.h"

namespace NKikimr::NMiniKQL {

TKeyTypes KeyTypesFromColumns(const std::vector<TType*>& types, const std::vector<ui32>& keyIndexes) {
    TKeyTypes kt;
    std::ranges::copy(keyIndexes | std::views::transform([&types](ui32 typeIndex) {
                          const TType* type = types[typeIndex];
                          MKQL_ENSURE(type->IsData(), "exepected data type");
                          return std::pair{*static_cast<const TDataType*>(type)->GetDataSlot(), false};
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
    case EJoinKind::SemiMask:
        return TJoinKindTag<EJoinKind::SemiMask>{};
    case EJoinKind::SemiSide:
        return TJoinKindTag<EJoinKind::SemiSide>{};
    case EJoinKind::Cross:
        return TJoinKindTag<EJoinKind::Cross>{};
    default:
        MKQL_ENSURE(false, "unreachable");
    }
}
} // namespace NKikimr::NMiniKQL