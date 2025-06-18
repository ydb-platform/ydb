#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceSelfJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceJoinWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceSelfJoinWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx);

// Helper predicates used both in join implementation and its unit-tests.
// They indicate whether result of join is guaranteed empty when one of the sides has no rows
// and therefore allow to skip reading the opposite side completely.
inline constexpr bool ShouldSkipRightIfLeftEmpty(EJoinKind kind) {
    switch (kind) {
        case EJoinKind::Inner:
        case EJoinKind::Left:
        case EJoinKind::LeftOnly:
        case EJoinKind::LeftSemi:
        case EJoinKind::Cross:
            return true;
        default:
            return false;
    }
}

inline constexpr bool ShouldSkipLeftIfRightEmpty(EJoinKind kind) {
    switch (kind) {
        case EJoinKind::Inner:
        case EJoinKind::Right:
        case EJoinKind::RightOnly:
        case EJoinKind::RightSemi:
        case EJoinKind::Cross:
            return true;
        default:
            return false;
    }
}

}
}
