#include "dq_opt_conflict_rules_collector.h"
#include <util/generic/hash_set.h>

namespace NYql::NDq {

/* To make ASSOC, RASSCOM, LASSCOM tables simplier */
EJoinKind GetEquivalentJoinByAlgebraicProperties(EJoinKind joinKind) {
    switch (joinKind) {
        case EJoinKind::Exclusion:
            return EJoinKind::OuterJoin;
        case EJoinKind::LeftOnly:
            return EJoinKind::LeftJoin;
        default:
            return joinKind;
    }    
}

bool OperatorIsCommutative(EJoinKind joinKind) {
    joinKind = GetEquivalentJoinByAlgebraicProperties(joinKind);
    switch (joinKind) {
        case EJoinKind::InnerJoin:
        case EJoinKind::OuterJoin:
        case EJoinKind::Cross:
            return true;
        default:
            return false;
    }

    Y_UNREACHABLE();
}

bool OperatorsAreAssociative(EJoinKind lhs, EJoinKind rhs) {
    lhs = GetEquivalentJoinByAlgebraicProperties(lhs);
    rhs = GetEquivalentJoinByAlgebraicProperties(rhs);

    static THashMap<EJoinKind, THashSet<EJoinKind>> ASSOC_TABLE = {
        {EJoinKind::Cross, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::InnerJoin, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::LeftJoin, {EJoinKind::LeftJoin}},
        {EJoinKind::OuterJoin, {EJoinKind::LeftJoin, EJoinKind::OuterJoin}}
    };

    if (!(ASSOC_TABLE.contains(lhs))) {
        return false;
    }

    return ASSOC_TABLE[lhs].contains(rhs);
}

bool OperatorsAreLeftAsscom(EJoinKind lhs, EJoinKind rhs) {
    lhs = GetEquivalentJoinByAlgebraicProperties(lhs);
    rhs = GetEquivalentJoinByAlgebraicProperties(rhs);

    static THashMap<EJoinKind, THashSet<EJoinKind>> LASSCOM_TABLE = {
        {EJoinKind::Cross, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::InnerJoin, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::LeftSemi, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::LeftJoin, {EJoinKind::Cross, EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin, EJoinKind::OuterJoin}},
        {EJoinKind::OuterJoin, {EJoinKind::LeftJoin, EJoinKind::OuterJoin}}
    };

    if (!(LASSCOM_TABLE.contains(lhs))) {
        return false;
    }

    return LASSCOM_TABLE[lhs].contains(rhs);
}

bool OperatorsAreRightAsscom(EJoinKind lhs, EJoinKind rhs) {
    lhs = GetEquivalentJoinByAlgebraicProperties(lhs);
    rhs = GetEquivalentJoinByAlgebraicProperties(rhs);

    static THashMap<EJoinKind, THashSet<EJoinKind>> RASSCOM_TABLE = {
        {EJoinKind::Cross, {EJoinKind::Cross, EJoinKind::InnerJoin}},
        {EJoinKind::InnerJoin, {EJoinKind::Cross, EJoinKind::InnerJoin}},
        {EJoinKind::OuterJoin, {EJoinKind::OuterJoin}}
    };

    if (!(RASSCOM_TABLE.contains(lhs))) {
        return false;
    }

    return RASSCOM_TABLE[lhs].contains(rhs);
}

} // namespace NYql::NDq
