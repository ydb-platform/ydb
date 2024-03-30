#include "dphyp_conflict_rules_collector.h"
#include <util/generic/hash_set.h>

namespace NYql::NDq::NDphyp {

bool OperatorIsCommut(EJoinKind joinKind) {
    switch (joinKind) {
        case EJoinKind::InnerJoin:
        case EJoinKind::OuterJoin:
        case EJoinKind::Exclusion:
            return true;
        default:
            return false;
    }

    Y_UNREACHABLE();
}

bool OperatorsAreAssoc(EJoinKind lhs, EJoinKind rhs) {
    static THashMap<EJoinKind, THashSet<EJoinKind>> ASSOC_TABLE = {
        {EJoinKind::InnerJoin, {EJoinKind::InnerJoin, EJoinKind::LeftJoin, EJoinKind::LeftSemi}},
        {EJoinKind::LeftJoin, {EJoinKind::LeftJoin}},
        {EJoinKind::OuterJoin, {EJoinKind::LeftJoin, EJoinKind::OuterJoin}}
    };

    if (!(ASSOC_TABLE.contains(lhs) && ASSOC_TABLE.contains(rhs))) {
        return false;
    }

    return ASSOC_TABLE[lhs].contains(rhs);
}

bool OperatorsAreLeftAsscom(EJoinKind lhs, EJoinKind rhs) {
    static THashMap<EJoinKind, THashSet<EJoinKind>> LASSCOM_TABLE = {
        {EJoinKind::InnerJoin, {EJoinKind::InnerJoin, EJoinKind::LeftJoin}},
        {EJoinKind::LeftSemi, {EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin}},
        {EJoinKind::LeftJoin, {EJoinKind::InnerJoin, EJoinKind::LeftSemi, EJoinKind::LeftJoin, EJoinKind::OuterJoin}},
        {EJoinKind::OuterJoin, {EJoinKind::LeftJoin, EJoinKind::OuterJoin}}
    };

    if (!(LASSCOM_TABLE.contains(lhs) && LASSCOM_TABLE.contains(rhs))) {
        return false;
    }

    return LASSCOM_TABLE[lhs].contains(rhs);
}

bool OperatorsAreRightAsscom(EJoinKind lhs, EJoinKind rhs) {
    static THashMap<EJoinKind, THashSet<EJoinKind>> RASSCOM_TABLE = {
        {EJoinKind::InnerJoin, {EJoinKind::InnerJoin}},
        {EJoinKind::LeftJoin, {}},
        {EJoinKind::OuterJoin, {EJoinKind::OuterJoin}}
    };

    if (!(RASSCOM_TABLE.contains(lhs) && RASSCOM_TABLE.contains(lhs))) {
        return false;
    }

    return RASSCOM_TABLE[lhs].contains(rhs);
}

}