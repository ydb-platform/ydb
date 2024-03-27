#include "dphyp_conflict_rules_collector.h"
#include <util/generic/hash_set.h>

namespace NYql::NDq::NDphyp {

bool OperatorIsCommut(EJoinKind) {
    return true;
}

bool OperatorsAreAssoc(EJoinKind lhs, EJoinKind rhs) {
    static THashMap<EJoinKind, THashSet<EJoinKind>> ASSOC_TABLE = {
        {EJoinKind::InnerJoin, {EJoinKind::InnerJoin, EJoinKind::LeftJoin}},
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
        {}, {},
        {}, {},
        {}, {}
    };

    if (!(LASSCOM_TABLE.contains(lhs) && LASSCOM_TABLE.contains(rhs))) {
        return false;
    }

    return LASSCOM_TABLE[lhs].contains(rhs);
}

bool OperatorsAreRightAsscom(EJoinKind lhs, EJoinKind rhs) {
    static THashMap<EJoinKind, THashSet<EJoinKind>> RASSCOM_TABLE = {
        {}, {},
        {}, {},
        {}, {}
    };

    if (!(RASSCOM_TABLE.contains(lhs) && RASSCOM_TABLE.contains(lhs))) {
        return false;
    }

    return RASSCOM_TABLE[lhs].contains(rhs);
}

}