#include "dq_hash_join_table.h"

namespace NKikimr::NMiniKQL::NJoinTable {
bool NeedToTrackUnusedRightTuples(EJoinKind kind) {
    switch (kind) {
        using enum NKikimr::NMiniKQL::EJoinKind;
        case Exclusion:
        case Full:
        case Right:
        case RightOnly:
        case RightSemi:
        return true;
        default:
        return false;
    }
}
bool NeedToTrackUnusedLeftTuples(EJoinKind kind) {
    switch (kind) {
        using enum NKikimr::NMiniKQL::EJoinKind;
        case Exclusion:
        case Full:
        case Left:
        case LeftOnly:
        return true;
        default:
        return false;
    }
}

}