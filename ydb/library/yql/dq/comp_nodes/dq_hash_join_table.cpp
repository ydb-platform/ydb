#include "dq_hash_join_table.h"

namespace NKikimr::NMiniKQL::NJoinTable {
bool NeedToTrackUnusedRightTuples(EJoinKind kind) {
    Cout << "NeedToTrackUnusedRightTuples: " << (static_cast<int>(kind)&4) << Endl;
    return (static_cast<int>(kind)&4) == 4;
}
bool NeedToTrackUnusedLeftTuples(EJoinKind kind) {
    return static_cast<int>(kind)&1 == 1;
}

}