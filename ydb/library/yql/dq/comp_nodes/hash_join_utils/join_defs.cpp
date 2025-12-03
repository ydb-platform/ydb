#include "join_defs.h"

namespace NKikimr::NMiniKQL {

bool IsBucketSpilled(const TSides<TBucket>& bucket) {
    TSides<bool> answers;
    ForEachSide([&](ESide side) { answers.SelectSide(side) = bucket.SelectSide(side).IsSpilled(); });
    MKQL_ENSURE(answers.Build == answers.Probe, "pair of buckets should be in 1 state");
    return answers.Build;
}

} // namespace NKikimr::NMiniKQL