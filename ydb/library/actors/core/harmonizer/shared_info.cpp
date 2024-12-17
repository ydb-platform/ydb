#include "shared_info.h"
#include <util/string/builder.h>

#include <ydb/library/actors/core/executor_pool_shared.h>

namespace NActors {

void TSharedInfo::Init(i16 poolCount) {
    HasSharedThread.resize(poolCount, false);
    HasSharedThreadWhichWasNotBorrowed.resize(poolCount, false);
    HasBorrowedSharedThread.resize(poolCount, false);
}

void TSharedInfo::Pull(const ISharedExecutorPool& shared) {
    auto sharedState = shared.GetState();
    for (ui32 poolIdx = 0; poolIdx < HasSharedThread.size(); ++poolIdx) {
        i16 threadIdx = sharedState.ThreadByPool[poolIdx];
        if (threadIdx != -1) {
            HasSharedThread[poolIdx] = true;
            if (sharedState.PoolByBorrowedThread[threadIdx] == -1) {
                HasSharedThreadWhichWasNotBorrowed[poolIdx] = true;
            } else {
                HasSharedThreadWhichWasNotBorrowed[poolIdx] = false;
            }
        }
        if (sharedState.BorrowedThreadByPool[poolIdx] != -1) {
            HasBorrowedSharedThread[poolIdx] = true;
        } else {
            HasBorrowedSharedThread[poolIdx] = false;
        }
    }
}

TString TSharedInfo::ToString() const {
    TStringBuilder builder;
    builder << "{";
    builder << "HasSharedThread: \"";
    for (ui32 i = 0; i < HasSharedThread.size(); ++i) {
        builder << (HasSharedThread[i] ? "1" : "0");
    }
    builder << "\", ";
    builder << "HasSharedThreadWhichWasNotBorrowed: \"";
    for (ui32 i = 0; i < HasSharedThreadWhichWasNotBorrowed.size(); ++i) {
        builder << (HasSharedThreadWhichWasNotBorrowed[i] ? "1" : "0");
    }
    builder << "\", ";
    builder << "HasBorrowedSharedThread: \"";
    for (ui32 i = 0; i < HasBorrowedSharedThread.size(); ++i) {
        builder << (HasBorrowedSharedThread[i] ? "1" : "0");
    }
    builder << "\"}";
    return builder;
}

} // namespace NActors
