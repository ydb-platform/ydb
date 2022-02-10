#pragma once

#include "defs.h"
#include "utility.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {

    constexpr ui64 MaxBatchedPutRequests = 16;

    template <typename T>
    class TBatchedVec : public TStackVec<T, MaxBatchedPutRequests> {
    public:
        using TBase = TStackVec<T, MaxBatchedPutRequests>;
        using TSelf = TBatchedVec<T>;

        using typename TBase::const_iterator;
        using typename TBase::const_reverse_iterator;
        using typename TBase::iterator;
        using typename TBase::reverse_iterator;
        using typename TBase::size_type;
        using typename TBase::value_type;

        using TBase::TBase;
        using TBase::operator=;

        static TString ToString(const TSelf &self) {
            return FormatList(self);
        }

        TString ToString() const {
            return ToString(*this);
        }

        void Output(IOutputStream &os) const {
            FormatList(os, *this);
        }
    };
}
