#pragma once
#include "status.h"

#include <ydb/library/conclusion/generic/result.h>

namespace NKikimr {

template <typename TResult>
class TConclusion: public TConclusionImpl<TConclusionStatus, TResult> {
private:
    using TBase = TConclusionImpl<TConclusionStatus, TResult>;
public:
    using TBase::TBase;

    TConclusion<TResult> AddMessageInfo(const TString& info) const {
        if (TBase::IsSuccess()) {
            return *this;
        } else {
            return TConclusionStatus::Fail(TBase::GetErrorMessage() + "; " + info);
        }
    }
};

}   // namespace NKikimr
