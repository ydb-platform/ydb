#pragma once
#include "status.h"
#include <util/system/yassert.h>
#include <optional>

namespace NKikimr {

template <class TResult>
class TConclusion {
private:
    std::variant<TConclusionStatus, TResult> Result;
public:

    TConclusion(TConclusionStatus&& status)
        : Result(std::move(status)) {
        auto* resStatus = std::get_if<TConclusionStatus>(&Result);
        Y_VERIFY(resStatus->IsFail());
    }

    bool IsFail() const {
        return std::get_if<TConclusionStatus>(&Result);
    }

    bool IsSuccess() const {
        return std::get_if<TResult>(&Result);
    }

    TConclusion(const TConclusionStatus& status)
        : Result(status) {
        Y_VERIFY(IsFail());
    }

    TConclusion(TResult&& result)
        : Result(std::move(result)) {
    }

    TConclusion(const TResult& result)
        : Result(result) {
    }

    const TConclusionStatus& GetError() const {
        auto result = std::get_if<TConclusionStatus>(&Result);
        Y_VERIFY(result, "incorrect object for error request");
        return *result;
    }

    const TResult& GetResult() const {
        auto result = std::get_if<TResult>(&Result);
        Y_VERIFY(result, "incorrect object for result request");
        return *result;
    }

    TResult&& DetachResult() {
        auto result = std::get_if<TResult>(&Result);
        Y_VERIFY(result, "incorrect object for result request");
        return std::move(*result);
    }

    const TResult& operator*() const {
        return GetResult();
    }

    bool operator!() const {
        return IsFail();
    }

    explicit operator bool() const {
        return IsSuccess();
    }

    const TString& GetErrorMessage() const {
        auto* status = std::get_if<TConclusionStatus>(&Result);
        if (!status) {
            return Default<TString>();
        } else {
            return status->GetErrorMessage();
        }
    }
};

}
