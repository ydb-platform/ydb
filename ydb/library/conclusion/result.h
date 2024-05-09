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
        Y_ABORT_UNLESS(resStatus->IsFail());
    }

    bool IsFail() const {
        return std::get_if<TConclusionStatus>(&Result);
    }

    bool IsSuccess() const {
        return std::get_if<TResult>(&Result);
    }

    TConclusion(const TConclusionStatus& status)
        : Result(status) {
        Y_ABORT_UNLESS(IsFail());
    }

    template <class TResultArg>
    TConclusion(TResultArg&& result)
        : Result(std::move(result)) {
    }

    template <class TResultArg>
    TConclusion(const TResultArg& result)
        : Result(result) {
    }

    const TConclusionStatus& GetError() const {
        auto result = std::get_if<TConclusionStatus>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for error request");
        return *result;
    }

    const TResult& GetResult() const {
        auto result = std::get_if<TResult>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for result request");
        return *result;
    }

    TResult& MutableResult() {
        auto result = std::get_if<TResult>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for result request");
        return *result;
    }

    TResult&& DetachResult() {
        auto result = std::get_if<TResult>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for result request: %s", GetErrorMessage().data());
        return std::move(*result);
    }

    const TResult* operator->() const {
        return &GetResult();
    }

    TResult* operator->() {
        return &MutableResult();
    }

    const TResult& operator*() const {
        return GetResult();
    }

    bool operator!() const {
        return IsFail();
    }

    operator TConclusionStatus() const {
        return GetError();
    }

    const TString& GetErrorMessage() const {
        auto* status = std::get_if<TConclusionStatus>(&Result);
        if (!status) {
            return Default<TString>();
        } else {
            return status->GetErrorMessage();
        }
    }

    Ydb::StatusIds::StatusCode GetStatus() const {
        auto* status = std::get_if<TConclusionStatus>(&Result);
        if (!status) {
            return Ydb::StatusIds::SUCCESS;
        } else {
            return status->GetStatus();
        }
    }
};

}
