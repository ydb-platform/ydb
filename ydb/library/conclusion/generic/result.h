#pragma once
#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <optional>
#include <variant>

namespace NKikimr {

template <class TStatus, class TResult>
class TConclusionImpl {
private:
    std::variant<TStatus, TResult> Result;

public:
    TConclusionImpl(TStatus&& status)
        : Result(std::move(status)) {
        auto* resStatus = std::get_if<TStatus>(&Result);
        Y_ABORT_UNLESS(resStatus->IsFail());
    }

    bool IsFail() const {
        return std::holds_alternative<TStatus>(Result);
    }

    bool IsSuccess() const {
        return std::holds_alternative<TResult>(Result);
    }

    TConclusionImpl(const TStatus& status)
        : Result(status) {
        Y_ABORT_UNLESS(IsFail());
    }

    template <class TResultArg>
    TConclusionImpl(TResultArg&& result)
        : Result(std::move(result)) {
    }

    template <class TResultArg>
    TConclusionImpl(const TResultArg& result)
        : Result(result) {
    }

    template <class TResultArg>
    TConclusionImpl(TResultArg& result)
        : Result(result) {
    }

    const TStatus& GetError() const {
        auto result = std::get_if<TStatus>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for error request");
        return *result;
    }

    const TResult& GetResult() const {
        auto result = std::get_if<TResult>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for result request: %s", GetErrorMessage().data());
        return *result;
    }

    TResult& MutableResult() {
        auto result = std::get_if<TResult>(&Result);
        Y_ABORT_UNLESS(result, "incorrect object for result request: %s", GetErrorMessage().data());
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

    operator TStatus() const {
        return GetError();
    }

    TString GetErrorMessage() const {
        auto* status = std::get_if<TStatus>(&Result);
        Y_ABORT_UNLESS(status, "incorrect object for extracting error message");
        return status->GetErrorMessage();
    }

    auto GetStatus() const {
        auto* status = std::get_if<TStatus>(&Result);
        if (!status) {
            return TStatus::Success().GetStatus();
        } else {
            return status->GetStatus();
        }
    }
};
}
