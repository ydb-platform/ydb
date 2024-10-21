#pragma once

#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <optional>

namespace NKikimr {

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionStatusImpl {
private:
    std::optional<TString> ErrorMessage;
    TStatus Status = StatusOk;
    TConclusionStatusImpl() = default;
    TConclusionStatusImpl(const TString& errorMessage, TStatus status = DefaultError)
        : ErrorMessage(errorMessage)
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionStatusImpl(const char* errorMessage, TStatus status = DefaultError)
        : ErrorMessage(errorMessage)
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionStatusImpl(const std::string& errorMessage, TStatus status = DefaultError)
        : ErrorMessage(TString(errorMessage.data(), errorMessage.size()))
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

public:
    void Validate(const TString& processInfo = Default<TString>()) const {
        if (processInfo) {
            Y_ABORT_UNLESS(Ok(), "error=%s, processInfo=%s", GetErrorMessage().c_str(), processInfo.c_str());
        } else {
            Y_ABORT_UNLESS(Ok(), "error=%s", GetErrorMessage().c_str());
        }
    }

    [[nodiscard]] const TString& GetErrorMessage() const {
        return ErrorMessage ? *ErrorMessage : Default<TString>();
    }

    [[nodiscard]] TStatus GetStatus() const {
        return Status;
    }

    [[nodiscard]] static TConclusionStatusImpl Fail(const char* errorMessage) {
        return TConclusionStatusImpl(errorMessage);
    }

    [[nodiscard]] static TConclusionStatusImpl Fail(const TString& errorMessage) {
        return TConclusionStatusImpl(errorMessage);
    }

    [[nodiscard]] static TConclusionStatusImpl Fail(const std::string& errorMessage) {
        return TConclusionStatusImpl(errorMessage);
    }

    [[nodiscard]] static TConclusionStatusImpl Fail(const TStatus& status, const char* errorMessage) {
        Y_ABORT_UNLESS(DefaultError == StatusOk || status != StatusOk);
        return TConclusionStatusImpl(errorMessage, status);
    }

    [[nodiscard]] static TConclusionStatusImpl Fail(const TStatus& status, const TString& errorMessage) {
        Y_ABORT_UNLESS(DefaultError == StatusOk || status != StatusOk);
        return TConclusionStatusImpl(errorMessage, status);
    }

    [[nodiscard]] bool IsFail() const {
        return !Ok();
    }

    [[nodiscard]] bool IsSuccess() const {
        return Ok();
    }

    [[nodiscard]] bool Ok() const {
        return !ErrorMessage;
    }

    [[nodiscard]] bool operator!() const {
        return !!ErrorMessage;
    }

    [[nodiscard]] static TConclusionStatusImpl Success() {
        return TConclusionStatusImpl();
    }
};

}   // namespace NKikimr
