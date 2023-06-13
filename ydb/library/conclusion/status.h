#pragma once
#include <util/generic/string.h>
#include <optional>

namespace NKikimr {

class TConclusionStatus {
private:
    std::optional<TString> ErrorMessage;
    TConclusionStatus() = default;
    TConclusionStatus(const TString& errorMessage)
        : ErrorMessage(errorMessage) {
        Y_VERIFY(!!ErrorMessage);
    }

    TConclusionStatus(const char* errorMessage)
        : ErrorMessage(errorMessage) {
        Y_VERIFY(!!ErrorMessage);
    }
public:

    const TString& GetErrorMessage() const {
        return ErrorMessage ? *ErrorMessage : Default<TString>();
    }

    static TConclusionStatus Fail(const char* errorMessage) {
        return TConclusionStatus(errorMessage);
    }

    static TConclusionStatus Fail(const TString& errorMessage) {
        return TConclusionStatus(errorMessage);
    }

    bool IsFail() const {
        return !Ok();
    }

    bool Ok() const {
        return !ErrorMessage;
    }

    bool operator!() const {
        return !!ErrorMessage;
    }

    explicit operator bool() const {
        return Ok();
    }

    static TConclusionStatus Success() {
        return TConclusionStatus();
    }
};

template<class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionSpecialStatus {
private:
    std::optional<TString> ErrorMessage;
    TStatus SpecialStatus = StatusOk;

    TConclusionSpecialStatus() = default;
    TConclusionSpecialStatus(const TStatus& status, const std::optional<TString>& errorMessage = {})
        : ErrorMessage(errorMessage)
        , SpecialStatus(status)
    {
        Y_VERIFY(!!ErrorMessage);
    }

    TConclusionSpecialStatus(const TStatus& status,const char* errorMessage)
        : ErrorMessage(errorMessage)
        , SpecialStatus(status)
    {
        Y_VERIFY(!!ErrorMessage);
    }
public:

    const TString& GetErrorMessage() const {
        return ErrorMessage ? *ErrorMessage : Default<TString>();
    }

    static TConclusionSpecialStatus Fail(const char* errorMessage) {
        return Fail(DefaultError, errorMessage);
    }

    static TConclusionSpecialStatus Fail(const TString& errorMessage) {
        return Fail(DefaultError, errorMessage);
    }

    static TConclusionSpecialStatus Fail(const TStatus& status, const char* errorMessage) {
        Y_VERIFY(status != StatusOk);
        return TConclusionSpecialStatus(status, errorMessage);
    }

    static TConclusionSpecialStatus Fail(const TStatus& status, const TString& errorMessage) {
        Y_VERIFY(status != StatusOk);
        return TConclusionSpecialStatus(status, errorMessage);
    }

    const TStatus& GetStatus() const {
        return SpecialStatus;
    }

    bool IsFail() const {
        return !Ok();
    }

    bool Ok() const {
        return SpecialStatus == StatusOk;
    }

    bool operator!() const {
        return !Ok();
    }

    explicit operator bool() const {
        return Ok();
    }

    static TConclusionSpecialStatus Success() {
        return TConclusionSpecialStatus();
    }
};

}
