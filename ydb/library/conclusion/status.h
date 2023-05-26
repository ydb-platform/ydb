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

}
