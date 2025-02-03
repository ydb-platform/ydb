#pragma once

#include <util/generic/singleton.h>
#include <util/system/yassert.h>

#include <optional>

namespace NKikimr {

template <class TDerived, class TErrorDescription, class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionStatusGenericImpl {
protected:
    std::optional<TErrorDescription> ErrorDescription;
    TStatus Status = StatusOk;

    TConclusionStatusGenericImpl() = default;

    TConclusionStatusGenericImpl(const TErrorDescription& error, TStatus status = DefaultError)
        : ErrorDescription(error)
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorDescription);
    }

    TConclusionStatusGenericImpl(TErrorDescription&& error, TStatus status = DefaultError)
        : ErrorDescription(std::move(error))
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorDescription);
    }

public:
    virtual ~TConclusionStatusGenericImpl() = default;

public:
    [[nodiscard]] const TErrorDescription& GetErrorDescription() const {
        return ErrorDescription ? *ErrorDescription : Default<TErrorDescription>();
    }

    [[nodiscard]] TStatus GetStatus() const {
        return Status;
    }

    template <class TErrorMessage>
    [[nodiscard]] static TDerived Fail(const TErrorMessage& errorMessage) {
        return TDerived(errorMessage);
    }

    template <class TErrorMessage>
    [[nodiscard]] static TDerived Fail(const TStatus& status, const TErrorMessage& errorMessage) {
        Y_ABORT_UNLESS(DefaultError == StatusOk || status != StatusOk);
        return TDerived(errorMessage, status);
    }

    [[nodiscard]] bool IsFail() const {
        return !Ok();
    }

    [[nodiscard]] bool IsSuccess() const {
        return Ok();
    }

    [[nodiscard]] bool Ok() const {
        return !ErrorDescription;
    }

    [[nodiscard]] bool operator!() const {
        return !!ErrorDescription;
    }

    [[nodiscard]] static TDerived Success() {
        return TDerived();
    }
};

}   // namespace NKikimr
