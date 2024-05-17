#pragma once
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/string.h>
#include <optional>

namespace NKikimr {

class TConclusionStatus {
private:
    std::optional<TString> ErrorMessage;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TConclusionStatus() = default;
    TConclusionStatus(const TString& errorMessage, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::INTERNAL_ERROR)
        : ErrorMessage(errorMessage)
        , Status(status)
    {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionStatus(const char* errorMessage, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::INTERNAL_ERROR)
        : ErrorMessage(errorMessage)
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionStatus(const std::string& errorMessage, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::INTERNAL_ERROR)
        : ErrorMessage(TString(errorMessage.data(), errorMessage.size()))
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }
public:
    void Validate(const TString& processInfo = Default<TString>()) const;

    [[nodiscard]] const TString& GetErrorMessage() const {
        return ErrorMessage ? *ErrorMessage : Default<TString>();
    }

    [[nodiscard]] Ydb::StatusIds::StatusCode GetStatus() const {
        return Status;
    }

    [[nodiscard]] static TConclusionStatus Fail(const char* errorMessage) {
        return TConclusionStatus(errorMessage);
    }

    [[nodiscard]] static TConclusionStatus Fail(const TString& errorMessage) {
        return TConclusionStatus(errorMessage);
    }

    [[nodiscard]] static TConclusionStatus Fail(const std::string& errorMessage) {
        return TConclusionStatus(errorMessage);
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

    [[nodiscard]] static TConclusionStatus Success() {
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
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionSpecialStatus(const TStatus& status,const char* errorMessage)
        : ErrorMessage(errorMessage)
        , SpecialStatus(status)
    {
        Y_ABORT_UNLESS(!!ErrorMessage);
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
        Y_ABORT_UNLESS(status != StatusOk);
        return TConclusionSpecialStatus(status, errorMessage);
    }

    static TConclusionSpecialStatus Fail(const TStatus& status, const TString& errorMessage) {
        Y_ABORT_UNLESS(status != StatusOk);
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
