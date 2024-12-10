#pragma once

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <optional>

namespace NKikimr {

template <class TStatus, TStatus StatusOk, TStatus DefaultError, class TError, class TDerived>
class TConclusionStatusGenericImpl {
protected:
    std::optional<TError> ErrorMessage;
    TStatus Status = StatusOk;

    TConclusionStatusGenericImpl() = default;

    TConclusionStatusGenericImpl(const TError& error, TStatus status = DefaultError)
        : ErrorMessage(error)
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

    TConclusionStatusGenericImpl(TError&& error, TStatus status = DefaultError)
        : ErrorMessage(std::move(error))
        , Status(status) {
        Y_ABORT_UNLESS(!!ErrorMessage);
    }

public:
    virtual ~TConclusionStatusGenericImpl() = default;

public:
    [[nodiscard]] const TError& GetErrorMessage() const {
        return ErrorMessage ? *ErrorMessage : Default<TError>();
    }

    [[nodiscard]] virtual TString GetErrorString() const {
        return ErrorMessage ? ToString(*ErrorMessage) : Default<TString>();
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
        return !ErrorMessage;
    }

    [[nodiscard]] bool operator!() const {
        return !!ErrorMessage;
    }

    [[nodiscard]] static TDerived Success() {
        return TDerived();
    }
};

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionStatusImpl : public TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, TString, TConclusionStatusImpl<TStatus, StatusOk, DefaultError>> {
protected:
    friend class TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, TString, TConclusionStatusImpl<TStatus, StatusOk, DefaultError>>;

    using TBase = TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, TString, TConclusionStatusImpl<TStatus, StatusOk, DefaultError>>;
    using TBase::TBase;

    TConclusionStatusImpl() = default;

    TConclusionStatusImpl(const char* errorMessage, TStatus status = DefaultError)
        : TBase(TString(errorMessage), status) {
    }

    TConclusionStatusImpl(const std::string& errorMessage, TStatus status = DefaultError)
        : TBase(TString(errorMessage), status) {
    }

public:
    void Validate(const TString& processInfo = Default<TString>()) const {
        if (processInfo) {
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s, processInfo=%s", TBase::GetErrorMessage().c_str(), processInfo.c_str());
        } else {
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s", TBase::GetErrorMessage().c_str());
        }
    }
};

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TYQLConclusionStatusImpl : public TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, NYql::TIssues, TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>> {
protected:
    friend class TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, NYql::TIssues, TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>>;

    using TBase = TConclusionStatusGenericImpl<TStatus, StatusOk, DefaultError, NYql::TIssues, TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>>;
    using TBase::TBase;

    TYQLConclusionStatusImpl() = default;

    TYQLConclusionStatusImpl(const TString& errorMessage, TStatus status = DefaultError)
        : TBase({NYql::TIssue(errorMessage)}, status) {
    }

public:
    TYQLConclusionStatusImpl& AddParentIssue(NYql::TIssue issue) {
        Y_ABORT_UNLESS(!!TBase::ErrorMessage);
        for (const auto& childIssue : *TBase::ErrorMessage) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(childIssue));
        }
        TBase::ErrorMessage = {std::move(issue)};
        return *this;
    }

    TYQLConclusionStatusImpl& AddParentIssue(const TString& message) {
        AddParentIssue(NYql::TIssue(message));
        return *this;
    }

    TYQLConclusionStatusImpl& AddIssue(NYql::TIssue issue) {
        Y_ABORT_UNLESS(!!TBase::ErrorMessage);
        TBase::ErrorMessage->AddIssue(std::move(issue));
        return *this;
    }

    TYQLConclusionStatusImpl& AddIssue(const TString& message) {
        AddIssue(NYql::TIssue(message));
        return *this;
    }

    [[nodiscard]] virtual TString GetErrorString() const override {
        return TBase::GetErrorMessage().ToOneLineString();
    }
};

}   // namespace NKikimr
