#pragma once

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/string/cast.h>
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

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionStatusImpl : public TConclusionStatusGenericImpl<TConclusionStatusImpl<TStatus, StatusOk, DefaultError>, TString, TStatus, StatusOk, DefaultError> {
protected:
    using TSelf = TConclusionStatusImpl<TStatus, StatusOk, DefaultError>;
    using TBase = TConclusionStatusGenericImpl<TSelf, TString, TStatus, StatusOk, DefaultError>;
    using TBase::TBase;

    friend class TConclusionStatusGenericImpl<TSelf, TString, TStatus, StatusOk, DefaultError>;

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
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s, processInfo=%s", GetErrorMessage().c_str(), processInfo.c_str());
        } else {
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s", GetErrorMessage().c_str());
        }
    }

    [[nodiscard]] TString GetErrorMessage() const {
        return TBase::GetErrorDescription();
    }
};

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TYQLConclusionStatusImpl : public TConclusionStatusGenericImpl<TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>, NYql::TIssues, TStatus, StatusOk, DefaultError> {
protected:
    using TSelf = TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>;
    using TBase = TConclusionStatusGenericImpl<TSelf, NYql::TIssues, TStatus, StatusOk, DefaultError>;
    using TBase::TBase;

    friend class TConclusionStatusGenericImpl<TSelf, NYql::TIssues, TStatus, StatusOk, DefaultError>;

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

    [[nodiscard]] TString GetErrorMessage() const {
        return TBase::GetErrorDescription().ToOneLineString();
    }
};

}   // namespace NKikimr
