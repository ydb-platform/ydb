#pragma once

#include "generic_status.h"

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr {

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
        Y_ABORT_UNLESS(!!TBase::ErrorDescription);
        for (const auto& childIssue : *TBase::ErrorDescription) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(childIssue));
        }
        TBase::ErrorDescription = {std::move(issue)};
        return *this;
    }

    TYQLConclusionStatusImpl& AddParentIssue(const TString& message) {
        AddParentIssue(NYql::TIssue(message));
        return *this;
    }

    TYQLConclusionStatusImpl& AddIssue(NYql::TIssue issue) {
        Y_ABORT_UNLESS(!!TBase::ErrorDescription);
        TBase::ErrorDescription->AddIssue(std::move(issue));
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
