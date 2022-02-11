#pragma once

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <library/cpp/threading/future/future.h>

namespace NYql {
namespace NCommon {

class TOperationResult {
public:
    [[nodiscard]]
    bool Success() const { return Status_ == TIssuesIds::SUCCESS; };
    const EYqlIssueCode& Status() const { return Status_; }
    const TIssues& Issues() const { return Issues_; }

    void SetSuccess() { Status_ = TIssuesIds::SUCCESS; }
    void SetStatus(EYqlIssueCode status) { Status_ = status; }

    void AddIssue(const TIssue& issue);
    void AddIssues(const TIssues& issues);
    void SetException(const std::exception& e, const TPosition& pos = {});

    void ReportIssues(TIssueManager& issueManager) const;
private:
    EYqlIssueCode Status_ = TIssuesIds::DEFAULT_ERROR;
    TIssues Issues_;
};

template<typename TResult>
TResult ResultFromException(const std::exception& e, const TPosition& pos = {}) {
    TResult result;
    result.SetException(e, pos);
    return result;
}

template<typename TResult>
TResult ResultFromIssues(EYqlIssueCode status, const TString& message, const TIssues& issues) {
    TIssue statusIssue = message.empty()
        ? YqlIssue(TPosition(), status)
        : YqlIssue(TPosition(), status, message);

    for (auto& issue : issues) {
        statusIssue.AddSubIssue(MakeIntrusive<TIssue>(issue));
    }

    TResult result;
    result.SetStatus(status);
    result.AddIssue(statusIssue);

    return result;
}

template<typename TResult>
TResult ResultFromIssues(EYqlIssueCode status, const TIssues& issues) {
    return ResultFromIssues<TResult>(status, "", issues);
}

template<typename TResult>
TResult ResultFromError(const TIssue& error) {
    TResult result;
    result.AddIssue(error);

    return result;
}

template<typename TResult>
TResult ResultFromError(const TIssues& error) {
    TResult result;
    result.AddIssues(error);

    return result;
}

template<typename TResult>
TResult ResultFromError(const TString& error, TPosition pos = TPosition()) {
    return ResultFromError<TResult>(TIssue(pos, error));
}

template<typename TResult>
TResult ResultFromErrors(const TIssues& errors) {
    TResult result;
    result.AddIssues(errors);
    return result;
}

template <typename T>
inline void SetPromiseValue(NThreading::TPromise<T>& promise, const NThreading::TFuture<T>& future)
{
    future.Subscribe([=] (const NThreading::TFuture<T>& f) mutable {
        try {
            promise.SetValue(f.GetValue());
        } catch (...) {
            promise.SetException(std::current_exception());
        }
    });
}

} // NCommon
} // NYql
