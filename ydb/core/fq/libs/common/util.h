#pragma once

#include <algorithm>
#include <array>

#include <google/protobuf/repeated_field.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <library/cpp/iterator/mapped.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/join.h>

namespace NFq {

enum class EYdbComputeAuth {
    UNKNOWN,
    NONE,
    SERVICE_ACCOUNT,
    BASIC,
    MDB_BASIC
};

template<std::size_t K, typename T, std::size_t N>
auto CreateArray(const T(&list)[N]) -> std::array<T, K> {
    static_assert(N == K, "not valid array size");
    std::array<T, K> result;
    std::copy(std::begin(list), std::end(list), std::begin(result));
    return result;
}

template <class TElement>
TVector<TElement> VectorFromProto(const ::google::protobuf::RepeatedPtrField<TElement>& field) {
    return { field.begin(), field.end() };
}

template <typename TIter, typename TFunc>
TString JoinMapRange(TString delim, const TIter beg, const TIter end, const TFunc func) {
    auto mappedBegin =
        MakeMappedIterator(beg, func);
    auto mappedEnd =
        MakeMappedIterator(end, func);
    return JoinRange(delim, mappedBegin, mappedEnd);
}

TString EscapeString(const TString& value,
                     const TString& enclosingSeq,
                     const TString& replaceWith);

TString EscapeString(const TString& value, char enclosingChar);

TString EncloseAndEscapeString(const TString& value, char enclosingChar);

TString EncloseAndEscapeString(const TString& value,
                               const TString& enclosingSeq,
                               const TString& replaceWith);

TString EncloseSecret(const TString& value);

TString HideSecrets(const TString& value);

TString ExtractServiceAccountId(const FederatedQuery::ConnectionSetting& setting);

TString ExtractServiceAccountId(const FederatedQuery::ConnectionContent& content);

TString ExtractServiceAccountId(const FederatedQuery::Connection& connection);

TMaybe<TString> GetLogin(const FederatedQuery::ConnectionSetting& setting);

TMaybe<TString> GetPassword(const FederatedQuery::ConnectionSetting& setting);

EYdbComputeAuth GetYdbComputeAuthMethod(const FederatedQuery::ConnectionSetting& setting);

FederatedQuery::IamAuth GetAuth(const FederatedQuery::Connection& connection);

TString RemoveDatabaseFromStr(TString str, const TString& substr);

NYql::TIssues RemoveDatabaseFromIssues(const NYql::TIssues& issues, const TString& databasePath);

template<typename TExecutable>
TMaybe<NYql::TIssues> GetIssuesFromYdbStatus(const TExecutable& executable, const NYdb::TAsyncStatus& future) {
    try {
        auto status = future.GetValue();
        if (status.IsSuccess()) {
            return {};
        }
        NYql::TIssues issues;
        issues.AddIssues(executable->Issues);
        issues.AddIssues(executable->InternalIssues);
        return issues;
    } catch (...) {
        return NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}};
    }
}

}  // namespace NFq
