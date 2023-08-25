#pragma once

#include "util.h"

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

#include <util/generic/fwd.h>
#include <util/generic/set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <library/cpp/scheme/scheme.h>

namespace NFq {

template<class P>
NYql::TIssues ValidateEvent(const P& ev, size_t maxSize)
{
    const auto& request = ev->Get()->Request;
    const TString& scope = ev->Get()->Scope;
    const TString& user = ev->Get()->User;
    const TString& token = ev->Get()->Token;
    const size_t byteSize = request.ByteSizeLong();

    NYql::TIssues issues;
    if (!scope) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "scope is not specified"));
    }

    if (!user) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "user is empty"));
    }

    if (!token) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "token is empty"));
    }

    if (byteSize > maxSize) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "request size exceeded " << maxSize << " bytes. Request size: " << byteSize));
    }

    TString error;
    if (!request.validate(error)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, error));
    }

    return issues;
}

template<typename T>
NYql::TIssues ValidateQuery(const T& ev, size_t maxSize)
{
    NYql::TIssues issues = ValidateEvent(ev, maxSize);
    auto& request = ev->Get()->Request;
    const auto& content = request.content();

    if (request.execute_mode() == FederatedQuery::ExecuteMode::EXECUTE_MODE_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "execute_mode field is not specified"));
    }

    if (content.type() == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "type field is not specified"));
    }

    if (content.acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "acl.visibility field is not specified"));
    }

    if (content.type() == FederatedQuery::QueryContent::STREAMING && !request.has_disposition()) {
        request.mutable_disposition()->mutable_fresh();
    }

    return issues;
}

NYql::TIssues ValidateFormatSetting(const TString& format, const google::protobuf::Map<TString, TString>& formatSetting);


NYql::TIssues ValidateDateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting, bool matchAllSettings = false);
NYql::TIssues ValidateProjectionColumns(const FederatedQuery::Schema& schema, const TVector<TString>& partitionedBy);
NYql::TIssues ValidateProjection(const FederatedQuery::Schema& schema, const TString& projection, const TVector<TString>& partitionedBy, size_t pathsLimit);

template<typename T>
NYql::TIssues ValidateBinding(const T& ev, size_t maxSize, const TSet<FederatedQuery::BindingSetting::BindingCase>& availableBindings, size_t pathsLimit)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (request.has_content()) {
        const FederatedQuery::BindingContent& content = request.content();
        if (content.acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.acl.visibility field is not specified"));
        }

        if (content.name() != to_lower(content.name())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Incorrect binding name: " << content.name() << ". Please use only lower case"));
        }

        if (!content.has_setting()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.setting field is not specified"));
        }

        const FederatedQuery::BindingSetting& setting = content.setting();
        if (!availableBindings.contains(setting.binding_case())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding of the specified type is disabled"));
        }

        switch (setting.binding_case()) {
        case FederatedQuery::BindingSetting::kDataStreams: {
            const FederatedQuery::DataStreamsBinding dataStreams = setting.data_streams();
            if (!dataStreams.has_schema()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "data streams with empty schema is forbidden"));
            }
            issues.AddIssues(ValidateDateFormatSetting(dataStreams.format_setting(), true));
            break;
        }
        case FederatedQuery::BindingSetting::BINDING_NOT_SET: {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding is not set"));
            break;
        }
            // Do not replace with default. Adding a new binding should cause a compilation error
        case FederatedQuery::BindingSetting::kObjectStorage:
            const FederatedQuery::ObjectStorageBinding objectStorage = setting.object_storage();
            for (const auto& subset: objectStorage.subset()) {
                issues.AddIssues(ValidateFormatSetting(subset.format(), subset.format_setting()));
                if (subset.projection_size() || subset.partitioned_by_size()) {
                    try {
                        TVector<TString> partitionedBy{subset.partitioned_by().begin(), subset.partitioned_by().end()};
                        issues.AddIssues(ValidateProjectionColumns(subset.schema(), partitionedBy));
                        TString projectionStr;
                        if (subset.projection_size()) {
                            NSc::TValue projection;
                            for (const auto& [key, value]: subset.projection()) {
                                projection[key] = value;
                            }
                            projectionStr = projection.ToJsonPretty();
                        }
                        issues.AddIssues(ValidateProjection(subset.schema(), projectionStr, partitionedBy, pathsLimit));
                    } catch (...) {
                        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST,CurrentExceptionMessage()));
                    }
                }
            }
            break;
        }
    } else {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content field is not specified"));
    }

    return issues;
}

NYql::TIssues ValidateConnectionSetting(
    const FederatedQuery::ConnectionSetting& setting,
    const TSet<FederatedQuery::ConnectionSetting::ConnectionCase>& availableConnections,
    bool disableCurrentIam,
    bool passwordRequired = true);

template<typename T>
NYql::TIssues ValidateConnection(
    const T& ev,
    size_t maxSize,
    const TSet<FederatedQuery::ConnectionSetting::ConnectionCase>& availableConnections,
    bool disableCurrentIam,
    bool passwordRequired = true)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (!request.has_content()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content field is not specified"));
    }

    const FederatedQuery::ConnectionContent& content = request.content();
    if (content.acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.acl.visibility field is not specified"));
    }

    if (content.name() != to_lower(content.name())) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Incorrect connection name: " << content.name() << ". Please use only lower case"));
    }

    if (!content.has_setting()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting field is not specified"));
    }

    const FederatedQuery::ConnectionSetting& setting = content.setting();
    issues.AddIssues(ValidateConnectionSetting(setting, availableConnections, disableCurrentIam, passwordRequired));
    return issues;
}

} // namespace NFq
