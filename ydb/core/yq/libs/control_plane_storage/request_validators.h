#pragma once

#include "util.h"

#include <ydb/core/yq/libs/config/yq_issue.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/yq.pb.h>

#include <util/generic/fwd.h>
#include <util/generic/set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYq {

template<class P>
NYql::TIssues ValidateEvent(P& ev, size_t maxSize)
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
NYql::TIssues ValidateQuery(T& ev, size_t maxSize)
{
    NYql::TIssues issues = ValidateEvent(ev, maxSize);
    auto& request = ev->Get()->Request;
    const auto& content = request.content();

    if (request.execute_mode() == YandexQuery::ExecuteMode::EXECUTE_MODE_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "execute_mode field is not specified"));
    }

    if (content.type() == YandexQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "type field is not specified"));
    }

    if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "acl.visibility field is not specified"));
    }

    if (content.type() == YandexQuery::QueryContent::STREAMING && !request.has_disposition()) {
        request.mutable_disposition()->mutable_fresh();
    }

    return issues;
}

NYql::TIssues ValidateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting);

template<typename T>
NYql::TIssues ValidateBinding(T& ev, size_t maxSize, const TSet<YandexQuery::BindingSetting::BindingCase>& availableBindings)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (request.has_content()) {
        const YandexQuery::BindingContent& content = request.content();
        if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.acl.visibility field is not specified"));
        }

        if (content.name() != to_lower(content.name())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Incorrect binding name: " << content.name() << ". Please use only lower case"));
        }

        if (!content.has_setting()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.setting field is not specified"));
        }

        const YandexQuery::BindingSetting& setting = content.setting();
        if (!availableBindings.contains(setting.binding_case())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding of the specified type is disabled"));
        }

        switch (setting.binding_case()) {
        case YandexQuery::BindingSetting::kDataStreams: {
            const YandexQuery::DataStreamsBinding dataStreams = setting.data_streams();
            if (!dataStreams.has_schema()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "data streams with empty schema is forbidden"));
            }
            issues.AddIssues(ValidateFormatSetting(dataStreams.format_setting()));
            break;
        }
        case YandexQuery::BindingSetting::BINDING_NOT_SET: {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding is not set"));
            break;
        }
            // Do not replace with default. Adding a new binding should cause a compilation error
        case YandexQuery::BindingSetting::kObjectStorage:
            const YandexQuery::ObjectStorageBinding objectStorage = setting.object_storage();
            for (const auto& subset: objectStorage.subset()) {
                issues.AddIssues(ValidateFormatSetting(subset.format_setting()));
            }
            break;
        }
    } else {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content field is not specified"));
    }

    return issues;
}

NYql::TIssues ValidateConnectionSetting(const YandexQuery::ConnectionSetting& setting, const TSet<YandexQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool clickHousePasswordRequire = true);

template<typename T>
NYql::TIssues ValidateConnection(T& ev, size_t maxSize, const TSet<YandexQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool clickHousePasswordRequire = true)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (!request.has_content()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content field is not specified"));
    }

    const YandexQuery::ConnectionContent& content = request.content();
    if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.acl.visibility field is not specified"));
    }

    if (content.name() != to_lower(content.name())) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Incorrect connection name: " << content.name() << ". Please use only lower case"));
    }

    if (!content.has_setting()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting field is not specified"));
    }

    const YandexQuery::ConnectionSetting& setting = content.setting();
    issues.AddIssues(ValidateConnectionSetting(setting, availableConnections, disableCurrentIam, clickHousePasswordRequire));
    return issues;
}

} // namespace NYq
