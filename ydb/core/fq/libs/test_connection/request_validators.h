#pragma once

#include <ydb/core/fq/libs/control_plane_storage/request_validators.h>

namespace NFq {

template<typename T>
NYql::TIssues ValidateTestConnection(T& ev, size_t maxSize, const TSet<FederatedQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool passwordRequired = true)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (!request.has_setting()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting field is not specified"));
    }

    const FederatedQuery::ConnectionSetting& setting = request.setting();
    issues.AddIssues(ValidateConnectionSetting(setting, availableConnections, disableCurrentIam, passwordRequired));
    return issues;
}

} // namespace NFq
