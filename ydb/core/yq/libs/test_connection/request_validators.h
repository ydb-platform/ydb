#pragma once

#include <ydb/core/yq/libs/control_plane_storage/request_validators.h>

namespace NYq {

template<typename T>
NYql::TIssues ValidateTestConnection(T& ev, size_t maxSize, const TSet<FederatedQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool clickHousePasswordRequire = true)
{
    const auto& request = ev->Get()->Request;
    NYql::TIssues issues = ValidateEvent(ev, maxSize);

    if (!request.has_setting()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting field is not specified"));
    }

    const FederatedQuery::ConnectionSetting& setting = request.setting();
    issues.AddIssues(ValidateConnectionSetting(setting, availableConnections, disableCurrentIam, clickHousePasswordRequire));
    return issues;
}

} // namespace NYq
