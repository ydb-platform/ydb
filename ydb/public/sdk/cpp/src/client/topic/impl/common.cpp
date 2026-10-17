#include "common.h"

#include <util/charset/unidata.h>

namespace NYdb::inline Dev::NTopic {

std::string IssuesSingleLineString(const NYdb::NIssue::TIssues& issues) {
    return SubstGlobalCopy(issues.ToString(), '\n', ' ');
}

void Cancel(NYdbGrpc::IQueueClientContextPtr& context) {
    if (context) {
        context->Cancel();
    }
}

NYdb::NIssue::TIssues MakeIssueWithSubIssues(const std::string& description, const NYdb::NIssue::TIssues& subissues) {
    NYdb::NIssue::TIssues issues;
    NYdb::NIssue::TIssue issue(description);
    for (const auto& i : subissues) {
        issue.AddSubIssue(MakeIntrusive<NYdb::NIssue::TIssue>(i));
    }
    issues.AddIssue(std::move(issue));
    return issues;
}

static std::string_view SplitPort(std::string_view endpoint) {
    for (int i = endpoint.size() - 1; i >= 0; --i) {
        if (endpoint[i] == ':') {
            return endpoint.substr(i + 1, std::string_view::npos);
        }
        if (!IsDigit(endpoint[i])) {
            return std::string_view(); // empty
        }
    }
    return std::string_view(); // empty
}

std::string ApplyClusterEndpoint(std::string_view driverEndpoint, const std::string& clusterDiscoveryEndpoint) {
    const std::string_view clusterDiscoveryPort = SplitPort(clusterDiscoveryEndpoint);
    if (!clusterDiscoveryPort.empty()) {
        return clusterDiscoveryEndpoint;
    }

    const std::string_view driverPort = SplitPort(driverEndpoint);
    if (driverPort.empty()) {
        return clusterDiscoveryEndpoint;
    }

    const bool hasColon = clusterDiscoveryEndpoint.find(':') != std::string::npos;
    if (hasColon) {
        return TStringBuilder() << '[' << clusterDiscoveryEndpoint << "]:" << driverPort;
    } else {
        return TStringBuilder() << clusterDiscoveryEndpoint << ':' << driverPort;
    }
}

} // namespace NYdb::NTopic
