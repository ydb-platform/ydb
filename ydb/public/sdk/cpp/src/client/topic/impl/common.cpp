#include "common.h"

#include <util/charset/unidata.h>

namespace NYdb::inline Dev::NTopic {

ERetryErrorClass GetRetryErrorClass(EStatus status) {
    switch (status) {
    case EStatus::SUCCESS:          // NoRetry?
    case EStatus::INTERNAL_ERROR:   // NoRetry?
    case EStatus::ABORTED:
    case EStatus::UNAVAILABLE:
    case EStatus::GENERIC_ERROR:    // NoRetry?
    case EStatus::BAD_SESSION:      // NoRetry?
    case EStatus::SESSION_EXPIRED:
    case EStatus::CANCELLED:
    case EStatus::UNDETERMINED:
    case EStatus::SESSION_BUSY:
    case EStatus::CLIENT_INTERNAL_ERROR:
    case EStatus::CLIENT_CANCELLED:
    case EStatus::CLIENT_OUT_OF_RANGE:
        return ERetryErrorClass::ShortRetry;

    case EStatus::OVERLOADED:
    case EStatus::TIMEOUT:
    case EStatus::TRANSPORT_UNAVAILABLE:
    case EStatus::CLIENT_RESOURCE_EXHAUSTED:
    case EStatus::CLIENT_DEADLINE_EXCEEDED:
    case EStatus::CLIENT_LIMITS_REACHED:
    case EStatus::CLIENT_DISCOVERY_FAILED:
        return ERetryErrorClass::LongRetry;

    case EStatus::SCHEME_ERROR:
    case EStatus::STATUS_UNDEFINED:
    case EStatus::BAD_REQUEST:
    case EStatus::UNAUTHORIZED:
    case EStatus::PRECONDITION_FAILED:
    case EStatus::UNSUPPORTED:
    case EStatus::ALREADY_EXISTS:
    case EStatus::NOT_FOUND:
    case EStatus::EXTERNAL_ERROR:
    case EStatus::CLIENT_UNAUTHENTICATED:
    case EStatus::CLIENT_CALL_UNIMPLEMENTED:
        return ERetryErrorClass::NoRetry;
    }
}

ERetryErrorClass GetRetryErrorClassV2(EStatus status) {
    switch (status) {
        case EStatus::SCHEME_ERROR:
            return ERetryErrorClass::NoRetry;
        default:
            return GetRetryErrorClass(status);

    }
}

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
