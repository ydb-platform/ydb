#include "common.h"

#include <util/charset/unidata.h>

namespace NYdb::NTopic {

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

TString IssuesSingleLineString(const NYql::TIssues& issues) {
    return SubstGlobalCopy(issues.ToString(), '\n', ' ');
}

void Cancel(NYdbGrpc::IQueueClientContextPtr& context) {
    if (context) {
        context->Cancel();
    }
}

NYql::TIssues MakeIssueWithSubIssues(const TString& description, const NYql::TIssues& subissues) {
    NYql::TIssues issues;
    NYql::TIssue issue(description);
    for (const NYql::TIssue& i : subissues) {
        issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
    }
    issues.AddIssue(std::move(issue));
    return issues;
}

static TStringBuf SplitPort(TStringBuf endpoint) {
    for (int i = endpoint.Size() - 1; i >= 0; --i) {
        if (endpoint[i] == ':') {
            return endpoint.SubString(i + 1, TStringBuf::npos);
        }
        if (!IsDigit(endpoint[i])) {
            return TStringBuf(); // empty
        }
    }
    return TStringBuf(); // empty
}

TString ApplyClusterEndpoint(TStringBuf driverEndpoint, const TString& clusterDiscoveryEndpoint) {
    const TStringBuf clusterDiscoveryPort = SplitPort(clusterDiscoveryEndpoint);
    if (!clusterDiscoveryPort.Empty()) {
        return clusterDiscoveryEndpoint;
    }

    const TStringBuf driverPort = SplitPort(driverEndpoint);
    if (driverPort.Empty()) {
        return clusterDiscoveryEndpoint;
    }

    const bool hasColon = clusterDiscoveryEndpoint.find(':') != TString::npos;
    if (hasColon) {
        return TStringBuilder() << '[' << clusterDiscoveryEndpoint << "]:" << driverPort;
    } else {
        return TStringBuilder() << clusterDiscoveryEndpoint << ':' << driverPort;
    }
}

}  // namespace NYdb::NTopic
