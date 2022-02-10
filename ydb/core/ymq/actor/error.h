#pragma once
#include "defs.h"
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/core/ymq/base/counters.h>

namespace NKikimr::NSQS {

/// The function creates an error message for a user.
/// There must not be implementation details in this message!
/// Examples of implementations details are:
/// - internal database query errors;
/// - paths to sources
/// - exception messages (if they contain source paths like those ones from ythrow macro)
void MakeError(NSQS::TError* error, const TErrorClass& errorClass, const TString& message = TString());

template <class TProtoMessage>
void MakeError(TProtoMessage& proto, const TErrorClass& errorClass, const TString& message = TString()) {
    MakeError(proto.MutableError(), errorClass, message);
}

template <class TProtoMessage>
void MakeError(TProtoMessage* proto, const TErrorClass& errorClass, const TString& message = TString()) {
    MakeError(proto->MutableError(), errorClass, message);
}

size_t ErrorsCount(const NKikimrClient::TSqsResponse& response, TAPIStatusesCounters* counters);

} // namespace NKikimr::NSQS
