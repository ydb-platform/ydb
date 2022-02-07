#pragma once

#include "types.h"
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/ymq/base/counters.h>

namespace NKikimr::NSQS {

TSqsHttpResponse MakeErrorXmlResponse(const TErrorClass& errorClass, TUserCounters* userCounters, const TString& message = TString(), const TString& requestId = TString());
TSqsHttpResponse MakeErrorXmlResponseFromCurrentException(TUserCounters* userCounters, const TString& requestId);

TSqsHttpResponse ResponseToAmazonXmlFormat(const NKikimrClient::TSqsResponse& resp);

} // namespace NKikimr::NSQS
