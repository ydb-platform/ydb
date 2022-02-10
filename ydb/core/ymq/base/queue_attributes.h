#pragma once

#include "dlq_helpers.h"
#include <ydb/library/http_proxy/error/error.h>

#include <ydb/core/protos/config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NSQS {

struct TQueueAttributes {
    static TQueueAttributes FromAttributesAndConfig(const THashMap<TString, TString>& attributes, const NKikimrConfig::TSqsConfig& config, bool isFifoQueue, bool clamp);
    bool Validate() const;
    bool HasClampedAttributes() const;
public:
    bool Clamped = false;
    const TErrorClass* Error = nullptr;
    TString ErrorText;
    TMaybe<ui64> VisibilityTimeout; // seconds
    TMaybe<ui64> MessageRetentionPeriod; // seconds
    TMaybe<ui64> ReceiveMessageWaitTimeSeconds; // seconds
    TMaybe<ui64> DelaySeconds; // seconds
    TMaybe<ui64> MaximumMessageSize; // bytes
    TMaybe<bool> ContentBasedDeduplication; // bool
    TRedrivePolicy RedrivePolicy;

private:
    bool TryParseLimitedValue(const TString& attrName, const TString& valueStr, const ui64 allowedMinValue, const ui64 allowedMaxValue, TMaybe<ui64>& result, bool clamp);
};

} // namespace NKikimr::NSQS
