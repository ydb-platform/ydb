#include "limits.h"
#include "queue_attributes.h"

#include <util/generic/utility.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NKikimr::NSQS {

TQueueAttributes TQueueAttributes::FromAttributesAndConfig(const THashMap<TString, TString>& attributes, const NKikimrConfig::TSqsConfig& config, bool isFifoQueue, bool clamp) {
    TQueueAttributes result;

#define INVALID_PARAM_MSG "Invalid value for the parameter %s."

#define TRY_SET_ATTRIBUTE(NAME, MIN_VAL, MAX_VAL)           \
    if (name == Y_STRINGIZE(NAME)) {                        \
        if (!result.TryParseLimitedValue(Y_STRINGIZE(NAME), \
            value,                                          \
            MIN_VAL,                                        \
            MAX_VAL,                                        \
            result.NAME,                                    \
            clamp))                                         \
        {                                                   \
            break;                                          \
        }                                                   \
    }

    for (const auto& [name, value] : attributes) {
        TRY_SET_ATTRIBUTE(VisibilityTimeout,
                          0,
                          TLimits::MaxVisibilityTimeout.Seconds())
        else TRY_SET_ATTRIBUTE(MessageRetentionPeriod,
                               TDuration::MilliSeconds(config.GetMinMessageRetentionPeriodMs()).Seconds(),
                               TLimits::MaxMessageRetentionPeriod.Seconds())
        else TRY_SET_ATTRIBUTE(ReceiveMessageWaitTimeSeconds,
                               0,
                               TDuration::MilliSeconds(config.GetMaxWaitTimeoutMs()).Seconds())
        else TRY_SET_ATTRIBUTE(DelaySeconds,
                               0,
                               TLimits::MaxDelaySeconds)
        else TRY_SET_ATTRIBUTE(MaximumMessageSize,
                               1024,
                               TLimits::MaxMessageSize)
        else if (name == "ContentBasedDeduplication" && isFifoQueue) {
            if (value == "true") {
                result.ContentBasedDeduplication = true;
            } else if (value == "false") {
                result.ContentBasedDeduplication = false;
            } else {
                result.Error = &NErrors::INVALID_ATTRIBUTE_VALUE;
                result.ErrorText = Sprintf(INVALID_PARAM_MSG " Valid values: true, false.", name.c_str());
                break;
            }
        } else if (name == "FifoQueue" && isFifoQueue) {
            if (value == "true") {
                continue; // just ignore the value
            } else {
                result.Error = &NErrors::INVALID_ATTRIBUTE_VALUE;
                if (value == "false") {
                    result.ErrorText = Sprintf(INVALID_PARAM_MSG " Reason: Modifying queue type is not supported.", name.c_str());
                } else {
                    result.ErrorText = Sprintf(INVALID_PARAM_MSG " Valid values: true, false.", name.c_str());
                }
                break;
            }
        } else if (config.GetEnableDeadLetterQueues() && name == "RedrivePolicy") {
            result.RedrivePolicy = TRedrivePolicy::FromJson(value, config);
            if (result.RedrivePolicy.IsValid()) {
                if (*result.RedrivePolicy.TargetQueueName && isFifoQueue != AsciiHasSuffixIgnoreCase(*result.RedrivePolicy.TargetQueueName, ".fifo")) {
                    result.ErrorText = "Target dead letter queue should have the same type as source queue.";
                } else {
                    continue;
                }
            } else {
                result.ErrorText = result.RedrivePolicy.GetErrorText();
            }
            result.Error = &NErrors::INVALID_PARAMETER_VALUE;
            break;
        } else {
            result.Error = &NErrors::INVALID_ATTRIBUTE_NAME;
            result.ErrorText = Sprintf("Unknown Attribute %s.", name.c_str());
            break;
        }
    }

    return result;
}

bool TQueueAttributes::Validate() const {
    return !Error;
}

bool TQueueAttributes::HasClampedAttributes() const {
    return Clamped;
}

bool TQueueAttributes::TryParseLimitedValue(const TString& attrName, const TString& valueStr, const ui64 allowedMinValue, const ui64 allowedMaxValue, TMaybe<ui64>& result, bool clamp) {
    ui64 value;
    if (TryFromString<ui64>(valueStr, value)) {
        const bool valid = (value >= allowedMinValue && value <= allowedMaxValue);
        if (valid || clamp) {
            result = valid ? value : ClampVal(value, allowedMinValue, allowedMaxValue);
            Clamped |= !valid;
            return true;
        }
    }

    result = Nothing();
    Error = &NErrors::INVALID_ATTRIBUTE_VALUE;
    ErrorText = Sprintf(INVALID_PARAM_MSG " Valid values are from %" PRIu64 " to %" PRIu64 " both inclusive.", attrName.c_str(), allowedMinValue, allowedMaxValue);

    return false;
}

} // namespace NKikimr::NQS
