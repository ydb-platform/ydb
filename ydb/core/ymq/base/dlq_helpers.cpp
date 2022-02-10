#include "dlq_helpers.h"

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/limits.h>
#include <library/cpp/scheme/scheme.h>

#include <util/string/split.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

static const TStringBuf maxReceiveCount = "maxReceiveCount";
static const TStringBuf deadLetterTargetArn = "deadLetterTargetArn";

static bool IsValidArn(const TString& arn, const NKikimrConfig::TSqsConfig& config) {
    TVector<TStringBuf> items;
    StringSplitter(arn).Split(':').AddTo(&items);
    if (items.size() != 6) {
        return false;
    }

    // prefix
    const TString& requiredPrefix = config.GetYandexCloudMode() ? cloudArnPrefix : yaSqsArnPrefix;
    if (!arn.StartsWith(requiredPrefix)) {
        return false;
    }

    // region
    if (items[3] != config.GetYandexCloudServiceRegion()) {
        return false;
    }

    return items[4] && items[5]; // account | folder id and queue name
}

TRedrivePolicy TRedrivePolicy::FromJson(const TString& json, const NKikimrConfig::TSqsConfig& config) {

    TRedrivePolicy policy;
    const NSc::TValue validatedRedrivePolicy = NSc::TValue::FromJson(json);
    if (validatedRedrivePolicy.IsDict() &&
        validatedRedrivePolicy.Has(maxReceiveCount) && (validatedRedrivePolicy[maxReceiveCount].IsString() || validatedRedrivePolicy[maxReceiveCount].IsIntNumber()) &&
        validatedRedrivePolicy.Has(deadLetterTargetArn) && validatedRedrivePolicy[deadLetterTargetArn].IsString())
    {
        size_t maxReceiveCountValue = 0;
        if (TryFromString(validatedRedrivePolicy[maxReceiveCount].ForceString(), maxReceiveCountValue)) {
            if (maxReceiveCountValue < TLimits::MinMaxReceiveCount || maxReceiveCountValue > TLimits::MaxMaxReceiveCount) {
                policy.ErrorText = "maxReceiveCount must be greater than 0 and less than 1001.";
                return policy;
            }
        } else {
            policy.ErrorText = "Failed to parse maxReceiveCount as an integer.";
            return policy;
        }

        const TString arn = validatedRedrivePolicy[deadLetterTargetArn].ForceString();
        if (!IsValidArn(arn, config)) {
            policy.ErrorText = "Invalid deadLetterTargetArn.";
            return policy;
        }

        TStringBuf skipped, queueName;
        if (!TStringBuf(arn).TryRSplit(':', skipped, queueName)) {
            queueName = arn;
        }

        if (!queueName) {
            policy.ErrorText = "Empty dead letter target queue name.";
            return policy;
        }

        policy.MaxReceiveCount = maxReceiveCountValue;
        policy.TargetQueueName = TString(queueName);
        policy.TargetArn = TString(arn);
    } else if (!json) {
        // Clear redrive policy
        policy.MaxReceiveCount = 0;
        policy.TargetQueueName = "";
        policy.TargetArn = "";
    } else {
        policy.ErrorText = "RedrivePolicy attribute is ill-constructed.";
    }

    return policy;
}

TString TRedrivePolicy::ToJson() const {
    if (MaxReceiveCount && TargetArn) {
        NSc::TValue policy;
        policy.SetDict();
        policy[maxReceiveCount] = *MaxReceiveCount;
        policy[deadLetterTargetArn] = *TargetArn;
        return policy.ToJson();
    }
    return {};
}

bool TRedrivePolicy::IsValid() const {
    return ErrorText.empty();
}

const TString& TRedrivePolicy::GetErrorText() const {
    return ErrorText;
}

} // namespace NKikimr::NSQS
