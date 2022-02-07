#pragma once

#include <ydb/core/protos/config.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NSQS {

struct TRedrivePolicy {
    static TRedrivePolicy FromJson(const TString& json, const NKikimrConfig::TSqsConfig& config);

    TString ToJson() const;
    bool IsValid() const;
    const TString& GetErrorText() const;

    TString ErrorText;
    TMaybe<size_t> MaxReceiveCount;
    TMaybe<TString> TargetArn;
    TMaybe<TString> TargetQueueName;
};

} // namespace NKikimr::NSQS
