#pragma once

#include "utils.h"

#include <util/generic/string.h>


namespace NKikimr::NSqsTopic {

    TString MakeQueueArn(bool cloud, const TStringBuf region, const TStringBuf account, const TRichQueueUrl& queueUrl);

    std::expected<TRichQueueUrl, TString> ParseQueueArn(const TStringBuf arn);

} // namespace NKikimr::NSqsTopic
