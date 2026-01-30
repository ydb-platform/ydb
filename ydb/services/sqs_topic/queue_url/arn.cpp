#include "arn.h"
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NSqsTopic {

    static constexpr TStringBuf YA_SQS_ARN_PREFIX = "yrn:ya:sqs";
    static constexpr TStringBuf CLOUD_ARN_PREFIX = "yrn:yc:ymq";
    static constexpr TStringBuf ARN_VERSION1_PREFIX = "/v1";

    TString MakeQueueArn(bool cloud, const TStringBuf region, const TStringBuf account, const TRichQueueUrl& queueUrl) {
        TStringBuf prefix = cloud ? CLOUD_ARN_PREFIX : YA_SQS_ARN_PREFIX;
        TStringBuilder resource;
        resource << ARN_VERSION1_PREFIX;
        WriteLengthDelimitedString(resource.Out, queueUrl.Database);
        WriteLengthDelimitedString(resource.Out, queueUrl.TopicPath);
        WriteLengthDelimitedString(resource.Out, queueUrl.Consumer);
        if (queueUrl.Fifo) {
            resource << ".fifo";
        }
        return Join(":", prefix, region, account, resource);
    }

} // namespace NKikimr::NSqsTopic
