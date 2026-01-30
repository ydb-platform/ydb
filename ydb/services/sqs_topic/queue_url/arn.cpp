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

    std::expected<TRichQueueUrl, TString> ParseQueueArn(const TStringBuf arn) {
        // ARN format: prefix:region:account:resource
        // where resource is /v1 + length-delimited strings

        TStringBuf remaining = arn;

        // Skip prefix (yrn:ya:sqs or yrn:yc:ymq)
        if (!remaining.SkipPrefix(YA_SQS_ARN_PREFIX) && !remaining.SkipPrefix(CLOUD_ARN_PREFIX)) {
            return std::unexpected("Invalid ARN: unknown prefix");
        }

        if (!remaining.SkipPrefix(":")) {
            return std::unexpected("Invalid ARN: missing separator after prefix");
        }

        // Skip region
        size_t regionEnd = remaining.find(':');
        if (regionEnd == TStringBuf::npos) {
            return std::unexpected("Invalid ARN: missing region separator");
        }
        remaining.Skip(regionEnd + 1);

        // Skip account
        size_t accountEnd = remaining.find(':');
        if (accountEnd == TStringBuf::npos) {
            return std::unexpected("Invalid ARN: missing account separator");
        }
        remaining.Skip(accountEnd + 1);

        // Parse resource part using ParseQueueUrlPath
        return ParseQueueUrlPath(remaining);
    }

} // namespace NKikimr::NSqsTopic
