#pragma once
#include <ydb/services/sqs_topic/queue_url/utils.h>

namespace NKikimr::NSqsTopic {

    class TQueueUrlHolder {
    public:
        explicit TQueueUrlHolder(std::expected<TRichQueueUrl, TString> queueUrl);
        ~TQueueUrlHolder();

        std::expected<TString, TString> GetTopicPath() const;

        bool FormalValidQueueUrl() const;

    protected:
        std::expected<TRichQueueUrl, TString> QueueUrl_;
        TString FullTopicPath_;
    };
} // namespace NKikimr::NSqsTopic
