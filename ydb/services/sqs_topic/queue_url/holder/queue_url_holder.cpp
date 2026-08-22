#include "queue_url_holder.h"
#include <ydb/core/base/path.h>

namespace NKikimr::NSqsTopic {
    TQueueUrlHolder::TQueueUrlHolder(std::expected<TRichQueueUrl, TString> queueUrl)
        : QueueUrl_(std::move(queueUrl))
    {
        if (FormalValidQueueUrl()) {
            FullTopicPath_ = CanonizePath(NKikimr::JoinPath({QueueUrl_->Database, QueueUrl_->TopicPath}));
        }
    }

    TQueueUrlHolder::~TQueueUrlHolder() = default;
    std::expected<TString, TString> TQueueUrlHolder::GetTopicPath() const {
        return QueueUrl_.transform([](const TRichQueueUrl& r) { return r.TopicPath; });
    }

    bool TQueueUrlHolder::FormalValidQueueUrl() const {
        return QueueUrl_.has_value() && !QueueUrl_.value().TopicPath.empty();
    }

} // namespace NKikimr::NSqsTopic
