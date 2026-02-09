#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

namespace NYql {

struct TCompositeTopicReadSessionSettings {
    TDuration MaxPartitionReadSkew;
};

class ICompositeTopicReadSessionControl {
public:
    using TPtr = std::shared_ptr<ICompositeTopicReadSessionControl>;

    virtual ~ICompositeTopicReadSessionControl() = default;

    virtual void AdvanceTime(TInstant readTime) = 0; // Minimal time for external partitions

    virtual TInstant GetReadTime() const = 0; // Minimal time for local partition

    virtual NThreading::TFuture<void> SubscribeOnUpdate() = 0;
};

std::pair<std::shared_ptr<NYdb::NTopic::IReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateCompositeTopicReadSession(const TCompositeTopicReadSessionSettings& settings, const std::vector<std::shared_ptr<NYdb::NTopic::IReadSession>>& readSessions);

} // namespace NYql
