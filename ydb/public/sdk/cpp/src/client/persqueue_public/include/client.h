#pragma once

#include "control_plane.h"
#include "read_session.h"
#include "write_session.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::inline Dev {
    class TProtoAccessor;
}

namespace NYdb::inline Dev::NPersQueue {

struct TPersQueueClientSettings : public TCommonClientSettingsBase<TPersQueueClientSettings> {
    using TSelf = TPersQueueClientSettings;

    //! Default executor for compression tasks.
    FLUENT_SETTING_DEFAULT(IExecutor::TPtr, DefaultCompressionExecutor, CreateThreadPoolExecutor(2));

    //! Default executor for callbacks.
    FLUENT_SETTING_DEFAULT(IExecutor::TPtr, DefaultHandlersExecutor, CreateThreadPoolExecutor(1));

    //! Manages cluster discovery mode.
    FLUENT_SETTING_DEFAULT(EClusterDiscoveryMode, ClusterDiscoveryMode, EClusterDiscoveryMode::Auto);
};

// PersQueue client.
class TPersQueueClient {
public:
    class TImpl;

    TPersQueueClient(const TDriver& driver, const TPersQueueClientSettings& settings = TPersQueueClientSettings());

    void ProvideCodec(ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl);

    // Create a new topic.
    TAsyncStatus CreateTopic(const std::string& path, const TCreateTopicSettings& = {});

    // Update a topic.
    TAsyncStatus AlterTopic(const std::string& path, const TAlterTopicSettings& = {});

    // Delete a topic.
    TAsyncStatus DropTopic(const std::string& path, const TDropTopicSettings& = {});

    // Add topic read rule
    TAsyncStatus AddReadRule(const std::string& path, const TAddReadRuleSettings& = {});

    // Remove topic read rule
    TAsyncStatus RemoveReadRule(const std::string& path, const TRemoveReadRuleSettings& = {});

    // Describe settings of topic.
    TAsyncDescribeTopicResult DescribeTopic(const std::string& path, const TDescribeTopicSettings& = {});

    //! Create read session.
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings);

    //! Create write session.
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);

protected:
    void OverrideCodec(ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NPersQueue
