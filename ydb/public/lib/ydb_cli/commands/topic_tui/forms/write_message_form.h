#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

#include <future>
#include <atomic>
#include <optional>

namespace NYdb::NConsoleClient {

class ITuiApp;

class TWriteMessageForm : public TFormBase {
public:
    explicit TWriteMessageForm(ITuiApp& app);
    
    void SetTopic(const TString& topicPath, std::optional<ui32> partitionId);
    void Reset() override;
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 60; }
    ftxui::Component BuildContainer() override;
    
private:
    void CheckAsyncCompletion();
    void DoAsyncSend();
    void CreateWriteSession();

    struct TWriteSessionKey {
        std::string TopicPath;
        std::string ProducerId;
        std::string MessageGroupId;
        std::optional<ui32> PartitionId;
        NTopic::ECodec Codec = NTopic::ECodec::RAW;

        bool operator==(const TWriteSessionKey& other) const {
            return TopicPath == other.TopicPath &&
                   ProducerId == other.ProducerId &&
                   MessageGroupId == other.MessageGroupId &&
                   PartitionId == other.PartitionId &&
                   Codec == other.Codec;
        }
        bool operator!=(const TWriteSessionKey& other) const {
            return !(*this == other);
        }
    };
    
private:
    TString TopicPath_;
    std::optional<ui32> PartitionId_;
    
    // Form fields
    std::string MessageData_;
    std::string ProducerIdInput_;
    std::string MessageGroupIdInput_;
    std::string PartitionInput_;
    
    // Cursor positions for InputOption
    int MessageCursor_ = 0;
    int ProducerCursor_ = 0;
    int MessageGroupCursor_ = 0;
    int PartitionCursor_ = 0;
    
    // Async state
    std::future<bool> SendFuture_;
    int MessagesSent_ = 0;
    
    // Components for rendering
    ftxui::Component MessageInputComponent_;
    ftxui::Component ProducerInputComponent_;
    ftxui::Component MessageGroupInputComponent_;
    ftxui::Component PartitionInputComponent_;

    // Write session (reused for multiple messages)
    std::shared_ptr<NTopic::ISimpleBlockingWriteSession> WriteSession_;
    std::optional<TWriteSessionKey> WriteSessionKey_;
};

} // namespace NYdb::NConsoleClient
