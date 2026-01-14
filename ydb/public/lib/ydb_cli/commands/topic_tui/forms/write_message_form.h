#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

#include <functional>
#include <future>
#include <atomic>
#include <optional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

class TWriteMessageForm : public TFormBase {
public:
    explicit TWriteMessageForm(TTopicTuiApp& app);
    
    void SetTopic(const TString& topicPath, std::optional<ui32> partitionId);
    void Reset() override;
    
    // Custom close callback (instead of OnSuccess, since form stays open for multiple messages)
    std::function<void()> OnClose;
    
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
    
private:
    TString TopicPath_;
    std::optional<ui32> PartitionId_;
    
    // Form fields
    std::string MessageData_;
    std::string ProducerIdInput_;
    std::string MessageGroupIdInput_;
    
    // Async state
    std::future<bool> SendFuture_;
    int MessagesSent_ = 0;
    
    // Components for rendering
    ftxui::Component MessageInputComponent_;
    ftxui::Component ProducerInputComponent_;
    ftxui::Component MessageGroupInputComponent_;
    
    // Write session (reused for multiple messages)
    std::shared_ptr<NTopic::ISimpleBlockingWriteSession> WriteSession_;
};

} // namespace NYdb::NConsoleClient
