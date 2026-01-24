#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

#include <future>

namespace NYdb::NConsoleClient {

class ITuiApp;

class TEditConsumerForm : public TFormBase {
public:
    explicit TEditConsumerForm(ITuiApp& app);
    
    // Set the consumer to edit
    void SetConsumer(const TString& topicPath, const TString& consumerName);
    void Reset() override;
    
    const TString& GetTopicPath() const { return TopicPath_; }
    const TString& GetConsumerName() const { return ConsumerName_; }
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 55; }
    ftxui::Component BuildContainer() override;
    
private:
    void CheckAsyncCompletion();
    void DoAsyncSubmit();
    
    TString TopicPath_;
    TString ConsumerName_;
    
    // Form fields
    bool Important_ = false;
    bool CodecRaw_ = true;
    bool CodecGzip_ = true;
    bool CodecZstd_ = false;
    bool CodecLzop_ = false;
    
    // Components
    ftxui::Component ImportantCheckbox_;
    ftxui::Component RawCheckbox_;
    ftxui::Component GzipCheckbox_;
    ftxui::Component ZstdCheckbox_;
    ftxui::Component LzopCheckbox_;
    
    // Async state
    std::future<TStatus> SubmitFuture_;
};

} // namespace NYdb::NConsoleClient
