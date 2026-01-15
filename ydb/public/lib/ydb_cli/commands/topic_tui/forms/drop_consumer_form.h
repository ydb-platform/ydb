#pragma once

#include "../widgets/form_base.h"

#include <util/generic/string.h>


namespace NYdb::NConsoleClient {

class ITuiApp;

class TDropConsumerForm : public TFormBase {
public:
    explicit TDropConsumerForm(ITuiApp& app);
    
    // Set the consumer to drop
    void SetConsumer(const TString& topicPath, const TString& consumerName);
    void Reset() override;
    
    const TString& GetTopicPath() const { return TopicPath_; }
    const TString& GetConsumerName() const { return ConsumerName_; }
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    ftxui::Element RenderFooter() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 50; }
    ftxui::Component BuildContainer() override;
    
private:
    TString TopicPath_;
    TString ConsumerName_;
    std::string ConfirmInput_;
    ftxui::Component ConfirmInputComponent_;
};

} // namespace NYdb::NConsoleClient
