#pragma once

#include "../widgets/form_base.h"

#include <util/generic/string.h>


namespace NYdb::NConsoleClient {

class ITuiApp;

class TDeleteConfirmForm : public TFormBase {
public:
    explicit TDeleteConfirmForm(ITuiApp& app);
    
    // Set the topic to delete
    void SetTopic(const TString& topicPath);
    void Reset() override;
    
    const TString& GetTopicPath() const { return TopicPath_; }
    
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
    TString TopicName_;  // Just the name part for display
    std::string ConfirmInput_;
    ftxui::Component ConfirmInputComponent_;
};

} // namespace NYdb::NConsoleClient
