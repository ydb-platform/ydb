#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

#include <functional>

namespace NYdb::NConsoleClient {

class ITuiApp;

struct TMessageFormData {
    TString Data;
    TString MessageGroupId;
    TMaybe<ui32> Partition;  // Nothing = auto
};

class TMessageForm {
public:
    explicit TMessageForm(ITuiApp& app);
    
    ftxui::Component Build();
    void SetTopicPath(const TString& topicPath);
    void Reset();
    
    // Callbacks
    std::function<void(const TMessageFormData& data)> OnSubmit;
    std::function<void()> OnCancel;
    
private:
    ftxui::Element RenderForm();
    
private:
    ITuiApp& App_;
    TString TopicPath_;
    TMessageFormData Data_;
    
    std::string DataInput_;
    std::string MessageGroupIdInput_;
    std::string PartitionInput_;
    
    int FocusedField_ = 0;
};

} // namespace NYdb::NConsoleClient
