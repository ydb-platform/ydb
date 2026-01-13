#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/string.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

class TOffsetForm {
public:
    explicit TOffsetForm(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void SetContext(const TString& topicPath, const TString& consumerName, ui64 partition, ui64 currentOffset, ui64 endOffset);
    void Reset();
    
    // Callbacks
    std::function<void(ui64 offset)> OnSubmit;
    std::function<void()> OnCancel;
    
private:
    ftxui::Element RenderForm();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    TString ConsumerName_;
    ui64 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    ui64 EndOffset_ = 0;
    
    std::string OffsetInput_;
    int FocusedField_ = 0;
};

} // namespace NYdb::NConsoleClient
