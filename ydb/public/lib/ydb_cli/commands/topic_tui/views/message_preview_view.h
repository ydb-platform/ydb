#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include "../http_client.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

class TMessagePreviewView {
public:
    explicit TMessagePreviewView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    void SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset = 0);
    
    // Callbacks
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderMessages();
    ftxui::Element RenderMessageContent(const TTopicMessage& msg, bool selected);
    void LoadMessages();
    void NavigateOlder();
    void NavigateNewer();
    void GoToOffset(ui64 offset);
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    ui32 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    
    TVector<TTopicMessage> Messages_;
    int SelectedIndex_ = 0;
    bool ExpandedView_ = false;
    
    bool Loading_ = false;
    TString ErrorMessage_;
    
    static constexpr ui32 PageSize = 10;
};

} // namespace NYdb::NConsoleClient
