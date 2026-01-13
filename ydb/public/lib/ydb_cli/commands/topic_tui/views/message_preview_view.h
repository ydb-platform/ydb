#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include "../http_client.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <functional>
#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <deque>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

class TMessagePreviewView {
public:
    explicit TMessagePreviewView(TTopicTuiApp& app);
    ~TMessagePreviewView();
    
    ftxui::Component Build();
    void Refresh();
    void SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset);
    void CheckAsyncCompletion();
    
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderMessages();
    ftxui::Element RenderMessageContent(const TTopicMessage& msg, bool selected);
    ftxui::Element RenderSpinner();
    void StartAsyncLoad();
    void StartTailPoll();
    void NavigateOlder();
    void NavigateNewer();
    void GoToOffset(ui64 offset);
    
    // SDK streaming for tail mode
    void StartTailSession();
    void StopTailSession();
    void TailReaderLoop();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    ui32 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    
    std::deque<TTopicMessage> Messages_;  // deque for O(1) pop_front
    int SelectedIndex_ = 0;
    bool ExpandedView_ = false;
    
    static constexpr ui32 PageSize = 20;
    
    std::atomic<bool> Loading_{false};
    std::future<std::deque<TTopicMessage>> LoadFuture_;
    TString ErrorMessage_;
    int SpinnerFrame_ = 0;
    
    bool TailMode_ = false;
    TInstant LastTailRefresh_;
    bool TailPollLoading_ = false;  // True when doing background poll (no spinner)
    
    // SDK streaming for tail mode
    std::shared_ptr<NTopic::IReadSession> TailSession_;
    std::thread TailReaderThread_;
    std::atomic<bool> TailReaderRunning_{false};
    std::mutex TailMessagesMutex_;
    std::deque<TTopicMessage> TailMessagesQueue_;
};

} // namespace NYdb::NConsoleClient
