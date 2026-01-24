#pragma once

#include "view_interface.h"
#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include "../http_client.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <deque>
#include <unordered_map>

namespace NYdb::NConsoleClient {


class ITuiApp;

class TMessagePreviewView : public ITuiView {
public:
    explicit TMessagePreviewView(ITuiApp& app);
    ~TMessagePreviewView() override;
    
    ftxui::Component Build() override;
    void Refresh() override;
    void SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset);
    void CheckAsyncCompletion();
    
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
    ITuiApp& App_;
    TString TopicPath_;
    ui32 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    
    std::deque<TTopicMessage> Messages_;  // deque for O(1) pop_front
    int SelectedIndex_ = 0;
    bool ExpandedView_ = false;
    
    // Partition bounds (refreshed in background)
    ui64 PartitionStartOffset_ = 0;
    ui64 PartitionEndOffset_ = 0;
    TInstant LastBoundsRefresh_;
    
    static constexpr ui32 PageSize = 20;
    
    std::atomic<bool> Loading_{false};
    ui64 LoadingForOffset_ = 0;  // Track which offset the current load is for
    std::future<std::deque<TTopicMessage>> LoadFuture_;
    // PendingFutures was moved to static storage in cpp
    // PendingFutures moved to static storage in cpp to prevent destructor hang
    TString ErrorMessage_;
    int SpinnerFrame_ = 0;
    
    bool TailMode_ = false;
    TInstant LastTailRefresh_;
    bool TailPollLoading_ = false;  // True when doing background poll (no spinner)
    
    // Go to offset input mode
    bool GotoOffsetMode_ = false;
    std::string GotoOffsetInput_;
    
    // Content scroll for expanded message view
    int ContentScrollY_ = 0;
    
    // SDK streaming for tail mode
    std::shared_ptr<NTopic::IReadSession> TailSession_;
    std::thread TailReaderThread_;
    std::atomic<bool> TailReaderRunning_{false};
    std::mutex TailMessagesMutex_;
    std::deque<TTopicMessage> TailMessagesQueue_;
    
    // Static cache of last viewed offset per topic:partition
    // Key format: "topic_path:partition_id"
    static std::unordered_map<TString, ui64> PositionCache_;
    
    // Stop flag for async cancellation
    std::shared_ptr<std::atomic<bool>> StopFlag_ = std::make_shared<std::atomic<bool>>(false);
};

} // namespace NYdb::NConsoleClient
