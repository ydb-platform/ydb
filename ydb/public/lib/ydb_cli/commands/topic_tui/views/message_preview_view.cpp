#include "message_preview_view.h"
#include "../app_interface.h"
#include "../widgets/sparkline.h"
#include "../http_client.h"
#include "../common/async_utils.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Static member definition for position cache
std::unordered_map<TString, ui64> TMessagePreviewView::PositionCache_;

// Global wastebin for detached futures to prevent destructor hangs on View destruction
static std::vector<std::future<std::deque<TTopicMessage>>> S_PendingFutures;
static std::mutex S_PendingFuturesMutex;

TMessagePreviewView::TMessagePreviewView(ITuiApp& app)
    : App_(app)
{}

Component TMessagePreviewView::Build() {
    return Renderer([this] {
        CheckAsyncCompletion();
        
        if (App_.GetViewerEndpoint().empty()) {
            return vbox({
                text("Message preview requires --viewer-endpoint option") | dim | center,
                text("Example: ydb topic tui --viewer-endpoint http://vla5-xxxx:8765 /db") | dim | center,
                text("") ,
                text("Press [Esc] to go back") | dim | center
            }) | border;
        }
        
        // Only show spinner on initial full load, not during background tail poll
        if (Loading_ && !TailPollLoading_) {
            return vbox({
                RenderHeader(),
                separator(),
                RenderSpinner() | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + std::string(ErrorMessage_.c_str())) | color(Color::Red) | center
            }) | border;
        }
        
        return vbox({
            RenderHeader(),
            separator(),
            GotoOffsetMode_ ? 
                hbox({
                    text("Go to offset: ") | bold,
                    text(GotoOffsetInput_ + "_") | color(Color::Yellow),
                    text(" (Enter to go, Esc to cancel)") | dim
                }) | center :
                RenderMessages() | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        if (App_.GetState().CurrentView != EViewType::MessagePreview) {
            return false;
        }
        
        // Block navigation in goto offset input mode
        if (GotoOffsetMode_) {
            // Let the input handling below process this
        } else {
            // Allow page navigation even during loading for snappy feel
            if (event == Event::ArrowLeft) {
                if (TailMode_) {
                    TailMode_ = false;
                    StopTailSession();
                }
                NavigateOlder();
                return true;
            }
            if (event == Event::ArrowRight) {
                NavigateNewer();
                return true;
            }
        }
        
        // Block other events during loading, BUT allow Escape
        if (Loading_ && event != Event::Escape) {
            return false;
        }
        
        if (event == Event::ArrowUp || (!ExpandedView_ && (event == Event::Character('k') || event == Event::Character('K')))) {
            if (TailMode_) {
                TailMode_ = false;
                StopTailSession();
            }
            if (SelectedIndex_ > 0) {
                SelectedIndex_--;
                ContentScrollY_ = 0;  // Reset scroll when selecting new message
            }
            return true;
        }
        if (event == Event::ArrowDown || (!ExpandedView_ && (event == Event::Character('j') || event == Event::Character('J')))) {
            if (SelectedIndex_ < static_cast<int>(Messages_.size()) - 1) {
                SelectedIndex_++;
                ContentScrollY_ = 0;  // Reset scroll when selecting new message
            }
            return true;
        }
        // j/k for scrolling message content in expanded view
        if (ExpandedView_ && (event == Event::Character('j') || event == Event::Character('J'))) {
            ContentScrollY_++;
            return true;
        }
        if (ExpandedView_ && (event == Event::Character('k') || event == Event::Character('K'))) {
            ContentScrollY_ = std::max(0, ContentScrollY_ - 1);
            return true;
        }
        if (event == Event::Return) {
            if (GotoOffsetMode_) {
                // Submit offset
                try {
                    ui64 offset = std::stoull(GotoOffsetInput_);
                    GotoOffsetMode_ = false;
                    GotoOffsetInput_.clear();
                    if (TailMode_) {
                        TailMode_ = false;
                        StopTailSession();
                    }
                    GoToOffset(offset);
                } catch (...) {
                    // Invalid input, just close
                    GotoOffsetMode_ = false;
                    GotoOffsetInput_.clear();
                }
                return true;
            }
            ExpandedView_ = !ExpandedView_;
            ContentScrollY_ = 0;  // Reset scroll when toggling view
            return true;
        }
        if (event == Event::Escape) {
            if (GotoOffsetMode_) {
                // Cancel goto offset input
                GotoOffsetMode_ = false;
                GotoOffsetInput_.clear();
            } else {
                // Save current position to cache before leaving
                TString cacheKey = Sprintf("%s:%u", TopicPath_.c_str(), Partition_);
                PositionCache_[cacheKey] = CurrentOffset_;
                
                // Stop tail mode when leaving the view
                if (TailMode_) {
                    TailMode_ = false;
                    StopTailSession();
                }
                // Navigate back to topic details
                App_.NavigateBack();
            }
            return true;
        }
        if (GotoOffsetMode_) {
            // Handle digit input
            if (event.is_character() && event.character().size() == 1) {
                char c = event.character()[0];
                if (c >= '0' && c <= '9') {
                    GotoOffsetInput_ += c;
                    return true;
                }
            }
            if (event == Event::Backspace && !GotoOffsetInput_.empty()) {
                GotoOffsetInput_.pop_back();
                return true;
            }
            return true;  // Consume all events in input mode
        }
        if (event == Event::Character('t') || event == Event::Character('T')) {
            TailMode_ = !TailMode_;
            if (TailMode_) {
                // Start real-time SDK streaming
                StartTailSession();
            } else {
                // Stop streaming
                StopTailSession();
            }
            return true;
        }
        if (event == Event::Character('g') || event == Event::Character('G')) {
            // Disable tail mode when entering goto offset
            if (TailMode_) {
                TailMode_ = false;
                StopTailSession();
            }
            GotoOffsetMode_ = true;
            GotoOffsetInput_.clear();
            return true;
        }
        return false;
    });
}

void TMessagePreviewView::SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset) {
    Y_UNUSED(startOffset);  // We use cached position or start from the end
    TopicPath_ = topicPath;
    Partition_ = partition;
    Messages_.clear();
    SelectedIndex_ = 0;
    ErrorMessage_.clear();
    
    // Check if we have a cached position for this topic:partition
    TString cacheKey = Sprintf("%s:%u", topicPath.c_str(), partition);
    auto it = PositionCache_.find(cacheKey);
    bool hasCachedPosition = (it != PositionCache_.end());
    ui64 cachedOffset = hasCachedPosition ? it->second : 0;
    
    // Query partition info and load messages
    Loading_ = true;
    LoadFuture_ = std::async(std::launch::async, [this, topicPath, partition, hasCachedPosition, cachedOffset]() -> std::deque<TTopicMessage> {
        try {
            // Get partition info to find bounds
            auto describeResult = App_.GetTopicClient().DescribePartition(
                std::string(topicPath.c_str()), 
                partition, 
                NTopic::TDescribePartitionSettings().IncludeStats(true)
            ).GetValueSync();
            
            if (!describeResult.IsSuccess()) {
                throw std::runtime_error("Failed to describe partition");
            }
            
            const auto& partInfo = describeResult.GetPartitionDescription().GetPartition();
            const auto& stats = partInfo.GetPartitionStats();
            ui64 endOffset = stats ? stats->GetEndOffset() : 0;
            ui64 startOff = stats ? stats->GetStartOffset() : 0;
            
            // Use cached position if valid, otherwise start from end
            ui64 offset;
            if (hasCachedPosition && cachedOffset >= startOff && cachedOffset <= endOffset) {
                offset = cachedOffset;
            } else {
                // Start from PageSize messages before end, or 0 if not enough messages
                offset = endOffset > PageSize ? endOffset - PageSize : 0;
            }
            CurrentOffset_ = offset;
            
            // Now fetch messages using HTTP API
            const TString& endpoint = App_.GetViewerEndpoint();
            if (endpoint.empty()) {
                return {};
            }
            TViewerHttpClient client(endpoint);
            auto vec = client.ReadMessages(topicPath, partition, offset, PageSize, App_.GetMessageSizeLimit());
            return std::deque<TTopicMessage>(std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
        } catch (const std::exception& e) {
            throw;
        }
    });
}

void TMessagePreviewView::Refresh() {
    StartAsyncLoad();
}

void TMessagePreviewView::CheckAsyncCompletion() {
    SpinnerFrame_++;
    
    // Drain SDK tail messages queue (real-time streaming)
    if (TailReaderRunning_) {
        std::lock_guard<std::mutex> lock(TailMessagesMutex_);
        while (!TailMessagesQueue_.empty()) {
            auto& msg = TailMessagesQueue_.front();
            // Only append if offset is greater than our last known
            if (Messages_.empty() || msg.Offset > Messages_.back().Offset) {
                Messages_.push_back(std::move(msg));
            }
            TailMessagesQueue_.pop_front();
        }
        
        // Trim old messages to prevent unbounded growth (keep last 100)
        static constexpr size_t MaxTailMessages = 100;
        while (Messages_.size() > MaxTailMessages) {
            Messages_.pop_front();  // O(1) with deque
        }
        
        // Keep selection at the bottom in tail mode
        if (!Messages_.empty()) {
            SelectedIndex_ = static_cast<int>(Messages_.size()) - 1;
        }
    }
    
    // Refresh partition bounds every 2 seconds (non-blocking)
    auto now = TInstant::Now();
    if (now - LastBoundsRefresh_ > TDuration::Seconds(2)) {
        LastBoundsRefresh_ = now;
        try {
            auto result = App_.GetTopicClient().DescribePartition(
                std::string(TopicPath_.c_str()), 
                Partition_, 
                NTopic::TDescribePartitionSettings().IncludeStats(true)
            ).GetValueSync();
            
            if (result.IsSuccess()) {
                const auto& stats = result.GetPartitionDescription().GetPartition().GetPartitionStats();
                if (stats) {
                    PartitionStartOffset_ = stats->GetStartOffset();
                    PartitionEndOffset_ = stats->GetEndOffset();
                }
            }
        } catch (...) {
            // Ignore errors on background refresh
        }
    }
    
    if (!Loading_ && !TailPollLoading_) {
        return;
    }
    
    if (!LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto newMessages = LoadFuture_.get();
            
            // Check if the loaded offset is stale (user navigated during load)
            if (!TailPollLoading_ && LoadingForOffset_ != CurrentOffset_) {
                // Stale result - discard and load the new offset
                Loading_ = false;
                StartAsyncLoad();
                return;
            }
            
            if (TailPollLoading_) {
                // Append new messages to the existing list
                for (auto& msg : newMessages) {
                    // Only append if offset is greater than our last known
                    if (Messages_.empty() || msg.Offset > Messages_.back().Offset) {
                        Messages_.push_back(std::move(msg));
                    }
                }
                // Keep selection at the bottom in tail mode
                if (!Messages_.empty()) {
                    SelectedIndex_ = static_cast<int>(Messages_.size()) - 1;
                }
            } else {
                // Full load - replace messages
                Messages_ = std::move(newMessages);
                if (TailMode_ && !Messages_.empty()) {
                    SelectedIndex_ = static_cast<int>(Messages_.size()) - 1;
                } else {
                    SelectedIndex_ = 0;
                }
            }
            ErrorMessage_.clear();
        } catch (const std::exception& e) {
            ErrorMessage_ = e.what();
        }
        Loading_ = false;
        TailPollLoading_ = false;
    }
}

Element TMessagePreviewView::RenderSpinner() {
    static const std::vector<std::string> frames = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    std::string frame = frames[SpinnerFrame_ % frames.size()];
    return hbox({
        text(frame) | color(Color::Cyan),
        text(" Loading messages...") | dim
    });
}

Element TMessagePreviewView::RenderHeader() {
    TString offsetInfo;
    if (Loading_) {
        // Show target offset during load for immediate feedback
        offsetInfo = Sprintf("→ %s (loading...)", FormatNumber(CurrentOffset_).c_str());
    } else if (!Messages_.empty()) {
        offsetInfo = Sprintf("%s - %s",
            FormatNumber(Messages_.front().Offset).c_str(),
            FormatNumber(Messages_.back().Offset).c_str());
    } else {
        offsetInfo = "No messages";
    }
    
    // Format topic:partition like CLI argument format (e.g., /Root/db/t10:4)
    TString topicPartition = Sprintf("%s:%u", TopicPath_.c_str(), Partition_);
    
    // Build responsive header using hflow for wrapping
    return vbox({
        // Row 1: Topic:Partition path and tail status
        hflow({
            hbox({text(" ") | dim, text(std::string(topicPartition.c_str())) | color(Color::Cyan) | bold}),
            filler(),
            TailMode_ 
                ? (text(" [Tail: ON] ") | color(Color::Green) | bold)
                : (text(" [t] Tail ") | dim)
        }),
        // Row 2: Offsets - using smaller tokens for wrapping
        hflow({
            hbox({text(" ") | dim, text(std::string(offsetInfo.c_str())) | bold}),
            hbox({text(" Start:") | dim, text(FormatNumber(PartitionStartOffset_).c_str()) | color(Color::Yellow)}),
            hbox({text(" End:") | dim, text(FormatNumber(PartitionEndOffset_).c_str()) | color(Color::Yellow)}),
            filler(),
            text(" [←→g↵] ") | dim
        })
    });
}

Element TMessagePreviewView::RenderMessages() {
    if (Messages_.empty()) {
        return text("No messages in this range") | dim | center;
    }
    
    Elements rows;
    
    for (size_t i = 0; i < Messages_.size(); ++i) {
        bool selected = static_cast<int>(i) == SelectedIndex_;
        Element row = RenderMessageContent(Messages_[i], selected);
        if (selected) {
            row = row | focus;
        }
        rows.push_back(row);
        if (i < Messages_.size() - 1) {
            rows.push_back(separator());
        }
    }
    
    return vbox(rows) | yframe | vscroll_indicator;
}

Element TMessagePreviewView::RenderMessageContent(const TTopicMessage& msg, bool selected) {
    std::string writeTimeStr = std::string(msg.WriteTime.FormatLocalTime("%Y-%m-%d %H:%M:%S").c_str());
    std::string createTimeStr = std::string(msg.CreateTime.FormatLocalTime("%H:%M:%S").c_str());
    
    // Codec name
    std::string codecName;
    switch (msg.Codec) {
        case 0: codecName = "raw"; break;
        case 1: codecName = "gzip"; break;
        case 2: codecName = "lzop"; break;
        case 3: codecName = "zstd"; break;
        default: codecName = std::to_string(msg.Codec); break;
    }
    
    // Granular metadata tokens to allow wrapping on narrow screens.
    // Each pair/group is a nested hflow to allow internal wrapping while grouping logically.
    Elements tokens;
    
    tokens.push_back(text(Sprintf("#%s  ", FormatNumber(msg.Offset).c_str())) | bold | color(Color::Cyan));
    
    auto addTokens = [&](const std::string& label, const std::string& value, Color valueColor = Color::White) {
        tokens.push_back(hflow({
            text(label) | dim,
            text(value) | color(valueColor),
            text("  ")
        }));
    };
    
    addTokens("SeqNo: ", std::to_string(msg.SeqNo));
    addTokens("Write: ", writeTimeStr);
    addTokens("Create: ", createTimeStr + Sprintf(" (+%ldms)", msg.TimestampDiff));
    addTokens("Producer: ", msg.ProducerId.empty() ? "-" : std::string(msg.ProducerId.c_str()), Color::Yellow);
    
    // Unified Size token group
    tokens.push_back(hflow({
        text("Size: ") | dim,
        text(std::string(FormatBytes(msg.OriginalSize).c_str())),
        text("  (stored: ") | dim,
        text(std::string(FormatBytes(msg.StorageSize).c_str()) + ", " + codecName) | color(Color::Magenta),
        text(")  ")
    }));
    
    Element metadata = hflow(std::move(tokens));
    
    // Calculate adaptive width for data eliding.
    // Ellipsis should be at the right side of the screen.
    auto terminalSize = ftxui::Terminal::Size();
    // 2 for borders, 1 for padding/rounding safety.
    int availableWidth = terminalSize.dimx - 3;
    if (availableWidth < 10) availableWidth = 80; // Fallback
    
    // Data preview or full data
    std::string dataPreview = std::string(msg.Data.c_str());
    // Replace newlines/carriage returns for single-line preview
    for (size_t i = 0; i < dataPreview.size(); ++i) {
        if (dataPreview[i] == '\n' || dataPreview[i] == '\r') {
            dataPreview[i] = ' ';
        }
    }
    
    Element dataElement;
    if (!ExpandedView_ || !selected) {
        // Compact: truncate based on terminal width with ellipsis at the very end
        if (dataPreview.size() > (size_t)availableWidth) {
            dataPreview = dataPreview.substr(0, std::max<int>(0, availableWidth - 3)) + "...";
        }
        dataElement = text(dataPreview) | color(Color::GrayLight);
    } else {
        // Expanded: wrap text character-by-character into multiple lines
        Elements allLines;
        for (size_t i = 0; i < dataPreview.size(); i += availableWidth) {
            allLines.push_back(text(dataPreview.substr(i, availableWidth)) | color(Color::GrayLight));
        }
        
        // Calculate visible area (leave space for metadata + some padding)
        auto terminalSize = ftxui::Terminal::Size();
        int maxVisibleLines = std::max(5, terminalSize.dimy - 10);  // Reserve lines for UI chrome
        
        // Clamp scroll position
        int totalLines = static_cast<int>(allLines.size());
        int maxScroll = std::max(0, totalLines - maxVisibleLines);
        ContentScrollY_ = std::min(ContentScrollY_, maxScroll);
        
        // Slice visible lines
        Elements visibleLines;
        int startLine = ContentScrollY_;
        int endLine = std::min(totalLines, startLine + maxVisibleLines);
        for (int i = startLine; i < endLine; ++i) {
            visibleLines.push_back(std::move(allLines[i]));
        }
        
        // Show scroll hint if content overflows
        if (totalLines > maxVisibleLines) {
            int remaining = totalLines - endLine;
            TString scrollHint;
            if (ContentScrollY_ > 0 && remaining > 0) {
                scrollHint = Sprintf("↑ [j/k to scroll: %d more below] ↓", remaining);
            } else if (ContentScrollY_ > 0) {
                scrollHint = "↑ [k to scroll up]";
            } else if (remaining > 0) {
                scrollHint = Sprintf("↓ [j to scroll: %d more lines]", remaining);
            }
            if (!scrollHint.empty()) {
                visibleLines.push_back(text(std::string(scrollHint.c_str())) | dim | center);
            }
        }
        
        dataElement = vbox(std::move(visibleLines));
    }
    
    Element content = vbox({
        metadata,
        dataElement
    }) | xflex;
    
    if (selected) {
        content = content | bgcolor(Color::RGB(40, 60, 100)) | bold;
    }
    
    return content;
}

void TMessagePreviewView::StartAsyncLoad() {
    const TString& endpoint = App_.GetViewerEndpoint();
    if (endpoint.empty()) {
        return;
    }
    
    // Clean up completed old futures (non-blocking)
    {
        std::lock_guard lock(S_PendingFuturesMutex);
        S_PendingFutures.erase(
            std::remove_if(S_PendingFutures.begin(), S_PendingFutures.end(),
                [](auto& f) { return f.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready; }),
            S_PendingFutures.end()
        );
        
        // Move old future to pending vector to prevent destructor blocking
        if (LoadFuture_.valid()) {
            S_PendingFutures.push_back(std::move(LoadFuture_));
        }
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    
    TString topicPath = TopicPath_;
    ui32 partition = Partition_;
    ui64 offset = CurrentOffset_;
    ui64 messageSizeLimit = App_.GetMessageSizeLimit();
    LoadingForOffset_ = offset;  // Track which offset this load is for
    
    LoadFuture_ = std::async(std::launch::async, [endpoint, topicPath, partition, offset, messageSizeLimit]() -> std::deque<TTopicMessage> {
        TViewerHttpClient client(endpoint);
        auto vec = client.ReadMessages(topicPath, partition, offset, 20, messageSizeLimit);
        return std::deque<TTopicMessage>(std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    });
}

void TMessagePreviewView::NavigateOlder() {
    if (CurrentOffset_ >= PageSize) {
        CurrentOffset_ -= PageSize;
    } else {
        CurrentOffset_ = 0;
    }
    // Clamp to partition start
    if (CurrentOffset_ < PartitionStartOffset_) {
        CurrentOffset_ = PartitionStartOffset_;
    }
    StartAsyncLoad();
}

void TMessagePreviewView::NavigateNewer() {
    CurrentOffset_ += PageSize;
    // Clamp to partition end
    if (PartitionEndOffset_ > 0 && CurrentOffset_ > PartitionEndOffset_) {
        CurrentOffset_ = PartitionEndOffset_ > PageSize ? PartitionEndOffset_ - PageSize : 0;
    }
    StartAsyncLoad();
}

void TMessagePreviewView::GoToOffset(ui64 offset) {
    CurrentOffset_ = offset;
    // Clamp to valid range
    if (CurrentOffset_ < PartitionStartOffset_) {
        CurrentOffset_ = PartitionStartOffset_;
    }
    if (PartitionEndOffset_ > 0 && CurrentOffset_ > PartitionEndOffset_) {
        CurrentOffset_ = PartitionEndOffset_ > PageSize ? PartitionEndOffset_ - PageSize : 0;
    }
    StartAsyncLoad();
}

void TMessagePreviewView::StartTailPoll() {
    const TString& endpoint = App_.GetViewerEndpoint();
    if (endpoint.empty()) {
        return;
    }
    
    if (Loading_ || TailPollLoading_) {
        return;
    }
    
    TailPollLoading_ = true;
    
    TString topicPath = TopicPath_;
    ui32 partition = Partition_;
    // Query from the offset after our last known message
    ui64 offset = Messages_.empty() ? CurrentOffset_ : Messages_.back().Offset + 1;
    ui64 messageSizeLimit = App_.GetMessageSizeLimit();
    
    LoadFuture_ = std::async(std::launch::async, [endpoint, topicPath, partition, offset, messageSizeLimit]() -> std::deque<TTopicMessage> {
        TViewerHttpClient client(endpoint);
        auto vec = client.ReadMessages(topicPath, partition, offset, 20, messageSizeLimit);
        return std::deque<TTopicMessage>(std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    });
}

TMessagePreviewView::~TMessagePreviewView() {
    // Signal stop to any async operations
    if (StopFlag_) {
        *StopFlag_ = true;
    }
    StopTailSession();
}

void TMessagePreviewView::StartTailSession() {
    if (TailReaderRunning_) {
        return;
    }
    
    // Jump to the end - clear current messages and set offset to end
    Messages_.clear();
    if (PartitionEndOffset_ > PageSize) {
        CurrentOffset_ = PartitionEndOffset_ - PageSize;
    } else {
        CurrentOffset_ = 0;
    }
    
    // Load the latest messages first via HTTP (they'll appear immediately)
    StartAsyncLoad();
    
    // Create read session settings for real-time streaming
    NTopic::TReadSessionSettings settings;
    settings.WithoutConsumer();
    
    NTopic::TTopicReadSettings topicSettings(std::string(TopicPath_.c_str()));
    topicSettings.AppendPartitionIds(Partition_);
    settings.AppendTopics(topicSettings);
    
    // Use small buffer for responsive reads
    settings.MaxMemoryUsageBytes(1 * 1024 * 1024);  // 1 MB
    
    try {
        TailSession_ = App_.GetTopicClient().CreateReadSession(settings);
    } catch (const std::exception& e) {
        ErrorMessage_ = TStringBuilder() << "Failed to create read session: " << e.what();
        return;
    }
    
    TailReaderRunning_ = true;
    TailReaderThread_ = std::thread([this]() {
        TailReaderLoop();
    });
}

void TMessagePreviewView::StopTailSession() {
    TailReaderRunning_ = false;
    
    // Wait for background thread to exit first (it will detect TailReaderRunning_ = false)
    if (TailReaderThread_.joinable()) {
        TailReaderThread_.join();
    }
    
    // Now safe to close and reset the session
    if (TailSession_) {
        TailSession_->Close(TDuration::MilliSeconds(100));
        TailSession_.reset();
    }
    
    // Clear the queue
    std::lock_guard<std::mutex> lock(TailMessagesMutex_);
    TailMessagesQueue_.clear();
}

void TMessagePreviewView::TailReaderLoop() {
    while (TailReaderRunning_ && TailSession_) {
        // Wait for events with short timeout (10ms) for responsive shutdown
        auto waitFuture = TailSession_->WaitEvent();
        waitFuture.Wait(TDuration::MilliSeconds(10));
        
        // Get all available events (non-blocking)
        auto events = TailSession_->GetEvents(false);
        
        bool gotNewMessages = false;
        
        for (auto& event : events) {
            if (auto* startEvent = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                // Confirm and start from our current offset (after last known message)
                ui64 startOffset = Messages_.empty() ? CurrentOffset_ : Messages_.back().Offset + 1;
                startEvent->Confirm(startOffset);
            }
            else if (auto* stopEvent = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                stopEvent->Confirm();
            }
            else if (auto* dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                for (auto& message : dataEvent->GetMessages()) {
                    TTopicMessage msg;
                    msg.Offset = message.GetOffset();
                    msg.SeqNo = message.GetSeqNo();
                    msg.WriteTime = message.GetWriteTime();
                    msg.CreateTime = message.GetCreateTime();
                    msg.TimestampDiff = (message.GetWriteTime() - message.GetCreateTime()).MilliSeconds();
                    msg.ProducerId = TString(message.GetProducerId());
                    msg.Data = TString(message.GetData());
                    msg.OriginalSize = message.GetData().size();
                    msg.StorageSize = message.GetData().size();  // Approximate
                    msg.Codec = 0;  // Raw (already decompressed by SDK)
                    
                    std::lock_guard<std::mutex> lock(TailMessagesMutex_);
                    TailMessagesQueue_.push_back(std::move(msg));
                    gotNewMessages = true;
                }
            }
            else if (std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                TailReaderRunning_ = false;
                break;
            }
        }
        
        // Immediately trigger UI refresh when new messages arrive
        if (gotNewMessages) {
            App_.PostRefresh();
        }
    }
}

} // namespace NYdb::NConsoleClient
