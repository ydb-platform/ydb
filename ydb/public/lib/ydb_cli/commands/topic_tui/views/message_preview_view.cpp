#include "message_preview_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"
#include "../http_client.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TMessagePreviewView::TMessagePreviewView(TTopicTuiApp& app)
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
            RenderMessages() | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        if (App_.GetState().CurrentView != EViewType::MessagePreview) {
            return false;
        }
        if (Loading_) {
            return false;
        }
        
        if (event == Event::ArrowUp) {
            TailMode_ = false;  // Manual nav disables tail mode
            if (SelectedIndex_ > 0) {
                SelectedIndex_--;
            }
            return true;
        }
        if (event == Event::ArrowDown) {
            if (SelectedIndex_ < static_cast<int>(Messages_.size()) - 1) {
                SelectedIndex_++;
            }
            return true;
        }
        if (event == Event::ArrowLeft) {
            TailMode_ = false;  // Manual nav disables tail mode
            NavigateOlder();
            return true;
        }
        if (event == Event::ArrowRight) {
            NavigateNewer();
            return true;
        }
        if (event == Event::Return) {
            ExpandedView_ = !ExpandedView_;
            return true;
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
        return false;
    });
}

void TMessagePreviewView::SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset) {
    Y_UNUSED(startOffset);  // We always start from the end
    TopicPath_ = topicPath;
    Partition_ = partition;
    Messages_.clear();
    SelectedIndex_ = 0;
    ErrorMessage_.clear();
    
    // Query end offset to start from the last messages
    Loading_ = true;
    LoadFuture_ = std::async(std::launch::async, [this, topicPath, partition]() -> std::deque<TTopicMessage> {
        try {
            // Get partition info to find end offset
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
            
            // Start from PageSize messages before end, or 0 if not enough messages
            ui64 offset = endOffset > PageSize ? endOffset - PageSize : 0;
            CurrentOffset_ = offset;
            
            // Now fetch messages using HTTP API
            const TString& endpoint = App_.GetViewerEndpoint();
            if (endpoint.empty()) {
                return {};
            }
            TViewerHttpClient client(endpoint);
            auto vec = client.ReadMessages(topicPath, partition, offset, PageSize);
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
    
    if (!Loading_ && !TailPollLoading_) {
        return;
    }
    
    if (!LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto newMessages = LoadFuture_.get();
            
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
    TString offsetRange;
    if (!Messages_.empty()) {
        offsetRange = Sprintf("%s - %s",
            FormatNumber(Messages_.front().Offset).c_str(),
            FormatNumber(Messages_.back().Offset).c_str());
    } else {
        offsetRange = "No messages";
    }
    
    return vbox({
        hbox({
            text(" Message Preview: ") | bold,
            text(std::string(TopicPath_.c_str())) | color(Color::Cyan),
            text(" / Partition ") | dim,
            text(std::to_string(Partition_)) | color(Color::White),
            filler(),
            TailMode_ 
                ? (text(" [Tail: ON] ") | color(Color::Green) | bold)
                : (text(" [t] Tail ") | dim)
        }),
        hbox({
            text(" Offset: ") | dim,
            text(std::string(offsetRange.c_str())) | bold,
            filler(),
            text(" [←] Older  [→] Newer  [Enter] Expand ") | dim
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
        Elements lines;
        for (size_t i = 0; i < dataPreview.size(); i += availableWidth) {
            lines.push_back(text(dataPreview.substr(i, availableWidth)) | color(Color::GrayLight));
        }
        dataElement = vbox(std::move(lines));
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
    
    if (Loading_) {
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    
    TString topicPath = TopicPath_;
    ui32 partition = Partition_;
    ui64 offset = CurrentOffset_;
    
    LoadFuture_ = std::async(std::launch::async, [endpoint, topicPath, partition, offset]() -> std::deque<TTopicMessage> {
        TViewerHttpClient client(endpoint);
        auto vec = client.ReadMessages(topicPath, partition, offset, 20);
        return std::deque<TTopicMessage>(std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    });
}

void TMessagePreviewView::NavigateOlder() {
    if (CurrentOffset_ >= 20) {
        CurrentOffset_ -= 20;
    } else {
        CurrentOffset_ = 0;
    }
    StartAsyncLoad();
}

void TMessagePreviewView::NavigateNewer() {
    if (!Messages_.empty()) {
        CurrentOffset_ = Messages_.back().Offset + 1;
    }
    StartAsyncLoad();
}

void TMessagePreviewView::GoToOffset(ui64 offset) {
    CurrentOffset_ = offset;
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
    
    LoadFuture_ = std::async(std::launch::async, [endpoint, topicPath, partition, offset]() -> std::deque<TTopicMessage> {
        TViewerHttpClient client(endpoint);
        auto vec = client.ReadMessages(topicPath, partition, offset, 20);
        return std::deque<TTopicMessage>(std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    });
}

TMessagePreviewView::~TMessagePreviewView() {
    StopTailSession();
}

void TMessagePreviewView::StartTailSession() {
    if (TailReaderRunning_) {
        return;
    }
    
    // Create read session settings
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
    
    if (TailSession_) {
        TailSession_->Close(TDuration::MilliSeconds(100));
        TailSession_.reset();
    }
    
    if (TailReaderThread_.joinable()) {
        TailReaderThread_.join();
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
