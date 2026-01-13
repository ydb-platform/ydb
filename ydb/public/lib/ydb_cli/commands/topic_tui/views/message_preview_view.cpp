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
        
        if (Loading_) {
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
        return false;
    });
}

void TMessagePreviewView::SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset) {
    TopicPath_ = topicPath;
    Partition_ = partition;
    CurrentOffset_ = startOffset;
    StartAsyncLoad();
}

void TMessagePreviewView::Refresh() {
    StartAsyncLoad();
}

void TMessagePreviewView::CheckAsyncCompletion() {
    SpinnerFrame_++;
    
    if (!Loading_ || !LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            Messages_ = LoadFuture_.get();
            ErrorMessage_.clear();
        } catch (const std::exception& e) {
            ErrorMessage_ = e.what();
        }
        Loading_ = false;
        SelectedIndex_ = 0;
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
            text(std::to_string(Partition_)) | color(Color::White)
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
    
    LoadFuture_ = std::async(std::launch::async, [endpoint, topicPath, partition, offset]() -> TVector<TTopicMessage> {
        TViewerHttpClient client(endpoint);
        return client.ReadMessages(topicPath, partition, offset, 20);
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

} // namespace NYdb::NConsoleClient
