#include "message_preview_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TMessagePreviewView::TMessagePreviewView(TTopicTuiApp& app)
    : App_(app)
{}

Component TMessagePreviewView::Build() {
    return Renderer([this] {
        if (Loading_) {
            return vbox({
                text("Loading messages...") | center
            }) | border;
        }
        
        if (!App_.GetViewerEndpoint().empty() && !ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + ErrorMessage_) | color(Color::Red) | center
            }) | border;
        }
        
        if (App_.GetViewerEndpoint().empty()) {
            return vbox({
                text("Message preview requires --viewer-endpoint option") | dim | center,
                text("Example: ydb topic tui --viewer-endpoint http://localhost:8765 /local") | dim | center
            }) | border;
        }
        
        return vbox({
            RenderHeader(),
            separator(),
            RenderMessages() | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::MessagePreview) {
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
        if (event == Event::Character('g') || event == Event::Character('G')) {
            // TODO: Show go-to-offset dialog
            return true;
        }
        return false;
    });
}

void TMessagePreviewView::SetTopic(const TString& topicPath, ui32 partition, ui64 startOffset) {
    TopicPath_ = topicPath;
    Partition_ = partition;
    CurrentOffset_ = startOffset;
    LoadMessages();
}

void TMessagePreviewView::Refresh() {
    LoadMessages();
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
            text(TopicPath_) | color(Color::Cyan),
            text(" / Partition ") | dim,
            text(ToString(Partition_)) | color(Color::White)
        }),
        hbox({
            text(" Offset: ") | dim,
            text(offsetRange) | bold,
            filler(),
            text(" [←] Older  [→] Newer  [g] Go to Offset ") | dim
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
        rows.push_back(RenderMessageContent(Messages_[i], selected));
        if (i < Messages_.size() - 1) {
            rows.push_back(separator());
        }
    }
    
    return vbox(rows) | yframe;
}

Element TMessagePreviewView::RenderMessageContent(const TTopicMessage& msg, bool selected) {
    TString timeStr = msg.WriteTime.FormatLocalTime("%Y-%m-%d %H:%M:%S");
    TString sizeStr = FormatBytes(msg.UncompressedSize);
    
    // Header line
    Element header = hbox({
        text(Sprintf("#%s", FormatNumber(msg.Offset).c_str())) | bold,
        text("  ") ,
        text(timeStr) | dim,
        text("  MsgGroup: ") | dim,
        text(msg.MessageGroupId.empty() ? "-" : msg.MessageGroupId) | color(Color::Yellow),
        text("  Size: ") | dim,
        text(sizeStr)
    });
    
    // Content preview
    TString dataPreview = msg.Data;
    const size_t maxLen = ExpandedView_ && selected ? 500 : 100;
    if (dataPreview.size() > maxLen) {
        dataPreview = dataPreview.substr(0, maxLen) + "...";
    }
    
    // Replace newlines with visible markers for compact view
    if (!ExpandedView_ || !selected) {
        for (size_t i = 0; i < dataPreview.size(); ++i) {
            if (dataPreview[i] == '\n') dataPreview[i] = ' ';
            if (dataPreview[i] == '\r') dataPreview[i] = ' ';
        }
    }
    
    Element content = vbox({
        header,
        paragraph(dataPreview) | color(Color::GrayLight)
    });
    
    if (selected) {
        content = content | bgcolor(Color::GrayDark);
    }
    
    return content | border;
}

void TMessagePreviewView::LoadMessages() {
    const TString& endpoint = App_.GetViewerEndpoint();
    if (endpoint.empty()) {
        ErrorMessage_ = "Viewer endpoint not configured";
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    Messages_.clear();
    SelectedIndex_ = 0;
    
    try {
        TViewerHttpClient client(endpoint);
        Messages_ = client.ReadMessages(TopicPath_, Partition_, CurrentOffset_, PageSize);
    } catch (const std::exception& e) {
        ErrorMessage_ = e.what();
    }
    
    Loading_ = false;
}

void TMessagePreviewView::NavigateOlder() {
    if (CurrentOffset_ >= PageSize) {
        CurrentOffset_ -= PageSize;
    } else {
        CurrentOffset_ = 0;
    }
    LoadMessages();
}

void TMessagePreviewView::NavigateNewer() {
    if (!Messages_.empty()) {
        CurrentOffset_ = Messages_.back().Offset + 1;
    }
    LoadMessages();
}

void TMessagePreviewView::GoToOffset(ui64 offset) {
    CurrentOffset_ = offset;
    LoadMessages();
}

} // namespace NYdb::NConsoleClient
