#include "topic_list_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicListView::TTopicListView(TTopicTuiApp& app)
    : App_(app)
{}

Component TTopicListView::Build() {
    return Renderer([this] {
        if (Loading_) {
            return vbox({
                text("Loading...") | center
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + ErrorMessage_) | color(Color::Red) | center
            }) | border;
        }
        
        if (Entries_.empty()) {
            return vbox({
                text("Empty directory") | dim | center,
                text("Press [Backspace] to go up") | dim | center
            }) | border;
        }
        
        Elements rows;
        
        // Header row
        rows.push_back(hbox({
            text(" Name") | size(WIDTH, EQUAL, 30) | bold,
            text("Type") | size(WIDTH, EQUAL, 10) | bold,
            text("Partitions") | size(WIDTH, EQUAL, 12) | bold | align_right,
            text("Retention") | size(WIDTH, EQUAL, 12) | bold | align_right,
            text("Write Speed") | size(WIDTH, EQUAL, 15) | bold | align_right
        }) | bgcolor(Color::GrayDark));
        
        rows.push_back(separator());
        
        for (size_t i = 0; i < Entries_.size(); ++i) {
            bool selected = static_cast<int>(i) == SelectedIndex_;
            rows.push_back(RenderEntry(Entries_[i], selected));
        }
        
        return vbox({
            hbox({
                text(" Topics in: ") | bold,
                text(App_.GetState().CurrentPath) | color(Color::Cyan)
            }),
            separator(),
            vbox(rows) | yframe | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::TopicList) {
            return false;
        }
        
        if (event == Event::ArrowUp) {
            if (SelectedIndex_ > 0) {
                SelectedIndex_--;
            }
            return true;
        }
        if (event == Event::ArrowDown) {
            if (SelectedIndex_ < static_cast<int>(Entries_.size()) - 1) {
                SelectedIndex_++;
            }
            return true;
        }
        if (event == Event::Return) {
            if (SelectedIndex_ >= 0 && SelectedIndex_ < static_cast<int>(Entries_.size())) {
                const auto& entry = Entries_[SelectedIndex_];
                if (entry.IsDirectory && OnDirectorySelected) {
                    OnDirectorySelected(entry.FullPath);
                } else if (entry.IsTopic && OnTopicSelected) {
                    OnTopicSelected(entry.FullPath);
                }
            }
            return true;
        }
        if (event == Event::Backspace) {
            // Go up one directory
            TString path = App_.GetState().CurrentPath;
            if (path != "/" && OnDirectorySelected) {
                size_t pos = path.rfind('/');
                if (pos != TString::npos && pos > 0) {
                    OnDirectorySelected(path.substr(0, pos));
                } else if (pos == 0) {
                    OnDirectorySelected("/");
                }
            }
            return true;
        }
        if (event == Event::Character('c') || event == Event::Character('C')) {
            if (OnCreateTopic) {
                OnCreateTopic();
            }
            return true;
        }
        if (event == Event::Character('e') || event == Event::Character('E')) {
            if (SelectedIndex_ >= 0 && SelectedIndex_ < static_cast<int>(Entries_.size())) {
                const auto& entry = Entries_[SelectedIndex_];
                if (entry.IsTopic && OnEditTopic) {
                    OnEditTopic(entry.FullPath);
                }
            }
            return true;
        }
        if (event == Event::Character('d') || event == Event::Character('D')) {
            if (SelectedIndex_ >= 0 && SelectedIndex_ < static_cast<int>(Entries_.size())) {
                const auto& entry = Entries_[SelectedIndex_];
                if (entry.IsTopic && OnDeleteTopic) {
                    OnDeleteTopic(entry.FullPath);
                }
            }
            return true;
        }
        return false;
    });
}

void TTopicListView::Refresh() {
    LoadEntries();
}

Element TTopicListView::RenderEntry(const TTopicListEntry& entry, bool selected) {
    Elements cols;
    
    TString prefix = selected ? "> " : "  ";
    TString name = prefix + entry.Name;
    
    if (entry.IsDirectory) {
        cols.push_back(text(name + "/") | size(WIDTH, EQUAL, 30) | color(Color::Blue));
        cols.push_back(text("dir") | size(WIDTH, EQUAL, 10) | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 12) | align_right | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 12) | align_right | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 15) | align_right | dim);
    } else if (entry.IsTopic) {
        cols.push_back(text(name) | size(WIDTH, EQUAL, 30) | color(Color::Green));
        cols.push_back(text("topic") | size(WIDTH, EQUAL, 10) | color(Color::Green));
        cols.push_back(text(ToString(entry.PartitionCount)) | size(WIDTH, EQUAL, 12) | align_right);
        cols.push_back(text(FormatDuration(entry.RetentionPeriod)) | size(WIDTH, EQUAL, 12) | align_right);
        cols.push_back(text(FormatBytes(entry.WriteSpeedBytesPerSec) + "/s") | size(WIDTH, EQUAL, 15) | align_right);
    } else {
        // Other schema objects (tables, etc.) - show but not interactive
        cols.push_back(text(name) | size(WIDTH, EQUAL, 30) | dim);
        cols.push_back(text("other") | size(WIDTH, EQUAL, 10) | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 12) | align_right | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 12) | align_right | dim);
        cols.push_back(text("-") | size(WIDTH, EQUAL, 15) | align_right | dim);
    }
    
    Element row = hbox(cols);
    
    if (selected) {
        row = row | bgcolor(Color::GrayDark);
    }
    
    return row;
}

void TTopicListView::LoadEntries() {
    Loading_ = true;
    ErrorMessage_.clear();
    Entries_.clear();
    SelectedIndex_ = 0;
    
    try {
        auto& schemeClient = App_.GetSchemeClient();
        auto result = schemeClient.ListDirectory(App_.GetState().CurrentPath).GetValueSync();
        
        if (!result.IsSuccess()) {
            ErrorMessage_ = TString(result.GetIssues().ToString());
            Loading_ = false;
            return;
        }
        
        
        // Add parent directory if not at root
        if (App_.GetState().CurrentPath != "/") {
            TTopicListEntry parent;
            parent.Name = "..";
            parent.IsDirectory = true;
            TString path = App_.GetState().CurrentPath;
            size_t pos = path.rfind('/');
            if (pos != TString::npos && pos > 0) {
                parent.FullPath = path.substr(0, pos);
            } else {
                parent.FullPath = "/";
            }
            Entries_.push_back(parent);
        }
        
        for (const auto& child : result.GetChildren()) {
            TTopicListEntry listEntry;
            listEntry.Name = TString(child.Name);
            listEntry.FullPath = App_.GetState().CurrentPath;
            if (!listEntry.FullPath.EndsWith("/")) {
                listEntry.FullPath += "/";
            }
            listEntry.FullPath += child.Name;
            
            switch (child.Type) {
                case NScheme::ESchemeEntryType::Topic:
                    listEntry.IsTopic = true;
                    // Don't fetch details here - it's slow and blocks the UI
                    // Details are shown when the user navigates into the topic
                    break;
                case NScheme::ESchemeEntryType::Directory:
                case NScheme::ESchemeEntryType::SubDomain:
                case NScheme::ESchemeEntryType::ColumnStore:
                case NScheme::ESchemeEntryType::ExternalDataSource:
                case NScheme::ESchemeEntryType::ExternalTable:
                case NScheme::ESchemeEntryType::View:
                    listEntry.IsDirectory = true;
                    break;
                default:
                    // Show but mark as non-navigable
                    break;
            }
            
            Entries_.push_back(listEntry);
        }
        
    } catch (const std::exception& e) {
        ErrorMessage_ = e.what();
    }
    
    Loading_ = false;
}

} // namespace NYdb::NConsoleClient
