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
        // Check if async operation completed
        CheckAsyncCompletion();
        
        if (Loading_) {
            return vbox({
                hbox({
                    text(" Topics in: ") | bold,
                    text(std::string(App_.GetState().CurrentPath.c_str())) | color(Color::Cyan)
                }),
                separator(),
                RenderSpinner() | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + std::string(ErrorMessage_.c_str())) | color(Color::Red) | center
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
            text(" Name") | size(WIDTH, EQUAL, 40) | bold,
            text("Type") | size(WIDTH, EQUAL, 10) | bold
        }) | bgcolor(Color::GrayDark));
        
        rows.push_back(separator());
        
        for (size_t i = 0; i < Entries_.size(); ++i) {
            bool selected = static_cast<int>(i) == SelectedIndex_;
            rows.push_back(RenderEntry(Entries_[i], selected));
        }
        
        return vbox({
            hbox({
                text(" Topics in: ") | bold,
                text(std::string(App_.GetState().CurrentPath.c_str())) | color(Color::Cyan)
            }),
            separator(),
            vbox(rows) | yframe | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::TopicList) {
            return false;
        }
        
        // Ignore events while loading
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
    StartAsyncLoad();
}

void TTopicListView::CheckAsyncCompletion() {
    if (!Loading_ || !LoadFuture_.valid()) {
        return;
    }
    
    // Check if future is ready (non-blocking)
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            Entries_ = LoadFuture_.get();
            ErrorMessage_.clear();
        } catch (const std::exception& e) {
            ErrorMessage_ = e.what();
            Entries_.clear();
        }
        Loading_ = false;
        SelectedIndex_ = 0;
    } else {
        // Still loading - advance spinner
        SpinnerFrame_++;
    }
}

Element TTopicListView::RenderSpinner() {
    static const std::vector<std::string> frames = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    std::string frame = frames[SpinnerFrame_ % frames.size()];
    return hbox({
        text(frame) | color(Color::Cyan),
        text(" Loading...") | dim
    });
}

Element TTopicListView::RenderEntry(const TTopicListEntry& entry, bool selected) {
    Elements cols;
    
    std::string prefix = selected ? "> " : "  ";
    std::string name = prefix + std::string(entry.Name.c_str());
    
    if (entry.IsDirectory) {
        cols.push_back(text(name + "/") | size(WIDTH, EQUAL, 40) | color(Color::Blue));
        cols.push_back(text("dir") | size(WIDTH, EQUAL, 10) | dim);
    } else if (entry.IsTopic) {
        cols.push_back(text(name) | size(WIDTH, EQUAL, 40) | color(Color::Green));
        cols.push_back(text("topic") | size(WIDTH, EQUAL, 10) | color(Color::Green));
    } else {
        cols.push_back(text(name) | size(WIDTH, EQUAL, 40) | dim);
        cols.push_back(text("other") | size(WIDTH, EQUAL, 10) | dim);
    }
    
    Element row = hbox(cols);
    
    if (selected) {
        row = row | bgcolor(Color::GrayDark);
    }
    
    return row;
}

void TTopicListView::StartAsyncLoad() {
    if (Loading_) {
        return;  // Already loading
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    
    // Capture what we need for the async operation
    TString path = App_.GetState().CurrentPath;
    auto* schemeClient = &App_.GetSchemeClient();
    
    LoadFuture_ = std::async(std::launch::async, [path, schemeClient]() -> TVector<TTopicListEntry> {
        TVector<TTopicListEntry> entries;
        
        auto result = schemeClient->ListDirectory(path).GetValueSync();
        
        if (!result.IsSuccess()) {
            throw std::runtime_error(result.GetIssues().ToString());
        }
        
        // Add parent directory if not at root
        if (path != "/") {
            TTopicListEntry parent;
            parent.Name = "..";
            parent.IsDirectory = true;
            size_t pos = path.rfind('/');
            if (pos != TString::npos && pos > 0) {
                parent.FullPath = path.substr(0, pos);
            } else {
                parent.FullPath = "/";
            }
            entries.push_back(parent);
        }
        
        for (const auto& child : result.GetChildren()) {
            TTopicListEntry entry;
            entry.Name = TString(child.Name);
            entry.FullPath = path;
            if (!entry.FullPath.EndsWith("/")) {
                entry.FullPath += "/";
            }
            entry.FullPath += child.Name;
            
            switch (child.Type) {
                case NScheme::ESchemeEntryType::Topic:
                    entry.IsTopic = true;
                    break;
                case NScheme::ESchemeEntryType::Directory:
                case NScheme::ESchemeEntryType::SubDomain:
                case NScheme::ESchemeEntryType::ColumnStore:
                case NScheme::ESchemeEntryType::ExternalDataSource:
                case NScheme::ESchemeEntryType::ExternalTable:
                case NScheme::ESchemeEntryType::View:
                    entry.IsDirectory = true;
                    break;
                default:
                    break;
            }
            
            entries.push_back(entry);
        }
        
        return entries;
    });
}

} // namespace NYdb::NConsoleClient
