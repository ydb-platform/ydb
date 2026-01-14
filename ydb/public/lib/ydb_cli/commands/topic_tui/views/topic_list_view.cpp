#include "topic_list_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Static cache definitions
std::unordered_map<std::string, TTopicInfoResult> TTopicListView::TopicInfoCache_;
std::unordered_map<std::string, TDirInfoResult> TTopicListView::DirInfoCache_;
std::unordered_map<std::string, int> TTopicListView::CursorPositionCache_;

// Define table columns for the explorer
static TVector<TTableColumn> CreateExplorerTableColumns() {
    return {
        {"Name", -1},  // flex
        {"Type", 9},   // Was 7, need room for " ▲"
        {"Parts", 9},  // Was 7, need room for " ▲"
        {"Size", NTheme::ColBytes},
        {"Cons", 8},   // Was 6, need room for " ▲"
        {"Ret", 10},
        {"WriteSpd", NTheme::ColBytesPerMin},
        {"Codecs", 16}
    };
}

TTopicListView::TTopicListView(TTopicTuiApp& app)
    : App_(app)
    , Table_(CreateExplorerTableColumns())
{
    // Handle Enter to select item
    Table_.OnSelect = [this](int row) {
        // In search mode, row is the filtered index
        size_t entryIndex = row;
        if (SearchMode_ && row >= 0 && row < static_cast<int>(FilteredIndices_.size())) {
            entryIndex = FilteredIndices_[row];
        }
        
        if (entryIndex < Entries_.size()) {
            const auto& entry = Entries_[entryIndex];
            if (entry.IsDirectory && OnDirectorySelected) {
                TString dbRoot = App_.GetDatabaseRoot();
                if (entry.Name == ".." && entry.FullPath.size() < dbRoot.size()) {
                    // Would go above database root - ignore
                } else {
                    CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = static_cast<int>(entryIndex);
                    OnDirectorySelected(entry.FullPath);
                }
            } else if (entry.IsTopic && OnTopicSelected) {
                CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = static_cast<int>(entryIndex);
                OnTopicSelected(entry.FullPath);
            }
        }
    };
    
    // Sort callback - actually sort the entries when sort column/direction changes
    Table_.OnSortChanged = [this](int /*col*/, bool /*ascending*/) {
        SortEntries();
        if (SearchMode_) {
            ApplySearchFilter();
        } else {
            PopulateTable();
        }
    };
}

Component TTopicListView::Build() {
    // Don't use Table_.Build() - it creates a nested CatchEvent that would
    // consume events even when this view is not active. Instead, just use
    // the Renderer for display and handle events manually in CatchEvent.
    
    return Renderer([this] {
        CheckAsyncCompletion();
        
        // Only show full loading screen if we have no content yet
        if (Loading_ && Entries_.empty()) {
            return vbox({
                hbox({
                    text(" Topics in: ") | bold,
                    text(std::string(App_.GetState().CurrentPath.c_str())) | color(NTheme::AccentText)
                }),
                separator(),
                NTheme::RenderSpinner(SpinnerFrame_, "Loading...") | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + std::string(ErrorMessage_.c_str())) | color(NTheme::ErrorText) | center
            }) | border;
        }
        
        if (Entries_.empty()) {
            return vbox({
                text("Empty directory") | dim | center,
                text("Press [Backspace] to go up") | dim | center
            }) | border;
        }
        
        // Build main content
        Elements mainContent;
        mainContent.push_back(hbox({
            text(" Topics in: ") | bold,
            text(std::string(App_.GetState().CurrentPath.c_str())) | color(NTheme::AccentText),
            filler(),
            Loading_ ? (text(" ⟳ ") | color(Color::Yellow) | dim) : text("")
        }));
        mainContent.push_back(separator());
        mainContent.push_back(Table_.Render() | flex);
        
        // Add search bar at bottom if in search mode
        if (SearchMode_) {
            mainContent.push_back(separator());
            mainContent.push_back(hbox({
                text("/") | bold | color(Color::Yellow),
                text(SearchQuery_) | color(Color::White),
                text("█") | blink | color(Color::Yellow),  // Cursor
                filler(),
                text(" ") | dim,
                text(std::to_string(FilteredIndices_.size())) | color(Color::Cyan),
                text("/") | dim,
                text(std::to_string(Entries_.size())) | dim,
                text(" matches ") | dim
            }));
        }
        
        Element content = vbox(std::move(mainContent)) | border;
        
        // Overlay go-to-path popup if active
        if (GoToPathMode_) {
            // Poll for async completion results
            if (CompletionLoading_ && CompletionFuture_.valid()) {
                auto status = CompletionFuture_.wait_for(std::chrono::milliseconds(0));
                if (status == std::future_status::ready) {
                    try {
                        auto results = CompletionFuture_.get();
                        PathCompletions_.clear();
                        CompletionTypes_.clear();
                        CompletionScrollOffset_ = 0;
                        for (const auto& [path, typeLabel] : results) {
                            PathCompletions_.push_back(path);
                            CompletionTypes_[path] = typeLabel;
                        }
                        // Re-filter with current input
                        PathCompletions_ = GetPathCompletions(GoToPathInput_);
                    } catch (...) {
                        // Ignore errors
                    }
                    CompletionLoading_ = false;
                }
            }
            
            // Build completions list - show up to 10 items with scroll
            const int maxVisible = 10;
            Elements completionElements;
            
            // Calculate visible range
            if (CompletionIndex_ >= 0) {
                // Adjust scroll to keep selection visible
                if (CompletionIndex_ < CompletionScrollOffset_) {
                    CompletionScrollOffset_ = CompletionIndex_;
                } else if (CompletionIndex_ >= CompletionScrollOffset_ + maxVisible) {
                    CompletionScrollOffset_ = CompletionIndex_ - maxVisible + 1;
                }
            }
            
            for (size_t i = CompletionScrollOffset_; 
                 i < PathCompletions_.size() && i < static_cast<size_t>(CompletionScrollOffset_ + maxVisible); 
                 ++i) {
                const auto& path = PathCompletions_[i];
                
                // Get type from cache (or show ... if not loaded)
                TString typeStr;
                auto it = CompletionTypes_.find(path);
                if (it != CompletionTypes_.end()) {
                    typeStr = it->second;
                } else {
                    typeStr = "...";
                }
                
                auto elem = hbox({
                    text(" " + path) | flex,
                    text(" ") | dim,
                    text(std::string(typeStr.c_str())) | dim | size(WIDTH, EQUAL, 8)
                });
                
                if (static_cast<int>(i) == CompletionIndex_) {
                    elem = elem | bgcolor(NTheme::HighlightBg) | color(Color::White);
                }
                completionElements.push_back(elem);
            }
            
            // Show scroll indicator if needed
            TString scrollInfo;
            if (PathCompletions_.size() > static_cast<size_t>(maxVisible)) {
                scrollInfo = Sprintf(" (%d-%d of %d)", 
                    CompletionScrollOffset_ + 1,
                    std::min(CompletionScrollOffset_ + maxVisible, static_cast<int>(PathCompletions_.size())),
                    static_cast<int>(PathCompletions_.size()));
            }
            
            // Show loading indicator if completing
            Element loadingIndicator = CompletionLoading_ 
                ? (text(" ⟳") | color(Color::Yellow))
                : text("");
            
            // Determine what to show in completions area
            Element completionsArea;
            if (completionElements.empty() && CompletionLoading_) {
                completionsArea = vbox({
                    separator(),
                    text(" Loading...") | dim | color(Color::Yellow)
                });
            } else if (completionElements.empty()) {
                completionsArea = text("");
            } else {
                completionsArea = vbox({
                    separator(),
                    vbox(std::move(completionElements))
                });
            }
            
            Element popup = vbox({
                hbox({
                    text(" Go to path ") | bold,
                    loadingIndicator,
                    text(std::string(scrollInfo.c_str())) | dim
                }) | center,
                separator(),
                hbox({
                    text(" Path: "),
                    text(GoToPathInput_) | color(Color::White),
                    text("█") | blink | color(Color::Cyan)
                }),
                completionsArea
            }) | border | bgcolor(Color::GrayDark) | size(WIDTH, GREATER_THAN, 60);
            
            content = dbox({
                content,
                popup | center
            });
        }
        
        return content;
    }) | CatchEvent([this](Event event) {
        // Set focus state FIRST (before any event handling)
        // Keep focused during search mode so selection highlight remains visible
        // Only unfocus during go-to-path mode (popup is shown)
        Table_.SetFocused(App_.GetState().CurrentView == EViewType::TopicList && !GoToPathMode_);
        
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::TopicList) {
            return false;
        }
        
        // Ignore events while loading
        if (Loading_) {
            return false;
        }
        
        // === GO-TO-PATH MODE EVENT HANDLING ===
        if (GoToPathMode_) {
            if (event == Event::Escape) {
                GoToPathMode_ = false;
                App_.GetState().InputCaptureActive = false;
                GoToPathInput_.clear();
                PathCompletions_.clear();
                CompletionIndex_ = -1;
                return true;
            }
            if (event == Event::Return) {
                // Navigate to the entered path
                TString targetPath = GoToPathInput_.empty() 
                    ? App_.GetState().CurrentPath
                    : TString(GoToPathInput_.c_str());
                
                // If completion is selected, use it
                if (CompletionIndex_ >= 0 && CompletionIndex_ < static_cast<int>(PathCompletions_.size())) {
                    targetPath = TString(PathCompletions_[CompletionIndex_].c_str());
                }
                
                GoToPathMode_ = false;
                App_.GetState().InputCaptureActive = false;
                GoToPathInput_.clear();
                PathCompletions_.clear();
                CompletionIndex_ = -1;
                
                if (OnNavigateToPath && !targetPath.empty()) {
                    OnNavigateToPath(targetPath);
                }
                return true;
            }
            if (event == Event::Tab) {
                // Cycle through completions
                if (!PathCompletions_.empty()) {
                    CompletionIndex_ = (CompletionIndex_ + 1) % static_cast<int>(PathCompletions_.size());
                }
                return true;
            }
            if (event == Event::ArrowDown) {
                if (!PathCompletions_.empty()) {
                    CompletionIndex_ = std::min(CompletionIndex_ + 1, static_cast<int>(PathCompletions_.size()) - 1);
                }
                return true;
            }
            if (event == Event::ArrowUp) {
                if (CompletionIndex_ > 0) {
                    CompletionIndex_--;
                }
                return true;
            }
            if (event == Event::Backspace) {
                if (!GoToPathInput_.empty()) {
                    GoToPathInput_.pop_back();
                    PathCompletions_ = GetPathCompletions(GoToPathInput_);
                    CompletionIndex_ = -1;
                }
                return true;
            }
            if (event.is_character()) {
                GoToPathInput_ += event.character();
                PathCompletions_ = GetPathCompletions(GoToPathInput_);
                CompletionIndex_ = -1;
                return true;
            }
            return true;  // Consume all events in go-to-path mode
        }
        
        // === SEARCH MODE EVENT HANDLING ===
        if (SearchMode_) {
            if (event == Event::Escape) {
                // Exit search mode, restore full list
                SearchMode_ = false;
                App_.GetState().InputCaptureActive = false;
                SearchQuery_.clear();
                ClearSearchFilter();
                return true;
            }
            if (event == Event::Return) {
                // Select current item and exit search mode
                int tableRow = Table_.GetSelectedRow();
                if (!FilteredIndices_.empty() && tableRow >= 0 && 
                    tableRow < static_cast<int>(FilteredIndices_.size())) {
                    size_t realIndex = FilteredIndices_[tableRow];
                    if (realIndex < Entries_.size()) {
                        const auto& entry = Entries_[realIndex];
                        SearchMode_ = false;
                        App_.GetState().InputCaptureActive = false;
                        SearchQuery_.clear();
                        ClearSearchFilter();
                        
                        if (entry.IsDirectory && OnDirectorySelected) {
                            CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = static_cast<int>(realIndex);
                            OnDirectorySelected(entry.FullPath);
                        } else if (entry.IsTopic && OnTopicSelected) {
                            CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = static_cast<int>(realIndex);
                            OnTopicSelected(entry.FullPath);
                        }
                    }
                }
                return true;
            }
            if (event == Event::ArrowDown || event == Event::Character('j')) {
                int tableRows = static_cast<int>(FilteredIndices_.size());
                int currentRow = Table_.GetSelectedRow();
                if (currentRow < tableRows - 1) {
                    Table_.SetSelectedRow(currentRow + 1);
                    SearchSelectedIndex_ = currentRow + 1;
                }
                return true;
            }
            if (event == Event::ArrowUp || event == Event::Character('k')) {
                int currentRow = Table_.GetSelectedRow();
                if (currentRow > 0) {
                    Table_.SetSelectedRow(currentRow - 1);
                    SearchSelectedIndex_ = currentRow - 1;
                }
                return true;
            }
            if (event == Event::Backspace) {
                if (!SearchQuery_.empty()) {
                    SearchQuery_.pop_back();
                    ApplySearchFilter();
                }
                return true;
            }
            if (event.is_character()) {
                SearchQuery_ += event.character();
                ApplySearchFilter();
                return true;
            }
            return true;  // Consume all events in search mode
        }
        
        // === NORMAL MODE EVENT HANDLING ===
        
        // Let table handle navigation
        if (Table_.HandleEvent(event)) {
            return true;
        }
        
        // Enter search mode with '/'
        if (event == Event::Character('/')) {
            SearchMode_ = true;
            App_.GetState().InputCaptureActive = true;
            SearchQuery_.clear();
            SearchSelectedIndex_ = 0;
            FilteredIndices_ = GetFilteredIndices();  // Start with all entries
            return true;
        }
        
        // Enter go-to-path mode with 'g'
        if (event == Event::Character('g')) {
            GoToPathMode_ = true;
            App_.GetState().InputCaptureActive = true;
            GoToPathInput_ = std::string(App_.GetState().CurrentPath.c_str());
            // Add trailing slash to show completions for current directory
            if (!GoToPathInput_.empty() && GoToPathInput_.back() != '/') {
                GoToPathInput_ += '/';
            }
            PathCompletions_ = GetPathCompletions(GoToPathInput_);
            CompletionIndex_ = -1;
            return true;
        }
        
        // Toggle sort direction with 's' (TTable handles </> via OnSortChanged)
        if (event == Event::Character('s') || event == Event::Character('S')) {
            Table_.SetSort(Table_.GetSortColumn(), !Table_.IsSortAscending());
            // OnSortChanged callback will handle the actual sorting
            return true;
        }
        
        // Alternative keys for sort column navigation (,/. without shift)
        if (event == Event::Character(',')) {
            Table_.ToggleSort((Table_.GetSortColumn() + 7) % 8);  // Previous column
            return true;
        }
        if (event == Event::Character('.')) {
            Table_.ToggleSort((Table_.GetSortColumn() + 1) % 8);  // Next column
            return true;
        }
        
        // Additional key handlers (j/k for vim-style navigation)
        if (event == Event::Character('k')) {
            int row = Table_.GetSelectedRow();
            if (row > 0) {
                Table_.SetSelectedRow(row - 1);
            }
            return true;
        }
        if (event == Event::Character('j')) {
            int row = Table_.GetSelectedRow();
            if (row < static_cast<int>(Entries_.size()) - 1) {
                Table_.SetSelectedRow(row + 1);
            }
            return true;
        }
        
        if (event == Event::Escape) {
            // Go up one directory, but don't go above database root
            TString path = App_.GetState().CurrentPath;
            TString dbRoot = App_.GetDatabaseRoot();
            
            if (path == dbRoot || path == "/") {
                return true;  // Already at root, ignore
            }
            
            if (OnDirectorySelected) {
                size_t pos = path.rfind('/');
                if (pos != TString::npos && pos > 0) {
                    TString parent = path.substr(0, pos);
                    if (parent.size() >= dbRoot.size() || dbRoot.StartsWith(parent)) {
                        CursorPositionCache_[std::string(path.c_str())] = Table_.GetSelectedRow();
                        OnDirectorySelected(parent);
                    }
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
            int row = Table_.GetSelectedRow();
            if (row >= 0 && row < static_cast<int>(Entries_.size())) {
                const auto& entry = Entries_[row];
                if (entry.IsTopic && OnEditTopic) {
                    OnEditTopic(entry.FullPath);
                }
            }
            return true;
        }
        if (event == Event::Character('d') || event == Event::Character('D')) {
            int row = Table_.GetSelectedRow();
            if (row >= 0 && row < static_cast<int>(Entries_.size())) {
                const auto& entry = Entries_[row];
                if ((entry.IsTopic || entry.IsDirectory) && entry.Name != ".." && OnDeleteTopic) {
                    OnDeleteTopic(entry.FullPath);
                }
            }
            return true;
        }
        return false;
    });
}

void TTopicListView::Refresh() {
    std::string currentPath(App_.GetState().CurrentPath.c_str());
    CursorPositionCache_[currentPath] = Table_.GetSelectedRow();
    
    TopicInfoFutures_.clear();
    DirInfoFutures_.clear();
    LastRefreshTime_ = TInstant::Now();  // Track refresh time
    StartAsyncLoad();
}

void TTopicListView::CheckAsyncCompletion() {
    // Auto-refresh if enough time has passed and not already loading
    TDuration refreshRate = App_.GetRefreshRate();
    if (!Loading_ && LastRefreshTime_ != TInstant::Zero() && 
        TInstant::Now() - LastRefreshTime_ > refreshRate) {
        // Save cursor position before refresh
        std::string currentPath(App_.GetState().CurrentPath.c_str());
        CursorPositionCache_[currentPath] = Table_.GetSelectedRow();
        StartAsyncLoad();
        LastRefreshTime_ = TInstant::Now();
    }
    
    // Check directory listing
    if (Loading_ && LoadFuture_.valid()) {
        if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                Entries_ = LoadFuture_.get();
                ErrorMessage_.clear();
                // StartTopicInfoLoads applies cached values AND populates table
                // (It sorts entries using current sort column before display)
                StartTopicInfoLoads();
            } catch (const std::exception& e) {
                ErrorMessage_ = e.what();
                Entries_.clear();
            }
            Loading_ = false;
            // Restore saved cursor position (only if not in search mode)
            if (!SearchMode_) {
                std::string currentPath(App_.GetState().CurrentPath.c_str());
                auto cachedPos = CursorPositionCache_.find(currentPath);
                if (cachedPos != CursorPositionCache_.end()) {
                    Table_.SetSelectedRow(cachedPos->second);
                } else {
                    Table_.SetSelectedRow(0);
                }
            }
        } else {
            SpinnerFrame_++;
        }
    }
    
    CheckTopicInfoCompletion();
}

TString TTopicListView::FormatCodecs(const TVector<NTopic::ECodec>& codecs) {
    TString result;
    for (size_t i = 0; i < codecs.size(); ++i) {
        if (i > 0) result += ", ";
        switch (codecs[i]) {
            case NTopic::ECodec::RAW: result += "RAW"; break;
            case NTopic::ECodec::GZIP: result += "GZIP"; break;
            case NTopic::ECodec::ZSTD: result += "ZSTD"; break;
            case NTopic::ECodec::LZOP: result += "LZOP"; break;
            default: result += "?"; break;
        }
    }
    return result.empty() ? "-" : result;
}

void TTopicListView::PopulateTable() {
    Table_.SetRowCount(Entries_.size());
    
    for (size_t i = 0; i < Entries_.size(); ++i) {
        const auto& entry = Entries_[i];
        auto& row = Table_.GetRow(i);
        
        if (entry.IsDirectory) {
            // Directory row
            row.RowColor = Color::Blue;
            row.RowType = "directory";
            row.UserData = const_cast<TTopicListEntry*>(&entry);
            
            TString nameDisplay = entry.Name + "/";
            
            if (entry.Name == "..") {
                // Parent directory - minimal info
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell(""));
                }
            } else if (entry.InfoLoaded) {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                Table_.UpdateCell(i, 2, ToString(entry.ChildCount));  // Parts column shows child count
                Table_.SetCell(i, 3, TTableCell("-").WithDim());
                Table_.SetCell(i, 4, TTableCell("-").WithDim());
                Table_.SetCell(i, 5, TTableCell("-").WithDim());
                Table_.SetCell(i, 6, TTableCell("-").WithDim());
                Table_.SetCell(i, 7, TTableCell("-").WithDim());
            } else if (entry.InfoLoading) {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("...").WithDim());
                }
            } else {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("-").WithDim());
                }
            }
        } else if (entry.IsTopic) {
            // Topic row
            row.RowColor = Color::Green;
            row.RowType = "topic";
            row.UserData = const_cast<TTopicListEntry*>(&entry);
            
            Table_.SetCell(i, 0, TTableCell(entry.Name).WithColor(Color::Green));
            Table_.SetCell(i, 1, TTableCell("topic").WithColor(NTheme::SuccessText));
            
            if (entry.InfoLoaded) {
                TString retention = entry.RetentionPeriod.Hours() > 24 
                    ? ToString(entry.RetentionPeriod.Days()) + " days"
                    : ToString(entry.RetentionPeriod.Hours()) + " hours";
                TString writeSpd = FormatBytes(entry.WriteSpeedBytesPerSec) + "/s";
                TString codecs = FormatCodecs(entry.SupportedCodecs);
                
                // Use UpdateCell for values that change - enables per-cell highlighting
                Table_.UpdateCell(i, 2, ToString(entry.PartitionCount));
                Table_.UpdateCell(i, 3, FormatBytes(entry.TotalSizeBytes));
                Table_.UpdateCell(i, 4, ToString(entry.ConsumerCount));
                Table_.SetCell(i, 5, TTableCell(retention));
                Table_.SetCell(i, 6, TTableCell(writeSpd));
                Table_.SetCell(i, 7, TTableCell(codecs));
            } else if (entry.InfoLoading) {
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("...").WithDim());
                }
                row.RowDim = true;
            } else {
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("-").WithDim());
                }
            }
        } else {
            // Other entry type
            row.RowDim = true;
            row.RowType = "other";
            Table_.SetCell(i, 0, TTableCell(entry.Name).WithDim());
            Table_.SetCell(i, 1, TTableCell("other").WithDim());
            for (size_t c = 2; c < 8; ++c) {
                Table_.SetCell(i, c, TTableCell(""));
            }
        }
    }
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
    TString dbRoot = App_.GetDatabaseRoot();
    auto* schemeClient = &App_.GetSchemeClient();
    
    LoadFuture_ = std::async(std::launch::async, [path, dbRoot, schemeClient]() -> TVector<TTopicListEntry> {
        TVector<TTopicListEntry> entries;
        
        auto result = schemeClient->ListDirectory(path).GetValueSync();
        
        if (!result.IsSuccess()) {
            throw std::runtime_error(result.GetIssues().ToString());
        }
        
        // Add parent directory if not at database root
        if (path != "/" && path != dbRoot) {
            TTopicListEntry parent;
            parent.Name = "..";
            parent.IsDirectory = true;
            size_t pos = path.rfind('/');
            if (pos != TString::npos && pos > 0) {
                parent.FullPath = path.substr(0, pos);
            } else {
                parent.FullPath = "/";
            }
            if (parent.FullPath.size() >= dbRoot.size()) {
                entries.push_back(parent);
            }
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

void TTopicListView::StartTopicInfoLoads() {
    TopicInfoFutures_.clear();
    DirInfoFutures_.clear();
    
    // First, apply cached data to entries immediately
    for (auto& entry : Entries_) {
        if (entry.IsTopic && !entry.InfoLoaded) {
            std::string key(entry.FullPath.c_str());
            auto it = TopicInfoCache_.find(key);
            if (it != TopicInfoCache_.end() && it->second.Success) {
                entry.InfoLoaded = true;
                entry.PartitionCount = it->second.PartitionCount;
                entry.ConsumerCount = it->second.ConsumerCount;
                entry.TotalSizeBytes = it->second.TotalSizeBytes;
                entry.RetentionPeriod = it->second.RetentionPeriod;
                entry.WriteSpeedBytesPerSec = it->second.WriteSpeedBytesPerSec;
                entry.SupportedCodecs = it->second.SupportedCodecs;
            }
        } else if (entry.IsDirectory && !entry.InfoLoaded && entry.Name != "..") {
            std::string key(entry.FullPath.c_str());
            auto it = DirInfoCache_.find(key);
            if (it != DirInfoCache_.end() && it->second.Success) {
                entry.InfoLoaded = true;
                entry.ChildCount = it->second.ChildCount;
                entry.TopicCount = it->second.TopicCount;
            }
        }
    }
    
    // Sort entries with cached data applied, then populate table
    SortEntries();
    if (SearchMode_) {
        ApplySearchFilter();
    } else {
        PopulateTable();
    }
    
    auto* topicClient = &App_.GetTopicClient();
    auto* schemeClient = &App_.GetSchemeClient();
    
    // Start async loads (limit to avoid overwhelming)
    int loadCount = 0;
    const int maxConcurrent = 10;
    
    for (auto& entry : Entries_) {
        if (loadCount >= maxConcurrent) break;
        
        if (entry.IsTopic) {
            entry.InfoLoading = true;
            TString topicPath = entry.FullPath;
            
            TopicInfoFutures_[std::string(topicPath.c_str())] = std::async(std::launch::async, 
                [topicPath, topicClient]() -> TTopicInfoResult {
                    TTopicInfoResult result;
                    result.TopicPath = topicPath;
                    
                    auto descResult = topicClient->DescribeTopic(topicPath, 
                        NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
                    
                    if (descResult.IsSuccess()) {
                        const auto& desc = descResult.GetTopicDescription();
                        result.Success = true;
                        result.PartitionCount = desc.GetPartitions().size();
                        result.ConsumerCount = desc.GetConsumers().size();
                        result.RetentionPeriod = desc.GetRetentionPeriod();
                        result.WriteSpeedBytesPerSec = desc.GetPartitionWriteSpeedBytesPerSecond();
                        result.WriteBurstBytes = desc.GetPartitionWriteBurstBytes();
                        
                        for (const auto& codec : desc.GetSupportedCodecs()) {
                            result.SupportedCodecs.push_back(codec);
                        }
                        
                        for (const auto& p : desc.GetPartitions()) {
                            if (p.GetPartitionStats()) {
                                result.TotalSizeBytes += p.GetPartitionStats()->GetStoreSizeBytes();
                                result.BytesWrittenPerMinute += p.GetPartitionStats()->GetBytesWrittenPerMinute();
                                if (p.GetPartitionStats()->GetMaxWriteTimeLag() > result.MaxWriteTimeLag) {
                                    result.MaxWriteTimeLag = p.GetPartitionStats()->GetMaxWriteTimeLag();
                                }
                            }
                        }
                    }
                    
                    return result;
                });
            
            loadCount++;
        } else if (entry.IsDirectory && entry.Name != "..") {
            entry.InfoLoading = true;
            TString dirPath = entry.FullPath;
            
            DirInfoFutures_[std::string(dirPath.c_str())] = std::async(std::launch::async,
                [dirPath, schemeClient]() -> TDirInfoResult {
                    TDirInfoResult result;
                    result.DirPath = dirPath;
                    
                    auto listResult = schemeClient->ListDirectory(dirPath).GetValueSync();
                    
                    if (listResult.IsSuccess()) {
                        result.Success = true;
                        for (const auto& child : listResult.GetChildren()) {
                            result.ChildCount++;
                            if (child.Type == NScheme::ESchemeEntryType::Topic) {
                                result.TopicCount++;
                            }
                        }
                    }
                    
                    return result;
                });
            
            loadCount++;
        }
    }
}

void TTopicListView::CheckTopicInfoCompletion() {
    std::vector<std::string> completed;
    
    // Check topic info futures
    for (auto& [path, future] : TopicInfoFutures_) {
        if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto result = future.get();
                
                // Find and update the entry
                for (size_t i = 0; i < Entries_.size(); ++i) {
                    auto& entry = Entries_[i];
                    if (std::string(entry.FullPath.c_str()) == path) {
                        entry.InfoLoading = false;
                        if (result.Success) {
                            entry.InfoLoaded = true;
                            entry.PartitionCount = result.PartitionCount;
                            entry.ConsumerCount = result.ConsumerCount;
                            entry.TotalSizeBytes = result.TotalSizeBytes;
                            entry.RetentionPeriod = result.RetentionPeriod;
                            entry.WriteSpeedBytesPerSec = result.WriteSpeedBytesPerSec;
                            entry.WriteBurstBytes = result.WriteBurstBytes;
                            entry.BytesWrittenPerMinute = result.BytesWrittenPerMinute;
                            entry.MaxWriteTimeLag = result.MaxWriteTimeLag;
                            entry.SupportedCodecs = result.SupportedCodecs;
                            
                            // Find the correct table row to update
                            size_t tableRow = i;  // Default: entry index == table row
                            if (SearchMode_) {
                                // In search mode, find this entry's position in FilteredIndices_
                                bool found = false;
                                for (size_t fi = 0; fi < FilteredIndices_.size(); ++fi) {
                                    if (FilteredIndices_[fi] == i) {
                                        tableRow = fi;
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    // Entry not visible in filtered view, skip table update
                                    break;
                                }
                            }
                            
                            // Update table cells using UpdateCell for change tracking
                            TString retention = entry.RetentionPeriod.Hours() > 24 
                                ? ToString(entry.RetentionPeriod.Days()) + " days"
                                : ToString(entry.RetentionPeriod.Hours()) + " hours";
                            TString writeSpd = FormatBytes(entry.WriteSpeedBytesPerSec) + "/s";
                            TString codecs = FormatCodecs(entry.SupportedCodecs);
                            
                            Table_.UpdateCell(tableRow, 2, ToString(entry.PartitionCount));
                            Table_.UpdateCell(tableRow, 3, FormatBytes(entry.TotalSizeBytes));
                            Table_.UpdateCell(tableRow, 4, ToString(entry.ConsumerCount));
                            Table_.SetCell(tableRow, 5, TTableCell(retention));
                            Table_.SetCell(tableRow, 6, TTableCell(writeSpd));
                            Table_.SetCell(tableRow, 7, TTableCell(codecs));
                            
                            // Clear dim styling now that data is loaded
                            Table_.GetRow(tableRow).RowDim = false;
                        }
                        break;
                    }
                }
                
                // Store in cache
                if (result.Success) {
                    TopicInfoCache_[path] = result;
                }
            } catch (...) {
                for (auto& entry : Entries_) {
                    if (std::string(entry.FullPath.c_str()) == path) {
                        entry.InfoLoading = false;
                        break;
                    }
                }
            }
            completed.push_back(path);
        }
    }
    
    for (const auto& path : completed) {
        TopicInfoFutures_.erase(path);
    }
    
    // Check directory info futures
    completed.clear();
    for (auto& [path, future] : DirInfoFutures_) {
        if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto result = future.get();
                
                for (size_t i = 0; i < Entries_.size(); ++i) {
                    auto& entry = Entries_[i];
                    if (std::string(entry.FullPath.c_str()) == path) {
                        entry.InfoLoading = false;
                        if (result.Success) {
                            entry.InfoLoaded = true;
                            entry.ChildCount = result.ChildCount;
                            entry.TopicCount = result.TopicCount;
                            
                            // Find the correct table row to update
                            size_t tableRow = i;
                            if (SearchMode_) {
                                bool found = false;
                                for (size_t fi = 0; fi < FilteredIndices_.size(); ++fi) {
                                    if (FilteredIndices_[fi] == i) {
                                        tableRow = fi;
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    break;  // Entry not visible
                                }
                            }
                            
                            // Update child count with change tracking
                            Table_.UpdateCell(tableRow, 2, ToString(entry.ChildCount));
                        }
                        break;
                    }
                }
                
                if (result.Success) {
                    DirInfoCache_[path] = result;
                }
            } catch (...) {
                for (auto& entry : Entries_) {
                    if (std::string(entry.FullPath.c_str()) == path) {
                        entry.InfoLoading = false;
                        break;
                    }
                }
            }
            completed.push_back(path);
        }
    }
    
    for (const auto& path : completed) {
        DirInfoFutures_.erase(path);
    }
    
    // If we completed any info loads and are sorting by a data-dependent column,
    // we need to re-sort and repopulate the table
    bool anyCompleted = !TopicInfoFutures_.empty() || !DirInfoFutures_.empty();  // Still have pending
    int sortCol = Table_.GetSortColumn();
    bool needsResort = (sortCol >= 2 && sortCol <= 6);  // Parts, Size, Cons, Ret, WriteSpd
    
    // Only re-sort if we just completed some loads AND all loads are now done
    if (!anyCompleted && needsResort && !Entries_.empty()) {
        // Check if we actually loaded any info this time (compare before/after)
        // For simplicity, always re-sort if all futures are done and sorting by data column
        SortEntries();
        if (SearchMode_) {
            ApplySearchFilter();
        } else {
            PopulateTable();
        }
    }
}

// === Search mode helpers ===

TVector<size_t> TTopicListView::GetFilteredIndices() const {
    TVector<size_t> result;
    
    if (SearchQuery_.empty()) {
        // Return all indices
        for (size_t i = 0; i < Entries_.size(); ++i) {
            result.push_back(i);
        }
        return result;
    }
    
    // Case-insensitive substring search
    std::string queryLower = SearchQuery_;
    for (auto& c : queryLower) {
        c = std::tolower(static_cast<unsigned char>(c));
    }
    
    for (size_t i = 0; i < Entries_.size(); ++i) {
        std::string nameLower = std::string(Entries_[i].Name.c_str());
        for (auto& c : nameLower) {
            c = std::tolower(static_cast<unsigned char>(c));
        }
        
        if (nameLower.find(queryLower) != std::string::npos) {
            result.push_back(i);
        }
    }
    
    return result;
}

void TTopicListView::ApplySearchFilter() {
    // Preserve current selection if valid
    int previousRow = Table_.GetSelectedRow();
    
    FilteredIndices_ = GetFilteredIndices();
    
    // Repopulate table with only filtered entries
    Table_.SetRowCount(FilteredIndices_.size());
    
    for (size_t i = 0; i < FilteredIndices_.size(); ++i) {
        size_t entryIdx = FilteredIndices_[i];
        const auto& entry = Entries_[entryIdx];
        auto& row = Table_.GetRow(i);
        
        if (entry.IsDirectory) {
            row.RowColor = Color::Blue;
            row.RowType = "directory";
            row.UserData = const_cast<TTopicListEntry*>(&entry);
            
            TString nameDisplay = entry.Name + "/";
            
            if (entry.Name == "..") {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell(""));
                }
            } else if (entry.InfoLoaded) {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                Table_.SetCell(i, 2, TTableCell(ToString(entry.ChildCount)));
                for (size_t c = 3; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("-").WithDim());
                }
            } else {
                Table_.SetCell(i, 0, TTableCell(nameDisplay).WithColor(Color::Blue));
                Table_.SetCell(i, 1, TTableCell("dir").WithDim());
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("-").WithDim());
                }
            }
        } else if (entry.IsTopic) {
            row.RowColor = Color::Green;
            row.RowType = "topic";
            row.UserData = const_cast<TTopicListEntry*>(&entry);
            
            Table_.SetCell(i, 0, TTableCell(entry.Name).WithColor(Color::Green));
            Table_.SetCell(i, 1, TTableCell("topic").WithColor(NTheme::SuccessText));
            
            if (entry.InfoLoaded) {
                TString retention = entry.RetentionPeriod.Hours() > 24 
                    ? ToString(entry.RetentionPeriod.Days()) + " days"
                    : ToString(entry.RetentionPeriod.Hours()) + " hours";
                TString writeSpd = FormatBytes(entry.WriteSpeedBytesPerSec) + "/s";
                TString codecs = FormatCodecs(entry.SupportedCodecs);
                
                Table_.SetCell(i, 2, TTableCell(ToString(entry.PartitionCount)));
                Table_.SetCell(i, 3, TTableCell(FormatBytes(entry.TotalSizeBytes)));
                Table_.SetCell(i, 4, TTableCell(ToString(entry.ConsumerCount)));
                Table_.SetCell(i, 5, TTableCell(retention));
                Table_.SetCell(i, 6, TTableCell(writeSpd));
                Table_.SetCell(i, 7, TTableCell(codecs));
            } else {
                for (size_t c = 2; c < 8; ++c) {
                    Table_.SetCell(i, c, TTableCell("-").WithDim());
                }
            }
        } else {
            row.RowDim = true;
            row.RowType = "other";
            Table_.SetCell(i, 0, TTableCell(entry.Name).WithDim());
            Table_.SetCell(i, 1, TTableCell("other").WithDim());
            for (size_t c = 2; c < 8; ++c) {
                Table_.SetCell(i, c, TTableCell(""));
            }
        }
    }
    
    // Preserve cursor position if still valid, otherwise reset to 0
    if (FilteredIndices_.empty()) {
        SearchSelectedIndex_ = 0;
    } else if (previousRow >= 0 && previousRow < static_cast<int>(FilteredIndices_.size())) {
        Table_.SetSelectedRow(previousRow);
        SearchSelectedIndex_ = previousRow;
    } else {
        // Previous row is now out of bounds - go to last valid row
        int lastRow = static_cast<int>(FilteredIndices_.size()) - 1;
        Table_.SetSelectedRow(lastRow);
        SearchSelectedIndex_ = lastRow;
    }
}

void TTopicListView::ClearSearchFilter() {
    FilteredIndices_.clear();
    SearchSelectedIndex_ = 0;
    // Restore full table
    PopulateTable();
}

// === Go-to-path helpers ===

TVector<std::string> TTopicListView::GetPathCompletions(const std::string& prefix) {
    // First, check if we have a pending async result
    if (CompletionLoading_ && CompletionFuture_.valid()) {
        auto status = CompletionFuture_.wait_for(std::chrono::milliseconds(0));
        if (status == std::future_status::ready) {
            try {
                auto results = CompletionFuture_.get();
                PathCompletions_.clear();
                CompletionTypes_.clear();
                CompletionScrollOffset_ = 0;
                for (const auto& [path, typeLabel] : results) {
                    PathCompletions_.push_back(path);
                    CompletionTypes_[path] = typeLabel;
                }
            } catch (...) {
                // Ignore errors
            }
            CompletionLoading_ = false;
        }
    }
    
    if (prefix.empty()) {
        PathCompletions_.clear();
        CompletionTypes_.clear();
        CachedCompletionDir_.clear();
        return PathCompletions_;
    }
    
    // Determine parent directory and prefix part
    std::string parentDir;
    std::string namePrefix;
    
    size_t lastSlash = prefix.rfind('/');
    if (lastSlash == std::string::npos) {
        // No slash - use current directory
        parentDir = std::string(App_.GetState().CurrentPath.c_str());
        namePrefix = prefix;
    } else if (lastSlash == 0) {
        // Slash at start - root directory
        parentDir = "/";
        namePrefix = prefix.substr(1);
    } else {
        parentDir = prefix.substr(0, lastSlash);
        namePrefix = prefix.substr(lastSlash + 1);
    }
    
    // Only fetch if parent directory changed
    if (parentDir != CachedCompletionDir_ && !CompletionLoading_) {
        CachedCompletionDir_ = parentDir;
        CompletionLoading_ = true;
        // Clear stale completions - they're from a different directory
        PathCompletions_.clear();
        CompletionTypes_.clear();
        
        // Start async load
        auto& schemeClient = App_.GetSchemeClient();
        CompletionFuture_ = std::async(std::launch::async, [&schemeClient, parentDir]() {
            TVector<std::pair<std::string, TString>> results;
            
            auto result = schemeClient.ListDirectory(TString(parentDir.c_str())).GetValueSync();
            if (!result.IsSuccess()) {
                return results;
            }
            
            for (const auto& child : result.GetChildren()) {
                std::string childName = std::string(child.Name.c_str());
                std::string fullPath = parentDir;
                if (!fullPath.empty() && fullPath.back() != '/') {
                    fullPath += "/";
                }
                fullPath += childName;
                
                // Add trailing slash for directories
                bool isDir = child.Type == NScheme::ESchemeEntryType::Directory ||
                             child.Type == NScheme::ESchemeEntryType::SubDomain;
                if (isDir) {
                    fullPath += "/";
                }
                
                // Determine type label
                TString typeLabel;
                switch (child.Type) {
                    case NScheme::ESchemeEntryType::Topic:
                    case NScheme::ESchemeEntryType::PqGroup:
                        typeLabel = "Topic";
                        break;
                    case NScheme::ESchemeEntryType::Directory:
                    case NScheme::ESchemeEntryType::SubDomain:
                        typeLabel = "Dir";
                        break;
                    case NScheme::ESchemeEntryType::Table:
                        typeLabel = "Table";
                        break;
                    case NScheme::ESchemeEntryType::ColumnTable:
                        typeLabel = "ColTable";
                        break;
                    default:
                        typeLabel = "";
                        break;
                }
                
                results.push_back({fullPath, typeLabel});
                
                // Limit results
                if (results.size() >= 100) {
                    break;
                }
            }
            
            return results;
        });
    }
    
    // Filter current completions by name prefix (case-insensitive)
    std::string namePrefixLower = namePrefix;
    for (auto& c : namePrefixLower) {
        c = std::tolower(static_cast<unsigned char>(c));
    }
    
    TVector<std::string> filtered;
    for (const auto& path : PathCompletions_) {
        // Extract name part from path
        size_t slash = path.rfind('/');
        if (slash != std::string::npos && slash > 0) {
            // Check if there's a trailing slash (for directories)
            size_t nameEnd = path.length();
            if (path.back() == '/') {
                nameEnd--;
                slash = path.rfind('/', nameEnd - 1);
                if (slash == std::string::npos) slash = 0;
            }
            std::string name = path.substr(slash + 1, nameEnd - slash - 1);
            std::string nameLower = name;
            for (auto& c : nameLower) {
                c = std::tolower(static_cast<unsigned char>(c));
            }
            if (namePrefixLower.empty() || nameLower.find(namePrefixLower) == 0) {
                filtered.push_back(path);
                if (filtered.size() >= 50) break;
            }
        }
    }
    
    return filtered;
}

// === Sorting helpers ===

void TTopicListView::SortEntries() {
    int sortColumn = Table_.GetSortColumn();
    bool sortAscending = Table_.IsSortAscending();
    
    // Always put ".." at the top, then sort directories first, then topics
    std::stable_sort(Entries_.begin(), Entries_.end(), 
        [sortColumn, sortAscending](const TTopicListEntry& a, const TTopicListEntry& b) {
            // ".." always comes first
            if (a.Name == "..") return true;
            if (b.Name == "..") return false;
            
            // Directories before topics
            if (a.IsDirectory && !b.IsDirectory) return true;
            if (!a.IsDirectory && b.IsDirectory) return false;
            
            // Same type - sort by selected column
            int cmp = 0;
            switch (sortColumn) {
                case 0: // Name
                    cmp = a.Name.compare(b.Name);
                    break;
                case 1: // Type (dir vs topic)
                    cmp = (a.IsTopic && !b.IsTopic) ? 1 : (!a.IsTopic && b.IsTopic) ? -1 : 0;
                    break;
                case 2: // Parts (partition count or child count)
                    {
                        ui32 aVal = a.IsTopic ? a.PartitionCount : a.ChildCount;
                        ui32 bVal = b.IsTopic ? b.PartitionCount : b.ChildCount;
                        cmp = (aVal < bVal) ? -1 : (aVal > bVal) ? 1 : 0;
                    }
                    break;
                case 3: // Size
                    cmp = (a.TotalSizeBytes < b.TotalSizeBytes) ? -1 : (a.TotalSizeBytes > b.TotalSizeBytes) ? 1 : 0;
                    break;
                case 4: // Consumers
                    cmp = (a.ConsumerCount < b.ConsumerCount) ? -1 : (a.ConsumerCount > b.ConsumerCount) ? 1 : 0;
                    break;
                case 5: // Retention
                    cmp = (a.RetentionPeriod < b.RetentionPeriod) ? -1 : (a.RetentionPeriod > b.RetentionPeriod) ? 1 : 0;
                    break;
                case 6: // Write speed
                    cmp = (a.WriteSpeedBytesPerSec < b.WriteSpeedBytesPerSec) ? -1 : (a.WriteSpeedBytesPerSec > b.WriteSpeedBytesPerSec) ? 1 : 0;
                    break;
                case 7: // Codecs (just by name for simplicity)
                    cmp = a.Name.compare(b.Name);
                    break;
                default:
                    cmp = a.Name.compare(b.Name);
                    break;
            }
            return sortAscending ? (cmp < 0) : (cmp > 0);
        });
}

TString TTopicListView::GetSortColumnName() const {
    static const char* names[] = {"Name", "Type", "Parts", "Size", "Cons", "Ret", "WriteSpd", "Codecs"};
    int col = Table_.GetSortColumn();
    if (col >= 0 && col < 8) {
        return TString(names[col]) + (Table_.IsSortAscending() ? " ↑" : " ↓");
    }
    return "Name";
}

} // namespace NYdb::NConsoleClient



