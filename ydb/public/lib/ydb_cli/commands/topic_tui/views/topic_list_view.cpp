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
        {"Type", 7},
        {"Parts", 7},
        {"Size", NTheme::ColBytes},
        {"Cons", 6},
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
        if (row >= 0 && row < static_cast<int>(Entries_.size())) {
            const auto& entry = Entries_[row];
            if (entry.IsDirectory && OnDirectorySelected) {
                TString dbRoot = App_.GetDatabaseRoot();
                if (entry.Name == ".." && entry.FullPath.size() < dbRoot.size()) {
                    // Would go above database root - ignore
                } else {
                    CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = row;
                    OnDirectorySelected(entry.FullPath);
                }
            } else if (entry.IsTopic && OnTopicSelected) {
                CursorPositionCache_[std::string(App_.GetState().CurrentPath.c_str())] = row;
                OnTopicSelected(entry.FullPath);
            }
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
        
        return vbox({
            hbox({
                text(" Topics in: ") | bold,
                text(std::string(App_.GetState().CurrentPath.c_str())) | color(NTheme::AccentText),
                filler(),
                Loading_ ? (text(" ⟳ ") | color(Color::Yellow) | dim) : text("")
            }),
            separator(),
            Table_.Render() | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Set focus state FIRST (before any event handling)
        Table_.SetFocused(App_.GetState().CurrentView == EViewType::TopicList);
        
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::TopicList) {
            return false;
        }
        
        // Ignore events while loading
        if (Loading_) {
            return false;
        }
        
        // Let table handle navigation
        if (Table_.HandleEvent(event)) {
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
                PopulateTable();
                StartTopicInfoLoads();
            } catch (const std::exception& e) {
                ErrorMessage_ = e.what();
                Entries_.clear();
            }
            Loading_ = false;
            // Restore saved cursor position
            std::string currentPath(App_.GetState().CurrentPath.c_str());
            auto cachedPos = CursorPositionCache_.find(currentPath);
            if (cachedPos != CursorPositionCache_.end()) {
                Table_.SetSelectedRow(cachedPos->second);
            } else {
                Table_.SetSelectedRow(0);
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
    
    // Re-populate table with cached data
    PopulateTable();
    
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
                            
                            // Update table cells using UpdateCell for change tracking
                            TString retention = entry.RetentionPeriod.Hours() > 24 
                                ? ToString(entry.RetentionPeriod.Days()) + " days"
                                : ToString(entry.RetentionPeriod.Hours()) + " hours";
                            TString writeSpd = FormatBytes(entry.WriteSpeedBytesPerSec) + "/s";
                            TString codecs = FormatCodecs(entry.SupportedCodecs);
                            
                            Table_.UpdateCell(i, 2, ToString(entry.PartitionCount));
                            Table_.UpdateCell(i, 3, FormatBytes(entry.TotalSizeBytes));
                            Table_.UpdateCell(i, 4, ToString(entry.ConsumerCount));
                            Table_.SetCell(i, 5, TTableCell(retention));
                            Table_.SetCell(i, 6, TTableCell(writeSpd));
                            Table_.SetCell(i, 7, TTableCell(codecs));
                            
                            // Clear dim styling now that data is loaded
                            Table_.GetRow(i).RowDim = false;
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
                            
                            // Update child count with change tracking
                            Table_.UpdateCell(i, 2, ToString(entry.ChildCount));
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
}

} // namespace NYdb::NConsoleClient
