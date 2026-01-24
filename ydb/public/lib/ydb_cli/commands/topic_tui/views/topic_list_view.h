#pragma once

#include "view_interface.h"
#include "../widgets/table.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <memory>
#include <future>
#include <atomic>
#include <unordered_map>

namespace NYdb::NConsoleClient {


// Entry from directory listing
struct TTopicListEntry {
    TString Name;
    TString FullPath;
    bool IsDirectory = false;
    bool IsTopic = false;
    
    // Async loading state (for both topics and directories)
    bool InfoLoaded = false;
    bool InfoLoading = false;
    
    // Topic metadata (if IsTopic)
    ui32 PartitionCount = 0;
    ui32 ConsumerCount = 0;
    ui64 TotalSizeBytes = 0;
    TDuration RetentionPeriod;
    ui64 WriteSpeedBytesPerSec = 0;
    ui64 WriteBurstBytes = 0;
    ui64 BytesWrittenPerMinute = 0;
    TDuration MaxWriteTimeLag;
    TVector<NTopic::ECodec> SupportedCodecs;
    
    // Directory metadata (if IsDirectory)
    ui32 ChildCount = 0;  // Number of items in directory
    ui32 TopicCount = 0;  // Number of topics in directory
};

// Result of async topic info load
struct TTopicInfoResult {
    TString TopicPath;
    bool Success = false;
    ui32 PartitionCount = 0;
    ui32 ConsumerCount = 0;
    ui64 TotalSizeBytes = 0;
    TDuration RetentionPeriod;
    ui64 WriteSpeedBytesPerSec = 0;
    ui64 WriteBurstBytes = 0;
    ui64 BytesWrittenPerMinute = 0;
    TDuration MaxWriteTimeLag;
    TVector<NTopic::ECodec> SupportedCodecs;
};

// Result of async directory info load
struct TDirInfoResult {
    TString DirPath;
    bool Success = false;
    ui32 ChildCount = 0;
    ui32 TopicCount = 0;
};

class ITuiApp;

class TTopicListView : public ITuiView {
public:
    explicit TTopicListView(ITuiApp& app);
    ~TTopicListView() override;
    
    ftxui::Component Build() override;
    void Refresh() override;
    void CheckAsyncCompletion();  // Called from render to check if async op finished
    TString GetSortDirectionArrow() const;
    
    // Testable helper for glob patching
    static bool GlobMatch(const char* pattern, const char* text);
    
private:
    void PopulateTable();
    void StartAsyncLoad();
    void StartTopicInfoLoads();  // Start async loads for visible topics
    void CheckTopicInfoCompletion();  // Check if any topic info loads finished
    TString FormatCodecs(const TVector<NTopic::ECodec>& codecs);
    
    // Search mode helpers
    TVector<size_t> GetFilteredIndices() const;  // Returns indices of entries matching search
    void ApplySearchFilter();  // Update table to show filtered entries
    void ClearSearchFilter();  // Restore full entry list in table
    
    // Go-to-path helpers
    TVector<std::string> GetPathCompletions(const std::string& prefix);  // Autocomplete suggestions
    

    // Sorting helpers
    void SortEntries();  // Sort entries by current sort column (uses Table_'s sort state)
    TString GetSortColumnName() const;  // Get current sort column name for display
    
private:
    ITuiApp& App_;
    TVector<TTopicListEntry> Entries_;
    
    // Use TTable for the file/topic list
    TTable Table_;
    
    // Async state for directory listing
    std::atomic<bool> Loading_{false};
    std::future<TVector<TTopicListEntry>> LoadFuture_;
    TString ErrorMessage_;
    int SpinnerFrame_ = 0;
    TInstant LastRefreshTime_;  // For auto-refresh
    
    // Async state for topic info
    std::unordered_map<std::string, std::future<TTopicInfoResult>> TopicInfoFutures_;
    
    // Async state for directory info
    std::unordered_map<std::string, std::future<TDirInfoResult>> DirInfoFutures_;
    
    // Static cache for topic and directory info (persists across navigation)
    static std::unordered_map<std::string, TTopicInfoResult> TopicInfoCache_;
    static std::unordered_map<std::string, TDirInfoResult> DirInfoCache_;
    
    // Static cache for cursor positions per path (persists across navigation)
    static std::unordered_map<std::string, int> CursorPositionCache_;
    
    // Search mode state (vim-style '/' search)
    bool SearchMode_ = false;
    std::string SearchQuery_;
    TVector<size_t> FilteredIndices_;  // Indices into Entries_ that match search
    int SearchSelectedIndex_ = 0;  // Index into FilteredIndices_
    
    // Go-to-path mode state ('g' key)
    bool GoToPathMode_ = false;
    std::string GoToPathInput_;
    TVector<std::string> PathCompletions_;  // Autocomplete suggestions
    int CompletionIndex_ = -1;  // Currently selected completion (-1 = none)
    int CompletionScrollOffset_ = 0;  // For scrolling completions
    std::unordered_map<std::string, TString> CompletionTypes_;  // Path -> type (Topic, Dir, etc.)
    
    // Async completion loading
    std::string CachedCompletionDir_;  // Last directory we fetched completions for
    std::future<TVector<std::pair<std::string, TString>>> CompletionFuture_;  // Async load of completions
    bool CompletionLoading_ = false;
    
    // Note: Sorting state is managed by Table_ (GetSortColumn(), IsSortAscending(), SetSort())
    
    // Stop flag to cancel async operations on destruction
    std::shared_ptr<std::atomic<bool>> StopFlag_ = std::make_shared<std::atomic<bool>>(false);
};

} // namespace NYdb::NConsoleClient
