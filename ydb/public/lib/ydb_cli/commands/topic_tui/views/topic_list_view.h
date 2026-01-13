#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

// Entry from directory listing
struct TTopicListEntry {
    TString Name;
    TString FullPath;
    bool IsDirectory = false;
    bool IsTopic = false;
    
    // Topic metadata (if IsTopic)
    ui32 PartitionCount = 0;
    TDuration RetentionPeriod;
    ui64 WriteSpeedBytesPerSec = 0;
};

class TTopicListView {
public:
    explicit TTopicListView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    
    // Callbacks
    std::function<void(const TString& topicPath)> OnTopicSelected;
    std::function<void(const TString& dirPath)> OnDirectorySelected;
    std::function<void()> OnCreateTopic;
    std::function<void(const TString& topicPath)> OnEditTopic;
    std::function<void(const TString& topicPath)> OnDeleteTopic;
    
private:
    ftxui::Element RenderEntry(const TTopicListEntry& entry, bool selected);
    void LoadEntries();
    
private:
    TTopicTuiApp& App_;
    TVector<TTopicListEntry> Entries_;
    int SelectedIndex_ = 0;
    bool Loading_ = false;
    TString ErrorMessage_;
};

} // namespace NYdb::NConsoleClient
