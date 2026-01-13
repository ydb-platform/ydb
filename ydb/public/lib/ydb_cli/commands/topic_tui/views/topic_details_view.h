#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

// Partition info for display
struct TPartitionDisplayInfo {
    ui64 PartitionId = 0;
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;
    ui64 StoreSizeBytes = 0;
    TDuration WriteTimeLag;
    TInstant LastWriteTime;
};

// Consumer summary for display
struct TConsumerDisplayInfo {
    TString Name;
    ui64 TotalLag = 0;
    TDuration MaxLagTime;
    bool IsImportant = false;
};

class TTopicDetailsView {
public:
    explicit TTopicDetailsView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    void SetTopic(const TString& topicPath);
    
    // Callbacks
    std::function<void(const TString& consumerName)> OnConsumerSelected;
    std::function<void()> OnAddConsumer;
    std::function<void(const TString& consumerName)> OnDropConsumer;
    std::function<void(ui32 partition)> OnPartitionSelected;
    std::function<void()> OnShowMessages;
    std::function<void()> OnWriteMessage;
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderPartitionsTable();
    ftxui::Element RenderConsumersList();
    ftxui::Element RenderWriteRateChart();
    void LoadTopicInfo();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    
    // Extracted display data (we don't cache the full description)
    TVector<TPartitionDisplayInfo> Partitions_;
    TVector<TConsumerDisplayInfo> Consumers_;
    
    // Cached topic metadata for header rendering
    size_t TotalPartitions_ = 0;
    TDuration RetentionPeriod_;
    ui64 WriteSpeedBytesPerSec_ = 0;
    
    // Write rate history for sparkline
    TVector<double> WriteRateHistory_;
    
    // Selection state
    int SelectedPartitionIndex_ = 0;
    int SelectedConsumerIndex_ = 0;
    int FocusPanel_ = 0;  // 0=partitions, 1=consumers
    
    bool Loading_ = false;
    TString ErrorMessage_;
};

} // namespace NYdb::NConsoleClient
