#pragma once

#include "view_interface.h"
#include "../widgets/table.h"
#include "../widgets/theme.h"
#include "../widgets/sparkline_history.h"
#include "../http_client.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <future>
#include <atomic>
#include <unordered_map>

namespace NYdb::NConsoleClient {


struct TPartitionDisplayInfo {
    ui64 PartitionId = 0;
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;
    ui64 StoreSizeBytes = 0;
    TDuration WriteTimeLag;
    TInstant LastWriteTime;
    ui64 BytesWrittenPerMinute = 0;
    i32 NodeId = 0;  // Partition host node
};

struct TConsumerDisplayInfo {
    TString Name;
    ui64 TotalLag = 0;
    TDuration MaxLagTime;
    bool IsImportant = false;
};


// Separate data structs for independent async loads
struct TTopicBasicData {
    size_t TotalPartitions = 0;
    TDuration RetentionPeriod;
    ui64 WriteSpeedBytesPerSec = 0;
    double WriteRateBytesPerSec = 0.0;
    TVector<TPartitionDisplayInfo> Partitions;
    
    // Additional stats
    ui64 TotalSizeBytes = 0;
    ui64 WriteBurstBytes = 0;
    TDuration MaxWriteTimeLag;
    size_t ConsumerCount = 0;
    TVector<NTopic::ECodec> SupportedCodecs;
    
    // Extended info for info modal
    TString Owner;
    std::map<std::string, std::string> Attributes;
    NTopic::EMeteringMode MeteringMode = NTopic::EMeteringMode::Unspecified;
    ui64 PartitionWriteBurstBytes = 0;
    ui64 RetentionStorageMb = 0;
    ui64 MinActivePartitions = 0;
    ui64 MaxActivePartitions = 0;
    NTopic::EAutoPartitioningStrategy AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Disabled;
};

struct TConsumersData {
    TVector<TConsumerDisplayInfo> Consumers;
};

class ITuiApp;

class TTopicDetailsView : public ITuiView {
public:
    explicit TTopicDetailsView(ITuiApp& app);
    ~TTopicDetailsView() override;
    
    ftxui::Component Build() override;
    void Refresh() override;
    void SetTopic(const TString& topicPath);
    void CheckAsyncCompletion();
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderWriteRateChart();
    void PopulatePartitionsTable();
    void PopulateConsumersTable();
    void StartAsyncLoads();
    void SortPartitions(int column, bool ascending);
    void SortConsumers(int column, bool ascending);
    
private:
    ITuiApp& App_;
    TString TopicPath_;
    
    // Topic basic data (partitions, header info)
    TVector<TPartitionDisplayInfo> Partitions_;
    size_t TotalPartitions_ = 0;
    TDuration RetentionPeriod_;
    ui64 WriteSpeedBytesPerSec_ = 0;
    TSparklineHistory TopicWriteRateHistory_;  // Fixed 5-second interval
    std::unordered_map<ui64, TSparklineHistory> PartitionWriteRateHistory_;  // Per-partition
    TVector<NTopic::ECodec> SupportedCodecs_;  // Topic codecs for header
    
    // Consumers data (loaded separately)
    TVector<TConsumerDisplayInfo> Consumers_;
    bool ConsumersLoadedOnce_ = false;  // Track if we've completed first load
    
    // Use TTable for both tables
    TTable PartitionsTable_;
    TTable ConsumersTable_;
    int FocusPanel_ = 0;  // 0 = partitions, 1 = consumers
    
    // Separate async state for each section
    std::atomic<bool> LoadingTopic_{false};
    std::atomic<bool> LoadingConsumers_{false};
    std::future<TTopicBasicData> TopicFuture_;
    std::future<TConsumersData> ConsumersFuture_;
    TString TopicError_;
    TString ConsumersError_;
    int SpinnerFrame_ = 0;
    TInstant LastRefreshTime_;  // For auto-refresh
    
    // Resizable split state
    int ConsumersPanelSize_ = 40;
    
    // Info view state (Viewer API based)
    int InfoScrollY_ = 0;
    std::atomic<bool> LoadingInfo_{false};
    std::future<TTopicDescribeResult> InfoFuture_;
    TTopicDescribeResult TopicDescribeResult_;  // From Viewer API
    
    // Tablets view state
    int TabletsScrollY_ = 0;
    TVector<TTabletInfo> Tablets_;
    std::atomic<bool> LoadingTablets_{false};
    std::future<TVector<TTabletInfo>> TabletsFuture_;
    
    // Cancellation support for async operations
    std::shared_ptr<std::atomic<bool>> StopFlag_;
    
    ftxui::Element RenderInfoView();
    ftxui::Element RenderTabletsView();
};

} // namespace NYdb::NConsoleClient
