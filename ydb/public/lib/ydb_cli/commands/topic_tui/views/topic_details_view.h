#pragma once

#include "../widgets/table.h"
#include "../widgets/theme.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <functional>
#include <future>
#include <atomic>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

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

class TTopicDetailsView {
public:
    explicit TTopicDetailsView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    void SetTopic(const TString& topicPath);
    void CheckAsyncCompletion();
    
    std::function<void(const TString& consumerName)> OnConsumerSelected;
    std::function<void()> OnAddConsumer;
    std::function<void(const TString& consumerName)> OnDropConsumer;
    std::function<void(ui32 partition)> OnPartitionSelected;
    std::function<void()> OnShowMessages;
    std::function<void()> OnWriteMessage;
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderWriteRateChart();
    void PopulatePartitionsTable();
    void PopulateConsumersTable();
    void StartAsyncLoads();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    
    // Topic basic data (partitions, header info)
    TVector<TPartitionDisplayInfo> Partitions_;
    size_t TotalPartitions_ = 0;
    TDuration RetentionPeriod_;
    ui64 WriteSpeedBytesPerSec_ = 0;
    TVector<double> WriteRateHistory_;
    
    // Consumers data (loaded separately)
    TVector<TConsumerDisplayInfo> Consumers_;
    
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
    
    // Resizable split state
    int ConsumersPanelSize_ = 40;
    
    // Info modal state
    bool ShowingInfo_ = false;
    int InfoScrollY_ = 0;
    
    // Extended topic description for info modal
    TString Owner_;
    std::vector<NTopic::ECodec> SupportedCodecs_;
    std::map<std::string, std::string> Attributes_;
    NTopic::EMeteringMode MeteringMode_ = NTopic::EMeteringMode::Unspecified;
    ui64 PartitionWriteBurstBytes_ = 0;
    ui64 RetentionStorageMb_ = 0;
    ui64 MinActivePartitions_ = 0;
    ui64 MaxActivePartitions_ = 0;
    NTopic::EAutoPartitioningStrategy AutoPartitioningStrategy_ = NTopic::EAutoPartitioningStrategy::Disabled;
    
    ftxui::Element RenderInfoModal();
};

} // namespace NYdb::NConsoleClient
