#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

// Per-partition consumer stats
struct TPartitionConsumerInfo {
    ui64 PartitionId = 0;
    ui64 CommittedOffset = 0;
    ui64 EndOffset = 0;
    ui64 Lag = 0;
    TInstant LastReadTime;
    TDuration ReadTimeLag;
    TDuration CommitTimeLag;
};

class TConsumerView {
public:
    explicit TConsumerView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    void SetConsumer(const TString& topicPath, const TString& consumerName);
    
    // Callbacks
    std::function<void(ui64 partition, ui64 offset)> OnCommitOffset;
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderPartitionStats();
    ftxui::Element RenderLagGauges();
    void LoadConsumerInfo();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    TString ConsumerName_;
    
    // Partition stats (populated from DescribeConsumer)
    TVector<TPartitionConsumerInfo> PartitionStats_;
    
    // Aggregate stats
    ui64 TotalLag_ = 0;
    TDuration MaxLagTime_;
    
    int SelectedPartitionIndex_ = 0;
    bool Loading_ = false;
    TString ErrorMessage_;
};

} // namespace NYdb::NConsoleClient
