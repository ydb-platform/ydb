#pragma once

#include "view_interface.h"
#include "../widgets/table.h"
#include "../widgets/theme.h"
#include "../widgets/sparkline_history.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <future>
#include <atomic>
#include <unordered_map>

namespace NYdb::NConsoleClient {


struct TPartitionConsumerInfo {
    ui64 PartitionId = 0;
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;
    ui64 CommittedOffset = 0;
    ui64 Lag = 0;  // Uncommitted messages (EndOffset - CommittedOffset)
    ui64 UnreadMessages = 0;  // EndOffset - LastReadOffset
    TInstant LastReadTime;
    TDuration ReadTimeLag;
    TDuration WriteTimeLag;
    TDuration CommitTimeLag;
    ui64 StoreSizeBytes = 0;
    ui64 BytesWrittenPerMinute = 0;  // Write speed
    ui64 BytesReadPerMinute = 0;     // Read speed
    TString ReaderName;
    TString ReadSessionId;
    i32 PartitionNodeId = 0;
};

struct TConsumerData {
    TVector<TPartitionConsumerInfo> PartitionStats;
    ui64 TotalLag = 0;
    TDuration MaxLagTime;
};

class ITuiApp;

class TConsumerView : public ITuiView {
public:
    explicit TConsumerView(ITuiApp& app);
    ~TConsumerView();
    
    ftxui::Component Build() override;
    void Refresh() override;
    void SetConsumer(const TString& topicPath, const TString& consumerName);
    void CheckAsyncCompletion();
    
private:
    ftxui::Element RenderHeader();
    void PopulateTable();
    void StartAsyncLoad();
    void SortPartitions(int column, bool ascending);
    
private:
    ITuiApp& App_;
    TString TopicPath_;
    TString ConsumerName_;
    
    TVector<TPartitionConsumerInfo> PartitionStats_;
    ui64 TotalLag_ = 0;
    TDuration MaxLagTime_;
    
    // Use TTable instead of manual rendering
    TTable Table_;
    
    std::atomic<bool> Loading_{false};
    std::future<TConsumerData> LoadFuture_;
    TString ErrorMessage_;
    int SpinnerFrame_ = 0;
    TInstant LastRefreshTime_;  // For auto-refresh
    
    // Per-partition write rate history for sparklines
    std::unordered_map<ui64, TSparklineHistory> PartitionWriteRateHistory_;
    // Per-partition read rate history for sparklines
    std::unordered_map<ui64, TSparklineHistory> PartitionReadRateHistory_;
    
    // Stop flag for async cancellation
    std::shared_ptr<std::atomic<bool>> StopFlag_ = std::make_shared<std::atomic<bool>>(false);
};

} // namespace NYdb::NConsoleClient
