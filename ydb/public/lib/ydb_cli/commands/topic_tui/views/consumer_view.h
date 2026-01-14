#pragma once

#include "../widgets/table.h"
#include "../widgets/theme.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <functional>
#include <future>
#include <atomic>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

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
    TString ReaderName;
    TString ReadSessionId;
    i32 PartitionNodeId = 0;
};

struct TConsumerData {
    TVector<TPartitionConsumerInfo> PartitionStats;
    ui64 TotalLag = 0;
    TDuration MaxLagTime;
};

class TConsumerView {
public:
    explicit TConsumerView(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void Refresh();
    void SetConsumer(const TString& topicPath, const TString& consumerName);
    void CheckAsyncCompletion();
    
    std::function<void(ui64 partition, ui64 offset)> OnCommitOffset;
    std::function<void()> OnBack;
    
private:
    ftxui::Element RenderHeader();
    void PopulateTable();
    void StartAsyncLoad();
    
private:
    TTopicTuiApp& App_;
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
};

} // namespace NYdb::NConsoleClient
