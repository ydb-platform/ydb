#pragma once

#include "view_interface.h"
#include "../widgets/table.h"
#include "../widgets/theme.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <future>
#include <atomic>
#include <optional>

namespace NYdb::NConsoleClient {

class ITuiApp;

// Cached topic data for display
struct TTopicInfoData {
    size_t PartitionCount = 0;
    TDuration RetentionPeriod;
    ui64 RetentionStorageMb = 0;
    TVector<TString> ConsumerNames;
    TVector<TString> Codecs;
    
    struct TPartitionInfo {
        ui64 PartitionId = 0;
        ui64 StartOffset = 0;
        ui64 EndOffset = 0;
        ui64 StoreSizeBytes = 0;
        TDuration WriteLag;
        TInstant LastWriteTime;
    };
    TVector<TPartitionInfo> Partitions;
};

// Full-screen topic information view
// Shows complete topic configuration and partition details
class TTopicInfoView : public ITuiView {
public:
    explicit TTopicInfoView(ITuiApp& app);
    
    ftxui::Component Build() override;
    void Refresh() override;
    
    void SetTopic(const TString& topicPath);
    
private:
    void StartAsyncLoad();
    void CheckAsyncCompletion();
    ftxui::Element RenderContent();
    ftxui::Element RenderTopicConfig();
    ftxui::Element RenderPartitionTable();
    
    ITuiApp& App_;
    TString TopicPath_;
    
    // Async loading
    std::future<NTopic::TDescribeTopicResult> DescribeFuture_;
    std::atomic<bool> Loading_{false};
    bool HasData_ = false;
    TString ErrorMessage_;
    
    // Cached topic data (avoids non-copyable NTopic::TTopicDescription)
    TTopicInfoData Data_;
    
    // Partition table
    TTable Table_;
};

} // namespace NYdb::NConsoleClient
