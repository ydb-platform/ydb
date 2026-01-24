#pragma once

#include "view_interface.h"
#include "../widgets/table.h"
#include "../widgets/theme.h"
#include "../http_client.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
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
// Shows topic configuration and Viewer JSON details
class TTopicInfoView : public ITuiView {
public:
    explicit TTopicInfoView(ITuiApp& app);
    ~TTopicInfoView();
    
    ftxui::Component Build() override;
    void Refresh() override;
    
    void SetTopic(const TString& topicPath);
    
private:
    struct TJsonTreeNode {
        TString Name;
        const NJson::TJsonValue* Value = nullptr;
        TVector<std::unique_ptr<TJsonTreeNode>> Children;
        TJsonTreeNode* Parent = nullptr;
        bool Open = false;
    };
    
    struct TJsonVisibleNode {
        TJsonTreeNode* Node = nullptr;
        int Depth = 0;
    };
    
    void StartAsyncLoad();
    void StartViewerLoad();
    void CheckAsyncCompletion();
    std::unique_ptr<TJsonTreeNode> MakeJsonNode(const TString& name, const NJson::TJsonValue& value,
                                                TJsonTreeNode* parent);
    void BuildJsonTree();
    void BuildJsonVisible(TJsonTreeNode* preferSelected = nullptr);
    void CollectVisibleNodes(TJsonTreeNode* node, int depth);
    bool HandleJsonTreeEvent(const ftxui::Event& event);
    ftxui::Element RenderContent();
    ftxui::Element RenderTopicConfig();
    ftxui::Element RenderViewerConfig();
    ftxui::Element RenderViewerJsonTree();
    ftxui::Element RenderPartitionTable();
    
    ITuiApp& App_;
    TString TopicPath_;
    
    // Async loading
    std::future<NTopic::TDescribeTopicResult> DescribeFuture_;
    std::atomic<bool> Loading_{false};
    bool HasData_ = false;
    TString ErrorMessage_;
    
    std::future<TTopicDescribeResult> ViewerDescribeFuture_;
    std::atomic<bool> ViewerLoading_{false};
    bool HasViewerData_ = false;
    TTopicDescribeResult ViewerData_;
    NJson::TJsonValue ViewerJson_;
    TString JsonTreeError_;
    bool JsonTreeReady_ = false;
    std::unique_ptr<TJsonTreeNode> JsonRoot_;
    TVector<TJsonVisibleNode> JsonVisibleNodes_;
    int JsonSelected_ = 0;
    
    // Cached topic data (avoids non-copyable NTopic::TTopicDescription)
    TTopicInfoData Data_;
    
    // Partition table
    TTable Table_;
    
    // Stop flag for async cancellation
    std::shared_ptr<std::atomic<bool>> StopFlag_ = std::make_shared<std::atomic<bool>>(false);
};

} // namespace NYdb::NConsoleClient
