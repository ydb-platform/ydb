#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

// Form data for creating/editing a topic
struct TTopicFormData {
    TString Path;
    ui32 MinPartitions = 1;
    ui32 MaxPartitions = 100;
    TDuration RetentionPeriod = TDuration::Hours(24);
    ui64 RetentionStorageMb = 0;
    ui32 WriteSpeedKbps = 1024;
    
    // Codecs
    bool CodecRaw = true;
    bool CodecGzip = true;
    bool CodecZstd = true;
    bool CodecLzop = false;
    
    // Auto-partitioning
    NTopic::EAutoPartitioningStrategy AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Disabled;
    ui32 StabilizationWindowSeconds = 300;
    ui32 ScaleUpPercent = 90;
    ui32 ScaleDownPercent = 30;
};

class TTopicForm {
public:
    explicit TTopicForm(TTopicTuiApp& app);
    
    ftxui::Component Build();
    
    // Set existing topic data for editing
    void SetEditMode(const TString& topicPath, const NTopic::TTopicDescription& desc);
    void SetCreateMode(const TString& basePath);
    void Reset();
    
    // Callbacks
    std::function<void(const TTopicFormData& data)> OnSubmit;
    std::function<void()> OnCancel;
    
    bool IsEditMode() const { return IsEditMode_; }
    
private:
    ftxui::Element RenderForm();
    TVector<NTopic::ECodec> GetSelectedCodecs() const;
    
private:
    TTopicTuiApp& App_;
    TTopicFormData Data_;
    bool IsEditMode_ = false;
    TString OriginalPath_;
    
    // Input field contents (std::string for ftxui compatibility)
    std::string PathInput_;
    std::string MinPartitionsInput_;
    std::string MaxPartitionsInput_;
    std::string RetentionInput_;
    std::string StorageInput_;
    std::string WriteSpeedInput_;
    
    int FocusedField_ = 0;
};

} // namespace NYdb::NConsoleClient
