#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYdb::NConsoleClient {

class ITuiApp;

// Form data for creating/editing a topic
struct TTopicFormData {
    TString Path;
    ui32 MinPartitions = 1;
    ui32 MaxPartitions = 100;
    TDuration RetentionPeriod = TDuration::Hours(24);
    ui64 RetentionStorageMb = 0;
    ui64 WriteSpeedBytesPerSecond = 1048576;  // 1MB/s default
    ui64 WriteBurstBytes = 0;  // 0 means use default
    
    // Codecs
    bool CodecRaw = true;
    bool CodecGzip = true;
    bool CodecZstd = true;
    bool CodecLzop = false;
    
    // Auto-partitioning
    NTopic::EAutoPartitioningStrategy AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Disabled;
    ui32 StabilizationWindowSeconds = 300;
    ui32 UpUtilizationPercent = 90;
    ui32 DownUtilizationPercent = 30;
    
    // Metering
    NTopic::EMeteringMode MeteringMode = NTopic::EMeteringMode::Unspecified;
};

class TTopicForm : public TFormBase {
public:
    explicit TTopicForm(ITuiApp& app);
    
    // Set existing topic data for editing
    void SetEditMode(const TString& topicPath, const NTopic::TTopicDescription& desc);
    void SetCreateMode(const TString& basePath);
    void Reset() override;
    
    bool IsEditMode() const { return IsEditMode_; }
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 75; }
    ftxui::Component BuildContainer() override;
    
private:
    TVector<NTopic::ECodec> GetSelectedCodecs() const;
    
private:
    TTopicFormData Data_;
    bool IsEditMode_ = false;
    TString OriginalPath_;
    ui32 OriginalMinPartitions_ = 0;
    
    // Input field contents (std::string for ftxui compatibility)
    std::string PathInput_;
    std::string MinPartitionsInput_;
    std::string MaxPartitionsInput_;
    std::string RetentionInput_;
    std::string StorageInput_;
    std::string WriteSpeedInput_;
    std::string WriteBurstInput_;
    std::string StabilizationWindowInput_;
    std::string UpUtilizationInput_;
    std::string DownUtilizationInput_;
    
    // Cursor positions for inputs (to position cursor at end of prefilled text)
    int PathCursor_ = 0;
    int MinPartCursor_ = 0;
    int MaxPartCursor_ = 0;
    int RetentionCursor_ = 0;
    int StorageCursor_ = 0;
    int WriteSpeedCursor_ = 0;
    int WriteBurstCursor_ = 0;
    int StabilizationCursor_ = 0;
    int UpUtilCursor_ = 0;
    int DownUtilCursor_ = 0;
    
    // Auto-partitioning strategy selection
    int AutoPartitioningStrategyIndex_ = 0;  // 0=Disabled, 1=ScaleUp, 2=ScaleUpAndDown, 3=Paused
    
    // Metering mode selection
    int MeteringModeIndex_ = 0;  // 0=Unspecified, 1=ReservedCapacity, 2=RequestUnits
    
    // Components for rendering
    ftxui::Component PathInputComponent_;
    ftxui::Component MinPartInputComponent_;
    ftxui::Component MaxPartInputComponent_;
    ftxui::Component RetentionInputComponent_;
    ftxui::Component StorageInputComponent_;
    ftxui::Component WriteSpeedInputComponent_;
    ftxui::Component WriteBurstInputComponent_;
    ftxui::Component StabilizationInputComponent_;
    ftxui::Component UpUtilInputComponent_;
    ftxui::Component DownUtilInputComponent_;
    ftxui::Component AutoPartSelectorComponent_;
    ftxui::Component MeteringSelectorComponent_;
    ftxui::Component CodecRawCheckbox_;
    ftxui::Component CodecGzipCheckbox_;
    ftxui::Component CodecZstdCheckbox_;
    ftxui::Component CodecLzopCheckbox_;
};

} // namespace NYdb::NConsoleClient
