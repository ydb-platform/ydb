#include "topic_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/split.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Static vectors for radiobox options
static std::vector<std::string> AutoPartStrategies = {"Disabled", "ScaleUp", "ScaleUpAndDown", "Paused"};
static std::vector<std::string> MeteringModes = {"Unspecified", "ReservedCapacity", "RequestUnits"};

TTopicForm::TTopicForm(ITuiApp& app)
    : TFormBase(app)
{
    Reset();
}

TString TTopicForm::GetTitle() const {
    return IsEditMode_ 
        ? "Edit Topic " + OriginalPath_
        : "Create Topic";
}

EViewType TTopicForm::GetViewType() const {
    return EViewType::TopicForm;
}

Component TTopicForm::BuildContainer() {
    // Create input options with cursor position tracking
    InputOption pathOpt; pathOpt.content = &PathInput_; pathOpt.cursor_position = &PathCursor_; pathOpt.multiline = false;
    InputOption minPartOpt; minPartOpt.content = &MinPartitionsInput_; minPartOpt.cursor_position = &MinPartCursor_; minPartOpt.multiline = false;
    InputOption maxPartOpt; maxPartOpt.content = &MaxPartitionsInput_; maxPartOpt.cursor_position = &MaxPartCursor_; maxPartOpt.multiline = false;
    InputOption retentionOpt; retentionOpt.content = &RetentionInput_; retentionOpt.cursor_position = &RetentionCursor_; retentionOpt.multiline = false;
    InputOption storageOpt; storageOpt.content = &StorageInput_; storageOpt.cursor_position = &StorageCursor_; storageOpt.multiline = false;
    InputOption writeSpeedOpt; writeSpeedOpt.content = &WriteSpeedInput_; writeSpeedOpt.cursor_position = &WriteSpeedCursor_; writeSpeedOpt.multiline = false;
    InputOption writeBurstOpt; writeBurstOpt.content = &WriteBurstInput_; writeBurstOpt.cursor_position = &WriteBurstCursor_; writeBurstOpt.multiline = false;
    InputOption stabOpt; stabOpt.content = &StabilizationWindowInput_; stabOpt.cursor_position = &StabilizationCursor_; stabOpt.multiline = false;
    InputOption upUtilOpt; upUtilOpt.content = &UpUtilizationInput_; upUtilOpt.cursor_position = &UpUtilCursor_; upUtilOpt.multiline = false;
    InputOption downUtilOpt; downUtilOpt.content = &DownUtilizationInput_; downUtilOpt.cursor_position = &DownUtilCursor_; downUtilOpt.multiline = false;
    
    // Basic settings inputs
    PathInputComponent_ = Input(pathOpt);
    MinPartInputComponent_ = Input(minPartOpt);
    MaxPartInputComponent_ = Input(maxPartOpt);
    RetentionInputComponent_ = Input(retentionOpt);
    StorageInputComponent_ = Input(storageOpt);
    WriteSpeedInputComponent_ = Input(writeSpeedOpt);
    WriteBurstInputComponent_ = Input(writeBurstOpt);
    
    // Auto-partitioning inputs
    StabilizationInputComponent_ = Input(stabOpt);
    UpUtilInputComponent_ = Input(upUtilOpt);
    DownUtilInputComponent_ = Input(downUtilOpt);
    
    // Selectors
    AutoPartSelectorComponent_ = Radiobox(&AutoPartStrategies, &AutoPartitioningStrategyIndex_);
    MeteringSelectorComponent_ = Radiobox(&MeteringModes, &MeteringModeIndex_);
    
    // Codec checkboxes
    CodecRawCheckbox_ = Checkbox("Raw", &Data_.CodecRaw);
    CodecGzipCheckbox_ = Checkbox("GZIP", &Data_.CodecGzip);
    CodecZstdCheckbox_ = Checkbox("ZSTD", &Data_.CodecZstd);
    CodecLzopCheckbox_ = Checkbox("LZOP", &Data_.CodecLzop);
    
    // Build container with all focusable components
    return Container::Vertical({
        PathInputComponent_,
        MinPartInputComponent_,
        MaxPartInputComponent_,
        RetentionInputComponent_,
        StorageInputComponent_,
        WriteSpeedInputComponent_,
        WriteBurstInputComponent_,
        AutoPartSelectorComponent_,
        StabilizationInputComponent_,
        UpUtilInputComponent_,
        DownUtilInputComponent_,
        MeteringSelectorComponent_,
        Container::Horizontal({CodecRawCheckbox_, CodecGzipCheckbox_, CodecZstdCheckbox_, CodecLzopCheckbox_})
    });
}

Element TTopicForm::RenderContent() {
    Elements formFields;
    
    // === Basic Settings ===
    formFields.push_back(NTheme::SectionHeader("Basic Settings"));
    
    if (IsEditMode_) {
        formFields.push_back(hbox({text(" Path: ") | size(WIDTH, EQUAL, 22), text(std::string(OriginalPath_.c_str())) | dim}));
    } else {
        formFields.push_back(hbox({text(" Path: ") | size(WIDTH, EQUAL, 22), PathInputComponent_->Render() | flex}));
    }
    
    if (IsEditMode_) {
        formFields.push_back(hbox({text(" Min Partitions: ") | size(WIDTH, EQUAL, 22), MinPartInputComponent_->Render() | size(WIDTH, EQUAL, 10), text(Sprintf(" (>= %u)", OriginalMinPartitions_)) | dim}));
    } else {
        formFields.push_back(hbox({text(" Min Partitions: ") | size(WIDTH, EQUAL, 22), MinPartInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
    }
    formFields.push_back(hbox({text(" Max Partitions: ") | size(WIDTH, EQUAL, 22), MaxPartInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
    
    formFields.push_back(separator());
    
    // === Retention & Storage ===
    formFields.push_back(NTheme::SectionHeader("Retention & Storage"));
    formFields.push_back(hbox({text(" Retention Period: ") | size(WIDTH, EQUAL, 22), RetentionInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
    formFields.push_back(hbox({text(" Storage Limit (MB): ") | size(WIDTH, EQUAL, 22), StorageInputComponent_->Render() | size(WIDTH, EQUAL, 10), text(" 0 = unlimited") | dim}));
    
    formFields.push_back(separator());
    
    // === Write Performance ===
    formFields.push_back(NTheme::SectionHeader("Write Performance"));
    formFields.push_back(hbox({text(" Write Speed (B/s): ") | size(WIDTH, EQUAL, 22), WriteSpeedInputComponent_->Render() | size(WIDTH, EQUAL, 15)}));
    formFields.push_back(hbox({text(" Write Burst (bytes): ") | size(WIDTH, EQUAL, 22), WriteBurstInputComponent_->Render() | size(WIDTH, EQUAL, 15), text(" 0 = default") | dim}));
    
    formFields.push_back(separator());
    
    // === Auto-Partitioning ===
    formFields.push_back(NTheme::SectionHeader("Auto-Partitioning"));
    formFields.push_back(hbox({text(" Strategy: ") | size(WIDTH, EQUAL, 22), AutoPartSelectorComponent_->Render() | size(WIDTH, EQUAL, 20)}));
    
    // Only show stabilization/utilization if auto-partitioning is enabled
    if (AutoPartitioningStrategyIndex_ > 0) {
        formFields.push_back(hbox({text(" Stabilization (s): ") | size(WIDTH, EQUAL, 22), StabilizationInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
        formFields.push_back(hbox({text(" Scale Up at (%): ") | size(WIDTH, EQUAL, 22), UpUtilInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
        if (AutoPartitioningStrategyIndex_ == 2) {  // ScaleUpAndDown
            formFields.push_back(hbox({text(" Scale Down at (%): ") | size(WIDTH, EQUAL, 22), DownUtilInputComponent_->Render() | size(WIDTH, EQUAL, 10)}));
        }
    }
    
    formFields.push_back(separator());
    
    // === Metering ===
    formFields.push_back(NTheme::SectionHeader("Metering"));
    formFields.push_back(hbox({text(" Mode: ") | size(WIDTH, EQUAL, 22), MeteringSelectorComponent_->Render() | size(WIDTH, EQUAL, 20)}));
    
    formFields.push_back(separator());
    
    // === Codecs ===
    formFields.push_back(NTheme::SectionHeader("Supported Codecs"));
    formFields.push_back(hbox({
        text(" ") | size(WIDTH, EQUAL, 2),
        CodecRawCheckbox_->Render(), text(" "),
        CodecGzipCheckbox_->Render(), text(" "),
        CodecZstdCheckbox_->Render(), text(" "),
        CodecLzopCheckbox_->Render()
    }));
    
    return vbox(formFields);
}

bool TTopicForm::HandleSubmit() {
    // Parse form data
    Data_.Path = IsEditMode_ ? OriginalPath_ : TString(PathInput_.c_str());
    try {
        Data_.MinPartitions = FromString<ui32>(TString(MinPartitionsInput_.c_str()));
        Data_.MaxPartitions = FromString<ui32>(TString(MaxPartitionsInput_.c_str()));
        Data_.WriteSpeedBytesPerSecond = FromString<ui64>(TString(WriteSpeedInput_.c_str()));
        Data_.WriteBurstBytes = FromString<ui64>(TString(WriteBurstInput_.c_str()));
        Data_.RetentionStorageMb = FromString<ui64>(TString(StorageInput_.c_str()));
        
        // Parse retention period (formats: "24h", "7d", "HH:MM:SS", "HH:MM", or plain hours)
        TString retentionStr = TString(RetentionInput_.c_str());
        if (retentionStr.Contains(':')) {
            // Parse HH:MM:SS or HH:MM format
            TVector<TString> parts;
            Split(retentionStr, ":", parts);
            if (parts.size() >= 2) {
                ui64 hours = FromString<ui64>(parts[0]);
                ui64 minutes = FromString<ui64>(parts[1]);
                ui64 seconds = (parts.size() >= 3) ? FromString<ui64>(parts[2]) : 0;
                Data_.RetentionPeriod = TDuration::Hours(hours) + TDuration::Minutes(minutes) + TDuration::Seconds(seconds);
            }
        } else if (retentionStr.EndsWith("h") || retentionStr.EndsWith("H")) {
            ui64 hours = FromString<ui64>(retentionStr.substr(0, retentionStr.size() - 1));
            Data_.RetentionPeriod = TDuration::Hours(hours);
        } else if (retentionStr.EndsWith("d") || retentionStr.EndsWith("D")) {
            ui64 days = FromString<ui64>(retentionStr.substr(0, retentionStr.size() - 1));
            Data_.RetentionPeriod = TDuration::Days(days);
        } else {
            // Assume hours if no suffix
            ui64 hours = FromString<ui64>(retentionStr);
            Data_.RetentionPeriod = TDuration::Hours(hours);
        }
        
        // Auto-partitioning settings
        Data_.StabilizationWindowSeconds = FromString<ui32>(TString(StabilizationWindowInput_.c_str()));
        Data_.UpUtilizationPercent = FromString<ui32>(TString(UpUtilizationInput_.c_str()));
        Data_.DownUtilizationPercent = FromString<ui32>(TString(DownUtilizationInput_.c_str()));
        
        // Map strategy index to enum
        switch (AutoPartitioningStrategyIndex_) {
            case 0: Data_.AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Disabled; break;
            case 1: Data_.AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::ScaleUp; break;
            case 2: Data_.AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::ScaleUpAndDown; break;
            case 3: Data_.AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Paused; break;
            default: Data_.AutoPartitioningStrategy = NTopic::EAutoPartitioningStrategy::Disabled; break;
        }
        
        // Map metering mode index to enum
        switch (MeteringModeIndex_) {
            case 0: Data_.MeteringMode = NTopic::EMeteringMode::Unspecified; break;
            case 1: Data_.MeteringMode = NTopic::EMeteringMode::ReservedCapacity; break;
            case 2: Data_.MeteringMode = NTopic::EMeteringMode::RequestUnits; break;
            default: Data_.MeteringMode = NTopic::EMeteringMode::Unspecified; break;
        }
        
        if (IsEditMode_ && Data_.MinPartitions < OriginalMinPartitions_) {
            Data_.MinPartitions = OriginalMinPartitions_;
        }
    } catch (...) {
        ErrorMessage_ = "Invalid input values";
        return false;
    }
    
    // Execute create/alter topic directly using ITuiApp interface
    if (IsEditMode_) {
        // Alter existing topic
        NTopic::TAlterTopicSettings settings;
        
        // Partitioning settings with auto-partitioning
        settings.BeginAlterPartitioningSettings()
            .MinActivePartitions(Data_.MinPartitions)
            .MaxActivePartitions(Data_.MaxPartitions)
            .BeginAlterAutoPartitioningSettings()
                .Strategy(Data_.AutoPartitioningStrategy)
                .StabilizationWindow(TDuration::Seconds(Data_.StabilizationWindowSeconds))
                .UpUtilizationPercent(Data_.UpUtilizationPercent)
                .DownUtilizationPercent(Data_.DownUtilizationPercent)
            .EndAlterAutoPartitioningSettings()
        .EndAlterTopicPartitioningSettings();
        
        // Retention
        settings.SetRetentionPeriod(Data_.RetentionPeriod);
        if (Data_.RetentionStorageMb > 0) {
            settings.SetRetentionStorageMb(Data_.RetentionStorageMb);
        }
        
        // Write performance
        settings.SetPartitionWriteSpeedBytesPerSecond(Data_.WriteSpeedBytesPerSecond);
        if (Data_.WriteBurstBytes > 0) {
            settings.SetPartitionWriteBurstBytes(Data_.WriteBurstBytes);
        }
        
        // Metering mode
        if (Data_.MeteringMode != NTopic::EMeteringMode::Unspecified) {
            settings.SetMeteringMode(Data_.MeteringMode);
        }
        
        // Codecs
        std::vector<NTopic::ECodec> codecs;
        if (Data_.CodecRaw) codecs.push_back(NTopic::ECodec::RAW);
        if (Data_.CodecGzip) codecs.push_back(NTopic::ECodec::GZIP);
        if (Data_.CodecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
        if (Data_.CodecLzop) codecs.push_back(NTopic::ECodec::LZOP);
        if (!codecs.empty()) {
            settings.SetSupportedCodecs(codecs);
        }
        
        auto result = GetApp().GetTopicClient().AlterTopic(Data_.Path, settings).GetValueSync();
        if (result.IsSuccess()) {
            GetApp().NavigateBack();
            GetApp().RequestRefresh();
            return true;
        } else {
            GetApp().ShowError(result.GetIssues().ToString());
            return false;
        }
    } else {
        // Create new topic
        NTopic::TCreateTopicSettings settings;
        
        // Partitioning with auto-partitioning
        NTopic::TAutoPartitioningSettings autoPartSettings(
            Data_.AutoPartitioningStrategy,
            TDuration::Seconds(Data_.StabilizationWindowSeconds),
            Data_.DownUtilizationPercent,
            Data_.UpUtilizationPercent
        );
        settings.PartitioningSettings(Data_.MinPartitions, Data_.MaxPartitions, autoPartSettings);
        
        // Retention
        settings.RetentionPeriod(Data_.RetentionPeriod);
        if (Data_.RetentionStorageMb > 0) {
            settings.RetentionStorageMb(Data_.RetentionStorageMb);
        }
        
        // Write performance
        settings.PartitionWriteSpeedBytesPerSecond(Data_.WriteSpeedBytesPerSecond);
        if (Data_.WriteBurstBytes > 0) {
            settings.PartitionWriteBurstBytes(Data_.WriteBurstBytes);
        }
        
        // Metering mode
        if (Data_.MeteringMode != NTopic::EMeteringMode::Unspecified) {
            settings.MeteringMode(Data_.MeteringMode);
        }
        
        // Codecs
        if (Data_.CodecRaw) settings.AppendSupportedCodecs(NTopic::ECodec::RAW);
        if (Data_.CodecGzip) settings.AppendSupportedCodecs(NTopic::ECodec::GZIP);
        if (Data_.CodecZstd) settings.AppendSupportedCodecs(NTopic::ECodec::ZSTD);
        if (Data_.CodecLzop) settings.AppendSupportedCodecs(NTopic::ECodec::LZOP);
        
        auto result = GetApp().GetTopicClient().CreateTopic(Data_.Path, settings).GetValueSync();
        if (result.IsSuccess()) {
            GetApp().NavigateBack();
            GetApp().RequestRefresh();
            return true;
        } else {
            GetApp().ShowError(result.GetIssues().ToString());
            return false;
        }
    }
}

void TTopicForm::SetEditMode(const TString& topicPath, const NTopic::TTopicDescription& desc) {
    IsEditMode_ = true;
    OriginalPath_ = topicPath;
    
    const auto& partSettings = desc.GetPartitioningSettings();
    OriginalMinPartitions_ = partSettings.GetMinActivePartitions();
    
    PathInput_ = std::string(topicPath.c_str());
    MinPartitionsInput_ = std::to_string(partSettings.GetMinActivePartitions());
    MaxPartitionsInput_ = std::to_string(partSettings.GetMaxActivePartitions());
    // Show retention in HH:MM:SS format
    ui64 totalSeconds = desc.GetRetentionPeriod().Seconds();
    ui64 hours = totalSeconds / 3600;
    ui64 minutes = (totalSeconds % 3600) / 60;
    ui64 seconds = totalSeconds % 60;
    RetentionInput_ = Sprintf("%02lu:%02lu:%02lu", hours, minutes, seconds);
    StorageInput_ = std::to_string(desc.GetRetentionStorageMb().value_or(0));
    WriteSpeedInput_ = std::to_string(desc.GetPartitionWriteSpeedBytesPerSecond());
    WriteBurstInput_ = std::to_string(desc.GetPartitionWriteBurstBytes());
    
    // Auto-partitioning settings
    const auto& autoPartSettings = partSettings.GetAutoPartitioningSettings();
    StabilizationWindowInput_ = std::to_string(autoPartSettings.GetStabilizationWindow().Seconds());
    UpUtilizationInput_ = std::to_string(autoPartSettings.GetUpUtilizationPercent());
    DownUtilizationInput_ = std::to_string(autoPartSettings.GetDownUtilizationPercent());
    
    // Map strategy enum to index
    switch (autoPartSettings.GetStrategy()) {
        case NTopic::EAutoPartitioningStrategy::Disabled: AutoPartitioningStrategyIndex_ = 0; break;
        case NTopic::EAutoPartitioningStrategy::ScaleUp: AutoPartitioningStrategyIndex_ = 1; break;
        case NTopic::EAutoPartitioningStrategy::ScaleUpAndDown: AutoPartitioningStrategyIndex_ = 2; break;
        case NTopic::EAutoPartitioningStrategy::Paused: AutoPartitioningStrategyIndex_ = 3; break;
        default: AutoPartitioningStrategyIndex_ = 0; break;
    }
    
    // Map metering mode enum to index
    switch (desc.GetMeteringMode()) {
        case NTopic::EMeteringMode::Unspecified: MeteringModeIndex_ = 0; break;
        case NTopic::EMeteringMode::ReservedCapacity: MeteringModeIndex_ = 1; break;
        case NTopic::EMeteringMode::RequestUnits: MeteringModeIndex_ = 2; break;
        default: MeteringModeIndex_ = 0; break;
    }
    
    // Parse codecs
    Data_.CodecRaw = false;
    Data_.CodecGzip = false;
    Data_.CodecZstd = false;
    Data_.CodecLzop = false;
    
    for (auto codec : desc.GetSupportedCodecs()) {
        switch (codec) {
            case NTopic::ECodec::RAW: Data_.CodecRaw = true; break;
            case NTopic::ECodec::GZIP: Data_.CodecGzip = true; break;
            case NTopic::ECodec::ZSTD: Data_.CodecZstd = true; break;
            case NTopic::ECodec::LZOP: Data_.CodecLzop = true; break;
            default: break;
        }
    }
    
    // Set cursor positions at end of values
    PathCursor_ = PathInput_.size();
    MinPartCursor_ = MinPartitionsInput_.size();
    MaxPartCursor_ = MaxPartitionsInput_.size();
    RetentionCursor_ = RetentionInput_.size();
    StorageCursor_ = StorageInput_.size();
    WriteSpeedCursor_ = WriteSpeedInput_.size();
    WriteBurstCursor_ = WriteBurstInput_.size();
    StabilizationCursor_ = StabilizationWindowInput_.size();
    UpUtilCursor_ = UpUtilizationInput_.size();
    DownUtilCursor_ = DownUtilizationInput_.size();
}

void TTopicForm::SetCreateMode(const TString& basePath) {
    Reset();
    IsEditMode_ = false;
    PathInput_ = std::string((basePath.EndsWith("/") ? basePath : basePath + "/").c_str());
    PathCursor_ = PathInput_.size();  // Position cursor at end
}

void TTopicForm::Reset() {
    TFormBase::Reset();
    IsEditMode_ = false;
    OriginalPath_.clear();
    OriginalMinPartitions_ = 0;
    Data_ = TTopicFormData();
    
    PathInput_.clear();
    MinPartitionsInput_ = "1";
    MaxPartitionsInput_ = "100";
    RetentionInput_ = "24h";
    StorageInput_ = "0";
    WriteSpeedInput_ = "1048576";  // 1MB/s
    WriteBurstInput_ = "0";
    StabilizationWindowInput_ = "300";
    UpUtilizationInput_ = "90";
    DownUtilizationInput_ = "30";
    AutoPartitioningStrategyIndex_ = 0;
    MeteringModeIndex_ = 0;
    
    // Set cursor positions at end of default values
    PathCursor_ = PathInput_.size();
    MinPartCursor_ = MinPartitionsInput_.size();
    MaxPartCursor_ = MaxPartitionsInput_.size();
    RetentionCursor_ = RetentionInput_.size();
    StorageCursor_ = StorageInput_.size();
    WriteSpeedCursor_ = WriteSpeedInput_.size();
    WriteBurstCursor_ = WriteBurstInput_.size();
    StabilizationCursor_ = StabilizationWindowInput_.size();
    UpUtilCursor_ = UpUtilizationInput_.size();
    DownUtilCursor_ = DownUtilizationInput_.size();
}

TVector<NTopic::ECodec> TTopicForm::GetSelectedCodecs() const {
    TVector<NTopic::ECodec> codecs;
    if (Data_.CodecRaw) codecs.push_back(NTopic::ECodec::RAW);
    if (Data_.CodecGzip) codecs.push_back(NTopic::ECodec::GZIP);
    if (Data_.CodecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
    if (Data_.CodecLzop) codecs.push_back(NTopic::ECodec::LZOP);
    return codecs;
}

} // namespace NYdb::NConsoleClient
