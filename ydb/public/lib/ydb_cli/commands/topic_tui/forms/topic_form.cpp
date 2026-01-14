#include "topic_form.h"
#include "../topic_tui_app.h"

#include <util/string/cast.h>
#include <util/string/printf.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Static vectors for radiobox options
static std::vector<std::string> AutoPartStrategies = {"Disabled", "ScaleUp", "ScaleUpAndDown", "Paused"};
static std::vector<std::string> MeteringModes = {"Unspecified", "ReservedCapacity", "RequestUnits"};

TTopicForm::TTopicForm(TTopicTuiApp& app)
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
    // Basic settings inputs
    PathInputComponent_ = Input(&PathInput_, "topic-name");
    MinPartInputComponent_ = Input(&MinPartitionsInput_, "1");
    MaxPartInputComponent_ = Input(&MaxPartitionsInput_, "100");
    RetentionInputComponent_ = Input(&RetentionInput_, "24h");
    StorageInputComponent_ = Input(&StorageInput_, "0");
    WriteSpeedInputComponent_ = Input(&WriteSpeedInput_, "1048576");
    WriteBurstInputComponent_ = Input(&WriteBurstInput_, "0");
    
    // Auto-partitioning inputs
    StabilizationInputComponent_ = Input(&StabilizationWindowInput_, "300");
    UpUtilInputComponent_ = Input(&UpUtilizationInput_, "90");
    DownUtilInputComponent_ = Input(&DownUtilizationInput_, "30");
    
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
    
    if (OnSubmit) {
        OnSubmit(Data_);
    }
    return true;  // Close form
}

void TTopicForm::SetEditMode(const TString& topicPath, const NTopic::TTopicDescription& desc) {
    IsEditMode_ = true;
    OriginalPath_ = topicPath;
    
    const auto& partSettings = desc.GetPartitioningSettings();
    OriginalMinPartitions_ = partSettings.GetMinActivePartitions();
    
    PathInput_ = std::string(topicPath.c_str());
    MinPartitionsInput_ = std::to_string(partSettings.GetMinActivePartitions());
    MaxPartitionsInput_ = std::to_string(partSettings.GetMaxActivePartitions());
    RetentionInput_ = std::to_string(desc.GetRetentionPeriod().Hours()) + "h";
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
}

void TTopicForm::SetCreateMode(const TString& basePath) {
    Reset();
    IsEditMode_ = false;
    PathInput_ = std::string((basePath.EndsWith("/") ? basePath : basePath + "/").c_str());
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
