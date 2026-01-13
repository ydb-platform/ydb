#include "topic_form.h"
#include "../topic_tui_app.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

#include <util/string/cast.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicForm::TTopicForm(TTopicTuiApp& app)
    : App_(app)
{
    Reset();
}

Component TTopicForm::Build() {
    auto pathInput = Input(&PathInput_, "topic-name");
    auto minPartInput = Input(&MinPartitionsInput_, "1");
    auto maxPartInput = Input(&MaxPartitionsInput_, "100");
    auto retentionInput = Input(&RetentionInput_, "24h");
    auto storageInput = Input(&StorageInput_, "0");
    auto writeSpeedInput = Input(&WriteSpeedInput_, "1024");
    
    auto container = Container::Vertical({
        pathInput,
        minPartInput,
        maxPartInput,
        retentionInput,
        storageInput,
        writeSpeedInput
    });
    
    return Renderer(container, [=, this] {
        std::string title = IsEditMode_ ? "Edit Topic" : "Create Topic";
        
        return vbox({
            text(" " + title + " ") | bold | center,
            separator(),
            hbox({text(" Path: ") | size(WIDTH, EQUAL, 20), pathInput->Render() | flex}),
            hbox({text(" Min Partitions: ") | size(WIDTH, EQUAL, 20), minPartInput->Render() | size(WIDTH, EQUAL, 10)}),
            hbox({text(" Max Partitions: ") | size(WIDTH, EQUAL, 20), maxPartInput->Render() | size(WIDTH, EQUAL, 10)}),
            hbox({text(" Retention: ") | size(WIDTH, EQUAL, 20), retentionInput->Render() | size(WIDTH, EQUAL, 10)}),
            hbox({text(" Storage (MB): ") | size(WIDTH, EQUAL, 20), storageInput->Render() | size(WIDTH, EQUAL, 10)}),
            hbox({text(" Write Speed (KB/s): ") | size(WIDTH, EQUAL, 20), writeSpeedInput->Render() | size(WIDTH, EQUAL, 10)}),
            separator(),
            hbox({
                text(" Codecs: ") | size(WIDTH, EQUAL, 20),
                text(Data_.CodecRaw ? "[x] Raw " : "[ ] Raw "),
                text(Data_.CodecGzip ? "[x] GZIP " : "[ ] GZIP "),
                text(Data_.CodecZstd ? "[x] ZSTD " : "[ ] ZSTD "),
                text(Data_.CodecLzop ? "[x] LZOP " : "[ ] LZOP ")
            }),
            separator(),
            hbox({
                filler(),
                text(" [Enter] Submit ") | color(Color::Green),
                text(" [Esc] Cancel ") | dim,
                filler()
            })
        }) | border | size(WIDTH, EQUAL, 60) | center;
    }) | CatchEvent([this](Event event) {
        if (event == Event::Return) {
            // Parse form data
            Data_.Path = TString(PathInput_.c_str());
            Data_.MinPartitions = FromString<ui32>(TString(MinPartitionsInput_.c_str()));
            Data_.MaxPartitions = FromString<ui32>(TString(MaxPartitionsInput_.c_str()));
            // TODO: Parse retention duration
            Data_.WriteSpeedKbps = FromString<ui32>(TString(WriteSpeedInput_.c_str()));
            Data_.RetentionStorageMb = FromString<ui64>(TString(StorageInput_.c_str()));
            
            if (OnSubmit) {
                OnSubmit(Data_);
            }
            return true;
        }
        if (event == Event::Escape) {
            if (OnCancel) {
                OnCancel();
            }
            return true;
        }
        return false;
    });
}

void TTopicForm::SetEditMode(const TString& topicPath, const NTopic::TTopicDescription& desc) {
    IsEditMode_ = true;
    OriginalPath_ = topicPath;
    
    PathInput_ = std::string(topicPath.c_str());
    MinPartitionsInput_ = std::to_string(desc.GetPartitioningSettings().GetMinActivePartitions());
    MaxPartitionsInput_ = std::to_string(desc.GetPartitioningSettings().GetMaxActivePartitions());
    RetentionInput_ = std::to_string(desc.GetRetentionPeriod().Hours()) + "h";
    StorageInput_ = std::to_string(desc.GetRetentionStorageMb().value_or(0));
    WriteSpeedInput_ = std::to_string(desc.GetPartitionWriteSpeedBytesPerSecond() / 1024);
    
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
    IsEditMode_ = false;
    OriginalPath_.clear();
    Data_ = TTopicFormData();
    
    PathInput_.clear();
    MinPartitionsInput_ = "1";
    MaxPartitionsInput_ = "100";
    RetentionInput_ = "24h";
    StorageInput_ = "0";
    WriteSpeedInput_ = "1024";
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
