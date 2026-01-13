#include "offset_form.h"
#include "../topic_tui_app.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

#include <util/string/cast.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TOffsetForm::TOffsetForm(TTopicTuiApp& app)
    : App_(app)
{
    Reset();
}

Component TOffsetForm::Build() {
    auto offsetInput = Input(&OffsetInput_, std::to_string(CurrentOffset_));
    
    auto container = Container::Vertical({
        offsetInput
    });
    
    return Renderer(container, [=, this] {
        return vbox({
            text(" Commit Offset ") | bold | center,
            separator(),
            hbox({text(" Consumer: "), text(std::string(ConsumerName_.c_str())) | color(Color::Cyan)}),
            hbox({text(" Topic: "), text(std::string(TopicPath_.c_str())) | color(Color::White)}),
            hbox({text(" Partition: "), text(std::to_string(Partition_))}),
            separator(),
            hbox({text(" Current Offset: "), text(std::to_string(CurrentOffset_)) | dim}),
            hbox({text(" End Offset: "), text(std::to_string(EndOffset_)) | dim}),
            separator(),
            hbox({text(" New Offset: ") | size(WIDTH, EQUAL, 15), offsetInput->Render() | size(WIDTH, EQUAL, 20)}),
            separator(),
            hbox({
                filler(),
                text(" [Enter] Commit ") | color(Color::Yellow),
                text(" [Esc] Cancel ") | dim,
                filler()
            })
        }) | border | size(WIDTH, EQUAL, 50) | center;
    }) | CatchEvent([this](Event event) {
        if (event == Event::Return) {
            ui64 offset = FromString<ui64>(TString(OffsetInput_.c_str()));
            if (OnSubmit) {
                OnSubmit(offset);
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

void TOffsetForm::SetContext(const TString& topicPath, const TString& consumerName, 
                              ui64 partition, ui64 currentOffset, ui64 endOffset) {
    TopicPath_ = topicPath;
    ConsumerName_ = consumerName;
    Partition_ = partition;
    CurrentOffset_ = currentOffset;
    EndOffset_ = endOffset;
    OffsetInput_ = std::to_string(currentOffset);
}

void TOffsetForm::Reset() {
    TopicPath_.clear();
    ConsumerName_.clear();
    Partition_ = 0;
    CurrentOffset_ = 0;
    EndOffset_ = 0;
    OffsetInput_.clear();
}

} // namespace NYdb::NConsoleClient
