#include "consumer_form.h"
#include "../topic_tui_app.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TConsumerForm::TConsumerForm(TTopicTuiApp& app)
    : App_(app)
{
    Reset();
}

Component TConsumerForm::Build() {
    auto nameInput = Input(&NameInput_, "consumer-name");
    auto readFromInput = Input(&ReadFromInput_, "");
    
    auto container = Container::Vertical({
        nameInput,
        readFromInput
    });
    
    return Renderer(container, [=, this] {
        return vbox({
            text(" Add Consumer ") | bold | center,
            separator(),
            hbox({text(" Name: ") | size(WIDTH, EQUAL, 15), nameInput->Render() | flex}),
            hbox({text(" Read From: ") | size(WIDTH, EQUAL, 15), readFromInput->Render() | size(WIDTH, EQUAL, 25)}),
            text("  (timestamp or empty for beginning)") | dim,
            separator(),
            hbox({
                text(" Codecs: "),
                text(Data_.CodecRaw ? "[x] Raw " : "[ ] Raw "),
                text(Data_.CodecGzip ? "[x] GZIP " : "[ ] GZIP "),
                text(Data_.CodecZstd ? "[x] ZSTD" : "[ ] ZSTD")
            }),
            hbox({
                text(" Important: "),
                text(Data_.IsImportant ? "[x]" : "[ ]")
            }),
            separator(),
            hbox({
                filler(),
                text(" [Enter] Submit ") | color(Color::Green),
                text(" [Esc] Cancel ") | dim,
                filler()
            })
        }) | border | size(WIDTH, EQUAL, 50) | center;
    }) | CatchEvent([this](Event event) {
        if (event == Event::Return) {
            Data_.Name = TString(NameInput_.c_str());
            // TODO: Parse ReadFrom timestamp
            
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

void TConsumerForm::SetTopicPath(const TString& topicPath) {
    TopicPath_ = topicPath;
}

void TConsumerForm::Reset() {
    Data_ = TConsumerFormData();
    NameInput_.clear();
    ReadFromInput_.clear();
}

} // namespace NYdb::NConsoleClient
