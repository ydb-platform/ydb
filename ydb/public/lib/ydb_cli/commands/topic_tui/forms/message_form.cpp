#include "message_form.h"
#include "../topic_tui_app.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

#include <util/string/cast.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TMessageForm::TMessageForm(TTopicTuiApp& app)
    : App_(app)
{
    Reset();
}

Component TMessageForm::Build() {
    auto dataInput = Input(&DataInput_, "Enter message data...");
    auto msgGroupInput = Input(&MessageGroupIdInput_, "");
    auto partitionInput = Input(&PartitionInput_, "auto");
    
    auto container = Container::Vertical({
        dataInput,
        msgGroupInput,
        partitionInput
    });
    
    return Renderer(container, [=, this] {
        return vbox({
            text(" Write Message ") | bold | center,
            text(std::string(" Topic: ") + std::string(TopicPath_.c_str())) | dim | center,
            separator(),
            text(" Data:") | bold,
            dataInput->Render() | size(HEIGHT, EQUAL, 5) | border,
            hbox({text(" Message Group ID: ") | size(WIDTH, EQUAL, 20), msgGroupInput->Render() | flex}),
            hbox({text(" Partition: ") | size(WIDTH, EQUAL, 20), partitionInput->Render() | size(WIDTH, EQUAL, 10)}),
            text("  (number or 'auto')") | dim,
            separator(),
            hbox({
                filler(),
                text(" [Enter] Send ") | color(Color::Green),
                text(" [Esc] Cancel ") | dim,
                filler()
            })
        }) | border | size(WIDTH, EQUAL, 60) | center;
    }) | CatchEvent([this](Event event) {
        if (event == Event::Return) {
            Data_.Data = TString(DataInput_.c_str());
            Data_.MessageGroupId = TString(MessageGroupIdInput_.c_str());
            
            if (PartitionInput_ != "auto" && !PartitionInput_.empty()) {
                Data_.Partition = FromString<ui32>(TString(PartitionInput_.c_str()));
            } else {
                Data_.Partition = Nothing();
            }
            
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

void TMessageForm::SetTopicPath(const TString& topicPath) {
    TopicPath_ = topicPath;
}

void TMessageForm::Reset() {
    Data_ = TMessageFormData();
    DataInput_.clear();
    MessageGroupIdInput_.clear();
    PartitionInput_ = "auto";
}

} // namespace NYdb::NConsoleClient
