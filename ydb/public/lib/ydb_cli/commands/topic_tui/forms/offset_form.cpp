#include "offset_form.h"
#include "../topic_tui_app.h"

#include <util/string/cast.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TOffsetForm::TOffsetForm(ITuiApp& app)
    : TFormBase(app)
{}

TString TOffsetForm::GetTitle() const {
    return "Commit Offset";
}

EViewType TOffsetForm::GetViewType() const {
    return EViewType::OffsetForm;
}

Component TOffsetForm::BuildContainer() {
    OffsetInputComponent_ = Input(&OffsetInput_, std::to_string(CurrentOffset_));
    return Container::Vertical({
        OffsetInputComponent_
    });
}

Element TOffsetForm::RenderContent() {
    return vbox({
        hbox({text(" Consumer: "), text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText)}),
        hbox({text(" Topic: "), text(std::string(TopicPath_.c_str())) | color(Color::White)}),
        hbox({text(" Partition: "), text(std::to_string(Partition_))}),
        separator(),
        hbox({text(" Current Offset: "), text(std::to_string(CurrentOffset_)) | dim}),
        hbox({text(" End Offset: "), text(std::to_string(EndOffset_)) | dim}),
        separator(),
        hbox({text(" New Offset: ") | size(WIDTH, EQUAL, 15), OffsetInputComponent_->Render() | size(WIDTH, EQUAL, 20)})
    });
}

bool TOffsetForm::HandleSubmit() {
    try {
        ui64 offset = FromString<ui64>(TString(OffsetInput_.c_str()));
        if (OnSubmitOffset) {
            OnSubmitOffset(offset);
        }
        return true;  // Close form
    } catch (...) {
        ErrorMessage_ = "Invalid offset value";
        return false;  // Keep form open
    }
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
    TFormBase::Reset();
    TopicPath_.clear();
    ConsumerName_.clear();
    Partition_ = 0;
    CurrentOffset_ = 0;
    EndOffset_ = 0;
    OffsetInput_.clear();
}

} // namespace NYdb::NConsoleClient
