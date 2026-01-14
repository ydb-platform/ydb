#include "drop_consumer_form.h"
#include "../topic_tui_app.h"

using namespace ftxui;

namespace NYdb::NConsoleClient {

TDropConsumerForm::TDropConsumerForm(TTopicTuiApp& app)
    : TFormBase(app)
{}

TString TDropConsumerForm::GetTitle() const {
    return "Drop Consumer";
}

EViewType TDropConsumerForm::GetViewType() const {
    return EViewType::DropConsumerConfirm;
}

Component TDropConsumerForm::BuildContainer() {
    ConfirmInputComponent_ = Input(&ConfirmInput_, "type consumer name");
    return Container::Vertical({
        ConfirmInputComponent_
    });
}

Element TDropConsumerForm::RenderContent() {
    return vbox({
        text(" "),
        text(" Are you sure you want to drop consumer:") | center,
        text(" " + std::string(ConsumerName_.c_str()) + " ") | bold | center | color(NTheme::WarningText),
        text(" from topic:") | center,
        text(" " + std::string(TopicPath_.c_str()) + " ") | dim | center,
        text(" "),
        text(" Type the consumer name to confirm:") | center,
        text(" "),
        hbox({
            filler(),
            ConfirmInputComponent_->Render() | size(WIDTH, EQUAL, 30) | border,
            filler()
        }),
        text(" "),
        hbox({
            filler(),
            text(" Expected: ") | dim,
            text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText),
            filler()
        })
    });
}

Element TDropConsumerForm::RenderFooter() {
    bool matches = TString(ConfirmInput_.c_str()) == ConsumerName_;
    return NTheme::DangerFooter(matches);
}

bool TDropConsumerForm::HandleSubmit() {
    if (TString(ConfirmInput_.c_str()) == ConsumerName_) {
        if (OnConfirm) {
            OnConfirm(TopicPath_, ConsumerName_);
        }
        return true;  // Close form
    }
    // Name doesn't match - don't close, don't show error
    return false;
}

void TDropConsumerForm::SetConsumer(const TString& topicPath, const TString& consumerName) {
    TopicPath_ = topicPath;
    ConsumerName_ = consumerName;
    ConfirmInput_.clear();
}

void TDropConsumerForm::Reset() {
    TFormBase::Reset();
    TopicPath_.clear();
    ConsumerName_.clear();
    ConfirmInput_.clear();
}

} // namespace NYdb::NConsoleClient
