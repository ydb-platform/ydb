#include "delete_confirm_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
using namespace ftxui;

namespace NYdb::NConsoleClient {

TDeleteConfirmForm::TDeleteConfirmForm(ITuiApp& app)
    : TFormBase(app)
{}

TString TDeleteConfirmForm::GetTitle() const {
    return "Delete Topic";
}

EViewType TDeleteConfirmForm::GetViewType() const {
    return EViewType::DeleteConfirm;
}

Component TDeleteConfirmForm::BuildContainer() {
    ConfirmInputComponent_ = Input(&ConfirmInput_, "type topic name");
    return Container::Vertical({
        ConfirmInputComponent_
    });
}

Element TDeleteConfirmForm::RenderContent() {
    return vbox({
        text(" "),
        text(" Are you sure you want to delete:") | center,
        text(" " + std::string(TopicPath_.c_str()) + " ") | bold | center | color(NTheme::WarningText),
        text(" "),
        text(" Type the topic name to confirm:") | center,
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
            text(std::string(TopicName_.c_str())) | color(NTheme::AccentText),
            filler()
        })
    });
}

Element TDeleteConfirmForm::RenderFooter() {
    bool matches = TString(ConfirmInput_.c_str()) == TopicName_;
    return NTheme::DangerFooter(matches);
}

bool TDeleteConfirmForm::HandleSubmit() {
    if (TString(ConfirmInput_.c_str()) == TopicName_) {
        // Execute the delete operation directly using ITuiApp interface
        // Try to delete as topic first
        auto topicResult = GetApp().GetTopicClient().DropTopic(TopicPath_).GetValueSync();
        if (topicResult.IsSuccess()) {
            GetApp().NavigateBack();
            GetApp().RequestRefresh();
            return true;  // Close form
        }
        
        // If topic delete failed, try as directory
        auto dirResult = GetApp().GetSchemeClient().RemoveDirectory(TopicPath_).GetValueSync();
        GetApp().NavigateBack();
        if (dirResult.IsSuccess()) {
            GetApp().RequestRefresh();
        } else {
            // Show directory error
            GetApp().ShowError(dirResult.GetIssues().ToString());
        }
        return true;  // Close form
    }
    // Name doesn't match - don't close, don't show error (just wait for correct input)
    return false;
}

void TDeleteConfirmForm::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    ConfirmInput_.clear();
    
    // Extract just the topic name (last component)
    size_t pos = topicPath.rfind('/');
    if (pos != TString::npos) {
        TopicName_ = topicPath.substr(pos + 1);
    } else {
        TopicName_ = topicPath;
    }
}

void TDeleteConfirmForm::Reset() {
    TFormBase::Reset();
    TopicPath_.clear();
    TopicName_.clear();
    ConfirmInput_.clear();
}

} // namespace NYdb::NConsoleClient
