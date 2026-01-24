#include "offset_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

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
    InputOption opt;
    opt.content = &OffsetInput_;
    opt.cursor_position = &OffsetCursor_;
    opt.multiline = false;
    
    OffsetInputComponent_ = Input(opt);
    
    // Return input directly - no need for Container wrapper for single input
    // This ensures proper event forwarding from Renderer to Input
    return OffsetInputComponent_;
}

Element TOffsetForm::RenderContent() {
    CheckAsyncCompletion();
    
    if (Submitting_) {
        return RenderSpinner("Committing offset...");
    }
    
    return vbox({
        hbox({text(" Consumer: "), text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText)}),
        hbox({text(" Topic: "), text(std::string(TopicPath_.c_str())) | color(Color::White)}),
        hbox({text(" Partition: "), text(std::to_string(Partition_))}),
        separator(),
        hbox({text(" Current Offset: "), text(std::to_string(CurrentOffset_)) | dim}),
        hbox({text(" End Offset: "), text(std::to_string(EndOffset_)) | dim}),
        separator(),
        hbox({text(" New Offset: ") | size(WIDTH, EQUAL, 15), OffsetInputComponent_->Render() | size(WIDTH, EQUAL, 20) | focus})
    });
}

bool TOffsetForm::HandleSubmit() {
    // Validate offset
    ui64 newOffset;
    try {
        newOffset = FromString<ui64>(TString(OffsetInput_.c_str()));
    } catch (...) {
        ErrorMessage_ = "Invalid offset value";
        return false;
    }
    
    // Validate range
    if (newOffset > EndOffset_) {
        ErrorMessage_ = "Offset cannot exceed end offset (" + std::to_string(EndOffset_) + ")";
        return false;
    }
    
    ErrorMessage_.clear();
    SuccessMessage_.clear();
    Submitting_ = true;
    SpinnerFrame_ = 0;
    
    DoAsyncCommit();
    return false;  // Don't close - wait for async completion
}

void TOffsetForm::DoAsyncCommit() {
    TString topicPath = TopicPath_;
    TString consumerName = ConsumerName_;
    ui64 partition = Partition_;
    ui64 offset;
    try {
        offset = FromString<ui64>(TString(OffsetInput_.c_str()));
    } catch (...) {
        offset = CurrentOffset_;
    }
    
    auto* topicClient = &GetApp().GetTopicClient();
    
    CommitFuture_ = std::async(std::launch::async, 
        [topicPath, topicClient, consumerName, partition, offset]() -> TStatus {
            NTopic::TCommitOffsetSettings settings;
            auto result = topicClient->CommitOffset(
                std::string(topicPath.c_str()),
                partition,
                std::string(consumerName.c_str()),
                offset,
                settings
            );
            return result.GetValueSync();
        });
}

void TOffsetForm::CheckAsyncCompletion() {
    if (!Submitting_) {
        return;
    }
    
    if (CommitFuture_.valid() && 
        CommitFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        
        auto status = CommitFuture_.get();
        Submitting_ = false;
        
        if (status.IsSuccess()) {
            // Success - close form and refresh
            GetApp().NavigateBack();
            GetApp().RequestRefresh();
        } else {
            // Show error
            ErrorMessage_ = status.GetIssues().ToString();
            if (ErrorMessage_.empty()) {
                ErrorMessage_ = "Failed to commit offset";
            }
        }
        GetApp().PostRefresh();
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
    OffsetCursor_ = static_cast<int>(OffsetInput_.size());  // Cursor at end
    
    // Request focus on the input component when form is shown
    if (OffsetInputComponent_) {
        OffsetInputComponent_->TakeFocus();
    }
}

void TOffsetForm::Reset() {
    TFormBase::Reset();
    TopicPath_.clear();
    ConsumerName_.clear();
    Partition_ = 0;
    CurrentOffset_ = 0;
    EndOffset_ = 0;
    OffsetInput_.clear();
    OffsetCursor_ = 0;
}

} // namespace NYdb::NConsoleClient
