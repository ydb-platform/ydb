#include "write_message_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/string/printf.h>
using namespace ftxui;

namespace NYdb::NConsoleClient {

TWriteMessageForm::TWriteMessageForm(ITuiApp& app)
    : TFormBase(app)
{
    Reset();
}

TString TWriteMessageForm::GetTitle() const {
    return "Write Message";
}

EViewType TWriteMessageForm::GetViewType() const {
    return EViewType::WriteMessage;
}

Component TWriteMessageForm::BuildContainer() {
    MessageInputComponent_ = Input(&MessageData_, "Enter message data...");
    ProducerInputComponent_ = Input(&ProducerIdInput_, "auto-generated");
    MessageGroupInputComponent_ = Input(&MessageGroupIdInput_, "auto-generated");
    
    return Container::Vertical({
        MessageInputComponent_,
        ProducerInputComponent_,
        MessageGroupInputComponent_
    });
}

Element TWriteMessageForm::RenderContent() {
    // Check async completion each render
    CheckAsyncCompletion();
    
    Elements content;
    
    content.push_back(text(" " + std::string(TopicPath_.c_str()) + " ") | dim | center);
    if (PartitionId_.has_value()) {
        content.push_back(text(" Partition: " + std::to_string(*PartitionId_) + " ") | dim | center);
    }
    content.push_back(separator());
    
    if (Submitting_) {
        content.push_back(RenderSpinner("Sending message..."));
    } else {
        content.push_back(NTheme::SectionHeader("Message Data"));
        content.push_back(MessageInputComponent_->Render() | size(HEIGHT, EQUAL, 5) | border);
        content.push_back(separator());
        
        content.push_back(text(" Advanced Options (optional):") | dim);
        content.push_back(hbox({text(" Producer ID: ") | size(WIDTH, EQUAL, 15), ProducerInputComponent_->Render() | flex}));
        content.push_back(hbox({text(" Message Group: ") | size(WIDTH, EQUAL, 15), MessageGroupInputComponent_->Render() | flex}));
        
        if (MessagesSent_ > 0) {
            content.push_back(separator());
            content.push_back(text(" Messages sent this session: " + std::to_string(MessagesSent_)) | dim);
        }
    }
    
    return vbox(content);
}

bool TWriteMessageForm::HandleSubmit() {
    if (MessageData_.empty()) {
        ErrorMessage_ = "Message data is required";
        return false;
    }
    
    ErrorMessage_.clear();
    SuccessMessage_.clear();
    Submitting_ = true;
    SpinnerFrame_ = 0;
    
    DoAsyncSend();
    return false;  // Never close on submit - user stays to send more messages
}

void TWriteMessageForm::SetTopic(const TString& topicPath, std::optional<ui32> partitionId) {
    TopicPath_ = topicPath;
    PartitionId_ = partitionId;
    Reset();
}

void TWriteMessageForm::Reset() {
    TFormBase::Reset();
    MessageData_.clear();
    ProducerIdInput_.clear();
    MessageGroupIdInput_.clear();
    MessagesSent_ = 0;
    
    // Close existing session
    if (WriteSession_) {
        WriteSession_->Close(TDuration::Seconds(1));
        WriteSession_.reset();
    }
    // Note: TFormBase handles Escape key navigation; session cleanup happens in destructor or Reset
}

void TWriteMessageForm::CreateWriteSession() {
    if (WriteSession_ && WriteSession_->IsAlive()) {
        return;  // Session already exists and is alive
    }
    
    NTopic::TWriteSessionSettings settings;
    settings.Path(std::string(TopicPath_.c_str()));
    
    // Use provided IDs or generate random ones
    std::string producerId = ProducerIdInput_.empty() 
        ? "tui-" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()))
        : ProducerIdInput_;
    std::string messageGroupId = MessageGroupIdInput_.empty() ? producerId : MessageGroupIdInput_;
    
    settings.ProducerId(producerId);
    settings.MessageGroupId(messageGroupId);
    
    if (PartitionId_.has_value()) {
        settings.PartitionId(*PartitionId_);
    }
    
    settings.Codec(NTopic::ECodec::RAW);  // Use raw for simplicity in TUI
    
    WriteSession_ = GetApp().GetTopicClient().CreateSimpleBlockingWriteSession(settings);
}

void TWriteMessageForm::DoAsyncSend() {
    std::string message = MessageData_;
    auto* thisPtr = this;
    
    SendFuture_ = std::async(std::launch::async, [thisPtr, message]() -> bool {
        try {
            thisPtr->CreateWriteSession();
            
            if (!thisPtr->WriteSession_) {
                thisPtr->ErrorMessage_ = "Failed to create write session";
                return false;
            }
            
            bool success = thisPtr->WriteSession_->Write(message, std::nullopt, std::nullopt, TDuration::Seconds(10));
            return success;
        } catch (const std::exception& e) {
            thisPtr->ErrorMessage_ = TString(e.what());
            return false;
        }
    });
}

void TWriteMessageForm::CheckAsyncCompletion() {
    if (!Submitting_ || !SendFuture_.valid()) {
        return;
    }
    
    if (SendFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            bool success = SendFuture_.get();
            Submitting_ = false;
            
            if (success) {
                MessagesSent_++;
                SuccessMessage_ = Sprintf("Message sent! (%d total)", MessagesSent_);
                MessageData_.clear();  // Clear for next message
                ErrorMessage_.clear();
            } else if (ErrorMessage_.empty()) {
                ErrorMessage_ = "Failed to send message";
            }
        } catch (const std::exception& e) {
            Submitting_ = false;
            ErrorMessage_ = e.what();
        }
    } else {
        SpinnerFrame_++;
    }
}

} // namespace NYdb::NConsoleClient
