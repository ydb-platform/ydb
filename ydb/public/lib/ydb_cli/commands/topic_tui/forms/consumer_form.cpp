#include "consumer_form.h"
#include "../topic_tui_app.h"

using namespace ftxui;

namespace NYdb::NConsoleClient {

TConsumerForm::TConsumerForm(TTopicTuiApp& app)
    : TFormBase(app)
{
    Reset();
}

TString TConsumerForm::GetTitle() const {
    return "Add Consumer to Topic";
}

EViewType TConsumerForm::GetViewType() const {
    return EViewType::ConsumerForm;
}

Component TConsumerForm::BuildContainer() {
    NameInputComponent_ = Input(&NameInput_, "consumer-name");
    ImportantCheckbox_ = Checkbox(" Important", &Important_);
    RawCheckbox_ = Checkbox(" RAW", &CodecRaw_);
    GzipCheckbox_ = Checkbox(" GZIP", &CodecGzip_);
    ZstdCheckbox_ = Checkbox(" ZSTD", &CodecZstd_);
    
    return Container::Vertical({
        NameInputComponent_,
        ImportantCheckbox_,
        Container::Horizontal({RawCheckbox_, GzipCheckbox_, ZstdCheckbox_})
    });
}

Element TConsumerForm::RenderContent() {
    // Check async completion each render
    CheckAsyncCompletion();
    
    if (Submitting_) {
        return RenderSpinner("Adding consumer...");
    }
    
    return vbox({
        text(" " + std::string(TopicPath_.c_str()) + " ") | dim | center,
        separator(),
        hbox({text(" Name: ") | size(WIDTH, EQUAL, 12), NameInputComponent_->Render() | flex}),
        separator(),
        NTheme::SectionHeader("Options"),
        ImportantCheckbox_->Render(),
        separator(),
        NTheme::SectionHeader("Supported Codecs"),
        hbox({
            RawCheckbox_->Render(),
            text(" "),
            GzipCheckbox_->Render(),
            text(" "),
            ZstdCheckbox_->Render()
        })
    });
}

bool TConsumerForm::HandleSubmit() {
    if (NameInput_.empty()) {
        ErrorMessage_ = "Consumer name is required";
        return false;
    }
    
    ErrorMessage_.clear();
    SuccessMessage_.clear();
    Submitting_ = true;
    SpinnerFrame_ = 0;
    
    DoAsyncSubmit();
    return false;  // Don't close - wait for async completion
}

void TConsumerForm::DoAsyncSubmit() {
    std::string name = NameInput_;
    bool important = Important_;
    bool codecRaw = CodecRaw_;
    bool codecGzip = CodecGzip_;
    bool codecZstd = CodecZstd_;
    TString topicPath = TopicPath_;
    auto* topicClient = &GetApp().GetTopicClient();
    
    SubmitFuture_ = std::async(std::launch::async, 
        [topicPath, topicClient, name, important, codecRaw, codecGzip, codecZstd]() -> TStatus {
            NTopic::TAlterTopicSettings settings;
            
            auto& consumer = settings.BeginAddConsumer(name);
            consumer.SetImportant(important);
            
            std::vector<NTopic::ECodec> codecs;
            if (codecRaw) codecs.push_back(NTopic::ECodec::RAW);
            if (codecGzip) codecs.push_back(NTopic::ECodec::GZIP);
            if (codecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
            consumer.SetSupportedCodecs(codecs);
            consumer.EndAddConsumer();
            
            return topicClient->AlterTopic(topicPath, settings).GetValueSync();
        });
}

void TConsumerForm::CheckAsyncCompletion() {
    if (!Submitting_ || !SubmitFuture_.valid()) {
        return;
    }
    
    if (SubmitFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto result = SubmitFuture_.get();
            Submitting_ = false;
            
            if (result.IsSuccess()) {
                SuccessMessage_ = "Consumer added successfully!";
                if (OnSuccess) {
                    OnSuccess();
                }
            } else {
                ErrorMessage_ = result.GetIssues().ToString();
            }
        } catch (const std::exception& e) {
            Submitting_ = false;
            ErrorMessage_ = e.what();
        }
    } else {
        SpinnerFrame_++;
    }
}

void TConsumerForm::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    Reset();
    // Auto-focus on name input
    if (NameInputComponent_) {
        NameInputComponent_->TakeFocus();
    }
}

void TConsumerForm::Reset() {
    TFormBase::Reset();
    NameInput_.clear();
    Important_ = false;
    CodecRaw_ = true;
    CodecGzip_ = true;
    CodecZstd_ = false;
}

} // namespace NYdb::NConsoleClient
