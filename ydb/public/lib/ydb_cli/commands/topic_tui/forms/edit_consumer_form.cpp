#include "edit_consumer_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TEditConsumerForm::TEditConsumerForm(ITuiApp& app)
    : TFormBase(app)
{}

TString TEditConsumerForm::GetTitle() const {
    return "Edit Consumer";
}

EViewType TEditConsumerForm::GetViewType() const {
    return EViewType::EditConsumer;
}

Component TEditConsumerForm::BuildContainer() {
    ImportantCheckbox_ = Checkbox(" Important", &Important_);
    RawCheckbox_ = Checkbox(" RAW", &CodecRaw_);
    GzipCheckbox_ = Checkbox(" GZIP", &CodecGzip_);
    ZstdCheckbox_ = Checkbox(" ZSTD", &CodecZstd_);
    LzopCheckbox_ = Checkbox(" LZOP", &CodecLzop_);
    
    return Container::Vertical({
        ImportantCheckbox_,
        Container::Horizontal({RawCheckbox_, GzipCheckbox_, ZstdCheckbox_, LzopCheckbox_})
    });
}

Element TEditConsumerForm::RenderContent() {
    CheckAsyncCompletion();
    
    if (Submitting_) {
        return RenderSpinner("Updating consumer...");
    }
    
    return vbox({
        text(" Consumer: " + std::string(ConsumerName_.c_str())) | bold | center,
        text(" Topic: " + std::string(TopicPath_.c_str())) | dim | center,
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
            ZstdCheckbox_->Render(),
            text(" "),
            LzopCheckbox_->Render()
        })
    });
}

bool TEditConsumerForm::HandleSubmit() {
    ErrorMessage_.clear();
    SuccessMessage_.clear();
    Submitting_ = true;
    SpinnerFrame_ = 0;
    
    DoAsyncSubmit();
    return false;  // Don't close - wait for async completion
}

void TEditConsumerForm::DoAsyncSubmit() {
    bool important = Important_;
    bool codecRaw = CodecRaw_;
    bool codecGzip = CodecGzip_;
    bool codecZstd = CodecZstd_;
    bool codecLzop = CodecLzop_;
    TString topicPath = TopicPath_;
    TString consumerName = ConsumerName_;
    auto* topicClient = &GetApp().GetTopicClient();
    
    SubmitFuture_ = std::async(std::launch::async, 
        [topicPath, topicClient, consumerName, important, codecRaw, codecGzip, codecZstd, codecLzop]() -> TStatus {
            NTopic::TAlterTopicSettings settings;
            
            auto& consumer = settings.BeginAlterConsumer(std::string(consumerName.c_str()));
            consumer.SetImportant(important);
            
            std::vector<NTopic::ECodec> codecs;
            if (codecRaw) codecs.push_back(NTopic::ECodec::RAW);
            if (codecGzip) codecs.push_back(NTopic::ECodec::GZIP);
            if (codecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
            if (codecLzop) codecs.push_back(NTopic::ECodec::LZOP);
            consumer.SetSupportedCodecs(codecs);
            consumer.EndAlterConsumer();
            
            return topicClient->AlterTopic(topicPath, settings).GetValueSync();
        });
}

void TEditConsumerForm::CheckAsyncCompletion() {
    if (!Submitting_ || !SubmitFuture_.valid()) {
        return;
    }
    
    if (SubmitFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto result = SubmitFuture_.get();
            Submitting_ = false;
            
            if (result.IsSuccess()) {
                SuccessMessage_ = "Consumer updated successfully!";
                // Navigate back and refresh using ITuiApp interface
                GetApp().NavigateBack();
                GetApp().RequestRefresh();
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

void TEditConsumerForm::SetConsumer(const TString& topicPath, const TString& consumerName) {
    TopicPath_ = topicPath;
    ConsumerName_ = consumerName;
    Reset();
    
    // Fetch current consumer settings
    auto result = GetApp().GetTopicClient().DescribeConsumer(topicPath, consumerName).GetValueSync();
    if (result.IsSuccess()) {
        const auto& consumer = result.GetConsumerDescription().GetConsumer();
        Important_ = consumer.GetImportant();
        
        // Parse supported codecs
        CodecRaw_ = false;
        CodecGzip_ = false;
        CodecZstd_ = false;
        CodecLzop_ = false;
        for (auto codec : consumer.GetSupportedCodecs()) {
            switch (codec) {
                case NTopic::ECodec::RAW: CodecRaw_ = true; break;
                case NTopic::ECodec::GZIP: CodecGzip_ = true; break;
                case NTopic::ECodec::ZSTD: CodecZstd_ = true; break;
                case NTopic::ECodec::LZOP: CodecLzop_ = true; break;
                default: break;
            }
        }
    }
}

void TEditConsumerForm::Reset() {
    TFormBase::Reset();
    Important_ = false;
    CodecRaw_ = true;
    CodecGzip_ = true;
    CodecZstd_ = false;
    CodecLzop_ = false;
}

} // namespace NYdb::NConsoleClient
