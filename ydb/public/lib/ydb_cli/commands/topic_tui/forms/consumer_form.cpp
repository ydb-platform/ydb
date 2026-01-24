#include "consumer_form.h"
#include "../app_interface.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <algorithm>
#include <util/string/builder.h>

using namespace ftxui;

namespace NYdb::NConsoleClient {

namespace {

bool HasCodec(const TVector<NTopic::ECodec>& codecs, NTopic::ECodec codec) {
    return std::find(codecs.begin(), codecs.end(), codec) != codecs.end();
}

TString FormatCodecs(const TVector<NTopic::ECodec>& codecs) {
    TStringBuilder out;
    for (size_t i = 0; i < codecs.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        switch (codecs[i]) {
            case NTopic::ECodec::RAW: out << "RAW"; break;
            case NTopic::ECodec::GZIP: out << "GZIP"; break;
            case NTopic::ECodec::ZSTD: out << "ZSTD"; break;
            case NTopic::ECodec::LZOP: out << "LZOP"; break;
            default: out << "UNKNOWN"; break;
        }
    }
    return out;
}

} // namespace

TConsumerForm::TConsumerForm(ITuiApp& app)
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
    InputOption nameOpt;
    nameOpt.content = &NameInput_;
    nameOpt.cursor_position = &NameCursor_;
    nameOpt.multiline = false;
    
    NameInputComponent_ = Input(nameOpt);
    ImportantCheckbox_ = Checkbox(" Important", &Important_);
    RawCheckbox_ = Checkbox(" RAW", &CodecRaw_);
    GzipCheckbox_ = Checkbox(" GZIP", &CodecGzip_);
    ZstdCheckbox_ = Checkbox(" ZSTD", &CodecZstd_);
    LzopCheckbox_ = Checkbox(" LZOP", &CodecLzop_);
    
    return Container::Vertical({
        NameInputComponent_,
        ImportantCheckbox_,
        Container::Horizontal({RawCheckbox_, GzipCheckbox_, ZstdCheckbox_, LzopCheckbox_})
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
            ZstdCheckbox_->Render(),
            text(" "),
            LzopCheckbox_->Render()
        })
    });
}

bool TConsumerForm::HandleSubmit() {
    if (NameInput_.empty()) {
        ErrorMessage_ = "Consumer name is required";
        return false;
    }

    if (!TopicSupportedCodecs_.empty()) {
        bool missing = false;
        if (HasCodec(TopicSupportedCodecs_, NTopic::ECodec::RAW) && !CodecRaw_) missing = true;
        if (HasCodec(TopicSupportedCodecs_, NTopic::ECodec::GZIP) && !CodecGzip_) missing = true;
        if (HasCodec(TopicSupportedCodecs_, NTopic::ECodec::ZSTD) && !CodecZstd_) missing = true;
        if (HasCodec(TopicSupportedCodecs_, NTopic::ECodec::LZOP) && !CodecLzop_) missing = true;
        if (missing) {
            ErrorMessage_ = TStringBuilder() << "Consumer must support all topic codecs: "
                                             << FormatCodecs(TopicSupportedCodecs_);
            return false;
        }
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
    bool codecLzop = CodecLzop_;
    TString topicPath = TopicPath_;
    auto* topicClient = &GetApp().GetTopicClient();
    
    SubmitFuture_ = std::async(std::launch::async, 
        [topicPath, topicClient, name, important, codecRaw, codecGzip, codecZstd, codecLzop]() -> TStatus {
            NTopic::TAlterTopicSettings settings;
            
            auto& consumer = settings.BeginAddConsumer(name);
            consumer.SetImportant(important);
            
            std::vector<NTopic::ECodec> codecs;
            if (codecRaw) codecs.push_back(NTopic::ECodec::RAW);
            if (codecGzip) codecs.push_back(NTopic::ECodec::GZIP);
            if (codecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
            if (codecLzop) codecs.push_back(NTopic::ECodec::LZOP);
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

void TConsumerForm::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    Reset();

    TopicSupportedCodecs_.clear();
    auto result = GetApp().GetTopicClient().DescribeTopic(topicPath).GetValueSync();
    if (result.IsSuccess()) {
        const auto& codecs = result.GetTopicDescription().GetSupportedCodecs();
        TopicSupportedCodecs_.assign(codecs.begin(), codecs.end());
        if (!TopicSupportedCodecs_.empty()) {
            CodecRaw_ = false;
            CodecGzip_ = false;
            CodecZstd_ = false;
            CodecLzop_ = false;
            for (auto codec : TopicSupportedCodecs_) {
                switch (codec) {
                    case NTopic::ECodec::RAW: CodecRaw_ = true; break;
                    case NTopic::ECodec::GZIP: CodecGzip_ = true; break;
                    case NTopic::ECodec::ZSTD: CodecZstd_ = true; break;
                    case NTopic::ECodec::LZOP: CodecLzop_ = true; break;
                    default: break;
                }
            }
        }
    } else {
        ErrorMessage_ = result.GetIssues().ToString();
    }

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
    CodecLzop_ = false;
}

} // namespace NYdb::NConsoleClient
