#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <future>
#include <atomic>

namespace NYdb::NConsoleClient {

class ITuiApp;

class TConsumerForm : public TFormBase {
public:
    explicit TConsumerForm(ITuiApp& app);
    
    void SetTopic(const TString& topicPath);
    void Reset() override;
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 55; }
    ftxui::Component BuildContainer() override;
    
private:
    void CheckAsyncCompletion();
    void DoAsyncSubmit();
    
private:
    TString TopicPath_;
    
    // Form fields
    std::string NameInput_;
    int NameCursor_ = 0;  // Cursor position for Input
    bool Important_ = false;
    
    // Codec checkboxes
    bool CodecRaw_ = true;
    bool CodecGzip_ = true;
    bool CodecZstd_ = false;
    bool CodecLzop_ = false;

    TVector<NTopic::ECodec> TopicSupportedCodecs_;
    
    // Async state
    std::future<TStatus> SubmitFuture_;
    
    // Components for rendering
    ftxui::Component NameInputComponent_;
    ftxui::Component ImportantCheckbox_;
    ftxui::Component RawCheckbox_;
    ftxui::Component GzipCheckbox_;
    ftxui::Component ZstdCheckbox_;
    ftxui::Component LzopCheckbox_;
};

} // namespace NYdb::NConsoleClient
