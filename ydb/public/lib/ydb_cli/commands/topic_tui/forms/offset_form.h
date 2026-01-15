#pragma once

#include "../widgets/form_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

#include <future>

namespace NYdb::NConsoleClient {

class ITuiApp;

class TOffsetForm : public TFormBase {
public:
    explicit TOffsetForm(ITuiApp& app);
    
    void SetContext(const TString& topicPath, const TString& consumerName, 
                    ui64 partition, ui64 currentOffset, ui64 endOffset);
    void Reset() override;
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 50; }
    ftxui::Component BuildContainer() override;
    
private:
    void DoAsyncCommit();
    void CheckAsyncCompletion();
    
    TString TopicPath_;
    TString ConsumerName_;
    ui64 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    ui64 EndOffset_ = 0;
    
    std::string OffsetInput_;
    int OffsetCursor_ = 0;  // Cursor position for Input
    ftxui::Component OffsetInputComponent_;
    
    // Async operation state
    std::future<TStatus> CommitFuture_;
};

} // namespace NYdb::NConsoleClient
