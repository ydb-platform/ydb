#pragma once

#include "../widgets/form_base.h"

#include <util/generic/string.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

class TOffsetForm : public TFormBase {
public:
    explicit TOffsetForm(TTopicTuiApp& app);
    
    void SetContext(const TString& topicPath, const TString& consumerName, 
                    ui64 partition, ui64 currentOffset, ui64 endOffset);
    void Reset() override;
    
    // Callback with the new offset value
    std::function<void(ui64 offset)> OnSubmitOffset;
    
protected:
    TString GetTitle() const override;
    EViewType GetViewType() const override;
    ftxui::Element RenderContent() override;
    bool HandleSubmit() override;
    int GetFormWidth() const override { return 50; }
    ftxui::Component BuildContainer() override;
    
private:
    TString TopicPath_;
    TString ConsumerName_;
    ui64 Partition_ = 0;
    ui64 CurrentOffset_ = 0;
    ui64 EndOffset_ = 0;
    
    std::string OffsetInput_;
    ftxui::Component OffsetInputComponent_;
};

} // namespace NYdb::NConsoleClient
