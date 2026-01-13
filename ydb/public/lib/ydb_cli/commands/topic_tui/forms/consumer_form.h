#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TTopicTuiApp;

struct TConsumerFormData {
    TString Name;
    TMaybe<TInstant> ReadFrom;
    bool IsImportant = false;
    
    // Codecs
    bool CodecRaw = true;
    bool CodecGzip = true;
    bool CodecZstd = true;
    bool CodecLzop = false;
};

class TConsumerForm {
public:
    explicit TConsumerForm(TTopicTuiApp& app);
    
    ftxui::Component Build();
    void SetTopicPath(const TString& topicPath);
    void Reset();
    
    // Callbacks
    std::function<void(const TConsumerFormData& data)> OnSubmit;
    std::function<void()> OnCancel;
    
private:
    ftxui::Element RenderForm();
    
private:
    TTopicTuiApp& App_;
    TString TopicPath_;
    TConsumerFormData Data_;
    
    std::string NameInput_;
    std::string ReadFromInput_;
    
    int FocusedField_ = 0;
};

} // namespace NYdb::NConsoleClient
