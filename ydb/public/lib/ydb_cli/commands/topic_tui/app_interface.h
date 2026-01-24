#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include "app_context.h"

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NConsoleClient {

class ITuiApp {
public:
    virtual ~ITuiApp() = default;

    // SDK Access
    virtual NScheme::TSchemeClient& GetSchemeClient() = 0;
    virtual NTopic::TTopicClient& GetTopicClient() = 0;
    
    // State Access
    virtual TAppState& GetState() = 0;
    virtual const TString& GetViewerEndpoint() const = 0;
    virtual const TString& GetDatabaseRoot() const = 0;
    virtual TDuration GetRefreshRate() const = 0;
    virtual ui64 GetMessageSizeLimit() const = 0;
    
    // Navigation & UI
    virtual void NavigateTo(EViewType view) = 0;
    virtual void NavigateBack() = 0;
    virtual void ShowError(const TString& message) = 0;
    virtual void RequestRefresh() = 0;
    virtual void RequestExit() = 0;
    virtual bool IsExiting() const = 0;  // Check if app is shutting down
    virtual void PostRefresh() = 0; // Trigger UI redraw
    
    // View Target Configuration (for navigation between views)
    // These methods configure the destination view before calling NavigateTo
    virtual void SetTopicDetailsTarget(const TString& topicPath) = 0;
    virtual void SetConsumerViewTarget(const TString& topicPath, const TString& consumerName) = 0;
    virtual void SetMessagePreviewTarget(const TString& topicPath, ui32 partition, i64 offset) = 0;
    virtual void SetTopicFormCreateMode(const TString& parentPath) = 0;
    virtual void SetTopicFormEditMode(const TString& topicPath) = 0;
    virtual void SetDeleteConfirmTarget(const TString& path) = 0;
    virtual void SetDropConsumerTarget(const TString& topicPath, const TString& consumerName) = 0;
    virtual void SetEditConsumerTarget(const TString& topicPath, const TString& consumerName) = 0;
    virtual void SetWriteMessageTarget(const TString& topicPath, std::optional<ui32> partition) = 0;
    virtual void SetConsumerFormTarget(const TString& topicPath) = 0;
    virtual void SetOffsetFormTarget(const TString& topicPath, const TString& consumerName,
                                      ui64 partition, ui64 currentOffset, ui64 endOffset) = 0;
    virtual void SetTopicInfoTarget(const TString& topicPath) = 0;
    
    // Formatting
    virtual TString GetRefreshRateLabel() const = 0;
    virtual void CycleRefreshRate() = 0;
};

} // namespace NYdb::NConsoleClient
