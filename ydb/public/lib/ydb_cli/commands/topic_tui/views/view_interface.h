#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>

namespace NYdb::NConsoleClient {

class ITuiView {
public:
    virtual ~ITuiView() = default;

    // Build the FTXUI component for this view
    virtual ftxui::Component Build() = 0;
    
    // Lifecycle hooks
    virtual void OnActivate() {}
    virtual void OnDeactivate() {}
    
    // Common actions
    virtual void Refresh() {}
};

} // namespace NYdb::NConsoleClient
