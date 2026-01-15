#pragma once

#include "../view_registry.h"

namespace NYdb::NConsoleClient {

class ITuiView : public IViewComponent {
public:
    ~ITuiView() override = default;

    // Build() inherited from IViewComponent
    // OnActivate() inherited from IViewComponent  
    // OnDeactivate() inherited from IViewComponent
    // Refresh() inherited from IViewComponent
};

} // namespace NYdb::NConsoleClient
