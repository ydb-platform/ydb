#pragma once

#include "run_display.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <memory>
#include <thread>

namespace NKvVolumeStress {

class TRunTui {
public:
    explicit TRunTui(std::shared_ptr<TRunDisplayData> data);
    ~TRunTui();

    void Update(std::shared_ptr<TRunDisplayData> data);

private:
    ftxui::Element BuildHeaderPart();
    ftxui::Element BuildActionsPart();
    ftxui::Element BuildWorkersPart();

    ftxui::Component BuildComponent();

private:
    ftxui::ScreenInteractive Screen_;
    std::thread Thread_;
    std::shared_ptr<TRunDisplayData> DataToDisplay_;
};

} // namespace NKvVolumeStress
