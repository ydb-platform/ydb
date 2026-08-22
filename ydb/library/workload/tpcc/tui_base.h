#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

#include <thread>

namespace NYdb::NTPCC {

// Base class for FTXUI-based TUI screens providing common lifecycle:
// - owns ScreenInteractive in fullscreen mode
// - starts a UI thread that runs Screen.Loop(BuildComponent())
// - requests global stop after loop exits
// - exits screen and joins thread in destructor
class TuiBase {
public:
    virtual ~TuiBase();

protected:
    TuiBase();

    // Start the UI loop in a background thread. Should be called by derived
    // classes at the end of their constructors, after all members are ready.
    void StartLoop();

    // Derived classes must implement the UI component tree.
    virtual ftxui::Component BuildComponent() = 0;

protected:
    ftxui::ScreenInteractive Screen;
    std::thread TuiThread;
};

} // namespace NYdb::NTPCC
