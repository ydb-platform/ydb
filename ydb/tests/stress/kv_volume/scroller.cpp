#include "scroller.h"

#include <algorithm>
#include <memory>
#include <utility>

#include <ftxui/component/component.hpp>
#include <ftxui/component/component_base.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/mouse.hpp>
#include <ftxui/dom/deprecated.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/dom/requirement.hpp>
#include <ftxui/screen/box.hpp>

namespace ftxui {

class TScrollerBase : public ComponentBase {
public:
    TScrollerBase(Component child, const char* windowTitle)
        : WindowTitle_(windowTitle)
    {
        Add(child);
    }

private:
    Element OnRender() final {
        const auto focused = Focused() ? focus : select;
        const auto style = Focused() ? inverted : nothing;

        Element background = ComponentBase::Render();
        background->ComputeRequirement();
        Size_ = background->requirement().min_y;
        return window(text(WindowTitle_), dbox({
                   std::move(background),
                   vbox({
                       text(L"") | size(HEIGHT, EQUAL, Selected_),
                       text(L"") | style | focused,
                   }),
               })
               | vscroll_indicator | yframe | yflex | reflect(Box_));
    }

    bool OnEvent(Event event) final {
        if (event.is_mouse() && Box_.Contain(event.mouse().x, event.mouse().y)) {
            TakeFocus();
        }

        const int selectedOld = Selected_;
        if (event == Event::ArrowUp || event == Event::Character('k')
            || (event.is_mouse() && event.mouse().button == Mouse::WheelUp))
        {
            --Selected_;
        }
        if (event == Event::ArrowDown || event == Event::Character('j')
            || (event.is_mouse() && event.mouse().button == Mouse::WheelDown))
        {
            ++Selected_;
        }
        if (event == Event::PageDown) {
            Selected_ += Box_.y_max - Box_.y_min;
        }
        if (event == Event::PageUp) {
            Selected_ -= Box_.y_max - Box_.y_min;
        }
        if (event == Event::Home) {
            Selected_ = 0;
        }
        if (event == Event::End) {
            Selected_ = Size_;
        }

        Selected_ = std::max(0, std::min(Size_ - 1, Selected_));
        return selectedOld != Selected_;
    }

    bool Focusable() const final {
        return true;
    }

private:
    const char* WindowTitle_;
    int Selected_ = 0;
    int Size_ = 0;
    Box Box_;
};

Component Scroller(Component child, const char* windowTitle) {
    return Make<TScrollerBase>(std::move(child), windowTitle);
}

} // namespace ftxui
