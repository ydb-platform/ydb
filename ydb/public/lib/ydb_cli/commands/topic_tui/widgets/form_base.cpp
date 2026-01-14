#include "form_base.h"
#include "../topic_tui_app.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TFormBase::TFormBase(TTopicTuiApp& app)
    : App_(app)
{}

Component TFormBase::Build() {
    auto container = BuildContainer();
    
    return Renderer(container, [this, container] {
        // Increment spinner frame each render
        if (Submitting_) {
            SpinnerFrame_++;
        }
        
        Elements content;
        
        // Title
        content.push_back(text(" " + std::string(GetTitle().c_str()) + " ") | bold | center);
        content.push_back(separator());
        
        // Form content (from subclass)
        content.push_back(RenderContent());
        
        // Error message if present
        if (!ErrorMessage_.empty()) {
            content.push_back(separator());
            content.push_back(text(" Error: " + std::string(ErrorMessage_.c_str())) 
                | color(NTheme::ErrorText));
        }
        
        // Success message if present
        if (!SuccessMessage_.empty()) {
            content.push_back(separator());
            content.push_back(text(" " + std::string(SuccessMessage_.c_str())) 
                | color(NTheme::SuccessText));
        }
        
        // Footer (only show if not submitting)
        if (!Submitting_) {
            content.push_back(separator());
            content.push_back(RenderFooter());
        }
        
        return vbox(content) | border | size(WIDTH, EQUAL, GetFormWidth()) | center;
        
    }) | CatchEvent([this](Event event) {
        // Only handle events when this form is active
        if (App_.GetState().CurrentView != GetViewType()) {
            return false;
        }
        
        // Mark that a form is capturing input (suppresses global shortcuts)
        App_.GetState().InputCaptureActive = true;
        
        // Ignore input while submitting
        if (Submitting_) {
            return true;
        }
        
        // Handle Escape - cancel form
        if (event == Event::Escape) {
            if (OnCancel) {
                OnCancel();
            }
            return true;
        }
        
        // Handle Enter - submit form
        if (event == Event::Return) {
            if (HandleSubmit()) {
                if (OnSuccess) {
                    OnSuccess();
                }
            }
            return true;
        }
        
        return false;
    });
}

void TFormBase::Reset() {
    ErrorMessage_.clear();
    SuccessMessage_.clear();
    Submitting_ = false;
    SpinnerFrame_ = 0;
}

Element TFormBase::LabeledInput(const std::string& label, Component input, int labelWidth) {
    return hbox({
        text(" " + label + ": ") | size(WIDTH, EQUAL, labelWidth),
        input->Render() | flex
    });
}

Element TFormBase::RenderSpinner(const std::string& message) {
    return NTheme::RenderSpinner(SpinnerFrame_, message) | center;
}

Element TFormBase::RenderFooter() {
    return NTheme::FormFooter(true);
}

} // namespace NYdb::NConsoleClient
