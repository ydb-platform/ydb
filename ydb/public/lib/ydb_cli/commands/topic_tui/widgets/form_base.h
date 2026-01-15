#pragma once

#include "theme.h"
#include "../view_registry.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/string.h>

#include <future>

namespace NYdb::NConsoleClient {

class ITuiApp;

// =============================================================================
// TFormBase - Base class for all modal forms
// =============================================================================
// 
// Provides:
// - Unified Enter/Escape event handling
// - View state checking (only processes events when active)
// - Standard form layout (title, content, footer)
// - Loading/submitting state with spinner
// - Error/success message display
//
// Subclasses implement:
// - GetTitle() - form title
// - GetViewType() - which EViewType this form corresponds to
// - RenderContent() - the form fields
// - GetFormWidth() - optional, defaults to 55
// - HandleSubmit() - validation and submission logic
//

class TFormBase : public IViewComponent {
public:
    explicit TFormBase(ITuiApp& app);
    ~TFormBase() override = default;
    
    // Build the complete form component
    // This is a template method - subclasses should NOT override
    ftxui::Component Build() override;
    
    // Reset form to initial state
    virtual void Reset();
    
protected:
    // === Subclass Interface ===
    
    // Returns the form title displayed at the top
    virtual TString GetTitle() const = 0;
    
    // Returns which view type this form represents (for event filtering)
    virtual EViewType GetViewType() const = 0;
    
    // Renders the form content (fields, checkboxes, etc.)
    // Should NOT include title, footer, or border - those are added by Build()
    virtual ftxui::Element RenderContent() = 0;
    
    // Called when Enter is pressed. Return true if form should close.
    // Subclass should set ErrorMessage_ if validation fails.
    virtual bool HandleSubmit() = 0;
    
    // Optional: form width (default 55)
    virtual int GetFormWidth() const { return 55; }
    
    // Optional: custom footer (default shows standard submit/cancel)
    virtual ftxui::Element RenderFooter();
    
    // Optional: build the internal component container
    // Override to add focusable inputs, checkboxes, etc.
    virtual ftxui::Component BuildContainer() { 
        return ftxui::Container::Vertical({}); 
    }
    
    // === Helpers for Subclasses ===
    
    // Create a labeled input field
    ftxui::Element LabeledInput(const std::string& label, ftxui::Component input, 
                                 int labelWidth = NTheme::FormLabelWidth);
    
    // Show loading spinner (call from RenderContent when Submitting_)
    ftxui::Element RenderSpinner(const std::string& message);
    
    // Access to app
    ITuiApp& GetApp() { return App_; }
    
    // === State ===
    TString ErrorMessage_;
    TString SuccessMessage_;
    bool Submitting_ = false;
    int SpinnerFrame_ = 0;
    
private:
    ITuiApp& App_;
};

} // namespace NYdb::NConsoleClient
