#pragma once

#include "app_context.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>

#include <unordered_map>
#include <memory>

namespace NYdb::NConsoleClient {

// =============================================================================
// IViewComponent - Unified interface for views and forms
// =============================================================================
// 
// Both ITuiView (main views) and TFormBase (modal forms) implement this
// interface to allow unified registry management.

class IViewComponent {
public:
    virtual ~IViewComponent() = default;
    
    // Build the FTXUI component for this view/form
    virtual ftxui::Component Build() = 0;
    
    // Called when this view becomes active
    virtual void OnActivate() {}
    
    // Called when leaving this view
    virtual void OnDeactivate() {}
    
    // Refresh data (views only, forms typically no-op)
    virtual void Refresh() {}
};

// =============================================================================
// TViewRegistry - Manages all views/forms by EViewType
// =============================================================================

class TViewRegistry {
public:
    using ViewPtr = std::shared_ptr<IViewComponent>;
    
    // Register a view/form for a given type
    void Register(EViewType type, ViewPtr view) {
        Views_[type] = std::move(view);
    }
    
    // Get view by type (returns nullptr if not found)
    ViewPtr Get(EViewType type) const {
        auto it = Views_.find(type);
        return (it != Views_.end()) ? it->second : nullptr;
    }
    
    // Get typed view (for views that need specific methods)
    template<typename T>
    std::shared_ptr<T> GetAs(EViewType type) const {
        return std::dynamic_pointer_cast<T>(Get(type));
    }
    
    // Get all registered types
    const std::unordered_map<EViewType, ViewPtr>& All() const {
        return Views_;
    }
    
    // Check if a type is registered
    bool Has(EViewType type) const {
        return Views_.count(type) > 0;
    }
    
    // Build all registered views and cache their components
    void BuildAll() {
        for (const auto& [type, view] : Views_) {
            if (view) {
                Components_[type] = view->Build();
            }
        }
    }
    
    // Get built component for a view type
    ftxui::Component GetComponent(EViewType type) const {
        auto it = Components_.find(type);
        return (it != Components_.end()) ? it->second : nullptr;
    }
    
    // Refresh a specific view
    void Refresh(EViewType type) {
        auto view = Get(type);
        if (view) {
            view->Refresh();
        }
    }
    
    // Refresh all views that support it
    void RefreshAll() {
        for (const auto& [type, view] : Views_) {
            if (view) {
                view->Refresh();
            }
        }
    }
    
private:
    std::unordered_map<EViewType, ViewPtr> Views_;
    std::unordered_map<EViewType, ftxui::Component> Components_;
};

} // namespace NYdb::NConsoleClient
