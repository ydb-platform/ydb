#include "topic_tui_app.h"

#include "views/topic_list_view.h"
#include "views/topic_details_view.h"
#include "views/consumer_view.h"
#include "views/message_preview_view.h"
#include "views/charts_view.h"
#include "views/topic_info_view.h"
#include "forms/topic_form.h"
#include "forms/delete_confirm_form.h"
#include "forms/consumer_form.h"
#include "forms/write_message_form.h"
#include "forms/drop_consumer_form.h"
#include "forms/edit_consumer_form.h"
#include "forms/offset_form.h"
#include "widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicTuiApp::TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint,
                           ui64 messageSizeLimit, const TString& initialTopicPath,
                           std::optional<ui32> initialPartition, const TString& initialConsumer)
    : SchemeClient_(std::make_unique<NScheme::TSchemeClient>(driver))
    , TopicClient_(std::make_unique<NTopic::TTopicClient>(driver))
    , Screen_(ScreenInteractive::Fullscreen())
    , RefreshRate_(refreshRate)
    , ViewerEndpoint_(viewerEndpoint)
    , MessageSizeLimit_(messageSizeLimit)
    , DatabaseRoot_(startPath)
    , InitialTopicPath_(initialTopicPath)
    , InitialPartition_(initialPartition)
    , InitialConsumer_(initialConsumer)
{
    State_.CurrentPath = startPath;
    
    // Create views and register in registry
    TopicListView_ = std::make_shared<TTopicListView>(*this);
    TopicDetailsView_ = std::make_shared<TTopicDetailsView>(*this);
    ConsumerView_ = std::make_shared<TConsumerView>(*this);
    MessagePreviewView_ = std::make_shared<TMessagePreviewView>(*this);
    ChartsView_ = std::make_shared<TChartsView>(*this);
    TopicForm_ = std::make_shared<TTopicForm>(*this);
    DeleteConfirmForm_ = std::make_shared<TDeleteConfirmForm>(*this);
    ConsumerForm_ = std::make_shared<TConsumerForm>(*this);
    WriteMessageForm_ = std::make_shared<TWriteMessageForm>(*this);
    DropConsumerForm_ = std::make_shared<TDropConsumerForm>(*this);
    EditConsumerForm_ = std::make_shared<TEditConsumerForm>(*this);
    OffsetForm_ = std::make_shared<TOffsetForm>(*this);
    TopicInfoView_ = std::make_shared<TTopicInfoView>(*this);
    
    // Register all views/forms in registry for unified access
    ViewRegistry_.Register(EViewType::TopicList, TopicListView_);
    ViewRegistry_.Register(EViewType::TopicDetails, TopicDetailsView_);
    ViewRegistry_.Register(EViewType::ConsumerDetails, ConsumerView_);
    ViewRegistry_.Register(EViewType::MessagePreview, MessagePreviewView_);
    ViewRegistry_.Register(EViewType::Charts, ChartsView_);
    ViewRegistry_.Register(EViewType::TopicForm, TopicForm_);
    ViewRegistry_.Register(EViewType::DeleteConfirm, DeleteConfirmForm_);
    ViewRegistry_.Register(EViewType::ConsumerForm, ConsumerForm_);
    ViewRegistry_.Register(EViewType::WriteMessage, WriteMessageForm_);
    ViewRegistry_.Register(EViewType::DropConsumerConfirm, DropConsumerForm_);
    ViewRegistry_.Register(EViewType::EditConsumer, EditConsumerForm_);
    ViewRegistry_.Register(EViewType::OffsetForm, OffsetForm_);
    ViewRegistry_.Register(EViewType::TopicInfo, TopicInfoView_);
    
    // Note: All views now navigate directly via ITuiApp interface
    // Note: All forms now use ITuiApp interface directly
}

TTopicTuiApp::~TTopicTuiApp() {
    StopRefreshThread();
}

int TTopicTuiApp::Run() {
    // Handle direct navigation to topic or partition
    if (!InitialTopicPath_.empty()) {
        // Always verify the topic exists first
        auto future = TopicClient_->DescribeTopic(InitialTopicPath_);
        auto result = future.GetValueSync();
        if (result.IsSuccess()) {
            // It's a valid topic - navigate to topic details or partition
            State_.SelectedTopic = InitialTopicPath_;
            TopicDetailsView_->SetTopic(InitialTopicPath_);
            
            // Set parent directory for topic list context
            TStringBuf parent, discard;
            if (TStringBuf(InitialTopicPath_).TryRSplit('/', parent, discard)) {
                State_.CurrentPath = parent ? TString(parent) : "/";
            }
            
            if (InitialPartition_.has_value()) {
                // Navigate to partition message preview
                State_.SelectedPartition = InitialPartition_.value();
                MessagePreviewView_->SetTopic(InitialTopicPath_, InitialPartition_.value(), 0);
                State_.CurrentView = EViewType::MessagePreview;
            } else if (!InitialConsumer_.empty()) {
                // Navigate to consumer page
                State_.SelectedConsumer = InitialConsumer_;
                ConsumerView_->SetConsumer(InitialTopicPath_, InitialConsumer_);
                State_.CurrentView = EViewType::ConsumerDetails;
            } else {
                State_.CurrentView = EViewType::TopicDetails;
            }
        }
        // If DescribeTopic failed, it's a directory or invalid - Path_ is already set
    }
    
    // Load topic list (for the current directory context)
    ViewRegistry_.Refresh(EViewType::TopicList);
    
    StartRefreshThread();
    
    auto mainComponent = BuildMainComponent();
    Screen_.Loop(mainComponent);
    
    StopRefreshThread();
    return 0;
}

void TTopicTuiApp::NavigateTo(EViewType view) {
    // Track previous view for forms/dialogs so we can return properly
    // Also track for main views when navigating between them
    if (view == EViewType::TopicForm || view == EViewType::DeleteConfirm ||
        view == EViewType::ConsumerForm || view == EViewType::WriteMessage ||
        view == EViewType::DropConsumerConfirm || view == EViewType::EditConsumer ||
        view == EViewType::OffsetForm ||  // Added!
        view == EViewType::ConsumerDetails || view == EViewType::TopicDetails ||
        view == EViewType::MessagePreview || view == EViewType::Charts) {
        State_.PreviousView = State_.CurrentView;
    }
    
    State_.CurrentView = view;
    
    // Ensure forms have focus when navigating to them
    if (view == EViewType::TopicForm && TopicFormComponent_) {
        TopicFormComponent_->TakeFocus();
    } else if (view == EViewType::DeleteConfirm && DeleteConfirmComponent_) {
        DeleteConfirmComponent_->TakeFocus();
    } else if (view == EViewType::OffsetForm && OffsetFormComponent_) {
        OffsetFormComponent_->TakeFocus();
    }
    
    Screen_.PostEvent(Event::Custom);
}

void TTopicTuiApp::NavigateBack() {
    // Clear input capture state when navigating back from any view
    State_.InputCaptureActive = false;
    
    switch (State_.CurrentView) {
        case EViewType::TopicList:
            // Go to parent directory if possible
            if (TopicListView_) {
                TString current = State_.CurrentPath;
                TString dbRoot = DatabaseRoot_;
                // Only go up if we are deeper than root
                if (current.size() > dbRoot.size() && current.StartsWith(dbRoot)) {
                     TStringBuf parent, discard;
                     if (TStringBuf(current).TryRSplit('/', parent, discard)) {
                         // Ensure we don't go above db root
                         if (parent.size() >= dbRoot.size()) {
                             State_.CurrentPath = TString(parent);
                             ViewRegistry_.Refresh(EViewType::TopicList);
                         }
                     }
                }
            }
            break;
        case EViewType::TopicDetails:
            // Return to topic list for the current directory
            State_.CurrentView = EViewType::TopicList;
            if (TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            }
            break;
        case EViewType::ConsumerDetails:
            // Go back to where we came from (TopicDetails or TopicList via Go To)
            State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                ? EViewType::TopicList : EViewType::TopicDetails;
            if (State_.CurrentView == EViewType::TopicList && TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            } else if (State_.CurrentView == EViewType::TopicDetails && TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            }
            break;
        case EViewType::MessagePreview:
            State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                ? EViewType::TopicList : EViewType::TopicDetails;
            if (State_.CurrentView == EViewType::TopicList && TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            } else if (State_.CurrentView == EViewType::TopicDetails && TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            }
            break;
        case EViewType::TopicInfo:
            State_.CurrentView = EViewType::TopicDetails;
            if (TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            }
            break;
        case EViewType::TopicTablets:
            State_.CurrentView = EViewType::TopicDetails;
            if (TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            }
            break;
        case EViewType::Charts:
            State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                ? EViewType::TopicList : EViewType::TopicDetails;
            if (State_.CurrentView == EViewType::TopicList && TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            } else if (State_.CurrentView == EViewType::TopicDetails && TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            }
            break;
        // Forms and dialogs return to where user came from
        case EViewType::TopicForm:
        case EViewType::DeleteConfirm:
        case EViewType::ConsumerForm:
        case EViewType::WriteMessage:
        case EViewType::DropConsumerConfirm:
        case EViewType::EditConsumer:
        case EViewType::OffsetForm:
            State_.CurrentView = State_.PreviousView;
            // Restore focus to the appropriate component
            if (State_.CurrentView == EViewType::TopicList && TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            } else if (State_.CurrentView == EViewType::TopicDetails && TopicDetailsComponent_) {
                TopicDetailsComponent_->TakeFocus();
            } else if (State_.CurrentView == EViewType::ConsumerDetails && ConsumerComponent_) {
                ConsumerComponent_->TakeFocus();
            }
            break;
        default:
            break;
    }
    Screen_.PostEvent(Event::Custom);
}

void TTopicTuiApp::ShowError(const TString& message) {
    State_.LastError = message;
    Screen_.PostEvent(Event::Custom);
}

void TTopicTuiApp::RequestRefresh() {
    State_.ShouldRefresh = true;
}

void TTopicTuiApp::RequestExit() {
    Exiting_ = true;  // Signal all async operations to abort
    State_.ShouldExit = true;
    Screen_.Exit();
}

// View Target Configuration - These configure the destination view before NavigateTo()
void TTopicTuiApp::SetTopicDetailsTarget(const TString& topicPath) {
    State_.SelectedTopic = topicPath;
    TopicDetailsView_->SetTopic(topicPath);
}

void TTopicTuiApp::SetConsumerViewTarget(const TString& topicPath, const TString& consumerName) {
    State_.SelectedTopic = topicPath;
    State_.SelectedConsumer = consumerName;
    ConsumerView_->SetConsumer(topicPath, consumerName);
}

void TTopicTuiApp::SetMessagePreviewTarget(const TString& topicPath, ui32 partition, i64 offset) {
    State_.SelectedTopic = topicPath;
    State_.SelectedPartition = partition;
    MessagePreviewView_->SetTopic(topicPath, partition, offset);
}

void TTopicTuiApp::SetTopicFormCreateMode(const TString& parentPath) {
    TopicForm_->SetCreateMode(parentPath);
}

void TTopicTuiApp::SetTopicFormEditMode(const TString& topicPath) {
    // Fetch topic description then configure form
    auto future = TopicClient_->DescribeTopic(topicPath);
    auto result = future.GetValueSync();
    if (result.IsSuccess()) {
        TopicForm_->SetEditMode(topicPath, result.GetTopicDescription());
    } else {
        ShowError(result.GetIssues().ToString());
    }
}

void TTopicTuiApp::SetDeleteConfirmTarget(const TString& path) {
    DeleteConfirmForm_->SetTopic(path);
}

void TTopicTuiApp::SetDropConsumerTarget(const TString& topicPath, const TString& consumerName) {
    DropConsumerForm_->SetConsumer(topicPath, consumerName);
}

void TTopicTuiApp::SetEditConsumerTarget(const TString& topicPath, const TString& consumerName) {
    EditConsumerForm_->SetConsumer(topicPath, consumerName);
}

void TTopicTuiApp::SetWriteMessageTarget(const TString& topicPath, std::optional<ui32> partition) {
    WriteMessageForm_->SetTopic(topicPath, partition);
}

void TTopicTuiApp::SetConsumerFormTarget(const TString& topicPath) {
    ConsumerForm_->SetTopic(topicPath);
}

void TTopicTuiApp::SetOffsetFormTarget(const TString& topicPath, const TString& consumerName,
                                        ui64 partition, ui64 currentOffset, ui64 endOffset) {
    OffsetForm_->SetContext(topicPath, consumerName, partition, currentOffset, endOffset);
}

void TTopicTuiApp::SetTopicInfoTarget(const TString& topicPath) {
    TopicInfoView_->SetTopic(topicPath);
}

// Predefined refresh rates for cycling with 'R' key
static const TDuration RefreshRates[] = {
    TDuration::Seconds(1),
    TDuration::Seconds(2),
    TDuration::Seconds(5),
    TDuration::Seconds(10),
    TDuration::Max()  // "off" - no auto-refresh
};
static constexpr int NumRefreshRates = 5;

void TTopicTuiApp::CycleRefreshRate() {
    RefreshRateIndex_ = (RefreshRateIndex_ + 1) % NumRefreshRates;
    RefreshRate_ = RefreshRates[RefreshRateIndex_];
}

TString TTopicTuiApp::GetRefreshRateLabel() const {
    // Show actual refresh rate, not just predefined labels
    if (RefreshRate_ == TDuration::Max() || RefreshRate_ == TDuration::Zero()) {
        return "off";
    }
    ui64 secs = RefreshRate_.Seconds();
    if (secs >= 60) {
        return Sprintf("%dm", static_cast<int>(secs / 60));
    }
    return Sprintf("%ds", static_cast<int>(secs));
}

Component TTopicTuiApp::BuildMainComponent() {
    // Build all views via registry and cache their components
    ViewRegistry_.BuildAll();
    
    // Keep references to components that need focus management
    TopicListComponent_ = ViewRegistry_.GetComponent(EViewType::TopicList);
    TopicDetailsComponent_ = ViewRegistry_.GetComponent(EViewType::TopicDetails);
    ConsumerComponent_ = ViewRegistry_.GetComponent(EViewType::ConsumerDetails);
    TopicFormComponent_ = ViewRegistry_.GetComponent(EViewType::TopicForm);
    DeleteConfirmComponent_ = ViewRegistry_.GetComponent(EViewType::DeleteConfirm);
    OffsetFormComponent_ = ViewRegistry_.GetComponent(EViewType::OffsetForm);
    
    // Use a simple container - we'll switch rendering manually
    auto container = Container::Stacked({
        TopicListComponent_,
        TopicDetailsComponent_,
        ConsumerComponent_,
        ViewRegistry_.GetComponent(EViewType::MessagePreview),
        ViewRegistry_.GetComponent(EViewType::Charts),
        TopicFormComponent_,
        DeleteConfirmComponent_,
        ViewRegistry_.GetComponent(EViewType::ConsumerForm),
        ViewRegistry_.GetComponent(EViewType::WriteMessage),
        ViewRegistry_.GetComponent(EViewType::DropConsumerConfirm),
        ViewRegistry_.GetComponent(EViewType::EditConsumer),
        ViewRegistry_.GetComponent(EViewType::OffsetForm),
        ViewRegistry_.GetComponent(EViewType::TopicInfo)
    });
    
    // Use |= to intercept events BEFORE children process them (critical for Escape in WriteMessage)
    container |= CatchEvent([this](Event event) {
        // DEBUG: Log all events when in WriteMessage view
        if (State_.CurrentView == EViewType::WriteMessage) {
            std::string eventDesc;
            if (event == Event::Escape) eventDesc = "Escape";
            else if (event == Event::Return) eventDesc = "Return";
            else if (event.is_character()) eventDesc = "Char: " + event.character();
            else if (event.is_mouse()) eventDesc = "Mouse";
            else eventDesc = "Other";
            
            // Handle Escape for WriteMessage at app level FIRST
            bool isEscape = (event == Event::Escape) || 
                           (event.is_character() && event.character() == "\x1b");
            if (isEscape) {
                NavigateBack();
                return true;
            }
            return false;  // Let other keys go to form for typing
        }
        
        // Global key handling
        // Skip global shortcuts when a view is capturing text input
        if (State_.InputCaptureActive) {
            return false;  // Let views handle all keyboard input
        }
        
        // Help overlay takes priority
        if (State_.ShowHelpOverlay) {
            if (event == Event::Escape || event == Event::Return || 
                event == Event::Character('?') || event == Event::Character('q')) {
                State_.ShowHelpOverlay = false;
                return true;
            }
            return true;  // Consume all events when help is shown
        }
        
        // Toggle help overlay with '?'
        if (event == Event::Character('?')) {
            State_.ShowHelpOverlay = true;
            return true;
        }
        
        if (event == Event::Character('q') || event == Event::Character('Q')) {
            RequestExit();
            return true;
        }
        if (event == Event::Escape) {
            // Don't handle Esc globally for views that handle it themselves
            if (State_.CurrentView != EViewType::MessagePreview) {
                NavigateBack();
                return true;
            }
        }
        if (event == Event::Character('R')) {
            // Cycle refresh rate
            CycleRefreshRate();
            return true;
        }
        if (event == Event::Character('r')) {
            // Manual refresh via registry
            if (State_.CurrentView == EViewType::TopicTablets) {
                return false;
            }
            ViewRegistry_.Refresh(State_.CurrentView);
            return true;
        }
        return false;
    });
    
    auto withGlobalKeys = container;
    
    return Renderer(withGlobalKeys, [=, this] {
        Element content;
        
        // Note: Auto-refresh removed - use 'r' key for manual refresh
        // to avoid disruptive full-page reloads
        
        switch (State_.CurrentView) {
            case EViewType::TopicList:
            case EViewType::TopicDetails:
            case EViewType::ConsumerDetails:
            case EViewType::MessagePreview:
            case EViewType::Charts:
            case EViewType::TopicForm:
            case EViewType::DeleteConfirm:
            case EViewType::ConsumerForm:
            case EViewType::WriteMessage:
            case EViewType::DropConsumerConfirm:
            case EViewType::EditConsumer:
            case EViewType::OffsetForm:
            case EViewType::TopicInfo: {
                auto component = ViewRegistry_.GetComponent(State_.CurrentView);
                if (component) {
                    content = component->Render();
                }
                break;
            }
            case EViewType::TopicTablets:
                // TopicTablets not yet implemented, fall back to TopicDetailsComponent
                content = TopicDetailsComponent_->Render();
                break;
        }
        
        // Build header
        auto header = hbox({
            text(" YDB Topic TUI ") | bold | color(Color::Cyan),
            filler(),
            text(" Path: ") | dim,
            text(std::string(State_.CurrentPath.c_str())) | color(Color::White),
            filler(),
            text(" [q] Quit  [?] Help ") | dim
        }) | bgcolor(Color::GrayDark);
        
        // Build footer with error if any
        Element footer;
        if (!State_.LastError.empty()) {
            footer = hbox({
                text(" Error: ") | color(Color::Red) | bold,
                text(std::string(State_.LastError.c_str())) | color(Color::Red)
            }) | bgcolor(Color::GrayDark);
        } else {
            footer = BuildHelpBar()->Render();
        }
        
        // Build main layout
        auto mainLayout = vbox({
            header,
            content | flex,
            footer
        });
        
        // Overlay help if active
        if (State_.ShowHelpOverlay) {
            auto helpContent = RenderHelpOverlay();
            return dbox({
                mainLayout | dim,  // Dim the background
                helpContent | center
            });
        }
        
        return mainLayout;
    });
}

Component TTopicTuiApp::BuildHelpBar() {
    return Renderer([this] {
        Elements parts;
        
        switch (State_.CurrentView) {
            case EViewType::TopicList:
                if (State_.TopicListMode == ETopicListMode::GoToPath) {
                    parts = {
                        text(" [Type] Path ") | dim,
                        text(" [↑↓] Select ") | dim,
                        text(" [Tab] Complete ") | dim,
                        text(" [Backspace] Delete ") | dim,
                        text(" [Enter] Go ") | color(Color::Cyan),
                        text(" [Esc] Cancel ") | dim
                    };
                } else if (State_.TopicListMode == ETopicListMode::Search) {
                    parts = {
                        text(" [Type] Filter ") | dim,
                        text(" [↑↓] Select ") | dim,
                        text(" [Backspace] Delete ") | dim,
                        text(" [Enter] Open ") | color(Color::Cyan),
                        text(" [Esc] Clear ") | dim
                    };
                } else {
                    TString sortArrow = "↕";
                    if (TopicListView_) {
                        sortArrow = TopicListView_->GetSortDirectionArrow();
                    }
                    parts = {
                        text(" [↑↓] Navigate ") | dim,
                        text(" [Enter] Select ") | dim,
                        text(" [g] Go ") | color(Color::Cyan),
                        text(" [/] Search ") | color(Color::Cyan),
                        text(" [c] Create ") | color(Color::Green),
                        text(" [e] Edit ") | color(Color::Yellow),
                        text(" [d] Delete ") | color(Color::Red),
                        text(" [</>] Col ") | dim,
                        text(" [s] Sort " + std::string(sortArrow.c_str()) + " ") | dim,
                        text(" [r] Refresh ") | dim,
                        text(" [R] Rate: ") | dim,
                        text(std::string(GetRefreshRateLabel().c_str())) | color(Color::Magenta),
                        text(" [Esc] Back ") | dim
                    };
                }
                break;
            case EViewType::TopicDetails:
                parts = {
                    text(" [↑↓] Navigate ") | dim,
                    text(" [Tab] Switch ") | dim,
                    text(" [←→/hl] Scroll ") | dim,
                    text(" [Enter] Open ") | color(Color::Cyan),
                    text(State_.TopicDetailsFocusPanel == 1 ? " [e] Edit Consumer " : " [e] Edit Topic ") | color(Color::Yellow),
                    text(" [w] Write ") | color(Color::Green),
                    text(" [a] Add ") | color(Color::Green)
                };
                if (State_.TopicDetailsFocusPanel == 1) {
                    parts.push_back(text(" [x] Drop ") | color(Color::Red));
                }
                parts.push_back(text(" [s] Sort ↕ ") | dim);
                parts.push_back(text(" [t] Tablets ") | dim);
                parts.push_back(text(" [i] Info ") | dim);
                parts.push_back(text(" [r] Refresh ") | dim);
                parts.push_back(text(" [R] Rate: ") | dim);
                parts.push_back(text(std::string(GetRefreshRateLabel().c_str())) | color(Color::Magenta));
                parts.push_back(text(" [Esc] Back ") | dim);
                break;
            case EViewType::ConsumerDetails:
                parts = {
                    text(" [↑↓] Navigate ") | dim,
                    text(" [←→/hl] Scroll ") | dim,
                    text(" [o] Commit Offset ") | color(Color::Yellow),
                    text(" [d] Drop ") | color(Color::Red),
                    text(" [s] Sort ↕ ") | dim,
                    text(" [r] Refresh ") | dim,
                    text(" [R] Rate: ") | dim,
                    text(std::string(GetRefreshRateLabel().c_str())) | color(Color::Magenta),
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::TopicInfo:
                parts = {
                    text(" [↑↓/jk] Navigate ") | dim,
                    text(" [←→/hl] Expand/Collapse ") | dim,
                    text(" [r] Refresh ") | dim,
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::TopicTablets:
                parts = {
                    text(" [↑↓/jk] Scroll ") | dim,
                    text(" [r] Refresh ") | dim,
                    text(" [t/Esc] Close ") | dim
                };
                break;
            case EViewType::MessagePreview:
                parts = {
                    text(" [←→] Navigate Pages ") | dim,
                    text(" [↑↓/jk] Select/Scroll ") | dim,
                    text(" [Enter] Expand ") | dim,
                    text(" [g] Go to Offset ") | color(Color::Cyan),
                    text(" [t] Tail ") | color(Color::Green),
                    text(" [r] Refresh ") | dim,
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::Charts:
                parts = {
                    text(" [r] Refresh ") | dim,
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::TopicForm:
                parts = {
                    text(" [Enter] Submit ") | color(Color::Green),
                    text(" [Esc] Cancel ") | dim
                };
                break;
            case EViewType::DeleteConfirm:
                parts = {
                    text(" [Enter] Confirm (type name first) ") | color(Color::Red),
                    text(" [Esc] Cancel ") | dim
                };
                break;
            case EViewType::ConsumerForm:
                parts = {
                    text(" [Enter] Add Consumer ") | color(Color::Green),
                    text(" [Esc] Cancel ") | dim
                };
                break;
            case EViewType::WriteMessage:
                parts = {
                    text(" [Enter] Send Message ") | color(Color::Green),
                    text(" (form stays open for more) ") | dim,
                    text(" [Esc] Close ") | dim
                };
                break;
            case EViewType::OffsetForm:
                parts = {
                    text(" [Enter] Commit ") | color(Color::Yellow),
                    text(" [Esc] Cancel ") | dim
                };
                break;
            case EViewType::DropConsumerConfirm:
                parts = {
                    text(" [Enter] Drop (type name first) ") | color(Color::Red),
                    text(" [Esc] Cancel ") | dim
                };
                break;
            case EViewType::EditConsumer:
                parts = {
                    text(" [Enter] Save Changes ") | color(Color::Green),
                    text(" [Esc] Cancel ") | dim
                };
                break;
        }
        
        return hbox(parts) | bgcolor(Color::GrayDark);
    });
}

Element TTopicTuiApp::RenderHelpOverlay() {
    Elements lines;
    
    // Header
    lines.push_back(text(" Keyboard Shortcuts ") | bold | color(Color::Cyan) | center);
    lines.push_back(separator());
    
    // Global shortcuts
    lines.push_back(text(" Global ") | bold);
    lines.push_back(hbox({text("  q") | bold | color(Color::Yellow), text("       Quit application")}));
    lines.push_back(hbox({text("  ?") | bold | color(Color::Yellow), text("       Show this help")}));
    lines.push_back(hbox({text("  Esc") | bold | color(Color::Yellow), text("     Go back / Cancel")}));
    lines.push_back(hbox({text("  r") | bold | color(Color::Yellow), text("       Manual refresh")}));
    lines.push_back(hbox({text("  R") | bold | color(Color::Yellow), text("       Cycle refresh rate")}));
    lines.push_back(text(""));
    
    // View-specific shortcuts
    switch (State_.CurrentView) {
        case EViewType::TopicList:
            lines.push_back(text(" Explorer ") | bold);
            lines.push_back(hbox({text("  ↑/↓") | bold, text("     Navigate list")}));
            lines.push_back(hbox({text("  Enter") | bold, text("   Open item")}));
            lines.push_back(hbox({text("  g") | bold | color(Color::Cyan), text("       Go to path (with @consumer, :partition)")}));
            lines.push_back(hbox({text("  /") | bold | color(Color::Cyan), text("       Search/filter")}));
            lines.push_back(hbox({text("  c") | bold | color(Color::Green), text("       Create topic")}));
            lines.push_back(hbox({text("  e") | bold | color(Color::Yellow), text("       Edit topic")}));
            lines.push_back(hbox({text("  d") | bold | color(Color::Red), text("       Delete item")}));
            lines.push_back(hbox({text("  </>/s") | bold, text("   Sort columns / direction")}));
            break;
            
        case EViewType::TopicDetails:
            lines.push_back(text(" Topic Details ") | bold);
            lines.push_back(hbox({text("  ↑/↓") | bold, text("     Navigate tables")}));
            lines.push_back(hbox({text("  Tab") | bold, text("     Switch panels")}));
            lines.push_back(hbox({text("  ←/→, h/l") | bold, text("  Scroll columns")}));
            lines.push_back(hbox({text("  Enter") | bold | color(Color::Cyan), text("   Open consumer / messages")}));
            lines.push_back(hbox({text("  e") | bold | color(Color::Yellow), text("       Edit topic / consumer")}));
            lines.push_back(hbox({text("  w") | bold | color(Color::Green), text("       Write message")}));
            lines.push_back(hbox({text("  a") | bold | color(Color::Green), text("       Add consumer")}));
            lines.push_back(hbox({text("  x") | bold | color(Color::Red), text("       Drop consumer (Consumers pane)")}));
            lines.push_back(hbox({text("  s") | bold, text("       Toggle sort direction")}));
            lines.push_back(hbox({text("  i") | bold, text("       Topic info")}));
            lines.push_back(hbox({text("  t") | bold, text("       Tablets view")}));
            break;
            
        case EViewType::ConsumerDetails:
            lines.push_back(text(" Consumer Details ") | bold);
            lines.push_back(hbox({text("  ↑/↓") | bold, text("     Navigate partitions")}));
            lines.push_back(hbox({text("  ←/→, h/l") | bold, text("  Scroll columns")}));
            lines.push_back(hbox({text("  o") | bold | color(Color::Yellow), text("       Commit offset")}));
            lines.push_back(hbox({text("  d") | bold | color(Color::Red), text("       Drop consumer")}));
            lines.push_back(hbox({text("  s") | bold, text("       Toggle sort direction")}));
            break;
            
        case EViewType::MessagePreview:
            lines.push_back(text(" Message Preview ") | bold);
            lines.push_back(hbox({text("  ←/→") | bold, text("     Previous/Next page")}));
            lines.push_back(hbox({text("  ↑/↓, j/k") | bold, text("  Select message / scroll (expanded)")}));
            lines.push_back(hbox({text("  Enter") | bold, text("   Expand/collapse message")}));
            lines.push_back(hbox({text("  t") | bold | color(Color::Green), text("       Toggle tail mode")}));
            lines.push_back(hbox({text("  g") | bold | color(Color::Cyan), text("       Go to offset")}));
            break;
            
        case EViewType::TopicInfo:
            lines.push_back(text(" Topic Info ") | bold);
            lines.push_back(hbox({text("  ↑/↓, j/k") | bold, text("  Navigate JSON")}));
            lines.push_back(hbox({text("  ←/→, h/l") | bold, text("  Collapse/expand")}));
            lines.push_back(hbox({text("  Esc") | bold, text("     Back")}));
            break;

        case EViewType::TopicTablets:
            lines.push_back(text(" Topic Tablets ") | bold);
            lines.push_back(hbox({text("  ↑/↓") | bold, text("     Scroll list")}));
            lines.push_back(hbox({text("  r") | bold, text("       Refresh")}));
            lines.push_back(hbox({text("  t/Esc") | bold, text("   Close")}));
            break;

        case EViewType::Charts:
            lines.push_back(text(" Charts ") | bold);
            lines.push_back(hbox({text("  r") | bold, text("       Refresh")}));
            break;
            
        default:
            lines.push_back(text(" Context-specific shortcuts available ") | dim);
            break;
    }
    
    lines.push_back(text(""));
    lines.push_back(separator());
    lines.push_back(text(" Press Esc, Enter, or ? to close ") | dim | center);
    
    return vbox(lines) | border | bgcolor(Color::GrayDark) | size(WIDTH, GREATER_THAN, 50);
}

void TTopicTuiApp::StartRefreshThread() {
    RefreshThreadRunning_ = true;
    RefreshThread_ = std::thread([this] {
        while (RefreshThreadRunning_) {
            // Use short interval (100ms) for responsive spinner animation
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Always post event to allow spinner animation and async completion checks
            Screen_.PostEvent(Event::Custom);
        }
    });
}

void TTopicTuiApp::StopRefreshThread() {
    RefreshThreadRunning_ = false;
    if (RefreshThread_.joinable()) {
        RefreshThread_.join();
    }
}

} // namespace NYdb::NConsoleClient
