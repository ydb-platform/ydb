#include "topic_tui_app.h"

#include "views/topic_list_view.h"
#include "views/topic_details_view.h"
#include "views/consumer_view.h"
#include "views/message_preview_view.h"
#include "views/charts_view.h"
#include "widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicTuiApp::TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint)
    : SchemeClient_(std::make_unique<NScheme::TSchemeClient>(driver))
    , TopicClient_(std::make_unique<NTopic::TTopicClient>(driver))
    , Screen_(ScreenInteractive::Fullscreen())
    , RefreshRate_(refreshRate)
    , ViewerEndpoint_(viewerEndpoint)
{
    State_.CurrentPath = startPath;
    
    // Create views
    TopicListView_ = std::make_shared<TTopicListView>(*this);
    TopicDetailsView_ = std::make_shared<TTopicDetailsView>(*this);
    ConsumerView_ = std::make_shared<TConsumerView>(*this);
    MessagePreviewView_ = std::make_shared<TMessagePreviewView>(*this);
    ChartsView_ = std::make_shared<TChartsView>(*this);
    
    // Wire up callbacks
    TopicListView_->OnTopicSelected = [this](const TString& path) {
        State_.SelectedTopic = path;
        TopicDetailsView_->SetTopic(path);
        NavigateTo(EViewType::TopicDetails);
    };
    
    TopicListView_->OnDirectorySelected = [this](const TString& path) {
        State_.CurrentPath = path;
        TopicListView_->Refresh();
    };
    
    TopicDetailsView_->OnConsumerSelected = [this](const TString& consumer) {
        State_.SelectedConsumer = consumer;
        ConsumerView_->SetConsumer(State_.SelectedTopic, consumer);
        NavigateTo(EViewType::ConsumerDetails);
    };
    
    TopicDetailsView_->OnShowMessages = [this]() {
        MessagePreviewView_->SetTopic(State_.SelectedTopic, State_.SelectedPartition, 0);
        NavigateTo(EViewType::MessagePreview);
    };
    
    TopicDetailsView_->OnBack = [this]() {
        NavigateBack();
    };
    
    ConsumerView_->OnBack = [this]() {
        NavigateBack();
    };
    
    MessagePreviewView_->OnBack = [this]() {
        NavigateBack();
    };
    
    ChartsView_->OnBack = [this]() {
        NavigateBack();
    };
}

TTopicTuiApp::~TTopicTuiApp() {
    StopRefreshThread();
}

int TTopicTuiApp::Run() {
    // Load initial entries
    TopicListView_->Refresh();
    
    StartRefreshThread();
    
    auto mainComponent = BuildMainComponent();
    Screen_.Loop(mainComponent);
    
    StopRefreshThread();
    return 0;
}

void TTopicTuiApp::NavigateTo(EViewType view) {
    State_.CurrentView = view;
    Screen_.PostEvent(Event::Custom);
}

void TTopicTuiApp::NavigateBack() {
    switch (State_.CurrentView) {
        case EViewType::TopicDetails:
            State_.CurrentView = EViewType::TopicList;
            break;
        case EViewType::ConsumerDetails:
            State_.CurrentView = EViewType::TopicDetails;
            break;
        case EViewType::MessagePreview:
            State_.CurrentView = EViewType::TopicDetails;
            break;
        case EViewType::Charts:
            State_.CurrentView = EViewType::TopicDetails;
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
    State_.ShouldExit = true;
    Screen_.Exit();
}

Component TTopicTuiApp::BuildMainComponent() {
    auto topicListComponent = TopicListView_->Build();
    auto topicDetailsComponent = TopicDetailsView_->Build();
    auto consumerComponent = ConsumerView_->Build();
    auto messagePreviewComponent = MessagePreviewView_->Build();
    auto chartsComponent = ChartsView_->Build();
    
    // Use a simple container - we'll switch rendering manually
    auto container = Container::Stacked({
        topicListComponent,
        topicDetailsComponent,
        consumerComponent,
        messagePreviewComponent,
        chartsComponent
    });
    
    // Add global key handling first, then let events flow to children
    auto withGlobalKeys = CatchEvent(container, [this](Event event) {
        // Global key handling - only handle if not consumed by children
        if (event == Event::Character('q') || event == Event::Character('Q')) {
            RequestExit();
            return true;
        }
        if (event == Event::Escape) {
            if (State_.CurrentView != EViewType::TopicList) {
                NavigateBack();
                return true;
            }
        }
        if (event == Event::Character('r') || event == Event::Character('R')) {
            switch (State_.CurrentView) {
                case EViewType::TopicList:
                    TopicListView_->Refresh();
                    break;
                case EViewType::TopicDetails:
                    TopicDetailsView_->Refresh();
                    break;
                case EViewType::ConsumerDetails:
                    ConsumerView_->Refresh();
                    break;
                case EViewType::Charts:
                    ChartsView_->Refresh();
                    break;
                default:
                    break;
            }
            return true;
        }
        return false;
    });
    
    return Renderer(withGlobalKeys, [=, this] {
        Element content;
        
        switch (State_.CurrentView) {
            case EViewType::TopicList:
                content = topicListComponent->Render();
                break;
            case EViewType::TopicDetails:
                content = topicDetailsComponent->Render();
                break;
            case EViewType::ConsumerDetails:
                content = consumerComponent->Render();
                break;
            case EViewType::MessagePreview:
                content = messagePreviewComponent->Render();
                break;
            case EViewType::Charts:
                content = chartsComponent->Render();
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
        
        return vbox({
            header,
            content | flex,
            footer
        });
    });
}

Component TTopicTuiApp::BuildHelpBar() {
    return Renderer([this] {
        Elements parts;
        
        switch (State_.CurrentView) {
            case EViewType::TopicList:
                parts = {
                    text(" [↑↓] Navigate ") | dim,
                    text(" [Enter] Select ") | dim,
                    text(" [c] Create ") | color(Color::Green),
                    text(" [e] Edit ") | color(Color::Yellow),
                    text(" [d] Delete ") | color(Color::Red),
                    text(" [r] Refresh ") | dim
                };
                break;
            case EViewType::TopicDetails:
                parts = {
                    text(" [↑↓] Navigate ") | dim,
                    text(" [Tab] Switch Panel ") | dim,
                    text(" [m] Messages ") | color(Color::Cyan),
                    text(" [w] Write ") | color(Color::Green),
                    text(" [a] Add Consumer ") | color(Color::Green),
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::ConsumerDetails:
                parts = {
                    text(" [↑↓] Navigate ") | dim,
                    text(" [o] Commit Offset ") | color(Color::Yellow),
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::MessagePreview:
                parts = {
                    text(" [←→] Navigate Pages ") | dim,
                    text(" [g] Go to Offset ") | dim,
                    text(" [Enter] Expand ") | dim,
                    text(" [Esc] Back ") | dim
                };
                break;
            case EViewType::Charts:
                parts = {
                    text(" [r] Refresh ") | dim,
                    text(" [Esc] Back ") | dim
                };
                break;
        }
        
        return hbox(parts) | bgcolor(Color::GrayDark);
    });
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
