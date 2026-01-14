#include "topic_tui_app.h"

#include "views/topic_list_view.h"
#include "views/topic_details_view.h"
#include "views/consumer_view.h"
#include "views/message_preview_view.h"
#include "views/charts_view.h"
#include "forms/topic_form.h"
#include "forms/delete_confirm_form.h"
#include "forms/consumer_form.h"
#include "forms/write_message_form.h"
#include "widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicTuiApp::TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint,
                           const TString& initialTopicPath, std::optional<ui32> initialPartition)
    : SchemeClient_(std::make_unique<NScheme::TSchemeClient>(driver))
    , TopicClient_(std::make_unique<NTopic::TTopicClient>(driver))
    , Screen_(ScreenInteractive::Fullscreen())
    , RefreshRate_(refreshRate)
    , ViewerEndpoint_(viewerEndpoint)
    , DatabaseRoot_(startPath)
    , InitialTopicPath_(initialTopicPath)
    , InitialPartition_(initialPartition)
{
    State_.CurrentPath = startPath;
    
    // Create views
    TopicListView_ = std::make_shared<TTopicListView>(*this);
    TopicDetailsView_ = std::make_shared<TTopicDetailsView>(*this);
    ConsumerView_ = std::make_shared<TConsumerView>(*this);
    MessagePreviewView_ = std::make_shared<TMessagePreviewView>(*this);
    ChartsView_ = std::make_shared<TChartsView>(*this);
    TopicForm_ = std::make_shared<TTopicForm>(*this);
    DeleteConfirmForm_ = std::make_shared<TDeleteConfirmForm>(*this);
    ConsumerForm_ = std::make_shared<TConsumerForm>(*this);
    WriteMessageForm_ = std::make_shared<TWriteMessageForm>(*this);
    
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
    
    TopicDetailsView_->OnWriteMessage = [this]() {
        WriteMessageForm_->SetTopic(State_.SelectedTopic, State_.SelectedPartition);
        NavigateTo(EViewType::WriteMessage);
    };
    
    TopicDetailsView_->OnAddConsumer = [this]() {
        ConsumerForm_->SetTopic(State_.SelectedTopic);
        NavigateTo(EViewType::ConsumerForm);
    };
    
    // Consumer form callbacks
    ConsumerForm_->OnSuccess = [this]() {
        TopicDetailsView_->Refresh();
        NavigateBack();
    };
    
    ConsumerForm_->OnCancel = [this]() {
        NavigateBack();
    };
    
    // Write message form callbacks
    WriteMessageForm_->OnClose = [this]() {
        NavigateBack();
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
    
    // Wire up topic CRUD callbacks
    TopicListView_->OnCreateTopic = [this]() {
        TopicForm_->SetCreateMode(State_.CurrentPath);
        NavigateTo(EViewType::TopicForm);
    };
    
    TopicListView_->OnEditTopic = [this](const TString& topicPath) {
        // Fetch topic description then show form in edit mode
        auto future = TopicClient_->DescribeTopic(topicPath);
        auto result = future.GetValueSync();
        if (result.IsSuccess()) {
            TopicForm_->SetEditMode(topicPath, result.GetTopicDescription());
            NavigateTo(EViewType::TopicForm);
        } else {
            ShowError(result.GetIssues().ToString());
        }
    };
    
    TopicListView_->OnDeleteTopic = [this](const TString& topicPath) {
        // Show confirmation dialog instead of deleting directly
        DeleteConfirmForm_->SetTopic(topicPath);
        NavigateTo(EViewType::DeleteConfirm);
    };
    
    // Wire up delete confirmation callbacks
    DeleteConfirmForm_->OnConfirm = [this](const TString& path) {
        // Try to delete as topic first
        auto topicResult = TopicClient_->DropTopic(path).GetValueSync();
        if (topicResult.IsSuccess()) {
            NavigateBack();
            TopicListView_->Refresh();
            return;
        }
        
        // If topic delete failed, try as directory
        auto dirResult = SchemeClient_->RemoveDirectory(path).GetValueSync();
        NavigateBack();
        if (dirResult.IsSuccess()) {
            TopicListView_->Refresh();
        } else {
            // Show original topic error or directory error
            ShowError(dirResult.GetIssues().ToString());
        }
    };
    
    DeleteConfirmForm_->OnCancel = [this]() {
        NavigateBack();
    };
    
    // Wire up form callbacks
    TopicForm_->OnSubmit = [this](const TTopicFormData& data) {
        if (TopicForm_->IsEditMode()) {
            // Alter existing topic
            NTopic::TAlterTopicSettings settings;
            
            // Partitioning settings with auto-partitioning
            settings.BeginAlterPartitioningSettings()
                .MinActivePartitions(data.MinPartitions)
                .MaxActivePartitions(data.MaxPartitions)
                .BeginAlterAutoPartitioningSettings()
                    .Strategy(data.AutoPartitioningStrategy)
                    .StabilizationWindow(TDuration::Seconds(data.StabilizationWindowSeconds))
                    .UpUtilizationPercent(data.UpUtilizationPercent)
                    .DownUtilizationPercent(data.DownUtilizationPercent)
                .EndAlterAutoPartitioningSettings()
            .EndAlterTopicPartitioningSettings();
            
            // Retention
            settings.SetRetentionPeriod(data.RetentionPeriod);
            if (data.RetentionStorageMb > 0) {
                settings.SetRetentionStorageMb(data.RetentionStorageMb);
            }
            
            // Write performance
            settings.SetPartitionWriteSpeedBytesPerSecond(data.WriteSpeedBytesPerSecond);
            if (data.WriteBurstBytes > 0) {
                settings.SetPartitionWriteBurstBytes(data.WriteBurstBytes);
            }
            
            // Metering mode
            if (data.MeteringMode != NTopic::EMeteringMode::Unspecified) {
                settings.SetMeteringMode(data.MeteringMode);
            }
            
            // Codecs
            std::vector<NTopic::ECodec> codecs;
            if (data.CodecRaw) codecs.push_back(NTopic::ECodec::RAW);
            if (data.CodecGzip) codecs.push_back(NTopic::ECodec::GZIP);
            if (data.CodecZstd) codecs.push_back(NTopic::ECodec::ZSTD);
            if (data.CodecLzop) codecs.push_back(NTopic::ECodec::LZOP);
            if (!codecs.empty()) {
                settings.SetSupportedCodecs(codecs);
            }
            
            auto future = TopicClient_->AlterTopic(data.Path, settings);
            auto result = future.GetValueSync();
            if (result.IsSuccess()) {
                NavigateBack();
                TopicListView_->Refresh();
            } else {
                ShowError(result.GetIssues().ToString());
            }
        } else {
            // Create new topic
            NTopic::TCreateTopicSettings settings;
            
            // Partitioning with auto-partitioning
            NTopic::TAutoPartitioningSettings autoPartSettings(
                data.AutoPartitioningStrategy,
                TDuration::Seconds(data.StabilizationWindowSeconds),
                data.DownUtilizationPercent,
                data.UpUtilizationPercent
            );
            settings.PartitioningSettings(data.MinPartitions, data.MaxPartitions, autoPartSettings);
            
            // Retention
            settings.RetentionPeriod(data.RetentionPeriod);
            if (data.RetentionStorageMb > 0) {
                settings.RetentionStorageMb(data.RetentionStorageMb);
            }
            
            // Write performance
            settings.PartitionWriteSpeedBytesPerSecond(data.WriteSpeedBytesPerSecond);
            if (data.WriteBurstBytes > 0) {
                settings.PartitionWriteBurstBytes(data.WriteBurstBytes);
            }
            
            // Metering mode
            if (data.MeteringMode != NTopic::EMeteringMode::Unspecified) {
                settings.MeteringMode(data.MeteringMode);
            }
            
            // Codecs
            if (data.CodecRaw) settings.AppendSupportedCodecs(NTopic::ECodec::RAW);
            if (data.CodecGzip) settings.AppendSupportedCodecs(NTopic::ECodec::GZIP);
            if (data.CodecZstd) settings.AppendSupportedCodecs(NTopic::ECodec::ZSTD);
            if (data.CodecLzop) settings.AppendSupportedCodecs(NTopic::ECodec::LZOP);
            
            auto future = TopicClient_->CreateTopic(data.Path, settings);
            auto result = future.GetValueSync();
            if (result.IsSuccess()) {
                NavigateBack();
                TopicListView_->Refresh();
            } else {
                ShowError(result.GetIssues().ToString());
            }
        }
    };
    
    TopicForm_->OnCancel = [this]() {
        NavigateBack();
    };
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
            } else {
                State_.CurrentView = EViewType::TopicDetails;
            }
        }
        // If DescribeTopic failed, it's a directory or invalid - Path_ is already set
    }
    
    // Load topic list (for the current directory context)
    TopicListView_->Refresh();
    
    StartRefreshThread();
    
    auto mainComponent = BuildMainComponent();
    Screen_.Loop(mainComponent);
    
    StopRefreshThread();
    return 0;
}

void TTopicTuiApp::NavigateTo(EViewType view) {
    State_.CurrentView = view;
    
    // Ensure forms have focus when navigating to them
    if (view == EViewType::TopicForm && TopicFormComponent_) {
        TopicFormComponent_->TakeFocus();
    } else if (view == EViewType::DeleteConfirm && DeleteConfirmComponent_) {
        DeleteConfirmComponent_->TakeFocus();
    }
    
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
        case EViewType::TopicForm:
            State_.CurrentView = EViewType::TopicList;
            if (TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            }
            break;
        case EViewType::DeleteConfirm:
            State_.CurrentView = EViewType::TopicList;
            if (TopicListComponent_) {
                TopicListComponent_->TakeFocus();
            }
            break;
        case EViewType::ConsumerForm:
            State_.CurrentView = EViewType::TopicDetails;
            break;
        case EViewType::WriteMessage:
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
    TopicListComponent_ = TopicListView_->Build();
    TopicDetailsComponent_ = TopicDetailsView_->Build();
    auto consumerComponent = ConsumerView_->Build();
    auto messagePreviewComponent = MessagePreviewView_->Build();
    auto chartsComponent = ChartsView_->Build();
    TopicFormComponent_ = TopicForm_->Build();
    DeleteConfirmComponent_ = DeleteConfirmForm_->Build();
    auto consumerFormComponent = ConsumerForm_->Build();
    auto writeMessageComponent = WriteMessageForm_->Build();
    
    // Use a simple container - we'll switch rendering manually
    auto container = Container::Stacked({
        TopicListComponent_,
        TopicDetailsComponent_,
        consumerComponent,
        messagePreviewComponent,
        chartsComponent,
        TopicFormComponent_,
        DeleteConfirmComponent_,
        consumerFormComponent,
        writeMessageComponent
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
            std::cerr << "[DEBUG] WriteMessage event: " << eventDesc << std::endl;
            
            // Handle Escape for WriteMessage at app level FIRST
            bool isEscape = (event == Event::Escape) || 
                           (event.is_character() && event.character() == "\x1b");
            if (isEscape) {
                std::cerr << "[DEBUG] Escape detected! Navigating back." << std::endl;
                NavigateBack();
                return true;
            }
            return false;  // Let other keys go to form for typing
        }
        
        // Skip all global shortcuts when in form views - let them type freely
        if (State_.CurrentView == EViewType::TopicForm || 
            State_.CurrentView == EViewType::DeleteConfirm ||
            State_.CurrentView == EViewType::ConsumerForm) {
            return false;  // Don't handle any keys globally in forms
        }
        
        // Global key handling
        if (event == Event::Character('q') || event == Event::Character('Q')) {
            RequestExit();
            return true;
        }
        if (event == Event::Escape) {
            // Don't handle Esc globally for views that handle it themselves
            if (State_.CurrentView != EViewType::TopicList && 
                State_.CurrentView != EViewType::MessagePreview) {
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
    
    auto withGlobalKeys = container;
    
    return Renderer(withGlobalKeys, [=, this] {
        Element content;
        
        // Note: Auto-refresh removed - use 'r' key for manual refresh
        // to avoid disruptive full-page reloads
        
        switch (State_.CurrentView) {
            case EViewType::TopicList:
                content = TopicListComponent_->Render();
                break;
            case EViewType::TopicDetails:
                content = TopicDetailsComponent_->Render();
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
            case EViewType::TopicForm:
                content = TopicFormComponent_->Render();
                break;
            case EViewType::DeleteConfirm:
                content = DeleteConfirmComponent_->Render();
                break;
            case EViewType::ConsumerForm:
                content = consumerFormComponent->Render();
                break;
            case EViewType::WriteMessage:
                content = writeMessageComponent->Render();
                break;
            case EViewType::OffsetForm:
                // Not rendered as standalone - used as modal
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
                    text(" [Enter] Open ") | color(Color::Cyan),
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
                    text(" [t] Tail ") | color(Color::Green),
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
