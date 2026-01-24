#include "topic_details_view.h"
#include "../app_interface.h"
#include "../widgets/sparkline.h"
#include "../common/async_utils.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Define table columns for partitions
static TVector<TTableColumn> CreatePartitionsTableColumns() {
    return {
        {"ID", 5},
        {"Size", NTheme::ColBytes},
        {"Write/min", NTheme::ColBytesPerMin},
        {"WriteRate", NTheme::ColSparkline},  // Sparkline column
        {"WriteLag", NTheme::ColDuration},
        {"Start", NTheme::ColOffset},
        {"End", NTheme::ColOffset},
        {"Node", NTheme::ColNodeId},
        {"LastWrite", NTheme::ColDuration}
    };
}

// Define table columns for consumers  
static TVector<TTableColumn> CreateConsumersTableColumns() {
    return {
        {"Consumer Name", -1},  // flex
        {"Lag", NTheme::ColCount},
        {"MaxLag", NTheme::ColDuration}
    };
}

TTopicDetailsView::TTopicDetailsView(ITuiApp& app)
    : App_(app)
    , PartitionsTable_(CreatePartitionsTableColumns())
    , ConsumersTable_(CreateConsumersTableColumns())
{
    PartitionsTable_.SetHorizontalScrollEnabled(true);
    ConsumersTable_.SetHorizontalScrollEnabled(true);

    // Set up partition table selection callback
    PartitionsTable_.OnSelect = [this](int row) {
        if (row >= 0 && row < static_cast<int>(Partitions_.size())) {
            App_.GetState().SelectedPartition = Partitions_[row].PartitionId;
            App_.SetMessagePreviewTarget(TopicPath_, static_cast<ui32>(Partitions_[row].PartitionId), 0);
            App_.NavigateTo(EViewType::MessagePreview);
        }
    };
    
    // Sort callback for partitions
    PartitionsTable_.OnSortChanged = [this](int col, bool asc) {
        SortPartitions(col, asc);
        PopulatePartitionsTable();
        App_.PostRefresh();
    };

    // Keep selected partition in sync with table navigation
    PartitionsTable_.OnNavigate = [this](int row) {
        if (row >= 0 && row < static_cast<int>(Partitions_.size())) {
            App_.GetState().SelectedPartition = Partitions_[row].PartitionId;
        }
    };
    
    // Set up consumer table selection callback
    ConsumersTable_.OnSelect = [this](int row) {
        if (row >= 0 && row < static_cast<int>(Consumers_.size())) {
            // Navigate to consumer details via ITuiApp interface
            App_.SetConsumerViewTarget(TopicPath_, Consumers_[row].Name);
            App_.NavigateTo(EViewType::ConsumerDetails);
        }
    };
    
    // Sort callback for consumers
    ConsumersTable_.OnSortChanged = [this](int col, bool asc) {
        SortConsumers(col, asc);
        PopulateConsumersTable();
        App_.PostRefresh();
    };
    
    StopFlag_ = std::make_shared<std::atomic<bool>>(false);
    App_.GetState().TopicDetailsFocusPanel = FocusPanel_;
}

TTopicDetailsView::~TTopicDetailsView() {
    if (StopFlag_) {
        *StopFlag_ = true;
    }
}

Component TTopicDetailsView::Build() {
    // Build table components
    auto partitionsTableComp = PartitionsTable_.Build();
    auto consumersTableComp = ConsumersTable_.Build();
    
    // Create partition panel component
    auto partitionsPanel = Renderer(partitionsTableComp, [this, partitionsTableComp] {
        Element content;
        // Only show spinner if loading AND we have no data yet
        if (LoadingTopic_ && Partitions_.empty()) {
            content = NTheme::RenderSpinner(SpinnerFrame_, "Loading partitions...") | center;
        } else if (!TopicError_.empty() && Partitions_.empty()) {
            content = text("Error: " + std::string(TopicError_.c_str())) | color(NTheme::ErrorText) | center;
        } else if (Partitions_.empty()) {
            content = text("No partitions") | dim | center;
        } else {
            content = partitionsTableComp->Render();
        }
        
        // Show subtle refresh indicator in header when loading with existing data
        auto header = LoadingTopic_ && !Partitions_.empty()
            ? hbox({text(" Partitions ") | bold, text(" ⟳") | dim | color(Color::Yellow)})
            : text(" Partitions ") | bold;
        
        return vbox({
            header,
            separator(),
            content | flex
        }) | border | flex;
    });
    
    auto consumersPanel = Renderer(consumersTableComp, [this, consumersTableComp] {
        Element content;
        // Only show full-screen spinner on FIRST load (never loaded before)
        if (LoadingConsumers_ && !ConsumersLoadedOnce_) {
            content = NTheme::RenderSpinner(SpinnerFrame_, "Loading consumers...") | center;
        } else if (!ConsumersError_.empty() && Consumers_.empty()) {
            content = text("Error: " + std::string(ConsumersError_.c_str())) | color(NTheme::ErrorText) | center;
        } else if (Consumers_.empty()) {
            content = text("No consumers") | dim | center;
        } else {
            content = consumersTableComp->Render();
        }
        
        // Show subtle refresh indicator in header when loading (after first load completed)
        auto header = LoadingConsumers_ && ConsumersLoadedOnce_
            ? hbox({text(" Consumers ") | bold, text(" ⟳") | dim | color(Color::Yellow)})
            : text(" Consumers ") | bold;
        
        return vbox({
            header,
            separator(),
            content | flex,
            separator(),
            RenderWriteRateChart()
        }) | border;
    });
    
    // Create resizable split with consumers on the right
    auto splitContainer = ResizableSplitRight(consumersPanel, partitionsPanel, &ConsumersPanelSize_);
    
    // CatchEvent comes FIRST (before split can consume events)
    // Then wrap in Renderer for custom rendering
    return CatchEvent(splitContainer, [this](Event event) {
        if (App_.GetState().CurrentView != EViewType::TopicDetails) {
            return false;
        }
        
        int previousFocus = FocusPanel_;
        
        // TopicInfo view event handling
        if (App_.GetState().CurrentView == EViewType::TopicInfo) {
            if (event == Event::Escape || event == Event::Character('i') || event == Event::Character('I')) {
                App_.NavigateBack();
                return true;
            }
            // Scroll
            if (event == Event::ArrowDown || event == Event::Character('j')) {
                InfoScrollY_ = std::min(InfoScrollY_ + 1, 100);
                return true;
            }
            if (event == Event::ArrowUp || event == Event::Character('k')) {
                InfoScrollY_ = std::max(InfoScrollY_ - 1, 0);
                return true;
            }
            return true;
        }
        
        // TopicTablets view event handling
        if (App_.GetState().CurrentView == EViewType::TopicTablets) {
            if (event == Event::Escape || event == Event::Character('t') || event == Event::Character('T')) {
                App_.NavigateBack();
                return true;
            }
            // Scroll
            if (event == Event::ArrowDown || event == Event::Character('j')) {
                TabletsScrollY_ = std::min(TabletsScrollY_ + 1, static_cast<int>(Tablets_.size()));
                return true;
            }
            if (event == Event::ArrowUp || event == Event::Character('k')) {
                TabletsScrollY_ = std::max(TabletsScrollY_ - 1, 0);
                return true;
            }
            // Refresh
            if (event == Event::Character('r') || event == Event::Character('R')) {
                Tablets_.clear();
                LoadingTablets_ = true;
                const TString& endpoint = App_.GetViewerEndpoint();
                TString topicPath = TopicPath_;
                TabletsFuture_ = std::async(std::launch::async, [endpoint, topicPath]() -> TVector<TTabletInfo> {
                    if (endpoint.empty()) {
                        return {};
                    }
                    TViewerHttpClient client(endpoint);
                    return client.GetTabletInfo(topicPath);
                });
                return true;
            }
            return true;
        }
        
        // Navigate to info view
        if (event == Event::Character('i') || event == Event::Character('I')) {
            InfoScrollY_ = 0;
            // Start async info load via Viewer API
            if (!LoadingInfo_) {
                LoadingInfo_ = true;
                const TString& endpoint = App_.GetViewerEndpoint();
                TString topicPath = TopicPath_;
                InfoFuture_ = std::async(std::launch::async, [endpoint, topicPath]() -> TTopicDescribeResult {
                    if (endpoint.empty()) {
                        TTopicDescribeResult result;
                        result.Error = "Viewer endpoint not configured";
                        return result;
                    }
                    TViewerHttpClient client(endpoint);
                    return client.GetTopicDescribe(topicPath, false);  // No tablets needed for info view
                });
            }
            App_.SetTopicInfoTarget(TopicPath_);
            App_.NavigateTo(EViewType::TopicInfo);
            return true;
        }
        
        // Navigate to tablets view
        if (event == Event::Character('t') || event == Event::Character('T')) {
            TabletsScrollY_ = 0;
            // Start async tablets load if not already loading
            if (!LoadingTablets_ && Tablets_.empty()) {
                LoadingTablets_ = true;
                const TString& endpoint = App_.GetViewerEndpoint();
                TString topicPath = TopicPath_;
                TabletsFuture_ = std::async(std::launch::async, [endpoint, topicPath]() -> TVector<TTabletInfo> {
                    if (endpoint.empty()) {
                        return {};
                    }
                    TViewerHttpClient client(endpoint);
                    return client.GetTabletInfo(topicPath);
                });
            }
            App_.NavigateTo(EViewType::TopicTablets);
            return true;
        }
        
        // Update focus state for both tables
        PartitionsTable_.SetFocused(FocusPanel_ == 0);
        ConsumersTable_.SetFocused(FocusPanel_ == 1);
        
        // Tab switches between panels
        if (event == Event::Tab) {
            FocusPanel_ = (FocusPanel_ + 1) % 2;
            App_.GetState().TopicDetailsFocusPanel = FocusPanel_;
            return true;
        }
        
        // Delegate navigation to the focused table
        if (FocusPanel_ == 0 && !Partitions_.empty()) {
            if (PartitionsTable_.HandleEvent(event)) {
                App_.GetState().TopicDetailsFocusPanel = FocusPanel_;
                return true;
            }
        } else if (FocusPanel_ == 1 && !Consumers_.empty()) {
            if (ConsumersTable_.HandleEvent(event)) {
                App_.GetState().TopicDetailsFocusPanel = FocusPanel_;
                return true;
            }
        }
        
        if (event == Event::Character('s') || event == Event::Character('S')) {
            if (FocusPanel_ == 0) {
                PartitionsTable_.SetSort(PartitionsTable_.GetSortColumn(),
                                         !PartitionsTable_.IsSortAscending());
            } else {
                ConsumersTable_.SetSort(ConsumersTable_.GetSortColumn(),
                                        !ConsumersTable_.IsSortAscending());
            }
            return true;
        }

        // Custom key handlers
        if (event == Event::Character('w') || event == Event::Character('W')) {
            // Write message via ITuiApp interface
            App_.SetWriteMessageTarget(TopicPath_, App_.GetState().SelectedPartition);
            App_.NavigateTo(EViewType::WriteMessage);
            return true;
        }
        if (event == Event::Character('a') || event == Event::Character('A')) {
            // Add consumer via ITuiApp interface
            App_.SetConsumerFormTarget(TopicPath_);
            App_.NavigateTo(EViewType::ConsumerForm);
            return true;
        }
        if (event == Event::Character('x') || event == Event::Character('X')) {
            if (FocusPanel_ == 1) {
                int row = ConsumersTable_.GetSelectedRow();
                if (row >= 0 && row < static_cast<int>(Consumers_.size())) {
                    // Drop consumer via ITuiApp interface
                    App_.SetDropConsumerTarget(TopicPath_, Consumers_[row].Name);
                    App_.NavigateTo(EViewType::DropConsumerConfirm);
                }
            }
            return true;
        }
        if (event == Event::Character('e') || event == Event::Character('E')) {
            if (FocusPanel_ == 1 && !Consumers_.empty()) {
                // Edit consumer
                int row = ConsumersTable_.GetSelectedRow();
                if (row >= 0 && row < static_cast<int>(Consumers_.size())) {
                    // Edit consumer via ITuiApp interface
                    App_.SetEditConsumerTarget(TopicPath_, Consumers_[row].Name);
                    App_.NavigateTo(EViewType::EditConsumer);
                }
            } else {
                // Edit topic via ITuiApp interface
                App_.SetTopicFormEditMode(TopicPath_);
                App_.NavigateTo(EViewType::TopicForm);
            }
            return true;
        }
        
        if (previousFocus != FocusPanel_) {
            App_.GetState().TopicDetailsFocusPanel = FocusPanel_;
        }
        return false;
    }) | Renderer([this, splitContainer](Element) {
        CheckAsyncCompletion();
        
        // Render TopicInfo full screen view
        if (App_.GetState().CurrentView == EViewType::TopicInfo) {
            return RenderInfoView();
        }
        
        // Render TopicTablets full screen view
        if (App_.GetState().CurrentView == EViewType::TopicTablets) {
            return RenderTabletsView();
        }
        
        // Default: render topic details
        return vbox({
            RenderHeader(),
            separator(),
            splitContainer->Render() | flex
        });
    });
}

void TTopicDetailsView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    ConsumersLoadedOnce_ = false;  // Reset for new topic
    LastRefreshTime_ = TInstant::Now();  // Enable auto-refresh
    StartAsyncLoads();
}

void TTopicDetailsView::Refresh() {
    LastRefreshTime_ = TInstant::Now();
    StartAsyncLoads();
}

void TTopicDetailsView::CheckAsyncCompletion() {
    SpinnerFrame_++;
    
    // Auto-refresh if enough time has passed and not already loading
    TDuration refreshRate = App_.GetRefreshRate();
    if (!LoadingTopic_ && !LoadingConsumers_ && 
        LastRefreshTime_ != TInstant::Zero() &&
        TInstant::Now() - LastRefreshTime_ > refreshRate) {
        StartAsyncLoads();
        LastRefreshTime_ = TInstant::Now();
    }
    
    // Check topic data future
    if (LoadingTopic_ && TopicFuture_.valid()) {
        if (TopicFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto data = TopicFuture_.get();
                Partitions_ = std::move(data.Partitions);
                TotalPartitions_ = data.TotalPartitions;
                RetentionPeriod_ = data.RetentionPeriod;
                WriteSpeedBytesPerSec_ = data.WriteSpeedBytesPerSec;
                SupportedCodecs_ = std::move(data.SupportedCodecs);
                
                // Update topic-level sparkline history
                TopicWriteRateHistory_.Update(data.WriteRateBytesPerSec);
                
                // Update per-partition sparkline histories
                for (const auto& p : Partitions_) {
                    PartitionWriteRateHistory_[p.PartitionId].Update(
                        static_cast<double>(p.BytesWrittenPerMinute) / 60.0);
                }
                
                TopicError_.clear();
                // Re-apply current sort order before populating table
                SortPartitions(PartitionsTable_.GetSortColumn(), PartitionsTable_.IsSortAscending());
                PopulatePartitionsTable();
            } catch (const std::exception& e) {
                TopicError_ = e.what();
            }
            LoadingTopic_ = false;
        }
    }
    
    // Check consumers data future
    if (LoadingConsumers_ && ConsumersFuture_.valid()) {
        if (ConsumersFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto data = ConsumersFuture_.get();
                Consumers_ = std::move(data.Consumers);
                ConsumersError_.clear();
                ConsumersLoadedOnce_ = true;  // Mark that we've completed at least one load
                // Re-apply current sort order before populating table
                SortConsumers(ConsumersTable_.GetSortColumn(), ConsumersTable_.IsSortAscending());
                PopulateConsumersTable();
            } catch (const std::exception& e) {
                ConsumersError_ = e.what();
            }
            LoadingConsumers_ = false;
        }
    }
    
    // Check tablets data future
    if (LoadingTablets_ && TabletsFuture_.valid()) {
        if (TabletsFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                Tablets_ = TabletsFuture_.get();
            } catch (const std::exception&) {
                // Ignore errors for tablets
            }
            LoadingTablets_ = false;
        }
    }
    
    // Check info data future (Viewer API)
    if (LoadingInfo_ && InfoFuture_.valid()) {
        if (InfoFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                TopicDescribeResult_ = InfoFuture_.get();
            } catch (const std::exception& e) {
                TopicDescribeResult_.Error = e.what();
            }
            LoadingInfo_ = false;
        }
    }
}

Element TTopicDetailsView::RenderHeader() {
    if (TopicPath_.empty()) {
        return text("No topic selected") | dim;
    }
    
    // Calculate totals from partitions
    ui64 totalWriteRate = 0;
    for (const auto& p : Partitions_) {
        totalWriteRate += p.BytesWrittenPerMinute;
    }
    
    // Always show current values - don't show "..." during loading to prevent flicker
    std::string partitionsText = std::to_string(TotalPartitions_);
    std::string retentionText = std::string(FormatRetention(RetentionPeriod_).c_str());
    std::string speedText = std::string(FormatBytes(WriteSpeedBytesPerSec_).c_str()) + "/s";
    std::string writeRateText = std::string(FormatBytes(totalWriteRate).c_str()) + "/m";
    
    // Format codecs
    std::string codecsText;
    for (size_t i = 0; i < SupportedCodecs_.size(); ++i) {
        if (i > 0) codecsText += ",";
        switch (SupportedCodecs_[i]) {
            case NTopic::ECodec::RAW: codecsText += "RAW"; break;
            case NTopic::ECodec::GZIP: codecsText += "GZIP"; break;
            case NTopic::ECodec::LZOP: codecsText += "LZOP"; break;
            case NTopic::ECodec::ZSTD: codecsText += "ZSTD"; break;
            default: codecsText += "?"; break;
        }
    }
    if (codecsText.empty()) codecsText = "-";
    
    return vbox({
        hbox({
            text(" Topic: ") | bold,
            text(std::string(TopicPath_.c_str())) | color(NTheme::AccentText)
        }),
        hbox({
            text(" Partitions: ") | dim,
            text(partitionsText) | bold,
            text("   Retention: ") | dim,
            text(retentionText) | bold,
            text("   Speed Limit: ") | dim,
            text(speedText) | bold,
            text("   Write Rate: ") | dim,
            text(writeRateText) | bold | color(Color::Green),
            text("   Codecs: ") | dim,
            text(codecsText) | bold | color(Color::Magenta)
        })
    });
}

void TTopicDetailsView::PopulatePartitionsTable() {
    PartitionsTable_.SetRowCount(Partitions_.size());
    
    for (size_t i = 0; i < Partitions_.size(); ++i) {
        const auto& p = Partitions_[i];
        
        // Format last write time
        TString lastWriteStr = p.LastWriteTime != TInstant() 
            ? p.LastWriteTime.FormatLocalTime("%H:%M:%S")
            : "-";
        
        // Use UpdateCell for each column - tracks changes automatically
        PartitionsTable_.UpdateCell(i, 0, ToString(p.PartitionId));
        PartitionsTable_.UpdateCell(i, 1, FormatBytes(p.StoreSizeBytes));
        PartitionsTable_.UpdateCell(i, 2, FormatBytes(p.BytesWrittenPerMinute));
        
        // Render per-partition sparkline with zero-based scaling
        auto it = PartitionWriteRateHistory_.find(p.PartitionId);
        if (it != PartitionWriteRateHistory_.end() && !it->second.Empty()) {
            const auto& values = it->second.GetValues();
            TString sparkStr;
            if (!values.empty()) {
                double maxVal = it->second.GetMax();
                size_t start = values.size() > 15 ? values.size() - 15 : 0;
                for (size_t j = start; j < values.size(); ++j) {
                    double normalized = values[j] / maxVal;
                    int level = static_cast<int>(normalized * 8);
                    level = std::max(0, std::min(8, level));
                    sparkStr += SparklineChars[level];
                }
            }
            PartitionsTable_.UpdateCell(i, 3, sparkStr.empty() ? "-" : sparkStr);
        } else {
            PartitionsTable_.UpdateCell(i, 3, "-");
        }
        
        PartitionsTable_.UpdateCell(i, 4, FormatDuration(p.WriteTimeLag));
        PartitionsTable_.UpdateCell(i, 5, FormatNumber(p.StartOffset));
        PartitionsTable_.UpdateCell(i, 6, FormatNumber(p.EndOffset));
        PartitionsTable_.UpdateCell(i, 7, p.NodeId > 0 ? ToString(p.NodeId) : "-");
        PartitionsTable_.UpdateCell(i, 8, lastWriteStr);
    }
}

void TTopicDetailsView::PopulateConsumersTable() {
    ConsumersTable_.SetRowCount(Consumers_.size());
    
    for (size_t i = 0; i < Consumers_.size(); ++i) {
        const auto& c = Consumers_[i];
        
        // Use UpdateCell for each column - tracks changes automatically
        ConsumersTable_.UpdateCell(i, 0, c.Name);
        ConsumersTable_.UpdateCell(i, 1, TTableCell(FormatNumber(c.TotalLag), NTheme::GetLagColor(c.TotalLag)));
        ConsumersTable_.UpdateCell(i, 2, FormatDuration(c.MaxLagTime));
    }
}

Element TTopicDetailsView::RenderWriteRateChart() {
    return vbox({
        text(" Write Rate ") | bold,
        hbox({
            RenderSparkline(TopicWriteRateHistory_.GetValues(), 30),
            text(" ") | flex
        })
    });
}

void TTopicDetailsView::SortPartitions(int column, bool ascending) {
    // Columns: ID, Size, Write/min, WriteRate(sparkline), WriteLag, Start, End, Node, LastWrite
    std::stable_sort(Partitions_.begin(), Partitions_.end(), 
        [column, ascending](const TPartitionDisplayInfo& a, const TPartitionDisplayInfo& b) {
            int cmp = 0;
            switch (column) {
                case 0: cmp = (a.PartitionId < b.PartitionId) ? -1 : (a.PartitionId > b.PartitionId) ? 1 : 0; break;
                case 1: cmp = (a.StoreSizeBytes < b.StoreSizeBytes) ? -1 : (a.StoreSizeBytes > b.StoreSizeBytes) ? 1 : 0; break;
                case 2: cmp = (a.BytesWrittenPerMinute < b.BytesWrittenPerMinute) ? -1 : (a.BytesWrittenPerMinute > b.BytesWrittenPerMinute) ? 1 : 0; break;
                case 3: cmp = (a.BytesWrittenPerMinute < b.BytesWrittenPerMinute) ? -1 : (a.BytesWrittenPerMinute > b.BytesWrittenPerMinute) ? 1 : 0; break; // sparkline
                case 4: cmp = (a.WriteTimeLag < b.WriteTimeLag) ? -1 : (a.WriteTimeLag > b.WriteTimeLag) ? 1 : 0; break;
                case 5: cmp = (a.StartOffset < b.StartOffset) ? -1 : (a.StartOffset > b.StartOffset) ? 1 : 0; break;
                case 6: cmp = (a.EndOffset < b.EndOffset) ? -1 : (a.EndOffset > b.EndOffset) ? 1 : 0; break;
                case 7: cmp = (a.NodeId < b.NodeId) ? -1 : (a.NodeId > b.NodeId) ? 1 : 0; break;
                case 8: cmp = (a.LastWriteTime < b.LastWriteTime) ? -1 : (a.LastWriteTime > b.LastWriteTime) ? 1 : 0; break;
                default: cmp = 0; break;
            }
            return ascending ? (cmp < 0) : (cmp > 0);
        });
}

void TTopicDetailsView::SortConsumers(int column, bool ascending) {
    // Columns: Name, Lag, MaxLag
    std::stable_sort(Consumers_.begin(), Consumers_.end(),
        [column, ascending](const TConsumerDisplayInfo& a, const TConsumerDisplayInfo& b) {
            int cmp = 0;
            switch (column) {
                case 0: cmp = a.Name.compare(b.Name); break;
                case 1: cmp = (a.TotalLag < b.TotalLag) ? -1 : (a.TotalLag > b.TotalLag) ? 1 : 0; break;
                case 2: cmp = (a.MaxLagTime < b.MaxLagTime) ? -1 : (a.MaxLagTime > b.MaxLagTime) ? 1 : 0; break;
                default: cmp = 0; break;
            }
            return ascending ? (cmp < 0) : (cmp > 0);
        });
}

void TTopicDetailsView::StartAsyncLoads() {
    if (TopicPath_.empty()) {
        return;
    }
    
    TString path = TopicPath_;
    auto* topicClient = &App_.GetTopicClient();
    
    // Start topic data load (partitions + header info) - fast
    if (!LoadingTopic_) {
        LoadingTopic_ = true;
        TopicError_.clear();
        
        auto stopFlag = StopFlag_;
        TopicFuture_ = std::async(std::launch::async, [path, topicClient, stopFlag]() -> TTopicBasicData {
            TTopicBasicData data;
            
            auto descFuture = topicClient->DescribeTopic(path,
                NTopic::TDescribeTopicSettings()
                    .IncludeStats(true)
                    .IncludeLocation(true));

            // Wait with timeout (5 seconds) and stop flag check
            bool status = WaitFor(descFuture, stopFlag, TDuration::Seconds(5));
            if (!status) {
                // Timeout or cancelled - don't block forever
                return data;
            }

            auto result = descFuture.GetValueSync();
            
            if (!result.IsSuccess()) {
                throw std::runtime_error(result.GetIssues().ToString());
            }
            
            const auto& desc = result.GetTopicDescription();
            
            data.TotalPartitions = desc.GetPartitions().size();
            data.RetentionPeriod = desc.GetRetentionPeriod();
            data.WriteSpeedBytesPerSec = desc.GetPartitionWriteSpeedBytesPerSecond();
            
            const auto& topicStats = desc.GetTopicStats();
            data.WriteRateBytesPerSec = topicStats.GetBytesWrittenPerMinute() / 60.0;
            
            for (const auto& p : desc.GetPartitions()) {
                TPartitionDisplayInfo info;
                info.PartitionId = p.GetPartitionId();
                
                if (p.GetPartitionStats()) {
                    const auto& stats = *p.GetPartitionStats();
                    info.StartOffset = stats.GetStartOffset();
                    info.EndOffset = stats.GetEndOffset();
                    info.StoreSizeBytes = stats.GetStoreSizeBytes();
                    info.WriteTimeLag = stats.GetMaxWriteTimeLag();
                    info.LastWriteTime = stats.GetLastWriteTime();
                    info.BytesWrittenPerMinute = stats.GetBytesWrittenPerMinute();
                }
                
                if (p.GetPartitionLocation()) {
                    info.NodeId = p.GetPartitionLocation()->GetNodeId();
                }
                
                data.Partitions.push_back(info);
            }
            
            // Extended info for info modal
            data.Owner = desc.GetOwner();
            const auto& sdkCodecs = desc.GetSupportedCodecs();
            data.SupportedCodecs.assign(sdkCodecs.begin(), sdkCodecs.end());
            data.Attributes = desc.GetAttributes();
            data.MeteringMode = desc.GetMeteringMode();
            data.PartitionWriteBurstBytes = desc.GetPartitionWriteBurstBytes();
            if (desc.GetRetentionStorageMb()) {
                data.RetentionStorageMb = *desc.GetRetentionStorageMb();
            }
            
            const auto& partSettings = desc.GetPartitioningSettings();
            data.MinActivePartitions = partSettings.GetMinActivePartitions();
            data.MaxActivePartitions = partSettings.GetMaxActivePartitions();
            data.AutoPartitioningStrategy = partSettings.GetAutoPartitioningSettings().GetStrategy();
            data.ConsumerCount = desc.GetConsumers().size();
            
            return data;
        });
    }
    
    // Start consumers load (slower - needs per-consumer DescribeConsumer calls)
    if (!LoadingConsumers_) {
        LoadingConsumers_ = true;
        ConsumersError_.clear();
        // Don't clear Consumers_ - keep showing old data until new data arrives
        
        auto stopFlag = StopFlag_;
        ConsumersFuture_ = std::async(std::launch::async, [path, topicClient, stopFlag]() -> TConsumersData {
            TConsumersData data;
            
            auto descFuture = topicClient->DescribeTopic(path,
                NTopic::TDescribeTopicSettings());

            // Wait with timeout (5 seconds) and stop flag check
            bool status = WaitFor(descFuture, stopFlag, TDuration::Seconds(5));
            if (!status) {
                // Timeout or cancelled - don't block forever
                return data;
            }

            auto result = descFuture.GetValueSync();
            
            if (!result.IsSuccess()) {
                throw std::runtime_error(result.GetIssues().ToString());
            }
            
            const auto& desc = result.GetTopicDescription();
            
            for (const auto& c : desc.GetConsumers()) {
                // Check if cancelled
                if (stopFlag && *stopFlag) return data;
                
                TConsumerDisplayInfo info;
                info.Name = TString(c.GetConsumerName());
                info.IsImportant = c.GetImportant();
                
                // Fetch detailed consumer stats
                auto consFuture = topicClient->DescribeConsumer(path, info.Name,
                    NTopic::TDescribeConsumerSettings().IncludeStats(true));
                
                // Wait with timeout
                if (WaitFor(consFuture, stopFlag, TDuration::Seconds(5))) {
                    auto consumerResult = consFuture.GetValueSync(); // Safe now
                
                    if (consumerResult.IsSuccess()) {
                        const auto& consumerDesc = consumerResult.GetConsumerDescription();
                        for (const auto& part : consumerDesc.GetPartitions()) {
                            if (part.GetPartitionStats() && part.GetPartitionConsumerStats()) {
                                ui64 endOffset = part.GetPartitionStats()->GetEndOffset();
                                ui64 committedOffset = part.GetPartitionConsumerStats()->GetCommittedOffset();
                                if (endOffset > committedOffset) {
                                    info.TotalLag += endOffset - committedOffset;
                                }
                                
                                if (part.GetPartitionConsumerStats()->GetMaxCommittedTimeLag() > info.MaxLagTime) {
                                    info.MaxLagTime = part.GetPartitionConsumerStats()->GetMaxCommittedTimeLag();
                                }
                            }
                        }
                    }
                }
                
                data.Consumers.push_back(info);
            }
            
            return data;
        });
    }
}

Element TTopicDetailsView::RenderInfoView() {
    using namespace ftxui;
    
    // Helper for formatting bytes
    auto formatBytes = [](ui64 bytes) -> TString {
        if (bytes >= 1024ULL * 1024 * 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / (1024.0 * 1024.0 * 1024.0)) << " GB";
        } else if (bytes >= 1024ULL * 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / (1024.0 * 1024.0)) << " MB";
        } else if (bytes >= 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / 1024.0) << " KB";
        }
        return TStringBuilder() << bytes << " B";
    };
    
    // Format duration
    auto formatDuration = [](ui64 seconds) -> TString {
        if (seconds >= 86400) {
            return Sprintf("%lud %luh", seconds / 86400, (seconds % 86400) / 3600);
        } else if (seconds >= 3600) {
            return Sprintf("%luh %lum", seconds / 3600, (seconds % 3600) / 60);
        } else if (seconds >= 60) {
            return Sprintf("%lum %lus", seconds / 60, seconds % 60);
        }
        return Sprintf("%lus", seconds);
    };
    
    Elements lines;
    
    // Title
    lines.push_back(text(" Topic Information (Viewer API) ") | bold | center);
    lines.push_back(separator());
    
    if (LoadingInfo_) {
        lines.push_back(text(" Loading topic info... ") | dim | center);
    } else if (!TopicDescribeResult_.Error.empty()) {
        lines.push_back(text(" Error: ") | color(Color::Red));
        lines.push_back(text(std::string("  " + TopicDescribeResult_.Error)) | color(Color::Red));
    } else {
        const auto& info = TopicDescribeResult_;
        
        // Basic Info
        lines.push_back(text(" Basic Info") | bold);
        lines.push_back(hbox({text("   Path:       ") | dim, text(info.Path.empty() ? TopicPath_.c_str() : info.Path.c_str())}));
        lines.push_back(hbox({text("   Owner:      ") | dim, text(info.Owner.c_str())}));
        lines.push_back(hbox({text("   PathId:     ") | dim, text(ToString(info.PathId).c_str())}));
        lines.push_back(hbox({text("   Schemeshard:") | dim, text(ToString(info.SchemeshardId).c_str())}));
        if (info.CreateTime != TInstant::Zero()) {
            lines.push_back(hbox({text("   Created:    ") | dim, text(info.CreateTime.FormatLocalTime("%Y-%m-%d %H:%M:%S").c_str())}));
        }
        lines.push_back(separator());
        
        // Partitions
        lines.push_back(text(" Partitions") | bold);
        lines.push_back(hbox({text("   Count: ") | dim, text(ToString(info.PartitionsCount).c_str())}));
        lines.push_back(separator());
        
        // Retention
        lines.push_back(text(" Retention") | bold);
        if (info.RetentionSeconds > 0) {
            lines.push_back(hbox({text("   Duration: ") | dim, text(formatDuration(info.RetentionSeconds).c_str())}));
        }
        if (info.RetentionBytes > 0) {
            lines.push_back(hbox({text("   Storage:  ") | dim, text(formatBytes(info.RetentionBytes).c_str())}));
        }
        if (info.RetentionSeconds == 0 && info.RetentionBytes == 0) {
            lines.push_back(hbox({text("   ") | dim, text("Not specified")}));
        }
        lines.push_back(separator());
        
        // Write Limits
        lines.push_back(text(" Write Speed") | bold);
        if (info.WriteSpeedBytesPerSec > 0) {
            lines.push_back(hbox({text("   Per Partition: ") | dim, text(formatBytes(info.WriteSpeedBytesPerSec).c_str()), text("/s")}));
        }
        if (info.BurstBytes > 0) {
            lines.push_back(hbox({text("   Burst:         ") | dim, text(formatBytes(info.BurstBytes).c_str())}));
        }
        if (info.WriteSpeedBytesPerSec == 0 && info.BurstBytes == 0) {
            lines.push_back(hbox({text("   ") | dim, text("Not specified")}));
        }
        lines.push_back(separator());
        
        // Codecs
        lines.push_back(text(" Supported Codecs") | bold);
        if (info.SupportedCodecs.empty()) {
            lines.push_back(hbox({text("   ") | dim, text("(default)")}));
        } else {
            TStringBuilder codecsLine;
            for (size_t i = 0; i < info.SupportedCodecs.size(); ++i) {
                if (i > 0) codecsLine << ", ";
                codecsLine << info.SupportedCodecs[i];
            }
            lines.push_back(hbox({text("   ") | dim, text(codecsLine.c_str())}));
        }
        lines.push_back(separator());
        
        // Metering
        if (!info.MeteringMode.empty()) {
            lines.push_back(hbox({text(" Metering: ") | bold, text(info.MeteringMode.c_str())}));
            lines.push_back(separator());
        }
    }
    
    // Clamp scroll position to valid range
    int maxScroll = std::max(0, static_cast<int>(lines.size()) - 20);
    InfoScrollY_ = std::min(InfoScrollY_, maxScroll);
    
    // Slice visible lines based on scroll position
    Elements visibleLines;
    int startLine = InfoScrollY_;
    int endLine = std::min(static_cast<int>(lines.size()), startLine + 25);
    for (int i = startLine; i < endLine; ++i) {
        visibleLines.push_back(std::move(lines[i]));
    }
    
    // Show scroll indicator if content exceeds visible area
    Element scrollIndicator = text("");
    if (lines.size() > 25) {
        TString indicator = Sprintf(" (%d/%d) ", InfoScrollY_ + 1, static_cast<int>(lines.size()));
        if (InfoScrollY_ > 0) indicator = "↑" + indicator;
        if (InfoScrollY_ < maxScroll) indicator = indicator + "↓";
        scrollIndicator = text(indicator.c_str()) | dim | center;
    }
    
    auto content = vbox({
        vbox(std::move(visibleLines)),
        scrollIndicator
    });
    
    // Full-screen view with border
    return content | flex | border;
}

Element TTopicDetailsView::RenderTabletsView() {
    Elements lines;
    
    // Title
    lines.push_back(text(" Topic Tablets ") | bold | center);
    lines.push_back(separator());
    
    if (LoadingTablets_) {
        lines.push_back(text(" Loading tablets... ") | dim | center);
    } else if (Tablets_.empty()) {
        lines.push_back(text(" No tablets found ") | dim | center);
        lines.push_back(text(" (Viewer API may not be available) ") | dim | center);
    } else {
        // Header row
        lines.push_back(hbox({
            text(" Type") | bold | size(WIDTH, EQUAL, 22),
            text(" Tablet ID") | bold | size(WIDTH, EQUAL, 20),
            text(" State") | bold | size(WIDTH, EQUAL, 10),
            text(" Overall") | bold | size(WIDTH, EQUAL, 8),
            text(" Node") | bold | size(WIDTH, EQUAL, 8),
            text(" Gen") | bold | size(WIDTH, EQUAL, 8),
            text(" Uptime") | bold | size(WIDTH, EQUAL, 12)
        }));
        lines.push_back(separator());
        
        // Helper to format uptime
        auto formatUptime = [](TInstant changeTime) -> TString {
            if (changeTime == TInstant::Zero()) {
                return "-";
            }
            TDuration uptime = TInstant::Now() - changeTime;
            ui64 totalSecs = uptime.Seconds();
            ui64 days = totalSecs / 86400;
            ui64 hours = (totalSecs % 86400) / 3600;
            ui64 mins = (totalSecs % 3600) / 60;
            ui64 secs = totalSecs % 60;
            if (days > 0) {
                return Sprintf("%lud %02lu:%02lu:%02lu", days, hours, mins, secs);
            }
            return Sprintf("%02lu:%02lu:%02lu", hours, mins, secs);
        };
        
        // Helper to get color for overall status
        auto overallColor = [](const TString& overall) {
            if (overall == "Green") return Color::Green;
            if (overall == "Yellow") return Color::Yellow;
            if (overall == "Red") return Color::Red;
            return Color::GrayDark;
        };
        
        // Tablet rows
        for (const auto& tablet : Tablets_) {
            auto stateColor = tablet.State == "Active" ? Color::Green : Color::Yellow;
            lines.push_back(hbox({
                text(TString(" ") + tablet.Type) | size(WIDTH, EQUAL, 22),
                text(TString(" ") + ToString(tablet.TabletId)) | size(WIDTH, EQUAL, 20),
                text(TString(" ") + tablet.State) | color(stateColor) | size(WIDTH, EQUAL, 10),
                text(TString(" ") + tablet.Overall) | color(overallColor(tablet.Overall)) | size(WIDTH, EQUAL, 8),
                text(TString(" ") + ToString(tablet.NodeId)) | size(WIDTH, EQUAL, 8),
                text(TString(" ") + ToString(tablet.Generation)) | size(WIDTH, EQUAL, 8),
                text(TString(" ") + formatUptime(tablet.ChangeTime)) | size(WIDTH, EQUAL, 12)
            }));
        }
    }
    
    lines.push_back(separator());
    lines.push_back(text(" [t/Esc] Close  [↑↓] Scroll  [r] Refresh ") | dim | center);
    
    // Clamp scroll position to valid range
    int maxScroll = std::max(0, static_cast<int>(lines.size()) - 15);
    TabletsScrollY_ = std::min(TabletsScrollY_, maxScroll);
    
    // Slice visible lines based on scroll position
    Elements visibleLines;
    int startLine = TabletsScrollY_;
    int endLine = std::min(static_cast<int>(lines.size()), startLine + 20);
    for (int i = startLine; i < endLine; ++i) {
        visibleLines.push_back(std::move(lines[i]));
    }
    
    auto content = vbox(std::move(visibleLines));
    
    // Full-screen view with border
    return content | flex | border;
}

} // namespace NYdb::NConsoleClient
