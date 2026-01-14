#include "topic_details_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Define table columns for partitions
static TVector<TTableColumn> CreatePartitionsTableColumns() {
    return {
        {"ID", 5},
        {"Size", NTheme::ColBytes},
        {"Write/min", NTheme::ColBytesPerMin},
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

TTopicDetailsView::TTopicDetailsView(TTopicTuiApp& app)
    : App_(app)
    , PartitionsTable_(CreatePartitionsTableColumns())
    , ConsumersTable_(CreateConsumersTableColumns())
{
    // Set up partition table selection callback
    PartitionsTable_.OnSelect = [this](int row) {
        if (row >= 0 && row < static_cast<int>(Partitions_.size())) {
            App_.GetState().SelectedPartition = Partitions_[row].PartitionId;
            if (OnShowMessages) {
                OnShowMessages();
            }
        }
    };
    
    // Set up consumer table selection callback
    ConsumersTable_.OnSelect = [this](int row) {
        if (row >= 0 && row < static_cast<int>(Consumers_.size())) {
            if (OnConsumerSelected) {
                OnConsumerSelected(Consumers_[row].Name);
            }
        }
    };
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
    
    // Create consumers panel component
    auto consumersPanel = Renderer(consumersTableComp, [this, consumersTableComp] {
        Element content;
        // Only show spinner if loading AND we have no data yet
        if (LoadingConsumers_ && Consumers_.empty()) {
            content = NTheme::RenderSpinner(SpinnerFrame_, "Loading consumers...") | center;
        } else if (!ConsumersError_.empty() && Consumers_.empty()) {
            content = text("Error: " + std::string(ConsumersError_.c_str())) | color(NTheme::ErrorText) | center;
        } else if (Consumers_.empty()) {
            content = text("No consumers") | dim | center;
        } else {
            content = consumersTableComp->Render();
        }
        
        // Show subtle refresh indicator in header when loading with existing data
        auto header = LoadingConsumers_ && !Consumers_.empty()
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
        
        // Info modal handling FIRST - block all other events when modal is open
        if (ShowingInfo_) {
            // Toggle off with 'i'
            if (event == Event::Character('i') || event == Event::Character('I')) {
                ShowingInfo_ = false;
                return true;
            }
            // Close with Esc
            if (event == Event::Escape) {
                ShowingInfo_ = false;
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
            // Consume all other events when modal is open
            return true;
        }
        
        // Tablets modal handling - block all other events when modal is open
        if (ShowingTablets_) {
            // Toggle off with 't'
            if (event == Event::Character('t') || event == Event::Character('T')) {
                ShowingTablets_ = false;
                return true;
            }
            // Close with Esc
            if (event == Event::Escape) {
                ShowingTablets_ = false;
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
            // Consume all other events when modal is open
            return true;
        }
        
        // Open info modal
        if (event == Event::Character('i') || event == Event::Character('I')) {
            ShowingInfo_ = true;
            InfoScrollY_ = 0;
            return true;
        }
        
        // Open tablets modal
        if (event == Event::Character('t') || event == Event::Character('T')) {
            ShowingTablets_ = true;
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
            return true;
        }
        
        // Update focus state for both tables
        PartitionsTable_.SetFocused(FocusPanel_ == 0);
        ConsumersTable_.SetFocused(FocusPanel_ == 1);
        
        // Tab switches between panels
        if (event == Event::Tab) {
            FocusPanel_ = (FocusPanel_ + 1) % 2;
            return true;
        }
        
        // Delegate navigation to the focused table
        if (FocusPanel_ == 0 && !Partitions_.empty()) {
            if (PartitionsTable_.HandleEvent(event)) {
                return true;
            }
        } else if (FocusPanel_ == 1 && !Consumers_.empty()) {
            if (ConsumersTable_.HandleEvent(event)) {
                return true;
            }
        }
        
        // Custom key handlers
        if (event == Event::Character('w') || event == Event::Character('W')) {
            if (OnWriteMessage) {
                OnWriteMessage();
            }
            return true;
        }
        if (event == Event::Character('a') || event == Event::Character('A')) {
            if (OnAddConsumer) {
                OnAddConsumer();
            }
            return true;
        }
        if (event == Event::Character('x') || event == Event::Character('X')) {
            if (FocusPanel_ == 1) {
                int row = ConsumersTable_.GetSelectedRow();
                if (row >= 0 && row < static_cast<int>(Consumers_.size())) {
                    if (OnDropConsumer) {
                        OnDropConsumer(Consumers_[row].Name);
                    }
                }
            }
            return true;
        }
        
        return false;
    }) | Renderer([this, splitContainer](Element) {
        CheckAsyncCompletion();
        
        // Base UI structure
        auto mainContent = vbox({
            RenderHeader(),
            separator(),
            splitContainer->Render() | flex
        });
        
        // Show info modal as overlay if active
        if (ShowingInfo_) {
            return dbox({
                mainContent | dim,
                RenderInfoModal() | clear_under | center
            });
        }
        
        // Show tablets modal as overlay if active
        if (ShowingTablets_) {
            return dbox({
                mainContent | dim,
                RenderTabletsModal() | clear_under | center
            });
        }
        
        return mainContent;
    });
}

void TTopicDetailsView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    StartAsyncLoads();
}

void TTopicDetailsView::Refresh() {
    StartAsyncLoads();
}

void TTopicDetailsView::CheckAsyncCompletion() {
    SpinnerFrame_++;
    
    // Check topic data future
    if (LoadingTopic_ && TopicFuture_.valid()) {
        if (TopicFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto data = TopicFuture_.get();
                Partitions_ = std::move(data.Partitions);
                TotalPartitions_ = data.TotalPartitions;
                RetentionPeriod_ = data.RetentionPeriod;
                WriteSpeedBytesPerSec_ = data.WriteSpeedBytesPerSec;
                
                // Store extended fields for info modal
                Owner_ = data.Owner;
                SupportedCodecs_ = std::move(data.SupportedCodecs);
                Attributes_ = std::move(data.Attributes);
                MeteringMode_ = data.MeteringMode;
                PartitionWriteBurstBytes_ = data.PartitionWriteBurstBytes;
                RetentionStorageMb_ = data.RetentionStorageMb;
                MinActivePartitions_ = data.MinActivePartitions;
                MaxActivePartitions_ = data.MaxActivePartitions;
                AutoPartitioningStrategy_ = data.AutoPartitioningStrategy;
                
                if (data.WriteRateBytesPerSec > 0) {
                    WriteRateHistory_.push_back(data.WriteRateBytesPerSec);
                    while (WriteRateHistory_.size() > 60) {
                        WriteRateHistory_.erase(WriteRateHistory_.begin());
                    }
                }
                TopicError_.clear();
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
}

Element TTopicDetailsView::RenderHeader() {
    if (TopicPath_.empty()) {
        return text("No topic selected") | dim;
    }
    
    std::string partitionsText = LoadingTopic_ ? "..." : std::to_string(TotalPartitions_);
    std::string retentionText = LoadingTopic_ ? "..." : std::string(FormatDuration(RetentionPeriod_).c_str());
    std::string speedText = LoadingTopic_ ? "..." : std::string(FormatBytes(WriteSpeedBytesPerSec_).c_str()) + "/s";
    
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
            text("   Write Speed: ") | dim,
            text(speedText) | bold
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
        PartitionsTable_.UpdateCell(i, 3, FormatDuration(p.WriteTimeLag));
        PartitionsTable_.UpdateCell(i, 4, FormatNumber(p.StartOffset));
        PartitionsTable_.UpdateCell(i, 5, FormatNumber(p.EndOffset));
        PartitionsTable_.UpdateCell(i, 6, p.NodeId > 0 ? ToString(p.NodeId) : "-");
        PartitionsTable_.UpdateCell(i, 7, lastWriteStr);
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
            RenderSparkline(WriteRateHistory_, 30),
            text(" ") | flex
        })
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
        
        TopicFuture_ = std::async(std::launch::async, [path, topicClient]() -> TTopicBasicData {
            TTopicBasicData data;
            
            auto result = topicClient->DescribeTopic(path,
                NTopic::TDescribeTopicSettings()
                    .IncludeStats(true)
                    .IncludeLocation(true)).GetValueSync();
            
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
        Consumers_.clear();
        
        ConsumersFuture_ = std::async(std::launch::async, [path, topicClient]() -> TConsumersData {
            TConsumersData data;
            
            auto result = topicClient->DescribeTopic(path,
                NTopic::TDescribeTopicSettings()).GetValueSync();
            
            if (!result.IsSuccess()) {
                throw std::runtime_error(result.GetIssues().ToString());
            }
            
            const auto& desc = result.GetTopicDescription();
            
            for (const auto& c : desc.GetConsumers()) {
                TConsumerDisplayInfo info;
                info.Name = TString(c.GetConsumerName());
                info.IsImportant = c.GetImportant();
                
                // Fetch detailed consumer stats
                auto consumerResult = topicClient->DescribeConsumer(path, info.Name,
                    NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
                
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
                
                data.Consumers.push_back(info);
            }
            
            return data;
        });
    }
}

Element TTopicDetailsView::RenderInfoModal() {
    using namespace ftxui;
    
    // Helper for formatting bytes
    auto formatBytes = [](ui64 bytes) -> TString {
        if (bytes >= 1024 * 1024 * 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / (1024.0 * 1024.0 * 1024.0)) << " GB";
        } else if (bytes >= 1024 * 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / (1024.0 * 1024.0)) << " MB";
        } else if (bytes >= 1024) {
            return TStringBuilder() << Sprintf("%.2f", bytes / 1024.0) << " KB";
        }
        return TStringBuilder() << bytes << " B";
    };
    
    // Helper for metering mode
    auto meteringStr = [](NTopic::EMeteringMode mode) -> TString {
        switch (mode) {
            case NTopic::EMeteringMode::ReservedCapacity: return "Reserved Capacity";
            case NTopic::EMeteringMode::RequestUnits: return "Request Units";
            default: return "Unspecified";
        }
    };
    
    // Helper for auto partitioning strategy
    auto strategyStr = [](NTopic::EAutoPartitioningStrategy s) -> TString {
        switch (s) {
            case NTopic::EAutoPartitioningStrategy::ScaleUp: return "Scale Up";
            case NTopic::EAutoPartitioningStrategy::ScaleUpAndDown: return "Scale Up and Down";
            case NTopic::EAutoPartitioningStrategy::Paused: return "Paused";
            default: return "Disabled";
        }
    };
    
    // Helper for codec
    auto codecStr = [](NTopic::ECodec c) -> TString {
        switch (c) {
            case NTopic::ECodec::RAW: return "RAW";
            case NTopic::ECodec::GZIP: return "GZIP";
            case NTopic::ECodec::LZOP: return "LZOP";
            case NTopic::ECodec::ZSTD: return "ZSTD";
            default: return "Unknown";
        }
    };
    
    Elements lines;
    
    // Title
    lines.push_back(text(" Topic Information ") | bold | center);
    lines.push_back(separator());
    
    // Path and Owner
    lines.push_back(hbox({text(" Path:   ") | dim, text(TopicPath_.c_str())}));
    lines.push_back(hbox({text(" Owner:  ") | dim, text(Owner_.c_str())}));
    lines.push_back(separator());
    
    // Partitioning
    lines.push_back(text(" Partitioning") | bold);
    lines.push_back(hbox({text("   Partitions:        ") | dim, text(ToString(TotalPartitions_).c_str())}));
    lines.push_back(hbox({text("   Min Active:        ") | dim, text(ToString(MinActivePartitions_).c_str())}));
    lines.push_back(hbox({text("   Max Active:        ") | dim, text(ToString(MaxActivePartitions_).c_str())}));
    lines.push_back(hbox({text("   Auto-Partitioning: ") | dim, text(strategyStr(AutoPartitioningStrategy_).c_str())}));
    lines.push_back(separator());
    
    // Retention
    lines.push_back(text(" Retention") | bold);
    lines.push_back(hbox({text("   Period:  ") | dim, text(Sprintf("%.2f hours", RetentionPeriod_.Hours()).c_str())}));
    lines.push_back(hbox({text("   Storage: ") | dim, text(RetentionStorageMb_ > 0 
        ? (TStringBuilder() << RetentionStorageMb_ << " MB").c_str() 
        : "Unlimited")}));
    lines.push_back(separator());
    
    // Write Limits
    lines.push_back(text(" Write Limits") | bold);
    lines.push_back(hbox({text("   Speed per partition: ") | dim, text(formatBytes(WriteSpeedBytesPerSec_).c_str()), text("/s")}));
    lines.push_back(hbox({text("   Burst per partition: ") | dim, text(formatBytes(PartitionWriteBurstBytes_).c_str())}));
    lines.push_back(separator());
    
    // Codecs
    lines.push_back(text(" Supported Codecs") | bold);
    TStringBuilder codecsLine;
    for (size_t i = 0; i < SupportedCodecs_.size(); ++i) {
        if (i > 0) codecsLine << ", ";
        codecsLine << codecStr(SupportedCodecs_[i]);
    }
    if (SupportedCodecs_.empty()) codecsLine << "(none)";
    lines.push_back(hbox({text("   ") | dim, text(codecsLine.c_str())}));
    lines.push_back(separator());
    
    // Metering
    lines.push_back(hbox({text(" Metering: ") | dim, text(meteringStr(MeteringMode_).c_str())}));
    lines.push_back(separator());
    
    // Attributes
    lines.push_back(text(" Attributes") | bold);
    if (Attributes_.empty()) {
        lines.push_back(hbox({text("   ") | dim, text("(none)")}));
    } else {
        for (const auto& [key, value] : Attributes_) {
            lines.push_back(hbox({
                text("   ") | dim,
                text(key) | color(Color::Cyan),
                text(" = "),
                text(value)
            }));
        }
    }
    
    lines.push_back(separator());
    lines.push_back(text(" [i/Esc] Close  [↑↓] Scroll ") | dim | center);
    
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
    
    // Use bgcolor to fully cover underlying content (prevents highlight bleed-through)
    return content 
        | size(WIDTH, LESS_THAN, 70) 
        | size(HEIGHT, LESS_THAN, 28) 
        | border
        | bgcolor(Color::Black);
}

Element TTopicDetailsView::RenderTabletsModal() {
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
            if (uptime.Hours() >= 24) {
                return Sprintf("%lud%luh", uptime.Days(), uptime.Hours() % 24);
            } else if (uptime.Hours() >= 1) {
                return Sprintf("%luh%lum", uptime.Hours(), uptime.Minutes() % 60);
            } else if (uptime.Minutes() >= 1) {
                return Sprintf("%lum%lus", uptime.Minutes(), uptime.Seconds() % 60);
            } else {
                return Sprintf("%lus", uptime.Seconds());
            }
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
    lines.push_back(text(" [t/Esc] Close  [↑↓] Scroll ") | dim | center);
    
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
    
    return content 
        | size(WIDTH, LESS_THAN, 100) 
        | size(HEIGHT, LESS_THAN, 22) 
        | border
        | bgcolor(Color::Black);
}

} // namespace NYdb::NConsoleClient
