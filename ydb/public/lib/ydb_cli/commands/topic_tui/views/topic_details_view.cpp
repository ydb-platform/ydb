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
        
        // Always render UI structure - each section shows spinner or content
        return vbox({
            RenderHeader(),
            separator(),
            splitContainer->Render() | flex
        });
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
                NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
            
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

} // namespace NYdb::NConsoleClient
