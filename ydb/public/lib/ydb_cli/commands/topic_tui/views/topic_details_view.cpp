#include "topic_details_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTopicDetailsView::TTopicDetailsView(TTopicTuiApp& app)
    : App_(app)
{}

Component TTopicDetailsView::Build() {
    return Renderer([this] {
        if (Loading_) {
            return vbox({
                text("Loading topic details...") | center
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + ErrorMessage_) | color(Color::Red) | center
            }) | border;
        }
        
        if (TotalPartitions_ == 0) {
            return vbox({
                text("No topic selected") | dim | center
            }) | border;
        }
        
        return vbox({
            RenderHeader(),
            separator(),
            hbox({
                vbox({
                    text(" Partitions ") | bold,
                    separator(),
                    RenderPartitionsTable() | flex
                }) | border | flex,
                vbox({
                    text(" Consumers ") | bold,
                    separator(),
                    RenderConsumersList() | flex,
                    separator(),
                    RenderWriteRateChart()
                }) | border | size(WIDTH, EQUAL, 40)
            }) | flex
        });
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::TopicDetails) {
            return false;
        }
        
        if (event == Event::ArrowUp) {
            if (FocusPanel_ == 0 && SelectedPartitionIndex_ > 0) {
                SelectedPartitionIndex_--;
            } else if (FocusPanel_ == 1 && SelectedConsumerIndex_ > 0) {
                SelectedConsumerIndex_--;
            }
            return true;
        }
        if (event == Event::ArrowDown) {
            if (FocusPanel_ == 0 && SelectedPartitionIndex_ < static_cast<int>(Partitions_.size()) - 1) {
                SelectedPartitionIndex_++;
            } else if (FocusPanel_ == 1 && SelectedConsumerIndex_ < static_cast<int>(Consumers_.size()) - 1) {
                SelectedConsumerIndex_++;
            }
            return true;
        }
        if (event == Event::Tab) {
            FocusPanel_ = (FocusPanel_ + 1) % 2;
            return true;
        }
        if (event == Event::Return) {
            if (FocusPanel_ == 1 && SelectedConsumerIndex_ >= 0 && 
                SelectedConsumerIndex_ < static_cast<int>(Consumers_.size())) {
                if (OnConsumerSelected) {
                    OnConsumerSelected(Consumers_[SelectedConsumerIndex_].Name);
                }
            }
            return true;
        }
        if (event == Event::Character('m') || event == Event::Character('M')) {
            if (SelectedPartitionIndex_ >= 0 && SelectedPartitionIndex_ < static_cast<int>(Partitions_.size())) {
                App_.GetState().SelectedPartition = Partitions_[SelectedPartitionIndex_].PartitionId;
            }
            if (OnShowMessages) {
                OnShowMessages();
            }
            return true;
        }
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
            if (FocusPanel_ == 1 && SelectedConsumerIndex_ >= 0 && 
                SelectedConsumerIndex_ < static_cast<int>(Consumers_.size())) {
                if (OnDropConsumer) {
                    OnDropConsumer(Consumers_[SelectedConsumerIndex_].Name);
                }
            }
            return true;
        }
        return false;
    });
}

void TTopicDetailsView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    LoadTopicInfo();
}

void TTopicDetailsView::Refresh() {
    LoadTopicInfo();
}

Element TTopicDetailsView::RenderHeader() {
    if (TotalPartitions_ == 0) {
        return text("No topic") | dim;
    }
    
    return vbox({
        hbox({
            text(" Topic: ") | bold,
            text(TopicPath_) | color(Color::Cyan)
        }),
        hbox({
            text(" Partitions: ") | dim,
            text(std::to_string(TotalPartitions_)) | bold,
            text("   Retention: ") | dim,
            text(std::string(FormatDuration(RetentionPeriod_).c_str())) | bold,
            text("   Write Speed: ") | dim,
            text(std::string(FormatBytes(WriteSpeedBytesPerSec_).c_str()) + "/s") | bold
        })
    });
}

Element TTopicDetailsView::RenderPartitionsTable() {
    if (Partitions_.empty()) {
        return text("No partitions") | dim | center;
    }
    
    Elements rows;
    
    // Header
    rows.push_back(hbox({
        text("ID") | size(WIDTH, EQUAL, 5) | bold,
        text("Offset Range") | size(WIDTH, EQUAL, 25) | bold,
        text("Size") | size(WIDTH, EQUAL, 10) | bold | align_right,
        text("Lag") | size(WIDTH, EQUAL, 8) | bold | align_right
    }) | bgcolor(Color::GrayDark));
    
    for (size_t i = 0; i < Partitions_.size(); ++i) {
        const auto& p = Partitions_[i];
        bool selected = FocusPanel_ == 0 && static_cast<int>(i) == SelectedPartitionIndex_;
        
        TString offsetRange = Sprintf("%s - %s", 
            FormatNumber(p.StartOffset).c_str(),
            FormatNumber(p.EndOffset).c_str());
        
        Element row = hbox({
            text(ToString(p.PartitionId)) | size(WIDTH, EQUAL, 5),
            text(offsetRange) | size(WIDTH, EQUAL, 25),
            text(FormatBytes(p.StoreSizeBytes)) | size(WIDTH, EQUAL, 10) | align_right,
            text(FormatDuration(p.WriteTimeLag)) | size(WIDTH, EQUAL, 8) | align_right
        });
        
        if (selected) {
            row = row | bgcolor(Color::GrayDark);
        }
        
        rows.push_back(row);
    }
    
    return vbox(rows) | yframe;
}

Element TTopicDetailsView::RenderConsumersList() {
    if (Consumers_.empty()) {
        return text("No consumers") | dim | center;
    }
    
    Elements rows;
    
    for (size_t i = 0; i < Consumers_.size(); ++i) {
        const auto& c = Consumers_[i];
        bool selected = FocusPanel_ == 1 && static_cast<int>(i) == SelectedConsumerIndex_;
        
        TString prefix = selected ? "> " : "  ";
        
        Elements parts;
        parts.push_back(text(prefix + c.Name) | flex);
        
        // Lag gauge
        double lagRatio = 0.0;
        if (c.TotalLag > 0) {
            // Assume max lag is 10000 for visualization
            lagRatio = std::min(1.0, c.TotalLag / 10000.0);
        }
        parts.push_back(RenderGaugeBar(lagRatio, 8, false));
        parts.push_back(text(" " + FormatNumber(c.TotalLag)));
        
        Element row = hbox(parts);
        
        if (selected) {
            row = row | bgcolor(Color::GrayDark);
        }
        if (c.IsImportant) {
            row = row | bold;
        }
        
        rows.push_back(row);
    }
    
    return vbox(rows);
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

void TTopicDetailsView::LoadTopicInfo() {
    Loading_ = true;
    ErrorMessage_.clear();
    Partitions_.clear();
    Consumers_.clear();
    TotalPartitions_ = 0;
    RetentionPeriod_ = TDuration::Zero();
    WriteSpeedBytesPerSec_ = 0;
    
    try {
        auto& topicClient = App_.GetTopicClient();
        
        // Describe topic with stats
        auto result = topicClient.DescribeTopic(TopicPath_,
            NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        
        if (!result.IsSuccess()) {
            ErrorMessage_ = TString(result.GetIssues().ToString());
            Loading_ = false;
            return;
        }
        
        const auto& desc = result.GetTopicDescription();
        
        // Cache topic metadata for header
        TotalPartitions_ = desc.GetPartitions().size();
        RetentionPeriod_ = desc.GetRetentionPeriod();
        WriteSpeedBytesPerSec_ = desc.GetPartitionWriteSpeedBytesPerSecond();
        
        // Extract partition info
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
            }
            
            Partitions_.push_back(info);
        }
        
        // Extract consumer info
        for (const auto& c : desc.GetConsumers()) {
            TConsumerDisplayInfo info;
            info.Name = TString(c.GetConsumerName());
            info.IsImportant = c.GetImportant();
            
            // Need to describe consumer for detailed stats
            auto consumerResult = topicClient.DescribeConsumer(TopicPath_, info.Name,
                NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            
            if (consumerResult.IsSuccess()) {
                const auto& consumerDesc = consumerResult.GetConsumerDescription();
                for (const auto& part : consumerDesc.GetPartitions()) {
                    if (part.GetPartitionStats() && part.GetPartitionConsumerStats()) {
                        const auto& pStats = *part.GetPartitionConsumerStats();
                        ui64 endOffset = part.GetPartitionStats()->GetEndOffset();
                        ui64 committedOffset = pStats.GetCommittedOffset();
                        if (endOffset > committedOffset) {
                            info.TotalLag += endOffset - committedOffset;
                        }
                        
                        // Track max lag time
                        if (pStats.GetMaxCommittedTimeLag() > info.MaxLagTime) {
                            info.MaxLagTime = pStats.GetMaxCommittedTimeLag();
                        }
                    }
                }
            }
            
            Consumers_.push_back(info);
        }
        
        // Update write rate history (just add current stats)
        const auto& topicStats = desc.GetTopicStats();
        // Convert bytes per minute to bytes per second
        double writeRate = topicStats.GetBytesWrittenPerMinute() / 60.0;
        WriteRateHistory_.push_back(writeRate);
        
        // Keep last 60 samples
        while (WriteRateHistory_.size() > 60) {
            WriteRateHistory_.erase(WriteRateHistory_.begin());
        }
        
    } catch (const std::exception& e) {
        ErrorMessage_ = e.what();
    }
    
    Loading_ = false;
}

} // namespace NYdb::NConsoleClient
