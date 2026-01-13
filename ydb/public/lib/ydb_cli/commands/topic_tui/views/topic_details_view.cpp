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
        CheckAsyncCompletion();
        
        // Always render UI structure - each section shows spinner or content
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
            } catch (const std::exception& e) {
                TopicError_ = e.what();
            }
            LoadingTopic_ = false;
            SelectedPartitionIndex_ = 0;
        }
    }
    
    // Check consumers data future
    if (LoadingConsumers_ && ConsumersFuture_.valid()) {
        if (ConsumersFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
            try {
                auto data = ConsumersFuture_.get();
                Consumers_ = std::move(data.Consumers);
                ConsumersError_.clear();
            } catch (const std::exception& e) {
                ConsumersError_ = e.what();
            }
            LoadingConsumers_ = false;
            SelectedConsumerIndex_ = 0;
        }
    }
}

Element TTopicDetailsView::RenderSpinner(const std::string& msg) {
    static const std::vector<std::string> frames = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    std::string frame = frames[SpinnerFrame_ % frames.size()];
    return hbox({
        text(frame) | color(Color::Cyan),
        text(" " + msg) | dim
    }) | center;
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
            text(std::string(TopicPath_.c_str())) | color(Color::Cyan)
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

Element TTopicDetailsView::RenderPartitionsTable() {
    if (LoadingTopic_) {
        return RenderSpinner("Loading partitions...");
    }
    
    if (!TopicError_.empty()) {
        return text("Error: " + std::string(TopicError_.c_str())) | color(Color::Red) | center;
    }
    
    if (Partitions_.empty()) {
        return text("No partitions") | dim | center;
    }
    
    Elements rows;
    
    rows.push_back(hbox({
        text("ID") | size(WIDTH, EQUAL, 5) | bold,
        text("Offset Range") | size(WIDTH, EQUAL, 25) | bold,
        text("Size") | size(WIDTH, EQUAL, 10) | bold | align_right,
        text("Lag") | size(WIDTH, EQUAL, 8) | bold | align_right
    }) | bgcolor(Color::GrayDark));
    
    for (size_t i = 0; i < Partitions_.size(); ++i) {
        const auto& p = Partitions_[i];
        bool selected = FocusPanel_ == 0 && static_cast<int>(i) == SelectedPartitionIndex_;
        
        std::string offsetRange = std::string(FormatNumber(p.StartOffset).c_str()) + " - " + std::string(FormatNumber(p.EndOffset).c_str());
        
        Element row = hbox({
            text(std::to_string(p.PartitionId)) | size(WIDTH, EQUAL, 5),
            text(offsetRange) | size(WIDTH, EQUAL, 25),
            text(std::string(FormatBytes(p.StoreSizeBytes).c_str())) | size(WIDTH, EQUAL, 10) | align_right,
            text(std::string(FormatDuration(p.WriteTimeLag).c_str())) | size(WIDTH, EQUAL, 8) | align_right
        });
        
        if (selected) {
            row = row | bgcolor(Color::GrayDark);
        }
        
        rows.push_back(row);
    }
    
    return vbox(rows) | yframe;
}

Element TTopicDetailsView::RenderConsumersList() {
    if (LoadingConsumers_) {
        return RenderSpinner("Loading consumers...");
    }
    
    if (!ConsumersError_.empty()) {
        return text("Error: " + std::string(ConsumersError_.c_str())) | color(Color::Red) | center;
    }
    
    if (Consumers_.empty()) {
        return text("No consumers") | dim | center;
    }
    
    Elements rows;
    
    for (size_t i = 0; i < Consumers_.size(); ++i) {
        const auto& c = Consumers_[i];
        bool selected = FocusPanel_ == 1 && static_cast<int>(i) == SelectedConsumerIndex_;
        
        std::string prefix = selected ? "> " : "  ";
        
        Elements parts;
        parts.push_back(text(prefix + std::string(c.Name.c_str())) | flex);
        
        double lagRatio = 0.0;
        if (c.TotalLag > 0) {
            lagRatio = std::min(1.0, c.TotalLag / 10000.0);
        }
        parts.push_back(RenderGaugeBar(lagRatio, 8, false));
        parts.push_back(text(" " + std::string(FormatNumber(c.TotalLag).c_str())));
        
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
