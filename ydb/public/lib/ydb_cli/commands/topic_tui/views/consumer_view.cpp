#include "consumer_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TConsumerView::TConsumerView(TTopicTuiApp& app)
    : App_(app)
{}

Component TConsumerView::Build() {
    return Renderer([this] {
        if (Loading_) {
            return vbox({
                text("Loading consumer details...") | center
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + ErrorMessage_) | color(Color::Red) | center
            }) | border;
        }
        
        return vbox({
            RenderHeader(),
            separator(),
            hbox({
                RenderPartitionStats() | flex,
                separator(),
                RenderLagGauges() | size(WIDTH, EQUAL, 35)
            }) | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::ConsumerDetails) {
            return false;
        }
        
        if (event == Event::ArrowUp) {
            if (SelectedPartitionIndex_ > 0) {
                SelectedPartitionIndex_--;
            }
            return true;
        }
        if (event == Event::ArrowDown) {
            if (SelectedPartitionIndex_ < static_cast<int>(PartitionStats_.size()) - 1) {
                SelectedPartitionIndex_++;
            }
            return true;
        }
        if (event == Event::Character('o') || event == Event::Character('O')) {
            if (SelectedPartitionIndex_ >= 0 && SelectedPartitionIndex_ < static_cast<int>(PartitionStats_.size())) {
                const auto& p = PartitionStats_[SelectedPartitionIndex_];
                if (OnCommitOffset) {
                    OnCommitOffset(p.PartitionId, p.CommittedOffset);
                }
            }
            return true;
        }
        return false;
    });
}

void TConsumerView::SetConsumer(const TString& topicPath, const TString& consumerName) {
    TopicPath_ = topicPath;
    ConsumerName_ = consumerName;
    LoadConsumerInfo();
}

void TConsumerView::Refresh() {
    LoadConsumerInfo();
}

Element TConsumerView::RenderHeader() {
    return vbox({
        hbox({
            text(" Consumer: ") | bold,
            text(ConsumerName_) | color(Color::Cyan),
            text(" on ") | dim,
            text(TopicPath_) | color(Color::White)
        }),
        hbox({
            text(" Total Lag: ") | dim,
            text(FormatNumber(TotalLag_)) | bold | color(TotalLag_ > 1000 ? Color::Red : Color::Green),
            text("   Max Lag Time: ") | dim,
            text(FormatDuration(MaxLagTime_)) | bold
        })
    });
}

Element TConsumerView::RenderPartitionStats() {
    if (PartitionStats_.empty()) {
        return text("No partition stats available") | dim | center;
    }
    
    Elements rows;
    
    // Header
    rows.push_back(hbox({
        text("Part") | size(WIDTH, EQUAL, 6) | bold,
        text("Committed") | size(WIDTH, EQUAL, 15) | bold | align_right,
        text("End Offset") | size(WIDTH, EQUAL, 15) | bold | align_right,
        text("Lag") | size(WIDTH, EQUAL, 12) | bold | align_right,
        text("Last Read") | size(WIDTH, EQUAL, 15) | bold | align_right
    }) | bgcolor(Color::GrayDark));
    
    for (size_t i = 0; i < PartitionStats_.size(); ++i) {
        const auto& p = PartitionStats_[i];
        bool selected = static_cast<int>(i) == SelectedPartitionIndex_;
        
        TString lastRead = p.LastReadTime != TInstant::Zero() ?
            p.LastReadTime.FormatLocalTime("%H:%M:%S") : "-";
        
        Element row = hbox({
            text(ToString(p.PartitionId)) | size(WIDTH, EQUAL, 6),
            text(FormatNumber(p.CommittedOffset)) | size(WIDTH, EQUAL, 15) | align_right,
            text(FormatNumber(p.EndOffset)) | size(WIDTH, EQUAL, 15) | align_right,
            text(FormatNumber(p.Lag)) | size(WIDTH, EQUAL, 12) | align_right | 
                color(p.Lag > 1000 ? Color::Red : (p.Lag > 100 ? Color::Yellow : Color::Green)),
            text(lastRead) | size(WIDTH, EQUAL, 15) | align_right
        });
        
        if (selected) {
            row = row | bgcolor(Color::GrayDark);
        }
        
        rows.push_back(row);
    }
    
    return vbox(rows) | yframe;
}

Element TConsumerView::RenderLagGauges() {
    Elements gauges;
    
    gauges.push_back(text(" Per-Partition Lag ") | bold);
    gauges.push_back(separator());
    
    for (const auto& p : PartitionStats_) {
        double ratio = 0.0;
        if (p.Lag > 0) {
            // Assume max lag is 10000 for visualization
            ratio = std::min(1.0, p.Lag / 10000.0);
        }
        
        gauges.push_back(hbox({
            text(Sprintf("P%d ", p.PartitionId)) | size(WIDTH, EQUAL, 5),
            RenderGaugeBar(ratio, 15, false),
            text(" " + FormatNumber(p.Lag)) | size(WIDTH, EQUAL, 10) | align_right
        }));
    }
    
    return vbox(gauges);
}

void TConsumerView::LoadConsumerInfo() {
    Loading_ = true;
    ErrorMessage_.clear();
    PartitionStats_.clear();
    TotalLag_ = 0;
    MaxLagTime_ = TDuration::Zero();
    
    try {
        auto& topicClient = App_.GetTopicClient();
        
        auto result = topicClient.DescribeConsumer(TopicPath_, ConsumerName_,
            NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
        
        if (!result.IsSuccess()) {
            ErrorMessage_ = TString(result.GetIssues().ToString());
            Loading_ = false;
            return;
        }
        
        const auto& desc = result.GetConsumerDescription();
        
        for (const auto& part : desc.GetPartitions()) {
            TPartitionConsumerInfo info;
            info.PartitionId = part.GetPartitionId();
            
            if (part.GetPartitionStats() && part.GetPartitionConsumerStats()) {
                const auto& consumerStats = *part.GetPartitionConsumerStats();
                const auto& partStats = *part.GetPartitionStats();
                
                info.CommittedOffset = consumerStats.GetCommittedOffset();
                info.EndOffset = partStats.GetEndOffset();
                info.Lag = info.EndOffset > info.CommittedOffset ? info.EndOffset - info.CommittedOffset : 0;
                info.LastReadTime = consumerStats.GetLastReadTime();
                info.ReadTimeLag = consumerStats.GetMaxReadTimeLag();
                info.CommitTimeLag = consumerStats.GetMaxCommittedTimeLag();
                
                TotalLag_ += info.Lag;
                if (info.CommitTimeLag > MaxLagTime_) {
                    MaxLagTime_ = info.CommitTimeLag;
                }
            }
            
            PartitionStats_.push_back(info);
        }
        
    } catch (const std::exception& e) {
        ErrorMessage_ = e.what();
    }
    
    Loading_ = false;
}

} // namespace NYdb::NConsoleClient
