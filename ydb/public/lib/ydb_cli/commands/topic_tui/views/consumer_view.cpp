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
        CheckAsyncCompletion();
        
        if (Loading_) {
            return vbox({
                hbox({
                    text(" Consumer: ") | bold,
                    text(std::string(ConsumerName_.c_str())) | color(Color::Cyan)
                }),
                separator(),
                RenderSpinner() | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + std::string(ErrorMessage_.c_str())) | color(Color::Red) | center
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
        if (App_.GetState().CurrentView != EViewType::ConsumerDetails) {
            return false;
        }
        if (Loading_) {
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
    StartAsyncLoad();
}

void TConsumerView::Refresh() {
    StartAsyncLoad();
}

void TConsumerView::CheckAsyncCompletion() {
    if (!Loading_ || !LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto data = LoadFuture_.get();
            PartitionStats_ = std::move(data.PartitionStats);
            TotalLag_ = data.TotalLag;
            MaxLagTime_ = data.MaxLagTime;
            ErrorMessage_.clear();
        } catch (const std::exception& e) {
            ErrorMessage_ = e.what();
        }
        Loading_ = false;
        SelectedPartitionIndex_ = 0;
    } else {
        SpinnerFrame_++;
    }
}

Element TConsumerView::RenderSpinner() {
    static const std::vector<std::string> frames = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    std::string frame = frames[SpinnerFrame_ % frames.size()];
    return hbox({
        text(frame) | color(Color::Cyan),
        text(" Loading consumer details...") | dim
    });
}

Element TConsumerView::RenderHeader() {
    return vbox({
        hbox({
            text(" Consumer: ") | bold,
            text(std::string(ConsumerName_.c_str())) | color(Color::Cyan),
            text(" on ") | dim,
            text(std::string(TopicPath_.c_str())) | color(Color::White)
        }),
        hbox({
            text(" Total Lag: ") | dim,
            text(std::string(FormatNumber(TotalLag_).c_str())) | bold | color(TotalLag_ > 1000 ? Color::Red : Color::Green),
            text("   Max Lag Time: ") | dim,
            text(std::string(FormatDuration(MaxLagTime_).c_str())) | bold
        })
    });
}

Element TConsumerView::RenderPartitionStats() {
    if (PartitionStats_.empty()) {
        return text("No partition stats available") | dim | center;
    }
    
    Elements rows;
    
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
        
        std::string lastRead = p.LastReadTime != TInstant::Zero() ?
            std::string(p.LastReadTime.FormatLocalTime("%H:%M:%S").c_str()) : "-";
        
        Element row = hbox({
            text(std::to_string(p.PartitionId)) | size(WIDTH, EQUAL, 6),
            text(std::string(FormatNumber(p.CommittedOffset).c_str())) | size(WIDTH, EQUAL, 15) | align_right,
            text(std::string(FormatNumber(p.EndOffset).c_str())) | size(WIDTH, EQUAL, 15) | align_right,
            text(std::string(FormatNumber(p.Lag).c_str())) | size(WIDTH, EQUAL, 12) | align_right | 
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
            ratio = std::min(1.0, p.Lag / 10000.0);
        }
        
        gauges.push_back(hbox({
            text("P" + std::to_string(p.PartitionId) + " ") | size(WIDTH, EQUAL, 5),
            RenderGaugeBar(ratio, 15, false),
            text(" " + std::string(FormatNumber(p.Lag).c_str())) | size(WIDTH, EQUAL, 10) | align_right
        }));
    }
    
    return vbox(gauges);
}

void TConsumerView::StartAsyncLoad() {
    if (Loading_ || ConsumerName_.empty()) {
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    
    TString topicPath = TopicPath_;
    TString consumerName = ConsumerName_;
    auto* topicClient = &App_.GetTopicClient();
    
    LoadFuture_ = std::async(std::launch::async, [topicPath, consumerName, topicClient]() -> TConsumerData {
        TConsumerData data;
        
        auto result = topicClient->DescribeConsumer(topicPath, consumerName,
            NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
        
        if (!result.IsSuccess()) {
            throw std::runtime_error(result.GetIssues().ToString());
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
                
                data.TotalLag += info.Lag;
                if (info.CommitTimeLag > data.MaxLagTime) {
                    data.MaxLagTime = info.CommitTimeLag;
                }
            }
            
            data.PartitionStats.push_back(info);
        }
        
        return data;
    });
}

} // namespace NYdb::NConsoleClient
