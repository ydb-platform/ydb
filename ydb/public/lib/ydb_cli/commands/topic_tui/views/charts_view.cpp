#include "charts_view.h"
#include "../app_interface.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TChartsView::TChartsView(ITuiApp& app)
    : App_(app)
{}

Component TChartsView::Build() {
    return Renderer([this] {
        if (Loading_) {
            return vbox({
                text("Loading metrics...") | center
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
            return vbox({
                text("Error: " + ErrorMessage_) | color(Color::Red) | center
            }) | border;
        }
        
        return vbox({
            hbox({
                text(" Charts: ") | bold,
                text(TopicPath_) | color(Color::Cyan)
            }),
            separator(),
            hbox({
                RenderWriteRateChart() | flex,
                separator(),
                vbox({
                    RenderConsumerLagGauges(),
                    separator(),
                    RenderPartitionDistribution()
                }) | size(WIDTH, EQUAL, 40)
            }) | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        // Only handle events when this view is active
        if (App_.GetState().CurrentView != EViewType::Charts) {
            return false;
        }
        
        if (event == Event::Character('r') || event == Event::Character('R')) {
            Refresh();
            return true;
        }
        return false;
    });
}

void TChartsView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    Metrics_ = TTopicMetrics();
    Metrics_.TopicPath = topicPath;
    UpdateMetrics();
}

void TChartsView::Refresh() {
    UpdateMetrics();
}

Element TChartsView::RenderWriteRateChart() {
    Elements lines;
    lines.push_back(text(" Write Rate (bytes/sec) ") | bold);
    lines.push_back(separator());
    
    // Convert metric points to vector of values
    TVector<double> values;
    for (const auto& p : Metrics_.WriteRateBytesPerSec) {
        values.push_back(p.Value);
    }
    
    if (values.empty()) {
        lines.push_back(text("No data yet") | dim | center);
    } else {
        // Big sparkline
        lines.push_back(RenderSparkline(values, 50));
        
        // Stats
        double current = values.empty() ? 0 : values.back();
        double max = 0;
        double avg = 0;
        for (double v : values) {
            max = std::max(max, v);
            avg += v;
        }
        if (!values.empty()) {
            avg /= values.size();
        }
        
        lines.push_back(hbox({
            text(" Current: ") | dim,
            text(FormatBytes(static_cast<ui64>(current)) + "/s") | bold,
            text("  Avg: ") | dim,
            text(FormatBytes(static_cast<ui64>(avg)) + "/s"),
            text("  Max: ") | dim,
            text(FormatBytes(static_cast<ui64>(max)) + "/s")
        }));
    }
    
    return vbox(lines);
}

Element TChartsView::RenderConsumerLagGauges() {
    Elements lines;
    lines.push_back(text(" Consumer Lag ") | bold);
    lines.push_back(separator());
    
    if (Metrics_.ConsumerLags.empty()) {
        lines.push_back(text("No consumers") | dim);
    } else {
        for (const auto& [name, lag] : Metrics_.ConsumerLags) {
            double ratio = lag > 0 ? std::min(1.0, lag / 10000.0) : 0.0;
            lines.push_back(hbox({
                text(name) | size(WIDTH, EQUAL, 15),
                RenderGaugeBar(ratio, 12, false),
                text(" " + FormatNumber(lag))
            }));
        }
    }
    
    return vbox(lines);
}

Element TChartsView::RenderPartitionDistribution() {
    Elements lines;
    lines.push_back(text(" Partition Activity ") | bold);
    lines.push_back(separator());
    
    // TODO: Show per-partition write activity
    lines.push_back(text("Not yet implemented") | dim);
    
    return vbox(lines);
}

void TChartsView::UpdateMetrics() {
    if (TopicPath_.empty()) {
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    
    try {
        auto& topicClient = App_.GetTopicClient();
        
        auto result = topicClient.DescribeTopic(TopicPath_,
            NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        
        if (!result.IsSuccess()) {
            ErrorMessage_ = TString(result.GetIssues().ToString());
            Loading_ = false;
            return;
        }
        
        const auto& desc = result.GetTopicDescription();
        
        // Update write rate history
        const auto& stats = desc.GetTopicStats();
        TMetricPoint point;
        point.Time = TInstant::Now();
        point.Value = stats.GetBytesWrittenPerMinute() / 60.0;
        
        Metrics_.WriteRateBytesPerSec.push_back(point);
        
        // Keep last N samples
        while (Metrics_.WriteRateBytesPerSec.size() > MaxHistoryPoints) {
            Metrics_.WriteRateBytesPerSec.erase(Metrics_.WriteRateBytesPerSec.begin());
        }
        
        // Update consumer lags
        Metrics_.ConsumerLags.clear();
        for (const auto& consumer : desc.GetConsumers()) {
            TString name(consumer.GetConsumerName());
            
            auto consumerResult = topicClient.DescribeConsumer(TopicPath_, name,
                NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            
            if (consumerResult.IsSuccess()) {
                const auto& consumerDesc = consumerResult.GetConsumerDescription();
                ui64 totalLag = 0;
                
                for (const auto& part : consumerDesc.GetPartitions()) {
                    // Get partition stats for end offset
                    if (part.GetPartitionStats() && part.GetPartitionConsumerStats()) {
                        ui64 endOffset = part.GetPartitionStats()->GetEndOffset();
                        ui64 committedOffset = part.GetPartitionConsumerStats()->GetCommittedOffset();
                        if (endOffset > committedOffset) {
                            totalLag += endOffset - committedOffset;
                        }
                    }
                }
                
                Metrics_.ConsumerLags[name] = totalLag;
            }
        }
        
    } catch (const std::exception& e) {
        ErrorMessage_ = e.what();
    }
    
    Loading_ = false;
}

} // namespace NYdb::NConsoleClient
