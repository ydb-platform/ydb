#pragma once

#include "view_interface.h"
#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/datetime/base.h>


namespace NYdb::NConsoleClient {


// Time-series data point
struct TMetricPoint {
    TInstant Time;
    double Value;
};

// Chart data for a topic
struct TTopicMetrics {
    TString TopicPath;
    TVector<TMetricPoint> WriteRateBytesPerSec;
    TVector<TMetricPoint> WriteRateMessagesPerSec;
    THashMap<TString, ui64> ConsumerLags;  // consumer -> total lag
};

class ITuiApp;

class TChartsView : public ITuiView {
public:
    explicit TChartsView(ITuiApp& app);
    
    ftxui::Component Build() override;
    void Refresh() override;
    void SetTopic(const TString& topicPath);
    
private:
    ftxui::Element RenderWriteRateChart();
    ftxui::Element RenderConsumerLagGauges();
    ftxui::Element RenderPartitionDistribution();
    void UpdateMetrics();
    
private:
    ITuiApp& App_;
    TString TopicPath_;
    
    TTopicMetrics Metrics_;
    
    // Historical data (last N samples)
    static constexpr size_t MaxHistoryPoints = 60;
    
    bool Loading_ = false;
    TString ErrorMessage_;
};

} // namespace NYdb::NConsoleClient
