#pragma once 
 
namespace NMonitoring { 
 
    struct ILabel; 
    struct ILabels; 
 
    class ICounter; 
    class IGauge; 
    class IHistogram; 
    class IIntGauge; 
    class ILazyCounter; 
    class ILazyGauge; 
    class ILazyIntGauge; 
    class ILazyRate; 
    class IMetric; 
    class IRate; 
    class TCounter; 
    class TGauge; 
    class THistogram; 
    class TIntGauge; 
    class TLazyCounter; 
    class TLazyGauge; 
    class TLazyIntGauge; 
    class TLazyRate; 
    class TRate; 
 
    class IMetricSupplier; 
    class IMetricFactory; 
    class IMetricConsumer; 
 
    class IMetricRegistry; 
    class TMetricRegistry; 
 
    class IHistogramCollector; 
    class IHistogramSnapshot; 
 
    class IExpMovingAverage; 
 
} // namespace NMonitoring 
