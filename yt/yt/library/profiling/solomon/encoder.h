#pragma once

#include "cube.h"
#include "tag_registry.h"

#include <library/cpp/monlib/encode/encoder.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

// Encodes incoming metrics into `NProto::TSensorDump`.
class TSensorEncoder
    : public NMonitoring::IMetricEncoder
{
public:
    explicit TSensorEncoder(std::string sensorNameLabel, std::string sensorNamePrefix = "");
    void Close() override;

    void OnStreamBegin() override;
    void OnStreamEnd() override;

    void OnCommonTime(TInstant time) override;

    void OnMetricBegin(NMonitoring::EMetricType type) override;
    void OnMetricEnd() override;

    void OnLabelsBegin() override;
    void OnLabelsEnd() override;
    void OnLabel(TStringBuf name, TStringBuf value) override;
    void OnLabel(ui32 name, ui32 value) override;
    std::pair<ui32, ui32> PrepareLabel(TStringBuf name, TStringBuf value) override;

    void OnDouble(TInstant time, double value) override;
    void OnInt64(TInstant time, i64 value) override;
    void OnUint64(TInstant time, ui64 value) override;

    void OnHistogram(TInstant time, NMonitoring::IHistogramSnapshotPtr snapshot) override;
    void OnLogHistogram(TInstant time, NMonitoring::TLogHistogramSnapshotPtr snapshot) override;
    void OnSummaryDouble(TInstant time, NMonitoring::ISummaryDoubleSnapshotPtr snapshot) override;

    NProto::TSensorDump BuildSensorDump();

private:
    const std::string SensorNameLabel_;
    const std::string SensorNamePrefix_;

    TTagRegistry TagRegistry_;
    TTagIdList CommonTagIds_;

    using TGenericCube = std::variant<TGaugeCube, TCounterCube, TGaugeHistogramCube, TRateHistogramCube, TSummaryCube>;
    THashMap<std::string, TGenericCube> Cubes_;

    struct TSensorContext
    {
        std::string Name;
        NMonitoring::EMetricType Type = NMonitoring::EMetricType::UNKNOWN;
        TTagIdList TagIds;
        TGenericCube* Cube = nullptr;
    };
    std::optional<TSensorContext> SensorContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
