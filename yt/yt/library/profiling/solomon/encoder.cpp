#include "encoder.h"

#include <util/generic/overloaded.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <std::derived_from<THistogramSnapshot> THistogram>
THistogram BuildHistogram(NMonitoring::IHistogramSnapshotPtr snapshot)
{
    THistogram hist;
    hist.Values.reserve(snapshot->Count());
    hist.Bounds.reserve(snapshot->Count());

    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        hist.Values.push_back(snapshot->Value(i));

        if (snapshot->UpperBound(i) != NMonitoring::HISTOGRAM_INF_BOUND) {
            hist.Bounds.push_back(snapshot->UpperBound(i));
        }
    }

    return hist;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSensorEncoder::TSensorEncoder(std::string sensorNameLabel, std::string sensorNamePrefix)
    : SensorNameLabel_(std::move(sensorNameLabel))
    , SensorNamePrefix_(std::move(sensorNamePrefix))
{ }

void TSensorEncoder::Close()
{ }

void TSensorEncoder::OnStreamBegin()
{ }

void TSensorEncoder::OnStreamEnd()
{ }

void TSensorEncoder::OnCommonTime(TInstant /*time*/)
{ }

void TSensorEncoder::OnMetricBegin(NMonitoring::EMetricType type)
{
    SensorContext_ = TSensorContext{ .Type = type, .TagIds = CommonTagIds_ };
}

void TSensorEncoder::OnMetricEnd()
{
    SensorContext_.reset();
}

void TSensorEncoder::OnLabelsBegin()
{ }

void TSensorEncoder::OnLabelsEnd()
{
    if (!SensorContext_) {
        return;
    }

    if (SensorContext_->Name.empty()) {
        THROW_ERROR_EXCEPTION("Failed to find label with sensor name")
            << TErrorAttribute("label", SensorNameLabel_);
    }

    auto* cube = Cubes_.FindPtr(SensorContext_->Name);
    if (cube == nullptr) {
        switch (SensorContext_->Type) {
            case NMonitoring::EMetricType::GAUGE:
                Cubes_.emplace(SensorContext_->Name, TGaugeCube{1, 0});
                break;
            case NMonitoring::EMetricType::RATE:
                Cubes_.emplace(SensorContext_->Name, TCounterCube{1, 0});
                break;
            case NMonitoring::EMetricType::HIST:
                Cubes_.emplace(SensorContext_->Name, TGaugeHistogramCube{1, 0});
                break;
            case NMonitoring::EMetricType::HIST_RATE:
                Cubes_.emplace(SensorContext_->Name, TRateHistogramCube{1, 0});
                break;
            case NMonitoring::EMetricType::DSUMMARY:
                Cubes_.emplace(SensorContext_->Name, TSummaryCube{1, 0});
                break;
            case NMonitoring::EMetricType::LOGHIST:
            case NMonitoring::EMetricType::IGAUGE:
            case NMonitoring::EMetricType::COUNTER:
            case NMonitoring::EMetricType::UNKNOWN:
                THROW_ERROR_EXCEPTION("Unsupported metric type %Qv", ToString(SensorContext_->Type));
        }
        cube = Cubes_.FindPtr(SensorContext_->Name);
    }

    SensorContext_->Cube = cube;
    std::visit(
        [this] (auto&& cube) {
            cube.Add(SensorContext_->TagIds);
        },
        *SensorContext_->Cube);
}

void TSensorEncoder::OnLabel(TStringBuf name, TStringBuf value)
{
    if (name == SensorNameLabel_) {
        if (!SensorContext_) {
            THROW_ERROR_EXCEPTION("Found label with sensor name among common labels")
                << TErrorAttribute("label", SensorNameLabel_);
        }
        if (!SensorContext_->Name.empty()) {
            THROW_ERROR_EXCEPTION("Found label with sensor name multiple times")
                << TErrorAttribute("label", SensorNameLabel_);
        }
        SensorContext_->Name = SensorNamePrefix_ + std::string{value};
        return;
    }

    auto tagId = TagRegistry_.Encode(TTag{std::string{name}, std::string{value}});
    if (SensorContext_) {
        SensorContext_->TagIds.push_back(tagId);
    } else {
        CommonTagIds_.push_back(tagId);
    }
}

void TSensorEncoder::OnLabel(ui32 /*name*/, ui32 /*value*/)
{
    YT_UNIMPLEMENTED();
}

std::pair<ui32, ui32> TSensorEncoder::PrepareLabel(TStringBuf /*name*/, TStringBuf /*value*/)
{
    YT_UNIMPLEMENTED();
}

void TSensorEncoder::OnDouble(TInstant /*time*/, double value)
{
    auto& cube = std::get<TGaugeCube>(*SensorContext_->Cube);
    cube.Update(SensorContext_->TagIds, value);
}

void TSensorEncoder::OnInt64(TInstant /*time*/, i64 value)
{
    auto& cube = std::get<TCounterCube>(*SensorContext_->Cube);
    cube.Update(SensorContext_->TagIds, value);
}

void TSensorEncoder::OnUint64(TInstant time, ui64 value)
{
    OnInt64(time, SafeIntegerCast<i64>(value));
}

void TSensorEncoder::OnHistogram(TInstant /*time*/, NMonitoring::IHistogramSnapshotPtr snapshot)
{
    std::visit(TOverloaded{
        [&] (TGaugeHistogramCube& cube) {
            cube.Update(SensorContext_->TagIds, BuildHistogram<TGaugeHistogramSnapshot>(snapshot));
        },
        [&] (TRateHistogramCube& cube) {
            cube.Update(SensorContext_->TagIds, BuildHistogram<TRateHistogramSnapshot>(snapshot));
        },
        [] (auto&& cube) {
            THROW_ERROR_EXCEPTION("Unexpected cube type for histogram snapshot %Qv", TypeName<decltype(cube)>());
        },
    }, *SensorContext_->Cube);
}

void TSensorEncoder::OnLogHistogram(TInstant /*time*/, NMonitoring::TLogHistogramSnapshotPtr /*snapshot*/)
{
    THROW_ERROR_EXCEPTION("OnLogHistogram method is not implemented");
}

void TSensorEncoder::OnSummaryDouble(TInstant /*time*/, NMonitoring::ISummaryDoubleSnapshotPtr snapshot)
{
    auto& cube = std::get<TSummaryCube>(*SensorContext_->Cube);

    TSummarySnapshot summary(
        snapshot->GetSum(),
        snapshot->GetMin(),
        snapshot->GetMax(),
        snapshot->GetLast(),
        snapshot->GetCount());
    cube.Update(SensorContext_->TagIds, std::move(summary));
}

NProto::TSensorDump TSensorEncoder::BuildSensorDump()
{
    NProto::TSensorDump sensorDump;

    TagRegistry_.DumpTags(&sensorDump);

    for (const auto& [name, cube] : Cubes_) {
        auto* cubeProto = sensorDump.add_cubes();

        cubeProto->set_name(name);
        std::visit(
            [&cubeProto] (auto&& cube) {
                cube.DumpCube(cubeProto);
            },
            cube);
    }

    return sensorDump;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
