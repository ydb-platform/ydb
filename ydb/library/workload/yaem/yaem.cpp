#include "yaem.h"

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <algorithm>
#include <cmath>

namespace NYdbWorkload {

namespace NYaem {

namespace {

constexpr TStringBuf FutureBucketName = "future";
const TDuration FutureWindow = TDuration::Minutes(1);

i64 ParseDurationUs(TStringBuf token, TStringBuf bucket) {
    TDuration duration;
    if (token.empty() || !TDuration::TryParse(token, duration)) {
        throw yexception() << "Invalid duration '" << token << "' in lag bucket '" << bucket << "'";
    }
    return static_cast<i64>(duration.MicroSeconds());
}

// The unit letters at the end of a duration token, empty for a bare number.
TStringBuf UnitSuffix(TStringBuf token) {
    size_t pos = token.size();
    while (pos > 0 && !std::isdigit(static_cast<unsigned char>(token[pos - 1])) && token[pos - 1] != '.') {
        --pos;
    }
    return token.SubStr(pos);
}

double LargestBelowOne() {
    return std::nextafter(1.0, 0.0);
}

} // namespace

const TString& TLagDistribution::DefaultSpec() {
    static const TString spec =
        "future:0.023,"
        "0-1m:58.426,"
        "1-2m:17.679,"
        "2-3m:15.129,"
        "3-4m:4.482,"
        "4-5m:1.035,"
        "5-10m:1.162,"
        "10-20m:0.284,"
        "20-30m:0.025,"
        "30-60m:0.013,"
        "60-90m:0.014,"
        "90-120m:0.01,"
        "2-6h:0.621,"
        "6-12h:0.119,"
        "12-24h:0.204,"
        "1-3d:0.573,"
        "3-7d:0.201,"
        "7-30d:0,"
        ">30d:0";
    return spec;
}

TLagDistribution TLagDistribution::Parse(TStringBuf spec) {
    TLagDistribution result;
    double total = 0;
    for (const auto& token : StringSplitter(spec).Split(',').SkipEmpty().ToList<TString>()) {
        const TStringBuf item = StripString(TStringBuf(token));
        TStringBuf name, weight;
        if (!item.TryRSplit(':', name, weight)) {
            throw yexception() << "Lag bucket '" << item << "' must look like '<from>-<to><unit>:<percent>'";
        }
        name = StripString(name);
        weight = StripString(weight);
        TBucket bucket;
        bucket.Name = TString(name);
        if (!TryFromString(weight, bucket.Weight) || !std::isfinite(bucket.Weight) || bucket.Weight < 0) {
            throw yexception() << "Invalid weight '" << weight << "' in lag bucket '" << item << "': expected a non-negative percent";
        }
        if (name == FutureBucketName) {
            bucket.FromUs = -static_cast<i64>(FutureWindow.MicroSeconds());
            bucket.ToUs = 0;
        } else if (name.StartsWith('>')) {
            bucket.FromUs = ParseDurationUs(name.SubStr(1), item);
            if (bucket.FromUs <= 0) {
                throw yexception() << "Lag bucket '" << item << "' must start above zero";
            }
            bucket.ToUs = 2 * bucket.FromUs;
        } else {
            TStringBuf from, to;
            if (!name.TrySplit('-', from, to)) {
                throw yexception() << "Lag bucket '" << item << "' must look like '<from>-<to><unit>:<percent>'";
            }
            TString fromWithUnit(from);
            if (UnitSuffix(from).empty()) {
                fromWithUnit += UnitSuffix(to);
            }
            bucket.FromUs = ParseDurationUs(fromWithUnit, item);
            bucket.ToUs = ParseDurationUs(to, item);
            if (bucket.FromUs >= bucket.ToUs) {
                throw yexception() << "Lag bucket '" << item << "' must end after it starts";
            }
        }
        total += bucket.Weight;
        result.Buckets.push_back(std::move(bucket));
    }
    if (result.Buckets.empty() || total <= 0) {
        throw yexception() << "Lag distribution '" << spec << "' must contain a bucket with a positive weight";
    }
    double accumulated = 0;
    for (const auto& bucket : result.Buckets) {
        accumulated += bucket.Weight;
        result.Cumulative.push_back(accumulated / total);
    }
    result.Cumulative.back() = 1.0;
    return result;
}

i64 TLagDistribution::SampleLagUs(double u) const {
    u = std::clamp(u, 0.0, LargestBelowOne());
    // The first bucket whose cumulative weight exceeds u; zero-weight buckets are skipped.
    size_t index = std::upper_bound(Cumulative.begin(), Cumulative.end(), u) - Cumulative.begin();
    index = std::min(index, Cumulative.size() - 1);
    const double lower = index ? Cumulative[index - 1] : 0.0;
    const double width = Cumulative[index] - lower;
    const double fraction = width > 0 ? std::min((u - lower) / width, LargestBelowOne()) : 0.0;
    const auto& bucket = Buckets[index];
    return bucket.FromUs + static_cast<i64>(fraction * static_cast<double>(bucket.ToUs - bucket.FromUs));
}

i64 TLagDistribution::SampleLagUs() const {
    return SampleLagUs(RandomNumber<double>());
}

TString TLagDistribution::ToString() const {
    TStringBuilder result;
    for (const auto& bucket : Buckets) {
        if (!result.empty()) {
            result << ',';
        }
        result << bucket.Name << ':' << bucket.Weight;
    }
    return result;
}

TPercentileDistribution TPercentileDistribution::Parse(TStringBuf spec) {
    TVector<TPoint> points;
    for (const auto& token : StringSplitter(spec).Split(',').SkipEmpty().ToList<TString>()) {
        const TStringBuf item = StripString(TStringBuf(token));
        TStringBuf percent, value;
        if (!item.TrySplit(':', percent, value)) {
            throw yexception() << "Percentile point '" << item << "' must look like '<percentile>:<value>'";
        }
        percent = StripString(percent);
        percent.SkipPrefix("p");
        value = StripString(value);
        TPoint point;
        if (!TryFromString(percent, point.Percent) || !(point.Percent >= 0 && point.Percent <= 100)) {
            throw yexception() << "Invalid percentile '" << percent << "' in '" << item << "': expected a number from 0 to 100";
        }
        if (!TryFromString(value, point.Value) || !std::isfinite(point.Value) || point.Value < 0) {
            throw yexception() << "Invalid value '" << value << "' in '" << item << "': expected a non-negative number";
        }
        if (!points.empty() && (point.Percent <= points.back().Percent || point.Value < points.back().Value)) {
            throw yexception() << "Percentile points must have increasing percentiles and non-decreasing values, got '" << item
                << "' after " << points.back().Percent << ':' << points.back().Value;
        }
        points.push_back(point);
    }
    if (points.empty()) {
        throw yexception() << "Percentile distribution '" << spec << "' is empty";
    }
    if (points.front().Percent > 0) {
        const TPoint a = points[0];
        double value = a.Value;
        if (points.size() > 1) {
            const TPoint b = points[1];
            value = std::max(0.0, a.Value - (b.Value - a.Value) / (b.Percent - a.Percent) * a.Percent);
        }
        points.insert(points.begin(), TPoint{0, value});
    }
    if (points.back().Percent < 100) {
        const TPoint b = points.back();
        double value = b.Value;
        if (points.size() > 1) {
            const TPoint a = points[points.size() - 2];
            value = b.Value + (b.Value - a.Value) / (b.Percent - a.Percent) * (100 - b.Percent);
        }
        points.push_back(TPoint{100, value});
    }
    TPercentileDistribution result;
    result.Points = std::move(points);
    return result;
}

double TPercentileDistribution::Sample(double u) const {
    const double percent = std::clamp(u, 0.0, 1.0) * 100;
    const auto next = std::upper_bound(Points.begin(), Points.end(), percent, [](double p, const TPoint& point) {
        return p < point.Percent;
    });
    if (next == Points.end()) {
        return Points.back().Value;
    }
    if (next == Points.begin()) {
        return Points.front().Value;
    }
    const auto& a = *(next - 1);
    const auto& b = *next;
    return a.Value + (b.Value - a.Value) * (percent - a.Percent) / (b.Percent - a.Percent);
}

double TPercentileDistribution::Sample() const {
    return Sample(RandomNumber<double>());
}

const TString& TYaemWorkloadParams::DefaultRowsPerShardSpec() {
    static const TString spec = "10:7.99,40:41.28,50:46.51,70:56.96,90:89.30,95:108.65,99:124.13";
    return spec;
}

TYaemWorkloadParams::TYaemWorkloadParams()
    : RowsPerShard(TPercentileDistribution::Parse(RowsPerShardSpec))
    , Lag(TLagDistribution::Parse(LagSpec))
{
    TableName = "yaem";
}

void TYaemWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    if (commandType != ECommandType::Run) {
        TLogWorkloadParams::ConfigureOpts(opts, commandType, workloadType);
        return;
    }
    ConfigureOptsPath(opts);
    switch (static_cast<TYaemGenerator::EType>(workloadType)) {
        case TYaemGenerator::EType::Insert:
        case TYaemGenerator::EType::Upsert:
        case TYaemGenerator::EType::BulkUpsert:
            ConfigureOptsColumns(opts);
            opts.AddLongOption("null-percent", "Percent of nulls in generated data")
                .DefaultValue(NullPercent).StoreResult(&NullPercent);
            opts.AddLongOption("shards", "Number of table shards one write is spread over. "
                "The number of rows in a write is a sample of --rows-per-shard multiplied by this value.")
                .DefaultValue(Shards).StoreResult(&Shards);
            opts.AddLongOption("rows-per-shard", "Distribution of the number of rows one write delivers to one shard: "
                "comma separated '<percentile>:<rows>' points. The quantile function is linear between the points; "
                "the points 0 and 100 are extrapolated when absent.")
                .DefaultValue(RowsPerShardSpec).StoreResult(&RowsPerShardSpec);
            opts.AddLongOption("lag-distribution", "Distribution of the lag between the write time and the timestamp of a row: "
                "comma separated '<bucket>:<percent>' pairs. A bucket is '<from>-<to><unit>' with unit s, m, h, d or w, "
                "'future' (up to one minute ahead of the write time) or '><from><unit>' (<from> to twice <from>). "
                "The lag is uniform inside a bucket; the percents are normalized by their sum.")
                .DefaultValue(LagSpec).StoreResult(&LagSpec);
            break;
        case TYaemGenerator::EType::Select:
            break;
    }
}

void TYaemWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
    if (commandType != ECommandType::Run) {
        return;
    }
    switch (static_cast<TYaemGenerator::EType>(workloadType)) {
        case TYaemGenerator::EType::Insert:
        case TYaemGenerator::EType::Upsert:
        case TYaemGenerator::EType::BulkUpsert:
            if (!Shards) {
                throw yexception() << "The `shards` option must be positive";
            }
            try {
                RowsPerShard = TPercentileDistribution::Parse(RowsPerShardSpec);
            } catch (const yexception& e) {
                throw yexception() << "Invalid `rows-per-shard` option: " << e.what();
            }
            try {
                Lag = TLagDistribution::Parse(LagSpec);
            } catch (const yexception& e) {
                throw yexception() << "Invalid `lag-distribution` option: " << e.what();
            }
            break;
        case TYaemGenerator::EType::Select:
            break;
    }
}

THolder<IWorkloadQueryGenerator> TYaemWorkloadParams::CreateGenerator() const {
    return MakeHolder<TYaemGenerator>(this);
}

TString TYaemWorkloadParams::GetWorkloadName() const {
    return "Yaem";
}

TWorkloadDataInitializer::TList TYaemWorkloadParams::CreateDataInitializers() const {
    return {};
}

ui64 TYaemWorkloadParams::SampleRowsPerWrite() const {
    const double rows = RowsPerShard.Sample() * static_cast<double>(Shards);
    return std::max<ui64>(1, static_cast<ui64>(std::llround(rows)));
}

namespace {

class TYaemRowGenerator final: public NLog::TRandomLogGenerator {
public:
    TYaemRowGenerator(const TYaemWorkloadParams& params, TInstant now)
        : TRandomLogGenerator(params)
        , Lag(params.GetLag())
        , Now(now)
    {}

protected:
    TInstant GenerateTimestamp() const override {
        const i64 lagUs = Lag.SampleLagUs();
        return lagUs >= 0 ? Now - TDuration::MicroSeconds(lagUs) : Now + TDuration::MicroSeconds(-lagUs);
    }

    TInstant GenerateIngestedAt() const override {
        return Now;
    }

private:
    const TLagDistribution& Lag;
    const TInstant Now;
};

} // namespace

TYaemGenerator::TYaemGenerator(const TYaemWorkloadParams* params)
    : TLogGenerator(params)
    , YaemParams(*params)
{}

TVector<TYaemGenerator::TRow> TYaemGenerator::GenerateWrite(TInstant now) const {
    return TYaemRowGenerator(YaemParams, now).GenerateRandomRows(YaemParams.SampleRowsPerWrite());
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TYaemGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::Insert), "insert", "Insert one write of rows: the row count follows the rows-per-shard distribution, the timestamps the lag distribution");
    result.emplace_back(static_cast<int>(EType::Upsert), "upsert", "Upsert one write of rows: the row count follows the rows-per-shard distribution, the timestamps the lag distribution");
    result.emplace_back(static_cast<int>(EType::BulkUpsert), "bulk-upsert", "Bulk upsert one write of rows: the row count follows the rows-per-shard distribution, the timestamps the lag distribution");
    result.emplace_back(static_cast<int>(EType::Select), "select", "Select some agregated queries");
    return result;
}

TQueryInfoList TYaemGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::Insert:
            return Insert(GenerateWrite(TInstant::Now()));
        case EType::Upsert:
            return Upsert(GenerateWrite(TInstant::Now()));
        case EType::BulkUpsert:
            return BulkUpsert(GenerateWrite(TInstant::Now()));
        case EType::Select:
            return Select();
    }
    return {};
}

} // namespace NYaem

} // namespace NYdbWorkload
