#pragma once

#include <ydb/library/workload/log/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdbWorkload {

namespace NYaem {

// Distribution of the lag between the time a write happens and the `timestamp` of its rows.
// Piecewise uniform: a bucket is chosen by its weight, the lag is uniform inside the bucket.
// A negative lag is a timestamp in the future.
class TLagDistribution {
public:
    struct TBucket {
        TString Name;
        i64 FromUs = 0;    // inclusive
        i64 ToUs = 0;      // exclusive
        double Weight = 0; // percent, normalized by the sum over all buckets
    };

    // "future:0.023,0-1m:58.426,2-6h:0.621,1-3d:0.573,>30d:0"
    // A bucket is '<from>-<to><unit>' (unit s, m, h, d, w; <from> inherits the unit of <to>),
    // 'future' (up to one minute ahead of the write time) or '><from><unit>' (<from> to twice <from>).
    static TLagDistribution Parse(TStringBuf spec);
    static const TString& DefaultSpec();

    // u in [0, 1). The lag in microseconds.
    i64 SampleLagUs(double u) const;
    i64 SampleLagUs() const;

    const TVector<TBucket>& GetBuckets() const {
        return Buckets;
    }
    TString ToString() const;

private:
    TVector<TBucket> Buckets;
    TVector<double> Cumulative; // Cumulative[i] = normalized weight of buckets 0..i
};

// A quantile function given by percentile points, linear between them.
class TPercentileDistribution {
public:
    struct TPoint {
        double Percent = 0;
        double Value = 0;
    };

    // "10:7.99,50:46.51,99:124.13" (an optional 'p' before the percentile is accepted).
    // Percentiles must increase, values must not decrease. Points 0 and 100 are extrapolated
    // from the nearest segment when absent, the value at 0 is clamped at zero.
    static TPercentileDistribution Parse(TStringBuf spec);

    // u in [0, 1).
    double Sample(double u) const;
    double Sample() const;

    const TVector<TPoint>& GetPoints() const {
        return Points;
    }

private:
    TVector<TPoint> Points; // sorted by Percent, first at 0, last at 100
};

// The log table and row content of the `log` workload, written the way the yaem service does:
// writes of varying size whose rows carry a heavy-tailed timestamp lag.
class TYaemWorkloadParams final: public NLog::TLogWorkloadParams {
public:
    TYaemWorkloadParams();

    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    void Validate(const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;

    static const TString& DefaultRowsPerShardSpec();

    // Rows of one write = a sample of RowsPerShard multiplied by Shards, at least one.
    ui64 SampleRowsPerWrite() const;
    const TLagDistribution& GetLag() const {
        return Lag;
    }
    const TPercentileDistribution& GetRowsPerShard() const {
        return RowsPerShard;
    }

    ui64 Shards = 128;
    TString RowsPerShardSpec = DefaultRowsPerShardSpec();
    TString LagSpec = TLagDistribution::DefaultSpec();

private:
    TPercentileDistribution RowsPerShard;
    TLagDistribution Lag;
};

class TYaemGenerator final: public NLog::TLogGenerator {
public:
    enum class EType {
        Insert,
        Upsert,
        BulkUpsert,
        Select
    };

    explicit TYaemGenerator(const TYaemWorkloadParams* params);

    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    // The rows of one write issued at `now`: `ingested_at` is `now`, `timestamp` is `now` minus a sampled lag.
    TVector<TRow> GenerateWrite(TInstant now) const;

private:
    const TYaemWorkloadParams& YaemParams;
};

} // namespace NYaem

} // namespace NYdbWorkload
