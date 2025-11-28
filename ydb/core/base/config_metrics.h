#pragma once

#include <ydb/core/protos/config_metrics.pb.h>

#include <util/generic/vector.h>

namespace NKikimr {

inline TVector<double> FromArray(const google::protobuf::RepeatedField<double>& array) {
    TVector<double> result;
    result.reserve(array.size());
    for (const auto& v : array) {
        result.push_back(v);
    }
    return result;
}

struct TCommonLatencyHistBounds {
    TVector<double> Unknown;
    TVector<double> Rot;
    TVector<double> Ssd;
    TVector<double> Nvme;

public:
    TCommonLatencyHistBounds() {
        InitializeWithDefaults();
    }

private:
    void InitializeWithDefaults() {
        Unknown = {
            8, 16, 32, 64, 128, 256, 512,   // ms
            1'024, 4'096,                   // s
            65'536                          // minutes
        };
        Rot = Unknown;
        Ssd = {
            0.5,                    // us
            1, 2, 8, 32, 128, 512,  // ms
            1'024, 4'096,           // s
            65'536                  // minutes
        };
        Nvme = {
            0.25, 0.5,              // us
            1, 2, 4, 8, 32, 128,    // ms
            1'024,                  // s
            65'536                  // minutes
        };
    }

public:
    void InitializeFromProto(const NKikimrConfig::TCommonLatencyHistBounds& bounds) {
        if (bounds.RotSize() > 0) {
            Rot = FromArray(bounds.GetRot());
        }
        if (bounds.SsdSize() > 0) {
            Ssd = FromArray(bounds.GetSsd());
        }
        if (bounds.NvmeSize() > 0) {
            Nvme = FromArray(bounds.GetNvme());
        }
    }
};

class TMetricsConfig {
public:
    TMetricsConfig()
    : CommonLatencyHistBounds()
    {}

    void InitializeFromProto(const NKikimrConfig::TMetricsConfig& config) {
        if (config.HasCommonLatencyHistBounds()) {
            CommonLatencyHistBounds.InitializeFromProto(config.GetCommonLatencyHistBounds());
        }
    }

    TCommonLatencyHistBounds GetCommonLatencyHistBounds() const {
        return CommonLatencyHistBounds;
    }
private:
    TCommonLatencyHistBounds CommonLatencyHistBounds;
};

} // NKikimr
