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
    TCommonLatencyHistBounds() = default;

private:
    TCommonLatencyHistBounds(TVector<double> unknown, TVector<double> rot, TVector<double> ssd, TVector<double> nvme)
        : Unknown(std::move(unknown))
        , Rot(std::move(rot))
        , Ssd(std::move(ssd))
        , Nvme(std::move(nvme))
    {}

public:
    static TCommonLatencyHistBounds Create(const NKikimrConfig::TCommonLatencyHistBounds& bounds) {
        TVector<double> unknown = {
            8, 16, 32, 64, 128, 256, 512,   // ms
            1'024, 4'096,                                       // s
            65'536                                                  // minutes
        };

        TVector<double> rot;
        TVector<double> ssd;
        TVector<double> nvme;

        if (bounds.RotSize() > 0) {
            rot = FromArray(bounds.GetRot());
        } else {
            rot = unknown;
        }

        if (bounds.SsdSize() > 0) {
            ssd = FromArray(bounds.GetSsd());
        } else {
            ssd = {
                0.5,                                        // us
                1, 2, 8, 32, 128, 512,  // ms
                1'024, 4'096,                           // s
                65'536                                      // minutes
            };
        }
        
        if (bounds.NvmeSize() > 0) {
            nvme = FromArray(bounds.GetNvme());
        } else {
            nvme = {
                0.25, 0.5,                              // us
                1, 2, 4, 8, 32, 128,    // ms
                1'024,                                      // s
                65'536                                      // minutes
            };
        }

        return TCommonLatencyHistBounds(std::move(unknown), std::move(rot), std::move(ssd), std::move(nvme));
    }
};

class TMetricsConfig {
public:
    TMetricsConfig() = default;

    TMetricsConfig(const NKikimrConfig::TMetricsConfig& config )
    : CommonLatencyHistBounds(TCommonLatencyHistBounds::Create(config.GetCommonLatencyHistBounds()))
    { }

    TCommonLatencyHistBounds GetCommonLatencyHistBounds() const {
        return CommonLatencyHistBounds;
    }
private:
    TCommonLatencyHistBounds CommonLatencyHistBounds;
};

} // NKikimr
