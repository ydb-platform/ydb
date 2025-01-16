#pragma once

#include "defs.h"
#include <ydb/core/protos/memory_stats.pb.h>

namespace NKikimr::NMemory {

namespace {

#define ADD_MEMORY(name) \
    inline void Add##name##To(NKikimrMemory::TMemoryStats& left, const NKikimrMemory::TMemoryStats& right) { \
        left.Set##name(left.Get##name() + right.Get##name()); \
    }

ADD_MEMORY(ExternalConsumption)
ADD_MEMORY(AnonRss)

ADD_MEMORY(MemTotal)
ADD_MEMORY(MemAvailable)
ADD_MEMORY(HardLimit)
ADD_MEMORY(SoftLimit)
ADD_MEMORY(TargetUtilization)

}

class TMemoryStatsAggregator {
    using TMemoryStats = NKikimrMemory::TMemoryStats;

    TMemoryStats Aggregated;    
    TMap<TString, TMemoryStats> PerHost;

public:
    void Add(const TMemoryStats& memoryStats, TString host) {
        AddMemoryStats(Aggregated, memoryStats);

        if (!memoryStats.HasExternalConsumption()) {
            return;
        }

        // a special case when there are multiple YDB nodes share one host memory should be handled as follows:

        auto& hostMemoryStats = PerHost[host];
        // subtract all ydb consumption seen before
        hostMemoryStats.SetExternalConsumption(memoryStats.GetExternalConsumption() - hostMemoryStats.GetAnonRss());
        AddAnonRssTo(hostMemoryStats, memoryStats);

        hostMemoryStats.SetMemTotal(memoryStats.GetMemTotal());
        hostMemoryStats.SetMemAvailable(memoryStats.GetMemAvailable());
        hostMemoryStats.SetHardLimit(memoryStats.GetHardLimit());
        hostMemoryStats.SetSoftLimit(memoryStats.GetSoftLimit());
        hostMemoryStats.SetTargetUtilization(memoryStats.GetTargetUtilization());
    }

    TMemoryStats Aggregate() {
        if (!PerHost) {
            return Aggregated;
        }

        // a special case when there are multiple YDB nodes share one host memory should be handled as follows:

        Aggregated.SetExternalConsumption(0);
        Aggregated.SetMemTotal(0);
        Aggregated.SetMemAvailable(0);
        Aggregated.SetHardLimit(0);
        Aggregated.SetSoftLimit(0);
        Aggregated.SetTargetUtilization(0);
        for (const auto& host_ : PerHost) {
            auto& host = host_.second;
            AddExternalConsumptionTo(Aggregated, host);
            AddMemTotalTo(Aggregated, host);
            AddMemAvailableTo(Aggregated, host);
            AddHardLimitTo(Aggregated, host);
            AddSoftLimitTo(Aggregated, host);
            AddTargetUtilizationTo(Aggregated, host);
        }

        return Aggregated;
    }

private:
    static void AddMemoryStats(TMemoryStats& left, const TMemoryStats& right) {
        using namespace ::google::protobuf;
        const Descriptor& descriptor = *TMemoryStats::GetDescriptor();
        const Reflection& reflection = *TMemoryStats::GetReflection();
        int fieldCount = descriptor.field_count();
        for (int index = 0; index < fieldCount; ++index) {
            const FieldDescriptor* field = descriptor.field(index);
            if (reflection.HasField(right, field)) {
                FieldDescriptor::CppType type = field->cpp_type();
                switch (type) {
                    case FieldDescriptor::CPPTYPE_UINT64:
                        reflection.SetUInt64(&left, field,
                            reflection.GetUInt64(left, field) + reflection.GetUInt64(right, field));
                        break;
                    default:
                        Y_DEBUG_ABORT_UNLESS("Unhandled field type");
                }
            }
        }
    }
};

}
