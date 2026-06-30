#pragma once

#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>


namespace NKikimr::NKqp {

struct TTaskResourceEstimation {
    ui64 TaskId = 0;
    ui32 ChannelBuffersCount = 0;
    ui64 ChannelBufferMemoryLimit = 0;
    ui64 MkqlProgramMemoryLimit = 0;
    ui64 TotalMemoryLimit = 0;
    bool HeavyProgram = false;

    TString ToString() const {
        return TStringBuilder() << "TaskResourceEstimation{"
            << " TaskId: " << TaskId
            << ", ChannelBuffersCount: " << ChannelBuffersCount
            << ", ChannelBufferMemoryLimit: " << ChannelBufferMemoryLimit
            << ", MkqlProgramMemoryLimit: " << MkqlProgramMemoryLimit
            << ", TotalMemoryLimit: " << TotalMemoryLimit
            << " }";
    }
};

TTaskResourceEstimation BuildInitialTaskResources(const NYql::NDqProto::TDqTask& task);

// Memory-limit formula inputs (a subset of TTableServiceConfig::TResourceManager). The resource manager passes its
// live atomic values; other callers (e.g. the executer's task placement in TMaxTasksGraph) pass the config getters.
struct TTaskResourceEstimationParams {
    ui64 ChannelBufferSize = 0;
    ui64 MinChannelBufferSize = 0;
    ui64 MaxTotalChannelBuffersSize = 0;
    ui64 MkqlHeavyProgramMemoryLimit = 0;
    ui64 MkqlLightProgramMemoryLimit = 0;
};

// Pure memory estimation: fills ChannelBufferMemoryLimit / MkqlProgramMemoryLimit / TotalMemoryLimit from
// ret.ChannelBuffersCount, ret.HeavyProgram and the per-query task count. Single source of truth shared by the
// resource manager and the executer's task placement.
void EstimateTaskResources(TTaskResourceEstimation& ret, const TTaskResourceEstimationParams& params, ui32 tasksCount);


} // namespace NKikimr::NKqp
