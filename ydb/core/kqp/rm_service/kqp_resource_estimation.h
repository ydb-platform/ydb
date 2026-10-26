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

struct TTaskResourceEstimationParams {
    ui64 ChannelBufferSize = 0;
    ui64 MinChannelBufferSize = 0;
    ui64 MaxTotalChannelBuffersSize = 0;
    ui64 MkqlHeavyProgramMemoryLimit = 0;
    ui64 MkqlLightProgramMemoryLimit = 0;
};

void EstimateTaskResources(TTaskResourceEstimation& ret, const TTaskResourceEstimationParams& params, ui32 tasksCount);

// The program which gets the heavy memory limit instead of the light one
bool IsHeavyProgram(const NYql::NDqProto::TDqTask& task);

// The memory a task is expected to take beyond its initial limit - the elastic part (E) of its memory demand.
// TODO: the first version - no statistics and data volumes, only the flags of the program, like the initial limits:
//       a heavy program is expected to grow up to the heavy limit, a light one - not to grow at all.
ui64 EstimateTaskElasticMemory(const NYql::NDqProto::TDqTask& task, ui64 initialLimit, ui64 lightLimit, ui64 heavyLimit);

} // namespace NKikimr::NKqp
