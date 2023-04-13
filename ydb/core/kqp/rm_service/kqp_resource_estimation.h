#pragma once

#include <ydb/core/protos/config.pb.h>
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

TTaskResourceEstimation EstimateTaskResources(const NYql::NDqProto::TDqTask& task,
    const NKikimrConfig::TTableServiceConfig::TResourceManager& config, const ui32 tasksCount);

void EstimateTaskResources(const NKikimrConfig::TTableServiceConfig::TResourceManager& config, TTaskResourceEstimation& result, const ui32 tasksCount);

} // namespace NKikimr::NKqp
