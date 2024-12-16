#pragma once

#include <ydb/core/protos/config.pb.h>
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


} // namespace NKikimr::NKqp
