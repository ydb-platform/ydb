#pragma once
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/request/config.h>

#include <util/datetime/base.h>

namespace NKikimr::NBackgroundTasks {
class TConfig {
private:
    YDB_READONLY_DEF(NMetadata::NRequest::TConfig, RequestConfig);
    YDB_READONLY(TDuration, PullPeriod, TDuration::Seconds(10));
    YDB_READONLY(TDuration, PingPeriod, TDuration::Seconds(2));
    YDB_READONLY(TDuration, PingCheckPeriod, TDuration::Seconds(20));
    YDB_READONLY(ui32, MaxInFlight, 1);
    YDB_READONLY(TString, InternalTablePath, ".bg_tasks/tasks");
    YDB_READONLY_FLAG(Enabled, true);
public:
    bool DeserializeFromProto(const NKikimrConfig::TBackgroundTasksConfig& config);
    TString GetTablePath() const;
};
}
