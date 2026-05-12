#include <ydb/public/api/protos/draft/ydb_backup.pb.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, Ydb::Cms::GetDatabaseStatusResult::State, stream, value) {
    stream << Ydb::Cms::GetDatabaseStatusResult::State_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::StatusIds::StatusCode, stream, value) {
    stream << Ydb::StatusIds::StatusCode_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Monitoring::SelfCheck::Result, stream, value) {
    stream << Ydb::Monitoring::SelfCheck_Result_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Monitoring::StatusFlag::Status, stream, value) {
    stream << Ydb::Monitoring::StatusFlag_Status_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Export::ExportProgress::Progress, stream, value) {
    stream << Ydb::Export::ExportProgress_Progress_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Import::ImportProgress::Progress, stream, value) {
    stream << Ydb::Import::ImportProgress_Progress_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Table::VectorIndexSettings::Metric, stream, value) {
    stream << Ydb::Table::VectorIndexSettings::Metric_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Table::VectorIndexSettings::VectorType, stream, value) {
    stream << Ydb::Table::VectorIndexSettings::VectorType_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Table::IndexBuildState_State, stream, value) {
    stream << IndexBuildState_State_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Maintenance::ActionState::ActionStatus, stream, value) {
    stream << Ydb::Maintenance::ActionState::ActionStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Maintenance::ActionState::ActionReason, stream, value) {
    stream << Ydb::Maintenance::ActionState::ActionReason_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Backup::BackupProgress::Progress, stream, value) {
    stream << Ydb::Backup::BackupProgress::Progress_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Table::CompactState_State, stream, value) {
    stream << CompactState_State_Name(value);
}
