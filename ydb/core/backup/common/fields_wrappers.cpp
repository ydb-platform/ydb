#include "fields_wrappers.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <util/generic/yexception.h>

namespace NKikimr::NBackup::NFieldsWrappers {

template <>
const NKikimrSchemeOp::TS3Settings& GetSettings(const NKikimrSchemeOp::TBackupTask& task) {
    Y_ENSURE(task.HasS3Settings());
    return task.GetS3Settings();
}

template <>
const NKikimrSchemeOp::TFSSettings& GetSettings(const NKikimrSchemeOp::TBackupTask& task) {
    Y_ENSURE(task.HasFSSettings());
    return task.GetFSSettings();
}

template <>
const TString& GetCommonDestination(const Ydb::Export::ExportToS3Settings& settings) {
    return settings.destination_prefix();
}

template <>
const TString& GetCommonDestination(const Ydb::Export::ExportToFsSettings& settings) {
    return settings.base_path();
}

template <>
TString& MutableItemDestination(Ydb::Export::ExportToS3Settings::Item& item) {
    return *item.mutable_destination_prefix();
}

template <>
TString& MutableItemDestination(Ydb::Export::ExportToFsSettings::Item& item) {
    return *item.mutable_destination_path();
}

template <>
const TString& GetItemDestination(const Ydb::Export::ExportToS3Settings::Item& item) {
    return item.destination_prefix();
}

template <>
const TString& GetItemDestination(const Ydb::Export::ExportToFsSettings::Item& item) {
    return item.destination_path();
}

} // NKikimr::NBackup
