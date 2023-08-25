#include "accessor.h"

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

namespace NYdb {

const Ydb::Type& TProtoAccessor::GetProto(const TType& type) {
    return type.GetProto();
}

const Ydb::Value& TProtoAccessor::GetProto(const TValue& value) {
    return value.GetProto();
}

// exports & imports
template <typename TProtoSettings>
typename TProtoSettings::Scheme TProtoAccessor::GetProto(ES3Scheme value) {
    switch (value) {
    case ES3Scheme::HTTP:
        return TProtoSettings::HTTP;
    case ES3Scheme::HTTPS:
        return TProtoSettings::HTTPS;
    }
}

const ::google::protobuf::Map<TString, Ydb::TypedValue>& TProtoAccessor::GetProtoMap(const TParams& params) {
    return params.GetProtoMap();
}

::google::protobuf::Map<TString, Ydb::TypedValue>* TProtoAccessor::GetProtoMapPtr(TParams& params) {
    return params.GetProtoMapPtr();
}

template Ydb::Export::ExportToS3Settings::Scheme TProtoAccessor::GetProto<Ydb::Export::ExportToS3Settings>(ES3Scheme value);
template Ydb::Import::ImportFromS3Settings::Scheme TProtoAccessor::GetProto<Ydb::Import::ImportFromS3Settings>(ES3Scheme value);

template <typename TProtoSettings>
ES3Scheme TProtoAccessor::FromProto(typename TProtoSettings::Scheme value) {
    switch (value) {
    case TProtoSettings::HTTP:
        return ES3Scheme::HTTP;
    default:
        return ES3Scheme::HTTPS;
    }
}

template ES3Scheme TProtoAccessor::FromProto<Ydb::Export::ExportToS3Settings>(Ydb::Export::ExportToS3Settings::Scheme value);
template ES3Scheme TProtoAccessor::FromProto<Ydb::Import::ImportFromS3Settings>(Ydb::Import::ImportFromS3Settings::Scheme value);

Ydb::Export::ExportToS3Settings::StorageClass TProtoAccessor::GetProto(NExport::TExportToS3Settings::EStorageClass value) {
    switch (value) {
    case NExport::TExportToS3Settings::EStorageClass::STANDARD:
        return Ydb::Export::ExportToS3Settings::STANDARD;
    case NExport::TExportToS3Settings::EStorageClass::REDUCED_REDUNDANCY:
        return Ydb::Export::ExportToS3Settings::REDUCED_REDUNDANCY;
    case NExport::TExportToS3Settings::EStorageClass::STANDARD_IA:
        return Ydb::Export::ExportToS3Settings::STANDARD_IA;
    case NExport::TExportToS3Settings::EStorageClass::ONEZONE_IA:
        return Ydb::Export::ExportToS3Settings::ONEZONE_IA;
    case NExport::TExportToS3Settings::EStorageClass::INTELLIGENT_TIERING:
        return Ydb::Export::ExportToS3Settings::INTELLIGENT_TIERING;
    case NExport::TExportToS3Settings::EStorageClass::GLACIER:
        return Ydb::Export::ExportToS3Settings::GLACIER;
    case NExport::TExportToS3Settings::EStorageClass::DEEP_ARCHIVE:
        return Ydb::Export::ExportToS3Settings::DEEP_ARCHIVE;
    case NExport::TExportToS3Settings::EStorageClass::OUTPOSTS:
        return Ydb::Export::ExportToS3Settings::OUTPOSTS;
    default:
        return Ydb::Export::ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED;
    }
}

NExport::TExportToS3Settings::EStorageClass TProtoAccessor::FromProto(Ydb::Export::ExportToS3Settings::StorageClass value) {
    switch (value) {
    case Ydb::Export::ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED:
        return NExport::TExportToS3Settings::EStorageClass::NOT_SET;
    case Ydb::Export::ExportToS3Settings::STANDARD:
        return NExport::TExportToS3Settings::EStorageClass::STANDARD;
    case Ydb::Export::ExportToS3Settings::REDUCED_REDUNDANCY:
        return NExport::TExportToS3Settings::EStorageClass::REDUCED_REDUNDANCY;
    case Ydb::Export::ExportToS3Settings::STANDARD_IA:
        return NExport::TExportToS3Settings::EStorageClass::STANDARD_IA;
    case Ydb::Export::ExportToS3Settings::ONEZONE_IA:
        return NExport::TExportToS3Settings::EStorageClass::ONEZONE_IA;
    case Ydb::Export::ExportToS3Settings::INTELLIGENT_TIERING:
        return NExport::TExportToS3Settings::EStorageClass::INTELLIGENT_TIERING;
    case Ydb::Export::ExportToS3Settings::GLACIER:
        return NExport::TExportToS3Settings::EStorageClass::GLACIER;
    case Ydb::Export::ExportToS3Settings::DEEP_ARCHIVE:
        return NExport::TExportToS3Settings::EStorageClass::DEEP_ARCHIVE;
    case Ydb::Export::ExportToS3Settings::OUTPOSTS:
        return NExport::TExportToS3Settings::EStorageClass::OUTPOSTS;
    default:
        return NExport::TExportToS3Settings::EStorageClass::UNKNOWN;
    }
}

NExport::EExportProgress TProtoAccessor::FromProto(Ydb::Export::ExportProgress::Progress value) {
    switch (value) {
    case Ydb::Export::ExportProgress::PROGRESS_UNSPECIFIED:
        return NExport::EExportProgress::Unspecified;
    case Ydb::Export::ExportProgress::PROGRESS_PREPARING:
        return NExport::EExportProgress::Preparing;
    case Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA:
        return NExport::EExportProgress::TransferData;
    case Ydb::Export::ExportProgress::PROGRESS_DONE:
        return NExport::EExportProgress::Done;
    case Ydb::Export::ExportProgress::PROGRESS_CANCELLATION:
        return NExport::EExportProgress::Cancellation;
    case Ydb::Export::ExportProgress::PROGRESS_CANCELLED:
        return NExport::EExportProgress::Cancelled;
    default:
        return NExport::EExportProgress::Unknown;
    }
}

NImport::EImportProgress TProtoAccessor::FromProto(Ydb::Import::ImportProgress::Progress value) {
    switch (value) {
    case Ydb::Import::ImportProgress::PROGRESS_UNSPECIFIED:
        return NImport::EImportProgress::Unspecified;
    case Ydb::Import::ImportProgress::PROGRESS_PREPARING:
        return NImport::EImportProgress::Preparing;
    case Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA:
        return NImport::EImportProgress::TransferData;
    case Ydb::Import::ImportProgress::PROGRESS_BUILD_INDEXES:
        return NImport::EImportProgress::BuildIndexes;
    case Ydb::Import::ImportProgress::PROGRESS_DONE:
        return NImport::EImportProgress::Done;
    case Ydb::Import::ImportProgress::PROGRESS_CANCELLATION:
        return NImport::EImportProgress::Cancellation;
    case Ydb::Import::ImportProgress::PROGRESS_CANCELLED:
        return NImport::EImportProgress::Cancelled;
    default:
        return NImport::EImportProgress::Unknown;
    }
}

} // namespace NYdb
