#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

#include <type_traits>

namespace NKikimrSchemeOp {
    class TBackupTask;
    class TS3Settings;
    class TFSSettings;
} // namespace NKikimrSchemeOp

namespace Ydb {
namespace Export {
    class ExportToS3Settings;
    class ExportToFsSettings;
} // namespace Export
namespace Import {
    class ImportFromS3Settings;
    class ImportFromFsSettings;
} // namespace Import
} // namespace Ydb

namespace NKikimr::NBackup::NFieldsWrappers {

template <typename TSettings>
const TSettings& GetSettings(const NKikimrSchemeOp::TBackupTask& task);

template <typename TSettings>
const TString& GetCommonDestination(const TSettings&);

template <typename TItem>
TString& MutableItemDestination(TItem&);

template <typename TItem>
const TString& GetItemDestination(const TItem&);

template <typename T>
concept CFsStorageSettings =
    std::is_same_v<T, NKikimrSchemeOp::TFSSettings> ||
    std::is_same_v<T, Ydb::Export::ExportToFsSettings> ||
    std::is_same_v<T, Ydb::Import::ImportFromFsSettings>;

template <typename T>
concept CS3StorageSettings =
    std::is_same_v<T, NKikimrSchemeOp::TS3Settings> ||
    std::is_same_v<T, Ydb::Export::ExportToS3Settings> ||
    std::is_same_v<T, Ydb::Import::ImportFromS3Settings>;

template <typename T>
concept CStorageSettings = CFsStorageSettings<T> || CS3StorageSettings<T>;

template <CStorageSettings TSettings>
constexpr TStringBuf GetStorageName() {
    if constexpr (CFsStorageSettings<TSettings>) {
        return "fs"sv;
    } else {
        return "s3"sv;
    }
}

} // NKikimr::NBackup
