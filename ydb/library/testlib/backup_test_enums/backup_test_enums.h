#pragma once

#include <ydb/core/tx/datashard/backup_restore_traits.h>

#include <util/system/yassert.h>

namespace NKikimr {

// Backup data formats a test can be parametrized with, e.g.
// Y_UNIT_TEST(Name, EBackupTestDataFormat) { ... ToDataFormat(Arg<0>()) ... }.
// Unlike NBackupRestoreTraits::EDataFormat it has no Invalid member, so the
// parametrized test macro iterates only over real formats.
enum class EBackupTestDataFormat {
    Csv /* "csv" */,
    Parquet /* "parquet" */,
};

inline NDataShard::NBackupRestoreTraits::EDataFormat ToDataFormat(EBackupTestDataFormat format) {
    switch (format) {
        case EBackupTestDataFormat::Csv:
            return NDataShard::NBackupRestoreTraits::EDataFormat::YdbDump;
        case EBackupTestDataFormat::Parquet:
            return NDataShard::NBackupRestoreTraits::EDataFormat::Parquet;
    }
    Y_ABORT("Unexpected backup test data format");
}

} // namespace NKikimr
