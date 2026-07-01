#include <ydb/core/tx/datashard/backup_restore_traits.h>

#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard::NBackupRestoreTraits {

Y_UNIT_TEST_SUITE(BackupRestoreTraitsTest) {
    // DataFormatFromTask: S3 settings -> data format mapping (oneof Format).
    Y_UNIT_TEST(DataFormatFromTaskS3) {
        NKikimrSchemeOp::TBackupTask task;

        // Format not set defaults to YdbDump (CSV).
        task.MutableS3Settings();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::YdbDump);

        task.MutableS3Settings()->MutableExportDataSettings()->MutableYdbDump();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::YdbDump);

        task.MutableS3Settings()->MutableExportDataSettings()->MutableParquet();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::Parquet);
    }

    // DataFormatFromTask: FS settings -> data format mapping (oneof Format).
    Y_UNIT_TEST(DataFormatFromTaskFS) {
        NKikimrSchemeOp::TBackupTask task;

        // Format not set defaults to YdbDump (CSV).
        task.MutableFSSettings();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::YdbDump);

        task.MutableFSSettings()->MutableExportDataSettings()->MutableYdbDump();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::YdbDump);

        task.MutableFSSettings()->MutableExportDataSettings()->MutableParquet();
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::Parquet);
    }

    // With no settings at all the task still resolves to a valid (YdbDump) format.
    Y_UNIT_TEST(DataFormatFromTaskNoSettings) {
        NKikimrSchemeOp::TBackupTask task;
        UNIT_ASSERT_EQUAL(DataFormatFromTask(task), EDataFormat::Invalid);
    }

    // ParquetExportSettingsFromTask must read the Parquet sub-message from S3 settings.
    Y_UNIT_TEST(ParquetExportSettingsFromTaskS3) {
        NKikimrSchemeOp::TBackupTask task;
        task.MutableS3Settings()->MutableExportDataSettings()->MutableParquet()->SetRowGroupSize(123);
        UNIT_ASSERT_VALUES_EQUAL(ParquetExportSettingsFromTask(task).RowGroupSize, 123);
    }

    // ParquetExportSettingsFromTask must read the Parquet sub-message from FS settings.
    Y_UNIT_TEST(ParquetExportSettingsFromTaskFS) {
        NKikimrSchemeOp::TBackupTask task;
        task.MutableFSSettings()->MutableExportDataSettings()->MutableParquet()->SetRowGroupSize(456);
        UNIT_ASSERT_VALUES_EQUAL(ParquetExportSettingsFromTask(task).RowGroupSize, 456);
    }

    // FS Parquet without an explicit RowGroupSize falls back to the proto default.
    Y_UNIT_TEST(ParquetExportSettingsFromTaskFSDefaultRowGroupSize) {
        NKikimrSchemeOp::TBackupTask task;
        task.MutableFSSettings()->MutableExportDataSettings()->MutableParquet();
        UNIT_ASSERT_VALUES_EQUAL(ParquetExportSettingsFromTask(task).RowGroupSize,
            NKikimrSchemeOp::TParquetFormat().GetRowGroupSize());
    }

    // For tasks without S3/FS settings a default (empty) Parquet message is returned.
    Y_UNIT_TEST(ParquetExportSettingsFromTaskNoSettings) {
        NKikimrSchemeOp::TBackupTask task;
        UNIT_ASSERT_VALUES_EQUAL(ParquetExportSettingsFromTask(task).RowGroupSize,
            NKikimrSchemeOp::TParquetFormat().GetRowGroupSize());
    }

    // The format iteration order used when probing for backup files.
    Y_UNIT_TEST(NextDataFormat) {
        UNIT_ASSERT_EQUAL(NextDataFormat(EDataFormat::YdbDump), EDataFormat::Parquet);
        UNIT_ASSERT_EQUAL(NextDataFormat(EDataFormat::Parquet), EDataFormat::Invalid);
        UNIT_ASSERT_EQUAL(NextDataFormat(EDataFormat::Invalid), EDataFormat::Invalid);
    }

    Y_UNIT_TEST(DataFileExtension) {
        UNIT_ASSERT_VALUES_EQUAL(DataFileExtension(EDataFormat::YdbDump, ECompressionCodec::None), ".csv");
        UNIT_ASSERT_VALUES_EQUAL(DataFileExtension(EDataFormat::YdbDump, ECompressionCodec::Zstd), ".csv.zst");

        // Parquet handles compression internally, so the codec must NOT add a suffix.
        UNIT_ASSERT_VALUES_EQUAL(DataFileExtension(EDataFormat::Parquet, ECompressionCodec::None), ".parquet");
        UNIT_ASSERT_VALUES_EQUAL(DataFileExtension(EDataFormat::Parquet, ECompressionCodec::Zstd), ".parquet");
    }

    Y_UNIT_TEST(DataKeySuffix) {
        UNIT_ASSERT_VALUES_EQUAL(DataKeySuffix(0, EDataFormat::YdbDump, ECompressionCodec::None, false), "data_00.csv");
        UNIT_ASSERT_VALUES_EQUAL(DataKeySuffix(1, EDataFormat::YdbDump, ECompressionCodec::Zstd, true), "data_01.csv.zst.enc");

        // Parquet keeps the .parquet extension regardless of the codec.
        UNIT_ASSERT_VALUES_EQUAL(DataKeySuffix(0, EDataFormat::Parquet, ECompressionCodec::None, false), "data_00.parquet");
        UNIT_ASSERT_VALUES_EQUAL(DataKeySuffix(3, EDataFormat::Parquet, ECompressionCodec::Zstd, false), "data_03.parquet");
        UNIT_ASSERT_VALUES_EQUAL(DataKeySuffix(2, EDataFormat::Parquet, ECompressionCodec::None, true), "data_02.parquet.enc");
    }
}

} // namespace NKikimr::NDataShard::NBackupRestoreTraits
