#include "operation_helpers.h"
#include "rpc_export_base.h"
#include "rpc_import_base.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/protos/forced_compaction.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>


namespace NKikimr {

namespace {

template <typename TSettings>
void SetEncryptionKey(TSettings& settings) {
    auto& encryption = *settings.mutable_encryption_settings();
    encryption.set_encryption_algorithm("AES-128-GCM");
    encryption.mutable_symmetric_key()->set_key("0123456789abcdef");
}

template <typename TMetadata>
void CheckEncryptionKeyCleared(const Ydb::Operations::Operation& operation) {
    TMetadata metadata;
    UNIT_ASSERT(operation.metadata().UnpackTo(&metadata));
    const auto& encryption = metadata.settings().encryption_settings();
    UNIT_ASSERT(!encryption.has_symmetric_key());
    UNIT_ASSERT_VALUES_EQUAL(encryption.encryption_algorithm(), "AES-128-GCM");
}

NKikimrExport::TExport MakeExport() {
    NKikimrExport::TExport exprt;
    // Metadata is filled only for SUCCESS or CANCELLED operations
    exprt.SetStatus(Ydb::StatusIds::SUCCESS);
    exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA);
    return exprt;
}

NKikimrImport::TImport MakeImport() {
    NKikimrImport::TImport importInfo;
    // Metadata is filled only for SUCCESS or CANCELLED operations
    importInfo.SetStatus(Ydb::StatusIds::SUCCESS);
    importInfo.SetProgress(Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);
    return importInfo;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(OperationMapping) {
    Y_UNIT_TEST(IndexBuildCanceled) {
        TString indexBuild = R"___(
  State: STATE_CANCELLED
  Settings {
    source_path: "/MyRoot/Table"
    index {
      name: "index1"
      index_columns: "index"
      global_index {
      }
    }
  }
  Progress: 0
)___";

        NKikimrIndexBuilder::TIndexBuild buildProto;
        google::protobuf::TextFormat::ParseFromString(indexBuild, &buildProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(buildProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), true);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::CANCELLED);
    }
    Y_UNIT_TEST(IndexBuildSuccess) {
        TString indexBuild = R"___(
  State: STATE_DONE
  Settings {
    source_path: "/MyRoot/Table"
    index {
      name: "index1"
      index_columns: "index"
      global_index {
      }
    }
  }
  Progress: 0
)___";

        NKikimrIndexBuilder::TIndexBuild buildProto;
        google::protobuf::TextFormat::ParseFromString(indexBuild, &buildProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(buildProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), true);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
    }
    Y_UNIT_TEST(IndexBuildRejected) {
        TString indexBuild = R"___(
  State: STATE_REJECTED
  Settings {
    source_path: "/MyRoot/Table"
    index {
      name: "index1"
      index_columns: "index"
      global_index {
      }
    }
  }
  Progress: 0
)___";

        NKikimrIndexBuilder::TIndexBuild buildProto;
        google::protobuf::TextFormat::ParseFromString(indexBuild, &buildProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(buildProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), true);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(CompactionInProgress) {
        TString compaction = R"(
    Settings {
        source_path: "/MyRoot/Table1"
        cascade: false
        max_shards_in_flight: 3
    }
    Progress: 50
    State: STATE_IN_PROGRESS
    ShardsTotal: 10
    ShardsDone: 5
)";
        NKikimrForcedCompaction::TForcedCompaction compactionProto;
        google::protobuf::TextFormat::ParseFromString(compaction, &compactionProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(compactionProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), false);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);

        Ydb::Table::CompactMetadata metadata;
        operation.metadata().UnpackTo(&metadata);
        UNIT_ASSERT_VALUES_EQUAL(metadata.path(), "/MyRoot/Table1");
        UNIT_ASSERT_VALUES_EQUAL(metadata.cascade(), false);
        UNIT_ASSERT_VALUES_EQUAL(metadata.max_shards_in_flight(), 3);
        UNIT_ASSERT_VALUES_EQUAL(metadata.state(), Ydb::Table::CompactState::STATE_IN_PROGRESS);
        UNIT_ASSERT_DOUBLES_EQUAL(metadata.progress(), 50., 1e-5);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_total(), 10);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_done(), 5);
    }

    Y_UNIT_TEST(CompactionCanceled) {
        TString compaction = R"(
    Settings {
        source_path: "/MyRoot/Table2"
        cascade: true
        max_shards_in_flight: 2
    }
    Progress: 70
    State: STATE_CANCELLED
    ShardsTotal: 10
    ShardsDone: 7
)";
        NKikimrForcedCompaction::TForcedCompaction compactionProto;
        google::protobuf::TextFormat::ParseFromString(compaction, &compactionProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(compactionProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), true);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::CANCELLED);

        Ydb::Table::CompactMetadata metadata;
        operation.metadata().UnpackTo(&metadata);
        UNIT_ASSERT_VALUES_EQUAL(metadata.path(), "/MyRoot/Table2");
        UNIT_ASSERT_VALUES_EQUAL(metadata.cascade(), true);
        UNIT_ASSERT_VALUES_EQUAL(metadata.max_shards_in_flight(), 2);
        UNIT_ASSERT_VALUES_EQUAL(metadata.state(), Ydb::Table::CompactState::STATE_CANCELLED);
        UNIT_ASSERT_DOUBLES_EQUAL(metadata.progress(), 70., 1e-5);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_total(), 10);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_done(), 7);
    }

    Y_UNIT_TEST(CompactionDone) {
        TString compaction = R"(
    Settings {
        source_path: "/MyRoot/Table3"
        cascade: false
        max_shards_in_flight: 1
    }
    Progress: 100
    State: STATE_DONE
    ShardsTotal: 10
    ShardsDone: 10
)";
        NKikimrForcedCompaction::TForcedCompaction compactionProto;
        google::protobuf::TextFormat::ParseFromString(compaction, &compactionProto);

        Ydb::Operations::Operation operation;

        NGRpcService::ToOperation(compactionProto, &operation);

        UNIT_ASSERT_VALUES_EQUAL(operation.ready(), true);
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);

        Ydb::Table::CompactMetadata metadata;
        operation.metadata().UnpackTo(&metadata);
        UNIT_ASSERT_VALUES_EQUAL(metadata.path(), "/MyRoot/Table3");
        UNIT_ASSERT_VALUES_EQUAL(metadata.cascade(), false);
        UNIT_ASSERT_VALUES_EQUAL(metadata.max_shards_in_flight(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.state(), Ydb::Table::CompactState::STATE_DONE);
        UNIT_ASSERT_DOUBLES_EQUAL(metadata.progress(), 100., 1e-5);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_total(), 10);
        UNIT_ASSERT_VALUES_EQUAL(metadata.shards_done(), 10);
    }

    Y_UNIT_TEST(ExportToS3ClearsEncryptionKey) {
        auto exprt = MakeExport();
        SetEncryptionKey(*exprt.MutableExportToS3Settings());
        CheckEncryptionKeyCleared<Ydb::Export::ExportToS3Metadata>(NGRpcService::TExportConv::ToOperation(exprt));
    }

    Y_UNIT_TEST(ExportToFsClearsEncryptionKey) {
        auto exprt = MakeExport();
        SetEncryptionKey(*exprt.MutableExportToFsSettings());
        CheckEncryptionKeyCleared<Ydb::Export::ExportToFsMetadata>(NGRpcService::TExportConv::ToOperation(exprt));
    }

    Y_UNIT_TEST(ImportFromS3ClearsEncryptionKey) {
        auto importInfo = MakeImport();
        SetEncryptionKey(*importInfo.MutableImportFromS3Settings());
        CheckEncryptionKeyCleared<Ydb::Import::ImportFromS3Metadata>(NGRpcService::TImportConv::ToOperation(importInfo));
    }

    Y_UNIT_TEST(ImportFromFsClearsEncryptionKey) {
        auto importInfo = MakeImport();
        SetEncryptionKey(*importInfo.MutableImportFromFsSettings());
        CheckEncryptionKeyCleared<Ydb::Import::ImportFromFsMetadata>(NGRpcService::TImportConv::ToOperation(importInfo));
    }
}

}

