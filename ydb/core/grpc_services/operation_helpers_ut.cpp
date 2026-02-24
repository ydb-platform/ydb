#include "operation_helpers.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/protos/forced_compaction.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>


namespace NKikimr {

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
    }

    Y_UNIT_TEST(CompactionCanceled) {
        TString compaction = R"(
    Settings {
        source_path: "/MyRoot/Table2"
        cascade: true
        max_shards_in_flight: 2
    }
    Progress: 0
    State: STATE_CANCELLED
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
        UNIT_ASSERT_DOUBLES_EQUAL(metadata.progress(), 0., 1e-5);
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
    }
}

}

