#include "operation_helpers.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>

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
}

}

