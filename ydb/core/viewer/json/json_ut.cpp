#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include "json.h"
#include <ydb/core/viewer/protos/viewer.pb.h>

Y_UNIT_TEST_SUITE(Json) {
    Y_UNIT_TEST(BasicRendering) {
        NKikimrViewer::TMetaTableInfo protoMessage;
        TJsonSettings settings;
        {
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, protoMessage, settings);
            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{}");
        }
        protoMessage.SetTaskId("taskId");
        protoMessage.SetUnique(true);
        protoMessage.SetMaxRecords(1234);
        protoMessage.SetMinTimestamp(1234);
        protoMessage.AddSrcTables("table1");
        protoMessage.AddSrcTables("table2");
        protoMessage.MutableSchema()->Add()->SetName("name1");
        protoMessage.MutableSchema()->Add()->SetName("name2");
        {
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, protoMessage, settings);
            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{\"Schema\":[{\"Name\":\"name1\"},{\"Name\":\"name2\"}],\"MinTimestamp\":\"1234\",\"MaxRecords\":1234,\"Unique\":true,\"TaskId\":\"taskId\",\"SrcTables\":[\"table1\",\"table2\"]}");
        }
        settings.UI64AsString = false;
        {
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, protoMessage, settings);
            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{\"Schema\":[{\"Name\":\"name1\"},{\"Name\":\"name2\"}],\"MinTimestamp\":1234,\"MaxRecords\":1234,\"Unique\":true,\"TaskId\":\"taskId\",\"SrcTables\":[\"table1\",\"table2\"]}");
        }
        protoMessage.ClearSrcTables();
        {
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, protoMessage, settings);
            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{\"Schema\":[{\"Name\":\"name1\"},{\"Name\":\"name2\"}],\"MinTimestamp\":1234,\"MaxRecords\":1234,\"Unique\":true,\"TaskId\":\"taskId\"}");
        }
        settings.EmptyRepeated = true;
        {
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, protoMessage, settings);
            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{\"Schema\":[{\"Name\":\"name1\"},{\"Name\":\"name2\"}],\"MinTimestamp\":1234,\"MaxRecords\":1234,\"Unique\":true,\"TaskId\":\"taskId\",\"Attrs\":{},\"SrcTables\":[]}");
        }
    }
}
