#include "transfer_common.h"

void KeyColumnFirst(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1")),
        }}
    });
}

void KeyColumnLast(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Message Utf8 NOT NULL,
                Key Uint64 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1")),
        }}
    });
}

void ComplexKey(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key1 Uint64 NOT NULL,
                Key3 Uint64 NOT NULL,
                Value1 Utf8,
                Key2 Uint64 NOT NULL,
                Value2 Utf8,
                Key4 Uint64 NOT NULL,
                PRIMARY KEY (Key3, Key2, Key1, Key4)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key1:CAST(1 AS Uint64),
                        Key2:CAST(2 AS Uint64),
                        Value2:CAST("value-2" AS Utf8),
                        Key4:CAST(4 AS Uint64),
                        Key3:CAST(3 AS Uint64),
                        Value1:CAST("value-1" AS Utf8),
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1"}},

        .Expectations = {{
            _C("Key1", ui64(1)),
            _C("Key2", ui64(2)),
            _C("Key3", ui64(3)),
            _C("Key4", ui64(4)),
            _C("Value1", TString("value-1")),
            _C("Value2", TString("value-2")),
        }}
    });
}

void ProcessingJsonMessage(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Id Uint64 NOT NULL,
                FirstName Utf8 NOT NULL,
                LastName Utf8 NOT NULL,
                Salary Uint64 NOT NULL,
                PRIMARY KEY (Id)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                $input = CAST($x._data AS JSON);

                return [
                    <|
                        Id:        Yson::ConvertToUint64($input.id),
                        FirstName: CAST(Yson::ConvertToString($input.first_name) AS Utf8),
                        LastName:  CAST(Yson::ConvertToString($input.last_name) AS Utf8),
                        Salary:    CAST(Yson::ConvertToString($input.salary) AS UInt64)
                    |>
                ];
            };
        )",

        .Messages = {{R"({
            "id": 1,
            "first_name": "Vasya",
            "last_name": "Pupkin",
            "salary": "123"
        })"}},

        .Expectations = {{
            _C("Id", ui64(1)),
            _C("FirstName", TString("Vasya")),
            _C("LastName", TString("Pupkin")),
            _C("Salary", ui64(123)),
        }}
    });
}

void NullableColumn(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1")),
        }}
    });
}

void ColumnType_Date(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Date,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message: CAST($x._data AS Date)
                    |>
                ];
            };
        )",

        .Messages = {{"2025-02-21"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TInstant::ParseIso8601("2025-02-21")),
        }}
    });
}

void ColumnType_Double(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Double,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message: CAST($x._data AS Double)
                    |>
                ];
            };
        )",

        .Messages = {{"1.23"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", 1.23),
        }}
    });
}

void ColumnType_Utf8_LongValue(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890")),
        }}
    });
}

void ColumnType_Uuid(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Uuid,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Uuid)
                    |>
                ];
            };
        )",

        .Messages = {{"123e4567-e89b-12d3-a456-426614174000"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", TUuidValue("123e4567-e89b-12d3-a456-426614174000")),
        }}
    });
}

void ColumnType_Bool(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Bool,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Bool)
                    |>
                ];
            };
        )",

        .Messages = {{"true"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", true),
        }}
    });
}

void ColumnType_Int8(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Int8,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Int8)
                    |>
                ];
            };
        )",

        .Messages = {{"7"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", i8(7)),
        }}
    });
}

void ColumnType_Int16(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Int16,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Int16)
                    |>
                ];
            };
        )",

        .Messages = {{"32767"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", i16(32767)),
        }}
    });
}

void ColumnType_Int32(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Int32,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Int32)
                    |>
                ];
            };
        )",

        .Messages = {{"2147483647"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", i32(2147483647)),
        }}
    });
}

void ColumnType_Int64(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Int64,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:CAST($x._offset AS Uint64),
                        Message:CAST($x._data AS Int64)
                    |>
                ];
            };
        )",

        .Messages = {{"9223372036854775807"}},

        .Expectations = {{
            _C("Key", ui64(0)),
            _C("Message", i64(9223372036854775807LL)),
        }}
    });
}

void MessageField_Partition(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Partition Uint32 NOT NULL,
                Message Utf8,
                PRIMARY KEY (Partition)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Partition:CAST($x._partition AS Uint32),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {{"Message-1", 7}},

        .Expectations = {{
            _C("Partition", ui32(7)),
            _C("Message", TString("Message-1")),
        }}
    });
}

void MessageField_SeqNo(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                SeqNo Uint64 NOT NULL,
                Message Utf8,
                PRIMARY KEY (SeqNo)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        SeqNo:CAST($x._seq_no AS Uint32),
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {_withSeqNo(13)},

        .Expectations = {{
            _C("SeqNo", ui64(13)),
        }}
    });
}

void MessageField_ProducerId(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Offset Uint64 NOT NULL,
                ProducerId Utf8,
                PRIMARY KEY (Offset)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Offset:CAST($x._offset AS Uint64),
                        ProducerId:CAST($x._producer_id AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {_withProducerId("Producer-13")},

        .Expectations = {{
            _C("ProducerId", TString("Producer-13")),
        }}
    });
}

void MessageField_MessageGroupId(const std::string& tableType) {
    MainTestCase(std::nullopt, tableType).Run({
        .TableDDL = R"(
            CREATE TABLE `%s` (
                Offset Uint64 NOT NULL,
                MessageGroupId Utf8,
                PRIMARY KEY (Offset)
            )  WITH (
                STORE = %s
            );
        )",

        .Lambda = R"(
            $l = ($x) -> {
                return [
                    <|
                        Offset:CAST($x._offset AS Uint64),
                        MessageGroupId:CAST($x._message_group_id AS Utf8)
                    |>
                ];
            };
        )",

        .Messages = {_withMessageGroupId("MessageGroupId-13")},

        .Expectations = {{
            _C("MessageGroupId", TString("MessageGroupId-13")),
        }}
    });
}

void WriteNullToKeyColumn(const std::string& tableType) {
    MainTestCase testCase(std::nullopt, tableType);

    testCase.CreateTable(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )");
    testCase.CreateTopic(1);
    testCase.CreateTransfer(R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:NULL,
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )");

    testCase.Write({"Message-1"});

    testCase.CheckTransferStateError("Error transform message partition 0 offset 0: The value of the 'Key' column must be non-NULL");

    testCase.DropTransfer();
    testCase.DropTable();
    testCase.DropTopic();
}

void WriteNullToColumn(const std::string& tableType) {
    MainTestCase testCase(std::nullopt, tableType);

    testCase.CreateTable(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )");
    testCase.CreateTopic(1);
    testCase.CreateTransfer(R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:$x._offset,
                        Message:NULL
                    |>
                ];
            };
        )");

    testCase.Write({"Message-1"});

    testCase.CheckTransferStateError("Error transform message partition 0 offset 0: The value of the 'Message' column must be non-NULL");

    testCase.DropTransfer();
    testCase.DropTable();
    testCase.DropTopic();
}

void ProcessingCDCMessage(const std::string& tableType) {
    MainTestCase testCase(std::nullopt, tableType);
    testCase.CreateSourceTable(R"(
        CREATE TABLE `%s` (
            object_id Utf8 NOT NULL,
            timestamp Datetime NOT NULL,
            operation Utf8,
            PRIMARY KEY (object_id, timestamp)
        )
        WITH (
            STORE = ROW
        )
    )");

    testCase.AddChangefeed();

    testCase.CreateTable(R"(
        CREATE TABLE `%s` (
            timestamp Datetime NOT NULL,
            object_id Utf8 NOT NULL,
            operation Utf8,
            PRIMARY KEY (timestamp, object_id)
        )
        WITH (
            STORE = %s
        )
    )");

    /*
        * Expected mesage:
        * { "update":{"operation":"value_1"},"key":["id_1","2019-01-01T15:30:00.000000Z"] }
        */
    testCase.CreateTransfer(R"(
        $l = ($x) -> {
            $d = CAST($x._data AS JSON);
            return [
                <|
                    timestamp: DateTime::MakeDatetime(DateTime::ParseIso8601(CAST(Yson::ConvertToString($d.key[1]) AS Utf8))),
                    object_id: CAST(Yson::ConvertToString($d.key[0]) AS Utf8),
                    operation: CAST(Yson::ConvertToString($d.update.operation) AS Utf8)
                |>
            ];
        };
    )", MainTestCase::CreateTransferSettings::WithTopic(TStringBuilder() << testCase.SourceTableName << "/" << testCase.ChangefeedName));

    testCase.CheckReplicationState(TReplicationDescription::EState::Running);

    testCase.ExecuteSourceTableQuery(R"(
        INSERT INTO `%s` (`object_id`, `timestamp`, `operation`)
        VALUES ('id_1', Datetime('2019-01-01T15:30:00Z'), 'value_1');
    )");

    testCase.CheckReplicationState(TReplicationDescription::EState::Running);

    testCase.CheckResult({{
        _C("operation", TString{"value_1"}),
        _C("object_id", TString{"id_1"}),
        _T<DateTimeChecker>("timestamp",TInstant::ParseIso8601("2019-01-01T15:30:00Z")),
    }});

    testCase.DropTransfer();
    testCase.DropTable();
    testCase.DropSourceTable();
}

void Upsert_DifferentBatch(const std::string& tableType) {
    MainTestCase testCase(std::nullopt, tableType);
    testCase.CreateTable(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )");

    testCase.CreateTopic(1);

    testCase.CreateTransfer(R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:1,
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )");

    testCase.Write({"Message-1"});

    testCase.CheckResult({{
        _C("Message", TString("Message-1"))
    }});

    testCase.Write({"Message-2"});

    Sleep(TDuration::Seconds(3));

    testCase.CheckResult({{
        _C("Message", TString("Message-2"))
    }});

    testCase.DropTransfer();
    testCase.DropTable();
}

void Upsert_OneBatch(const std::string& tableType) {
    MainTestCase testCase(std::nullopt, tableType);
    testCase.CreateTable(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Message Utf8,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = %s
            );
        )");

    testCase.CreateTopic(1);

    testCase.CreateTransfer(R"(
            $l = ($x) -> {
                return [
                    <|
                        Key:1,
                        Message:CAST($x._data AS Utf8)
                    |>
                ];
            };
        )");


    testCase.Write({"Message-1"});
    testCase.Write({"Message-2"});

    testCase.CheckResult({{
        _C("Message", TString("Message-2"))
    }});

    testCase.DropTransfer();
    testCase.DropTable();
}
