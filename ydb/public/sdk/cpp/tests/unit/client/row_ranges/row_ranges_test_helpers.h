#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/rows.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

#include <google/protobuf/text_format.h>

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace NRowRangesTest {

inline NYdb::TType PrimitiveType(NYdb::EPrimitiveType primitive) {
    return NYdb::TTypeBuilder().Primitive(primitive).Build();
}

class TResultSetBuilder {
public:
    TResultSetBuilder& Column(std::string name, const NYdb::TType& type) {
        auto* column = Proto_.add_columns();
        column->set_name(std::move(name));
        column->mutable_type()->CopyFrom(type.GetProto());
        return *this;
    }

    TResultSetBuilder& Row(const NYdb::TValue& cell) {
        return Row(std::vector<NYdb::TValue>{cell});
    }

    TResultSetBuilder& Row(std::vector<NYdb::TValue> cells) {
        auto* row = Proto_.add_rows();
        for (const auto& cell : cells) {
            row->add_items()->CopyFrom(cell.GetProto());
        }
        return *this;
    }

    NYdb::TResultSet Build() {
        return NYdb::TResultSet(std::move(Proto_));
    }

private:
    Ydb::ResultSet Proto_;
};

inline NYdb::TResultSet SingleColumn(std::string name, NYdb::TValue value) {
    return TResultSetBuilder()
        .Column(std::move(name), value.GetType())
        .Row(std::move(value))
        .Build();
}

inline NYdb::TResultSet MakeSingleInt32ResultSet(int32_t value) {
    return SingleColumn("v", NYdb::TValueBuilder().Int32(value).Build());
}

inline NYdb::TResultSet MakeEmptyInt32SchemaResultSet() {
    return TResultSetBuilder()
        .Column("v", PrimitiveType(NYdb::EPrimitiveType::Int32))
        .Build();
}

inline NYdb::TResultSet MakeThreeInt32RowsResultSet() {
    TResultSetBuilder builder;
    builder.Column("v", PrimitiveType(NYdb::EPrimitiveType::Int32));
    for (int32_t value : {1, 2, 3}) {
        builder.Row(NYdb::TValueBuilder().Int32(value).Build());
    }
    return builder.Build();
}

inline NYdb::TResultSet MakeOptionalInt32ResultSet(const std::vector<std::optional<int32_t>>& values) {
    TResultSetBuilder builder;
    builder.Column("v", NYdb::TTypeBuilder()
        .BeginOptional()
        .Primitive(NYdb::EPrimitiveType::Int32)
        .EndOptional()
        .Build());
    for (const auto& value : values) {
        if (value) {
            builder.Row(NYdb::TValueBuilder().OptionalInt32(value).Build());
        } else {
            builder.Row(NYdb::TValueBuilder().EmptyOptional(NYdb::EPrimitiveType::Int32).Build());
        }
    }
    return builder.Build();
}

inline NYdb::TResultSet MakeIdNameResultSet(const std::vector<std::pair<int32_t, std::string>>& rows) {
    TResultSetBuilder builder;
    builder.Column("id", PrimitiveType(NYdb::EPrimitiveType::Int32));
    builder.Column("name", PrimitiveType(NYdb::EPrimitiveType::Utf8));
    for (const auto& [id, name] : rows) {
        builder.Row({
            NYdb::TValueBuilder().Int32(id).Build(),
            NYdb::TValueBuilder().Utf8(name).Build(),
        });
    }
    return builder.Build();
}

inline NYdb::TResultSet MakeSingleDateResultSet(uint32_t daysSinceEpoch) {
    const TInstant instant = TInstant::Seconds(static_cast<ui64>(daysSinceEpoch) * 86400ULL);
    return SingleColumn("d", NYdb::TValueBuilder().Date(instant).Build());
}

inline NYdb::TResultSet MakeSingleDatetimeResultSet(uint32_t secondsSinceEpoch) {
    return SingleColumn(
        "d",
        NYdb::TValueBuilder().Datetime(TInstant::Seconds(secondsSinceEpoch)).Build());
}

inline NYdb::TResultSet MakeSingleTimestampResultSet(uint64_t microsSinceEpoch) {
    return SingleColumn(
        "d",
        NYdb::TValueBuilder().Timestamp(TInstant::MicroSeconds(microsSinceEpoch)).Build());
}

inline NYdb::TResultSet MakeOptionalDateResultSet(const std::vector<std::optional<uint32_t>>& values) {
    TResultSetBuilder builder;
    builder.Column("d", NYdb::TTypeBuilder()
        .BeginOptional()
        .Primitive(NYdb::EPrimitiveType::Date)
        .EndOptional()
        .Build());
    for (const auto& value : values) {
        if (value) {
            const TInstant instant = TInstant::Seconds(static_cast<ui64>(*value) * 86400ULL);
            builder.Row(NYdb::TValueBuilder().OptionalDate(instant).Build());
        } else {
            builder.Row(NYdb::TValueBuilder().EmptyOptional(NYdb::EPrimitiveType::Date).Build());
        }
    }
    return builder.Build();
}

inline NYdb::TResultSet MakeSingleIntervalResultSet(int64_t micros) {
    return SingleColumn("v", NYdb::TValueBuilder().Interval(micros).Build());
}

inline NYdb::TResultSet MakeSingleJsonResultSet(const std::string& json) {
    return SingleColumn("v", NYdb::TValueBuilder().Json(json).Build());
}

inline NYdb::TResultSet MakeSingleBytesResultSet(const std::string& bytesLiteral) {
    return SingleColumn("v", NYdb::TValueBuilder().String(bytesLiteral).Build());
}

inline NYdb::TResultSet MakeSingleTimestamp64ResultSet(int64_t micros) {
    const auto value = std::chrono::sys_time<NYdb::TWideMicroseconds>(NYdb::TWideMicroseconds(micros));
    return SingleColumn("v", NYdb::TValueBuilder().Timestamp64(value).Build());
}

inline NYdb::TResultSet MakeSingleInterval64ResultSet(int64_t micros) {
    return SingleColumn("v", NYdb::TValueBuilder().Interval64(NYdb::TWideMicroseconds(micros)).Build());
}

inline NYdb::TResultSet MakeListInt32ResultSet(const std::vector<int32_t>& values) {
    NYdb::TValueBuilder list;
    list.BeginList();
    for (int32_t value : values) {
        list.AddListItem().Int32(value);
    }
    list.EndList();
    return SingleColumn("v", list.Build());
}

inline NYdb::TResultSet MakeTupleColumnResultSet() {
    const NYdb::TValue value = NYdb::TValueBuilder()
        .BeginTuple()
        .AddElement().BeginOptional().Utf8("hello").EndOptional()
        .AddElement().Int8(-5)
        .EndTuple()
        .Build();
    return SingleColumn("t", value);
}

inline NYdb::TResultSet MakeStructColumnResultSet() {
    const NYdb::TValue value = NYdb::TValueBuilder()
        .BeginStruct()
        .AddMember("id").Int32(42)
        .AddMember("name").Utf8("struct")
        .EndStruct()
        .Build();
    return SingleColumn("s", value);
}

inline NYdb::TResultSet MakeDictColumnResultSet() {
    const NYdb::TValue value = NYdb::TValueBuilder()
        .BeginDict()
        .AddDictItem().DictKey().Uint32(1).DictPayload().Utf8("one")
        .AddDictItem().DictKey().Uint32(2).DictPayload().Utf8("two")
        .EndDict()
        .Build();
    return SingleColumn("d", value);
}

inline NYdb::TResultSet MakeOptionalListInt32ResultSet() {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { optional_type { item { list_type { item { type_id: INT32 } } } } }\n"
        "}\n"
        "rows {\n"
        "  items { null_flag_value: NULL_VALUE }\n"
        "}\n"
        "rows {\n"
        "  items {\n"
        "    items { int32_value: 5 }\n"
        "    items { int32_value: 6 }\n"
        "  }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(text, &proto);
    return NYdb::TResultSet(std::move(proto));
}

inline NYdb::TResultSet MakeTaggedColumnResultSet() {
    const NYdb::TValue value = NYdb::TValueBuilder()
        .BeginTagged("my_tag")
        .Utf8("tagged_value")
        .EndTagged()
        .Build();
    return SingleColumn("t", value);
}

inline NYdb::TValue MakeVariantUtf8Value() {
    Ydb::Type type;
    auto* tupleItems = type.mutable_variant_type()->mutable_tuple_items();
    tupleItems->add_elements()->set_type_id(Ydb::Type::INT32);
    tupleItems->add_elements()->set_type_id(Ydb::Type::UTF8);
    Ydb::Value value;
    value.set_variant_index(1);
    value.mutable_nested_value()->set_text_value("variant_utf8");
    return NYdb::TValue(NYdb::TType(type), std::move(value));
}

inline NYdb::TResultSet MakeVariantColumnResultSet() {
    return SingleColumn("v", MakeVariantUtf8Value());
}

inline NYdb::TResultSet MakeSingleColumnResultSet(const std::string& columnName, const NYdb::TValue& value) {
    return SingleColumn(columnName, value);
}

inline int32_t SumColumnV(const NYdb::TResultSet& rs) {
    int32_t sum = 0;
    NYdb::TResultSetParser parser(rs);
    while (parser.TryNextRow()) {
        sum += static_cast<int32_t>(parser.ColumnParser("v").GetInt32());
    }
    return sum;
}

inline NYdb::NTable::TDataQueryResult MakeDataQueryResult(std::vector<NYdb::TResultSet>&& sets) {
    return NYdb::NTable::TDataQueryResult(
        NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{}),
        std::move(sets),
        std::nullopt,
        std::nullopt,
        false,
        std::nullopt);
}

template <class T>
T ReadSingle(NYdb::TResultSet&& rs, std::string_view col) {
    NYdb::TRowRange range(std::move(rs));
    for (auto [value] : range.Get<T>({col})) {
        return value;
    }
    ythrow yexception() << "empty result set";
}

template <class T>
std::vector<T> ReadAll(NYdb::TResultSet&& rs, std::string_view col) {
    std::vector<T> result;
    NYdb::TRowRange range(std::move(rs));
    for (auto [value] : range.Get<T>({col})) {
        result.push_back(std::move(value));
    }
    return result;
}

template <class... Args>
std::tuple<Args...> ReadSingleRow(
    NYdb::TResultSet&& rs,
    std::initializer_list<std::string_view> cols) {
    NYdb::TRowRange range(std::move(rs));
    for (auto row : range.Get<Args...>(cols)) {
        return row;
    }
    ythrow yexception() << "empty result set";
}

template <class... Args>
std::vector<std::tuple<Args...>> ReadAllRows(
    NYdb::TResultSet&& rs,
    std::initializer_list<std::string_view> cols) {
    std::vector<std::tuple<Args...>> result;
    NYdb::TRowRange range(std::move(rs));
    for (auto row : range.Get<Args...>(cols)) {
        result.push_back(std::move(row));
    }
    return result;
}

template <class T, class Exception>
void AssertGetThrows(NYdb::TResultSet&& rs, std::initializer_list<std::string_view> cols) {
    NYdb::TRowRange range(std::move(rs));
    UNIT_ASSERT_EXCEPTION(
        ([&] {
            for (auto row : range.Get<T>(cols)) {
                (void)row;
            }
        }()),
        Exception);
}

} // namespace NRowRangesTest
