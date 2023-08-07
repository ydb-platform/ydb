#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeIO, AsYqlType) {
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Void().Get()),
        "[VoidType]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Null().Get()),
        "[NullType]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Bool().Get()),
        "[DataType; Bool]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Int8().Get()),
        "[DataType; Int8]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Int16().Get()),
        "[DataType; Int16]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Int32().Get()),
        "[DataType; Int32]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Int64().Get()),
        "[DataType; Int64]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Uint8().Get()),
        "[DataType; Uint8]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Uint16().Get()),
        "[DataType; Uint16]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Uint32().Get()),
        "[DataType; Uint32]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Uint64().Get()),
        "[DataType; Uint64]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Float().Get()),
        "[DataType; Float]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Double().Get()),
        "[DataType; Double]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.String().Get()),
        "[DataType; String]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Utf8().Get()),
        "[DataType; Utf8]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Date().Get()),
        "[DataType; Date]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Datetime().Get()),
        "[DataType; Datetime]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Timestamp().Get()),
        "[DataType; Timestamp]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.TzDate().Get()),
        "[DataType; TzDate]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.TzDatetime().Get()),
        "[DataType; TzDatetime]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.TzTimestamp().Get()),
        "[DataType; TzTimestamp]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Interval().Get()),
        "[DataType; Interval]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Json().Get()),
        "[DataType; Json]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Yson().Get()),
        "[DataType; Yson]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Uuid().Get()),
        "[DataType; Uuid]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Decimal(20, 10).Get()),
        "[DataType; Decimal; \"20\"; \"10\"]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Decimal(35, 35).Get()),
        "[DataType; Decimal; \"35\"; \"35\"]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(Optional(f.Bool()).Get()),
        "[OptionalType; [DataType; Bool]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.List(f.Bool()).Get()),
        "[ListType; [DataType; Bool]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Dict(f.Bool(), f.Int32()).Get()),
        "[DictType; [DataType; Bool]; [DataType; Int32]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Struct({}).Get()),
        "[StructType; []]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Struct({{"a", f.Bool()}}).Get()),
        "[StructType; [[a; [DataType; Bool]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Struct({{"a", f.Yson()}, {"b", f.Bool()}}).Get()),
        "[StructType; [[a; [DataType; Yson]]; [b; [DataType; Bool]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Struct({{"a", f.Int32()}, {"b", f.Int32()}, {"c", f.Int64()}}).Get()),
        "[StructType; [[a; [DataType; Int32]]; [b; [DataType; Int32]]; [c; [DataType; Int64]]]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tuple({}).Get()),
        "[TupleType; []]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tuple({{f.Bool()}}).Get()),
        "[TupleType; [[DataType; Bool]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tuple({{f.Yson()}, {f.Bool()}}).Get()),
        "[TupleType; [[DataType; Yson]; [DataType; Bool]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tuple({{f.Int32()}, {f.Int32()}, {f.Int64()}}).Get()),
        "[TupleType; [[DataType; Int32]; [DataType; Int32]; [DataType; Int64]]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Struct({{"a", f.Bool()}})).Get()),
        "[VariantType; [StructType; [[a; [DataType; Bool]]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Struct({{"a", f.Yson()}, {"b", f.Bool()}})).Get()),
        "[VariantType; [StructType; [[a; [DataType; Yson]]; [b; [DataType; Bool]]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Struct({{"a", f.Int32()}, {"b", f.Int32()}, {"c", f.Int64()}})).Get()),
        "[VariantType; [StructType; [[a; [DataType; Int32]]; [b; [DataType; Int32]]; [c; [DataType; Int64]]]]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Tuple({{f.Bool()}})).Get()),
        "[VariantType; [TupleType; [[DataType; Bool]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Tuple({{f.Yson()}, {f.Bool()}})).Get()),
        "[VariantType; [TupleType; [[DataType; Yson]; [DataType; Bool]]]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Variant(f.Tuple({{f.Int32()}, {f.Int32()}, {f.Int32()}})).Get()),
        "[VariantType; [TupleType; [[DataType; Int32]; [DataType; Int32]; [DataType; Int32]]]]");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tagged(f.String(), "Url").Get()),
        "[TaggedType; Url; [DataType; String]]");
    ASSERT_YSON_EQ(
        NTi::NIo::AsYqlType(f.Tagged(f.String(), "Url").Get(), /* includeTags = */ false),
        "[DataType; String]");
}

TEST_TF(TypeIO, AsYqlRowSpec) {
    {
        auto type = f.Struct({});
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; []]}");
    }
    {
        auto type = f.Tagged(f.Struct({}), "Event");
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; []]}");
    }
    {
        auto type = f.Struct({{"x", f.Tagged(f.String(), "Url")}});
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; [[x; [TaggedType; Url; [DataType; String]]]]]}");
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get(), /* includeTags = */ false),
            "{StrictSchema=%true; Type=[StructType; [[x; [DataType; String]]]]}");
    }
    {
        auto type = f.Struct({{"a", f.Bool()}});
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; [[a; [DataType; Bool]]]]}");
    }
    {
        auto type = f.Struct({{"a", f.Yson()}, {"b", f.Bool()}});
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; [[a; [DataType; Yson]]; [b; [DataType; Bool]]]]}");
    }
    {
        auto type = f.Struct({{"a", f.Int32()}, {"b", f.Int32()}, {"c", f.Int32()}});
        ASSERT_YSON_EQ(
            NTi::NIo::AsYqlRowSpec(type.Get()),
            "{StrictSchema=%true; Type=[StructType; [[a; [DataType; Int32]]; [b; [DataType; Int32]]; [c; [DataType; Int32]]]]}");
    }

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        NTi::NIo::AsYqlRowSpec(f.Void().Get());
    }(),
                                   NTi::TApiException, "expected a struct type");

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        NTi::NIo::AsYqlRowSpec(Optional(f.Struct({})).Get());
    }(),
                                   NTi::TApiException, "expected a struct type");
}

TEST_TF(TypeIO, AsYtSchema) {
    {
        auto type = f.Struct({
            {"void", f.Void()},
            {"null", f.Null()},
            {"bool", f.Bool()},
            {"int8", f.Int8()},
            {"int16", f.Int16()},
            {"int32", f.Int32()},
            {"int64", f.Int64()},
            {"uint8", f.Uint8()},
            {"uint16", f.Uint16()},
            {"uint32", f.Uint32()},
            {"uint64", f.Uint64()},
            {"float", f.Float()},
            {"double", f.Double()},
            {"string", f.String()},
            {"utf8", f.Utf8()},
            {"date", f.Date()},
            {"datetime", f.Datetime()},
            {"timestamp", f.Timestamp()},
            {"tzdate", f.TzDate()},
            {"tzdatetime", f.TzDatetime()},
            {"tztimestamp", f.TzTimestamp()},
            {"interval", f.Interval()},
            {"decimal", f.Decimal(20, 10)},
            {"json", f.Json()},
            {"yson", f.Yson()},
            {"uuid", f.Uuid()},
            {"list", f.List(f.Bool())},
            {"dict", f.Dict(f.Bool(), f.Bool())},
            {"struct", f.Struct({})},
            {"tuple", f.Tuple({})},
            {"variant", f.Variant(f.Struct({{"x", f.Bool()}}))},
        });

        ASSERT_YSON_EQ(
            NTi::NIo::AsYtSchema(type.Get()),
            R"(
            <strict=%true; unique_keys=%false>
            [
                {name=void;             required=%false;    type=any        };
                {name=null;             required=%false;    type=any        };
                {name=bool;             required=%true;     type=boolean    };
                {name=int8;             required=%true;     type=int8       };
                {name=int16;            required=%true;     type=int16      };
                {name=int32;            required=%true;     type=int32      };
                {name=int64;            required=%true;     type=int64      };
                {name=uint8;            required=%true;     type=uint8      };
                {name=uint16;           required=%true;     type=uint16     };
                {name=uint32;           required=%true;     type=uint32     };
                {name=uint64;           required=%true;     type=uint64     };
                {name=float;            required=%true;     type=double     };
                {name=double;           required=%true;     type=double     };
                {name=string;           required=%true;     type=string     };
                {name=utf8;             required=%true;     type=utf8       };
                {name=date;             required=%true;     type=uint16     };
                {name=datetime;         required=%true;     type=uint32     };
                {name=timestamp;        required=%true;     type=uint64     };
                {name=tzdate;           required=%true;     type=string     };
                {name=tzdatetime;       required=%true;     type=string     };
                {name=tztimestamp;      required=%true;     type=string     };
                {name=interval;         required=%true;     type=int64      };
                {name=decimal;          required=%true;     type=string     };
                {name=json;             required=%true;     type=string     };
                {name=yson;             required=%false;    type=any        };
                {name=uuid;             required=%true;     type=string     };
                {name=list;             required=%false;    type=any        };
                {name=dict;             required=%false;    type=any        };
                {name=struct;           required=%false;    type=any        };
                {name=tuple;            required=%false;    type=any        };
                {name=variant;          required=%false;    type=any        };
            ]
            )");
    }

    {
        auto type = f.Struct({
            {"void", f.Tagged(f.Void(), "Tag")},
            {"null", f.Tagged(f.Null(), "Tag")},
            {"bool", f.Tagged(f.Bool(), "Tag")},
            {"int8", f.Tagged(f.Int8(), "Tag")},
            {"int16", f.Tagged(f.Int16(), "Tag")},
            {"int32", f.Tagged(f.Int32(), "Tag")},
            {"int64", f.Tagged(f.Int64(), "Tag")},
            {"uint8", f.Tagged(f.Uint8(), "Tag")},
            {"uint16", f.Tagged(f.Uint16(), "Tag")},
            {"uint32", f.Tagged(f.Uint32(), "Tag")},
            {"uint64", f.Tagged(f.Uint64(), "Tag")},
            {"float", f.Tagged(f.Float(), "Tag")},
            {"double", f.Tagged(f.Double(), "Tag")},
            {"string", f.Tagged(f.String(), "Tag")},
            {"utf8", f.Tagged(f.Utf8(), "Tag")},
            {"date", f.Tagged(f.Date(), "Tag")},
            {"datetime", f.Tagged(f.Datetime(), "Tag")},
            {"timestamp", f.Tagged(f.Timestamp(), "Tag")},
            {"tzdate", f.Tagged(f.TzDate(), "Tag")},
            {"tzdatetime", f.Tagged(f.TzDatetime(), "Tag")},
            {"tztimestamp", f.Tagged(f.TzTimestamp(), "Tag")},
            {"interval", f.Tagged(f.Interval(), "Tag")},
            {"decimal", f.Tagged(f.Decimal(20, 10), "Tag")},
            {"json", f.Tagged(f.Json(), "Tag")},
            {"yson", f.Tagged(f.Yson(), "Tag")},
            {"uuid", f.Tagged(f.Uuid(), "Tag")},
            {"list", f.Tagged(f.List(f.Bool()), "Tag")},
            {"dict", f.Tagged(f.Dict(f.Bool(), f.Bool()), "Tag")},
            {"struct", f.Tagged(f.Struct({}), "Tag")},
            {"tuple", f.Tagged(f.Tuple({}), "Tag")},
            {"variant", f.Tagged(f.Variant(f.Struct({{"x", f.Bool()}})), "Tag")},
        });

        ASSERT_YSON_EQ(
            NTi::NIo::AsYtSchema(type.Get()),
            R"(
            <strict=%true; unique_keys=%false>
            [
                {name=void;             required=%false;    type=any        };
                {name=null;             required=%false;    type=any        };
                {name=bool;             required=%true;     type=boolean    };
                {name=int8;             required=%true;     type=int8       };
                {name=int16;            required=%true;     type=int16      };
                {name=int32;            required=%true;     type=int32      };
                {name=int64;            required=%true;     type=int64      };
                {name=uint8;            required=%true;     type=uint8      };
                {name=uint16;           required=%true;     type=uint16     };
                {name=uint32;           required=%true;     type=uint32     };
                {name=uint64;           required=%true;     type=uint64     };
                {name=float;            required=%true;     type=double     };
                {name=double;           required=%true;     type=double     };
                {name=string;           required=%true;     type=string     };
                {name=utf8;             required=%true;     type=utf8       };
                {name=date;             required=%true;     type=uint16     };
                {name=datetime;         required=%true;     type=uint32     };
                {name=timestamp;        required=%true;     type=uint64     };
                {name=tzdate;           required=%true;     type=string     };
                {name=tzdatetime;       required=%true;     type=string     };
                {name=tztimestamp;      required=%true;     type=string     };
                {name=interval;         required=%true;     type=int64      };
                {name=decimal;          required=%true;     type=string     };
                {name=json;             required=%true;     type=string     };
                {name=yson;             required=%false;    type=any        };
                {name=uuid;             required=%true;     type=string     };
                {name=list;             required=%false;    type=any        };
                {name=dict;             required=%false;    type=any        };
                {name=struct;           required=%false;    type=any        };
                {name=tuple;            required=%false;    type=any        };
                {name=variant;          required=%false;    type=any        };
            ]
            )");
    }

    {
        auto type = f.Struct({
            {"void", Optional(f.Void())},
            {"null", Optional(f.Null())},
            {"bool", Optional(f.Bool())},
            {"int8", Optional(f.Int8())},
            {"int16", Optional(f.Int16())},
            {"int32", Optional(f.Int32())},
            {"int64", Optional(f.Int64())},
            {"uint8", Optional(f.Uint8())},
            {"uint16", Optional(f.Uint16())},
            {"uint32", Optional(f.Uint32())},
            {"uint64", Optional(f.Uint64())},
            {"float", Optional(f.Float())},
            {"double", Optional(f.Double())},
            {"string", Optional(f.String())},
            {"utf8", Optional(f.Utf8())},
            {"date", Optional(f.Date())},
            {"datetime", Optional(f.Datetime())},
            {"timestamp", Optional(f.Timestamp())},
            {"tzdate", Optional(f.TzDate())},
            {"tzdatetime", Optional(f.TzDatetime())},
            {"tztimestamp", Optional(f.TzTimestamp())},
            {"interval", Optional(f.Interval())},
            {"decimal", Optional(f.Decimal(20, 10))},
            {"json", Optional(f.Json())},
            {"yson", Optional(f.Yson())},
            {"uuid", Optional(f.Uuid())},
            {"list", Optional(f.List(f.Bool()))},
            {"dict", Optional(f.Dict(f.Bool(), f.Bool()))},
            {"struct", Optional(f.Struct({}))},
            {"tuple", Optional(f.Tuple({}))},
            {"variant", Optional(f.Variant(f.Struct({{"x", f.Bool()}})))},
        });

        ASSERT_YSON_EQ(
            NTi::NIo::AsYtSchema(type.Get()),
            R"(
            <strict=%true; unique_keys=%false>
            [
                {name=void;             required=%false;    type=any        };
                {name=null;             required=%false;    type=any        };
                {name=bool;             required=%false;    type=boolean    };
                {name=int8;             required=%false;    type=int8       };
                {name=int16;            required=%false;    type=int16      };
                {name=int32;            required=%false;    type=int32      };
                {name=int64;            required=%false;    type=int64      };
                {name=uint8;            required=%false;    type=uint8      };
                {name=uint16;           required=%false;    type=uint16     };
                {name=uint32;           required=%false;    type=uint32     };
                {name=uint64;           required=%false;    type=uint64     };
                {name=float;            required=%false;    type=double     };
                {name=double;           required=%false;    type=double     };
                {name=string;           required=%false;    type=string     };
                {name=utf8;             required=%false;    type=utf8       };
                {name=date;             required=%false;    type=uint16     };
                {name=datetime;         required=%false;    type=uint32     };
                {name=timestamp;        required=%false;    type=uint64     };
                {name=tzdate;           required=%false;    type=string     };
                {name=tzdatetime;       required=%false;    type=string     };
                {name=tztimestamp;      required=%false;    type=string     };
                {name=interval;         required=%false;    type=int64      };
                {name=decimal;          required=%false;    type=string     };
                {name=json;             required=%false;    type=string     };
                {name=yson;             required=%false;    type=any        };
                {name=uuid;             required=%false;    type=string     };
                {name=list;             required=%false;    type=any        };
                {name=dict;             required=%false;    type=any        };
                {name=struct;           required=%false;    type=any        };
                {name=tuple;            required=%false;    type=any        };
                {name=variant;          required=%false;    type=any        };
            ]
            )");
    }

    {
        auto type = f.Struct({
            {"void", Optional(Optional(f.Void()))},
            {"null", Optional(Optional(f.Null()))},
            {"bool", Optional(Optional(f.Bool()))},
            {"int8", Optional(Optional(f.Int8()))},
            {"int16", Optional(Optional(f.Int16()))},
            {"int32", Optional(Optional(f.Int32()))},
            {"int64", Optional(Optional(f.Int64()))},
            {"uint8", Optional(Optional(f.Uint8()))},
            {"uint16", Optional(Optional(f.Uint16()))},
            {"uint32", Optional(Optional(f.Uint32()))},
            {"uint64", Optional(Optional(f.Uint64()))},
            {"float", Optional(Optional(f.Float()))},
            {"double", Optional(Optional(f.Double()))},
            {"string", Optional(Optional(f.String()))},
            {"utf8", Optional(Optional(f.Utf8()))},
            {"date", Optional(Optional(f.Date()))},
            {"datetime", Optional(Optional(f.Datetime()))},
            {"timestamp", Optional(Optional(f.Timestamp()))},
            {"tzdate", Optional(Optional(f.TzDate()))},
            {"tzdatetime", Optional(Optional(f.TzDatetime()))},
            {"tztimestamp", Optional(Optional(f.TzTimestamp()))},
            {"interval", Optional(Optional(f.Interval()))},
            {"decimal", Optional(Optional(f.Decimal(20, 10)))},
            {"json", Optional(Optional(f.Json()))},
            {"yson", Optional(Optional(f.Yson()))},
            {"uuid", Optional(Optional(f.Uuid()))},
            {"list", Optional(Optional(f.List(f.Bool())))},
            {"dict", Optional(Optional(f.Dict(f.Bool(), f.Bool())))},
            {"struct", Optional(Optional(f.Struct({})))},
            {"tuple", Optional(Optional(f.Tuple({})))},
            {"variant", Optional(Optional(f.Variant(f.Struct({{"x", f.Bool()}}))))},
        });

        ASSERT_YSON_EQ(
            NTi::NIo::AsYtSchema(type.Get()),
            R"(
            <strict=%true; unique_keys=%false>
            [
                {name=void;             required=%false;    type=any        };
                {name=null;             required=%false;    type=any        };
                {name=bool;             required=%false;    type=any        };
                {name=int8;             required=%false;    type=any        };
                {name=int16;            required=%false;    type=any        };
                {name=int32;            required=%false;    type=any        };
                {name=int64;            required=%false;    type=any        };
                {name=uint8;            required=%false;    type=any        };
                {name=uint16;           required=%false;    type=any        };
                {name=uint32;           required=%false;    type=any        };
                {name=uint64;           required=%false;    type=any        };
                {name=float;            required=%false;    type=any        };
                {name=double;           required=%false;    type=any        };
                {name=string;           required=%false;    type=any        };
                {name=utf8;             required=%false;    type=any        };
                {name=date;             required=%false;    type=any        };
                {name=datetime;         required=%false;    type=any        };
                {name=timestamp;        required=%false;    type=any        };
                {name=tzdate;           required=%false;    type=any        };
                {name=tzdatetime;       required=%false;    type=any        };
                {name=tztimestamp;      required=%false;    type=any        };
                {name=interval;         required=%false;    type=any        };
                {name=decimal;          required=%false;    type=any        };
                {name=json;             required=%false;    type=any        };
                {name=yson;             required=%false;    type=any        };
                {name=uuid;             required=%false;    type=any        };
                {name=list;             required=%false;    type=any        };
                {name=dict;             required=%false;    type=any        };
                {name=struct;           required=%false;    type=any        };
                {name=tuple;            required=%false;    type=any        };
                {name=variant;          required=%false;    type=any        };
            ]
            )");
    }

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        NTi::NIo::AsYtSchema(f.Void().Get());
    }(),
                                   NTi::TApiException, "expected a struct type");

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        NTi::NIo::AsYtSchema(Optional(f.Struct({})).Get());
    }(),
                                   NTi::TApiException, "expected a struct type");

    UNIT_ASSERT_EXCEPTION_CONTAINS([&f]() {
        NTi::NIo::AsYtSchema(f.Struct({}).Get());
    }(),
                                   NTi::TApiException, "expected a non-empty struct");

    ASSERT_YSON_EQ(
        NTi::NIo::AsYtSchema(f.Struct({}).Get(), /* failOnEmptyf.Struct = */ false),
        "<strict=%true; unique_keys=%false>[{name=_yql_fake_column; required=%false; type=boolean}]");
}
