#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <ydb/library/yql/public/purecalc/ut/lib/helpers.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/stream/str.h>

#include <library/cpp/skiff/skiff.h>

#include <util/generic/yexception.h>


namespace {
    TStringStream GetYsonStream(
        const TVector<TString>& fields,
        const TVector<TString>& optionalFields={},
        ui32 start = 0, ui32 stop = 5, ui32 step = 1
    ) {
        THashSet<TString> filter {fields.begin(), fields.end()};
        THashSet<TString> optionalFilter {optionalFields.begin(), optionalFields.end()};

        auto addField = [&] (
            NYT::TNode& node, const TString& field, NYT::TNode&& value
        ) {
            if (filter.contains(field) && !optionalFilter.contains(field)) {
                node(field, value);
            }
        };

        TStringStream stream;
        NYson::TYsonWriter writer(&stream, NYson::EYsonFormat::Binary, NYson::EYsonType::ListFragment);
        NYT::TNodeVisitor visitor(&writer);

        for (ui32 i = start; i < stop; i += step) {
            auto item = NYT::TNode::CreateMap();

            addField(item, "int64", (i64)(i));
            addField(item, "uint64", (ui64)(i * 2));
            addField(item, "double", (double)(i * 3.5));
            addField(item, "bool", true);
            addField(item, "string", "foo");
            addField(item, "yson", (i % 2 == 0 ? NYT::TNode(true) : NYT::TNode(false)));

            visitor.Visit(item);
        }

        return stream;
    }

    TStringStream GetMultitableYsonStream(
        const TVector<TVector<int>>& groupedValues,
        const TVector<TString>& etalonTableNames = {}
    ) {
        bool isEtalon = !etalonTableNames.empty();

        Y_ENSURE(!isEtalon || groupedValues.size() == etalonTableNames.size());

        TStringStream stream;
        NYson::TYsonWriter writer(&stream, NYson::EYsonFormat::Binary, NYson::EYsonType::ListFragment);
        NYT::TNodeVisitor visitor(&writer);

        for (ui64 tableIndex = 0; tableIndex < groupedValues.size(); ++tableIndex) {
            if (!isEtalon) {
                auto indexNode = NYT::TNode::CreateEntity();
                indexNode.Attributes() = NYT::TNode::CreateMap()("table_index", static_cast<i64>(tableIndex));
                visitor.Visit(indexNode);
            }

            const auto& values = groupedValues[tableIndex];

            for (ui64 i = 0; i < values.size(); ++i) {
                auto item = NYT::TNode::CreateMap()("int64", values[i]);
                if (isEtalon) {
                    item("tname", etalonTableNames[tableIndex]);
                }
                visitor.Visit(item);
            }
        }

        return stream;
    }

    void AssertEqualYsonStreams(TStringStream etalonStream, TStringStream stream) {
        NYT::TNode etalonList {
            NYT::NodeFromYsonStream(&etalonStream, NYson::EYsonType::ListFragment)
        };

        NYT::TNode list {
            NYT::NodeFromYsonStream(&stream, NYson::EYsonType::ListFragment)
        };

        UNIT_ASSERT_EQUAL(etalonList, list);
    }

    TStringStream GetSkiffStream(
        const TVector<TString>& fields,
        const TVector<TString>& optionalFields={},
        ui32 start = 0, ui32 stop = 5, ui32 step = 1
    ) {
        THashSet<TString> filter {fields.begin(), fields.end()};
        THashSet<TString> optionalFilter {optionalFields.begin(), optionalFields.end()};

        TStringStream stream;
        NSkiff::TUncheckedSkiffWriter writer {&stream};

#define WRITE_FIELD(field, type, value) \
    do { \
        if (filter.contains(field)) { \
            if (optionalFilter.contains(field)) { \
                writer.WriteVariant8Tag(0); \
            } else { \
                writer.Write ## type(value); \
            } \
        } \
    } while (0)

        for (ui32 i = start; i < stop; i += step) {
            auto item = NYT::TNode::CreateMap();

            writer.WriteVariant16Tag(0);
            WRITE_FIELD("bool", Boolean, true);
            WRITE_FIELD("double", Double, (double)(i * 3.5));
            WRITE_FIELD("int64", Int64, (i64)(i));
            WRITE_FIELD("string", String32, "foo");
            WRITE_FIELD("uint64", Uint64, (ui64)(i * 2));
            WRITE_FIELD("yson", Yson32, (i % 2 == 0 ? "\x05" : "\x04"));  // boolean values
        }

#undef WRITE_FIELD

        return stream;
    }

    TStringStream GetMultitableSkiffStream(
        const TVector<TVector<int>>& groupedValues,
        const TVector<TString>& etalonTableNames = {}
    ) {
        bool isEtalon = !etalonTableNames.empty();

        Y_ENSURE(!isEtalon || groupedValues.size() == etalonTableNames.size());

        TStringStream stream;
        NSkiff::TUncheckedSkiffWriter writer {&stream};

        for (ui64 tableIndex = 0; tableIndex < groupedValues.size(); ++tableIndex) {
            const auto& values = groupedValues[tableIndex];

            for (ui64 i = 0; i < values.size(); ++i) {
                if (isEtalon) {
                    writer.WriteVariant16Tag(0);
                } else {
                    writer.WriteVariant16Tag(tableIndex);
                }

                writer.WriteInt64(values[i]);
                if (isEtalon) {
                    writer.WriteString32(etalonTableNames[tableIndex]);
                }
            }
        }

        return stream;
    }

    NYT::TNode GetSkiffSchemaWithStruct(bool sorted) {
        auto aMember = NYT::TNode::CreateList()
            .Add("a")
            .Add(NYT::TNode::CreateList().Add("DataType").Add("String"));

        auto bMember = NYT::TNode::CreateList()
            .Add("b")
            .Add(NYT::TNode::CreateList().Add("DataType").Add("Uint64"));

        auto members = NYT::TNode::CreateList();

        if (sorted) {
            members.Add(std::move(aMember)).Add(std::move(bMember));
        } else {
            members.Add(std::move(bMember)).Add(std::move(aMember));
        }

        auto structColumn = NYT::TNode::CreateList()
            .Add("Struct")
            .Add(NYT::TNode::CreateList().Add("StructType").Add(std::move(members)));

        auto indexColumn = NYT::TNode::CreateList()
            .Add("Index")
            .Add(NYT::TNode::CreateList().Add("DataType").Add("Uint64"));

        auto schema = NYT::TNode::CreateList()
            .Add("StructType")
            .Add(NYT::TNode::CreateList().Add(std::move(indexColumn)).Add(std::move(structColumn)));

        return schema;
    }

    TStringStream GetSkiffStreamWithStruct(bool sorted, ui32 start = 0, ui32 stop = 5) {
        TStringStream stream;
        NSkiff::TUncheckedSkiffWriter writer {&stream};

        auto writeStructMembers = [sorted, &writer](TStringBuf stringMember, ui64 numberMember) {
            if (sorted) {
                writer.WriteString32(stringMember);
                writer.WriteUint64(numberMember);
            } else {
                writer.WriteUint64(numberMember);
                writer.WriteString32(stringMember);
            }
        };

        for (ui32 idx = start; idx < stop; ++idx) {
            auto stringData = TStringBuilder{} << "text" << idx;
            writer.WriteVariant16Tag(0);
            writer.WriteUint64(idx);
            writeStructMembers(stringData, idx + 3);
        }

        return stream;
    }

    void AssertEqualSkiffStreams(TStringStream etalonStream, TStringStream stream) {
        UNIT_ASSERT_VALUES_EQUAL(etalonStream.Str(), stream.Str());
    }
}

template <typename T>
TVector<T> JoinVectors(const TVector<T>& first, const TVector<T>& second) {
    TVector<T> result;
    result.reserve(first.size() + second.size());

    result.insert(result.end(), first.begin(), first.end());
    result.insert(result.end(), second.begin(), second.end());

    return result;
}

#define PULL_STREAM_MODE
#define TEST_SUITE_NAME TestPullStreamYsonIO
#define CREATE_PROGRAM(...) factory->MakePullStreamProgram(__VA_ARGS__)
#define INPUT_SPEC TYsonInputSpec
#define OUTPUT_SPEC TYsonOutputSpec
#define GET_STREAM GetYsonStream
#define GET_MULTITABLE_STREAM GetMultitableYsonStream
#define ASSERT_EQUAL_STREAMS AssertEqualYsonStreams
#include "test.inl"
#undef ASSERT_EQUAL_STREAMS
#undef GET_MULTITABLE_STREAM
#undef GET_STREAM
#undef OUTPUT_SPEC
#undef INPUT_SPEC
#undef CREATE_PROGRAM
#undef TEST_SUITE_NAME
#undef PULL_STREAM_MODE

#define PULL_STREAM_MODE
#define TEST_SUITE_NAME TestPullStreamSkiffIO
#define CREATE_PROGRAM(...) factory->MakePullStreamProgram(__VA_ARGS__)
#define INPUT_SPEC TSkiffInputSpec
#define OUTPUT_SPEC TSkiffOutputSpec
#define GET_STREAM GetSkiffStream
#define GET_STREAM_WITH_STRUCT GetSkiffStreamWithStruct
#define GET_SCHEMA_WITH_STRUCT GetSkiffSchemaWithStruct
#define GET_MULTITABLE_STREAM GetMultitableSkiffStream
#define ASSERT_EQUAL_STREAMS AssertEqualSkiffStreams
#include "test.inl"
#undef ASSERT_EQUAL_STREAMS
#undef GET_MULTITABLE_STREAM
#undef GET_SCHEMA_WITH_STRUCT
#undef GET_STREAM_WITH_STRUCT
#undef GET_STREAM
#undef OUTPUT_SPEC
#undef INPUT_SPEC
#undef CREATE_PROGRAM
#undef TEST_SUITE_NAME
#undef PULL_STREAM_MODE

#define PULL_LIST_MODE
#define TEST_SUITE_NAME TestPullListYsonIO
#define CREATE_PROGRAM(...) factory->MakePullListProgram(__VA_ARGS__)
#define INPUT_SPEC TYsonInputSpec
#define OUTPUT_SPEC TYsonOutputSpec
#define GET_STREAM GetYsonStream
#define GET_MULTITABLE_STREAM GetMultitableYsonStream
#define ASSERT_EQUAL_STREAMS AssertEqualYsonStreams
#include "test.inl"
#undef ASSERT_EQUAL_STREAMS
#undef GET_MULTITABLE_STREAM
#undef GET_STREAM
#undef OUTPUT_SPEC
#undef INPUT_SPEC
#undef CREATE_PROGRAM
#undef TEST_SUITE_NAME
#undef PULL_LIST_MODE

#define PULL_LIST_MODE
#define TEST_SUITE_NAME TestPullListSkiffIO
#define CREATE_PROGRAM(...) factory->MakePullListProgram(__VA_ARGS__)
#define INPUT_SPEC TSkiffInputSpec
#define OUTPUT_SPEC TSkiffOutputSpec
#define GET_STREAM GetSkiffStream
#define GET_STREAM_WITH_STRUCT GetSkiffStreamWithStruct
#define GET_SCHEMA_WITH_STRUCT GetSkiffSchemaWithStruct
#define GET_MULTITABLE_STREAM GetMultitableSkiffStream
#define ASSERT_EQUAL_STREAMS AssertEqualSkiffStreams
#include "test.inl"
#undef ASSERT_EQUAL_STREAMS
#undef GET_MULTITABLE_STREAM
#undef GET_SCHEMA_WITH_STRUCT
#undef GET_STREAM_WITH_STRUCT
#undef GET_STREAM
#undef OUTPUT_SPEC
#undef INPUT_SPEC
#undef CREATE_PROGRAM
#undef TEST_SUITE_NAME
#undef PULL_LIST_MODE
