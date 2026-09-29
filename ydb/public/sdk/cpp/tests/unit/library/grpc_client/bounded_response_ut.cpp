#include <ydb/public/sdk/cpp/src/library/grpc/client/bounded_response.h>

#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/descriptor.pb.h>

#include <string>

namespace {

    using NYdbGrpc::TBoundedResponseLimits;
    using NYdbGrpc::ValidateBoundedProtobuf;

    std::string Wire(const google::protobuf::Message& message) {
        const auto wire = message.SerializeAsString();
        return std::string(wire.data(), wire.size());
    }

    void AppendVarint(std::string& wire, uint64_t value) {
        while (value >= 0x80) {
            wire.push_back(static_cast<char>((value & 0x7f) | 0x80));
            value >>= 7;
        }
        wire.push_back(static_cast<char>(value));
    }

    std::string LengthField(unsigned number, std::string_view value) {
        std::string wire;
        AppendVarint(wire, (number << 3) | 2);
        AppendVarint(wire, value.size());
        wire.append(value);
        return wire;
    }

} // namespace

Y_UNIT_TEST_SUITE(BoundedResponseTests) {
    Y_UNIT_TEST(AcceptsArrowPayloadAndMetadataAny) {
        Ydb::Query::ExecuteQueryResponsePart part;
        part.set_status(Ydb::StatusIds::SUCCESS);
        part.mutable_result_set()->set_data(std::string(1024 * 1024, 'a'));
        UNIT_ASSERT(ValidateBoundedProtobuf(Wire(part), part.GetDescriptor(), TBoundedResponseLimits::StreamBytes));

        Ydb::Table::DescribeTableResult table;
        auto* column = table.add_columns();
        column->set_name("value");
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
        Ydb::Table::DescribeTableResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->mutable_result()->PackFrom(table);
        UNIT_ASSERT(ValidateBoundedProtobuf(Wire(response), response.GetDescriptor(), TBoundedResponseLimits::UnaryBytes));
    }

    Y_UNIT_TEST(AcceptsMaximumMetadataProjection) {
        Ydb::Table::DescribeTableResult table;
        for (size_t i = 0; i < 1024; ++i) {
            auto* column = table.add_columns();
            column->set_name("column");
            column->mutable_type()->set_type_id(Ydb::Type::UTF8);
        }
        Ydb::Table::DescribeTableResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->mutable_result()->PackFrom(table);
        UNIT_ASSERT(ValidateBoundedProtobuf(Wire(response), response.GetDescriptor(), TBoundedResponseLimits::UnaryBytes));
    }

    Y_UNIT_TEST(RejectsRepeatedEmptyMessagesBeforeMaterialization) {
        Ydb::ResultSet result;
        const auto* columns = result.GetDescriptor()->FindFieldByName("columns");
        std::string wire;
        for (size_t i = 0; i <= TBoundedResponseLimits::Fields; ++i) {
            wire += LengthField(columns->number(), {});
        }
        UNIT_ASSERT(wire.size() < TBoundedResponseLimits::UnaryBytes);
        UNIT_ASSERT(!ValidateBoundedProtobuf(wire, result.GetDescriptor(), TBoundedResponseLimits::StreamBytes));
        grpc::Slice slice(wire.data(), wire.size());
        grpc::ByteBuffer buffer(&slice, 1);
        const auto status = NYdbGrpc::DeserializeBoundedProtobuf(&buffer, &result, TBoundedResponseLimits::StreamBytes);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(status.error_code()), static_cast<int>(grpc::StatusCode::RESOURCE_EXHAUSTED));
        UNIT_ASSERT_VALUES_EQUAL(result.columns_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Length(), 0);
    }

    Y_UNIT_TEST(RejectsUnknownFieldsGroupsAndMalformedLengths) {
        const auto* descriptor = Ydb::ResultSet::descriptor();
        UNIT_ASSERT(!ValidateBoundedProtobuf(LengthField(1000, "unknown"), descriptor, TBoundedResponseLimits::StreamBytes));
        UNIT_ASSERT(!ValidateBoundedProtobuf(std::string("\x0b\x0c", 2), descriptor, TBoundedResponseLimits::StreamBytes));
        UNIT_ASSERT(!ValidateBoundedProtobuf(std::string("\x0a\xff\xff\xff\xff\x7f", 6), descriptor, TBoundedResponseLimits::StreamBytes));
    }

    Y_UNIT_TEST(RejectsNestedAnyPayloadBeforeUnpack) {
        Ydb::Table::DescribeTableResult table;
        std::string payload;
        for (size_t i = 0; i <= TBoundedResponseLimits::UnaryFields; ++i) {
            payload += LengthField(2, {});
        }
        Ydb::Table::DescribeTableResponse response;
        auto* any = response.mutable_operation()->mutable_result();
        any->set_type_url("type.googleapis.com/Ydb.Table.DescribeTableResult");
        any->set_value(payload);
        UNIT_ASSERT(!ValidateBoundedProtobuf(Wire(response), response.GetDescriptor(), TBoundedResponseLimits::UnaryBytes));
        any->set_type_url("type.googleapis.com/Unsupported.Type");
        any->clear_value();
        UNIT_ASSERT(!ValidateBoundedProtobuf(Wire(response), response.GetDescriptor(), TBoundedResponseLimits::UnaryBytes));
    }

    Y_UNIT_TEST(CountsPackedScalarElementsBeforeAllocation) {
        const auto* descriptor = google::protobuf::FileDescriptorProto::descriptor();
        const auto* field = descriptor->FindFieldByName("public_dependency");
        const auto wire = LengthField(field->number(), std::string(TBoundedResponseLimits::Fields, '\0'));
        UNIT_ASSERT(!ValidateBoundedProtobuf(wire, descriptor, TBoundedResponseLimits::StreamBytes));
        UNIT_ASSERT(ValidateBoundedProtobuf(LengthField(field->number(), std::string_view("\0", 1)), descriptor,
                                            TBoundedResponseLimits::StreamBytes));
    }

    Y_UNIT_TEST(RejectsDeepTypesAndDuplicateSingularMessages) {
        Ydb::Type type;
        auto* nested = &type;
        for (size_t i = 0; i < TBoundedResponseLimits::Depth; ++i) {
            nested = nested->mutable_optional_type()->mutable_item();
        }
        nested->set_type_id(Ydb::Type::UTF8);
        UNIT_ASSERT(!ValidateBoundedProtobuf(Wire(type), type.GetDescriptor(), TBoundedResponseLimits::StreamBytes));
        const auto* optional = type.GetDescriptor()->FindFieldByName("optional_type");
        const auto field = LengthField(optional->number(), {});
        UNIT_ASSERT(!ValidateBoundedProtobuf(field + field, type.GetDescriptor(), TBoundedResponseLimits::StreamBytes));
    }

    Y_UNIT_TEST(EnforcesWireByteBoundary) {
        Ydb::Table::CreateSessionResult result;
        result.set_session_id(std::string(TBoundedResponseLimits::UnaryBytes, 's'));
        const auto wire = Wire(result);
        UNIT_ASSERT(!ValidateBoundedProtobuf(wire, result.GetDescriptor(), TBoundedResponseLimits::UnaryBytes));
        UNIT_ASSERT(ValidateBoundedProtobuf(wire, result.GetDescriptor(), wire.size()));
    }
} // Y_UNIT_TEST_SUITE(BoundedResponseTests)
