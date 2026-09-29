#include "bounded_response.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace NYdbGrpc::inline Dev {
    namespace {

        using TField = google::protobuf::FieldDescriptor;

        bool ReadVarint(std::string_view& wire, uint64_t& value) {
            value = 0;
            for (unsigned shift = 0; shift < 64; shift += 7) {
                if (wire.empty()) {
                    return false;
                }
                const auto byte = static_cast<unsigned char>(wire.front());
                wire.remove_prefix(1);
                if (shift == 63 && byte > 1) {
                    return false;
                }
                value |= static_cast<uint64_t>(byte & 0x7f) << shift;
                if (!(byte & 0x80)) {
                    return true;
                }
            }
            return false;
        }

        unsigned ScalarWireType(const TField* field) {
            switch (field->type()) {
                case TField::TYPE_DOUBLE:
                case TField::TYPE_FIXED64:
                case TField::TYPE_SFIXED64:
                    return 1;
                case TField::TYPE_STRING:
                case TField::TYPE_BYTES:
                case TField::TYPE_MESSAGE:
                    return 2;
                case TField::TYPE_FLOAT:
                case TField::TYPE_FIXED32:
                case TField::TYPE_SFIXED32:
                    return 5;
                case TField::TYPE_GROUP:
                    return 3;
                default:
                    return 0;
            }
        }

        bool SkipScalar(std::string_view& wire, unsigned type) {
            uint64_t value = 0;
            if (type == 0) {
                return ReadVarint(wire, value);
            }
            const size_t size = type == 1 ? 8 : type == 5 ? 4
                                                          : 0;
            if (!size || wire.size() < size) {
                return false;
            }
            wire.remove_prefix(size);
            return true;
        }

        class TValidator {
        public:
            explicit TValidator(size_t maxFields)
                : MaxFields_(maxFields)
            {
            }

            bool Validate(std::string_view wire, const google::protobuf::Descriptor* descriptor, size_t depth = 0) {
                if (!descriptor || depth >= TBoundedResponseLimits::Depth) {
                    return false;
                }
                const auto* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
                if (!prototype || prototype->SpaceUsedLong() > TBoundedResponseLimits::MessageBytes) {
                    return false;
                }
                const bool isAny = descriptor->full_name() == "google.protobuf.Any";
                std::string_view anyType;
                std::string_view anyValue;
                std::vector<int> singularMessages;
                while (!wire.empty()) {
                    if (++Fields_ > MaxFields_) {
                        return false;
                    }
                    uint64_t tag = 0;
                    if (!ReadVarint(wire, tag) || tag > UINT32_MAX || !(tag >> 3)) {
                        return false;
                    }
                    const auto* field = descriptor->FindFieldByNumber(static_cast<int>(tag >> 3));
                    if (!field) {
                        return false; // Never materialize unknown-field storage.
                    }
                    const unsigned wireType = static_cast<unsigned>(tag & 7);
                    const unsigned expectedType = ScalarWireType(field);
                    const bool packed = field->is_repeated() && field->is_packable() && wireType == 2;
                    if ((!packed && wireType != expectedType) || expectedType == 3) {
                        return false;
                    }
                    if (wireType != 2) {
                        if (!SkipScalar(wire, wireType)) {
                            return false;
                        }
                        continue;
                    }
                    uint64_t size = 0;
                    if (!ReadVarint(wire, size) || size > wire.size()) {
                        return false;
                    }
                    auto value = wire.substr(0, static_cast<size_t>(size));
                    wire.remove_prefix(static_cast<size_t>(size));
                    if (packed) {
                        while (!value.empty()) {
                            if (++Fields_ > MaxFields_ || !SkipScalar(value, expectedType)) {
                                return false;
                            }
                        }
                    } else if (field->cpp_type() == TField::CPPTYPE_MESSAGE) {
                        // Protobuf merges duplicate singular messages, including Any fragments.
                        // Reject them so each Any is checked with its complete type and payload.
                        if (!field->is_repeated()) {
                            if (std::find(singularMessages.begin(), singularMessages.end(), field->number()) != singularMessages.end()) {
                                return false;
                            }
                            singularMessages.push_back(field->number());
                        }
                        if (!Validate(value, field->message_type(), depth + 1)) {
                            return false;
                        }
                    } else if (isAny) {
                        auto& target = field->number() == 1 ? anyType : anyValue;
                        if (target.data()) {
                            return false;
                        }
                        target = value;
                    }
                }
                if (isAny && (!anyType.empty() || !anyValue.empty())) {
                    if (anyType.size() > 256 || anyType.empty()) {
                        return false;
                    }
                    const auto slash = anyType.rfind('/');
                    const auto name = anyType.substr(slash == std::string_view::npos ? 0 : slash + 1);
                    // Only results used by bounded table metadata requests are accepted.
                    if (name != "Ydb.Table.CreateSessionResult" && name != "Ydb.Table.DescribeTableResult") {
                        return false;
                    }
                    const auto* payload = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(
                        NYdb::TStringType(name.data(), name.size()));
                    return Validate(anyValue, payload, depth + 1);
                }
                return true;
            }

        private:
            size_t Fields_ = 0;
            const size_t MaxFields_;
        };

    } // namespace

    bool ValidateBoundedProtobuf(std::string_view wire, const google::protobuf::Descriptor* descriptor, size_t maxBytes) {
        const auto fields = maxBytes <= TBoundedResponseLimits::UnaryBytes
                                ? TBoundedResponseLimits::UnaryFields
                                : TBoundedResponseLimits::Fields;
        return wire.size() <= std::min(maxBytes, TBoundedResponseLimits::StreamBytes) && TValidator(fields).Validate(wire, descriptor);
    }

    grpc::Status DeserializeBoundedProtobuf(grpc::ByteBuffer* buffer, google::protobuf::Message* message, size_t maxBytes) {
        if (!buffer || !message) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Missing bounded response");
        }
        maxBytes = std::min(maxBytes, TBoundedResponseLimits::StreamBytes);
        if (buffer->Length() > maxBytes) {
            buffer->Clear();
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Bounded response byte limit exceeded");
        }
        grpc::Slice slice;
        const auto status = buffer->DumpToSingleSlice(&slice);
        buffer->Clear();
        if (!status.ok()) {
            return status;
        }
        const std::string_view wire(reinterpret_cast<const char*>(slice.begin()), slice.size());
        if (!ValidateBoundedProtobuf(wire, message->GetDescriptor(), maxBytes)) {
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Bounded response structure limit exceeded");
        }
        if (!message->ParseFromArray(wire.data(), static_cast<int>(wire.size()))) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Invalid bounded protobuf response");
        }
        return grpc::Status::OK;
    }

} // namespace NYdbGrpc::inline Dev
