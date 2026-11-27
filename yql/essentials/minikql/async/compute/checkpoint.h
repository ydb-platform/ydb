#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/pack_num.h>
#include <util/stream/output.h>

namespace NKikimr::NMiniKQL::NAsync {

// Fixed-capacity output: reject BEFORE allocating/copying an oversized chunk.
// Allocated lazily, after the first size check. The save reservation covers this
// buffer, the packer's scratch space, graph assembly and the returned image.
class TSnapshotWriter final: public IOutputStream {
public:
    explicit TSnapshotWriter(ui64 limit)
        : Limit_(limit)
    {
    }

    ui64 Remaining() const {
        return Limit_ - Size_;
    }
    TStringBuf View() const {
        return {Data_.get(), Size_};
    }
    void Check(ui64 bytes) const {
        MKQL_ENSURE(bytes <= Remaining(), "Async snapshot exceeds size limit");
    }
    void Byte(ui8 value) {
        Write(&value, 1);
    }
    void Bool(bool value) {
        Byte(value);
    }
    void Ui32(ui32 value) {
        char bytes[MAX_PACKED32_SIZE];
        Write(bytes, Pack32(value, bytes));
    }
    void Ui64(ui64 value) {
        char bytes[MAX_PACKED64_SIZE];
        Write(bytes, Pack64(value, bytes));
    }
    void String(std::string_view value) {
        MKQL_ENSURE(value.size() <= Max<ui32>(), "Async snapshot exceeds size limit");
        char prefix[MAX_PACKED32_SIZE];
        const auto length = Pack32(value.size(), prefix);
        Check(ui64(length) + value.size());
        Write(prefix, length);
        Write(value.data(), value.size());
    }

    // Only the D2 scalar value domain is allowed here. Walk it without copying
    // payloads before invoking TValuePacker. 32 bytes per node conservatively
    // cover scalar encodings, the top header and optional-mask scratch space.
    // Every addition is checked, including nested/aggregate string lengths.
    static void CheckPackedValue(TType* type, const NUdf::TUnboxedValuePod& value, ui64& remaining) {
        auto charge = [&](ui64 bytes) {
            MKQL_ENSURE(bytes <= remaining, "Async snapshot exceeds size limit before value packing");
            remaining -= bytes;
        };
        charge(32);
        switch (type->GetKind()) {
            case TType::EKind::Data: {
                switch (*static_cast<TDataType*>(type)->GetDataSlot()) {
                    case NUdf::EDataSlot::String:
                    case NUdf::EDataSlot::Utf8:
                    case NUdf::EDataSlot::Yson:
                    case NUdf::EDataSlot::Json:
                    case NUdf::EDataSlot::JsonDocument:
                    case NUdf::EDataSlot::DyNumber:
                        charge(value.AsStringRef().Size());
                        break;
                    default:
                        break; // fixed-size scalar, at most Decimal/Uuid
                }
                return;
            }
            case TType::EKind::Optional:
                if (value) {
                    CheckPackedValue(static_cast<TOptionalType*>(type)->GetItemType(), value.GetOptionalValue(), remaining);
                }
                return;
            case TType::EKind::Tuple: {
                auto* tuple = static_cast<TTupleType*>(type);
                for (ui32 i = 0; i < tuple->GetElementsCount(); ++i) {
                    CheckPackedValue(tuple->GetElementType(i), value.GetElement(i), remaining);
                }
                return;
            }
            case TType::EKind::Struct: {
                auto* structure = static_cast<TStructType*>(type);
                for (ui32 i = 0; i < structure->GetMembersCount(); ++i) {
                    CheckPackedValue(structure->GetMemberType(i), value.GetElement(i), remaining);
                }
                return;
            }
            default:
                MKQL_ENSURE(false, "Unsupported async checkpoint value type");
        }
    }
    void Packed(TType* type, const NUdf::TUnboxedValuePod& value) {
        Check(MAX_PACKED32_SIZE);
        auto remaining = Remaining() - MAX_PACKED32_SIZE;
        CheckPackedValue(type, value, remaining);
        // The packer dies before the next slot/descriptor is serialized.
        TValuePacker packer(false, type);
        String(packer.Pack(value));
    }

private:
    void DoWrite(const void* data, size_t size) override {
        Check(size);
        if (!size) {
            return;
        }
        if (!Data_) {
            Data_.reset(new char[Limit_]);
        }
        memcpy(Data_.get() + Size_, data, size);
        Size_ += size;
    }

    const ui64 Limit_;
    ui64 Size_ = 0;
    std::unique_ptr<char[]> Data_;
};

} // namespace NKikimr::NMiniKQL::NAsync
