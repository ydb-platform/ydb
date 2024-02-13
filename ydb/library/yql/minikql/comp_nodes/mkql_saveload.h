#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

#include <string_view>

namespace NKikimr {
namespace NMiniKQL {

Y_FORCE_INLINE void WriteByte(TString& out, ui8 value) {
    out.append((char)value);
}

Y_FORCE_INLINE void WriteBool(TString& out, bool value) {
    out.append((char)value);
}

Y_FORCE_INLINE void WriteUi32(TString& out, ui32 value) {
    char buf[MAX_PACKED32_SIZE];
    out.AppendNoAlias(buf, Pack32(value, buf));
}

Y_FORCE_INLINE void WriteUi64(TString& out, ui64 value) {
    char buf[MAX_PACKED64_SIZE];
    out.AppendNoAlias(buf, Pack64(value, buf));
}

Y_FORCE_INLINE bool ReadBool(TStringBuf& in) {
    MKQL_ENSURE(in.size(), "Serialized state is corrupted");
    bool result = (bool)*in.data();
    in.Skip(1);
    return result;
}

Y_FORCE_INLINE ui8 ReadByte(TStringBuf& in) {
    MKQL_ENSURE(in.size(), "Serialized state is corrupted");
    ui8 result = *in.data();
    in.Skip(1);
    return result;
}

Y_FORCE_INLINE ui32 ReadUi32(TStringBuf& in) {
    ui32 result;
    auto count = Unpack32(in.data(), in.size(), result);
    MKQL_ENSURE(count, "Serialized state is corrupted");
    in.Skip(count);
    return result;
}

Y_FORCE_INLINE ui64 ReadUi64(TStringBuf& in) {
    ui64 result;
    auto count = Unpack64(in.data(), in.size(), result);
    MKQL_ENSURE(count, "Serialized state is corrupted");
    in.Skip(count);
    return result;
}

Y_FORCE_INLINE void WriteString(TString& out, std::string_view str) {
    WriteUi32(out, str.size());
    out.AppendNoAlias(str.data(), str.size());
}

Y_FORCE_INLINE std::string_view ReadString(TStringBuf& in) {
    const ui32 size = ReadUi32(in);
    MKQL_ENSURE(in.Size() >= size, "Serialized state is corrupted");
    TStringBuf head = in.Head(size);
    in = in.Tail(size);
    return head;
}

Y_FORCE_INLINE void WriteUnboxedValue(TString& out, const TValuePacker& packer, const NUdf::TUnboxedValue& value) {
    auto state = packer.Pack(value);
    WriteUi32(out, state.size());
    out.AppendNoAlias(state.data(), state.size());
}

Y_FORCE_INLINE NUdf::TUnboxedValue ReadUnboxedValue(TStringBuf& in, const TValuePacker& packer, TComputationContext& ctx) {
    auto size = ReadUi32(in);
    MKQL_ENSURE(size <= in.size(), "Serialized state is corrupted");
    auto value = packer.Unpack(TStringBuf(in.data(), in.data() + size), ctx.HolderFactory);
    in.Skip(size);
    return value;
}

class TNodeStateHelper {

public:
    enum class EType {
        SIMPLE_BLOB,
        SNAPSHOT,
        INCREMENT
    };

    static void AddNodeState(TString& result, const TStringBuf& state) {
        WriteUi32(result, state.Size());
        result.AppendNoAlias(state.Data(), state.Size());
    }

    static NUdf::TUnboxedValue MakeSimpleBlobState(const TString& blob) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EType::SIMPLE_BLOB));
        WriteString(out, blob);
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    template<typename TContainer>
    static NUdf::TUnboxedValue MakeSnapshotState(TContainer& items) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EType::SNAPSHOT));
        WriteUi32(out, static_cast<ui32>(items.size()));
        for (const auto& [key, value] : items) {
            WriteString(out, key);
            WriteString(out, value);
        }
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    template<typename TContainer, typename TContainer2>
    static NUdf::TUnboxedValue MakeIncrementState(TContainer& createdOrChanged, TContainer2& deleted) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EType::INCREMENT));
        WriteUi32(out, static_cast<ui32>(createdOrChanged.size()));
        WriteUi32(out, static_cast<ui32>(deleted.size()));
        for(const auto& [key, value] : createdOrChanged) {
            WriteString(out, key);
            WriteString(out, value);
        }
        for(const auto& key : deleted) {
            WriteString(out, key);
        }
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    class Reader {
    public:
        explicit Reader(TStringBuf& buf)
            : Buf(buf)
        {
            MKQL_ENSURE(Buf.size(), "Serialized state is corrupted");
            const auto type = ReadUi32(Buf);
            Type = static_cast<EType>(type);
        }

        EType GetType() const {
            return Type;
        }

        static TStringBuf GetSimpleSnapshot(const NUdf::TStringRef& state) {
            TStringBuf buf(state.Data(), state.Size());
            Reader reader(buf);
            MKQL_ENSURE(reader.GetType() == EType::SIMPLE_BLOB, "state type is not SIMPLE_BLOB");
            auto str = ReadString(buf);
            MKQL_ENSURE(buf.empty(), "broken state");
            return str;
        }

        std::string_view ReadSimpleSnapshot() {
            return ReadString(Buf);
        }

        template<class TCallbackUpdate, class TCallbackDelete>
        void ReadItems(TCallbackUpdate updateItem, TCallbackDelete deleteKey) {
            MKQL_ENSURE(Buf.size(), "Serialized state is corrupted");
            ui32 itemsCount = ReadUi32(Buf);
            ui32 deletedCount = 0;
            if (Type == EType::INCREMENT) {
                deletedCount = ReadUi32(Buf);
            }
            for (ui32 i = 0; i < itemsCount; ++i) {
                auto key = ReadString(Buf);
                auto value = ReadString(Buf);
                updateItem(key, value);
            }
            if (deletedCount) {
                auto key = ReadString(Buf);
                deleteKey(key);
            }
        }
    private:
        TStringBuf& Buf;
        EType Type{EType::SIMPLE_BLOB};
    };
};

} // namespace NMiniKQL
} // namespace NKikimr
