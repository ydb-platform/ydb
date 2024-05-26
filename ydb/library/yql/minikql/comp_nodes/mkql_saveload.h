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

Y_FORCE_INLINE std::string_view ReadString(TStringBuf& in) {
    const ui32 size = ReadUi32(in);
    MKQL_ENSURE(in.Size() >= size, "Serialized state is corrupted");
    TStringBuf head = in.Head(size);
    in = in.Tail(size);
    return head;
}

Y_FORCE_INLINE void WriteString(TString& out, std::string_view str) {
    WriteUi32(out, str.size());
    out.AppendNoAlias(str.data(), str.size());
}

template<class>
inline constexpr bool always_false_v = false;

enum class EMkqlStateType {
    SIMPLE_BLOB,
    SNAPSHOT,
    INCREMENT
};

struct TOutputSerializer {
public:
    static NUdf::TUnboxedValue MakeSimpleBlobState(const TString& blob, ui32 stateVersion) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EMkqlStateType::SIMPLE_BLOB));
        WriteUi32(out, stateVersion);
        out.AppendNoAlias(blob.data(), blob.size());
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    template<typename TContainer>
    static NUdf::TUnboxedValue MakeSnapshotState(TContainer& items, ui32 stateVersion) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EMkqlStateType::SNAPSHOT));
        WriteUi32(out, stateVersion);
        WriteUi32(out, static_cast<ui32>(items.size()));
        for (const auto& [key, value] : items) {
            WriteString(out, key);
            WriteString(out, value);
        }
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    template<typename TContainer, typename TContainer2>
    static NUdf::TUnboxedValue MakeIncrementState(TContainer& createdOrChanged, TContainer2& deleted, ui32 stateVersion) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EMkqlStateType::INCREMENT));
        WriteUi32(out, stateVersion);
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

public:
    TOutputSerializer(EMkqlStateType stateType, ui32 stateVersion, TComputationContext& ctx)
        : Ctx(ctx) {
        Write(static_cast<ui32>(stateType));
        Write(stateVersion);
    }

    template <typename... Ts>
    void operator()(Ts&&... args) {
        (Write(std::forward<Ts>(args)), ...);
    }

    template<typename Type>
    void Write(const Type& value ) {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            WriteString(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            WriteUi64(Buf, value); 
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, i64>) {
            WriteUi64(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            WriteBool(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            WriteByte(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            WriteUi32(Buf, value);
        } else if constexpr (std::is_empty_v<Type>){
            // Empty struct is not saved/loaded.
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    template<class Type1, class Type2>
    void Write(const std::pair<Type1, Type2>& value) {
        Write(value.first);
        Write(value.second);
    }

    template<class Type, class Allocator>
    void Write(const std::vector<Type, Allocator>& value) {
        Write(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            Write(value[i]);
        }
    }

    Y_FORCE_INLINE void WriteUnboxedValue(const TValuePacker& packer, const NUdf::TUnboxedValue& value) {
        auto state = packer.Pack(value);
        Write<ui32>(state.size());
        Buf.AppendNoAlias(state.data(), state.size());
    }

    static NUdf::TUnboxedValue MakeArray(TComputationContext& ctx, const TStringBuf& buf) {
        const size_t MaxItemLen = 1048576;
        
        size_t count = buf.size() / MaxItemLen + (buf.size() % MaxItemLen ? 1 : 0);
        NUdf::TUnboxedValue *items = nullptr;
        auto array = ctx.HolderFactory.CreateDirectArrayHolder(count, items);

        size_t pos = 0;
        for (size_t index = 0; index < count; ++index) {
            size_t itemSize = std::min(buf.size() - pos, MaxItemLen);
            NUdf::TStringValue str(itemSize);
            std::memcpy(str.Data(), buf.Data() + pos, itemSize);
            items[index] = NUdf::TUnboxedValuePod(std::move(str));
            pos += itemSize;
        }
        return array;
    }

    NUdf::TUnboxedValue MakeState() {
        return MakeArray(Ctx, Buf);
    }
protected:
    TString Buf;
    TComputationContext& Ctx;
};

struct TInputSerializer {
public:
    TInputSerializer(const TStringBuf& state, TMaybe<EMkqlStateType> expectedType = Nothing()) 
        : Buf(state) {
        Type = static_cast<EMkqlStateType>(Read<ui32>());
        Read(StateVersion);
        if (expectedType) {
            MKQL_ENSURE(Type == *expectedType, "state type is not expected");
        }
    }
   
    TInputSerializer(const NUdf::TUnboxedValue& state, TMaybe<EMkqlStateType> expectedType = Nothing())
        : State(StateToString(state))
        , Buf(State) {
        Type = static_cast<EMkqlStateType>(Read<ui32>());
        Read(StateVersion);
        if (expectedType) {
            MKQL_ENSURE(Type == *expectedType, "state type is not expected");
        }
    }

    ui32 GetStateVersion() {
        return StateVersion;
    }

    EMkqlStateType GetType() {
        return Type;
    }

    template <typename... Ts>
    void operator()(Ts&... args) {
        (Read(args), ...);
    }

    template<typename Type, typename ReturnType = Type>
    ReturnType Read() {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            return ReturnType(ReadString(Buf));
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            return ReadUi64(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, i64>) {
            return ReadUi64(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            return ReadBool(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            return ReadByte(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            return ReadUi32(Buf);
        } else if constexpr (std::is_empty_v<Type>){
            // Empty struct is not saved/loaded.
            return ReturnType{};
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    Y_FORCE_INLINE NUdf::TUnboxedValue ReadUnboxedValue(const TValuePacker& packer, TComputationContext& ctx) {
        auto size = Read<ui32>();
        MKQL_ENSURE_S(size <= Buf.size(), "Serialized state is corrupted, size " << size << ", Buf.size " << Buf.size());
        auto value = packer.Unpack(TStringBuf(Buf.data(), Buf.data() + size), ctx.HolderFactory);
        Buf.Skip(size);
        return value;
    }

    template<typename Type>
    void Read(Type& value) {
        value = Read<Type, Type>();
    }

    template<class Type1, class Type2>
    void Read(std::pair<Type1, Type2>& value) {
        Read(value.first);
        Read(value.second);
    }

    template<class Type, class Allocator>
    void Read(std::vector<Type, Allocator>& value) {
        using TVector = std::vector<Type, Allocator>;
        auto size = Read<typename TVector::size_type>();
        value.clear();
        value.resize(size);
        for (size_t i = 0; i < size; ++i) {
            Read(value[i]);
        }
    }

    template<class TCallbackUpdate, class TCallbackDelete>
    void ReadItems(TCallbackUpdate updateItem, TCallbackDelete deleteKey) {
        MKQL_ENSURE(Buf.size(), "Serialized state is corrupted");
        ui32 itemsCount = ReadUi32(Buf);
        ui32 deletedCount = 0;
        if (Type == EMkqlStateType::INCREMENT) {
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
    
    bool Empty() const {
        return Buf.empty();
    }

private:
    TString StateToString(const NUdf::TUnboxedValue& state) {
        TString result;
        auto listIt = state.GetListIterator();
        NUdf::TUnboxedValue str;
        while (listIt.Next(str)) {
            const TStringBuf strRef = str.AsStringRef();
            result.AppendNoAlias(strRef.Data(), strRef.Size());
        }
        return result;
    }

protected:
    TString State;
    TStringBuf Buf;
    EMkqlStateType Type{EMkqlStateType::SIMPLE_BLOB};
    ui32 StateVersion{0};
};


class TNodeStateHelper {
public:
    static void AddNodeState(TString& result, const TStringBuf& state) {
        WriteUi64(result, state.Size());
        result.AppendNoAlias(state.Data(), state.Size());
    }
};

} // namespace NMiniKQL
} // namespace NKikimr
