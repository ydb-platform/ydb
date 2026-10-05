#pragma once

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/pack_num.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

#include <array>
#include <string_view>

namespace NKikimr::NMiniKQL {

Y_FORCE_INLINE void WriteByte(TString& out, ui8 value) {
    out.append((char)value);
}

Y_FORCE_INLINE void WriteBool(TString& out, bool value) {
    out.append((char)value);
}

Y_FORCE_INLINE void WriteUi32(TString& out, ui32 value) {
    std::array<char, MAX_PACKED32_SIZE> buf;
    out.AppendNoAlias(buf.data(), Pack32(value, buf.data()));
}

Y_FORCE_INLINE void WriteUi64(TString& out, ui64 value) {
    std::array<char, MAX_PACKED64_SIZE> buf;
    out.AppendNoAlias(buf.data(), Pack64(value, buf.data()));
}

Y_FORCE_INLINE bool ReadBool(TStringBuf& in) {
    MKQL_ENSURE(!in.empty(), "Serialized state is corrupted");
    bool result = (bool)*in.data();
    in.Skip(1);
    return result;
}

Y_FORCE_INLINE ui8 ReadByte(TStringBuf& in) {
    MKQL_ENSURE(!in.empty(), "Serialized state is corrupted");
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
    MKQL_ENSURE(in.size() >= size, "Serialized state is corrupted");
    const std::string_view result(in.data(), size);
    in.Skip(size);
    return result;
}

Y_FORCE_INLINE void WriteString(TString& out, std::string_view str) {
    WriteUi32(out, str.size());
    out.AppendNoAlias(str.data(), str.size());
}

template <class>
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

    template <typename TContainer>
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

    template <typename TContainer, typename TContainer2>
    static NUdf::TUnboxedValue MakeIncrementState(TContainer& createdOrChanged, TContainer2& deleted, ui32 stateVersion) {
        TString out;
        WriteUi32(out, static_cast<ui32>(EMkqlStateType::INCREMENT));
        WriteUi32(out, stateVersion);
        WriteUi32(out, static_cast<ui32>(createdOrChanged.size()));
        WriteUi32(out, static_cast<ui32>(deleted.size()));
        for (const auto& [key, value] : createdOrChanged) {
            WriteString(out, key);
            WriteString(out, value);
        }
        for (const auto& key : deleted) {
            WriteString(out, key);
        }
        auto strRef = NUdf::TStringRef(out);
        return NMiniKQL::MakeString(strRef);
    }

    TOutputSerializer(EMkqlStateType stateType, ui32 stateVersion, TComputationContext& ctx)
        : Ctx_(ctx)
    {
        Write(static_cast<ui32>(stateType));
        Write(stateVersion);
    }

    template <typename... Ts>
    void operator()(Ts&&... args) {
        (Write(std::forward<Ts>(args)), ...);
    }

    template <typename Type>
    void Write(const Type& value) {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            WriteString(Buf_, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            WriteUi64(Buf_, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, i64>) {
            WriteUi64(Buf_, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            WriteBool(Buf_, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            WriteByte(Buf_, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            WriteUi32(Buf_, value);
        } else if constexpr (std::is_empty_v<Type>) {
            // Empty struct is not saved/loaded.
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    template <class Type1, class Type2>
    void Write(const std::pair<Type1, Type2>& value) {
        Write(value.first);
        Write(value.second);
    }

    template <class Type, class Allocator>
    void Write(const std::vector<Type, Allocator>& value) {
        Write(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            Write(value[i]);
        }
    }

    Y_FORCE_INLINE void WriteUnboxedValue(const TValuePacker& packer, const NUdf::TUnboxedValue& value) {
        auto state = packer.Pack(value);
        Write<ui32>(state.size());
        Buf_.AppendNoAlias(state.data(), state.size());
    }

    static NUdf::TUnboxedValue MakeArray(TComputationContext& ctx, const TStringBuf& buf) {
        const size_t MaxItemLen = 1048576;

        size_t count = buf.size() / MaxItemLen + (buf.size() % MaxItemLen ? 1 : 0);
        NUdf::TUnboxedValue* items = nullptr;
        auto array = ctx.HolderFactory.CreateDirectArrayHolder(count, items);

        size_t pos = 0;
        for (size_t index = 0; index < count; ++index) {
            size_t itemSize = std::min(buf.size() - pos, MaxItemLen);
            NUdf::TStringValue str(itemSize);
            std::memcpy(str.Data(), buf.data() + pos, itemSize);
            items[index] = NUdf::TUnboxedValuePod(std::move(str));
            pos += itemSize;
        }
        return array;
    }

    NUdf::TUnboxedValue MakeState() {
        return MakeArray(Ctx_, Buf_);
    }

protected:
    TString Buf_;
    TComputationContext& Ctx_;
};

struct TInputSerializer {
public:
    explicit TInputSerializer(const TStringBuf& state, TMaybe<EMkqlStateType> expectedType = Nothing())
        : Buf_(state)
    {
        Type_ = static_cast<EMkqlStateType>(Read<ui32>());
        Read(StateVersion_);
        if (expectedType) {
            MKQL_ENSURE(Type_ == *expectedType, "state type is not expected");
        }
    }

    explicit TInputSerializer(const NUdf::TUnboxedValue& state, TMaybe<EMkqlStateType> expectedType = Nothing())
        : State_(StateToString(state))
        , Buf_(State_)
    {
        Type_ = static_cast<EMkqlStateType>(Read<ui32>());
        Read(StateVersion_);
        if (expectedType) {
            MKQL_ENSURE(Type_ == *expectedType, "state type is not expected");
        }
    }

    ui32 GetStateVersion() {
        return StateVersion_;
    }

    EMkqlStateType GetType() {
        return Type_;
    }

    template <typename... Ts>
    void operator()(Ts&... args) {
        (Read(args), ...);
    }

    template <typename Type, typename ReturnType = Type>
    ReturnType Read() {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            return ReturnType(ReadString(Buf_));
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            return ReadUi64(Buf_);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, i64>) {
            return ReadUi64(Buf_);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            return ReadBool(Buf_);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            return ReadByte(Buf_);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            return ReadUi32(Buf_);
        } else if constexpr (std::is_empty_v<Type>) {
            // Empty struct is not saved/loaded.
            return ReturnType{};
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    Y_FORCE_INLINE NUdf::TUnboxedValue ReadUnboxedValue(const TValuePacker& packer, TComputationContext& ctx) {
        auto size = Read<ui32>();
        MKQL_ENSURE_S(size <= Buf_.size(), "Serialized state is corrupted, size " << size << ", Buf.size " << Buf_.size());
        auto value = packer.Unpack(TStringBuf(Buf_.data(), Buf_.data() + size), ctx.HolderFactory);
        Buf_.Skip(size);
        return value;
    }

    template <typename Type>
    void Read(Type& value) {
        value = Read<Type, Type>();
    }

    template <class Type1, class Type2>
    void Read(std::pair<Type1, Type2>& value) {
        Read(value.first);
        Read(value.second);
    }

    template <class Type, class Allocator>
    void Read(std::vector<Type, Allocator>& value) {
        using TVector = std::vector<Type, Allocator>;
        auto size = Read<typename TVector::size_type>();
        value.clear();
        value.resize(size);
        for (size_t i = 0; i < size; ++i) {
            Read(value[i]);
        }
    }

    template <class TCallbackUpdate, class TCallbackDelete>
    void ReadItems(TCallbackUpdate updateItem, TCallbackDelete deleteKey) {
        MKQL_ENSURE(!Buf_.empty(), "Serialized state is corrupted");
        ui32 itemsCount = ReadUi32(Buf_);
        ui32 deletedCount = 0;
        if (Type_ == EMkqlStateType::INCREMENT) {
            deletedCount = ReadUi32(Buf_);
        }
        for (ui32 i = 0; i < itemsCount; ++i) {
            auto key = ReadString(Buf_);
            auto value = ReadString(Buf_);
            updateItem(key, value);
        }
        if (deletedCount) {
            auto key = ReadString(Buf_);
            deleteKey(key);
        }
    }

    bool Empty() const {
        return Buf_.empty();
    }

private:
    TString StateToString(const NUdf::TUnboxedValue& state) {
        TString result;
        auto listIt = state.GetListIterator();
        NUdf::TUnboxedValue str;
        while (listIt.Next(str)) {
            const TStringBuf strRef = str.AsStringRef();
            result.AppendNoAlias(strRef.data(), strRef.size());
        }
        return result;
    }

protected:
    TString State_;
    TStringBuf Buf_;
    EMkqlStateType Type_{EMkqlStateType::SIMPLE_BLOB};
    ui32 StateVersion_{0};
};

class TNodeStateHelper {
public:
    static void AddNodeState(TString& result, const TStringBuf& state) {
        WriteUi64(result, state.size());
        result.AppendNoAlias(state.data(), state.size());
    }
};

} // namespace NKikimr::NMiniKQL
