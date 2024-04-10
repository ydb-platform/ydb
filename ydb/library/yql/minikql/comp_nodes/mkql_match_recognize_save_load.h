#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

struct TSerializerContext {

    TSerializerContext(
        TComputationContext& ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker)
        : Ctx(ctx)
        , RowType(rowType)
        , RowPacker(rowPacker)
        {}

    TComputationContext&    Ctx;
    TType*                  RowType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed>& RowPacker;
};

template<class>
inline constexpr bool always_false_v = false;

struct TOutputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TOutputSerializer(const TSerializerContext& context)
        : Context(context)
    {} 

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
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            WriteBool(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            WriteByte(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            WriteUi32(Buf, value);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, NUdf::TUnboxedValue>) {     // Only Row type (StateType) supported !
            WriteUnboxedValue(Buf, Context.RowPacker.RefMutableObject(Context.Ctx, false, Context.RowType), value);
        } else if constexpr (std::is_empty_v<Type>){
            // Empty struct is not saved/loaded.
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    template<class Type>
    void Write(const TIntrusivePtr<Type>& ptr) {
        bool isValid = static_cast<bool>(ptr);
        WriteBool(Buf, isValid);
        if (!isValid) {
            return;
        }
        auto addr = reinterpret_cast<std::uintptr_t>(ptr.Get());
        WriteUi64(Buf, addr);

        auto it = Cache.find(addr);
        if (it != Cache.end()) {
            WriteByte(Buf, static_cast<ui8>(TPtrStateMode::FromCache));
            return;
        }
        WriteByte(Buf, static_cast<ui8>(TPtrStateMode::Saved));
        ptr->Save(*this);
        Cache[addr] = addr;
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

    NUdf::TUnboxedValuePod MakeString() {
        auto strRef = NUdf::TStringRef(Buf.data(), Buf.size());
        return NKikimr::NMiniKQL::MakeString(strRef);
    }

private:
    const TSerializerContext& Context;
    TString Buf;
    mutable std::map<std::uintptr_t, std::uintptr_t> Cache;
};

struct TInputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TInputSerializer(TSerializerContext& context, const NUdf::TStringRef& state)
        : Context(context)
        , Buf(state.Data(), state.Size())
    {}

    template <typename... Ts>
    void operator()(Ts&... args) {
        (Read(args), ...);
    }

    template<typename Type, typename ReturnType = Type>
    ReturnType Read() {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            return ReadString(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            return ReadUi64(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            return ReadBool(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            return ReadByte(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            return ReadUi32(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, NUdf::TUnboxedValue>) {
            return ReadUnboxedValue(Buf, Context.RowPacker.RefMutableObject(Context.Ctx, false, Context.RowType), Context.Ctx);
        } else if constexpr (std::is_empty_v<Type>){
            // Empty struct is not saved/loaded.
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    template<typename Type>
    void Read(Type& value) {
        if constexpr (std::is_same_v<std::remove_cv_t<Type>, TString>) {
            value = ReadString(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui64>) {
            value = ReadUi64(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, bool>) {
            value = ReadBool(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui8>) {
            value = ReadByte(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, ui32>) {
            value = ReadUi32(Buf);
        } else if constexpr (std::is_same_v<std::remove_cv_t<Type>, NUdf::TUnboxedValue>) {
            value = ReadUnboxedValue(Buf, Context.RowPacker.RefMutableObject(Context.Ctx, false, Context.RowType), Context.Ctx);
        } else if constexpr (std::is_empty_v<Type>){
            // Empty struct is not saved/loaded.
        } else {
            static_assert(always_false_v<Type>, "Not supported type / not implemented");
        }
    }

    template<class Type>
    void Read(TIntrusivePtr<Type>& ptr) {
        bool isValid = Read<bool>();
        if (!isValid) {
            ptr.Reset();
            return;
        }
        ui64 addr = Read<ui64>();
        TPtrStateMode mode = static_cast<TPtrStateMode>(Read<ui8>());
        if (mode == TPtrStateMode::Saved) {
            ptr = MakeIntrusive<Type>();
            ptr->Load(*this);
            Cache[addr] = ptr.Get();
            return;
        }
        auto it = Cache.find(addr);
        MKQL_ENSURE(it != Cache.end(), "Internal error");
        auto* cachePtr = static_cast<Type*>(it->second);
        ptr = TIntrusivePtr<Type>(cachePtr);
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
        //auto size = Read<TVector::size_type>();
        value.clear();
        value.resize(size);
        for (size_t i = 0; i < size; ++i) {
            Read(value[i]);
        }
    }

    NUdf::TUnboxedValuePod MakeString() {
        auto strRef = NUdf::TStringRef(Buf.data(), Buf.size());
        return NKikimr::NMiniKQL::MakeString(strRef);
    }

    bool Empty() const {
        return Buf.empty();
    }

private:
    TSerializerContext& Context;
    TStringBuf Buf;
    mutable std::map<std::uintptr_t, void *> Cache;
};

} //namespace NKikimr::NMiniKQL::NMatchRecognize 
