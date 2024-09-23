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

struct TMrOutputSerializer : TOutputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TMrOutputSerializer(const TSerializerContext& context, EMkqlStateType stateType, ui32 stateVersion, TComputationContext& ctx)
        : TOutputSerializer(stateType, stateVersion, ctx)
        , Context(context)
    {} 

    using TOutputSerializer::Write;

    template <typename... Ts>
    void operator()(Ts&&... args) {
        (Write(std::forward<Ts>(args)), ...);
    }

    void Write(const NUdf::TUnboxedValue& value) {
        WriteUnboxedValue(Context.RowPacker.RefMutableObject(Context.Ctx, false, Context.RowType), value);
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

private:
    const TSerializerContext& Context;
    mutable std::map<std::uintptr_t, std::uintptr_t> Cache;
};

struct TMrInputSerializer : TInputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TMrInputSerializer(TSerializerContext& context, const NUdf::TUnboxedValue& state)
        : TInputSerializer(state, EMkqlStateType::SIMPLE_BLOB)
        , Context(context) {    
    }

    using TInputSerializer::Read;

    template <typename... Ts>
    void operator()(Ts&... args) {
        (Read(args), ...);
    }

    void Read(NUdf::TUnboxedValue& value) {
        value = ReadUnboxedValue(Context.RowPacker.RefMutableObject(Context.Ctx, false, Context.RowType), Context.Ctx);
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
 
private:
    TSerializerContext& Context;
    mutable std::map<std::uintptr_t, void *> Cache;
};

} //namespace NKikimr::NMiniKQL::NMatchRecognize 
