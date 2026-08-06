#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

struct TSerializerContext {
    TSerializerContext(
        TComputationContext& ctx,
        TType* rowType,
        const TMutableObjectOverBoxedValue<TValuePackerBoxed>& rowPacker)
        : Ctx(ctx)
        , RowType(rowType)
        , RowPacker(rowPacker)
    {
    }

    TComputationContext& Ctx;
    TType* RowType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed>& RowPacker;
};

template <class>
inline constexpr bool always_false_v = false;

struct TMrOutputSerializer: TOutputSerializer {
private:
    enum class ETPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TMrOutputSerializer(const TSerializerContext& context, EMkqlStateType stateType, ui32 stateVersion, TComputationContext& ctx)
        : TOutputSerializer(stateType, stateVersion, ctx)
        , Context_(context)
    {
    }

    using TOutputSerializer::Write;

    template <typename... Ts>
    void operator()(Ts&&... args) {
        (Write(std::forward<Ts>(args)), ...);
    }

    void Write(const NUdf::TUnboxedValue& value) {
        WriteUnboxedValue(Context_.RowPacker.RefMutableObject(Context_.Ctx, false, Context_.RowType), value);
    }

    template <class Type>
    void Write(const TIntrusivePtr<Type>& ptr) {
        bool isValid = static_cast<bool>(ptr);
        WriteBool(Buf_, isValid);
        if (!isValid) {
            return;
        }
        auto addr = reinterpret_cast<std::uintptr_t>(ptr.Get());
        WriteUi64(Buf_, addr);

        auto it = Cache_.find(addr);
        if (it != Cache_.end()) {
            WriteByte(Buf_, static_cast<ui8>(ETPtrStateMode::FromCache));
            return;
        }
        WriteByte(Buf_, static_cast<ui8>(ETPtrStateMode::Saved));
        ptr->Save(*this);
        Cache_[addr] = addr;
    }

private:
    const TSerializerContext& Context_;
    mutable std::map<std::uintptr_t, std::uintptr_t> Cache_;
};

struct TMrInputSerializer: TInputSerializer {
private:
    enum class ETPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TMrInputSerializer(TSerializerContext& context, const NUdf::TUnboxedValue& state)
        : TInputSerializer(state, EMkqlStateType::SIMPLE_BLOB)
        , Context_(context)
    {
    }

    using TInputSerializer::Read;

    template <typename... Ts>
    void operator()(Ts&... args) {
        (Read(args), ...);
    }

    void Read(NUdf::TUnboxedValue& value) {
        value = ReadUnboxedValue(Context_.RowPacker.RefMutableObject(Context_.Ctx, false, Context_.RowType), Context_.Ctx);
    }

    template <class Type>
    void Read(TIntrusivePtr<Type>& ptr) {
        bool isValid = Read<bool>();
        if (!isValid) {
            ptr.Reset();
            return;
        }
        ui64 addr = Read<ui64>();
        ETPtrStateMode mode = static_cast<ETPtrStateMode>(Read<ui8>());
        if (mode == ETPtrStateMode::Saved) {
            ptr = MakeIntrusive<Type>();
            ptr->Load(*this);
            Cache_[addr] = ptr.Get();
            return;
        }
        auto it = Cache_.find(addr);
        MKQL_ENSURE(it != Cache_.end(), "Internal error");
        auto* cachePtr = static_cast<Type*>(it->second);
        ptr = TIntrusivePtr<Type>(cachePtr);
    }

private:
    TSerializerContext& Context_;
    mutable std::map<std::uintptr_t, void*> Cache_;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
