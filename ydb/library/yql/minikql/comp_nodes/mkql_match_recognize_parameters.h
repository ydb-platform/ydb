#pragma once

#include "mkql_match_recognize_measure_arg.h"
#include "mkql_saveload.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <util/generic/hash.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

enum class EOutputColumnSource {PartitionKey, Measure};
using TOutputColumnOrder = std::vector<std::pair<EOutputColumnSource, size_t>, TMKQLAllocator<std::pair<EOutputColumnSource, size_t>>>;

struct TMatchRecognizeProcessorParameters {
    IComputationExternalNode* InputDataArg;
    NYql::NMatchRecognize::TRowPattern Pattern;
    TUnboxedValueVector       VarNames;
    THashMap<TString, size_t> VarNamesLookup;
    IComputationExternalNode* MatchedVarsArg;
    IComputationExternalNode* CurrentRowIndexArg;
    TComputationNodePtrVector Defines;
    IComputationExternalNode* MeasureInputDataArg;
    TMeasureInputColumnOrder  MeasureInputColumnOrder;
    TComputationNodePtrVector Measures;
    TOutputColumnOrder        OutputColumnOrder;
};

struct TSaveLoadContext {

    TComputationContext&    Ctx;
    TType*                  StateType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed>& Packer;
};

template<class>
inline constexpr bool always_false_v2 = false;

struct TOutputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TOutputSerializer(const TSaveLoadContext& context)
        : Context(context)
    {} 

    void Write(ui32 value) {
        WriteUi32(Buf, value);
    }

    void Write(ui64 value) {
        WriteUi64(Buf, value);
    }

    void Write(std::string_view value) {
        WriteString(Buf, value);
    }

    void Write(const NUdf::TUnboxedValue& value) {
        WriteUnboxedValue(Buf, Context.Packer.RefMutableObject(Context.Ctx, false, Context.StateType), value);
    }

    void Write(bool value) {
        WriteBool(Buf, value);
    }

    template<class TPtrType>
    void Write(const TIntrusivePtr<TPtrType>& ptr) {
        // Format
        //      bool isValid
        //      Ui64 addr
        //      ui8  mode
        //      ...data
        auto refCount = ptr.RefCount();
        bool isValid =  static_cast<bool>(ptr);
        WriteBool(Buf, isValid);

        if (!isValid) {
            return;
        }
        auto addr = reinterpret_cast<std::uintptr_t>(ptr.Get());
        WriteUi64(Buf, addr);

        auto it = WriteCache.find(addr);
        if (it == WriteCache.end()) {
            WriteByte(Buf, static_cast<ui8>(TPtrStateMode::Saved));
            ptr->Save(*this);
            WriteCache[addr] = addr;
        } else {
            WriteByte(Buf, static_cast<ui8>(TPtrStateMode::FromCache));
        }
    }

    NUdf::TUnboxedValuePod MakeString() {
        auto strRef = NUdf::TStringRef(Buf.data(), Buf.size());
        return NKikimr::NMiniKQL::MakeString(strRef);
    }

    size_t Size() // TODO : delete
    {
        return Buf.size();
    }
private:
    TString Buf;
    const TSaveLoadContext& Context;
    mutable std::map<std::uintptr_t, std::uintptr_t> WriteCache;
};

// template<class>
// inline constexpr bool always_false_v = false;
#include <typeinfo>

struct TInputSerializer {
private:
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };

public:
    TInputSerializer(TSaveLoadContext& context, const NUdf::TStringRef& state)
        : Context(context)
        , Buf(state.Data(), state.Size())
    {}

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
            return ReadUnboxedValue(Buf, Context.Packer.RefMutableObject(Context.Ctx, false, Context.StateType), Context.Ctx);
        }
        else 
            static_assert(always_false_v2<Type>, "non-exhaustive visitor!");
        MKQL_ENSURE(false, "Not implemented");
    }

    template<class TPtrType>
    void Read(TIntrusivePtr<TPtrType>& ptr) {
        bool isValid = Read<bool>();
        if (!isValid) {
            ptr.Reset();
            return;
        }
        ui64 addr = Read<ui64>();
        TPtrStateMode mode = static_cast<TPtrStateMode>(Read<ui8>());
        if (mode == TPtrStateMode::Saved) {
            ptr = MakeIntrusive<TPtrType>();
            ptr->Load(*this);
            ReadCache[addr] = ptr.Get();
        } else {
            auto it = ReadCache.find(addr);
            MKQL_ENSURE(it != ReadCache.end(), "Internal error");
            auto* cachePtr = static_cast<TPtrType*>(it->second);
            ptr = TIntrusivePtr<TPtrType>(cachePtr);
            auto refCount = ptr.RefCount();
        }
    }

    NUdf::TUnboxedValuePod MakeString() {
        auto strRef = NUdf::TStringRef(Buf.data(), Buf.size());
        return NKikimr::NMiniKQL::MakeString(strRef);
    }

    size_t Size() // TODO : delete
    {
        return Buf.size();
    }
private:
    TStringBuf Buf;
    TSaveLoadContext& Context;
    mutable std::map<std::uintptr_t, void *> ReadCache;
};

} //namespace NKikimr::NMiniKQL::NMatchRecognize 
