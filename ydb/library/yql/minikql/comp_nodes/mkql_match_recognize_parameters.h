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
    enum class TPtrStateMode {
        Saved = 0,
        FromCache = 1
    };
    TComputationContext&    Ctx;
    TType*                  StateType;
    const TMutableObjectOverBoxedValue<TValuePackerBoxed>& Packer;

    mutable std::map<std::uintptr_t, std::uintptr_t> WriteCache;
    mutable std::map<std::uintptr_t, void *> ReadCache;

    template<class TPtrType>
    void SavePtr(TString& out, const TIntrusivePtr<TPtrType>& ptr) const {
        std::cerr << "        SavePtr) "  << out.size() << std::endl;
        auto refCount = ptr.RefCount();

        bool isValid =  static_cast<bool>(ptr);
        WriteBool(out, isValid);

        if (!isValid) {
            return;
        }
        std::cerr << "        SavePtr) isValid "  << isValid << std::endl;

        auto addr = reinterpret_cast<std::uintptr_t>(ptr.Get());
        WriteUi64(out, addr);

        auto it = WriteCache.find(addr);
        if (it == WriteCache.end()) {
            std::cerr << "        SavePtr) new "  << std::endl;
            WriteByte(out, static_cast<ui8>(TPtrStateMode::Saved));
            ptr->Save(out, *this);
            WriteCache[addr] = addr;
        } else {
            WriteByte(out, static_cast<ui8>(TPtrStateMode::FromCache));
            std::cerr << "        SavePtr) from cache "  << std::endl;
        }
        std::cerr << "        SavePtr) end "  << out.size() << std::endl;
    }

    template<class TPtrType>
    void Load(TStringBuf& in, TIntrusivePtr<TPtrType>& ptr) const {
        std::cerr << "        Load) "  << in.size() << std::endl;
        //assert(false);
        bool isValid = ReadBool(in);
        if (!isValid) {
            ptr.Reset();
            return;
        }
        ui64 addr = ReadUi64(in);
        TPtrStateMode mode = static_cast<TPtrStateMode>(ReadByte(in));
        if (mode == TPtrStateMode::Saved) {
            auto newPtr = MakeIntrusive<TPtrType>();
            newPtr->Load(in, *this);
            ptr = newPtr;
            ReadCache[addr] = newPtr.Get();
        } else {
            auto it = ReadCache.find(addr);
            MKQL_ENSURE(it != ReadCache.end(), "Internal error");
            auto* cachePtr = static_cast<TPtrType*>(it->second);
            ptr = TIntrusivePtr<TPtrType>(cachePtr);
        }
        std::cerr << "        Load) end "  << in.size() << std::endl;
    }

};

} //namespace NKikimr::NMiniKQL::NMatchRecognize 
