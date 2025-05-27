#pragma once
#include "mkql_engine_flat.h"
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {
    typedef THashMap<ui32, TVector<TString>> TIncomingResults;

    struct TBuiltinStrings {
        TBuiltinStrings(const TTypeEnvironment& env)
            : Filter(env.InternName(TStringBuf("Filter")))
            , FilterNullMembers(env.InternName(TStringBuf("FilterNullMembers")))
            , SkipNullMembers(env.InternName(TStringBuf("SkipNullMembers")))
            , FlatMap(env.InternName(TStringBuf("FlatMap")))
            , Map(env.InternName(TStringBuf("Map")))
            , Member(env.InternName(TStringBuf("Member")))
            , ToHashedDict(env.InternName(TStringBuf("ToHashedDict")))
            , DictItems(env.InternName(TStringBuf("DictItems")))
            , Take(env.InternName(TStringBuf("Take")))
            , Length(env.InternName(TStringBuf("Length")))
            , Arg(env.InternName(TStringBuf("Arg")))
        {
            All.reserve(20);
            All.insert(Filter);
            All.insert(FilterNullMembers);
            All.insert(SkipNullMembers);
            All.insert(FlatMap);
            All.insert(Map);
            All.insert(Member);
            All.insert(ToHashedDict);
            All.insert(DictItems);
            All.insert(Take);
            All.insert(Length);
            All.insert(Arg);
        }

        const TInternName Filter;
        const TInternName FilterNullMembers;
        const TInternName SkipNullMembers;
        const TInternName FlatMap;
        const TInternName Map;
        const TInternName Member;
        const TInternName ToHashedDict;
        const TInternName DictItems;
        const TInternName Take;
        const TInternName Length;
        const TInternName Arg;

        THashSet<TInternName> All;
    };

    struct TFlatEngineStrings : public TTableStrings {
        TFlatEngineStrings(const TTypeEnvironment& env)
            : TTableStrings(env)
            , Builtins(env)
            , SetResult(env.InternName(TStringBuf("SetResult")))
            , Abort(env.InternName(TStringBuf("Abort")))
            , StepTxId(env.InternName(TStringBuf("StepTxId")))
            , AcquireLocks(env.InternName(TStringBuf("AcquireLocks")))
            , CombineByKeyMerge(env.InternName(TStringBuf("CombineByKeyMerge")))
            , Diagnostics(env.InternName(TStringBuf("Diagnostics")))
            , PartialTake(env.InternName(TStringBuf("PartialTake")))
            , PartialSort(env.InternName(TStringBuf("PartialSort")))
        {
            All.insert(SetResult);
            All.insert(Abort);
            All.insert(StepTxId);
            All.insert(AcquireLocks);
            All.insert(CombineByKeyMerge);
            All.insert(Diagnostics);
        }

        TBuiltinStrings Builtins;

        const TInternName SetResult;
        const TInternName Abort;
        const TInternName StepTxId;
        const TInternName AcquireLocks;
        const TInternName CombineByKeyMerge;
        const TInternName Diagnostics;
        const TInternName PartialTake;
        const TInternName PartialSort;
    };

    struct TShardExecData {
        TShardExecData(const TEngineFlatSettings& settings, const TFlatEngineStrings& strings,
                       const std::pair<ui64, ui64>& stepTxId)
            : Settings(settings)
            , Strings(strings)
            , StepTxId(stepTxId)
        {}

        const TEngineFlatSettings& Settings;
        const TFlatEngineStrings& Strings;
        const std::pair<ui64, ui64>& StepTxId;
        THashSet<ui32> LocalReadCallables;
        TIncomingResults Results;
    };

    struct TProxyExecData {
        TProxyExecData(const TEngineFlatSettings& settings, const TFlatEngineStrings& strings,
                     const std::pair<ui64, ui64>& stepTxId, const TVector<IEngineFlat::TTabletInfo>& tabletInfos,
                     const TVector<IEngineFlat::TTxLock>& locks)
            : Settings(settings)
            , Strings(strings)
            , StepTxId(stepTxId)
            , TabletInfos(tabletInfos)
            , TxLocks(locks)
        {}

        const TEngineFlatSettings& Settings;
        const TFlatEngineStrings& Strings;
        const std::pair<ui64, ui64> StepTxId;
        const TVector<IEngineFlat::TTabletInfo>& TabletInfos;
        const TVector<IEngineFlat::TTxLock>& TxLocks;
        TIncomingResults Results;
    };

    TComputationNodeFactory GetFlatShardExecutionFactory(TShardExecData& execData, bool validateOnly);
    TComputationNodeFactory GetFlatProxyExecutionFactory(TProxyExecData& execData);

    NUdf::TUnboxedValue PerformLocalSelectRow(TCallable& callable, IEngineFlatHost& engineHost,
        const THolderFactory& holderFactory, const TTypeEnvironment& env);
    NUdf::TUnboxedValue PerformLocalSelectRange(TCallable& callable, IEngineFlatHost& engineHost,
        const THolderFactory& holderFactory, const TTypeEnvironment& env);

    struct TEngineFlatApplyContext : public NUdf::IApplyContext {
        bool IsAborted = false;
        IEngineFlatHost* Host = nullptr;
        THashMap<TString, NUdf::TUnboxedValue>* ResultValues = nullptr;
        const TTypeEnvironment* Env = nullptr;
    };

    TStructType* GetTxLockType(const TTypeEnvironment& env, bool v2);
    TStructType* GetDiagnosticsType(const TTypeEnvironment& env);
    TType* GetActualReturnType(const TCallable& callable, const TTypeEnvironment& env,
        const TFlatEngineStrings& strings);
}
}
