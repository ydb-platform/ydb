#pragma once

#include "mkql_match_recognize_measure_arg.h"
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

} //namespace NKikimr::NMiniKQL::NMatchRecognize 
