#pragma once

#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetPerfTestFactory(TComputationNodeFactory customFactory = {});

template<bool SPILLING>
TRuntimeNode WideLastCombiner(TProgramBuilder& pb, TRuntimeNode flow, const TProgramBuilder::TWideLambda& extractor, const TProgramBuilder::TBinaryWideLambda& init, const TProgramBuilder::TTernaryWideLambda& update, const TProgramBuilder::TBinaryWideLambda& finish) {
    return SPILLING ?
        pb.WideLastCombinerWithSpilling(flow, extractor, init, update, finish):
        pb.WideLastCombiner(flow, extractor, init, update, finish);
}

}
}
