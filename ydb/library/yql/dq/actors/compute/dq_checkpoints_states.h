#pragma once

#include <list>

namespace NYql::NDq {

struct TStateData {
    TString Blob;
    ui64 Version{0};
};

struct TMiniKqlProgramState {
    TStateData Data;
    ui64 RuntimeVersion{0};
};

struct TSourceState {
// State data for source.
// Typically there is only one element with state that
// source saved. But when we are migrating states
// between tasks there can be state
// from several different tasks sources.
    std::list<TStateData> Data;
    ui64 InputIndex;
};

struct TSinkState {
    TStateData Data;
    ui64 OutputIndex;
};

// Checkpoint for single compute actor.
struct TComputeActorState {
    TMiniKqlProgramState MiniKqlProgram;
    std::list<TSourceState> Sources;
    std::list<TSinkState> Sinks;

    void Clear() {
        // TODO;
    }
};

} // namespace NDq::NYql
