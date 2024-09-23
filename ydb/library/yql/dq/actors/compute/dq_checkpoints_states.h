#pragma once

#include <list>
#include <util/ysaveload.h>

namespace NYql::NDq {

struct TStateData {
    TStateData() {}

    TStateData(const TString& blob, ui64 version)
    : Blob(blob)
    , Version(version) {}

    TString Blob;
    ui64 Version{0};

    Y_SAVELOAD_DEFINE(Blob, Version);
};

struct TMiniKqlProgramState {
    TStateData Data;
    ui64 RuntimeVersion{0};

    void Clear() {
        Data.Blob.clear();
    }

    Y_SAVELOAD_DEFINE(Data, RuntimeVersion);
};

struct TSourceState {
// State data for source.
// Typically there is only one element with state that
// source saved. But when we are migrating states
// between tasks there can be state
// from several different tasks sources.
    std::list<TStateData> Data;
    ui64 InputIndex{0};

    size_t DataSize() const { return Data.size(); }

    Y_SAVELOAD_DEFINE(Data, InputIndex);
};

struct TSinkState {
    TStateData Data;
    ui64 OutputIndex{0};

    Y_SAVELOAD_DEFINE(Data, OutputIndex);
};

// Checkpoint for single compute actor.
struct TComputeActorState {
    TMaybe<TMiniKqlProgramState> MiniKqlProgram;
    std::list<TSourceState> Sources;
    std::list<TSinkState> Sinks;

    void Clear() {
        MiniKqlProgram.Clear();
        Sources.clear();
        Sinks.clear();
    }

    bool ParseFromString(const TString& in) {
        TStringStream str(in);
        Load(&str);
        return true;
    }
    bool SerializeToString(TString* out) const { 
        TStringStream result;
        Save(&result);
        *out = result.Str();
        return true;
    }
    size_t ByteSizeLong() const {return MiniKqlProgram ? MiniKqlProgram->Data.Blob.Size() : 0; }

    Y_SAVELOAD_DEFINE(MiniKqlProgram, Sources, Sinks);
};

} // namespace NYql::NDq 
