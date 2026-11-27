#pragma once

#include "compute.h"
#include <util/ysaveload.h>

namespace NKikimr::NMiniKQL::NAsync {

// All indices refer to immutable MKQL nodes/slots, never runtime pointers.
// A new async occurrence has its own slot; sharing the same expression node
// explicitly shares the saved value within one invocation only.
enum class EInstruction: ui32 { Input,
                                Sync,
                                Branch,
                                Invoke,
                                Unwrap };
struct TInstruction {
    EInstruction Kind = EInstruction::Input;
    std::vector<ui32> Dependencies;
    ui32 Fragment = 0;
    std::vector<ui32> Arguments;
    TString Provider;
    TString Operation;
    ui64 MaxResponseBytes = 0;
    Y_SAVELOAD_DEFINE(Kind, Dependencies, Fragment, Arguments, Provider, Operation, MaxResponseBytes);
};
struct TProgram {
    ui32 Version = 2;
    ui32 Root = 0;
    ui32 MaxFrames = 0;
    std::vector<TInstruction> Instructions;
    Y_SAVELOAD_DEFINE(Version, Root, MaxFrames, Instructions);
};

} // namespace NKikimr::NMiniKQL::NAsync
