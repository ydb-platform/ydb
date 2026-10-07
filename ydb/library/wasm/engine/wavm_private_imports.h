#pragma once

// Disable warnings in included headers.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#if defined(__APPLE__) || defined(__darwin__)
#pragma clang diagnostic ignored "-Wformat"
#pragma clang diagnostic ignored "-Wundefined-inline"
#endif

#include <WAVM/Runtime/Intrinsics.h>
#include <WAVM/Runtime/Linker.h>
#include <WAVM/Runtime/Runtime.h>
#include <WAVM/RuntimeABI/RuntimeABI.h>

#include <WAVM/IR/Module.h>
#include <WAVM/IR/Types.h>

#include <WAVM/WASTParse/WASTParse.h>

#include <WAVM/WASM/WASM.h>

#pragma clang diagnostic pop
