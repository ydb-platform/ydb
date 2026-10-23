// written by build/gn/scripts/pch_update.py
#include <__chrono/formatter.h>
#include <__chrono/literals.h>
#include <queue>
#include <set>
#include <unordered_set>
#include "contrib/libs/llvm16/include/llvm/ADT/SparseBitVector.h"
#include "contrib/libs/llvm16/include/llvm/CodeGen/MachineMemOperand.h"
#include "contrib/libs/llvm16/include/llvm/CodeGen/TargetSubtargetInfo.h"
#include "contrib/libs/llvm16/include/llvm/IR/Constants.h"
#include "contrib/libs/llvm16/include/llvm/IR/DataLayout.h"
#include "contrib/libs/llvm16/include/llvm/IR/DebugLoc.h"
#include "contrib/libs/llvm16/include/llvm/Pass.h"
#include "contrib/libs/llvm16/include/llvm/Support/TimeProfiler.h"
