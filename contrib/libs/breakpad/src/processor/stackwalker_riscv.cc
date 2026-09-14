// Copyright 2013 Google LLC
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

/* stackwalker_riscv.cc: riscv-specific stackwalker.
 *
 * See stackwalker_riscv.h for documentation.
 *
 * Author: Iacopo Colonnelli
 */

#include "common/scoped_ptr.h"
#include "google_breakpad/processor/call_stack.h"
#include "google_breakpad/processor/code_modules.h"
#include "google_breakpad/processor/memory_region.h"
#include "google_breakpad/processor/stack_frame_cpu.h"
#include "google_breakpad/processor/system_info.h"
#include "processor/cfi_frame_info.h"
#include "processor/logging.h"
#include "processor/stackwalker_riscv.h"

namespace google_breakpad {

StackwalkerRISCV::StackwalkerRISCV(const SystemInfo* system_info,
                                   const MDRawContextRISCV* context,
                                   MemoryRegion* memory,
                                   const CodeModules* modules,
                                   StackFrameSymbolizer* resolver_helper)
    : Stackwalker(system_info, memory, modules, resolver_helper),
      context_(context),
      context_frame_validity_(StackFrameRISCV::CONTEXT_VALID_ALL) {
}


StackFrame* StackwalkerRISCV::GetContextFrame() {
  if (!context_) {
    BPLOG(ERROR) << "Can't get context frame without context";
    return NULL;
  }

  StackFrameRISCV* frame = new StackFrameRISCV();

  frame->context = *context_;
  frame->context_validity = context_frame_validity_;
  frame->trust = StackFrame::FRAME_TRUST_CONTEXT;
  frame->instruction = frame->context.pc;

  return frame;
}

StackFrameRISCV* StackwalkerRISCV::GetCallerByCFIFrameInfo(
    const vector<StackFrame*>& frames,
    CFIFrameInfo* cfi_frame_info) {
  StackFrameRISCV* last_frame =
      static_cast<StackFrameRISCV*>(frames.back());

  // Populate a dictionary with the valid register values in last_frame.
  CFIFrameInfo::RegisterValueMap<uint32_t> callee_registers;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_PC)
    callee_registers["pc"] = last_frame->context.pc;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_RA)
    callee_registers["ra"] = last_frame->context.ra;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_SP)
    callee_registers["sp"] = last_frame->context.sp;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_GP)
    callee_registers["gp"] = last_frame->context.gp;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_TP)
    callee_registers["tp"] = last_frame->context.tp;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T0)
    callee_registers["t0"] = last_frame->context.t0;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T1)
    callee_registers["t1"] = last_frame->context.t1;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T2)
    callee_registers["t2"] = last_frame->context.t2;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S0)
    callee_registers["s0"] = last_frame->context.s0;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S1)
    callee_registers["s1"] = last_frame->context.s1;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A0)
    callee_registers["a0"] = last_frame->context.a0;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A1)
    callee_registers["a1"] = last_frame->context.a1;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A2)
    callee_registers["a2"] = last_frame->context.a2;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A3)
    callee_registers["a3"] = last_frame->context.a3;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A4)
    callee_registers["a4"] = last_frame->context.a4;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A5)
    callee_registers["a5"] = last_frame->context.a5;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A6)
    callee_registers["a6"] = last_frame->context.a6;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_A7)
    callee_registers["a7"] = last_frame->context.a7;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S2)
    callee_registers["s2"] = last_frame->context.s2;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S3)
    callee_registers["s3"] = last_frame->context.s3;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S4)
    callee_registers["s4"] = last_frame->context.s4;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S5)
    callee_registers["s5"] = last_frame->context.s5;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S6)
    callee_registers["s6"] = last_frame->context.s6;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S7)
    callee_registers["s7"] = last_frame->context.s7;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S8)
    callee_registers["s8"] = last_frame->context.s8;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S9)
    callee_registers["s9"] = last_frame->context.s9;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S10)
    callee_registers["s10"] = last_frame->context.s10;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_S11)
    callee_registers["s11"] = last_frame->context.s11;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T3)
    callee_registers["t3"] = last_frame->context.t3;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T4)
    callee_registers["t4"] = last_frame->context.t4;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T5)
    callee_registers["t5"] = last_frame->context.t5;
  if (last_frame->context_validity & StackFrameRISCV::CONTEXT_VALID_T6)
    callee_registers["t6"] = last_frame->context.t6;

  // Use the STACK CFI data to recover the caller's register values.
  CFIFrameInfo::RegisterValueMap<uint32_t> caller_registers;
  if (!cfi_frame_info->FindCallerRegs(callee_registers, *memory_,
                                      &caller_registers)) {
    return NULL;
  }

  // Construct a new stack frame given the values the CFI recovered.
  CFIFrameInfo::RegisterValueMap<uint32_t>::iterator entry;
  scoped_ptr<StackFrameRISCV> frame(new StackFrameRISCV());
  entry = caller_registers.find("pc");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_PC;
    frame->context.pc = entry->second;
  } else{
    // If the CFI doesn't recover the PC explicitly, then use .ra.
    entry = caller_registers.find(".ra");
    if (entry != caller_registers.end()) {
      frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_PC;
      frame->context.pc = entry->second;
    }
  }
  entry = caller_registers.find("ra");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_RA;
    frame->context.ra = entry->second;
  }
  entry = caller_registers.find("sp");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_SP;
    frame->context.sp = entry->second;
  } else {
    // If the CFI doesn't recover the SP explicitly, then use .cfa.
    entry = caller_registers.find(".cfa");
    if (entry != caller_registers.end()) {
      frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_SP;
      frame->context.sp = entry->second;
    }
  }
  entry = caller_registers.find("gp");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_GP;
    frame->context.gp = entry->second;
  }
  entry = caller_registers.find("tp");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_TP;
    frame->context.tp = entry->second;
  }
  entry = caller_registers.find("t0");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T0;
    frame->context.t0 = entry->second;
  }
  entry = caller_registers.find("t1");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T1;
    frame->context.t1 = entry->second;
  }
  entry = caller_registers.find("t2");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T2;
    frame->context.t2 = entry->second;
  }
  entry = caller_registers.find("s0");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S0;
    frame->context.s0 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S0) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S0;
    frame->context.s0 = last_frame->context.s0;
  }
  entry = caller_registers.find("s1");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S1;
    frame->context.s1 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S1) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S1;
    frame->context.s1 = last_frame->context.s1;
  }
  entry = caller_registers.find("a0");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A0;
    frame->context.a0 = entry->second;
  }
  entry = caller_registers.find("a1");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A1;
    frame->context.a1 = entry->second;
  }
  entry = caller_registers.find("a2");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A2;
    frame->context.a2 = entry->second;
  }
  entry = caller_registers.find("a3");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A3;
    frame->context.a3 = entry->second;
  }
  entry = caller_registers.find("a4");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A4;
    frame->context.a4 = entry->second;
  }
  entry = caller_registers.find("a5");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A5;
    frame->context.a5 = entry->second;
  }
  entry = caller_registers.find("a6");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A6;
    frame->context.a6 = entry->second;
  }
  entry = caller_registers.find("a7");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_A7;
    frame->context.a7 = entry->second;
  }
  entry = caller_registers.find("s2");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S2;
    frame->context.s2 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S2) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S2;
    frame->context.s2 = last_frame->context.s2;
  }
  entry = caller_registers.find("s3");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S3;
    frame->context.s3 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S3) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S3;
    frame->context.s3 = last_frame->context.s3;
  }
  entry = caller_registers.find("s4");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S4;
    frame->context.s4 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S4) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S4;
    frame->context.s4 = last_frame->context.s4;
  }
  entry = caller_registers.find("s5");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S5;
    frame->context.s5 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S5) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S5;
    frame->context.s5 = last_frame->context.s5;
  }
  entry = caller_registers.find("s6");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S6;
    frame->context.s6 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S6) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S6;
    frame->context.s6 = last_frame->context.s6;
  }
  entry = caller_registers.find("s7");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S7;
    frame->context.s7 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S7) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S7;
    frame->context.s7 = last_frame->context.s7;
  }
  entry = caller_registers.find("s8");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S8;
    frame->context.s8 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S8) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S8;
    frame->context.s8 = last_frame->context.s8;
  }
  entry = caller_registers.find("s9");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S9;
    frame->context.s9 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S9) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S9;
    frame->context.s9 = last_frame->context.s9;
  }
  entry = caller_registers.find("s10");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S10;
    frame->context.s10 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S10) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S10;
    frame->context.s10 = last_frame->context.s10;
  }
  entry = caller_registers.find("s11");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S11;
    frame->context.s11 = entry->second;
  } else if (last_frame->context_validity &
             StackFrameRISCV::CONTEXT_VALID_S11) {
    // Since the register is callee-saves, assume the callee
    // has not yet changed it.
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_S11;
    frame->context.s11 = last_frame->context.s11;
  }
  entry = caller_registers.find("t3");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T3;
    frame->context.t3 = entry->second;
  }
  entry = caller_registers.find("t4");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T4;
    frame->context.t4 = entry->second;
  }
  entry = caller_registers.find("t5");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T5;
    frame->context.t5 = entry->second;
  }
  entry = caller_registers.find("t6");
  if (entry != caller_registers.end()) {
    frame->context_validity |= StackFrameRISCV::CONTEXT_VALID_T6;
    frame->context.t6 = entry->second;
  }

  // If we didn't recover the PC and the SP, then the frame isn't very useful.
  static const uint64_t essentials = (StackFrameRISCV::CONTEXT_VALID_SP
                                      | StackFrameRISCV::CONTEXT_VALID_PC);
  if ((frame->context_validity & essentials) != essentials)
    return NULL;

  frame->trust = StackFrame::FRAME_TRUST_CFI;
  return frame.release();
}

StackFrameRISCV* StackwalkerRISCV::GetCallerByStackScan(
    const vector<StackFrame*>& frames) {
  StackFrameRISCV* last_frame =
      static_cast<StackFrameRISCV*>(frames.back());
  uint32_t last_sp = last_frame->context.sp;
  uint32_t caller_sp, caller_pc;

  if (!ScanForReturnAddress(last_sp, &caller_sp, &caller_pc,
      last_frame->trust == StackFrame::FRAME_TRUST_CONTEXT)) {
    // No plausible return address was found.
    return NULL;
  }

  // ScanForReturnAddress found a reasonable return address. Advance
  // sp to the location above the one where the return address was
  // found.
  caller_sp += 4;

  // Create a new stack frame (ownership will be transferred to the caller)
  // and fill it in.
  StackFrameRISCV* frame = new StackFrameRISCV();

  frame->trust = StackFrame::FRAME_TRUST_SCAN;
  frame->context = last_frame->context;
  frame->context.pc = caller_pc;
  frame->context.sp = caller_sp;
  frame->context_validity = StackFrameRISCV::CONTEXT_VALID_PC |
                            StackFrameRISCV::CONTEXT_VALID_SP;

  return frame;
}

  StackFrameRISCV* StackwalkerRISCV::GetCallerByFramePointer(
    const vector<StackFrame*>& frames) {
  StackFrameRISCV* last_frame =
      static_cast<StackFrameRISCV*>(frames.back());

  uint32_t last_fp = last_frame->context.s0;

  uint32_t caller_fp = 0;
  if (last_fp && !memory_->GetMemoryAtAddress(last_fp, &caller_fp)) {
    BPLOG(ERROR) << "Unable to read caller_fp from last_fp: 0x"
                 << std::hex << last_fp;
    return NULL;
  }

  uint32_t caller_ra = 0;
  if (last_fp && !memory_->GetMemoryAtAddress(last_fp + 4, &caller_ra)) {
    BPLOG(ERROR) << "Unable to read caller_ra from last_fp + 4: 0x"
                 << std::hex << (last_fp + 4);
    return NULL;
  }

  uint32_t caller_sp = last_fp ? last_fp + 8 : last_frame->context.s0;

  // Create a new stack frame (ownership will be transferred to the caller)
  // and fill it in.
  StackFrameRISCV* frame = new StackFrameRISCV();

  frame->trust = StackFrame::FRAME_TRUST_FP;
  frame->context = last_frame->context;
  frame->context.s0 = caller_fp;
  frame->context.sp = caller_sp;
  frame->context.pc = last_frame->context.ra;
  frame->context.ra = caller_ra;
  frame->context_validity = StackFrameRISCV::CONTEXT_VALID_PC |
                            StackFrameRISCV::CONTEXT_VALID_RA |
                            StackFrameRISCV::CONTEXT_VALID_S0 |
                            StackFrameRISCV::CONTEXT_VALID_SP;
  return frame;
}

StackFrame* StackwalkerRISCV::GetCallerFrame(const CallStack* stack,
                                             bool stack_scan_allowed) {
  if (!memory_ || !stack) {
    BPLOG(ERROR) << "Can't get caller frame without memory or stack";
    return NULL;
  }

  const vector<StackFrame*>& frames = *stack->frames();
  StackFrameRISCV* last_frame =
      static_cast<StackFrameRISCV*>(frames.back());
  scoped_ptr<StackFrameRISCV> frame;

  // Try to recover caller information from CFI.
  scoped_ptr<CFIFrameInfo> cfi_frame_info(
      frame_symbolizer_->FindCFIFrameInfo(last_frame));
  if (cfi_frame_info.get())
    frame.reset(GetCallerByCFIFrameInfo(frames, cfi_frame_info.get()));

  // If CFI failed, or there wasn't CFI available, fall back to frame pointer.
  if (!frame.get())
    frame.reset(GetCallerByFramePointer(frames));

  // If everything failed, fall back to stack scanning.
  if (stack_scan_allowed && !frame.get())
    frame.reset(GetCallerByStackScan(frames));

  // If nothing worked, tell the caller.
  if (!frame.get())
    return NULL;

  // Should we terminate the stack walk? (end-of-stack or broken invariant)
  if (TerminateWalk(frame->context.pc, frame->context.sp,
                    last_frame->context.sp,
                    last_frame->trust == StackFrame::FRAME_TRUST_CONTEXT)) {
    return NULL;
  }

  // The new frame's context's PC is the return address, which is one
  // instruction past the instruction that caused us to arrive at the callee.
  // RISCV instructions have a uniform 4-byte encoding, so subtracting 4 off
  // the return address gets back to the beginning of the call instruction.
  // Callers that require the exact return address value may access
  // frame->context.pc.
  frame->instruction = frame->context.pc - 4;

  return frame.release();
}

}  // namespace google_breakpad
