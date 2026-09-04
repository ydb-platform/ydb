#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Intrinsic Function Source Fragment                                         *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

#ifndef LLVM_IR_INTRINSIC_DX_ENUMS_H
#define LLVM_IR_INTRINSIC_DX_ENUMS_H

namespace llvm {
namespace Intrinsic {
enum DXIntrinsics : unsigned {
// Enum values for intrinsics
    dx_create_handle = 3336,                          // llvm.dx.create.handle
    dx_flattened_thread_id_in_group,           // llvm.dx.flattened.thread.id.in.group
    dx_group_id,                               // llvm.dx.group.id
    dx_thread_id,                              // llvm.dx.thread.id
    dx_thread_id_in_group,                     // llvm.dx.thread.id.in.group
}; // enum
} // namespace Intrinsic
} // namespace llvm

#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
