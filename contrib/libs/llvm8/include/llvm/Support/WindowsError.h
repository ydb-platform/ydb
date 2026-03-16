#pragma once

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

//===-- WindowsError.h - Support for mapping windows errors to posix-------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_SUPPORT_WINDOWSERROR_H
#define LLVM_SUPPORT_WINDOWSERROR_H

#include <system_error>

namespace llvm {
std::error_code mapWindowsError(unsigned EV);
}

#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
