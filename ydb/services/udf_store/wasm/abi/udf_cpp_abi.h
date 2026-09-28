#pragma once

#include "bridge_abi.h"

#include <stddef.h>

struct TExpressionContext;

//! Allocates size bytes within context.
extern "C" char* AllocateBytes(TExpressionContext* context, size_t size);

//! Throws an exception with the supplied error message.
extern "C" void ThrowException(const char* error);
