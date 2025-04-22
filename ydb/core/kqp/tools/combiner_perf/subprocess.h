#pragma once

#include "printout.h"

#include <functional>

using TRunPayload = std::function<TRunResult()>;

TRunResult RunForked(TRunPayload payload);