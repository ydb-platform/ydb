#pragma once

#include "write.h"

#include <library/cpp/json/common/defs.h>

#include <functional>

namespace NKikimr::NBinaryJson {

// Fills the callbacks with a json document; the serializer that turns them into BinaryJson stays
// private to write.cpp, so the abi-dependent part of the module reaches it through this.
using TJsonEmitter = std::function<void(NJson::TJsonCallbacks&)>;

TBinaryJson SerializeToBinaryJson(const TJsonEmitter& emit);

} // namespace NKikimr::NBinaryJson
