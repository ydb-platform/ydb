#pragma once

namespace NKikimr::NUdfStore::NWasm {

//! Ensures AllocateBytes / ThrowException / Bridge* host intrinsics are linked
//! into the process (and registered on the WAVM standard intrinsic module).
void EnsureUdfHostIntrinsicsRegistered();

//! Keep bridge intrinsic statics linked (called from EnsureUdfHostIntrinsicsRegistered).
void KeepBridgeHostIntrinsicsLinked();

} // namespace NKikimr::NUdfStore::NWasm
