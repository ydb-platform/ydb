#pragma once

namespace NKikimr::NUdfStore::NWasm {

//! Ensures AllocateBytes / ThrowException host intrinsics are linked into the
//! process (and registered on the WAVM standard intrinsic module).
void EnsureUdfHostIntrinsicsRegistered();

} // namespace NKikimr::NUdfStore::NWasm
