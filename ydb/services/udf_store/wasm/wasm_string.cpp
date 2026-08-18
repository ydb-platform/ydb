#include "wasm_string.h"

#include "compartment_manager.h"
#include "prefer_wasm_stats.h"

#include <ydb/library/wasm/api/allocation_registry.h>
#include <ydb/library/wasm/api/data_transfer.h>

#include <util/generic/yexception.h>
#include <util/system/compiler.h>

#include <bit>
#include <cstring>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;

TUnboxedValuePod TWasmStringValue::Make(
    TStringRef data,
    IWebAssemblyCompartment* compartment,
    ui64 generation,
    std::shared_ptr<void> owner)
{
    if (!compartment) {
        ythrow yexception() << "TWasmStringValue::Make: compartment is null";
    }
    if (data.Size() == 0) {
        return TUnboxedValuePod::Embedded(0);
    }
    if (data.Size() <= TUnboxedValuePod::InternalBufferSize) {
        return TUnboxedValuePod::Embedded(data);
    }

    const ui64 allocBytes = TStringValue::AllocationBytes(data.Size());
    auto buffer = TGuestBuffer::Allocate(compartment, allocBytes);
    auto* header = TStringValue::ConstructInPlace(buffer.HostData(), data.Size(), data.Size());
    std::memcpy(header->Data(), data.Data(), data.Size());

    const uintptr_t offset = buffer.Offset();
    TWasmAllocationRegistry::Instance().Register(
        header, compartment, offset, allocBytes, generation, std::move(owner));
    buffer.Release();

    // ConstructInPlace starts at one reference while a freshly built pod must
    // carry none (see MiniKQL MakeString): the owner that wraps the pod into a
    // TUnboxedValue takes the first one. Keeping the extra reference would hold
    // the guest buffer until the whole query compartment is torn down instead of
    // freeing it on the last UnRef (→ UdfTryFreeExternalString → registry).
    header->ReleaseRef();
    return TUnboxedValuePod(TStringValue(header));
}

TUnboxedValuePod TWasmStringValue::MakePreferWasm(TStringRef data)
{
    if (data.Size() <= TUnboxedValuePod::InternalBufferSize) {
        if (data.Size() == 0) {
            return TUnboxedValuePod::Embedded(0);
        }
        return TUnboxedValuePod::Embedded(data);
    }

    // Only the query compartment: it can be kept alive for as long as the value
    // lives (the registry holds the handle). A compartment borrowed from a UDF
    // call in flight offers no such guarantee, and the value may well outlive it.
    auto* handle = GetCurrentQueryCompartment();
    if (!handle || !handle->Compartment) {
        // Host fallback via UDF allocator (no MiniKQL MakeString dependency).
        TPreferWasmStats::Instance().OnFallbackNoCompartment();
        return TUnboxedValuePod(TStringValue(data));
    }

    TPreferWasmStats::Instance().OnMaterializedInWasm();
    return Make(
        data,
        handle->Compartment.get(),
        handle->Generation,
        handle->shared_from_this());
}

bool TWasmStringValue::TryGetResidentOffset(
    const TUnboxedValuePod& value,
    IWebAssemblyCompartment* compartment,
    ui64 expectedGeneration,
    uintptr_t& offset,
    ui32& length)
{
    if (!compartment || !value) {
        return false;
    }
    // Only heap/WASM-backed strings; embedded payloads are not in linear memory.
    if (!value.IsString()) {
        return false;
    }

    if (expectedGeneration != 0) {
        if (auto* handle = GetCurrentQueryCompartment()) {
            if (handle->Generation != expectedGeneration || handle->Compartment.get() != compartment) {
                return false;
            }
        }
    }

    const TStringRef ref = value.AsStringRef();
    length = ref.Size();
    if (length == 0) {
        offset = 0;
        return true;
    }

    // Plain range check rather than GetCompartmentOffset: a host string is the
    // expected outcome here, and throwing per UDF argument costs tens of
    // microseconds. Base is re-read every time because memory.grow moves it.
    const auto base = std::bit_cast<uintptr_t>(compartment->GetHostPointer(0, 1));
    const auto data = std::bit_cast<uintptr_t>(ref.Data());
    if (data < base || data - base + length > compartment->GetLinearMemorySize()) {
        return false;
    }

    offset = data - base;
    return true;
}

void TWasmStringValue::FillAbiStringArg(
    IWebAssemblyCompartment* compartment,
    const TUnboxedValuePod& arg,
    TUnversionedValue& value,
    TCopyGuard& stringGuard)
{
    value.Type = EAbiValueType::String;

    uintptr_t residentOffset = 0;
    ui32 residentLength = 0;
    ui64 generation = 0;
    if (auto* handle = GetCurrentQueryCompartment()) {
        generation = handle->Generation;
    }

    if (TryGetResidentOffset(arg, compartment, generation, residentOffset, residentLength)) {
        value.Length = residentLength;
        value.Data.String = std::bit_cast<char*>(residentOffset);
        TPreferWasmStats::Instance().OnResidentReused();
        return;
    }

    const TStringBuf string = arg.AsStringRef();
    stringGuard = CopyIntoCompartment(string, compartment);
    value.Length = static_cast<ui32>(string.size());
    value.Data.String = std::bit_cast<char*>(stringGuard.GetCopiedOffset());
    TPreferWasmStats::Instance().OnCopiedIntoCompartment();
}

} // namespace NKikimr::NUdfStore::NWasm

// Must be a strong GLOBAL symbol so it overrides the Y_WEAK stub in
// yql/essentials/public/udf/udf_allocator.cpp. With hidden visibility the
// definition becomes LOCAL and UnRef never reaches TWasmAllocationRegistry.
extern "C" __attribute__((visibility("default"), used)) bool UdfTryFreeExternalString(void* mem, ui64 /*size*/) {
    return NYdb::NWasm::TWasmAllocationRegistry::Instance().TryFree(mem);
}
