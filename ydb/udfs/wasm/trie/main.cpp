#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include "binary_trie.h"

#include <util/generic/strbuf.h>

#include <cstdlib>
#include <cstring>
#include <exception>

using namespace NYdb::NUdfStore::NAbi;
using namespace NBinaryTrie;

namespace {

ui32 ExtractValidSize(TStringBuf dict, ui64 offset) {
    if (offset > dict.size() - sizeof(ui32)) {
        throw yexception() << "Corrupt trie: out of range (size)";
    }
    auto size = ReadUnaligned<ui32>(dict.data() + offset);
    if (size > dict.size() - offset - sizeof(ui32)) {
        throw yexception() << "Corrupt trie: out of range (content)";
    }
    return size;
}

//! Guest-side copy of a dictionary blob, built once per distinct value and
//! kept in that value's bridge user-data slot.
struct TGuestDict {
    uint64_t Tag;
    char* Data;
    size_t Len;
};

constexpr uint64_t GuestDictTag = 0x54524945444943ull; // "TRIEDIC"

void ReleaseGuestDict(uint64_t userData) {
    auto* dict = reinterpret_cast<TGuestDict*>(static_cast<uintptr_t>(userData));
    if (!dict || dict->Tag != GuestDictTag) {
        return;
    }
    free(dict->Data);
    free(dict);
}

TGuestDict* BuildGuestDict(uint64_t dictHandle) {
    const uint64_t offset = BridgeEnsureString(dictHandle);
    const int64_t len = BridgeGetStringLen(dictHandle);
    auto* dict = static_cast<TGuestDict*>(malloc(sizeof(TGuestDict)));
    if (!dict) {
        ThrowException("BuildGuestDict: malloc failed");
    }
    dict->Tag = GuestDictTag;
    dict->Len = static_cast<size_t>(len);
    dict->Data = len > 0 ? static_cast<char*>(malloc(dict->Len)) : nullptr;
    if (len > 0 && !dict->Data) {
        free(dict);
        ThrowException("BuildGuestDict: malloc failed");
    }
    if (len > 0) {
        memcpy(dict->Data, reinterpret_cast<const char*>(static_cast<uintptr_t>(offset)), dict->Len);
    }
    return dict;
}

} // namespace

extern "C" {

//! Trie::Lookup(haystack, dictBlob) -> Int64.
//! RegisterOrReuse + BridgeEnsureString pin the dict blob once per distinct
//! value; later rows get the same offset (BridgeRef not required).
__attribute__((visibility("default"))) void lookup(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t haystackH,
    uint64_t dictH)
{
    if (BridgeIsNull(haystackH) || BridgeIsNull(dictH)) {
        *result = MakeNull().Release();
        return;
    }

    const uint64_t dictOff = BridgeEnsureString(dictH);
    const int64_t dictLen = BridgeGetStringLen(dictH);
    const uint64_t hayOff = BridgeEnsureString(haystackH);
    const int64_t hayLen = BridgeGetStringLen(haystackH);

    const TStringBuf dict(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(dictOff)),
        static_cast<size_t>(dictLen));
    const TStringBuf haystack(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(hayOff)),
        static_cast<size_t>(hayLen));
    try {
        *result = MakeInt64(LookupTrie(haystack, dict)).Release();
    } catch (const std::exception& e) {
        ThrowException(e.what());
    }
}

//! Trie::LookupWithString → String payload at trie hit (null on miss).
__attribute__((visibility("default"))) void lookup_with_string(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t haystackH,
    uint64_t dictH)
{
    if (BridgeIsNull(haystackH) || BridgeIsNull(dictH)) {
        *result = MakeNull().Release();
        return;
    }

    const uint64_t dictOff = BridgeEnsureString(dictH);
    const int64_t dictLen = BridgeGetStringLen(dictH);
    const uint64_t hayOff = BridgeEnsureString(haystackH);
    const int64_t hayLen = BridgeGetStringLen(haystackH);

    const TStringBuf dict(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(dictOff)),
        static_cast<size_t>(dictLen));
    const TStringBuf haystack(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(hayOff)),
        static_cast<size_t>(hayLen));
    try {
        const i64 offset = LookupTrie(haystack, dict);
        if (offset < 0) {
            *result = MakeNull().Release();
            return;
        }
        const ui32 size = ExtractValidSize(dict, static_cast<ui64>(offset));
        const char* payload = dict.data() + static_cast<ui64>(offset) + sizeof(ui32);
        *result = MakeString(payload, static_cast<int64_t>(size)).Release();
    } catch (const std::exception& e) {
        ThrowException(e.what());
    }
}

//! Trie::LookupDict(haystack: String, dict: Dict<String,Int64>) -> Optional<Int64>
//! The dict stays on the host; only the looked up payload crosses the bridge.
__attribute__((visibility("default"))) void lookup_dict(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t haystackH,
    uint64_t dictH)
{
    TBridgeString haystack(haystackH, /*owned*/ false);
    TBridgeDict dict(dictH, /*owned*/ false);

    auto payload = dict.Lookup(haystack);
    if (!payload) {
        *result = MakeNull().Release();
        return;
    }
    const int64_t value = BridgeGetInt64(payload.Get());
    payload.Reset();
    *result = MakeOptional(MakeInt64(value)).Release();
}

//! Trie::LookupCachedBlob(haystack, dictBlob) -> Int64.
//! Same lookup, but the dictionary is materialized into guest memory once per
//! distinct value via the user-data slot: later rows touch the host only for
//! the per-row haystack.
__attribute__((visibility("default"))) void lookup_cached_blob(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t haystackH,
    uint64_t dictH)
{
    BridgeDrainReleasedUserData(&ReleaseGuestDict);

    if (BridgeIsNull(haystackH) || BridgeIsNull(dictH)) {
        *result = MakeNull().Release();
        return;
    }

    auto* dict = BridgeGetOrBuild<TGuestDict>(dictH, [dictH] {
        return BuildGuestDict(dictH);
    });

    const uint64_t hayOff = BridgeEnsureString(haystackH);
    const int64_t hayLen = BridgeGetStringLen(haystackH);
    const TStringBuf haystack(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(hayOff)),
        static_cast<size_t>(hayLen));
    try {
        *result = MakeInt64(LookupTrie(haystack, TStringBuf(dict->Data, dict->Len))).Release();
    } catch (const std::exception& e) {
        ThrowException(e.what());
    }
}

} // extern "C"
