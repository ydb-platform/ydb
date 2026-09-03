#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

//! Opaque handle into the host bridge node table. 0 is null.
typedef uint64_t TBridgeHandle;

//! Kind returned by BridgeGetKind (must match EBridgeValueKind).
enum {
    BRIDGE_KIND_NULL = 0,
    BRIDGE_KIND_INT64 = 1,
    BRIDGE_KIND_UINT64 = 2,
    BRIDGE_KIND_DOUBLE = 3,
    BRIDGE_KIND_BOOLEAN = 4,
    BRIDGE_KIND_STRING = 5,
    BRIDGE_KIND_OPTIONAL = 6,
    BRIDGE_KIND_LIST = 7,
    BRIDGE_KIND_DICT = 8,
    BRIDGE_KIND_RESOURCE = 9,
    BRIDGE_KIND_CALLABLE = 10,
    BRIDGE_KIND_TUPLE = 11,
    BRIDGE_KIND_STRUCT = 12,
    BRIDGE_KIND_VARIANT = 13,
    BRIDGE_KIND_INT8 = 14,
    BRIDGE_KIND_UINT8 = 15,
    BRIDGE_KIND_INT16 = 16,
    BRIDGE_KIND_UINT16 = 17,
    BRIDGE_KIND_INT32 = 18,
    BRIDGE_KIND_UINT32 = 19,
    BRIDGE_KIND_FLOAT = 20,
    BRIDGE_KIND_UTF8 = 21,
    BRIDGE_KIND_YSON = 22,
    BRIDGE_KIND_JSON = 23,
    BRIDGE_KIND_DATE = 24,
    BRIDGE_KIND_DATETIME = 25,
    BRIDGE_KIND_TIMESTAMP = 26,
    BRIDGE_KIND_INTERVAL = 27,
    BRIDGE_KIND_DECIMAL = 28
};

int32_t BridgeGetKind(TBridgeHandle handle);
int32_t BridgeIsNull(TBridgeHandle handle);

//! Integral getters widen: BridgeGetInt64 reads any signed kind (Int8..Int64,
//! Interval) and BridgeGetUint64 any unsigned one (Uint8..Uint64, Date,
//! Datetime, Timestamp). The narrow getters insist on the exact kind.
int64_t BridgeGetInt64(TBridgeHandle handle);
uint64_t BridgeGetUint64(TBridgeHandle handle);
int32_t BridgeGetInt32(TBridgeHandle handle);
uint32_t BridgeGetUint32(TBridgeHandle handle);
float BridgeGetFloat(TBridgeHandle handle);
double BridgeGetDouble(TBridgeHandle handle);
int32_t BridgeGetBool(TBridgeHandle handle);
//! Writes the raw 16-byte decimal at dstOff.
void BridgeCopyDecimal(TBridgeHandle handle, uint64_t dstOff);

int64_t BridgeGetStringLen(TBridgeHandle handle);
//! Copy string bytes into guest linear memory at dstOff (capacity cap).
//! Returns number of bytes copied; throws if cap is too small.
int64_t BridgeCopyString(TBridgeHandle handle, uint64_t dstOff, int64_t cap);
//! Offset of the string bytes in linear memory, copying them there on first
//! use. Values with identity (anything but tiny inline strings) are copied
//! once per query and reused on later rows, so calling this every row is
//! cheap. The offset is valid until the UDF returns: ask again, do not cache.
uint64_t BridgeEnsureString(TBridgeHandle handle);

TBridgeHandle BridgeGetOptional(TBridgeHandle handle);
TBridgeHandle BridgeGetElement(TBridgeHandle handle, int32_t index);
//! Struct/Tuple shape, from the declared type.
int32_t BridgeGetMemberCount(TBridgeHandle handle);
//! Index of a struct member by name, or -1 when there is no such member.
int32_t BridgeGetMemberIndex(TBridgeHandle handle, uint64_t nameOff, int64_t nameLen);

int32_t BridgeGetVariantIndex(TBridgeHandle handle);
TBridgeHandle BridgeGetVariantItem(TBridgeHandle handle);

int64_t BridgeListLength(TBridgeHandle handle);
int32_t BridgeListHasItems(TBridgeHandle handle);
TBridgeHandle BridgeListMakeIterator(TBridgeHandle handle);
//! Writes item handle to *outItem when has==1. Returns 1 if next, 0 if end.
int32_t BridgeListIterNext(TBridgeHandle iter, TBridgeHandle* outItem);

int64_t BridgeDictLength(TBridgeHandle handle);
int32_t BridgeDictHasItems(TBridgeHandle handle);
int32_t BridgeDictContains(TBridgeHandle dict, TBridgeHandle key);
//! Payload for `key`, or 0 when the key is absent. A key that is present but
//! holds a null payload answers with a live handle for which BridgeIsNull is
//! 1, so the two cases stay distinguishable.
TBridgeHandle BridgeDictLookup(TBridgeHandle dict, TBridgeHandle key);
TBridgeHandle BridgeDictMakeIterator(TBridgeHandle handle);
TBridgeHandle BridgeDictMakeKeysIterator(TBridgeHandle handle);
TBridgeHandle BridgeDictMakePayloadsIterator(TBridgeHandle handle);
int32_t BridgeDictIterNext(
    TBridgeHandle iter,
    TBridgeHandle* outKey,
    TBridgeHandle* outPayload);

TBridgeHandle BridgeMakeNull(void);
TBridgeHandle BridgeMakeInt64(int64_t value);
TBridgeHandle BridgeMakeUint64(uint64_t value);
TBridgeHandle BridgeMakeInt32(int32_t value);
TBridgeHandle BridgeMakeUint32(uint32_t value);
TBridgeHandle BridgeMakeFloat(float value);
TBridgeHandle BridgeMakeDouble(double value);
TBridgeHandle BridgeMakeBool(int32_t value);
TBridgeHandle BridgeMakeString(uint64_t srcOff, int64_t len);
//! Does not consume `inner`, and may return `inner` itself with an added ref
//! when MiniKQL represents the Optional exactly like its payload.
TBridgeHandle BridgeMakeOptional(TBridgeHandle inner);
//! Build a Tuple from an array of handles in linear memory. The result is
//! typed from the declared result type when that type holds a Tuple of the
//! same arity; otherwise it stays untyped and BridgeGetElement /
//! BridgeGetMemberCount on it will fail.
TBridgeHandle BridgeMakeArray(uint64_t elemsOff, int32_t n);
//! Same layout and the same typing rule, but the result reads back as a
//! Struct; members follow the declared member order of the result type.
TBridgeHandle BridgeMakeStruct(uint64_t membersOff, int32_t n);
TBridgeHandle BridgeMakeList(uint64_t itemsOff, int32_t n);
//! Does not consume `item`: the caller still owns the handle it passed in.
TBridgeHandle BridgeMakeVariant(int32_t index, TBridgeHandle item);
//! Type of the value the running UDF must return, as a value-less handle.
TBridgeHandle BridgeGetResultType(void);
//! Build a dict of the type named by `typeHandle` (BridgeGetResultType or an
//! input dict) from 2*n handles laid out as key, payload, key, payload, ...
TBridgeHandle BridgeMakeDict(TBridgeHandle typeHandle, uint64_t pairsOff, int32_t n);

TBridgeHandle BridgeRun(TBridgeHandle callable, uint64_t argsOff, int32_t n);
int64_t BridgeGetResourceTagLen(TBridgeHandle handle);
int64_t BridgeCopyResourceTag(TBridgeHandle handle, uint64_t dstOff, int64_t cap);

//! Lazily built guest state for the value behind `handle` (a parsed index, a
//! built trie, ...). Keyed by value identity, so it survives the handle and is
//! found again on the next row without BridgeRef. 0 means "nothing yet".
//! Requires a value with identity: strings and boxed values, not scalars.
uint64_t BridgeGetUserData(TBridgeHandle handle);
void BridgeSetUserData(TBridgeHandle handle, uint64_t value);

//! Blocks in linear memory outside the guest heap, for state the guest wants
//! to keep across rows. Freed only by BridgeFreeResident.
uint64_t BridgeAllocResident(uint64_t length);
void BridgeFreeResident(uint64_t offset);

//! User-data of cache entries the host dropped, so the guest can free them.
//! Writes up to `cap` values at `dstOff`, returns how many. The host never
//! calls guest code, so releasing is the guest's job: drain at each entry.
int32_t BridgeTakeReleasedUserData(uint64_t dstOff, int32_t cap);

//! Extra ref so a handle survives the auto-Unref at the end of each host Run
//! and TryReuse returns the same handle on the next row. Not needed to keep
//! pinned bytes or user-data alive: both are keyed by value, not by handle.
void BridgeRef(TBridgeHandle handle);
void BridgeUnref(TBridgeHandle handle);

#ifdef __cplusplus
} // extern "C"
#endif
