#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <util/random/random.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

inline EObjectType TypeFromId(TObjectId id)
{
    return EObjectType(id.Parts32[1] & 0xffff);
}

inline TCellTag CellTagFromId(TObjectId id)
{
    return TCellTag(id.Parts32[1] >> 16);
}

inline ui64 CounterFromId(TObjectId id)
{
    ui64 result;
    result   = id.Parts32[3];
    result <<= 32;
    result  |= id.Parts32[2];
    return result;
}

inline ui32 EntropyFromId(TObjectId id)
{
    return id.Parts32[0];
}

inline NHydra::TVersion VersionFromId(TObjectId id)
{
    YT_ASSERT(!IsSequoiaId(id));
    return NHydra::TVersion::FromRevision(CounterFromId(id));
}

inline NTransactionClient::TTimestamp TimestampFromId(TObjectId id)
{
    YT_ASSERT(IsSequoiaId(id));
    return CounterFromId(id) & ~SequoiaCounterMask;
}

inline EObjectType SchemaTypeFromType(EObjectType type)
{
    YT_ASSERT(HasSchema(type));
    return EObjectType(static_cast<int>(type) | SchemaObjectTypeMask);
}

inline EObjectType TypeFromSchemaType(EObjectType type)
{
    YT_ASSERT(static_cast<int>(type) & SchemaObjectTypeMask);
    return EObjectType(static_cast<int>(type) & ~SchemaObjectTypeMask);
}

inline TObjectId MakeId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter,
    ui32 entropy)
{
    return TObjectId(
        entropy,
        (static_cast<ui32>(cellTag.Underlying()) << 16) | static_cast<ui32>(type),
        counter & 0xffffffff,
        counter >> 32);
}

inline TObjectId MakeRandomId(
    EObjectType type,
    TCellTag cellTag)
{
    return MakeId(
        type,
        cellTag,
        RandomNumber<ui64>(),
        RandomNumber<ui32>());
}

inline bool IsWellKnownId(TObjectId id)
{
    return CounterFromId(id) & WellKnownCounterMask;
}

inline bool IsSequoiaId(TObjectId id)
{
    // NB: Well-known objects have Sequoia bit set.
    return (CounterFromId(id) & SequoiaCounterMask) && !IsWellKnownId(id);
}

inline TObjectId MakeRegularId(
    EObjectType type,
    TCellTag cellTag,
    NHydra::TVersion version,
    ui32 entropy)
{
    return TObjectId(
        entropy,
        (static_cast<ui32>(cellTag.Underlying()) << 16) | static_cast<ui32>(type),
        version.RecordId,
        version.SegmentId);
}

inline TObjectId MakeSequoiaId(
    EObjectType type,
    TCellTag cellTag,
    NTransactionClient::TTimestamp timestamp,
    ui32 entropy)
{
    YT_ASSERT(!(timestamp & SequoiaCounterMask));
    return MakeId(
        type,
        cellTag,
        timestamp | SequoiaCounterMask,
        entropy);
}

inline TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter /*= 0xffffffffffffffff*/)
{
    YT_VERIFY(counter & WellKnownCounterMask);
    return MakeId(
        type,
        cellTag,
        counter,
        static_cast<ui32>(cellTag.Underlying() * 901517) ^ 0x140a8383);
}

inline TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellTag cellTag)
{
    return MakeWellKnownId(SchemaTypeFromType(type), cellTag);
}

inline TObjectId ReplaceTypeInId(
    TObjectId id,
    EObjectType type)
{
    auto result = id;
    result.Parts32[1] &= ~0x0000ffff;
    result.Parts32[1] |= static_cast<ui32>(type);
    return result;
}

inline TObjectId ReplaceCellTagInId(
    TObjectId id,
    TCellTag cellTag)
{
    auto result = id;
    result.Parts32[1] &= ~0xffff0000;
    result.Parts32[1] |= static_cast<ui32>(cellTag.Underlying()) << 16;
    return result;
}

template <int ShardCount>
inline int GetShardIndex(TObjectId id)
{
    return EntropyFromId(id) % static_cast<ui32>(ShardCount);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE size_t TObjectIdEntropyHash::operator()(TObjectId id) const
{
    return (static_cast<size_t>(id.Parts32[0]) | (static_cast<size_t>(id.Parts32[0]) << 32)) ^ id.Parts64[1];
}

Y_FORCE_INLINE size_t TVersionedObjectIdEntropyHash::operator()(const TVersionedObjectId& id) const
{
    return
        TObjectIdEntropyHash()(id.TransactionId) * 497 +
        TObjectIdEntropyHash()(id.ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
