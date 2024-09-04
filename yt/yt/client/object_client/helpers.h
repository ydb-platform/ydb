#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

//! Function for temporary use: to gradually allow types supported in Sequoia.
bool IsScalarType(NObjectClient::EObjectType type);

//! Creates the YPath pointing to an object with a given #id.
NYPath::TYPath FromObjectId(TObjectId id);

//! Checks if the given type is versioned, i.e. represents a Cypress node.
bool IsVersionedType(EObjectType type);

//! Checks if the given type is user, i.e. regular users are allowed to create its instances.
bool IsUserType(EObjectType type);

//! Checks if the given type can have schema assigned to it, i.e. Cypress table or chaos replicated table.
bool IsSchemafulType(EObjectType type);

//! Checks if the given type is table, i.e. represents a Cypress table.
bool IsTableType(EObjectType type);

//! Checks if the given type is log table, i.e. table which contains replication log.
bool IsLogTableType(EObjectType type);

//! Checks if the given type is tablet owner.
bool IsTabletOwnerType(EObjectType type);

//! Checks if the given type is chunk owner.
bool IsChunkOwnerType(EObjectType type);

//! Checks if the given type is cell.
bool IsCellType(EObjectType type);

//! Checks if the given type is cell bundle.
bool IsCellBundleType(EObjectType type);

//! Checks if the given type allows alien objects.
bool IsAlienType(EObjectType type);

//! Checks if the given type is a tablet.
bool IsTabletType(EObjectType type);

//! Checks if the given type holds replication metadata.
bool IsReplicatedTableType(EObjectType);

//! Checks if the given type is a table replica.
bool IsTableReplicaType(EObjectType type);

//! Checks if the given type is a chaos replica.
bool IsChaosTableReplicaType(EObjectType type);

//! Checks if the given type is a collocation.
bool IsCollocationType(EObjectType type);

//! Checks if the given type is a medium.
bool IsMediumType(EObjectType type);

//! Checks if the given type is a Cypress (i.e. master, simple) transaction.
bool IsCypressTransactionType(EObjectType type);

//! Checks if the given type is a system transaction.
bool IsSystemTransactionType(EObjectType type);

//! Checks if the given type if an upload transaction.
bool IsUploadTransactionType(EObjectType type);

//! Checks if node with the given type can contain other nodes.
bool IsCompositeNodeType(EObjectType type);

//! Checks if the given type is either Link or SequoiaLink.
bool IsLinkType(EObjectType);

//! Extracts the type component from #id.
EObjectType TypeFromId(TObjectId id);

//! Extracts the cell id component from #id.
TCellTag CellTagFromId(TObjectId id);

//! Extracts the counter component from #id.
ui64 CounterFromId(TObjectId id);

//! Extracts the entropy component from #id.
ui32 EntropyFromId(TObjectId id);

//! Extracts Hydra revision from #id for non-Sequoia objects.
NHydra::TVersion VersionFromId(TObjectId id);

//! Extracts the object creation timestamp from #id for Sequoia object.
NTransactionClient::TTimestamp TimestampFromId(TObjectId id);

//! Returns |true| iff a given #type denotes a schema.
bool IsSchemaType(EObjectType type);

//! Returns |true| iff a given regular #type has an associated schema type.
bool HasSchema(EObjectType type);

//! Returns the schema type for a given regular #type.
EObjectType SchemaTypeFromType(EObjectType type);

//! Returns the regular type for a given schema #type.
EObjectType TypeFromSchemaType(EObjectType type);

//! Constructs the id from its parts.
TObjectId MakeId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter,
    ui32 entropy);

//! Creates a random id with given type and cell tag.
TObjectId MakeRandomId(
    EObjectType type,
    TCellTag cellTag);

//! Returns |true| if a given #id is well-known.
/*
 *  This method checks the highest bit of counter part.
 */
bool IsWellKnownId(TObjectId id);

//! Returns |true| if a given #id corresponds to Sequoia.
/*
 *  This method checks the second highest bit of counter part.
 */
bool IsSequoiaId(TObjectId id);

//! Constructs the id for a regular object.
TObjectId MakeRegularId(
    EObjectType type,
    TCellTag cellTag,
    NHydra::TVersion version,
    ui32 entropy);

//! Constructs the id for a regular Sequoia object.
TObjectId MakeSequoiaId(
    EObjectType type,
    TCellTag cellTag,
    NTransactionClient::TTimestamp timestamp,
    ui32 entropy);

//! Constructs the id corresponding to well-known (usually singleton) entities.
/*
 *  The highest bit of #counter must be set.
 */
TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter = 0xffffffffffffffff);

//! Returns the id of the schema object for a given regular type.
TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellTag cellTag);

//! Constructs a new object id by replacing type component in a given one.
TObjectId ReplaceTypeInId(
    TObjectId id,
    EObjectType type);

//! Constructs a new object id by replacing cell tag component in a given one.
TObjectId ReplaceCellTagInId(
    TObjectId id,
    TCellTag cellTag);

//! Useful for uniform partition of objects between shards.
// NB: #shardCount must be a power of 2.
template <int ShardCount>
int GetShardIndex(TObjectId id);

//! Returns true if #cellId is uniquely identified by its tag component.
//! Currently these are master and chaos cells.
bool IsGlobalCellId(TCellId cellId);

////////////////////////////////////////////////////////////////////////////////

//! Relies on first 32 bits of object id ("entropy") to be pseudo-random,
//! cf. MakeRegularId.
struct TObjectIdEntropyHash
{
    size_t operator()(TObjectId id) const;
};

//! Cf. TObjectIdEntropyHash
struct TVersionedObjectIdEntropyHash
{
    size_t operator()(const TVersionedObjectId& id) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
