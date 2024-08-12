#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(ui64* protoReplica, TChunkReplicaWithMedium replica);
void FromProto(TChunkReplicaWithMedium* replica, ui64 protoReplica);

////////////////////////////////////////////////////////////////////////////////

//! A compact representation of |(nodeId, replicaIndex, mediumIndex)| triplet.
class TChunkReplicaWithMedium
{
public:
    TChunkReplicaWithMedium();
    TChunkReplicaWithMedium(
        NNodeTrackerClient::TNodeId nodeId,
        int replicaIndex,
        int mediumIndex);
    // NB: Will be assigned to generic medium.
    explicit TChunkReplicaWithMedium(TChunkReplica replica);

    NNodeTrackerClient::TNodeId GetNodeId() const;
    int GetReplicaIndex() const;
    int GetMediumIndex() const;

    TChunkReplica ToChunkReplica() const;
    static TChunkReplicaList ToChunkReplicas(const TChunkReplicaWithMediumList& replicasWithMedia);

private:
    /*!
     *  Bits:
     *   0-23: node id (24 bits)
     *  24-28: replica index (5 bits)
     *  29-37: medium index (7 bits)
     */
    ui64 Value_;

    explicit TChunkReplicaWithMedium(ui64 value);

    friend void ToProto(ui64* value, TChunkReplicaWithMedium replica);
    friend void FromProto(TChunkReplicaWithMedium* replica, ui64 value);
    friend void ToProto(NProto::TConfirmChunkReplicaInfo* value, TChunkReplicaWithLocation replica);
    friend void FromProto(TChunkReplicaWithLocation* replica, NProto::TConfirmChunkReplicaInfo value);
};

// These protect from accidently serializing TChunkReplicaWithMedium as ui32.
void ToProto(ui32* value, TChunkReplicaWithMedium replica) = delete;
void FromProto(TChunkReplicaWithMedium* replica, ui32 value) = delete;

void FormatValue(TStringBuilderBase* builder, TChunkReplicaWithMedium replica, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TConfirmChunkReplicaInfo* value, TChunkReplicaWithLocation replica);
void FromProto(TChunkReplicaWithLocation* replica, NProto::TConfirmChunkReplicaInfo value);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(kvk1920): Remove GetMediumIndex().
class TChunkReplicaWithLocation
    : public TChunkReplicaWithMedium
{
public:
    TChunkReplicaWithLocation();
    TChunkReplicaWithLocation(TChunkReplicaWithMedium replica, TChunkLocationUuid locationUuid);
    TChunkReplicaWithLocation(
        NNodeTrackerClient::TNodeId nodeId,
        int replicaIndex,
        int mediumIndex,
        TChunkLocationUuid locationUuid);

    TChunkLocationUuid GetChunkLocationUuid() const;

    friend void ToProto(NProto::TConfirmChunkReplicaInfo* value, TChunkReplicaWithLocation replica);
    friend void FromProto(TChunkReplicaWithLocation* replica, NProto::TConfirmChunkReplicaInfo value);

private:
    TChunkLocationUuid ChunkLocationUuid_;
};

void FormatValue(TStringBuilderBase* builder, TChunkReplicaWithLocation replica, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TWrittenChunkReplicasInfo
{
    TChunkReplicaWithLocationList Replicas;
    // Revision upon confirmation of the chunk. Not every writer is expected to set this field.
    NHydra::TRevision ConfirmationRevision = NHydra::NullRevision;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplica
{
public:
    TChunkReplica();
    TChunkReplica(NNodeTrackerClient::TNodeId nodeId, int replicaIndex);
    TChunkReplica(const TChunkReplicaWithMedium& replica);

    NNodeTrackerClient::TNodeId GetNodeId() const;
    int GetReplicaIndex() const;

private:
    /*!
     *  Bits:
     *   0-23: node id (24 bits)
     *  24-28: replica index (5 bits)
     */
    ui32 Value_;

    explicit TChunkReplica(ui32 value);

    friend void ToProto(ui32* value, TChunkReplica replica);
    friend void FromProto(TChunkReplica* replica, ui32 value);
};

void FormatValue(TStringBuilderBase* builder, TChunkReplica replica, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndex
{
    TChunkIdWithIndex();
    TChunkIdWithIndex(TChunkId id, int replicaIndex);

    TChunkId Id;
    int ReplicaIndex;

    bool operator==(const TChunkIdWithIndex& other) const = default;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndexes
    : public TChunkIdWithIndex
{
    TChunkIdWithIndexes();
    TChunkIdWithIndexes(const TChunkIdWithIndex& chunkIdWithIndex, int mediumIndex);
    TChunkIdWithIndexes(TChunkId id, int replicaIndex, int mediumIndex);

    int MediumIndex;

    bool operator==(const TChunkIdWithIndexes& other) const = default;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

bool operator<(const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndex& id, TStringBuf spec = {});

////////////////////////////////////////////////////////////////////////////////

bool operator<(const TChunkIdWithIndexes& lhs, const TChunkIdWithIndexes& rhs);

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndexes& id, TStringBuf spec = {});

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| iff this is an artifact chunk.
bool IsArtifactChunkId(TChunkId id);

//! Returns |true| iff this is a chunk or any type (journal or blob, replicated or erasure-coded).
bool IsPhysicalChunkType(NObjectClient::EObjectType type);

//! Returns |true| iff this is a chunk or any type (journal or blob, replicated or erasure-coded).
bool IsPhysicalChunkId(TChunkId id);

//! Returns |true| iff this is a journal chunk type.
bool IsJournalChunkType(NObjectClient::EObjectType type);

//! Returns |true| iff this is a journal chunk.
bool IsJournalChunkId(TChunkId id);

//! Returns |true| iff this is a blob chunk (regular or erasure).
bool IsBlobChunkType(NObjectClient::EObjectType type);

//! Returns |true| iff this is a blob chunk (regular or erasure).
bool IsBlobChunkId(TChunkId id);

//! Returns |true| iff this is an erasure chunk.
bool IsErasureChunkType(NObjectClient::EObjectType type);

//! Returns |true| iff this is an erasure chunk.
bool IsErasureChunkId(TChunkId id);

//! Returns |true| iff this is an erasure chunk part.
bool IsErasureChunkPartType(NObjectClient::EObjectType type);

//! Returns |true| iff this is an erasure chunk part.
bool IsErasureChunkPartId(TChunkId id);

//! Returns |true| iff this is a regular (not erasure) chunk.
inline bool IsRegularChunkType(NObjectClient::EObjectType type);

//! Returns |true| iff this is a regular (not erasure) chunk.
inline bool IsRegularChunkId(TChunkId id);

//! Returns id for a part of a given erasure chunk.
TChunkId ErasurePartIdFromChunkId(TChunkId id, int index);

//! Returns the whole chunk id for a given erasure chunk part id.
TChunkId ErasureChunkIdFromPartId(TChunkId id);

//! Returns part index for a given erasure chunk part id.
int ReplicaIndexFromErasurePartId(TChunkId id);

//! For usual chunks, preserves the id.
//! For erasure chunks, constructs the part id using the given replica index.
TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex);

//! For regular chunks, preserves the id and returns #GenericChunkReplicaIndex.
//! For erasure chunk parts, constructs the whole chunk id and extracts part index.
TChunkIdWithIndex DecodeChunkId(TChunkId id);

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaAddressFormatter
{
public:
    explicit TChunkReplicaAddressFormatter(NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

    void operator()(TStringBuilderBase* builder, TChunkReplicaWithMedium replica) const;

    void operator()(TStringBuilderBase* builder, TChunkReplica replica) const;

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

//! A hasher for TChunkIdWithIndex.
template <>
struct THash<NYT::NChunkClient::TChunkIdWithIndex>
{
    size_t operator()(const NYT::NChunkClient::TChunkIdWithIndex& value) const;
};

//! A hasher for TChunkIdWithIndexes.
template <>
struct THash<NYT::NChunkClient::TChunkIdWithIndexes>
{
    size_t operator()(const NYT::NChunkClient::TChunkIdWithIndexes& value) const;
};

////////////////////////////////////////////////////////////////////////////////

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
