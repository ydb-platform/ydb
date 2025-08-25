#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_io_options.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/phoenix/context.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TEpochSchema
{
public:
    TEpochSchema() = default;
    TEpochSchema(const TEpochSchema& other);
    TEpochSchema& operator=(const TEpochSchema& other);

    TEpochSchema(const TEpochSchema&& other) = delete;
    TEpochSchema& operator=(TEpochSchema&& other) = delete;

    TEpochSchema& operator=(TTableSchemaPtr schema);

    const TTableSchema* operator->() const;
    const TTableSchemaPtr& operator*() const;

    const TTableSchemaPtr& Get() const;
    ui64 GetRevision() const;

    ui64 Set(const TTableSchemaPtr& schema);

    void Persist(const NPhoenix::TPersistenceContext& context);

    ui64 Reset();

private:
    TTableSchemaPtr TableSchema_ = New<TTableSchema>();
    ui64 Revision_ = 0;
};

struct TTableUploadOptions
{
    NChunkClient::EUpdateMode UpdateMode = NChunkClient::EUpdateMode::Overwrite;
    NCypressClient::ELockMode LockMode = NCypressClient::ELockMode::Exclusive;
    TEpochSchema TableSchema;
    TMasterTableSchemaId SchemaId;
    ETableSchemaModification SchemaModification = ETableSchemaModification::None;
    TVersionedWriteOptions VersionedWriteOptions;
    ETableSchemaMode SchemaMode = ETableSchemaMode::Strong;
    EOptimizeFor OptimizeFor = EOptimizeFor::Lookup;
    std::optional<NChunkClient::EChunkFormat> ChunkFormat;
    NCompression::ECodec CompressionCodec = NCompression::ECodec::None;
    NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
    bool EnableStripedErasure = false;
    std::optional<std::vector<NSecurityClient::TSecurityTag>> SecurityTags;
    bool PartiallySorted = false;

    TTableSchemaPtr GetUploadSchema() const;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

const std::vector<std::string>& GetTableUploadOptionsAttributeKeys();

TTableUploadOptions GetTableUploadOptions(
    const NYPath::TRichYPath& path,
    const NYTree::IAttributeDictionary& cypressTableAttributes,
    const TTableSchemaPtr& schema,
    i64 rowCount);

TTableUploadOptions GetFileUploadOptions(
    const NYPath::TRichYPath& path,
    const NYTree::IAttributeDictionary& cypressTableAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
