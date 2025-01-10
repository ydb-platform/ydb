#pragma once

#include "public.h"

#include <yt/yt/client/table_client/table_upload_options.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/key.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

// Used by distributed table writer to patch
// its config based on table data.
// NB(arkady-e1ppa): TableUploadOptions are
// encoded in RichPath + TableAttributes and thus
// are not required to be stored directly.
struct TTableWriterPatchInfo
    : public NYTree::TYsonStructLite
{
    NObjectClient::TObjectId ObjectId;

    NYPath::TRichYPath RichPath;

    // NB(arkady-e1ppa): Empty writer last key Serialization into Deserialization
    // somehow results in a []; row which is considered non-empty for some reason.
    // This matters too little for me to bother saving up the std::optional wrapper.
    std::optional<NTableClient::TLegacyOwningKey> WriterLastKey;
    int MaxHeavyColumns;

    NTableClient::TMasterTableSchemaId ChunkSchemaId;
    NTableClient::TTableSchemaPtr ChunkSchema;

    NYTree::INodePtr TableAttributes;

    NObjectClient::TCellTag ExternalCellTag;

    NTransactionClient::TTimestamp Timestamp;

    REGISTER_YSON_STRUCT_LITE(TTableWriterPatchInfo)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteFragmentResult
    : public NYTree::TYsonStructLite
{
    TGuid SessionId;
    TGuid CookieId;

    NTableClient::TLegacyOwningKey MinBoundaryKey;
    NTableClient::TLegacyOwningKey MaxBoundaryKey;
    NChunkClient::TChunkListId ChunkListId;

    REGISTER_YSON_STRUCT_LITE(TWriteFragmentResult)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteFragmentCookie
    : public NYTree::TYsonStructLite
{
    TGuid SessionId;
    TGuid CookieId;

    NCypressClient::TTransactionId MainTransactionId;

    TTableWriterPatchInfo PatchInfo;

    REGISTER_YSON_STRUCT_LITE(TWriteFragmentCookie);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteSession
    : public NYTree::TYsonStructLite
{

    // RootChunkListId is used for indentification.
    NChunkClient::TChunkListId RootChunkListId;

    NCypressClient::TTransactionId MainTransactionId;
    NCypressClient::TTransactionId UploadTransactionId;

    TTableWriterPatchInfo PatchInfo;

    TDistributedWriteSession(
        NCypressClient::TTransactionId mainTransactionId,
        NCypressClient::TTransactionId uploadTransactionId,
        NChunkClient::TChunkListId rootChunkListId,
        NYPath::TRichYPath richPath,
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        NTableClient::TMasterTableSchemaId chunkSchemaId,
        NTableClient::TTableSchemaPtr chunkSchema,
        std::optional<NTableClient::TLegacyOwningKey> writerLastKey,
        int maxHeavyColumns,
        NTransactionClient::TTimestamp timestamp,
        const NYTree::IAttributeDictionary& tableAttributes);

    TWriteFragmentCookie CookieFromThis() const;

    REGISTER_YSON_STRUCT_LITE(TDistributedWriteSession);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
