#pragma once

#include "public.h"

#include <yt/yt/client/table_client/table_upload_options.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/key.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteSessionId, TGuid);

////////////////////////////////////////////////////////////////////////////////

// Used by distributed table writer to patch
// its config based on table data.
// NB(arkady-e1ppa): TableUploadOptions are
// encoded in RichPath + TableAttributes and thus
// are not required to be stored directly.
struct TTableWriterPatchInfo
    : public NYTree::TYsonStructLite
{
public:
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

    TTableWriterPatchInfo(
        NYPath::TRichYPath richPath,
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        NTableClient::TMasterTableSchemaId chunkSchemaId,
        NTableClient::TTableSchemaPtr chunkSchema,
        std::optional<NTableClient::TLegacyOwningKey> writerLastKey,
        int maxHeavyColumns,
        NTransactionClient::TTimestamp timestamp,
        const NYTree::IAttributeDictionary& tableAttributes);

    REGISTER_YSON_STRUCT_LITE(TTableWriterPatchInfo)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TFragmentWriteResult
    : public NYTree::TYsonStructLite
{
    NTableClient::TLegacyOwningKey MinBoundaryKey;
    NTableClient::TLegacyOwningKey MaxBoundaryKey;
    NChunkClient::TChunkListId ChunkListId;

    REGISTER_YSON_STRUCT_LITE(TFragmentWriteResult)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TFragmentWriteCookie
    : public NYTree::TYsonStruct
{
public:
    const TTableWriterPatchInfo& GetPatchInfo() const;

    NCypressClient::TTransactionId GetMainTransactionId() const;
    NCypressClient::TTransactionId GetUploadTransactionId() const;

private:
    TDistributedWriteSessionId Id_;

    NCypressClient::TTransactionId MainTxId_;
    NCypressClient::TTransactionId UploadTxId_;

    TTableWriterPatchInfo PatchInfo_;

    std::vector<TFragmentWriteResult> WriteResults_;

    REGISTER_YSON_STRUCT(TFragmentWriteCookie);

    static void Register(TRegistrar registrar);

    friend class TDistributedWriteSession;
    friend struct IDistributedTableClientBase;
};

DEFINE_REFCOUNTED_TYPE(TFragmentWriteCookie);

////////////////////////////////////////////////////////////////////////////////

class TDistributedWriteSession
    : public NYTree::TYsonStruct
{
public:
    TDistributedWriteSession(
        NCypressClient::TTransactionId mainTxId,
        NCypressClient::TTransactionId uploadTxId,
        NChunkClient::TChunkListId rootChunkListId,
        TTableWriterPatchInfo patchInfo);

    NCypressClient::TTransactionId GetMainTransactionId() const;
    NCypressClient::TTransactionId GetUploadTransactionId() const;
    const TTableWriterPatchInfo& GetPatchInfo() const Y_LIFETIME_BOUND;
    NChunkClient::TChunkListId GetRootChunkListId() const;

    TFragmentWriteCookiePtr GiveCookie();
    void TakeCookie(TFragmentWriteCookiePtr cookie);

    TFuture<void> Ping(IClientPtr client);

    REGISTER_YSON_STRUCT(TDistributedWriteSession);

    static void Register(TRegistrar registrar);

private:
    TDistributedWriteSessionId Id_;

    NCypressClient::TTransactionId MainTxId_;
    NCypressClient::TTransactionId UploadTxId_;

    NChunkClient::TChunkListId RootChunkListId_;

    TTableWriterPatchInfo PatchInfo_;

    // This is used to commit changes when session is over.
    std::vector<TFragmentWriteResult> WriteResults_;

    friend struct IDistributedTableClientBase;
};

DEFINE_REFCOUNTED_TYPE(TDistributedWriteSession);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
