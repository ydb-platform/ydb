#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TWriteFileFragmentResult
    : public NYTree::TYsonStructLite
{
    TGuid SessionId;
    TGuid CookieId;

    NChunkClient::TChunkListId ChunkListId;

    REGISTER_YSON_STRUCT_LITE(TWriteFileFragmentResult)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteFileFragmentCookieData
    : public NYTree::TYsonStructLite
{
    NYPath::TRichYPath RichPath;
    NYTree::INodePtr FileAttributes;

    NCypressClient::TTransactionId MainTransactionId;
    NObjectClient::TObjectId FileId;
    NObjectClient::TCellTag ExternalCellTag;

    REGISTER_YSON_STRUCT_LITE(TWriteFileFragmentCookieData);

    static void Register(TRegistrar registrar);
};

struct TWriteFileFragmentCookie
    : public NYTree::TYsonStructLite
{
    TGuid SessionId;
    TGuid CookieId;

    TWriteFileFragmentCookieData CookieData;

    REGISTER_YSON_STRUCT_LITE(TWriteFileFragmentCookie);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDistributedWriteFileSession
    : public NYTree::TYsonStructLite
{
    NChunkClient::TChunkListId RootChunkListId;
    NCypressClient::TTransactionId UploadTransactionId;
    NTransactionClient::TTimestamp Timestamp;

    TWriteFileFragmentCookieData HostData;

    TDistributedWriteFileSession(
        NCypressClient::TTransactionId mainTransactionId,
        NCypressClient::TTransactionId uploadTransactionId,
        NChunkClient::TChunkListId rootChunkListId,
        NYPath::TRichYPath richPath,
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        NTransactionClient::TTimestamp timestamp,
        NYTree::INodePtr fileAttributes);

    TWriteFileFragmentCookie CookieFromThis() const;

    REGISTER_YSON_STRUCT_LITE(TDistributedWriteFileSession);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
