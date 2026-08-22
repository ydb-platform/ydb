#include "distributed_file_session.h"

#include <yt/yt/client/signature/signature.h>

namespace NYT::NApi {

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TWriteFileFragmentResult::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("cookie_id", &TThis::CookieId);
    registrar.Parameter("chunk_list_id", &TThis::ChunkListId);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFileFragmentCookieData::Register(TRegistrar registrar)
{
    registrar.Parameter("rich_path", &TThis::RichPath);
    registrar.Parameter("file_attributes", &TThis::FileAttributes);

    registrar.Parameter("main_transaction_id", &TThis::MainTransactionId);
    registrar.Parameter("file_id", &TThis::FileId);
    registrar.Parameter("external_cell_tag", &TThis::ExternalCellTag);
}

void TWriteFileFragmentCookie::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("cookie_id", &TThis::CookieId);

    registrar.Parameter("cookie_data", &TThis::CookieData);
}

////////////////////////////////////////////////////////////////////////////////

TDistributedWriteFileSession::TDistributedWriteFileSession(
    TTransactionId mainTransactionId,
    TTransactionId uploadTransactionId,
    TChunkListId rootChunkListId,
    TRichYPath richPath,
    TObjectId fileId,
    TCellTag externalCellTag,
    TTimestamp timestamp,
    INodePtr fileAttributes)
    : TDistributedWriteFileSession()
{
    RootChunkListId = rootChunkListId;
    UploadTransactionId = uploadTransactionId;
    Timestamp = timestamp;

    HostData.RichPath = std::move(richPath);
    HostData.FileAttributes = std::move(fileAttributes);
    HostData.MainTransactionId = mainTransactionId;
    HostData.FileId = fileId;
    HostData.ExternalCellTag = externalCellTag;
}

TWriteFileFragmentCookie TDistributedWriteFileSession::CookieFromThis() const
{
    auto cookie = TWriteFileFragmentCookie();
    cookie.SessionId = RootChunkListId;
    cookie.CookieId = TGuid::Create();

    cookie.CookieData = HostData;

    return cookie;
}

void TDistributedWriteFileSession::Register(TRegistrar registrar)
{
    registrar.Parameter("root_chunk_list_id", &TThis::RootChunkListId);
    registrar.Parameter("upload_transaction_id", &TThis::UploadTransactionId);
    registrar.Parameter("timestamp", &TThis::Timestamp);

    registrar.Parameter("host_data", &TThis::HostData);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
