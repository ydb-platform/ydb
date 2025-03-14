#include "distributed_table_session.h"

#include "client.h"
#include "distributed_table_client.h"
#include "transaction.h"

#include <yt/yt/client/signature/signature.h>

namespace NYT::NApi {

using namespace NYPath;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TTableWriterPatchInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("table_id", &TThis::ObjectId);
    registrar.Parameter("table_path", &TThis::RichPath);

    registrar.Parameter("chunk_schema_id", &TThis::ChunkSchemaId);
    registrar.Parameter("chunk_schema", &TThis::ChunkSchema);

    registrar.Parameter("writer_last_key", &TThis::WriterLastKey);
    registrar.Parameter("max_heavy_columns", &TThis::MaxHeavyColumns);

    registrar.Parameter("table_attributes", &TThis::TableAttributes);

    registrar.Parameter("external_cell_tag", &TThis::ExternalCellTag);

    registrar.Parameter("timestamp", &TThis::Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFragmentResult::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("cookie_id", &TThis::CookieId);
    registrar.Parameter("min_boundary_key", &TThis::MinBoundaryKey);
    registrar.Parameter("max_boundary_key", &TThis::MaxBoundaryKey);
    registrar.Parameter("chunk_list_id", &TThis::ChunkListId);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFragmentCookie::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::SessionId);
    registrar.Parameter("cookie_id", &TThis::CookieId);

    registrar.Parameter("transaction_id", &TThis::MainTransactionId);

    registrar.Parameter("patch_info", &TThis::PatchInfo);
}

////////////////////////////////////////////////////////////////////////////////

TDistributedWriteSession::TDistributedWriteSession(
    TTransactionId mainTransactionId,
    TTransactionId uploadTransactionId,
    TChunkListId rootChunkListId,
    NYPath::TRichYPath richPath,
    NObjectClient::TObjectId objectId,
    NObjectClient::TCellTag externalCellTag,
    NTableClient::TMasterTableSchemaId chunkSchemaId,
    NTableClient::TTableSchemaPtr chunkSchema,
    std::optional<NTableClient::TLegacyOwningKey> writerLastKey,
    int maxHeavyColumns,
    NTransactionClient::TTimestamp timestamp,
    const NYTree::IAttributeDictionary& tableAttributes)
    : TDistributedWriteSession()
{
    MainTransactionId = mainTransactionId;
    UploadTransactionId = uploadTransactionId;

    RootChunkListId = rootChunkListId;

    PatchInfo.ObjectId = objectId;
    PatchInfo.RichPath = std::move(richPath);

    PatchInfo.ChunkSchemaId = chunkSchemaId;
    PatchInfo.ChunkSchema = std::move(chunkSchema);

    PatchInfo.WriterLastKey = std::move(writerLastKey);
    PatchInfo.MaxHeavyColumns = maxHeavyColumns;

    PatchInfo.TableAttributes = tableAttributes.ToMap();

    PatchInfo.ExternalCellTag = externalCellTag;

    PatchInfo.Timestamp = timestamp;
}

TWriteFragmentCookie TDistributedWriteSession::CookieFromThis() const
{
    auto cookie = TWriteFragmentCookie{};
    cookie.CookieId = TGuid::Create();
    cookie.SessionId = RootChunkListId;
    cookie.MainTransactionId = MainTransactionId;
    cookie.PatchInfo = PatchInfo;

    return cookie;
}

void TDistributedWriteSession::Register(TRegistrar registrar)
{
    registrar.Parameter("main_transaction_id", &TThis::MainTransactionId);
    registrar.Parameter("upload_transaction_id", &TThis::UploadTransactionId);

    registrar.Parameter("root_chunk_list_id", &TThis::RootChunkListId);

    registrar.Parameter("patch_info", &TThis::PatchInfo);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> PingDistributedWriteSession(
    const TSignedDistributedWriteSessionPtr& session,
    const IClientPtr& client)
{
    auto concreteSession = ConvertTo<TDistributedWriteSession>(TYsonStringBuf(session.Underlying()->Payload()));

    // NB(arkady-e1ppa): AutoAbort = false by default.
    auto mainTransaction = client->AttachTransaction(concreteSession.MainTransactionId);

    return mainTransaction->Ping();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
