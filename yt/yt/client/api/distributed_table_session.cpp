#include "distributed_table_session.h"

#include "client.h"
#include "transaction.h"

namespace NYT::NApi {

using namespace NYPath;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NCypressClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TTableWriterPatchInfo::TTableWriterPatchInfo(
    TRichYPath richPath,
    TObjectId objectId,
    TCellTag externalCellTag,
    TMasterTableSchemaId chunkSchemaId,
    TTableSchemaPtr chunkSchema,
    std::optional<TLegacyOwningKey> writerLastKey,
    int maxHeavyColumns,
    TTimestamp timestamp,
    const IAttributeDictionary& tableAttributes)
    : TTableWriterPatchInfo()
{
    ObjectId = objectId;
    RichPath = std::move(richPath);

    ChunkSchemaId = chunkSchemaId;
    ChunkSchema = std::move(chunkSchema);

    WriterLastKey = writerLastKey;
    MaxHeavyColumns = maxHeavyColumns;

    TableAttributes = tableAttributes.ToMap();

    ExternalCellTag = externalCellTag;

    Timestamp = timestamp;
}

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

void TFragmentWriteResult::Register(TRegistrar registrar)
{
    registrar.Parameter("min_boundary_key", &TThis::MinBoundaryKey);
    registrar.Parameter("max_boundary_key", &TThis::MaxBoundaryKey);
    registrar.Parameter("chunk_list_id", &TThis::ChunkListId);
}

////////////////////////////////////////////////////////////////////////////////

const TTableWriterPatchInfo& TFragmentWriteCookie::GetPatchInfo() const
{
    return PatchInfo_;
}

TTransactionId TFragmentWriteCookie::GetMainTransactionId() const
{
    return MainTxId_;
}

TTransactionId TFragmentWriteCookie::GetUploadTransactionId() const
{
    return UploadTxId_;
}

void TFragmentWriteCookie::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::Id_);

    registrar.Parameter("tx_id", &TThis::MainTxId_);
    registrar.Parameter("upload_tx_id", &TThis::UploadTxId_);

    registrar.Parameter("patch_info", &TThis::PatchInfo_);

    registrar.Parameter("writer_results", &TThis::WriteResults_);
}

////////////////////////////////////////////////////////////////////////////////

TDistributedWriteSession::TDistributedWriteSession(
    TTransactionId mainTxId,
    TTransactionId uploadTxId,
    TChunkListId rootChunkListId,
    TTableWriterPatchInfo patchInfo)
    : TDistributedWriteSession()
{
    Id_ = TDistributedWriteSessionId(TGuid::Create());
    MainTxId_ = mainTxId;
    UploadTxId_ = uploadTxId;

    RootChunkListId_ = rootChunkListId;

    PatchInfo_ = std::move(patchInfo);

    // Signatures?
}

TTransactionId TDistributedWriteSession::GetMainTransactionId() const
{
    return MainTxId_;
}

TTransactionId TDistributedWriteSession::GetUploadTransactionId() const
{
    return UploadTxId_;
}

const TTableWriterPatchInfo& TDistributedWriteSession::GetPatchInfo() const Y_LIFETIME_BOUND
{
    return PatchInfo_;
}

TChunkListId TDistributedWriteSession::GetRootChunkListId() const
{
    return RootChunkListId_;
}

TFragmentWriteCookiePtr TDistributedWriteSession::GiveCookie()
{
    auto cookie = New<TFragmentWriteCookie>();
    cookie->Id_ = Id_;
    cookie->MainTxId_ = MainTxId_;
    cookie->UploadTxId_ = UploadTxId_;
    cookie->PatchInfo_ = PatchInfo_;

    return cookie;
}

void TDistributedWriteSession::TakeCookie(TFragmentWriteCookiePtr cookie)
{
    // Verify cookie signature?
    WriteResults_.reserve(std::ssize(WriteResults_) + std::ssize(cookie->WriteResults_));
    for (const auto& writeResult : cookie->WriteResults_) {
        WriteResults_.push_back(writeResult);
    }
}

TFuture<void> TDistributedWriteSession::Ping(IClientPtr client)
{
    // NB(arkady-e1ppa): AutoAbort = false by default.
    auto mainTx = client->AttachTransaction(MainTxId_);

    return mainTx->Ping();
}

void TDistributedWriteSession::Register(TRegistrar registrar)
{
    registrar.Parameter("session_id", &TThis::Id_);
    registrar.Parameter("tx_id", &TThis::MainTxId_);
    registrar.Parameter("upload_tx_id", &TThis::UploadTxId_);

    registrar.Parameter("root_chunk_list_id", &TThis::RootChunkListId_);

    registrar.Parameter("patch_info", &TThis::PatchInfo_);

    registrar.Parameter("write_results", &TThis::WriteResults_);
}

////////////////////////////////////////////////////////////////////////////////

void* IDistributedTableClientBase::GetOpaqueDistributedWriteResults(Y_LIFETIME_BOUND const TDistributedWriteSessionPtr& session)
{
    return static_cast<void*>(&session->WriteResults_);
}

void IDistributedTableClientBase::RecordOpaqueWriteResult(const TFragmentWriteCookiePtr& cookie, void* opaqueWriteResult)
{
    auto* concrete = static_cast<TFragmentWriteResult*>(opaqueWriteResult);
    YT_ASSERT(concrete);
    cookie->WriteResults_.push_back(*concrete);
}

} // namespace NYT::NApi
