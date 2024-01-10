#pragma once

#include "client_reader.h"
#include "client_writer.h"
#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TYtPoller;

class TClientBase;
using TClientBasePtr = ::TIntrusivePtr<TClientBase>;

class TClient;
using TClientPtr = ::TIntrusivePtr<TClient>;

////////////////////////////////////////////////////////////////////////////////

class TClientBase
    : virtual public IClientBase
{
public:
    TClientBase(
        const TClientContext& context,
        const TTransactionId& transactionId,
        IClientRetryPolicyPtr retryPolicy);

    ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options) override;

    // cypress

    TNodeId Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options) override;

    void Remove(
        const TYPath& path,
        const TRemoveOptions& options) override;

    bool Exists(
        const TYPath& path,
        const TExistsOptions& options) override;

    TNode Get(
        const TYPath& path,
        const TGetOptions& options) override;

    void Set(
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options) override;

    void MultisetAttributes(
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options) override;

    TNode::TListType List(
        const TYPath& path,
        const TListOptions& options) override;

    TNodeId Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options) override;

    TNodeId Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options) override;

    TNodeId Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options) override;

    void Concatenate(
        const TVector<TRichYPath>& sourcePaths,
        const TRichYPath& destinationPath,
        const TConcatenateOptions& options) override;

    TRichYPath CanonizeYPath(const TRichYPath& path) override;

    TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options) override;

    TMultiTablePartitions GetTablePartitions(
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options) override;

    TMaybe<TYPath> GetFileFromCache(
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions()) override;

    TYPath PutFileToCache(
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = TPutFileToCacheOptions()) override;

    IFileReaderPtr CreateFileReader(
        const TRichYPath& path,
        const TFileReaderOptions& options) override;

    IFileWriterPtr CreateFileWriter(
        const TRichYPath& path,
        const TFileWriterOptions& options) override;

    TTableWriterPtr<::google::protobuf::Message> CreateTableWriter(
        const TRichYPath& path,
        const ::google::protobuf::Descriptor& descriptor,
        const TTableWriterOptions& options) override;

    TRawTableReaderPtr CreateRawReader(
        const TRichYPath& path,
        const TFormat& format,
        const TTableReaderOptions& options) override;

    TRawTableWriterPtr CreateRawWriter(
        const TRichYPath& path,
        const TFormat& format,
        const TTableWriterOptions& options) override;

    IFileReaderPtr CreateBlobTableReader(
        const TYPath& path,
        const TKey& key,
        const TBlobTableReaderOptions& options) override;

    // operations

    IOperationPtr DoMap(
        const TMapOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> mapper,
        const TOperationOptions& options) override;

    IOperationPtr RawMap(
        const TRawMapOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        const TOperationOptions& options) override;

    IOperationPtr DoReduce(
        const TReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) override;

    IOperationPtr RawReduce(
        const TRawReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        const TOperationOptions& options) override;

    IOperationPtr DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) override;

    IOperationPtr RawJoinReduce(
        const TRawJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        const TOperationOptions& options) override;

    IOperationPtr DoMapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IStructuredJob> mapper,
        ::TIntrusivePtr<IStructuredJob> reduceCombiner,
        ::TIntrusivePtr<IStructuredJob> reducer,
        const TOperationOptions& options) override;

    IOperationPtr RawMapReduce(
        const TRawMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        ::TIntrusivePtr<IRawJob> reduceCombiner,
        ::TIntrusivePtr<IRawJob> reducer,
        const TOperationOptions& options) override;

    IOperationPtr Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options) override;

    IOperationPtr Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options) override;

    IOperationPtr Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options) override;

    IOperationPtr RemoteCopy(
        const TRemoteCopyOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) override;

    IOperationPtr RunVanilla(
        const TVanillaOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) override;

    IOperationPtr AttachOperation(const TOperationId& operationId) override;

    EOperationBriefState CheckOperation(const TOperationId& operationId) override;

    void AbortOperation(const TOperationId& operationId) override;

    void CompleteOperation(const TOperationId& operationId) override;

    void WaitForOperation(const TOperationId& operationId) override;

    void AlterTable(
        const TYPath& path,
        const TAlterTableOptions& options) override;

    TBatchRequestPtr CreateBatchRequest() override;

    IClientPtr GetParentClient() override;

    const TClientContext& GetContext() const;

    const IClientRetryPolicyPtr& GetRetryPolicy() const;

    virtual ITransactionPingerPtr GetTransactionPinger() = 0;

protected:
    virtual TClientPtr GetParentClientImpl() = 0;

protected:
    const TClientContext Context_;
    TTransactionId TransactionId_;
    IClientRetryPolicyPtr ClientRetryPolicy_;

private:
    ::TIntrusivePtr<TClientReader> CreateClientReader(
        const TRichYPath& path,
        const TFormat& format,
        const TTableReaderOptions& options,
        bool useFormatFromTableAttributes = false);

    THolder<TClientWriter> CreateClientWriter(
        const TRichYPath& path,
        const TFormat& format,
        const TTableWriterOptions& options);

    ::TIntrusivePtr<INodeReaderImpl> CreateNodeReader(
        const TRichYPath& path, const TTableReaderOptions& options) override;

    ::TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(
        const TRichYPath& path, const TTableReaderOptions& options) override;

    ::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
        const TRichYPath& path,
        const TTableReaderOptions& options,
        const Message* prototype) override;

    ::TIntrusivePtr<ISkiffRowReaderImpl> CreateSkiffRowReader(
        const TRichYPath& path,
        const TTableReaderOptions& options,
        const ISkiffRowSkipperPtr& skipper,
        const NSkiff::TSkiffSchemaPtr& schema) override;

    ::TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(
        const TRichYPath& path, const TTableWriterOptions& options) override;

    ::TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(
        const TRichYPath& path, const TTableWriterOptions& options) override;

    ::TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(
        const TRichYPath& path,
        const TTableWriterOptions& options,
        const Message* prototype) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public ITransaction
    , public TClientBase
{
public:
    //
    // Start a new transaction.
    TTransaction(
        TClientPtr parentClient,
        const TClientContext& context,
        const TTransactionId& parentTransactionId,
        const TStartTransactionOptions& options);

    //
    // Attach an existing transaction.
    TTransaction(
        TClientPtr parentClient,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TAttachTransactionOptions& options);

    const TTransactionId& GetId() const override;

    ILockPtr Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options) override;

    void Unlock(
        const TYPath& path,
        const TUnlockOptions& options) override;

    void Commit() override;

    void Abort() override;

    void Ping() override;

    void Detach() override;

    ITransactionPingerPtr GetTransactionPinger() override;

protected:
    TClientPtr GetParentClientImpl() override;

private:
    ITransactionPingerPtr TransactionPinger_;
    THolder<TPingableTransaction> PingableTx_;
    TClientPtr ParentClient_;
};

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
    , public TClientBase
{
public:
    TClient(
        const TClientContext& context,
        const TTransactionId& globalId,
        IClientRetryPolicyPtr retryPolicy);

    ~TClient();

    ITransactionPtr AttachTransaction(
        const TTransactionId& transactionId,
        const TAttachTransactionOptions& options) override;

    void MountTable(
        const TYPath& path,
        const TMountTableOptions& options) override;

    void UnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options) override;

    void RemountTable(
        const TYPath& path,
        const TRemountTableOptions& options) override;

    void FreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options) override;

    void UnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options) override;

    void ReshardTable(
        const TYPath& path,
        const TVector<TKey>& keys,
        const TReshardTableOptions& options) override;

    void ReshardTable(
        const TYPath& path,
        i64 tabletCount,
        const TReshardTableOptions& options) override;

    void InsertRows(
        const TYPath& path,
        const TNode::TListType& rows,
        const TInsertRowsOptions& options) override;

    void DeleteRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TDeleteRowsOptions& options) override;

    void TrimRows(
        const TYPath& path,
        i64 tabletIndex,
        i64 rowCount,
        const TTrimRowsOptions& options) override;

    TNode::TListType LookupRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TLookupRowsOptions& options) override;

    TNode::TListType SelectRows(
        const TString& query,
        const TSelectRowsOptions& options) override;

    void AlterTableReplica(
        const TReplicaId& replicaId,
        const TAlterTableReplicaOptions& alterTableReplicaOptions) override;

    ui64 GenerateTimestamp() override;

    TAuthorizationInfo WhoAmI() override;

    TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options) override;

    TOperationAttributes GetOperation(
        const TString& alias,
        const TGetOperationOptions& options) override;

    TListOperationsResult ListOperations(
        const TListOperationsOptions& options) override;

    void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options) override;

    TJobAttributes GetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options) override;

    TListJobsResult ListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options = TListJobsOptions()) override;

    IFileReaderPtr GetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& options = TGetJobInputOptions()) override;

    IFileReaderPtr GetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options = TGetJobFailContextOptions()) override;

    IFileReaderPtr GetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = TGetJobStderrOptions()) override;

    TNode::TListType SkyShareTable(
        const std::vector<TYPath>& tablePaths,
        const TSkyShareTableOptions& options = TSkyShareTableOptions()) override;

    TCheckPermissionResponse CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options) override;

    TVector<TTabletInfo> GetTabletInfos(
        const TYPath& path,
        const TVector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options) override;

    void SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options) override;

    void ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options) override;

    void Shutdown() override;

    ITransactionPingerPtr GetTransactionPinger() override;

    // Helper methods
    TYtPoller& GetYtPoller();

protected:
    TClientPtr GetParentClientImpl() override;

private:
    template <class TOptions>
    void SetTabletParams(
        THttpHeader& header,
        const TYPath& path,
        const TOptions& options);

    void CheckShutdown() const;

    ITransactionPingerPtr TransactionPinger_;

    std::atomic<bool> Shutdown_ = false;
    TMutex Lock_;
    THolder<TYtPoller> YtPoller_;
};

////////////////////////////////////////////////////////////////////////////////

TClientPtr CreateClientImpl(
    const TString& serverName,
    const TCreateClientOptions& options = TCreateClientOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
