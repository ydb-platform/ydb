#include "operation_preparer.h"

#include "init.h"
#include "file_writer.h"
#include "operation.h"
#include "operation_helpers.h"
#include "operation_tracker.h"
#include "transaction.h"
#include "transaction_pinger.h"
#include "yt_poller.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>
#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>

#include <yt/cpp/mapreduce/interface/error_codes.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/folder/path.h>

#include <util/string/builder.h>

#include <util/system/execpath.h>

namespace NYT::NDetail {

using namespace NRawClient;

////////////////////////////////////////////////////////////////////////////////

class TWaitOperationStartPollerItem
    : public IYtPollerItem
{
public:
    TWaitOperationStartPollerItem(TOperationId operationId, THolder<TPingableTransaction> transaction)
        : OperationId_(operationId)
        , Transaction_(std::move(transaction))
    { }

    void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        Future_ = batchRequest->GetOperation(
            OperationId_,
            TGetOperationOptions().AttributeFilter(
                TOperationAttributeFilter().Add(EOperationAttribute::State)));
    }

    EStatus OnRequestExecuted() override
    {
        try {
            auto attributes = Future_.GetValue();
            Y_ENSURE(attributes.State.Defined());
            bool operationHasLockedFiles =
                *attributes.State != "starting" &&
                *attributes.State != "pending" &&
                *attributes.State != "orphaned" &&
                *attributes.State != "waiting_for_agent" &&
                *attributes.State != "initializing";
            return operationHasLockedFiles ? EStatus::PollBreak : EStatus::PollContinue;
        } catch (const TErrorResponse& e) {
            YT_LOG_ERROR("get_operation request failed: %v (RequestId: %v)",
                e.GetError().GetMessage(),
                e.GetRequestId());
            return IsRetriable(e) ? PollContinue : PollBreak;
        } catch (const std::exception& e) {
            YT_LOG_ERROR("%v", e.what());
            return PollBreak;
        }
    }

    void OnItemDiscarded() override {
    }

private:
    TOperationId OperationId_;
    THolder<TPingableTransaction> Transaction_;
    ::NThreading::TFuture<TOperationAttributes> Future_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationForwardingRequestRetryPolicy
    : public IRequestRetryPolicy
{
public:
    TOperationForwardingRequestRetryPolicy(const IRequestRetryPolicyPtr& underlying, const TOperationPtr& operation)
        : Underlying_(underlying)
        , Operation_(operation)
    { }

    void NotifyNewAttempt() override
    {
        Underlying_->NotifyNewAttempt();
    }

    TMaybe<TDuration> OnGenericError(const std::exception& e) override
    {
        UpdateOperationStatus(e.what());
        return Underlying_->OnGenericError(e);
    }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        auto msg = e.GetError().ShortDescription();
        UpdateOperationStatus(msg);
        return Underlying_->OnRetriableError(e);
    }

    void OnIgnoredError(const TErrorResponse& e) override
    {
        Underlying_->OnIgnoredError(e);
    }

    TString GetAttemptDescription() const override
    {
        return Underlying_->GetAttemptDescription();
    }

private:
    void UpdateOperationStatus(TStringBuf err)
    {
        Y_ABORT_UNLESS(Operation_);
        Operation_->OnStatusUpdated(
            ::TStringBuilder() << "Retriable error during operation start: " << err);
    }

private:
    IRequestRetryPolicyPtr Underlying_;
    TOperationPtr Operation_;
};

////////////////////////////////////////////////////////////////////////////////

TOperationPreparer::TOperationPreparer(TClientPtr client, TTransactionId transactionId)
    : Client_(std::move(client))
    , TransactionId_(transactionId)
    , FileTransaction_(MakeHolder<TPingableTransaction>(
        Client_->GetRetryPolicy(),
        Client_->GetContext(),
        TransactionId_,
        Client_->GetTransactionPinger()->GetChildTxPinger(),
        TStartTransactionOptions()))
    , ClientRetryPolicy_(Client_->GetRetryPolicy())
    , PreparationId_(CreateGuidAsString())
{ }

const TClientContext& TOperationPreparer::GetContext() const
{
    return Client_->GetContext();
}

TTransactionId TOperationPreparer::GetTransactionId() const
{
    return TransactionId_;
}

TClientPtr TOperationPreparer::GetClient() const
{
    return Client_;
}

const TString& TOperationPreparer::GetPreparationId() const
{
    return PreparationId_;
}

const IClientRetryPolicyPtr& TOperationPreparer::GetClientRetryPolicy() const
{
    return ClientRetryPolicy_;
}

TOperationId TOperationPreparer::StartOperation(
    TOperation* operation,
    const TString& operationType,
    const TNode& spec,
    bool useStartOperationRequest)
{
    CheckValidity();

    THttpHeader header("POST", (useStartOperationRequest ? "start_op" : operationType));
    if (useStartOperationRequest) {
        header.AddParameter("operation_type", operationType);
    }
    header.AddTransactionId(TransactionId_);
    header.AddMutationId();

    auto ysonSpec = NodeToYsonString(spec);
    auto responseInfo = RetryRequestWithPolicy(
        ::MakeIntrusive<TOperationForwardingRequestRetryPolicy>(
            ClientRetryPolicy_->CreatePolicyForStartOperationRequest(),
            TOperationPtr(operation)),
        GetContext(),
        header,
        ysonSpec);
    TOperationId operationId = ParseGuidFromResponse(responseInfo.Response);
    YT_LOG_DEBUG("Operation started (OperationId: %v; PreparationId: %v)",
        operationId,
        GetPreparationId());

    YT_LOG_INFO("Operation %v started (%v): %v",
        operationId,
        operationType,
        GetOperationWebInterfaceUrl(GetContext().ServerName, operationId));

    TOperationExecutionTimeTracker::Get()->Start(operationId);

    Client_->GetYtPoller().Watch(
        new TWaitOperationStartPollerItem(operationId, std::move(FileTransaction_)));

    return operationId;
}

void TOperationPreparer::LockFiles(TVector<TRichYPath>* paths)
{
    CheckValidity();

    TVector<::NThreading::TFuture<TLockId>> lockIdFutures;
    lockIdFutures.reserve(paths->size());
    TRawBatchRequest lockRequest(GetContext().Config);
    for (const auto& path : *paths) {
        lockIdFutures.push_back(lockRequest.Lock(
            FileTransaction_->GetId(),
            path.Path_,
            ELockMode::LM_SNAPSHOT,
            TLockOptions().Waitable(true)));
    }
    ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), GetContext(), lockRequest);

    TVector<::NThreading::TFuture<TNode>> nodeIdFutures;
    nodeIdFutures.reserve(paths->size());
    TRawBatchRequest getNodeIdRequest(GetContext().Config);
    for (const auto& lockIdFuture : lockIdFutures) {
        nodeIdFutures.push_back(getNodeIdRequest.Get(
            FileTransaction_->GetId(),
            ::TStringBuilder() << '#' << GetGuidAsString(lockIdFuture.GetValue()) << "/@node_id",
            TGetOptions()));
    }
    ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), GetContext(), getNodeIdRequest);

    for (size_t i = 0; i != paths->size(); ++i) {
        auto& richPath = (*paths)[i];
        richPath.OriginalPath(richPath.Path_);
        richPath.Path("#" + nodeIdFutures[i].GetValue().AsString());
        YT_LOG_DEBUG("Locked file %v, new path is %v",
            *richPath.OriginalPath_,
            richPath.Path_);
    }
}

void TOperationPreparer::CheckValidity() const
{
    Y_ENSURE(
        FileTransaction_,
        "File transaction is already moved, are you trying to use preparer for more than one operation?");
}

////////////////////////////////////////////////////////////////////////////////

class TRetryPolicyIgnoringLockConflicts
    : public TAttemptLimitedRetryPolicy
{
public:
    using TAttemptLimitedRetryPolicy::TAttemptLimitedRetryPolicy;
    using TAttemptLimitedRetryPolicy::OnGenericError;

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        if (IsAttemptLimitExceeded()) {
            return Nothing();
        }
        if (e.IsConcurrentTransactionLockConflict()) {
            return GetBackoffDuration(Config_);
        }
        return TAttemptLimitedRetryPolicy::OnRetriableError(e);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileToUpload
    : public IItemToUpload
{
public:
    TFileToUpload(TString fileName, TMaybe<TString> md5)
        : FileName_(std::move(fileName))
        , MD5_(std::move(md5))
    { }

    TString CalculateMD5() const override
    {
        if (MD5_) {
            return *MD5_;
        }
        constexpr size_t md5Size = 32;
        TString result;
        result.ReserveAndResize(md5Size);
        MD5::File(FileName_.data(), result.Detach());
        MD5_ = result;
        return result;
    }

    THolder<IInputStream> CreateInputStream() const override
    {
        return MakeHolder<TFileInput>(FileName_);
    }

    TString GetDescription() const override
    {
        return FileName_;
    }

    i64 GetDataSize() const override
    {
        return GetFileLength(FileName_);
    }

private:
    TString FileName_;
    mutable TMaybe<TString> MD5_;
};

class TDataToUpload
    : public IItemToUpload
{
public:
    TDataToUpload(TString data, TString description)
        : Data_(std::move(data))
        , Description_(std::move(description))
    { }

    TString CalculateMD5() const override
    {
        constexpr size_t md5Size = 32;
        TString result;
        result.ReserveAndResize(md5Size);
        MD5::Data(reinterpret_cast<const unsigned char*>(Data_.data()), Data_.size(), result.Detach());
        return result;
    }

    THolder<IInputStream> CreateInputStream() const override
    {
        return MakeHolder<TMemoryInput>(Data_.data(), Data_.size());
    }

    TString GetDescription() const override
    {
        return Description_;
    }

    i64 GetDataSize() const override
    {
        return std::ssize(Data_);
    }

private:
    TString Data_;
    TString Description_;
};

////////////////////////////////////////////////////////////////////////////////

static const TString& GetPersistentExecPathMd5()
{
    static TString md5 = MD5::File(GetPersistentExecPath());
    return md5;
}

static TMaybe<TSmallJobFile> GetJobState(const IJob& job)
{
    TString result;
    {
        TStringOutput output(result);
        job.Save(output);
        output.Finish();
    }
    if (result.empty()) {
        return Nothing();
    } else {
        return TSmallJobFile{"jobstate", result};
    }
}

////////////////////////////////////////////////////////////////////////////////

TJobPreparer::TJobPreparer(
    TOperationPreparer& operationPreparer,
    const TUserJobSpec& spec,
    const IJob& job,
    size_t outputTableCount,
    const TVector<TSmallJobFile>& smallFileList,
    const TOperationOptions& options)
    : OperationPreparer_(operationPreparer)
    , Spec_(spec)
    , Options_(options)
    , Layers_(spec.Layers_)
{

    CreateStorage();
    auto cypressFileList = CanonizeYPaths(/* retryPolicy */ nullptr, OperationPreparer_.GetContext(), spec.Files_);

    for (const auto& file : cypressFileList) {
        UseFileInCypress(file);
    }
    for (const auto& localFile : spec.GetLocalFiles()) {
        UploadLocalFile(std::get<0>(localFile), std::get<1>(localFile));
    }
    auto jobStateSmallFile = GetJobState(job);
    if (jobStateSmallFile) {
        UploadSmallFile(*jobStateSmallFile);
    }
    for (const auto& smallFile : smallFileList) {
        UploadSmallFile(smallFile);
    }

    if (auto commandJob = dynamic_cast<const ICommandJob*>(&job)) {
        IsCommandJob_ = true;
        ClassName_ = TJobFactory::Get()->GetJobName(&job);
        Command_ = commandJob->GetCommand();
    } else {
        PrepareJobBinary(job, outputTableCount, jobStateSmallFile.Defined());
    }

    operationPreparer.LockFiles(&CachedFiles_);
}

TVector<TRichYPath> TJobPreparer::GetFiles() const
{
    TVector<TRichYPath> allFiles = CypressFiles_;
    allFiles.insert(allFiles.end(), CachedFiles_.begin(), CachedFiles_.end());
    return allFiles;
}

TVector<TYPath> TJobPreparer::GetLayers() const
{
    return Layers_;
}

const TString& TJobPreparer::GetClassName() const
{
    return ClassName_;
}

const TString& TJobPreparer::GetCommand() const
{
    return Command_;
}

const TUserJobSpec& TJobPreparer::GetSpec() const
{
    return Spec_;
}

bool TJobPreparer::ShouldMountSandbox() const
{
    return OperationPreparer_.GetContext().Config->MountSandboxInTmpfs || Options_.MountSandboxInTmpfs_;
}

ui64 TJobPreparer::GetTotalFileSize() const
{
    return TotalFileSize_;
}

bool TJobPreparer::ShouldRedirectStdoutToStderr() const
{
    return !IsCommandJob_ && OperationPreparer_.GetContext().Config->RedirectStdoutToStderr;
}

TString TJobPreparer::GetFileStorage() const
{
    return Options_.FileStorage_ ?
        *Options_.FileStorage_ :
        OperationPreparer_.GetContext().Config->RemoteTempFilesDirectory;
}

TYPath TJobPreparer::GetCachePath() const
{
    return AddPathPrefix(
        ::TStringBuilder() << GetFileStorage() << "/new_cache",
        OperationPreparer_.GetContext().Config->Prefix);
}

void TJobPreparer::CreateStorage() const
{
    Create(
        OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        OperationPreparer_.GetContext(),
        Options_.FileStorageTransactionId_,
        GetCachePath(),
        NT_MAP,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
}

int TJobPreparer::GetFileCacheReplicationFactor() const
{
    if (IsLocalMode()) {
        return 1;
    } else {
        return OperationPreparer_.GetContext().Config->FileCacheReplicationFactor;
    }
}

void TJobPreparer::CreateFileInCypress(const TString& path) const
{
    auto attributes = TNode()("replication_factor", GetFileCacheReplicationFactor());
    if (Options_.FileExpirationTimeout_) {
        attributes["expiration_timeout"] = Options_.FileExpirationTimeout_->MilliSeconds();
    }

    Create(
        OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        OperationPreparer_.GetContext(),
        Options_.FileStorageTransactionId_,
        path,
        NT_FILE,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true)
            .Attributes(attributes)
    );
}

TString TJobPreparer::PutFileToCypressCache(
    const TString& path,
    const TString& md5Signature,
    TTransactionId transactionId) const
{
    constexpr ui32 LockConflictRetryCount = 30;
    auto retryPolicy = MakeIntrusive<TRetryPolicyIgnoringLockConflicts>(
        LockConflictRetryCount,
        OperationPreparer_.GetContext().Config);

    auto putFileToCacheOptions = TPutFileToCacheOptions();
    if (Options_.FileExpirationTimeout_) {
        putFileToCacheOptions.PreserveExpirationTimeout(true);
    }

    auto cachePath = PutFileToCache(
        retryPolicy,
        OperationPreparer_.GetContext(),
        transactionId,
        path,
        md5Signature,
        GetCachePath(),
        putFileToCacheOptions);

    Remove(
        OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        OperationPreparer_.GetContext(),
        transactionId,
        path,
        TRemoveOptions().Force(true));

    return cachePath;
}

TMaybe<TString> TJobPreparer::GetItemFromCypressCache(const TString& md5Signature, const TString& fileName) const
{
    constexpr ui32 LockConflictRetryCount = 30;
    auto retryPolicy = MakeIntrusive<TRetryPolicyIgnoringLockConflicts>(
        LockConflictRetryCount,
        OperationPreparer_.GetContext().Config);
    auto maybePath = GetFileFromCache(
        retryPolicy,
        OperationPreparer_.GetContext(),
        TTransactionId(),
        md5Signature,
        GetCachePath(),
        TGetFileFromCacheOptions());
    if (maybePath) {
        YT_LOG_DEBUG(
            "File is already in cache (FileName: %v, FilePath: %v)",
            fileName,
            *maybePath);
    }
    return maybePath;
}

TDuration TJobPreparer::GetWaitForUploadTimeout(const IItemToUpload& itemToUpload) const
{
    TDuration extraTime = OperationPreparer_.GetContext().Config->WaitLockPollInterval +
        TDuration::MilliSeconds(100);
    auto dataSizeGb = (itemToUpload.GetDataSize() + 1_GB - 1) / 1_GB;
    dataSizeGb = Max<ui64>(dataSizeGb, 1);
    return extraTime + dataSizeGb * OperationPreparer_.GetContext().Config->CacheLockTimeoutPerGb;
}

TString TJobPreparer::UploadToRandomPath(const IItemToUpload& itemToUpload) const
{
    TString uniquePath = AddPathPrefix(
        ::TStringBuilder() << GetFileStorage() << "/cpp_" << CreateGuidAsString(),
        OperationPreparer_.GetContext().Config->Prefix);
    YT_LOG_INFO("Uploading file to random cypress path (FileName: %v; CypressPath: %v; PreparationId: %v)",
        itemToUpload.GetDescription(),
        uniquePath,
        OperationPreparer_.GetPreparationId());

    CreateFileInCypress(uniquePath);

    {
        TFileWriter writer(
            uniquePath,
            OperationPreparer_.GetClientRetryPolicy(),
            OperationPreparer_.GetClient()->GetTransactionPinger(),
            OperationPreparer_.GetContext(),
            Options_.FileStorageTransactionId_,
            TFileWriterOptions().ComputeMD5(true));
        itemToUpload.CreateInputStream()->ReadAll(writer);
        writer.Finish();
    }
    return uniquePath;
}

TMaybe<TString> TJobPreparer::TryUploadWithDeduplication(const IItemToUpload& itemToUpload) const
{
    const auto md5Signature = itemToUpload.CalculateMD5();

    auto fileName = ::TStringBuilder() << GetFileStorage() << "/cpp_md5_" << md5Signature;
    if (OperationPreparer_.GetContext().Config->CacheUploadDeduplicationMode == EUploadDeduplicationMode::Host) {
        fileName << "_" << MD5::Data(TProcessState::Get()->FqdnHostName);
    }
    TString cypressPath = AddPathPrefix(fileName, OperationPreparer_.GetContext().Config->Prefix);

    CreateFileInCypress(cypressPath);

    auto uploadTx = MakeIntrusive<TTransaction>(
        OperationPreparer_.GetClient(),
        OperationPreparer_.GetContext(),
        TTransactionId(),
        TStartTransactionOptions());

    ILockPtr lock;
    try {
        lock = uploadTx->Lock(cypressPath, ELockMode::LM_EXCLUSIVE, TLockOptions().Waitable(true));
    } catch (const TErrorResponse& e) {
        if (e.IsResolveError()) {
            // If the node doesn't exist, it must be removed by concurrent uploading process.
            // Let's try to find it in the cache.
            return GetItemFromCypressCache(md5Signature, itemToUpload.GetDescription());
        }
        throw;
    }

    auto waitTimeout = GetWaitForUploadTimeout(itemToUpload);
    YT_LOG_DEBUG("Waiting for the lock on file (FileName: %v; CypressPath: %v; LockTimeout: %v)",
            itemToUpload.GetDescription(),
            cypressPath,
            waitTimeout);

    if (!TWaitProxy::Get()->WaitFuture(lock->GetAcquiredFuture(), waitTimeout)) {
        YT_LOG_DEBUG("Waiting for the lock timed out. Fallback to random path uploading (FileName: %v; CypressPath: %v)",
            itemToUpload.GetDescription(),
            cypressPath);
        return Nothing();
    }

    YT_LOG_DEBUG("Exclusive lock successfully acquired (FileName: %v; CypressPath: %v)",
            itemToUpload.GetDescription(),
            cypressPath);

    // Ensure that this process is the first to take a lock.
    if (auto cachedItemPath = GetItemFromCypressCache(md5Signature, itemToUpload.GetDescription())) {
        return *cachedItemPath;
    }

    YT_LOG_INFO("Uploading file to cypress (FileName: %v; CypressPath: %v; PreparationId: %v)",
        itemToUpload.GetDescription(),
        cypressPath,
        OperationPreparer_.GetPreparationId());

    {
        auto writer = uploadTx->CreateFileWriter(cypressPath, TFileWriterOptions().ComputeMD5(true));
        YT_VERIFY(writer);
        itemToUpload.CreateInputStream()->ReadAll(*writer);
        writer->Finish();
    }

    auto path = PutFileToCypressCache(cypressPath, md5Signature, uploadTx->GetId());

    uploadTx->Commit();
    return path;
}

TString TJobPreparer::UploadToCacheUsingApi(const IItemToUpload& itemToUpload) const
{
    auto md5Signature = itemToUpload.CalculateMD5();
    Y_ABORT_UNLESS(md5Signature.size() == 32);

    if (auto cachedItemPath = GetItemFromCypressCache(md5Signature, itemToUpload.GetDescription())) {
        return *cachedItemPath;
    }

    YT_LOG_INFO("File not found in cache; uploading to cypress (FileName: %v; PreparationId: %v)",
        itemToUpload.GetDescription(),
        OperationPreparer_.GetPreparationId());

    const auto& config = OperationPreparer_.GetContext().Config;

    if (config->CacheUploadDeduplicationMode != EUploadDeduplicationMode::Disabled &&
        itemToUpload.GetDataSize() > config->CacheUploadDeduplicationThreshold) {
        if (auto path = TryUploadWithDeduplication(itemToUpload)) {
            return *path;
        }
    }

    auto path = UploadToRandomPath(itemToUpload);
    return PutFileToCypressCache(path, md5Signature, Options_.FileStorageTransactionId_);
}

TString TJobPreparer::UploadToCache(const IItemToUpload& itemToUpload) const
{
    YT_LOG_INFO("Uploading file (FileName: %v; PreparationId: %v)",
        itemToUpload.GetDescription(),
        OperationPreparer_.GetPreparationId());

    TString result;
    switch (Options_.FileCacheMode_) {
        case TOperationOptions::EFileCacheMode::ApiCommandBased:
            Y_ENSURE_EX(Options_.FileStorageTransactionId_.IsEmpty(), TApiUsageError() <<
                "Default cache mode (API command-based) doesn't allow non-default 'FileStorageTransactionId_'");
            result = UploadToCacheUsingApi(itemToUpload);
            break;
        case TOperationOptions::EFileCacheMode::CachelessRandomPathUpload:
            result = UploadToRandomPath(itemToUpload);
            break;
        default:
            Y_ABORT("Unknown file cache mode: %d", static_cast<int>(Options_.FileCacheMode_));
    }

    YT_LOG_INFO("Complete uploading file (FileName: %v; PreparationId: %v)",
        itemToUpload.GetDescription(),
        OperationPreparer_.GetPreparationId());

    return result;
}

void TJobPreparer::UseFileInCypress(const TRichYPath& file)
{
    if (!Exists(
        OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        OperationPreparer_.GetContext(),
        file.TransactionId_.GetOrElse(OperationPreparer_.GetTransactionId()),
        file.Path_))
    {
        ythrow yexception() << "File " << file.Path_ << " does not exist";
    }

    if (ShouldMountSandbox()) {
        auto size = Get(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetContext(),
            file.TransactionId_.GetOrElse(OperationPreparer_.GetTransactionId()),
            file.Path_ + "/@uncompressed_data_size")
            .AsInt64();

        TotalFileSize_ += RoundUpFileSize(static_cast<ui64>(size));
    }
    CypressFiles_.push_back(file);
}

void TJobPreparer::UploadLocalFile(
    const TLocalFilePath& localPath,
    const TAddLocalFileOptions& options,
    bool isApiFile)
{
    TFsPath fsPath(localPath);
    fsPath.CheckExists();

    TFileStat stat;
    fsPath.Stat(stat);

    bool isExecutable = stat.Mode & (S_IXUSR | S_IXGRP | S_IXOTH);
    auto cachePath = UploadToCache(TFileToUpload(localPath, options.MD5CheckSum_));

    TRichYPath cypressPath;
    if (isApiFile) {
        cypressPath = OperationPreparer_.GetContext().Config->ApiFilePathOptions;
    }
    cypressPath.Path(cachePath).FileName(options.PathInJob_.GetOrElse(fsPath.Basename()));
    if (isExecutable) {
        cypressPath.Executable(true);
    }
    if (options.BypassArtifactCache_) {
        cypressPath.BypassArtifactCache(*options.BypassArtifactCache_);
    }

    if (ShouldMountSandbox()) {
        TotalFileSize_ += RoundUpFileSize(stat.Size);
    }

    CachedFiles_.push_back(cypressPath);
}

void TJobPreparer::UploadBinary(const TJobBinaryConfig& jobBinary)
{
    if (std::holds_alternative<TJobBinaryLocalPath>(jobBinary)) {
        auto binaryLocalPath = std::get<TJobBinaryLocalPath>(jobBinary);
        auto opts = TAddLocalFileOptions().PathInJob("cppbinary");
        if (binaryLocalPath.MD5CheckSum) {
            opts.MD5CheckSum(*binaryLocalPath.MD5CheckSum);
        }
        UploadLocalFile(binaryLocalPath.Path, opts, /* isApiFile */ true);
    } else if (std::holds_alternative<TJobBinaryCypressPath>(jobBinary)) {
        auto binaryCypressPath = std::get<TJobBinaryCypressPath>(jobBinary);
        TRichYPath ytPath = OperationPreparer_.GetContext().Config->ApiFilePathOptions;
        ytPath.Path(binaryCypressPath.Path);
        if (binaryCypressPath.TransactionId) {
            ytPath.TransactionId(*binaryCypressPath.TransactionId);
        }
        UseFileInCypress(ytPath.FileName("cppbinary").Executable(true));
    } else {
        Y_ABORT("%s", (::TStringBuilder() << "Unexpected jobBinary tag: " << jobBinary.index()).data());
    }
}

void TJobPreparer::UploadSmallFile(const TSmallJobFile& smallFile)
{
    auto cachePath = UploadToCache(TDataToUpload(smallFile.Data, smallFile.FileName + " [generated-file]"));
    auto path = OperationPreparer_.GetContext().Config->ApiFilePathOptions;
    CachedFiles_.push_back(path.Path(cachePath).FileName(smallFile.FileName));
    if (ShouldMountSandbox()) {
        TotalFileSize_ += RoundUpFileSize(smallFile.Data.size());
    }
}

bool TJobPreparer::IsLocalMode() const
{
    return UseLocalModeOptimization(OperationPreparer_.GetContext(), OperationPreparer_.GetClientRetryPolicy());
}

void TJobPreparer::PrepareJobBinary(const IJob& job, int outputTableCount, bool hasState)
{
    auto jobBinary = TJobBinaryConfig();
    if (!std::holds_alternative<TJobBinaryDefault>(Spec_.GetJobBinary())) {
        jobBinary = Spec_.GetJobBinary();
    }
    TString binaryPathInsideJob;
    if (std::holds_alternative<TJobBinaryDefault>(jobBinary)) {
        if (GetInitStatus() != EInitStatus::FullInitialization) {
            ythrow yexception() << "NYT::Initialize() must be called prior to any operation";
        }

        const bool isLocalMode = IsLocalMode();
        const TMaybe<TString> md5 = !isLocalMode ? MakeMaybe(GetPersistentExecPathMd5()) : Nothing();
        jobBinary = TJobBinaryLocalPath{GetPersistentExecPath(), md5};

        if (isLocalMode) {
            binaryPathInsideJob = GetExecPath();
        }
    } else if (std::holds_alternative<TJobBinaryLocalPath>(jobBinary)) {
        const bool isLocalMode = IsLocalMode();
        if (isLocalMode) {
            binaryPathInsideJob = TFsPath(std::get<TJobBinaryLocalPath>(jobBinary).Path).RealPath();
        }
    }
    Y_ASSERT(!std::holds_alternative<TJobBinaryDefault>(jobBinary));

    // binaryPathInsideJob is only set when LocalModeOptimization option is on, so upload is not needed
    if (!binaryPathInsideJob) {
        binaryPathInsideJob = "./cppbinary";
        UploadBinary(jobBinary);
    }

    TString jobCommandPrefix = Options_.JobCommandPrefix_;
    if (!Spec_.JobCommandPrefix_.empty()) {
        jobCommandPrefix = Spec_.JobCommandPrefix_;
    }

    TString jobCommandSuffix = Options_.JobCommandSuffix_;
    if (!Spec_.JobCommandSuffix_.empty()) {
        jobCommandSuffix = Spec_.JobCommandSuffix_;
    }

    ClassName_ = TJobFactory::Get()->GetJobName(&job);

    auto jobArguments = TNode::CreateMap();
    jobArguments["job_name"] = ClassName_;
    jobArguments["output_table_count"] = static_cast<i64>(outputTableCount);
    jobArguments["has_state"] = hasState;
    Spec_.AddEnvironment("YT_JOB_ARGUMENTS", NodeToYsonString(jobArguments));

    Command_ = ::TStringBuilder() <<
        jobCommandPrefix <<
        (OperationPreparer_.GetContext().Config->UseClientProtobuf ? "YT_USE_CLIENT_PROTOBUF=1" : "YT_USE_CLIENT_PROTOBUF=0") << " " <<
        binaryPathInsideJob <<
        jobCommandSuffix;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
