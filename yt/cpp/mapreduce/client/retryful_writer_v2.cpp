#include "retryful_writer_v2.h"

#include <util/generic/scope.h>
#include <yt/cpp/mapreduce/client/retry_heavy_write_request.h>
#include <yt/cpp/mapreduce/client/transaction.h>
#include <yt/cpp/mapreduce/client/transaction_pinger.h>
#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http.h>

#include <util/system/condvar.h>

#include <queue>

namespace NYT::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRetryfulWriterV2::TSentBuffer
{
public:
    TSentBuffer() = default;
    TSentBuffer(const TSentBuffer& ) = delete;

    std::pair<std::shared_ptr<std::string>, ssize_t> Snapshot() const
    {
        return {Buffer_, Size_};
    }

    void Clear()
    {
        // This method can be called only if no other object is holding snapshot.
        Y_ABORT_IF(Buffer_.use_count() != 1);

        Size_ = 0;
    }

    ssize_t Size() const
    {
        return Size_;
    }

    void Append(const void* data, ssize_t size)
    {
        auto newSize = Size_ + size;
        if (newSize < Capacity_) {
            memcpy(Buffer_->data() + Size_, data, size);
        } else {
            // Closest power of 2 exceeding new size
            auto newCapacity = 1 << (MostSignificantBit(newSize) + 1);
            newCapacity = Max<ssize_t>(64, newCapacity);
            auto newBuffer = std::make_shared<std::string>();
            newBuffer->resize(newCapacity);
            memcpy(newBuffer->data(), Buffer_->data(), Size_);
            memcpy(newBuffer->data() + Size_, data, size);
            Buffer_ = newBuffer;
            Capacity_ = newCapacity;
        }
        Size_ = newSize;
    }

private:
    std::shared_ptr<std::string> Buffer_ = std::make_shared<std::string>();
    ssize_t Size_ = 0;
    ssize_t Capacity_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRetryfulWriterV2::TSender
{
public:
    TSender(TRichYPath path, THeavyRequestRetrier::TParameters parameters)
        : SenderThread_(
            [this, path=std::move(path), parameters=std::move(parameters)] {
                ThreadMain(std::move(path), parameters);
            })
    {
        SenderThread_.SetCurrentThreadName("retryful-writer-v2-sender");
        SenderThread_.Start();
    }

    ~TSender()
    {
        Abort();
        SenderThread_.Join();
    }

    bool IsRunning() const
    {
        auto g = Guard(Lock_);
        return State_.load() == EState::Running && !Error_;
    }

    void Abort()
    {
        auto g = Guard(Lock_);
        SetFinishedState(EState::Aborted, g);
    }

    void Finish()
    {
        {
            auto g = Guard(Lock_);
            SetFinishedState(EState::Completed, g);
        }
        SenderThread_.Join();
        CheckNoError();

        Y_ABORT_UNLESS(TaskIdQueue_.empty());
        Y_ABORT_UNLESS(TaskMap_.empty());
    }

    // Return future that is complete once upload of this buffer is successfully complete
    std::pair<NThreading::TFuture<void>, int> StartBlock()
    {
        auto g = Guard(Lock_);

        CheckNoError();

        auto taskId = NextTaskId_++;
        const auto& [it, inserted] = TaskMap_.emplace(taskId, TWriteTask{});
        Y_ABORT_IF(!inserted);
        TaskIdQueue_.push(taskId);
        HaveMoreData_.Signal();
        it->second.SendingComplete = NThreading::NewPromise();
        return {it->second.SendingComplete, taskId};
    }

    void UpdateBlock(int taskId, const TSentBuffer& buffer, bool complete)
    {
        auto snapshot = buffer.Snapshot();

        {
            auto g = Guard(Lock_);

            CheckNoError();

            auto it = TaskMap_.find(taskId);
            Y_ABORT_IF(it == TaskMap_.end());
            auto& writeTask = it->second;
            writeTask.Data = std::move(snapshot.first);
            writeTask.Size = snapshot.second;
            writeTask.BufferComplete = complete;

            if (TaskIdQueue_.empty() || TaskIdQueue_.back() != taskId) {
                TaskIdQueue_.push(taskId);
            }

            HaveMoreData_.Signal();
        }
    }

private:
    enum class EState;

private:
    void CheckNoError()
    {
        if (Error_) {
            std::rethrow_exception(Error_);
        }
    }

    void SetFinishedState(EState state, TGuard<TMutex>&)
    {
        if (State_ == EState::Running) {
            State_ = state;
        }
        HaveMoreData_.Signal();
    }

    void ThreadMain(TRichYPath path, const THeavyRequestRetrier::TParameters& parameters)
    {
        THolder<THeavyRequestRetrier> retrier;

        auto firstRequestParameters = parameters;
        auto restRequestParameters = parameters;

        {
            TNode firstPath = PathToNode(path);
            firstRequestParameters.Header.MergeParameters(TNode()("path", firstPath), /*overwrite*/ true);

            TNode restPath = PathToNode(TRichYPath(path.Path_).Append(true));
            restRequestParameters.Header.MergeParameters(TNode()("path", restPath), /*overwrite*/ true);
        }

        const auto* currentParameters = &firstRequestParameters;

        while (true) {
            int taskId = 0;
            TWriteTask task;
            {
                auto g = Guard(Lock_);
                while (State_ == EState::Running && TaskIdQueue_.empty()) {
                    HaveMoreData_.Wait(Lock_);
                }

                if (
                    State_ == EState::Aborted ||
                    State_ == EState::Completed && TaskIdQueue_.empty()
                ) {
                    break;
                }

                taskId = TaskIdQueue_.front();
                TaskIdQueue_.pop();
                if (auto it = TaskMap_.find(taskId); it != TaskMap_.end()) {
                    task = it->second;
                } else {
                    Y_ABORT();
                }
            }

            try {
                if (!retrier) {
                    retrier = MakeHolder<THeavyRequestRetrier>(*currentParameters);
                }
                retrier->Update([task=task] {
                    return MakeHolder<TMemoryInput>(task.Data->data(), task.Size);
                });
                if (task.BufferComplete) {
                    retrier->Finish();
                    retrier.Reset();
                }
            } catch (const std::exception& ex) {
                task.SendingComplete.SetException(std::current_exception());
                auto g = Guard(Lock_);
                Error_ = std::current_exception();
                return;
            }

            if (task.BufferComplete) {
                retrier.Reset();
                task.SendingComplete.SetValue();
                currentParameters = &restRequestParameters;

                auto g = Guard(Lock_);
                auto erased = TaskMap_.erase(taskId);
                Y_ABORT_UNLESS(erased == 1);
            }
        }

        if (State_ == EState::Completed) {
            auto g = Guard(Lock_);
            Y_ABORT_UNLESS(TaskIdQueue_.empty());
            Y_ABORT_UNLESS(TaskMap_.empty());
        }
    }

private:
    struct TWriteTask
    {
        NThreading::TPromise<void> SendingComplete;
        std::shared_ptr<std::string> Data = std::make_shared<std::string>();
        ssize_t Size = 0;
        bool BufferComplete = false;
    };

    TMutex Lock_;
    TCondVar HaveMoreData_;

    TThread SenderThread_;

    THashMap<int, TWriteTask> TaskMap_;
    std::queue<int> TaskIdQueue_;

    std::exception_ptr Error_;
    enum class EState {
        Running,
        Completed,
        Aborted,
    };
    std::atomic<EState> State_ = EState::Running;
    int NextTaskId_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryfulWriterV2::TSendTask
{
    TSentBuffer Buffer;
    NThreading::TFuture<void> SentFuture = NThreading::MakeFuture();
    int TaskId = 0;
};

////////////////////////////////////////////////////////////////////////////////

TRetryfulWriterV2::TRetryfulWriterV2(
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& parentId,
    const TString& command,
    const TMaybe<TFormat>& format,
    const TRichYPath& path,
    const TNode& serializedWriterOptions,
    ssize_t bufferSize,
    bool createTransaction)
    : BufferSize_(bufferSize)
    , Current_(MakeHolder<TSendTask>())
    , Previous_(MakeHolder<TSendTask>())
{
    THttpHeader httpHeader("PUT", command);
    httpHeader.SetInputFormat(format);
    httpHeader.MergeParameters(serializedWriterOptions);

    if (createTransaction) {
        WriteTransaction_ = MakeHolder<TPingableTransaction>(
            clientRetryPolicy,
            context,
            parentId,
            transactionPinger->GetChildTxPinger(),
            TStartTransactionOptions()
        );
        auto append = path.Append_.GetOrElse(false);
        auto lockMode = (append  ? LM_SHARED : LM_EXCLUSIVE);
        NDetail::NRawClient::Lock(
            clientRetryPolicy->CreatePolicyForGenericRequest(),
            context,
            WriteTransaction_->GetId(),
            path.Path_,
            lockMode
        );
    }

    THeavyRequestRetrier::TParameters parameters = {
        .ClientRetryPolicy = clientRetryPolicy,
        .TransactionPinger = transactionPinger,
        .Context = context,
        .TransactionId = WriteTransaction_ ? WriteTransaction_->GetId() : parentId,
        .Header = std::move(httpHeader),
    };

    Sender_ = MakeHolder<TSender>(path, parameters);

    DoStartBatch();
}

void TRetryfulWriterV2::Abort()
{
    auto sender = std::move(Sender_);
    auto writeTransaction = std::move(WriteTransaction_);
    if (sender) {
        sender->Abort();
        if (writeTransaction) {
            writeTransaction->Abort();
        }
    }
}

size_t TRetryfulWriterV2::GetBufferMemoryUsage() const
{
    return BufferSize_ * 4;
}

void TRetryfulWriterV2::DoFinish()
{
    auto sender = std::move(Sender_);
    auto writeTransaction = std::move(WriteTransaction_);
    if (sender && sender->IsRunning()) {
        sender->UpdateBlock(Current_->TaskId, Current_->Buffer, true);
        sender->Finish();
        if (writeTransaction) {
            writeTransaction->Commit();
        }
    }
}

void TRetryfulWriterV2::DoStartBatch()
{
    Previous_->SentFuture.Wait();

    std::swap(Previous_, Current_);
    auto&& [future, taskId] = Sender_->StartBlock();
    Current_->SentFuture = future;
    Current_->TaskId = taskId;
    Current_->Buffer.Clear();
    NextSizeToSend_ = SendStep_;
}

void TRetryfulWriterV2::DoWrite(const void* buf, size_t len)
{
    Current_->Buffer.Append(buf, len);
    auto currentSize = Current_->Buffer.Size();
    if (currentSize >= NextSizeToSend_) {
        Sender_->UpdateBlock(Current_->TaskId, Current_->Buffer, false);
        NextSizeToSend_ = currentSize + SendStep_;
    }
}

void TRetryfulWriterV2::NotifyRowEnd()
{
    if (Current_->Buffer.Size() >= BufferSize_) {
        Sender_->UpdateBlock(Current_->TaskId, Current_->Buffer, true);
        DoStartBatch();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
