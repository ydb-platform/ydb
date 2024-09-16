#include "producer_client.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NTableClient;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TProducerSession
    : public IProducerSession
{
public:
    TProducerSession(
        IClientPtr client,
        TRichYPath producerPath,
        TRichYPath queuePath,
        TNameTablePtr nameTable,
        TQueueProducerSessionId sessionId,
        TCreateQueueProducerSessionResult createSessionResult,
        TProducerSessionOptions options)
        : Client_(std::move(client))
        , ProducerPath_(std::move(producerPath))
        , QueuePath_(std::move(queuePath))
        , NameTable_(std::move(nameTable))
        , SessionId_(std::move(sessionId))
        , Options_(std::move(options))
        , Epoch_(createSessionResult.Epoch)
        , LastSequenceNumber_(createSessionResult.SequenceNumber)
        , UserMeta_(std::move(createSessionResult.UserMeta))
        , BufferedRowWriter_(CreateWireProtocolWriter())
    { }

    // TODO(nadya73): add possibility to pass user meta.

    TQueueProducerSequenceNumber GetLastSequenceNumber() const override
    {
        return LastSequenceNumber_;
    }

    const INodePtr& GetUserMeta() const override
    {
        return UserMeta_;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        for (const auto& row : rows) {
            BufferedRowWriter_->WriteUnversionedRow(row);
            ++BufferedRowCount_;
        }
        return BufferedRowWriter_->GetByteSize() < Options_.MaxBufferSize;
    }

    TFuture<void> GetReadyEvent() override
    {
        return TryToFlush();
    }

    TFuture<void> Close() override
    {
        return Flush();
    }


    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const IClientPtr Client_;
    const TRichYPath ProducerPath_;
    const TRichYPath QueuePath_;
    const TNameTablePtr NameTable_;
    const TQueueProducerSessionId SessionId_;
    const TProducerSessionOptions Options_;

    TQueueProducerEpoch Epoch_ = TQueueProducerEpoch{0};
    TQueueProducerSequenceNumber LastSequenceNumber_ = TQueueProducerSequenceNumber{0};
    INodePtr UserMeta_;

    std::unique_ptr<IWireProtocolWriter> BufferedRowWriter_;
    i64 BufferedRowCount_ = 0;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);

    TFuture<void> TryToFlush()
    {
        if (BufferedRowWriter_->GetByteSize() >= Options_.MaxBufferSize) {
            return Flush();
        }
        return VoidFuture;
    }

    TFuture<void> Flush()
    {
        auto guard = Guard(SpinLock_);

        auto writer = CreateWireProtocolWriter();
        writer->WriteSerializedRowset(BufferedRowCount_, BufferedRowWriter_->Finish());
        BufferedRowWriter_ = CreateWireProtocolWriter();
        BufferedRowCount_ = 0;

        return Client_->StartTransaction(ETransactionType::Tablet)
            .Apply(BIND([writer = std::move(writer), this, this_ = MakeStrong(this)] (const ITransactionPtr& transaction) {
                TPushQueueProducerOptions pushQueueProducerOptions;
                if (Options_.AutoSequenceNumber) {
                    pushQueueProducerOptions.SequenceNumber = TQueueProducerSequenceNumber{LastSequenceNumber_.Underlying() + 1};
                }

                return transaction->PushQueueProducer(ProducerPath_, QueuePath_, SessionId_, Epoch_, NameTable_, writer->Finish(), pushQueueProducerOptions)
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TPushQueueProducerResult& pushQueueProducerResult) {
                        LastSequenceNumber_ = pushQueueProducerResult.LastSequenceNumber;
                        return transaction->Commit();
                    }));
            })).AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProducerClient
    : public IProducerClient
{
public:
    TProducerClient(IClientPtr client, TRichYPath producerPath)
        : Client_(std::move(client))
        , ProducerPath_(std::move(producerPath))
    { }

    TFuture<IProducerSessionPtr> CreateSession(
        const TRichYPath& queuePath,
        const TNameTablePtr& nameTable,
        const TQueueProducerSessionId& sessionId,
        const TProducerSessionOptions& options) override
    {
        return Client_->CreateQueueProducerSession(ProducerPath_, queuePath, sessionId)
            .Apply(BIND([=, this, this_ = MakeStrong(this)]
                        (const TCreateQueueProducerSessionResult& createSessionResult) -> IProducerSessionPtr {
                return New<TProducerSession>(Client_, ProducerPath_, queuePath, nameTable, sessionId, createSessionResult, options);
            }));
    }

private:
    const IClientPtr Client_;
    const TRichYPath ProducerPath_;
};

////////////////////////////////////////////////////////////////////////////////

IProducerClientPtr CreateProducerClient(
    const IClientPtr& client,
    const TRichYPath& producerPath)
{
    return New<TProducerClient>(client, producerPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
