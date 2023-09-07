#include "journal_commands.h"
#include "config.h"

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/formats/format.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TReadJournalCommand::TReadJournalCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("journal_reader", JournalReader)
        .Default();
}

void TReadJournalCommand::DoExecute(ICommandContextPtr context)
{
    auto checkLimit = [] (const TReadLimit& limit) {
        if (limit.KeyBound()) {
            THROW_ERROR_EXCEPTION("Reading key range is not supported in journals");
        }
        if (limit.GetChunkIndex()) {
            THROW_ERROR_EXCEPTION("Reading chunk index range is not supported in journals");
        }
        if (limit.GetOffset()) {
            THROW_ERROR_EXCEPTION("Reading offset range is not supported in journals");
        }
    };

    if (Path.GetNewRanges().size() > 1) {
        THROW_ERROR_EXCEPTION("Reading multiple ranges is not supported in journals");
    }

    Options.Config = UpdateYsonStruct(
        context->GetConfig()->JournalReader,
        JournalReader);

    if (Path.GetNewRanges().size() == 1) {
        auto range = Path.GetNewRanges()[0];

        checkLimit(range.LowerLimit());
        checkLimit(range.UpperLimit());

        Options.FirstRowIndex = range.LowerLimit().GetRowIndex().value_or(0);

        if (auto upperRowIndex = range.UpperLimit().GetRowIndex()) {
            Options.RowCount = *upperRowIndex - *Options.FirstRowIndex;
        }
    }

    auto reader = context->GetClient()->CreateJournalReader(
        Path.GetPath(),
        Options);

    WaitFor(reader->Open())
        .ThrowOnError();

    auto output = context->Request().OutputStream;

    // TODO(babenko): provide custom allocation tag
    TBlobOutput buffer;
    auto flushBuffer = [&] () {
        WaitFor(output->Write(buffer.Flush()))
            .ThrowOnError();
    };

    auto format = context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    while (true) {
        auto rowsOrError = WaitFor(reader->Read());
        const auto& rows = rowsOrError.ValueOrThrow();

        if (rows.empty())
            break;

        for (auto row : rows) {
            BuildYsonListFragmentFluently(consumer.get())
                .Item().BeginMap()
                    .Item(JournalPayloadKey).Value(TStringBuf(row.Begin(), row.Size()))
                .EndMap();
        }

        if (std::ssize(buffer) > context->GetConfig()->ReadBufferSize) {
            flushBuffer();
        }
    }

    consumer->Flush();

    if (buffer.Size() > 0) {
        flushBuffer();
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJournalConsumerState,
    (Root)
    (AtItem)
    (InsideMap)
    (AtData)
);

class TJournalConsumer
    : public TYsonConsumerBase
{
public:
    explicit TJournalConsumer(IJournalWriterPtr writer)
        : Writer_(std::move(writer))
    { }

    void Flush()
    {
        if (BufferedRows_.empty()) {
            return;
        }

        WaitFor(Writer_->Write(BufferedRows_))
            .ThrowOnError();

        BufferedRows_.clear();
        BufferedByteSize_ = 0;
    }

private:
    const IJournalWriterPtr Writer_;

    EJournalConsumerState State_ = EJournalConsumerState::Root;

    std::vector<TSharedRef> BufferedRows_;
    i64 BufferedByteSize_ = 0;
    static constexpr i64 MaxBufferedSize = 64_KB;


    void OnStringScalar(TStringBuf value) override
    {
        if (State_ != EJournalConsumerState::AtData) {
            ThrowMalformedPayload();
        }

        auto row = TSharedRef::FromString(TString(value));
        BufferedByteSize_ += row.Size();
        BufferedRows_.push_back(std::move(row));

        State_ = EJournalConsumerState::InsideMap;
    }

    void OnInt64Scalar(i64 /*value*/) override
    {
        ThrowMalformedPayload();
    }

    void OnUint64Scalar(ui64 /*value*/) override
    {
        ThrowMalformedPayload();
    }

    void OnDoubleScalar(double /*value*/) override
    {
        ThrowMalformedPayload();
    }

    void OnBooleanScalar(bool /*value*/) override
    {
        ThrowMalformedPayload();
    }

    void OnEntity() override
    {
        ThrowMalformedPayload();
    }

    void OnBeginList() override
    {
        ThrowMalformedPayload();
    }

    void OnListItem() override
    {
        if (State_ != EJournalConsumerState::Root) {
            ThrowMalformedPayload();
        }
        State_ = EJournalConsumerState::AtItem;
    }

    void OnEndList() override
    {
        YT_ABORT();
    }

    void OnBeginMap() override
    {
        if (State_ != EJournalConsumerState::AtItem) {
            ThrowMalformedPayload();
        }
        State_ = EJournalConsumerState::InsideMap;
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedPayload();
        }
        if (key != JournalPayloadKey) {
            ThrowMalformedPayload();
        }
        State_ = EJournalConsumerState::AtData;
    }

    void OnEndMap() override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedPayload();
        }
        State_ = EJournalConsumerState::Root;
    }

    void OnBeginAttributes() override
    {
        ThrowMalformedPayload();
    }

    void OnEndAttributes() override
    {
        YT_ABORT();
    }


    void ThrowMalformedPayload()
    {
        THROW_ERROR_EXCEPTION("Malformed journal payload");
    }

    void MaybeFlush()
    {
        if (BufferedByteSize_ >= MaxBufferedSize) {
            Flush();
        }
    }
};

TWriteJournalCommand::TWriteJournalCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("journal_writer", JournalWriter)
        .Default();
    RegisterParameter("enable_chunk_preallocation", Options.EnableChunkPreallocation)
        .Optional();
    RegisterParameter("replica_lag_limit", Options.ReplicaLagLimit)
        .Optional();
}

void TWriteJournalCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->JournalWriter,
        JournalWriter);

    auto writer = context->GetClient()->CreateJournalWriter(
        Path.GetPath(),
        Options);

    WaitFor(writer->Open())
        .ThrowOnError();

    TJournalConsumer consumer(writer);

    auto format = context->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };

    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(context->GetConfig()->WriteBufferSize, {.InitializeStorage = false});

    auto input = context->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (bytesRead == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), bytesRead));
    }

    parser->Finish();

    consumer.Flush();

    WaitFor(writer->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TTruncateJournalCommand::TTruncateJournalCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("row_count", RowCount);
}

void TTruncateJournalCommand::DoExecute(NYT::NDriver::ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->TruncateJournal(
        Path,
        RowCount,
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
