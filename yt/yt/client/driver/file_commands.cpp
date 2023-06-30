#include "file_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReadFileCommand::TReadFileCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("offset", Options.Offset)
        .Optional();
    RegisterParameter("length", Options.Length)
        .Optional();
    RegisterParameter("file_reader", FileReader)
        .Default(nullptr);
    RegisterParameter("etag", Etag)
        .Alias("etag_revision")
        .Default();
}

void TReadFileCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->FileReader,
        FileReader);

    PutMethodInfoInTraceContext("read_file");

    auto reader = WaitFor(context->GetClient()->CreateFileReader(Path.GetPath(), Options))
        .ValueOrThrow();

    ProduceResponseParameters(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonMapFragmentFluently(consumer)
            .Item("id").Value(reader->GetId())
            .Item("revision").Value(reader->GetRevision());
    });

    if (!Etag.empty()) {
        if (auto etag = ParseEtag(Etag);
            etag.IsOK() &&
            etag.Value().Id == reader->GetId() && etag.Value().Revision == reader->GetRevision())
        {
            return;
        }

        // COMPAT(shakurov)
        NHydra::TRevision etagRevision;
        if (TryFromString(Etag, etagRevision) && etagRevision == reader->GetRevision()) {
            return;
        }
    }

    auto output = context->Request().OutputStream;

    while (true) {
        auto block = WaitFor(reader->Read())
            .ValueOrThrow();

        if (!block)
            break;

        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

bool TReadFileCommand::HasResponseParameters() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TWriteFileCommand::TWriteFileCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("file_writer", FileWriter)
        .Default();
    RegisterParameter("compute_md5", ComputeMD5)
        .Default(false);
}

void TWriteFileCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->FileWriter,
        FileWriter);
    Options.ComputeMD5 = ComputeMD5;

    PutMethodInfoInTraceContext("write_file");

    auto writer = context->GetClient()->CreateFileWriter(Path, Options);

    WaitFor(writer->Open())
        .ThrowOnError();

    struct TWriteBufferTag { };

    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(context->GetConfig()->WriteBufferSize, {.InitializeStorage = false});

    auto input = context->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (bytesRead == 0)
            break;

        WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
            .ThrowOnError();
    }

    WaitFor(writer->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TGetFileFromCacheCommand::TGetFileFromCacheCommand()
{
    RegisterParameter("md5", MD5);
    RegisterParameter("cache_path", Options.CachePath);
}

void TGetFileFromCacheCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetFileFromCache(MD5, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result.Path));
}

////////////////////////////////////////////////////////////////////////////////

TPutFileToCacheCommand::TPutFileToCacheCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("md5", MD5);
    RegisterParameter("cache_path", Options.CachePath);
    RegisterParameter("preserve_expiration_timeout", Options.PreserveExpirationTimeout)
        .Optional();
}

void TPutFileToCacheCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->PutFileToCache(Path, MD5, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result.Path));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
