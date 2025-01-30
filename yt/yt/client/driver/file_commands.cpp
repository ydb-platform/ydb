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

void TReadFileCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.Parameter("file_reader", &TThis::FileReader)
        .Default(nullptr);

    registrar.Parameter("etag", &TThis::Etag)
        .Alias("etag_revision")
        .Default();

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "offset",
        [] (TThis* command) -> auto& {
            return command->Options.Offset;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "length",
        [] (TThis* command) -> auto& {
            return command->Options.Length;
        })
        .Optional(/*init*/ false);
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

void TWriteFileCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("file_writer", &TThis::FileWriter)
        .Default();
    registrar.Parameter("compute_md5", &TThis::ComputeMD5)
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

    auto input = context->Request().InputStream;

    while (true) {
        auto data = WaitFor(input->Read())
            .ValueOrThrow();

        if (!data) {
            break;
        }

        WaitFor(writer->Write(std::move(data)))
            .ThrowOnError();
    }

    WaitFor(writer->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetFileFromCacheCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("md5", &TThis::MD5);

    registrar.ParameterWithUniversalAccessor<TYPath>(
        "cache_path",
        [] (TThis* command) -> auto& {
            return command->Options.CachePath;
        });
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

void TPutFileToCacheCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.Parameter("md5", &TThis::MD5);

    registrar.ParameterWithUniversalAccessor<TYPath>(
        "cache_path",
        [] (TThis* command) -> auto& {
            return command->Options.CachePath;
        });

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_expiration_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveExpirationTimeout;
        })
        .Optional(/*init*/ false);
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
