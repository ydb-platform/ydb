#include "internal_commands.h"

#include "config.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TReadHunksCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("descriptors", &TThis::Descriptors);

    registrar.Parameter("parse_header", &TThis::ParseHeader)
        .Default(true);
}

void TReadHunksCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->ChunkFragmentReader,
        ChunkFragmentReader);
    Options.ParseHeader = ParseHeader;

    std::vector<THunkDescriptor> descriptors;
    descriptors.reserve(Descriptors.size());
    for (const auto& descriptor : Descriptors) {
        descriptors.push_back(*descriptor);
    }

    auto internalClient = context->GetInternalClientOrThrow();
    auto responses = WaitFor(internalClient->ReadHunks(descriptors, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("hunks").DoListFor(responses, [&] (auto fluent, const TSharedRef& response) {
                fluent
                    .Item().BeginMap()
                        .Item(HunkPayloadKey).Value(TStringBuf(response.Begin(), response.End()))
                    .EndMap();
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TWriteHunksCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("tablet_index", &TThis::TabletIndex);
    registrar.Parameter("payloads", &TThis::Payloads);
}

void TWriteHunksCommand::DoExecute(ICommandContextPtr context)
{
    auto internalClient = context->GetInternalClientOrThrow();

    std::vector<TSharedRef> payloads;
    payloads.reserve(Payloads.size());
    for (const auto& payload : Payloads) {
        payloads.push_back(TSharedRef::FromString(payload));
    }

    auto descriptors = WaitFor(internalClient->WriteHunks(Path, TabletIndex, payloads))
        .ValueOrThrow();

    std::vector<NApi::TSerializableHunkDescriptorPtr> serializableDescriptors;
    serializableDescriptors.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        serializableDescriptors.push_back(New<NApi::TSerializableHunkDescriptor>(descriptor));
    }

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(serializableDescriptors));
}

////////////////////////////////////////////////////////////////////////////////

void TLockHunkStoreCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("tablet_index", &TThis::TabletIndex);
    registrar.Parameter("store_id", &TThis::StoreId);
    registrar.Parameter("locker_tablet_id", &TThis::LockerTabletId);
}

void TLockHunkStoreCommand::DoExecute(ICommandContextPtr context)
{
    auto internalClient = context->GetInternalClientOrThrow();

    WaitFor(internalClient->LockHunkStore(
        Path,
        TabletIndex,
        StoreId,
        LockerTabletId))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUnlockHunkStoreCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("tablet_index", &TThis::TabletIndex);
    registrar.Parameter("store_id", &TThis::StoreId);
    registrar.Parameter("locker_tablet_id", &TThis::LockerTabletId);
}

void TUnlockHunkStoreCommand::DoExecute(ICommandContextPtr context)
{
    auto internalClient = context->GetInternalClientOrThrow();

    WaitFor(internalClient->UnlockHunkStore(
        Path,
        TabletIndex,
        StoreId,
        LockerTabletId))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetConnectionConfigCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetRootClient();

    context->ProduceOutputValue(client->GetConnection()->GetConfigYson());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
