#pragma once

#include "command.h"

#include <yt/yt/client/api/internal_client.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadHunksCommand
    : public TTypedCommand<NApi::TReadHunksOptions>
{
public:
    TReadHunksCommand();

private:
    std::vector<NApi::TSerializableHunkDescriptorPtr> Descriptors;
    NYTree::INodePtr ChunkFragmentReader;

    bool ParseHeader;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteHunksCommand
    : public TTypedCommand<NApi::TWriteHunksOptions>
{
public:
    TWriteHunksCommand();

private:
    NYTree::TYPath Path;
    int TabletIndex;
    std::vector<TString> Payloads;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockHunkStoreCommand
    : public TTypedCommand<NApi::TLockHunkStoreOptions>
{
public:
    TLockHunkStoreCommand();

private:
    NYTree::TYPath Path;
    int TabletIndex;
    NTabletClient::TStoreId StoreId;
    NTabletClient::TTabletId LockerTabletId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnlockHunkStoreCommand
    : public TTypedCommand<NApi::TUnlockHunkStoreOptions>
{
public:
    TUnlockHunkStoreCommand();

private:
    NYTree::TYPath Path;
    int TabletIndex;
    NTabletClient::TStoreId StoreId;
    NTabletClient::TTabletId LockerTabletId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetConnectionConfigCommandOptions
{ };

class TGetConnectionConfigCommand
    : public TTypedCommand<TGetConnectionConfigCommandOptions>
{
public:
    TGetConnectionConfigCommand() = default;

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
