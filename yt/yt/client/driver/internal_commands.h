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
    REGISTER_YSON_STRUCT_LITE(TReadHunksCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TWriteHunksCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TLockHunkStoreCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TUnlockHunkStoreCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TGetConnectionConfigCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TIssueLeaseCommand
    : public TTypedCommand<NApi::TIssueLeaseOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TIssueLeaseCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId;
    NObjectClient::TObjectId LeaseId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRevokeLeaseCommand
    : public TTypedCommand<NApi::TRevokeLeaseOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRevokeLeaseCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId;
    NObjectClient::TObjectId LeaseId;
    bool Force;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReferenceLeaseCommand
    : public TTypedCommand<NApi::TReferenceLeaseOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReferenceLeaseCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId;
    NObjectClient::TObjectId LeaseId;
    bool Persistent;
    bool Force;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnreferenceLeaseCommand
    : public TTypedCommand<NApi::TUnreferenceLeaseOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUnreferenceLeaseCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId;
    NObjectClient::TObjectId LeaseId;
    bool Persistent;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
