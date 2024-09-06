#pragma once

#include "table_commands.h"

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

// -> DistributedWriteSession
class TStartDistributedWriteSessionCommand
    : public TTypedCommand<NApi::TDistributedWriteSessionStartOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartDistributedWriteSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

// -> Nothing
class TFinishDistributedWriteSessionCommand
    : public TTypedCommand<NApi::TDistributedWriteSessionFinishOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TFinishDistributedWriteSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr Session;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

// -> Cookie
class TParticipantWriteTableCommand
    : public TTypedCommand<NApi::TParticipantTableWriterOptions>
    , private TWriteTableCommand
{
public:
    // Shadow normal execute in order to fix
    // ambiguity in dispatch.
    void Execute(ICommandContextPtr context) override;

    REGISTER_YSON_STRUCT_LITE(TParticipantWriteTableCommand);

    static void Register(TRegistrar registrar);

private:
    using TBase = TWriteTableCommand;

    NYTree::INodePtr Cookie;
    TRefCountedPtr ResultingCookie;

    TFuture<NApi::ITableWriterPtr> CreateTableWriter(
        const ICommandContextPtr& context) const override;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
