#include "cypress_commands.h"
#include "config.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<TAttributeFilter>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);

    // TODO(babenko): rename to "limit"
    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "max_size",
        [] (TThis* command) -> auto& {
            return command->Options.MaxSize;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("return_only_value", &TThis::ShouldReturnOnlyValue)
        .Default(false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "node_count_limit",
        [] (TThis* command) -> auto& {
            return command->Options.ComplexityLimits.NodeCount;
        })
        .Optional(/*init*/ false)
        .GreaterThanOrEqual(0);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "result_size_limit",
        [] (TThis* command) -> auto& {
            return command->Options.ComplexityLimits.ResultSize;
        })
        .Optional(/*init*/ false)
        .GreaterThanOrEqual(0);
}

void TGetCommand::DoExecute(ICommandContextPtr context)
{
    Options.Options = IAttributeDictionary::FromMap(GetLocalUnrecognized());

    auto asyncResult = context->GetClient()->GetNode(Path.GetPath(), Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    // XXX(levysotsky): Python client doesn't want to parse the returned |map_node|.
    if (ShouldReturnOnlyValue) {
        context->ProduceOutputValue(result);
    } else {
        ProduceSingleOutputValue(context, "value", result);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TSetCommand::DoExecute(ICommandContextPtr context)
{
    auto value = context->ConsumeInputValue();

    auto asyncResult = context->GetClient()->SetNode(Path.GetPath(), value, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TMultisetAttributesCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TMultisetAttributesCommand::DoExecute(ICommandContextPtr context)
{
    auto attributes = ConvertTo<IMapNodePtr>(context->ConsumeInputValue());

    auto asyncResult = context->GetClient()->MultisetAttributesNode(Path.GetPath(), attributes, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TRemoveCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemoveNode(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TListCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    // NB: default value is an empty filter in contrast to GetCommand, for which it is the universal filter.
    // Refer to YT-5543 for details.
    registrar.ParameterWithUniversalAccessor<TAttributeFilter>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Default(TAttributeFilter({}, {}));

    // TODO(babenko): rename to "limit"
    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "max_size",
        [] (TThis* command) -> auto& {
            return command->Options.MaxSize;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("return_only_value", &TThis::ShouldReturnOnlyValue)
        .Default(false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "node_count_limit",
        [] (TThis* command) -> auto& {
            return command->Options.ComplexityLimits.NodeCount;
        })
        .Optional(/*init*/ false)
        .GreaterThanOrEqual(0);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "result_size_limit",
        [] (TThis* command) -> auto& {
            return command->Options.ComplexityLimits.ResultSize;
        })
        .Optional(/*init*/ false)
        .GreaterThanOrEqual(0);
}

void TListCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->ListNode(Path.GetPath(), Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    // XXX(levysotsky): Python client doesn't want to parse the returned |map_node|.
    if (ShouldReturnOnlyValue) {
        context->ProduceOutputValue(result);
    } else {
        ProduceSingleOutputValue(context, "value", result);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);
}

void TCreateCommand::Execute(ICommandContextPtr context)
{
    // For historical reasons, we handle both CreateNode and CreateObject requests
    // in a single command. Here we route the request to an appropriate backend command.
    Deserialize(*this, context->Request().Parameters);
    auto backend = IsVersionedType(Type)
        ? std::unique_ptr<ICommand>(new TCreateNodeCommand())
        : std::unique_ptr<ICommand>(new TCreateObjectCommand());
    backend->Execute(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateNodeCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Optional();

    registrar.Parameter("type", &TThis::Type);

    registrar.Parameter("attributes", &TThis::Attributes)
        .Optional();

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ignore_existing",
        [] (TThis* command) -> auto& {
            return command->Options.IgnoreExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "lock_existing",
        [] (TThis* command) -> auto& {
            return command->Options.LockExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ignore_type_mismatch",
        [] (TThis* command) -> auto& {
            return command->Options.IgnoreTypeMismatch;
        })
        .Optional(/*init*/ false);
}

void TCreateNodeCommand::DoExecute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncNodeId = context->GetClient()->CreateNode(Path.GetPath(), Type, Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateObjectCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ignore_existing",
        [] (TThis* command) -> auto& {
            return command->Options.IgnoreExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "sync",
        [] (TThis* command) -> auto& {
            return command->Options.Sync;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("attributes", &TThis::Attributes)
        .Optional();
}

void TCreateObjectCommand::DoExecute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncObjectId = context->GetClient()->CreateObject(
        Type,
        Options);
    auto objectId = WaitFor(asyncObjectId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "object_id", objectId);
}

////////////////////////////////////////////////////////////////////////////////

void TLockCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.Parameter("mode", &TThis::Mode)
        .Default(NCypressClient::ELockMode::Exclusive);

    registrar.ParameterWithUniversalAccessor<bool>(
        "waitable",
        [] (TThis* command) -> auto& {
            return command->Options.Waitable;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "child_key",
        [] (TThis* command) -> auto& {
            return command->Options.ChildKey;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "attribute_key",
        [] (TThis* command) -> auto& {
            return command->Options.AttributeKey;
        })
        .Optional(/*init*/ false);

    registrar.Postprocessor([] (TThis* command) {
        if (command->Mode != NCypressClient::ELockMode::Shared) {
            if (command->Options.ChildKey) {
                THROW_ERROR_EXCEPTION("\"child_key\" can only be specified for shared locks");
            }
            if (command->Options.AttributeKey) {
                THROW_ERROR_EXCEPTION("\"attribute_key\" can only be specified for shared locks");
            }
        }
        if (command->Options.ChildKey && command->Options.AttributeKey) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"child_key\" and \"attribute_key\"");
        }
    });
}

void TLockCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncLockResult = context->GetClient()->LockNode(Path.GetPath(), Mode, Options);
    auto lockResult = WaitFor(asyncLockResult)
        .ValueOrThrow();

    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            ProduceSingleOutputValue(context, "lock_id", lockResult.LockId);
            break;
        default:
            ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("lock_id").Value(lockResult.LockId)
                        .Item("node_id").Value(lockResult.NodeId)
                        .Item("revision").Value(lockResult.Revision)
                    .EndMap();
            });
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TUnlockCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

void TUnlockCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncUnlockResult = context->GetClient()->UnlockNode(Path.GetPath(), Options);
    WaitFor(asyncUnlockResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("source_path", &TThis::SourcePath);

    registrar.Parameter("destination_path", &TThis::DestinationPath);

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ignore_existing",
        [] (TThis* command) -> auto& {
            return command->Options.IgnoreExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "lock_existing",
        [] (TThis* command) -> auto& {
            return command->Options.LockExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_account",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveAccount;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_creation_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveCreationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_modification_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveModificationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_expiration_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveExpirationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_expiration_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveExpirationTimeout;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_owner",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveOwner;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_acl",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveAcl;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "pessimistic_quota_check",
        [] (TThis* command) -> auto& {
            return command->Options.PessimisticQuotaCheck;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "enable_cross_cell_copying",
        [] (TThis* command) -> auto& {
            return command->Options.EnableCrossCellCopying;
        })
        .Optional(/*init*/ false);
}

void TCopyCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->CopyNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("source_path", &TThis::SourcePath);

    registrar.Parameter("destination_path", &TThis::DestinationPath);

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_account",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveAccount;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_creation_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveCreationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_modification_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveModificationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_expiration_time",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveExpirationTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_expiration_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveExpirationTimeout;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_owner",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveOwner;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "pessimistic_quota_check",
        [] (TThis* command) -> auto& {
            return command->Options.PessimisticQuotaCheck;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "enable_cross_cell_copying",
        [] (TThis* command) -> auto& {
            return command->Options.EnableCrossCellCopying;
        })
        .Optional(/*init*/ false);
}

void TMoveCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->MoveNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

void TExistsCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->NodeExists(Path.GetPath(), Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

void TLinkCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("link_path", &TThis::LinkPath);
    registrar.Parameter("target_path", &TThis::TargetPath);
    registrar.Parameter("attributes", &TThis::Attributes)
        .Optional();

    registrar.ParameterWithUniversalAccessor<bool>(
        "recursive",
        [] (TThis* command) -> auto& {
            return command->Options.Recursive;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "ignore_existing",
        [] (TThis* command) -> auto& {
            return command->Options.IgnoreExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "lock_existing",
        [] (TThis* command) -> auto& {
            return command->Options.LockExisting;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TLinkCommand::DoExecute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncNodeId = context->GetClient()->LinkNode(
        TargetPath.GetPath(),
        LinkPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

void TConcatenateCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("source_paths", &TThis::SourcePaths);
    registrar.Parameter("destination_path", &TThis::DestinationPath);

    registrar.ParameterWithUniversalAccessor<bool>(
        "uniqualize_chunks",
        [] (TThis* command) -> auto& {
            return command->Options.UniqualizeChunks;
        })
        .Default(false);
}

void TConcatenateCommand::DoExecute(ICommandContextPtr context)
{
    Options.ChunkMetaFetcherConfig = context->GetConfig()->Fetcher;

    auto asyncResult = context->GetClient()->ConcatenateNodes(
        SourcePaths,
        DestinationPath,
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TExternalizeCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("cell_tag", &TThis::CellTag);
}

void TExternalizeCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->ExternalizeNode(
        Path,
        CellTag,
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TInternalizeCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

void TInternalizeCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->InternalizeNode(
        Path,
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
