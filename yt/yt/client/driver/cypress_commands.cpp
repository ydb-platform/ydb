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

TGetCommand::TGetCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
    // TODO(babenko): rename to "limit"
    RegisterParameter("max_size", Options.MaxSize)
        .Optional();
    RegisterParameter("return_only_value", ShouldReturnOnlyValue)
        .Default(false);
    RegisterParameter("node_count_limit", Options.ComplexityLimits.NodeCount)
        .Optional()
        .GreaterThanOrEqual(0);
    RegisterParameter("result_size_limit", Options.ComplexityLimits.ResultSize)
        .Optional()
        .GreaterThanOrEqual(0);
}

void TGetCommand::DoExecute(ICommandContextPtr context)
{
    Options.Options = IAttributeDictionary::FromMap(GetUnrecognized());

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

TSetCommand::TSetCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
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

TMultisetAttributesCommand::TMultisetAttributesCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("force", Options.Force)
        .Optional();
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

TRemoveCommand::TRemoveCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
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

TListCommand::TListCommand()
{
    RegisterParameter("path", Path);
    // NB: default value is an empty filter in contrast to GetCommand, for which it is the universal filter.
    // Refer to YT-5543 for details.
    RegisterParameter("attributes", Options.Attributes)
        .Default(TAttributeFilter({}, {}));
    // TODO(babenko): rename to "limit"
    RegisterParameter("max_size", Options.MaxSize)
        .Optional();
    RegisterParameter("return_only_value", ShouldReturnOnlyValue)
        .Default(false);
    RegisterParameter("node_count_limit", Options.ComplexityLimits.NodeCount)
        .Optional()
        .GreaterThanOrEqual(0);
    RegisterParameter("result_size_limit", Options.ComplexityLimits.ResultSize)
        .Optional()
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

TCreateNodeCommand::TCreateNodeCommand()
{
    RegisterParameter("path", Path)
        .Optional();
    RegisterParameter("type", Type);
    RegisterParameter("attributes", Attributes)
        .Optional();
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("lock_existing", Options.LockExisting)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
    RegisterParameter("ignore_type_mismatch", Options.IgnoreTypeMismatch)
        .Optional();
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

TCreateObjectCommand::TCreateObjectCommand()
{
    RegisterParameter("type", Type);
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("sync", Options.Sync)
        .Optional();
    RegisterParameter("attributes", Attributes)
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

TLockCommand::TLockCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("mode", Mode)
        .Default(NCypressClient::ELockMode::Exclusive);
    RegisterParameter("waitable", Options.Waitable)
        .Optional();
    RegisterParameter("child_key", Options.ChildKey)
        .Optional();
    RegisterParameter("attribute_key", Options.AttributeKey)
        .Optional();

    RegisterPostprocessor([&] () {
        if (Mode != NCypressClient::ELockMode::Shared) {
            if (Options.ChildKey) {
                THROW_ERROR_EXCEPTION("\"child_key\" can only be specified for shared locks");
            }
            if (Options.AttributeKey) {
                THROW_ERROR_EXCEPTION("\"attribute_key\" can only be specified for shared locks");
            }
        }
        if (Options.ChildKey && Options.AttributeKey) {
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
            ProduceOutput(context, [&](NYson::IYsonConsumer* consumer) {
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

TUnlockCommand::TUnlockCommand()
{
    RegisterParameter("path", Path);
}

void TUnlockCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncUnlockResult = context->GetClient()->UnlockNode(Path.GetPath(), Options);
    WaitFor(asyncUnlockResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TCopyCommand::TCopyCommand()
{
    RegisterParameter("source_path", SourcePath);
    RegisterParameter("destination_path", DestinationPath);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("lock_existing", Options.LockExisting)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
    RegisterParameter("preserve_account", Options.PreserveAccount)
        .Optional();
    RegisterParameter("preserve_creation_time", Options.PreserveCreationTime)
        .Optional();
    RegisterParameter("preserve_modification_time", Options.PreserveModificationTime)
        .Optional();
    RegisterParameter("preserve_expiration_time", Options.PreserveExpirationTime)
        .Optional();
    RegisterParameter("preserve_expiration_timeout", Options.PreserveExpirationTimeout)
        .Optional();
    RegisterParameter("preserve_owner", Options.PreserveOwner)
        .Optional();
    RegisterParameter("preserve_acl", Options.PreserveAcl)
        .Optional();
    RegisterParameter("pessimistic_quota_check", Options.PessimisticQuotaCheck)
        .Optional();
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

TMoveCommand::TMoveCommand()
{
    RegisterParameter("source_path", SourcePath);
    RegisterParameter("destination_path", DestinationPath);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
    RegisterParameter("preserve_account", Options.PreserveAccount)
        .Optional();
    RegisterParameter("preserve_creation_time", Options.PreserveCreationTime)
        .Optional();
    RegisterParameter("preserve_modification_time", Options.PreserveModificationTime)
        .Optional();
    RegisterParameter("preserve_expiration_time", Options.PreserveExpirationTime)
        .Optional();
    RegisterParameter("preserve_expiration_timeout", Options.PreserveExpirationTimeout)
        .Optional();
    RegisterParameter("preserve_owner", Options.PreserveOwner)
        .Optional();
    RegisterParameter("pessimistic_quota_check", Options.PessimisticQuotaCheck)
        .Optional();
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

TExistsCommand::TExistsCommand()
{
    RegisterParameter("path", Path);
}

void TExistsCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->NodeExists(Path.GetPath(), Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

TLinkCommand::TLinkCommand()
{
    RegisterParameter("link_path", LinkPath);
    RegisterParameter("target_path", TargetPath);
    RegisterParameter("attributes", Attributes)
        .Optional();
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("lock_existing", Options.LockExisting)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
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

TConcatenateCommand::TConcatenateCommand()
{
    RegisterParameter("source_paths", SourcePaths);
    RegisterParameter("destination_path", DestinationPath);

    RegisterParameter("uniqualize_chunks", Options.UniqualizeChunks)
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

TExternalizeCommand::TExternalizeCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("cell_tag", CellTag);
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

TInternalizeCommand::TInternalizeCommand()
{
    RegisterParameter("path", Path);
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
