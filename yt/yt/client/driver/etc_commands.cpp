#include "etc_commands.h"
#include "config.h"
#include "proxy_discovery_cache.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/build/build.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/async_writer.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TAddMemberCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AddMember(
        Group,
        Member,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMemberCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RemoveMember(
        Group,
        Member,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TParseYPathCommand::TParseYPathCommand()
{
    RegisterParameter("path", Path);
}

void TParseYPathCommand::DoExecute(ICommandContextPtr context)
{
    auto richPath = TRichYPath::Parse(Path);
    ProduceSingleOutputValue(context, "path", richPath);
}

////////////////////////////////////////////////////////////////////////////////

void TGetVersionCommand::DoExecute(ICommandContextPtr context)
{
    ProduceSingleOutputValue(context, "version", GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

void TGetSupportedFeaturesCommand::DoExecute(ICommandContextPtr context)
{
    TGetClusterMetaOptions options;
    options.PopulateFeatures = true;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.DisablePerUserCache = true;

    auto meta = WaitFor(context->GetClient()->GetClusterMeta(options))
        .ValueOrThrow();
    if (!meta.Features) {
        THROW_ERROR_EXCEPTION("Feature querying is not supported by current master version");
    }
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("features").Value(meta.Features)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionCommand::TCheckPermissionCommand()
{
    RegisterParameter("user", User);
    RegisterParameter("permission", Permission);
    RegisterParameter("path", Path);
    RegisterParameter("columns", Options.Columns)
        .Optional();
    RegisterParameter("vital", Options.Vital)
        .Optional();
}

void TCheckPermissionCommand::DoExecute(ICommandContextPtr context)
{
    auto response =
        WaitFor(context->GetClient()->CheckPermission(
            User,
            Path.GetPath(),
            Permission,
            Options))
        .ValueOrThrow();

    auto produceResult = [] (auto fluent, const auto& result) {
        fluent
            .Item("action").Value(result.Action)
            .OptionalItem("object_id", result.ObjectId)
            .OptionalItem("object_name", result.ObjectName)
            .OptionalItem("subject_id", result.SubjectId)
            .OptionalItem("subject_name", result.SubjectName);
    };

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Do([&] (auto fluent) { produceResult(fluent, response); })
            .DoIf(response.Columns.has_value(), [&] (auto fluent) {
                fluent
                    .Item("columns").DoListFor(*response.Columns, [&] (auto fluent, const auto& result) {
                        fluent
                            .Item().BeginMap()
                                .Do([&] (auto fluent) { produceResult(fluent, result); })
                            .EndMap();
                    });
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionByAclCommand::TCheckPermissionByAclCommand()
{
    RegisterParameter("user", User);
    RegisterParameter("permission", Permission);
    RegisterParameter("acl", Acl);
}

void TCheckPermissionByAclCommand::DoExecute(ICommandContextPtr context)
{
    auto result =
        WaitFor(context->GetClient()->CheckPermissionByAcl(
            User,
            Permission,
            Acl,
            Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("action").Value(result.Action)
            .OptionalItem("subject_id", result.SubjectId)
            .OptionalItem("subject_name", result.SubjectName)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TTransferAccountResourcesCommand::TTransferAccountResourcesCommand()
{
    RegisterParameter("source_account", SourceAccount);
    RegisterParameter("destination_account", DestinationAccount);
    RegisterParameter("resource_delta", ResourceDelta);
}

void TTransferAccountResourcesCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->TransferAccountResources(
        SourceAccount,
        DestinationAccount,
        ResourceDelta,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TTransferPoolResourcesCommand::TTransferPoolResourcesCommand()
{
    RegisterParameter("source_pool", SourcePool);
    RegisterParameter("destination_pool", DestinationPool);
    RegisterParameter("pool_tree", PoolTree);
    RegisterParameter("resource_delta", ResourceDelta);
}

void TTransferPoolResourcesCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->TransferPoolResources(
        SourcePool,
        DestinationPool,
        PoolTree,
        ResourceDelta,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TExecuteBatchCommandRequest::Register(TRegistrar registrar)
{
    registrar.Parameter("command", &TThis::Command);
    registrar.Parameter("parameters", &TThis::Parameters);
    registrar.Parameter("input", &TThis::Input)
        .Default();
}

class TExecuteBatchCommand::TRequestExecutor
    : public TRefCounted
{
public:
    TRequestExecutor(
        ICommandContextPtr context,
        int requestIndex,
        TRequestPtr request,
        NRpc::TMutationId mutationId,
        bool retry)
        : Context_(std::move(context))
        , RequestIndex_(requestIndex)
        , Request_(std::move(request))
        , MutationId_(mutationId)
        , Retry_(retry)
        , SyncInput_(Input_)
        , AsyncInput_(CreateAsyncAdapter(
            &SyncInput_,
            Context_->GetClient()->GetConnection()->GetInvoker()))
        , SyncOutput_(Output_)
        , AsyncOutput_(CreateAsyncAdapter(
            &SyncOutput_,
            Context_->GetClient()->GetConnection()->GetInvoker()))
    { }

    TFuture<TYsonString> Run()
    {
        try {
            auto driver = Context_->GetDriver();
            Descriptor_ = driver->GetCommandDescriptorOrThrow(Request_->Command);

            if (Descriptor_.InputType != EDataType::Null &&
                Descriptor_.InputType != EDataType::Structured &&
                Descriptor_.InputType != EDataType::Tabular)
            {
                THROW_ERROR_EXCEPTION("Command %Qv cannot be part of a batch since it has inappropriate input type %Qlv",
                    Request_->Command,
                    Descriptor_.InputType);
            }

            if (Descriptor_.OutputType != EDataType::Null &&
                Descriptor_.OutputType != EDataType::Structured)
            {
                THROW_ERROR_EXCEPTION("Command %Qv cannot be part of a batch since it has inappropriate output type %Qlv",
                    Request_->Command,
                    Descriptor_.OutputType);
            }

            TDriverRequest driverRequest;
            driverRequest.Id = Context_->Request().Id;
            driverRequest.CommandName = Request_->Command;
            auto parameters = IAttributeDictionary::FromMap(Request_->Parameters);
            if (Descriptor_.InputType == EDataType::Tabular || Descriptor_.InputType == EDataType::Structured) {
                if (!Request_->Input) {
                    THROW_ERROR_EXCEPTION("Command %Qv requires input",
                        Descriptor_.CommandName);
                }
                // COMPAT(ignat): disable this option in proxy configuration and remove it.
                if ((Context_->GetConfig()->ExpectStructuredInputInStructuredBatchCommands && Descriptor_.InputType == EDataType::Structured) ||
                    !parameters->Contains("input_format"))
                {
                    Input_ = ConvertToYsonString(Request_->Input).ToString();
                    parameters->Set("input_format", TFormat(EFormatType::Yson));
                } else {
                    Input_ = Request_->Input->AsString()->GetValue();
                }
                driverRequest.InputStream = AsyncInput_;
            }
            if (Descriptor_.OutputType == EDataType::Structured) {
                parameters->Set("output_format", TFormat(EFormatType::Yson));
                driverRequest.OutputStream = AsyncOutput_;
            }
            if (Descriptor_.Volatile) {
                parameters->Set("mutation_id", MutationId_);
                parameters->Set("retry", Retry_);
            }
            driverRequest.Parameters = parameters->ToMap();
            driverRequest.AuthenticatedUser = Context_->Request().AuthenticatedUser;
            driverRequest.UserTag = Context_->Request().UserTag;
            driverRequest.LoggingTags = Format("SubrequestIndex: %v", RequestIndex_);

            return driver->Execute(driverRequest).Apply(
                BIND(&TRequestExecutor::OnResponse, MakeStrong(this)));
        } catch (const std::exception& ex) {
            return MakeFuture<TYsonString>(ex);
        }
    }

private:
    const ICommandContextPtr Context_;
    const int RequestIndex_;
    const TRequestPtr Request_;
    const NRpc::TMutationId MutationId_;
    const bool Retry_;

    TCommandDescriptor Descriptor_;

    TString Input_;
    TStringInput SyncInput_;
    IAsyncInputStreamPtr AsyncInput_;

    TString Output_;
    TStringOutput SyncOutput_;
    IFlushableAsyncOutputStreamPtr AsyncOutput_;

    TYsonString OnResponse(const TError& error)
    {
        return BuildYsonStringFluently()
            .BeginMap()
                .DoIf(!error.IsOK(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("error").Value(error);
                })
                .DoIf(error.IsOK() && Descriptor_.OutputType == EDataType::Structured, [&] (TFluentMap fluent) {
                    fluent
                        .Item("output").Value(TYsonStringBuf(Output_));
                })
            .EndMap();
    }
};

TExecuteBatchCommand::TExecuteBatchCommand()
{
    RegisterParameter("concurrency", Options.Concurrency)
        .Default(50)
        .GreaterThan(0);
    RegisterParameter("requests", Requests);
}

void TExecuteBatchCommand::DoExecute(ICommandContextPtr context)
{
    auto mutationId = Options.GetOrGenerateMutationId();

    std::vector<TCallback<TFuture<TYsonString>()>> callbacks;
    for (int requestIndex = 0; requestIndex < std::ssize(Requests); ++requestIndex) {
        auto executor = New<TRequestExecutor>(
            context,
            requestIndex,
            Requests[requestIndex],
            mutationId,
            Options.Retry);
        mutationId = NRpc::GenerateNextBatchMutationId(mutationId);
        callbacks.push_back(BIND(&TRequestExecutor::Run, executor));
    }

    auto results = WaitFor(RunWithBoundedConcurrency(callbacks, Options.Concurrency))
        .ValueOrThrow();

    ProduceSingleOutput(context, "results", [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoListFor(results, [&] (TFluentList fluent, const TErrorOr<TYsonString>& result) {
                fluent.Item().Value(result.ValueOrThrow());
            });
    });
}

////////////////////////////////////////////////////////////////////////////////

TDiscoverProxiesCommand::TDiscoverProxiesCommand()
{
    RegisterParameter("type", Type)
        .Default(EProxyType::Rpc);
    RegisterParameter("role", Role)
        .Default(DefaultRpcProxyRole);
    RegisterParameter("address_type", AddressType)
        .Default(NApi::NRpcProxy::DefaultAddressType);
    RegisterParameter("network_name", NetworkName)
        .Default(NApi::NRpcProxy::DefaultNetworkName);
    RegisterParameter("ignore_balancers", IgnoreBalancers)
        .Default(false);
}

void TDiscoverProxiesCommand::DoExecute(ICommandContextPtr context)
{
    TProxyDiscoveryRequest request{
        .Type = Type,
        .Role = Role,
        .AddressType = AddressType,
        .NetworkName = NetworkName,
        .IgnoreBalancers = IgnoreBalancers
    };

    const auto& proxyDiscoveryCache = context->GetDriver()->GetProxyDiscoveryCache();
    auto response = WaitFor(proxyDiscoveryCache->Discover(request))
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "proxies", response.Addresses);
}

////////////////////////////////////////////////////////////////////////////////

TBalanceTabletCellsCommand::TBalanceTabletCellsCommand()
{
    RegisterParameter("bundle", TabletCellBundle);
    RegisterParameter("tables", MovableTables)
        .Optional();
    RegisterParameter("keep_actions", Options.KeepActions)
        .Default(false);
}

void TBalanceTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->BalanceTabletCells(
        TabletCellBundle,
        MovableTables,
        Options);
    auto tabletActions = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently().List(tabletActions));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
