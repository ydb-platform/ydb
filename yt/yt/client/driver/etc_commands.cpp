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

void TParseYPathCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
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

// These features are guaranteed to be deployed before or with this code.
constexpr auto StaticFeatures = std::to_array<std::pair<TStringBuf, bool>>({
    {"user_tokens_metadata", true},
});

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
    auto features = meta.Features;
    for (auto staticFeature : StaticFeatures) {
        features->AddChild(TString(staticFeature.first), BuildYsonNodeFluently().Value(staticFeature.second));
    }
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("features").Value(features)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TCheckPermissionCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User);
    registrar.Parameter("permission", &TThis::Permission);
    registrar.Parameter("path", &TThis::Path);
    registrar.ParameterWithUniversalAccessor<std::optional<std::vector<TString>>>(
        "columns",
        [] (TThis* command) -> auto& {
            return command->Options.Columns;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "vital",
        [] (TThis* command) -> auto& {
            return command->Options.Vital;
        })
        .Optional(/*init*/ false);
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

void TCheckPermissionByAclCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User);
    registrar.Parameter("permission", &TThis::Permission);
    registrar.Parameter("acl", &TThis::Acl);
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

void TTransferAccountResourcesCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("source_account", &TThis::SourceAccount);
    registrar.Parameter("destination_account", &TThis::DestinationAccount);
    registrar.Parameter("resource_delta", &TThis::ResourceDelta);
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

void TTransferPoolResourcesCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("source_pool", &TThis::SourcePool);
    registrar.Parameter("destination_pool", &TThis::DestinationPool);
    registrar.Parameter("pool_tree", &TThis::PoolTree);
    registrar.Parameter("resource_delta", &TThis::ResourceDelta);
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
        , AsyncInput_(
            CreateZeroCopyAdapter(
                CreateAsyncAdapter(
                    &SyncInput_,
                    Context_->GetClient()->GetConnection()->GetInvoker())))
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
    IAsyncZeroCopyInputStreamPtr AsyncInput_;

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

void TExecuteBatchCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<int>(
        "concurrency",
        [] (TThis* command) -> auto& {
            return command->Options.Concurrency;
        })
        .Default(50)
        .GreaterThan(0);
    registrar.Parameter("requests", &TThis::Requests);
}

void TExecuteBatchCommand::DoExecute(ICommandContextPtr context)
{
    auto mutationId = Options.GetOrGenerateMutationId();

    std::vector<TCallback<TFuture<TYsonString>()>> callbacks;
    callbacks.reserve(Requests.size());
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

    auto results = WaitFor(RunWithBoundedConcurrency(std::move(callbacks), Options.Concurrency))
        .ValueOrThrow();

    ProduceSingleOutput(context, "results", [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoListFor(results, [&] (TFluentList fluent, const TErrorOr<TYsonString>& result) {
                fluent.Item().Value(result.ValueOrThrow());
            });
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoverProxiesCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EProxyType::Rpc);
    registrar.Parameter("role", &TThis::Role)
        .Default(DefaultRpcProxyRole);
    registrar.Parameter("address_type", &TThis::AddressType)
        .Default(NApi::NRpcProxy::DefaultAddressType);
    registrar.Parameter("network_name", &TThis::NetworkName)
        .Default(NApi::NRpcProxy::DefaultNetworkName);
    registrar.Parameter("ignore_balancers", &TThis::IgnoreBalancers)
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

void TBalanceTabletCellsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle", &TThis::TabletCellBundle);
    registrar.Parameter("tables", &TThis::MovableTables)
        .Optional();
    registrar.ParameterWithUniversalAccessor<bool>(
        "keep_actions",
        [] (TThis* command) -> auto& {
            return command->Options.KeepActions;
        })
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
