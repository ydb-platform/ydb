#include "yql_config_provider.h"

#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/providers/common/config/yql_config_qplayer.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/ast/yql_gc_nodes.h>
#include <yql/essentials/utils/log/log.h>

#include <util/string/cast.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <functional>
#include <utility>
#include <vector>

namespace NYql {

const TString YqlCoreActivationLabel = "YqlCore";

namespace {
using namespace NNodes;

constexpr TStringBuf RuntimeSettingsActivationLabel = "RuntimeSetting/";
constexpr TStringBuf CoreActivationLabel = "";

class TConfigCallableExecutionTransformer: public TSyncTransformerBase {
public:
    explicit TConfigCallableExecutionTransformer(const TTypeAnnotationContext& types)
        : Types_(types)
    {
        Y_UNUSED(Types_);
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        YQL_ENSURE(input->Type() == TExprNode::Callable);
        if (input->Content() == "Pull") {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(*input);
            YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Yson);
            NYson::EYsonFormat ysonFormat = NCommon::GetYsonFormat(fillSettings);

            auto nodeToPull = input->Child(0)->Child(0);
            if (nodeToPull->IsCallable(ConfReadName)) {
                auto key = nodeToPull->Child(2);
                auto tag = key->Child(0)->Child(0)->Content();
                if (tag == "data_sinks" || tag == "data_sources") {
                    TStringStream out;
                    NYson::TYsonWriter writer(&out, ysonFormat);
                    writer.OnBeginMap();
                    writer.OnKeyedItem("Data");
                    writer.OnBeginList();
                    if (tag == "data_sinks") {
                        for (const auto& ds : Types_.DataSinks) {
                            writer.OnListItem();
                            writer.OnStringScalar(ds->GetName());
                        }
                    } else if (tag == "data_sources") {
                        for (const auto& ds : Types_.DataSources) {
                            writer.OnListItem();
                            writer.OnStringScalar(ds->GetName());
                        }
                    }
                    writer.OnEndList();
                    writer.OnEndMap();

                    input->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
                    input->SetState(TExprNode::EState::ExecutionComplete);
                    return TStatus::Ok;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported tag: " << tag));
                    return TStatus::Error;
                }
            }

            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unknown node to pull, type: "
                                                                                << nodeToPull->Type() << ", content: " << nodeToPull->Content()));
            return TStatus::Error;
        }

        if (input->Content() == ConfReadName) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return TStatus::Ok;
        }

        if (input->Content() == ConfigureName) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return requireStatus;
            }

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return TStatus::Ok;
        }

        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Failed to execute node: " << input->Content()));
        return TStatus::Error;
    }
    void Rewind() final {
    }

private:
    const TTypeAnnotationContext& Types_;
};

class TConfigProvider: public TDataProviderBase, public TConfigFlags {
public:
    struct TFunctions {
        THashSet<TStringBuf> Names;

        TFunctions() {
            Names.insert(ConfReadName);
        }
    };

    TConfigProvider(TTypeAnnotationContext& types, const TGatewaysConfig* config, TString username,
                    TAllowSettingPolicy policy, bool forPartialTypeCheck, const TVector<TString>& activatedGroups)
        : TConfigFlags(types, policy, forPartialTypeCheck)
        , CoreConfig_(config && config->HasYqlCore() ? &config->GetYqlCore() : nullptr)
        , RuntimeSettingsConfig_(config && config->HasRuntimeSettings() ? &config->GetRuntimeSettings() : nullptr)
        , Username_(std::move(username))
    {
        for (const auto& activationGroup : activatedGroups) {
            RecordActivation(CoreActivationLabel, activationGroup);
        }
    }

    TStringBuf GetName() const override {
        return ConfigProviderName;
    }

    bool Initialize(TExprContext& ctx) override {
        std::unordered_set<std::string_view> groups;
        bool isRobot = false;
        if (Types_.Credentials != nullptr) {
            groups.insert(Types_.Credentials->GetGroups().begin(), Types_.Credentials->GetGroups().end());
            isRobot = Types_.Credentials->IsRobot();
        }
        auto filter = [this, groups = std::move(groups), isRobot](const TCoreAttr& attr) {
            if (!attr.HasActivation() || !Username_) {
                return true;
            }
            if (NConfig::Allow(attr.GetActivation(), Username_, isRobot, groups)) {
                RecordActivation(CoreActivationLabel, attr.GetName());
                return true;
            }
            return false;
        };
        if (CoreConfig_) {
            TPosition pos;
            const auto flags = NCommon::SelectAndSaveActivatedFlags<TCoreAttr>(
                YqlCoreActivationLabel, Types_.QContext, CoreConfig_->GetFlags(), filter, /*hasProviderName=*/true);
            for (const auto& flag : flags) {
                const auto& flagArgs = flag.GetArgs();
                TVector<TStringBuf> args(flagArgs.begin(), flagArgs.end());
                if (!ApplyFlag(pos, flag.GetName(), args, ctx, /*fromInitialize=*/true)) {
                    return false;
                }
            }
        }
        if (RuntimeSettingsConfig_) {
            Types_.RuntimeSettings = CreateRuntimeSettingsFromProto(
                *RuntimeSettingsConfig_, Username_, Types_.Credentials, Types_.QContext,
                [this](const TString& name) {
                    RecordActivation(RuntimeSettingsActivationLabel, name);
                });
        }
        return true;
    }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        if (Statistics_.Entries.empty()) {
            return false;
        }

        THashMap<ui32, TOperationStatistics> tmp;
        tmp.emplace(Max<ui32>(), Statistics_);
        NCommon::WriteStatistics(writer, totalOnly, tmp);

        return true;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (!EnsureArgsCount(node, 1, ctx)) {
            return false;
        }

        cluster = Nothing();
        return true;
    }

    bool MatchCategory(const TExprNode& node) {
        return (node.Child(1)->Child(0)->Content() == ConfigProviderName);
    }

    bool CanParse(const TExprNode& node) override {
        if (ConfigProviderFunctions().contains(node.Content()) ||
            node.Content() == ConfigureName)
        {
            return MatchCategory(node);
        }

        return false;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        if (ConfigurationTransformer_) {
            return *ConfigurationTransformer_;
        }

        ConfigurationTransformer_ = CreateFunctorTransformer(
            [this](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                output = input;
                if (ctx.Step.IsDone(TExprStep::Configure)) {
                    return IGraphTransformer::TStatus::Ok;
                }

                bool hasPendingEvaluations = false;
                TOptimizeExprSettings settings(nullptr);
                settings.VisitChanges = true;
                auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                    auto res = node;
                    if (!hasPendingEvaluations && node->Content() == ConfigureName) {
                        if (!EnsureMinArgsCount(*node, 2, ctx)) {
                            return {};
                        }

                        if (!node->Child(1)->IsCallable("DataSource")) {
                            return node;
                        }

                        if (node->Child(1)->Child(0)->Content() != ConfigProviderName) {
                            return node;
                        }

                        if (!EnsureMinArgsCount(*node, 3, ctx)) {
                            return {};
                        }

                        if (!EnsureAtom(*node->Child(2), ctx)) {
                            return {};
                        }

                        TStringBuf command = node->Child(2)->Content();
                        if (!command.empty() && '_' == command[0]) {
                            ctx.AddError(TIssue(ctx.GetPosition(node->Child(2)->Pos()), "Flags started with underscore are not allowed"));
                            return {};
                        }

                        TVector<TStringBuf> args;
                        for (size_t i = 3; i < node->ChildrenSize(); ++i) {
                            if (node->Child(i)->IsCallable("EvaluateAtom")) {
                                hasPendingEvaluations = true;
                                return res;
                            }
                            if (!EnsureAtom(*node->Child(i), ctx)) {
                                return {};
                            }
                            args.push_back(node->Child(i)->Content());
                        }

                        if (!ApplyFlag(ctx.GetPosition(node->Child(2)->Pos()), command, args, ctx)) {
                            return {};
                        }

                        if (command == "PureDataSource") {
                            if (Types_.PureResultDataSource != node->Child(3)->Content()) {
                                res = ctx.ChangeChild(*node, 3, ctx.RenameNode(*node->Child(3), Types_.PureResultDataSource));
                            }
                        }
                        if (command == "Layer") {
                            res = ctx.ChangeChild(*node, 2, ctx.RenameNode(*node->Child(2), "ProcessedLayer"));
                        }
                    }

                    return res;
                }, ctx, settings);

                return status;
            });

        return *ConfigurationTransformer_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        if (!TypeAnnotationTransformer_) {
            TypeAnnotationTransformer_ = CreateFunctorTransformer(
                [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                    output = input;
                    if (input->Content() == ConfReadName) {
                        if (!EnsureWorldType(*input->Child(0), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataSource(*input->Child(1), ConfigProviderName, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto key = input->Child(2);
                        if (!key->IsCallable("Key")) {
                            ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Expected key"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (key->ChildrenSize() == 0) {
                            ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Empty key is not allowed"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto tag = key->Child(0)->Child(0)->Content();
                        if (key->Child(0)->ChildrenSize() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(key->Child(0)->Pos()), "Only tag must be specified"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (key->ChildrenSize() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Too many tags"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto fields = input->Child(3);
                        if (!EnsureTuple(*fields, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (fields->ChildrenSize() != 0) {
                            ctx.AddError(TIssue(ctx.GetPosition(fields->Pos()), "Fields tuple must be empty"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!input->Child(3)->GetTypeAnn() || !input->Child(3)->IsComposable()) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(3)->Pos()), "Expected composable data"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto settings = input->Child(4);
                        if (!EnsureTuple(*settings, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (settings->ChildrenSize() != 0) {
                            ctx.AddError(TIssue(ctx.GetPosition(settings->Pos()), "Unsupported settings"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto stringAnnotation = ctx.MakeType<TDataExprType>(EDataSlot::String);
                        auto listOfString = ctx.MakeType<TListExprType>(stringAnnotation);
                        TTypeAnnotationNode::TListType children;
                        children.push_back(input->Child(0)->GetTypeAnn());
                        if (tag == "data_sources" || tag == "data_sinks") {
                            children.push_back(listOfString);
                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), TStringBuilder() << "Unknown tag: " << tag));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto tupleAnn = ctx.MakeType<TTupleExprType>(children);
                        input->SetTypeAnn(tupleAnn);
                        return IGraphTransformer::TStatus::Ok;
                    } else if (input->Content() == ConfigureName) {
                        if (!EnsureWorldType(*input->Child(0), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }

                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "(Config) Unsupported function: " << input->Content()));
                    return IGraphTransformer::TStatus::Error;
                });
        }

        return *TypeAnnotationTransformer_;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        auto read = node->Child(0);
        TString newName;
        if (read->Content() == ReadName) {
            newName = ConfReadName;
        } else {
            YQL_ENSURE(false, "Expected Read!");
        }

        YQL_CLOG(INFO, ProviderConfig) << "RewriteIO";
        auto newRead = ctx.RenameNode(*read, newName);
        auto retChildren = node->ChildrenList();
        retChildren[0] = newRead;
        return ctx.ChangeChildren(*node, std::move(retChildren));
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);

        if (node.IsCallable(RightName)) {
            if (node.Child(0)->IsCallable(ConfReadName)) {
                canRef = false;
                return true;
            }
        }

        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        if (ConfigProviderFunctions().contains(node.Content()) ||
            node.Content() == ConfigureName)
        {
            return MatchCategory(node);
        }

        return false;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        if (!CallableExecutionTransformer_) {
            CallableExecutionTransformer_ = new TConfigCallableExecutionTransformer(Types_);
        }

        return *CallableExecutionTransformer_;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
        }

        return false;
    }

    void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        YQL_ENSURE(node.IsCallable(RightName));

        writer.OnKeyedItem("PullOperation");
        writer.OnStringScalar(node.Child(0)->Content());
    }

    TString GetProviderPath(const TExprNode& node) override {
        Y_UNUSED(node);
        return "config";
    }

private:
    void RecordActivation(TStringBuf activationLabel, TStringBuf feature) {
        Statistics_.Entries.emplace_back(TStringBuilder() << "Activation:" << activationLabel << feature, 0, 0, 0, 0, 1);
    }

    TAutoPtr<IGraphTransformer> TypeAnnotationTransformer_;
    TAutoPtr<IGraphTransformer> ConfigurationTransformer_;
    TAutoPtr<IGraphTransformer> CallableExecutionTransformer_;
    const TYqlCoreConfig* CoreConfig_;
    const NProto::TRuntimeSettings* RuntimeSettingsConfig_;
    TString Username_;
    TOperationStatistics Statistics_;
};
} // namespace

TIntrusivePtr<IDataProvider> CreateConfigProvider(TTypeAnnotationContext& types, const TGatewaysConfig* config, const TString& username,
                                                  const TAllowSettingPolicy& policy, bool forPartialTypeCheck,
                                                  const TVector<TString>& activatedGroups)
{
    return new TConfigProvider(types, config, username, policy, forPartialTypeCheck, activatedGroups);
}

const THashSet<TStringBuf>& ConfigProviderFunctions() {
    return Singleton<TConfigProvider::TFunctions>()->Names;
}

} // namespace NYql
