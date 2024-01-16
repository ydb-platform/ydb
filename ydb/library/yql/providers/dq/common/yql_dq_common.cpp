#include "yql_dq_common.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <util/string/split.h>

#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include "yql_dq_settings.h"
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NYql {
namespace NCommon {

using namespace NKikimr::NMiniKQL;

TString GetSerializedResultType(const TString& program) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment typeEnv(alloc);

    TRuntimeNode programNode = DeserializeRuntimeNode(program, typeEnv);

    YQL_ENSURE(programNode.IsImmediate() && programNode.GetNode()->GetType()->IsStruct());

    // copy-paste from dq_task_runner.cpp
    auto& programStruct = static_cast<TStructLiteral&>(*programNode.GetNode());
    auto programType = programStruct.GetType();
    YQL_ENSURE(programType);

    auto programRootIdx = programType->FindMemberIndex("Program");
    YQL_ENSURE(programRootIdx);
    TRuntimeNode programRoot = programStruct.GetValue(*programRootIdx);
    YQL_ENSURE(programRoot.GetNode()->GetType()->IsCallable());
    auto programResultType = static_cast<const TCallableType*>(programRoot.GetNode()->GetType());
    YQL_ENSURE(programResultType->GetReturnType()->IsStream());
    auto programResultItemType = static_cast<const TStreamType*>(programResultType->GetReturnType())->GetItemType();

    return SerializeNode(programResultItemType, typeEnv);
}

TMaybe<TString> SqlToSExpr(const TString& query) {
    NSQLTranslation::TTranslationSettings settings;
    settings.SyntaxVersion = 1;
    settings.Mode = NSQLTranslation::ESqlMode::QUERY;
    settings.DefaultCluster = "undefined";
    settings.ClusterMapping[settings.DefaultCluster] = "undefined";
    settings.ClusterMapping["csv"] = "csv";
    settings.ClusterMapping["memory"] = "memory";
    settings.ClusterMapping["ydb"] = "ydb";
    settings.EnableGenericUdfs = true;
    settings.File = "generated.sql";

    auto astRes = NSQLTranslation::SqlToYql(query, settings);
    if (!astRes.Issues.Empty()) {
        Cerr << astRes.Issues.ToString() << Endl;
    }

    if (!astRes.Root) {
        return {};
    }

    TStringStream sexpr;
    astRes.Root->PrintTo(sexpr);
    return sexpr.Str();
}

bool ParseCounterName(TString* prefix, std::map<TString, TString>* labels, TString* name, const TString& counterName) {
    auto pos = counterName.find(":");
    if (pos == TString::npos) {
        return false;
    }
    *prefix = counterName.substr(0, pos);

    auto labelsString = counterName.substr(pos+1);

    *name = "";
    for (const auto& kv : StringSplitter(labelsString).Split(',')) {
        TStringBuf key, value;
        const TStringBuf& line = kv.Token();
        if (!line.empty()) {
            line.Split('=', key, value);
            if (key == "Name") {
                *name = value;
            } else {
                (*labels)[TString(key)] = TString(value);
            }
        }
    }

    return !name->empty();
}

bool IsRetriable(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    switch (statusCode) {
    case NYql::NDqProto::StatusIds::UNSPECIFIED:
    case NYql::NDqProto::StatusIds::SUCCESS:
    case NYql::NDqProto::StatusIds::BAD_REQUEST:
    case NYql::NDqProto::StatusIds::LIMIT_EXCEEDED:
    case NYql::NDqProto::StatusIds::UNSUPPORTED:
    case NYql::NDqProto::StatusIds::ABORTED:
    case NYql::NDqProto::StatusIds::CANCELLED:
        return false;
    case NYql::NDqProto::StatusIds::UNAVAILABLE:
    default:
        return true;
    }
}

bool IsRetriable(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    return IsRetriable(ev->Get()->Record.GetStatusCode());
}

bool NeedFallback(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    switch (statusCode) {
    case NYql::NDqProto::StatusIds::UNSPECIFIED:
    case NYql::NDqProto::StatusIds::SUCCESS:
    case NYql::NDqProto::StatusIds::ABORTED:
    case NYql::NDqProto::StatusIds::CANCELLED:
    case NYql::NDqProto::StatusIds::BAD_REQUEST:
    case NYql::NDqProto::StatusIds::PRECONDITION_FAILED:
        return false;
    case NYql::NDqProto::StatusIds::LIMIT_EXCEEDED:
    case NYql::NDqProto::StatusIds::UNAVAILABLE:
    default:
        return true;
    }
}

bool NeedFallback(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    return NeedFallback(ev->Get()->Record.GetStatusCode());
}

} // namespace NCommon

using namespace NNodes;
namespace NDq {
    bool CheckJoinColumns(const TExprBase& node);
} // namespace NDq


const THashSet<TStringBuf> VALID_SOURCES = {DqProviderName, ConfigProviderName, YtProviderName, ClickHouseProviderName, YdbProviderName, S3ProviderName, PgProviderName};
const THashSet<TStringBuf> VALID_SINKS = {ResultProviderName, YtProviderName, S3ProviderName};


class TDqsS3RecaptureTransformer : public TSyncTransformerBase {
public:
    TDqsS3RecaptureTransformer(TS3State::TPtr state)
        : State_(state)
    {
        std::cout << "TDqsS3RecaptureTransformer()" << std::endl;
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {

        std::cout << "------------TDqsS3RecaptureTransformer()::DoTransform" << std::endl;
        YQL_CLOG(INFO, ProviderDq) << "TDqsS3RecaptureTransformer::DoTransform";

        output = input;
        if (ctx.Step.IsDone(TExprStep::Recapture)) {
             std::cout << "return ok" << std::endl;
            return TStatus::Ok;
        }

        Y_SCOPE_EXIT(&) {
            FlushStatistics();
        };

    /*    if (State_->ExternalUser && !State_->Settings->_EnablePorto.Get().GetOrElse(TDqSettings::TDefault::EnablePorto)) {
            Statistics_["DqExternalUser"]++;
            YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
            State_->AbortHidden();
            return TStatus::Ok;
        }

        if (State_->TypeCtx->ForceDq) {
            Statistics_["DqForce"]++;
        }

        if (!State_->TypeCtx->ForceDq) {
            if (!State_->Settings->AnalyzeQuery.Get().GetOrElse(false)) {
                Statistics_["DqAnalyzerOff"]++;
            }

            if (State_->TypeCtx->PureResultDataSource != DqProviderName) {
                Statistics_["DqPureResultDataSourceMismatch"]++;
            }

            if (State_->TypeCtx->PureResultDataSource != DqProviderName || !State_->Settings->AnalyzeQuery.Get().GetOrElse(false)) {
                YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
                State_->AbortHidden();
                return TStatus::Ok;
            }

            Statistics_["DqAnalyzerOn"]++;

            bool good = true;
            TNodeSet visited;
            Scan(*input, ctx, good, visited);

            if (good) {
                Statistics_["DqAnalyzerOk"]++;
            } else {
                Statistics_["DqAnalyzerFail"] ++;
            }

            if (!good) {
                YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
                State_->AbortHidden();
                return TStatus::Ok;
            }
        }
*/
     //   State_->Types->DqFallbackPolicy = State_->Settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default);

        IGraphTransformer::TStatus status = NDq::DqWrapRead(input, output, ctx, *State_->Types, NYql::TDqSettings());
        if (input != output) {
            YQL_CLOG(INFO, ProviderDq) << "DqsRecapture";
            // TODO: Add before/after recapture transformers
            State_->Types->DqCaptured = true;
            // TODO: drop this after implementing DQS ConstraintTransformer
            State_->Types->ExpectedConstraints.clear();
        }
        return status;
    }

    void Rewind() final {
    }

private:
    void AddInfo(TExprContext& ctx, const TString& message) const {
        YQL_CLOG(DEBUG, ProviderDq) << message;
        TIssue info("DQ cannot execute the query. Cause: " + message);
        info.Severity = TSeverityIds::S_INFO;
        ctx.IssueManager.RaiseIssue(info);
    }

    void Scan(const TExprNode& node, TExprContext& ctx, bool& good, TNodeSet& visited) const {
        if (!visited.insert(&node).second) {
            return;
        }

        TExprBase expr(&node);

        if (TCoCommit::Match(&node)) {
            for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                if (i != TCoCommit::idx_DataSink) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        } else if (auto datasource = TMaybeNode<TCoDataSource>(&node).Category()) {
            if (!VALID_SOURCES.contains(datasource.Cast().Value())) {
                AddInfo(ctx, TStringBuilder() << "source '" << datasource.Cast().Value() << "' is not supported by DQ");
                good = false;
            }
        } else if (auto datasink = TMaybeNode<TCoDataSink>(&node).Category()) {
            if (!VALID_SINKS.contains(datasink.Cast().Value())) {
                AddInfo(ctx, TStringBuilder() << "sink '" << datasink.Cast().Value() << "' is not supported by DQ");
                good = false;
            }
        } else if (TMaybeNode<TCoEquiJoin>(&node) && !NDq::CheckJoinColumns(expr)) {
            AddInfo(ctx, TStringBuilder() << "unsupported join column");
            good = false;
        } else if (node.ChildrenSize() > 1 && TCoDataSource::Match(node.Child(1))) {
            auto dataSourceName = node.Child(1)->Child(0)->Content();
            if (dataSourceName != DqProviderName && !node.IsCallable(ConfigureName)) {
                auto datasource = State_->Types->DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(datasource);
                auto dqIntegration = (*datasource)->GetDqIntegration();
                if (dqIntegration) {
                    bool pragmas = dqIntegration->CheckPragmas(node, ctx, false);
                    bool canRead = pragmas && dqIntegration->CanRead(node, ctx, /*skipIssues = */ false);

                    if (!pragmas || !canRead) {
                        good = false;
                        if (!pragmas) {
                            State_->Types->PureResultDataSource.clear();
                            std::erase_if(State_->Types->AvailablePureResultDataSources,
                                          [&](const auto& name) { return name == DqProviderName; });
                        }
                    }
                } else {
                    AddInfo(ctx, TStringBuilder() << "source '" << dataSourceName << "' is not supported by DQ");
                    good = false;
                }
            }

            if (good) {
                Scan(node.Head(), ctx,good, visited);
            }
        } else if (node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::World
            && !TCoCommit::Match(&node)
            && node.ChildrenSize() > 1
            && TCoDataSink::Match(node.Child(1))) {
            auto dataSinkName = node.Child(1)->Child(0)->Content();
            auto dataSink = State_->Types->DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(dataSink);
            if (auto dqIntegration = dataSink->Get()->GetDqIntegration()) {
                if (auto canWrite = dqIntegration->CanWrite(node, ctx)) {
                    if (!canWrite.GetRef()) {
                        good = false;
                    } else if (false/*!State_->Settings->EnableInsert.Get().GetOrElse(false)*/) {
                        AddInfo(ctx, TStringBuilder() << "'insert' support is disabled. Use PRAGMA dq.EnableInsert to explicitly enable it");
                        good = false;
                    }
                }
            }
            if (good) {
                for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        }
        else if (TCoScriptUdf::Match(&node)) {
            if (good && TCoScriptUdf::Match(&node) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(node.Head().Content()))) {
                AddInfo(ctx, TStringBuilder() << "system python udf");
                good = false;
            }
            if (good) {
                for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        }
        else {
            for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                Scan(*node.Child(i), ctx, good, visited);
            }
        }
    }

private:
    TS3State::TPtr State_;

    THashMap<TString, int> Statistics_;

    void FlushStatistics() {
        // TOperationStatistics statistics;
        // for (const auto& [k, v] : Statistics_) {
        //     if (v == 1) {
        //         statistics.Entries.push_back(TOperationStatistics::TEntry(k, 0, 0, 0, 0, 1));
        //     }
        // }

        // TGuard<TMutex> lock(State_->Mutex);
        // if (!statistics.Entries.empty()) {
        //     State_->Statistics[State_->MetricId++] = statistics;
        // }
    }
};

THolder<IGraphTransformer> CreateDqsS3RecaptureTransformer(TS3State::TPtr state) {
    return THolder(new TDqsS3RecaptureTransformer(state));
}

} // namespace NYql
