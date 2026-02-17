#include "worker_factory.h"

#include "type_from_schema.h"
#include "worker.h"
#include "compile_mkql.h"

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/langver/yql_core_langver.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/providers/common/arrow_resolve/yql_simple_arrow_resolver.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/aligned_page_pool.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/public/purecalc/common/names.h>
#include <yql/essentials/public/purecalc/common/transformations/type_annotation.h>
#include <yql/essentials/public/purecalc/common/transformations/align_output_schema.h>
#include <yql/essentials/public/purecalc/common/transformations/extract_used_columns.h>
#include <yql/essentials/public/purecalc/common/transformations/output_columns_filter.h>
#include <yql/essentials/public/purecalc/common/transformations/replace_table_reads.h>
#include <yql/essentials/public/purecalc/common/transformations/root_to_blocks.h>
#include <yql/essentials/public/purecalc/common/transformations/utils.h>
#include <yql/essentials/utils/log/log.h>
#include <util/stream/trace.h>

using namespace NYql;
using namespace NYql::NPureCalc;

template <typename TBase>
TWorkerFactory<TBase>::TWorkerFactory(TWorkerFactoryOptions options, EProcessorMode processorMode)
    : Factory_(std::move(options.Factory))
    , FuncRegistry_(std::move(options.FuncRegistry))
    , UserData_(std::move(options.UserData))
    , LLVMSettings_(std::move(options.LLVMSettings))
    , BlockEngineMode_(options.BlockEngineMode)
    , ExprOutputStream_(options.ExprOutputStream)
    , CountersProvider_(options.CountersProvider)
    , NativeYtTypeFlags_(options.NativeYtTypeFlags)
    , DeterministicTimeProviderSeed_(options.DeterministicTimeProviderSeed)
    , UseSystemColumns_(options.UseSystemColumns)
    , UseWorkerPool_(options.UseWorkerPool)
    , LangVer_(options.LangVer)
    , IssueReportTarget_(options.IssueReportTarget)
{
    HandleInternalSettings(options.InternalSettings);

    // Prepare input struct types and extract all column names from inputs
    auto typeCtx = PrepareTypeContext(options.ModuleResolver);

    const auto& inputSchemas = options.InputSpec.GetSchemas();
    const auto& allVirtualColumns = options.InputSpec.GetAllVirtualColumns();

    YQL_ENSURE(inputSchemas.size() == allVirtualColumns.size());

    const auto inputsCount = inputSchemas.size();

    for (ui32 i = 0; i < inputsCount; ++i) {
        const auto* originalInputType = MakeTypeFromSchema(inputSchemas[i], ExprContext_, IssueReportTarget_);
        if (!ValidateInputSchema(originalInputType, ExprContext_, *typeCtx)) {
            ythrow TCompileError("", GetIssues().ToString()) << "invalid schema for #" << i << " input";
        }

        const auto* originalStructType = originalInputType->template Cast<TStructExprType>();
        const auto* structType = ExtendStructType(originalStructType, allVirtualColumns[i], ExprContext_, IssueReportTarget_);

        InputTypes_.push_back(structType);
        OriginalInputTypes_.push_back(originalStructType);
        RawInputTypes_.push_back(originalStructType);

        auto& columnsSet = AllColumns_.emplace_back();
        for (const auto* structItem : structType->GetItems()) {
            columnsSet.insert(TString(structItem->GetName()));

            if (!UseSystemColumns_ && structItem->GetName().StartsWith(PurecalcSysColumnsPrefix)) {
                ythrow TCompileError("", GetIssues().ToString())
                    << "#" << i << " input provides system column " << structItem->GetName()
                    << ", but it is forbidden by options";
            }
        }
    }

    // Prepare output type

    auto outputSchema = options.OutputSpec.GetSchema();
    if (!outputSchema.IsNull()) {
        OutputType_ = MakeTypeFromSchema(outputSchema, ExprContext_, IssueReportTarget_);
        if (!ValidateOutputSchema(OutputType_, ExprContext_, *typeCtx)) {
            ythrow TCompileError("", GetIssues().ToString()) << "invalid output schema";
        }
    } else {
        OutputType_ = nullptr;
    }

    RawOutputType_ = OutputType_;

    // Translate

    if (options.TranslationMode == ETranslationMode::Mkql) {
        SerializedProgram_ = TString{options.Query};
    } else {
        ExprRoot_ = Compile(options.Query, options.TranslationMode,
                            options.SyntaxVersion, options.Modules,
                            options.InputSpec, options.OutputSpec, processorMode, typeCtx.Get());

        RawOutputType_ = GetSequenceItemType(ExprRoot_->Pos(), ExprRoot_->GetTypeAnn(), true, ExprContext_);

        // Deduce output type if it wasn't provided by output spec

        if (!OutputType_) {
            OutputType_ = RawOutputType_;
            // XXX: Tweak the obtained expression type, is the spec supports blocks:
            // 1. Remove "_yql_block_length" attribute, since it's for internal usage.
            // 2. Strip block container from the type to store its internal type.
            if (options.OutputSpec.AcceptsBlocks()) {
                Y_ENSURE(OutputType_->GetKind() == ETypeAnnotationKind::Struct);
                OutputType_ = UnwrapBlockStruct(OutputType_->Cast<TStructExprType>(), ExprContext_);
            }
        }
        if (!OutputType_) {
            ythrow TCompileError("", GetIssues().ToString()) << "cannot deduce output schema";
        }
    }
}

template <typename TBase>
void TWorkerFactory<TBase>::HandleInternalSettings(const TInternalProgramSettings& settings) {
    if (settings.NodesAllocationLimit) {
        ExprContext_.NodesAllocationLimit = *settings.NodesAllocationLimit;
    }

    if (settings.StringsAllocationLimit) {
        ExprContext_.StringsAllocationLimit = *settings.StringsAllocationLimit;
    }

    if (settings.RepeatTransformLimit) {
        ExprContext_.RepeatTransformLimit = *settings.RepeatTransformLimit;
    }
}

template <typename TBase>
TIntrusivePtr<TTypeAnnotationContext> TWorkerFactory<TBase>::PrepareTypeContext(
    IModuleResolver::TPtr factoryModuleResolver) {
    // Prepare type annotation context

    IModuleResolver::TPtr moduleResolver = factoryModuleResolver ? factoryModuleResolver->CreateMutableChild() : nullptr;
    auto typeContext = MakeIntrusive<TTypeAnnotationContext>();
    typeContext->LangVer = LangVer_;
    typeContext->UseTypeDiffForConvertToError = true;
    typeContext->RandomProvider = CreateDefaultRandomProvider();
    typeContext->TimeProvider = DeterministicTimeProviderSeed_ ? CreateDeterministicTimeProvider(*DeterministicTimeProviderSeed_) : CreateDefaultTimeProvider();
    typeContext->UdfResolver = NCommon::CreateSimpleUdfResolver(FuncRegistry_.Get());
    typeContext->ArrowResolver = MakeSimpleArrowResolver(*FuncRegistry_.Get());
    typeContext->UserDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, UserData_, nullptr, nullptr);
    typeContext->Modules = moduleResolver;
    typeContext->BlockEngineMode = BlockEngineMode_;
    auto configProvider = CreateConfigProvider(*typeContext, nullptr, "");
    typeContext->AddDataSource(ConfigProviderName, configProvider);
    typeContext->Initialize(ExprContext_);

    if (auto modules = dynamic_cast<TModuleResolver*>(moduleResolver.get())) {
        modules->AttachUserData(typeContext->UserDataStorage);
        modules->SetUseCanonicalLibrarySuffix(true);
    }

    return typeContext;
}

template <typename TBase>
TExprNode::TPtr TWorkerFactory<TBase>::Compile(
    TStringBuf query,
    ETranslationMode mode,
    ui16 syntaxVersion,
    const THashMap<TString, TString>& modules,
    const TInputSpecBase& inputSpec,
    const TOutputSpecBase& outputSpec,
    EProcessorMode processorMode,
    TTypeAnnotationContext* typeContext) {
    if (mode == ETranslationMode::PG && processorMode != EProcessorMode::PullList) {
        ythrow TCompileError("", "") << "only PullList mode is compatible to PostgreSQL syntax";
    }

    TMaybe<TIssue> verIssue;
    if (!CheckLangVersion(LangVer_, GetMaxReleasedLangVersion(), verIssue)) {
        TIssues issues;
        issues.AddIssue(*verIssue);
        ythrow TCompileError("", issues.ToString());
    }

    // Parse SQL/s-expr into AST

    TAstParseResult astRes;

    if (mode == ETranslationMode::SQL || mode == ETranslationMode::PG) {
        NSQLTranslation::TTranslationSettings settings;

        typeContext->DeprecatedSQL = (syntaxVersion == 0);
        if (mode == ETranslationMode::PG) {
            settings.PgParser = true;
        }

        settings.LangVer = LangVer_;
        settings.SyntaxVersion = syntaxVersion;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        settings.EmitReadsForExists = true;
        settings.Antlr4Parser = true;
        settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;
        settings.DefaultCluster = PurecalcDefaultCluster;
        settings.ClusterMapping[settings.DefaultCluster] = PurecalcDefaultService;
        settings.ModuleMapping = modules;
        settings.EnableGenericUdfs = true;
        settings.File = "generated.sql";
        settings.Flags = {
            "AnsiOrderByLimitInUnionAll",
            "AnsiRankForNullableKeys",
            "DisableAnsiOptionalAs",
            "DisableCoalesceJoinKeysOnQualifiedAll",
            "DisableUnorderedSubqueries",
            "FlexibleTypes"};
        if (BlockEngineMode_ != EBlockEngineMode::Disable) {
            settings.Flags.insert("EmitAggApply");
        }
        for (const auto& [key, block] : UserData_) {
            TStringBuf alias(key.Alias());
            if (block.Usage.Test(EUserDataBlockUsage::Library) && !alias.StartsWith("/lib")) {
                alias.SkipPrefix("/home/");
                settings.Libraries.emplace(alias);
            }
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

        NSQLTranslation::TTranslators translators(
            nullptr,
            NSQLTranslationV1::MakeTranslator(lexers, parsers),
            NSQLTranslationPG::MakeTranslator());

        astRes = SqlToYql(translators, TString(query), settings);
    } else {
        astRes = ParseAst(query);
    }

    if (verIssue) {
        ExprContext_.IssueManager.RaiseIssue(*verIssue);
    }

    ExprContext_.IssueManager.AddIssues(astRes.Issues);
    if (!astRes.IsOk()) {
        ythrow TCompileError(TString(query), GetIssues().ToString()) << "failed to parse " << mode;
    }

    if (ETraceLevel::TRACE_DETAIL <= StdDbgLevel()) {
        Cdbg << "Before optimization:" << Endl;
        astRes.Root->PrettyPrintTo(Cdbg, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote | TAstPrintFlags::AdaptArbitraryContent);
    }

    // Translate AST into expression

    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*astRes.Root, exprRoot, ExprContext_, typeContext->Modules.get(), nullptr, 0, syntaxVersion)) {
        TStringStream astStr;
        astRes.Root->PrettyPrintTo(astStr, TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine);
        ythrow TCompileError(astStr.Str(), GetIssues().ToString()) << "failed to compile";
    }

    // Prepare transformation pipeline
    THolder<IGraphTransformer> calcTransformer = CreateFunctorTransformer([&](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx)
                                                                              -> IGraphTransformer::TStatus {
        output = input;
        auto valueNode = input->HeadPtr();

        auto peepHole = MakePeepholeOptimization(typeContext);
        auto status = SyncTransform(*peepHole, valueNode, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        TStringStream out;
        NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
        writer.OnBeginMap();

        writer.OnKeyedItem("Data");

        TWorkerGraph graph(
            valueNode,
            ctx,
            {},
            *FuncRegistry_,
            UserData_,
            {},
            {},
            {},
            valueNode->GetTypeAnn(),
            valueNode->GetTypeAnn(),
            LLVMSettings_,
            CountersProvider_,
            NativeYtTypeFlags_,
            DeterministicTimeProviderSeed_,
            LangVer_,
            true);

        with_lock (graph.ScopedAlloc) {
            const auto value = graph.ComputationGraph->GetValue();
            NCommon::WriteYsonValue(writer, value, const_cast<NKikimr::NMiniKQL::TType*>(graph.OutputType), nullptr);
        }
        writer.OnEndMap();

        auto ysonAtom = ctx.NewAtom(TPositionHandle(), out.Str());
        input->SetResult(std::move(ysonAtom));
        return IGraphTransformer::TStatus::Ok;
    });

    const TString& selfName = TString(inputSpec.ProvidesBlocks()
                                          ? PurecalcBlockInputCallableName
                                          : PurecalcInputCallableName);

    TTypeAnnCallableFactory typeAnnCallableFactory = [&]() {
        return MakeTypeAnnotationTransformer(typeContext, InputTypes_, RawInputTypes_, processorMode, selfName);
    };

    TTransformationPipeline pipeline(typeContext, typeAnnCallableFactory);

    pipeline.Add(MakeTableReadsReplacer(InputTypes_, UseSystemColumns_, processorMode, selfName),
                 "ReplaceTableReads", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Replace reads from tables");
    pipeline.AddServiceTransformers();
    pipeline.AddPreTypeAnnotation();
    pipeline.AddExpressionEvaluation(*FuncRegistry_, calcTransformer.Get());
    pipeline.AddIOAnnotation();
    pipeline.AddTypeAnnotationTransformer();
    pipeline.AddPostTypeAnnotation();
    pipeline.Add(CreateFunctorTransformer(
                     [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                         return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
                             if (node->IsCallable("Unordered") && node->Child(0)->IsCallable({PurecalcInputCallableName, PurecalcBlockInputCallableName})) {
                                 return node->ChildPtr(0);
                             }
                             return node;
                         }, ctx, TOptimizeExprSettings(nullptr));
                     }), "Unordered", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Unordered optimizations");
    pipeline.Add(CreateFunctorTransformer(
                     [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                         return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
                             if (node->IsCallable("Right!") && node->Head().IsCallable("Cons!")) {
                                 return node->Head().ChildPtr(1);
                             }

                             return node;
                         }, ctx, TOptimizeExprSettings(nullptr));
                     }), "Cons", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Cons optimizations");
    pipeline.Add(MakeOutputColumnsFilter(outputSpec.GetOutputColumnsFilter()),
                 "Filter", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Filter output columns");
    pipeline.Add(MakeRootToBlocks(outputSpec.AcceptsBlocks(), processorMode),
                 "RootToBlocks", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Rewrite the root if the output spec accepts blocks");
    pipeline.Add(MakeOutputAligner(OutputType_, outputSpec.AcceptsBlocks(), processorMode, *typeContext),
                 "Convert", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Align return type of the program to output schema");
    pipeline.AddCommonOptimization();
    pipeline.AddFinalCommonOptimization();
    pipeline.Add(MakeUsedColumnsExtractor(&UsedColumns_, AllColumns_),
                 "ExtractColumns", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Extract used columns");
    pipeline.Add(MakePeepholeOptimization(typeContext),
                 "PeepHole", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                 "Peephole optimizations");
    pipeline.AddCheckExecution(false);

    // Apply optimizations

    auto transformer = pipeline.Build();
    auto status = SyncTransform(*transformer, exprRoot, ExprContext_);
    auto transformStats = transformer->GetStatistics();
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Pretty);
    NCommon::TransformerStatsToYson("", transformStats, writer);
    YQL_CLOG(DEBUG, Core) << "Transform stats: " << out.Str();
    if (status == IGraphTransformer::TStatus::Error) {
        ythrow TCompileError("", GetIssues().ToString()) << "Failed to optimize";
    }

    IOutputStream* exprOut = nullptr;
    if (ExprOutputStream_) {
        exprOut = ExprOutputStream_;
    } else if (ETraceLevel::TRACE_DETAIL <= StdDbgLevel()) {
        exprOut = &Cdbg;
    }

    if (exprOut) {
        *exprOut << "After optimization:" << Endl;
        ConvertToAst(*exprRoot, ExprContext_, 0, true).Root->PrettyPrintTo(*exprOut, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote | TAstPrintFlags::AdaptArbitraryContent);
    }
    return exprRoot;
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeInputSchema(ui32 inputIndex) const {
    Y_ENSURE(
        inputIndex < InputTypes_.size(),
        "invalid input index (" << inputIndex << ") in MakeInputSchema call");

    return NCommon::TypeToYsonNode(InputTypes_[inputIndex]);
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeInputSchema() const {
    Y_ENSURE(
        InputTypes_.size() == 1,
        "MakeInputSchema() can be used only with single-input programs");

    return NCommon::TypeToYsonNode(InputTypes_[0]);
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeOutputSchema() const {
    Y_ENSURE(OutputType_, "MakeOutputSchema() cannot be used with precompiled programs");
    Y_ENSURE(
        OutputType_->GetKind() == ETypeAnnotationKind::Struct,
        "MakeOutputSchema() cannot be used with multi-output programs");

    return NCommon::TypeToYsonNode(OutputType_);
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeOutputSchema(ui32 index) const {
    Y_ENSURE(OutputType_, "MakeOutputSchema() cannot be used with precompiled programs");
    Y_ENSURE(
        OutputType_->GetKind() == ETypeAnnotationKind::Variant,
        "MakeOutputSchema(ui32) cannot be used with single-output programs");

    auto vtype = OutputType_->template Cast<TVariantExprType>();

    Y_ENSURE(
        vtype->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple,
        "MakeOutputSchema(ui32) cannot be used to process variants over struct");

    auto ttype = vtype->GetUnderlyingType()->template Cast<TTupleExprType>();

    Y_ENSURE(
        index < ttype->GetSize(),
        "Invalid table index " << index);

    return NCommon::TypeToYsonNode(ttype->GetItems()[index]);
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeOutputSchema(TStringBuf tableName) const {
    Y_ENSURE(OutputType_, "MakeOutputSchema() cannot be used with precompiled programs");
    Y_ENSURE(
        OutputType_->GetKind() == ETypeAnnotationKind::Variant,
        "MakeOutputSchema(TStringBuf) cannot be used with single-output programs");

    auto vtype = OutputType_->template Cast<TVariantExprType>();

    Y_ENSURE(
        vtype->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct,
        "MakeOutputSchema(TStringBuf) cannot be used to process variants over tuple");

    auto stype = vtype->GetUnderlyingType()->template Cast<TStructExprType>();

    auto index = stype->FindItem(tableName);

    Y_ENSURE(
        index.Defined(),
        "Invalid table index " << TString{tableName}.Quote());

    return NCommon::TypeToYsonNode(stype->GetItems()[*index]->GetItemType());
}

template <typename TBase>
NYT::TNode TWorkerFactory<TBase>::MakeFullOutputSchema() const {
    Y_ENSURE(OutputType_, "MakeFullOutputSchema() cannot be used with precompiled programs");
    return NCommon::TypeToYsonNode(OutputType_);
}

template <typename TBase>
const THashSet<TString>& TWorkerFactory<TBase>::GetUsedColumns(ui32 inputIndex) const {
    Y_ENSURE(
        inputIndex < UsedColumns_.size(),
        "invalid input index (" << inputIndex << ") in GetUsedColumns call");

    return UsedColumns_[inputIndex];
}

template <typename TBase>
const THashSet<TString>& TWorkerFactory<TBase>::GetUsedColumns() const {
    Y_ENSURE(
        UsedColumns_.size() == 1,
        "GetUsedColumns() can be used only with single-input programs");

    return UsedColumns_[0];
}

template <typename TBase>
TIssues TWorkerFactory<TBase>::GetIssues() const {
    auto issues = ExprContext_.IssueManager.GetCompletedIssues();
    CheckFatalIssues(issues, IssueReportTarget_);
    return issues;
}

template <typename TBase>
TString TWorkerFactory<TBase>::GetCompiledProgram() {
    if (ExprRoot_) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                                              FuncRegistry_->SupportsSizedAllocators());
        NKikimr::NMiniKQL::TTypeEnvironment env(alloc);

        auto rootNode = CompileMkql(ExprRoot_, ExprContext_, *FuncRegistry_, env, UserData_);
        return NKikimr::NMiniKQL::SerializeRuntimeNode(rootNode, env.GetNodeStack());
    }

    return SerializedProgram_;
}

template <typename TBase>
void TWorkerFactory<TBase>::ReturnWorker(IWorker* worker) {
    THolder<IWorker> tmp(worker);
    if (UseWorkerPool_) {
        WorkerPool_.push_back(std::move(tmp));
    }
}

#define DEFINE_WORKER_MAKER(MODE)                                                   \
    TWorkerHolder<I##MODE##Worker> T##MODE##WorkerFactory::MakeWorker() {           \
        if (!WorkerPool_.empty()) {                                                 \
            auto res = std::move(WorkerPool_.back());                               \
            WorkerPool_.pop_back();                                                 \
            return TWorkerHolder<I##MODE##Worker>((I##MODE##Worker*)res.Release()); \
        }                                                                           \
        return TWorkerHolder<I##MODE##Worker>(new T##MODE##Worker(                  \
            weak_from_this(),                                                       \
            ExprRoot_,                                                              \
            ExprContext_,                                                           \
            SerializedProgram_,                                                     \
            *FuncRegistry_,                                                         \
            UserData_,                                                              \
            InputTypes_,                                                            \
            OriginalInputTypes_,                                                    \
            RawInputTypes_,                                                         \
            OutputType_,                                                            \
            RawOutputType_,                                                         \
            LLVMSettings_,                                                          \
            CountersProvider_,                                                      \
            NativeYtTypeFlags_,                                                     \
            DeterministicTimeProviderSeed_,                                         \
            LangVer_));                                                             \
    }

DEFINE_WORKER_MAKER(PullStream)
DEFINE_WORKER_MAKER(PullList)
DEFINE_WORKER_MAKER(PushStream)

namespace NYql::NPureCalc {
template class TWorkerFactory<IPullStreamWorkerFactory>;

template class TWorkerFactory<IPullListWorkerFactory>;

template class TWorkerFactory<IPushStreamWorkerFactory>;
} // namespace NYql::NPureCalc
