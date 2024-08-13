#include "yql_yt_file.h"
#include "yql_yt_file_mkql_compiler.h"
#include "yql_yt_file_comp_nodes.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <ydb/library/yql/providers/yt/lib/infer_schema/infer_schema.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_mkql_compiler.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/gateway/lib/query_cache.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_table.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/utils/threading/async_queue.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/node/node_builder.h>

#include <library/cpp/yson/writer.h>
#include <library/cpp/yson/parser.h>

#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/string/split.h>
#include <util/string/builder.h>
#include <util/folder/path.h>
#include <util/generic/yexception.h>
#include <util/generic/xrange.h>
#include <util/generic/ptr.h>
#include <util/random/random.h>

#include <algorithm>
#include <iterator>
#include <cmath>


namespace NYql {

using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using namespace NThreading;

namespace NFile {

///////////////////////////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IFunctionRegistry> MakeFunctionRegistry(
    const IFunctionRegistry& functionRegistry, const TUserDataTable& files,
    const TFileStoragePtr& fileStorage, TVector<TFileLinkPtr>& externalFiles) {
    auto cloned = functionRegistry.Clone();
    for (auto& d : files) {
        if (d.first.IsFile() && d.second.Usage.Test(EUserDataBlockUsage::Udf)) {
            if (d.second.Type == EUserDataType::PATH) {
                cloned->LoadUdfs(d.second.Data, {}, 0, d.second.CustomUdfPrefix);
            } else if (fileStorage && d.second.Type == EUserDataType::URL) {
                auto externalFile = fileStorage->PutUrl(d.second.Data, "");
                externalFiles.push_back(externalFile);
                cloned->LoadUdfs(externalFile->GetPath().GetPath(), {}, 0, d.second.CustomUdfPrefix);
            } else {
                MKQL_ENSURE(false, "Unsupported block type");
            }
        }
    }
    return cloned;
}


///////////////////////////////////////////////////////////////////////////////////////////////////////

struct TSession {
    TSession(const IYtGateway::TOpenSessionOptions& options, bool keepTempTables)
        : RandomProvider_(options.RandomProvider())
        , TimeProvider_(options.TimeProvider())
        , KeepTempTables_(keepTempTables)
        , InflightTempTablesLimit_(Max<ui32>())
        , ConfigInitDone_(false)
    {
    }

    ~TSession() {
        if (!KeepTempTables_) {
            for (auto& x : TempTables_) {
                for (auto& path: x.second) {
                    try {
                        NFs::Remove(path);
                        NFs::Remove(path + ".attr");
                    } catch (...) {
                    }
                }
            }
        }
    }

    void DeleteAtFinalize(const TYtSettings::TConstPtr& config, const TString& cluster, const TString& table) {
        if (!ConfigInitDone_) {
            InflightTempTablesLimit_ = config->InflightTempTablesLimit.Get().GetOrElse(Max<ui32>());
            if (GetReleaseTempDataMode(*config) == EReleaseTempDataMode::Never) {
                KeepTempTables_ = true;
            }
            ConfigInitDone_ = true;
        }

        auto& tempTables = TempTables_[cluster];
        tempTables.insert(table);
        if (tempTables.size() > InflightTempTablesLimit_) {
            ythrow yexception() << "Too many temporary tables registered - limit is " << InflightTempTablesLimit_;
        }
    }

    void CancelDeleteAtFinalize(const TString& cluster, const TString& table) {
        TempTables_[cluster].erase(table);
    }

    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    bool KeepTempTables_;
    ui32 InflightTempTablesLimit_;
    bool ConfigInitDone_;

    THashMap<TString, THashSet<TString>> TempTables_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

struct TFileYtLambdaBuilder: public TLambdaBuilder {
    TFileYtLambdaBuilder(TScopedAlloc& alloc, const TSession& /*session*/,
        TIntrusivePtr<IFunctionRegistry> customFunctionRegistry,
        const NUdf::ISecureParamsProvider* secureParamsProvider)
        : TLambdaBuilder(customFunctionRegistry.Get(), alloc, nullptr, CreateDeterministicRandomProvider(1), CreateDeterministicTimeProvider(10000000),
          nullptr, nullptr, secureParamsProvider)
        , CustomFunctionRegistry_(customFunctionRegistry)
    {}

    TIntrusivePtr<IFunctionRegistry> CustomFunctionRegistry_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TFileTransformProvider {
public:
    TFileTransformProvider(const TYtFileServices::TPtr& services, const TUserDataTable& userDataBlocks)
        : Services(services)
        , UserDataBlocks(userDataBlocks)
        , ExtraArgs(std::make_shared<THashMap<TString, TRuntimeNode>>())
    {
    }

    TCallableVisitFunc operator()(TInternName name) {
        if (name == "FilePath") {
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
                const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                auto block = TUserDataStorage::FindUserDataBlock(UserDataBlocks, name);
                MKQL_ENSURE(block, "File not found: " << name);
                MKQL_ENSURE(block->Type == EUserDataType::PATH, "FilePath not supported for non-file data block, name: "
                    << name << ", block type: " << block->Type);
                return TProgramBuilder(env, *Services->GetFunctionRegistry()).NewDataLiteral<NUdf::EDataSlot::String>(block->Data);
            };
        }

        if (name == "FolderPath") {
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
                const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                auto folderName = TUserDataStorage::MakeFolderName(name);
                TMaybe<TString> folderPath;
                for (const auto& x : UserDataBlocks) {
                    if (!x.first.Alias().StartsWith(folderName)) {
                        continue;
                    }

                    MKQL_ENSURE(x.second.Type == EUserDataType::PATH, "FilePath not supported for non-file data block, name: "
                        << x.first.Alias() << ", block type: " << x.second.Type);
                    auto newFolderPath = x.second.Data.substr(0, x.second.Data.size() - (x.first.Alias().size() - folderName.size()));
                    if (!folderPath) {
                        folderPath = newFolderPath;
                    } else {
                        MKQL_ENSURE(*folderPath == newFolderPath, "File " << x.second.Data << " is out of directory " << *folderPath);
                    }
                }

                return TProgramBuilder(env, *Services->GetFunctionRegistry()).NewDataLiteral<NUdf::EDataSlot::String>(*folderPath);
            };
        }

        if (name == "FileContent") {
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
                const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                auto block = TUserDataStorage::FindUserDataBlock(UserDataBlocks, name);
                MKQL_ENSURE(block, "File not found: " << name);
                const TProgramBuilder pgmBuilder(env, *Services->GetFunctionRegistry());
                if (block->Type == EUserDataType::PATH) {
                    auto content = TFileInput(block->Data).ReadAll();
                    return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
                }
                else if (block->Type == EUserDataType::RAW_INLINE_DATA) {
                    return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(block->Data);
                }
                else if (Services->GetFileStorage() && block->Type == EUserDataType::URL) {
                    auto link = Services->GetFileStorage()->PutUrl(block->Data, "");
                    auto content = TFileInput(link->GetPath()).ReadAll();
                    return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
                } else {
                   MKQL_ENSURE(false, "Unsupported block type");
                }
            };
        }

        if (name == TYtTableIndex::CallableName()) {
            return [this, name](NMiniKQL::TCallable&, const TTypeEnvironment& env) {
                return GetExtraArg(TString{name.Str()}, NUdf::EDataSlot::Uint32, env);
            };
        }
        if (name == TYtTablePath::CallableName()) {
            return [this, name](NMiniKQL::TCallable&, const TTypeEnvironment& env) {
                return GetExtraArg(TString{name.Str()}, NUdf::EDataSlot::String, env);
            };
        }
        if (name == TYtTableRecord::CallableName()) {
            return [this, name](NMiniKQL::TCallable&, const TTypeEnvironment& env) {
                return GetExtraArg(TString{name.Str()}, NUdf::EDataSlot::Uint64, env);
            };
        }
        if (name == TYtIsKeySwitch::CallableName()) {
            return [this, name](NMiniKQL::TCallable&, const TTypeEnvironment& env) {
                return GetExtraArg(TString{name.Str()}, NUdf::EDataSlot::Bool, env);
            };
        }
        if (name == TYtRowNumber::CallableName()) {
            return [this, name](NMiniKQL::TCallable&, const TTypeEnvironment& env) {
                return GetExtraArg(TString{name.Str()}, NUdf::EDataSlot::Uint64, env);
            };
        }

        if (name == TYtTableContent::CallableName()) {
            return [name](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                TCallableBuilder callableBuilder(env,
                    TStringBuilder() << TYtTableContent::CallableName() << "File",
                    callable.GetType()->GetReturnType(), false);
                for (ui32 i: xrange(callable.GetInputsCount())) {
                    callableBuilder.Add(callable.GetInput(i));
                }
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        if (name == "YtTableInput") {
            return [this, name](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                TCallableBuilder callableBuilder(env, "YtTableInputFile", callable.GetType()->GetReturnType(), false);
                for (ui32 i: xrange(callable.GetInputsCount())) {
                    callableBuilder.Add(callable.GetInput(i));
                }
                callableBuilder.Add(GetExtraArg(TString(TYtTableIndex::CallableName()), NUdf::EDataSlot::Uint32, env));
                callableBuilder.Add(GetExtraArg(TString(TYtTablePath::CallableName()), NUdf::EDataSlot::String, env));
                callableBuilder.Add(GetExtraArg(TString(TYtTableRecord::CallableName()), NUdf::EDataSlot::Uint64, env));
                callableBuilder.Add(GetExtraArg(TString(TYtRowNumber::CallableName()), NUdf::EDataSlot::Uint64, env));
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        if (name == "YtTableInputNoCtx") {
            return [name](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                TCallableBuilder callableBuilder(env, "YtTableInputNoCtxFile", callable.GetType()->GetReturnType(), false);
                for (ui32 i: xrange(callable.GetInputsCount())) {
                    callableBuilder.Add(callable.GetInput(i));
                }
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        if (name == "YtUngroupingList") {
            return [this, name](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                TCallableBuilder callableBuilder(env, "YtUngroupingListFile", callable.GetType()->GetReturnType(), false);
                for (ui32 i: xrange(callable.GetInputsCount())) {
                    callableBuilder.Add(callable.GetInput(i));
                }
                callableBuilder.Add(GetExtraArg(TString(TYtIsKeySwitch::CallableName()), NUdf::EDataSlot::Bool, env));
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        return TCallableVisitFunc();
    }

private:
    TRuntimeNode GetExtraArg(const TString& name, NUdf::EDataSlot slot, const TTypeEnvironment& env) {
        TRuntimeNode& node = (*ExtraArgs)[name];
        if (!node) {
            TCallableBuilder builder(env, "Arg", TDataType::Create(NUdf::GetDataTypeInfo(slot).TypeId, env), true);
            node = TRuntimeNode(builder.Build(), false);
        }
        return node;
    }

private:
    TYtFileServices::TPtr Services;
    const TUserDataTable& UserDataBlocks;
    std::shared_ptr<THashMap<TString, TRuntimeNode>> ExtraArgs;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TYtFileGateway : public IYtGateway {
public:
    TYtFileGateway(const TYtFileServices::TPtr& services, bool* emulateOutputForMultirunPtr)
        : Services_(services)
        , MkqlCompiler_(MakeIntrusive<NCommon::TMkqlCommonCallableCompiler>())
        , EmulateOutputForMultirunPtr(emulateOutputForMultirunPtr)
        , FakeQueue_(TAsyncQueue::Make(1, "FakePool"))
    {
        RegisterYtMkqlCompilers(*MkqlCompiler_);
        RegisterYtFileMkqlCompilers(*MkqlCompiler_);
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        if (!Sessions.emplace(options.SessionId(), TSession(options, Services_->GetKeepTempTables())).second) {
            ythrow yexception() << "Session already exists: " << options.SessionId();
        }
    }

    NThreading::TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        Sessions.erase(options.SessionId());
        return MakeFuture();
    }

    NThreading::TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        Y_UNUSED(options);
        return MakeFuture();
    }

    template<typename T>
    TSession* GetSession(const T& options) {
        const auto session = Sessions.FindPtr(options.SessionId());
        YQL_ENSURE(session);
        return session;
    }

    template<typename T>
    const TSession* GetSession(const T& options) const {
        const auto session = Sessions.FindPtr(options.SessionId());
        YQL_ENSURE(session);
        return session;
    }

    TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& /*options*/) final {
        TFinalizeResult res;
        res.SetSuccess();
        return MakeFuture(res);
    }

    TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) final {
        TCanonizePathsResult res;
        std::transform(
            options.Paths().begin(), options.Paths().end(),
            std::back_inserter(res.Data),
            [] (const TCanonizeReq& req) {
                return CanonizedPath(req.Path());
            });
        res.SetSuccess();
        return MakeFuture(res);
    }

    bool ShouldEmulateOutputForMultirun(const TTableReq& req) {
        return EmulateOutputForMultirunPtr && *EmulateOutputForMultirunPtr &&
            req.Cluster() == "plato" && req.Table() == "Output";
    }

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) final {
        TTableInfoResult res;
        try {
            for (const TTableReq& req: options.Tables()) {
                auto path = Services_->GetTablePath(req.Cluster(), req.Table(), req.Anonymous(), true);
                const bool exists = NFs::Exists(path) && !ShouldEmulateOutputForMultirun(req);

                res.Data.emplace_back();

                res.Data.back().WriteLock = HasModifyIntents(req.Intents());

                TYtTableMetaInfo::TPtr metaData = new TYtTableMetaInfo;
                metaData->DoesExist = exists;
                if (exists) {
                    try {
                        LoadTableMetaInfo(req, path, *metaData);
                    } catch (const yexception& e) {
                        throw yexception() << "Error loading " << req.Cluster() << '.' << req.Table() << " table metadata: " << e.what();
                    }
                }
                res.Data.back().Meta = metaData;

                if (exists) {
                    TYtTableStatInfo::TPtr statData = new TYtTableStatInfo;
                    statData->Id = req.Table();
                    if (metaData->SqlView.empty()) {
                        try {
                            LoadTableStatInfo(path, *statData);
                        } catch (const yexception& e) {
                            throw yexception() << "Error loading " << req.Cluster() << '.' << req.Table() << " table stat: " << e.what();
                        }

                        auto fullTableName = TString(YtProviderName).append('.').append(req.Cluster()).append('.').append(req.Table());
                        Services_->LockPath(path, fullTableName);
                    }
                    res.Data.back().Stat = statData;
                }
            }

            res.SetSuccess();
        } catch (const yexception& e) {
            res = NCommon::ResultFromException<TTableInfoResult>(e);
        }
        return MakeFuture(res);
    }

    TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) final {
        auto pos = options.Pos();
        try {
            TSession* session = GetSession(options);
            
            TSet<TString> uniqueTables;
            const auto fullPrefix = options.Prefix().Empty() ? TString() : (options.Prefix() + '/');
            const auto fullSuffix = options.Suffix().Empty() ? TString() : ('/' + options.Suffix());
            for (const auto& [tableName, _] : Services_->GetTablesMapping()) {
                TVector<TString> parts;
                Split(tableName, ".", parts);
                if (parts.size() != 3) {
                    continue;
                }
                if (parts[0] != YtProviderName || parts[1] != options.Cluster()) {
                    continue;
                }
                if (!parts[2].StartsWith(fullPrefix)) {
                    continue;
                }
                if (!parts[2].EndsWith(fullSuffix)) {
                    continue;
                }
                uniqueTables.insert(parts[2]);
            }

            TTableRangeResult res;
            res.SetSuccess();

            if (!uniqueTables.empty()) {
                if (auto filter = options.Filter()) {
                    auto exprCtx = options.ExprCtx();
                    YQL_ENSURE(exprCtx);
                    TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
                        Services_->GetFunctionRegistry()->SupportsSizedAllocators());
                    alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                    auto secureParamsProvider = MakeSimpleSecureParamsProvider(options.SecureParams());
                    TVector<TFileLinkPtr> externalFiles;
                    TFileYtLambdaBuilder builder(alloc, *session,
                        MakeFunctionRegistry(*Services_->GetFunctionRegistry(), options.UserDataBlocks(), Services_->GetFileStorage(), externalFiles), secureParamsProvider.get());
                    TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *Services_->GetFunctionRegistry());

                    TVector<TRuntimeNode> strings;
                    for (auto& tableName: uniqueTables) {
                        auto stripped = TStringBuf(tableName);
                        stripped.SkipPrefix(fullPrefix);
                        stripped.ChopSuffix(fullSuffix);
                        strings.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(TString(stripped)));
                    }

                    auto inputNode = pgmBuilder.AsList(strings);
                    auto data = pgmBuilder.Filter(inputNode, [&](TRuntimeNode item) {
                        TMkqlBuildContext ctx(*MkqlCompiler_, pgmBuilder, *exprCtx, filter->UniqueId(), {{&filter->Head().Head(), item}});
                        return pgmBuilder.Coalesce(MkqlBuildExpr(filter->Tail(), ctx), pgmBuilder.NewDataLiteral(false));
                    });

                    data = builder.TransformAndOptimizeProgram(data, TFileTransformProvider(Services_, options.UserDataBlocks()));
                    TExploringNodeVisitor explorer;
                    auto nodeFactory = GetYtFileFullFactory(Services_);
                    auto compGraph = builder.BuildGraph(nodeFactory, options.UdfValidateMode(),
                        NUdf::EValidatePolicy::Exception, options.OptLLVM(), EGraphPerProcess::Multi, explorer, data);
                    compGraph->Prepare();
                    const TBindTerminator bind(compGraph->GetTerminator());
                    const auto& value = compGraph->GetValue();
                    const auto it = value.GetListIterator();
                    for (NUdf::TUnboxedValue current; it.Next(current);) {
                        TString tableName = TString(current.AsStringRef());
                        tableName.prepend(fullPrefix);
                        tableName.append(fullSuffix);
                        res.Tables.push_back(TCanonizedPath{std::move(tableName), Nothing(), {}, Nothing()});
                    }
                }
                else {
                    std::transform(
                        uniqueTables.begin(), uniqueTables.end(),
                        std::back_inserter(res.Tables),
                        [] (const TString& path) {
                            return TCanonizedPath{path, Nothing(), {}, Nothing()};
                        });
                }
            }

            return MakeFuture(res);
        } catch (const yexception& e) {
            return MakeFuture(NCommon::ResultFromException<TTableRangeResult>(e, pos));
        }
    }

    TFuture<TFolderResult> GetFolder(TFolderOptions&& options) final {
        auto pos = options.Pos();
        try {
            TSet<TString> uniqueTables;
            const auto fullPrefix = options.Prefix().Empty() ? "" : (options.Prefix() + '/');
            for (const auto& [tableName, _] : Services_->GetTablesMapping()) {
                TVector<TString> parts;
                Split(tableName, ".", parts);
                if (parts.size() != 3) {
                    continue;
                }
                if (parts[0] != YtProviderName || parts[1] != options.Cluster()) {
                    continue;
                }
                if (!parts[2].StartsWith(fullPrefix)) {
                    continue;
                }
                uniqueTables.insert(parts[2]);
            }

            TVector<TFolderResult::TFolderItem> items;
            TFolderResult res;
            res.SetSuccess();

            for (auto& table : uniqueTables) {
                TFolderResult::TFolderItem item;
                item.Path = table;
                item.Type = "table";
                auto allAttrs = LoadTableAttrs(Services_->GetTablePath(options.Cluster(), table, false, true));
                auto attrs = NYT::TNode::CreateMap();
                for (const auto& attrName : options.Attributes()) {
                    if (attrName && allAttrs.HasKey(attrName)) {
                        attrs[attrName] = allAttrs[attrName];
                    }
                }

                item.Attributes = NYT::NodeToYsonString(attrs);
                items.push_back(std::move(item));
            }
            res.ItemsOrFileLink = std::move(items);
            return MakeFuture(res);
        } catch (const yexception& e) {
            return MakeFuture(NCommon::ResultFromException<TFolderResult>(e, pos));
        }
    }

    TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) final {
        TBatchFolderResult res;
        res.SetSuccess();
        for (auto&& [item, reqAttrs] : options.Items()) {
            if (item.Type != "link") {
                res.Items.push_back(item);
            }
            else {
                if (item.Attributes.HasKey("broken") || item.Attributes["broken"].AsBool()) {
                    continue;
                }
                const TStringBuf targetPath = item.Attributes["target_path"].AsString();
                const auto folder = targetPath.RBefore('/');
                const auto folderContent = GetFolder(TFolderOptions(options.SessionId())
                    .Attributes(reqAttrs)
                    .Cluster(options.Cluster())
                    .Prefix(TString(folder))
                    .Config(options.Config())
                    .Pos(options.Pos())).GetValue();

                if (std::holds_alternative<TFileLinkPtr>(folderContent.ItemsOrFileLink)) {
                    Y_ENSURE(false, "File link result from file gateway GetFolder() is unexpected");
                }
                for (const auto& item: std::get<TVector<TFolderResult::TFolderItem>>(folderContent.ItemsOrFileLink)) {
                    if (item.Path == targetPath) {
                        res.Items.push_back({item.Path, item.Type, NYT::NodeFromYsonString(item.Attributes)});
                        break;
                    }
                }
            }
        }
        return MakeFuture(res);
    }

    TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) final {
        TBatchFolderResult res;
        res.SetSuccess();
        for (const auto& folder : options.Folders()) {
            TFolderOptions folderOptions(options.SessionId());
            folderOptions.Attributes(folder.AttrKeys)
                .Cluster(options.Cluster())
                .Prefix(folder.Prefix)
                .Config(options.Config())
                .Pos(options.Pos());
            const auto folderContent = GetFolder(TFolderOptions(std::move(folderOptions))).GetValue();
            if (std::holds_alternative<TFileLinkPtr>(folderContent.ItemsOrFileLink)) {
                Y_ENSURE(false, "File link result from file gateway GetFolder() is unexpected");
            }
            for (const auto& item: std::get<TVector<TFolderResult::TFolderItem>>(folderContent.ItemsOrFileLink)) {
                res.Items.push_back({item.Path, item.Type, NYT::NodeFromYsonString(item.Attributes)});
            }
        }
        return MakeFuture(res);
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        TResOrPullResult res;
        auto nodePos = ctx.GetPosition(node->Pos());
        try {
            TSession* session = GetSession(options);
            TVector<TString> columns(NCommon::GetResOrPullColumnHints(*node));
            if (columns.empty()) {
                columns = NCommon::GetStructFields(node->Child(0)->GetTypeAnn());
            }

            TStringStream out;
            NYson::TYsonWriter writer(&out, NCommon::GetYsonFormat(options.FillSettings()), ::NYson::EYsonType::Node, false);
            writer.OnBeginMap();
            if (NCommon::HasResOrPullOption(*node, "type")) {
                writer.OnKeyedItem("Type");
                NCommon::WriteResOrPullType(writer, node->Child(0)->GetTypeAnn(), columns);
            }

            bool truncated = false;
            if (TStringBuf("Result") == node->Content()) {
                truncated = ExecuteResult(*session, writer, NNodes::TResult(node).Input(), ctx, std::move(options), columns);
            } else if (TStringBuf("Pull") == node->Content()) {
                truncated = ExecutePull(*session, writer, NNodes::TPull(node), ctx, std::move(options), columns);
            } else {
                ythrow yexception() << "Don't know how to execute " << node->Content();
            }

            if (truncated) {
                writer.OnKeyedItem("Truncated");
                writer.OnBooleanScalar(true);
            }

            writer.OnEndMap();
            res.Data = out.Str();
            res.SetSuccess();
        } catch (const yexception& e) {
            res = NCommon::ResultFromException<TResOrPullResult>(e, nodePos);
        }

        return MakeFuture(res);
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        TRunResult res;
        auto nodePos = ctx.GetPosition(node->Pos());
        if (auto reduce = TMaybeNode<TYtReduce>(node)) {
            auto maxDataSizePerJob = NYql::GetMaxJobSizeForFirstAsPrimary(reduce.Cast().Settings().Ref());
            // YT wants max_data_size_per_job > 0
            if (maxDataSizePerJob && *maxDataSizePerJob <= 1) {
                TIssue rootIssue = YqlIssue(nodePos, TIssuesIds::YT_MAX_DATAWEIGHT_PER_JOB_EXCEEDED);
                res.SetStatus(TIssuesIds::UNEXPECTED);
                res.AddIssue(rootIssue);
                return MakeFuture(res);
            }
        }

        try {
            TSession* session = GetSession(options);
            if (TYtTouch::Match(node.Get())) {
                res.OutTableStats = ExecuteTouch(options.Config(), *session, TYtTouch(node));
                res.SetSuccess();
            }
            else if (TYtOutputOpBase::Match(node.Get())) {
                res.OutTableStats = ExecuteOpWithOutput(*session, node, ctx, std::move(options));
                res.SetSuccess();
            }
            else if (TYtDropTable::Match(node.Get())) {
                ExecuteDrop(node);
                res.SetSuccess();
            }
            else {
                res.AddIssue(TIssue(nodePos, TStringBuilder() << "Unsupported function: " << node->Content()));
            }
        }
        catch (const TNodeException& e) {
            res.SetException(e, ctx.GetPosition(e.Pos()));
        }
        catch (const yexception& e) {
            res.SetException(e, nodePos);
        }
        return MakeFuture(res);
    }


    TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const final {
        TRunResult res;
        auto nodePos = ctx.GetPosition(node->Pos());

        try {
            auto session = GetSession(options);
            res.OutTableStats = ExecutePrepare(options, *session, TYtOutputOpBase(node));
            res.SetSuccess();
        }
        catch (const TNodeException& e) {
            res.SetException(e, ctx.GetPosition(e.Pos()));
        }
        catch (const yexception& e) {
            res.SetException(e, nodePos);
        }
        return MakeFuture(res);
    }

    TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) override {
        TRunResult res;
        auto nodePos = ctx.GetPosition(node->Pos());

        try {
            auto session = GetSession(options);
            const TYtOutputOpBase op(node);
            const auto cluster = op.DataSink().Cluster().StringValue();
            Y_ENSURE(1U ==  op.Output().Size(), "Single output expected.");
            const auto table = op.Output().Item(0);

            TYtOutTableInfo tableInfo(table);
            auto outTablePath = Services_->GetTablePath(cluster, tableInfo.Name, true);
            TFsQueryCacheItem queryCacheItem(*options.Config(), cluster, Services_->GetTmpDir(), options.OperationHash(), outTablePath);

            NYT::TNode outSpec = NYT::TNode::CreateList();
            tableInfo.RowSpec->FillCodecNode(outSpec.Add()[YqlRowSpecAttribute]);
            outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, std::move(outSpec));

            auto content = Services_->GetTableContent(tableInfo.Name);
            TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
                Services_->GetFunctionRegistry()->SupportsSizedAllocators());
            TMemoryUsageInfo memInfo("Stat");
            TTypeEnvironment env(alloc);
            NKikimr::NMiniKQL::TTypeBuilder typeBuilder(env);
            THolderFactory holderFactory(alloc.Ref(), memInfo, Services_->GetFunctionRegistry());

            NCommon::TCodecContext codecCtx(env, *Services_->GetFunctionRegistry(), &holderFactory);
            TMkqlIOSpecs spec;
            spec.Init(codecCtx, outSpec);

            TStringStream out;
            TMkqlWriterImpl writer(out, 0, 4_MB);
            writer.SetSpecs(spec);

            TStringStream err;
            auto type = BuildType(*tableInfo.RowSpec->GetType(), typeBuilder, err);
            TValuePacker packer(true, type);
            for (auto& c: content) {
                auto val = packer.Unpack(c, holderFactory);
                writer.AddRow(val);
            }
            writer.Finish();

            WriteOutTable(options.Config(), *session, cluster, tableInfo, out.Str());
            queryCacheItem.Store();

            auto statInfo = MakeIntrusive<TYtTableStatInfo>();
            LoadTableStatInfo(outTablePath, *statInfo);
            statInfo->Id = tableInfo.Name;

            res.OutTableStats.emplace_back(statInfo->Id, statInfo);
            res.SetSuccess();
        }
        catch (const TNodeException& e) {
            res.SetException(e, ctx.GetPosition(e.Pos()));
        }
        catch (const yexception& e) {
            res.SetException(e, nodePos);
        }
        return MakeFuture(res);
    }


    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) final {
        TCalcResult res;
        Y_UNUSED(ctx);
        // TODO: fixme
        try {
            TSession* session = GetSession(options);
            TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
                Services_->GetFunctionRegistry()->SupportsSizedAllocators());
            alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            auto secureParamsProvider = MakeSimpleSecureParamsProvider(options.SecureParams());
            TVector<TFileLinkPtr> externalFiles;
            TFileYtLambdaBuilder builder(alloc, *session,
                MakeFunctionRegistry(*Services_->GetFunctionRegistry(), options.UserDataBlocks(), Services_->GetFileStorage(), externalFiles), secureParamsProvider.get());
            auto nodeFactory = GetYtFileFullFactory(Services_);
            for (auto& node: nodes) {
                auto data = builder.BuildLambda(*MkqlCompiler_, node, ctx);
                auto transform = TFileTransformProvider(Services_, options.UserDataBlocks());
                data = builder.TransformAndOptimizeProgram(data, transform);
                TExploringNodeVisitor explorer;
                auto compGraph = builder.BuildGraph(nodeFactory, options.UdfValidateMode(),
                    NUdf::EValidatePolicy::Exception, options.OptLLVM(), EGraphPerProcess::Multi, explorer, data, {data.GetNode()});
                const TBindTerminator bind(compGraph->GetTerminator());
                compGraph->Prepare();
                auto value = compGraph->GetValue();
                res.Data.push_back(NCommon::ValueToNode(value, data.GetStaticType()));
            }
            res.SetSuccess();
        } catch (const yexception& e) {
            res = NCommon::ResultFromException<TCalcResult>(e);
        }
        return MakeFuture(res);
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& exprCtx, TPublishOptions&& options) final {
        TPublishResult res;
        try {
            TSession* session = GetSession(options);

            auto publish = TYtPublish(node);

            auto mode = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::Mode);
            bool append = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Append;
            auto cluster = TString{publish.DataSink().Cluster().Value()};

            bool isAnonymous = NYql::HasSetting(publish.Publish().Settings().Ref(), EYtSettingType::Anonymous);
            auto destFilePath = Services_->GetTablePath(cluster, publish.Publish().Name().Value(), isAnonymous, true);

            append = append && NFs::Exists(destFilePath);

            TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
                Services_->GetFunctionRegistry()->SupportsSizedAllocators());
            alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TVector<TFileLinkPtr> externalFiles;
            TFileYtLambdaBuilder builder(alloc, *session,
                MakeFunctionRegistry(*Services_->GetFunctionRegistry(), {}, Services_->GetFileStorage(), externalFiles), nullptr);

            TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), builder.GetFunctionRegistry());
            TMkqlBuildContext ctx(*MkqlCompiler_, pgmBuilder, exprCtx);

            auto dstRowSpec = options.DestinationRowSpec();
            NYT::TNode spec;
            dstRowSpec->FillCodecNode(spec[YqlRowSpecAttribute]);

            std::vector<std::pair<std::string_view, TType*>> members;
            members.reserve(dstRowSpec->GetType()->GetItems().size());
            for (auto& item : dstRowSpec->GetType()->GetItems()) {
                members.emplace_back(item->GetName(), NCommon::BuildType(publish.Ref(), *item->GetItemType(), pgmBuilder));
            }
            for (size_t i: xrange(dstRowSpec->SortedBy.size())) {
                if (!dstRowSpec->GetType()->FindItem(dstRowSpec->SortedBy[i])) {
                    members.emplace_back(dstRowSpec->SortedBy[i], NCommon::BuildType(publish.Ref(), *dstRowSpec->SortedByTypes[i], pgmBuilder));
                }
            }
            auto srcType = pgmBuilder.NewStructType(members);

            TVector<TRuntimeNode> inputs;
            if (append) {
                inputs.push_back(BuildRuntimeTableInput("YtTableInputNoCtx", srcType, cluster, publish.Publish().Name().Value(), NYT::NodeToYsonString(spec), isAnonymous, ctx));
            }

            for (auto out: publish.Input()) {
                inputs.push_back(BuildTableContentCall("YtTableInputNoCtx", srcType, cluster, out.Ref(), Nothing(), ctx, false));
            }

            auto data = pgmBuilder.Extend(inputs);
            if (inputs.size() > 1 && dstRowSpec->IsSorted()) {
                data = SortListBy(data, dstRowSpec->GetForeignSort(), ctx);
            }
            data = BuildTableOutput(data, ctx);

            auto transform = TFileTransformProvider(Services_, {});
            data = builder.TransformAndOptimizeProgram(data, transform);

            TExploringNodeVisitor explorer;
            auto nodeFactory = GetYtFileFullFactory(Services_);
            auto compGraph = builder.BuildGraph(nodeFactory, NUdf::EValidateMode::None,
                NUdf::EValidatePolicy::Exception, options.OptLLVM(), EGraphPerProcess::Multi, explorer, data, {data.GetNode()});
            const TBindTerminator bind(compGraph->GetTerminator());
            compGraph->Prepare();

            NYT::TNode outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(spec));

            TVector<TString> outTableContent = GetFileWriteResult(
                builder.GetTypeEnvironment(),
                builder.GetFunctionRegistry(),
                compGraph->GetContext(),
                compGraph->GetValue(),
                outSpec);
            YQL_ENSURE(1 == outTableContent.size());

            {
                TMemoryInput in(outTableContent.front());
                TOFStream of(destFilePath);
                TDoubleHighPrecisionYsonWriter writer(&of, ::NYson::EYsonType::ListFragment);
                NYson::TYsonParser parser(&writer, &in, ::NYson::EYsonType::ListFragment);
                parser.Parse();
            }

            {
                NYT::TNode attrs = NYT::TNode::CreateMap();
                TString srcFilePath = Services_->GetTablePath(cluster, GetOutTable(publish.Input().Item(0)).Cast<TYtOutTable>().Name().Value(), true);
                if (NFs::Exists(srcFilePath + ".attr")) {
                    TIFStream input(srcFilePath + ".attr");
                    attrs = NYT::NodeFromYsonStream(&input);
                }

                const auto nativeYtTypeCompatibility = options.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
                const bool rowSpecCompactForm = options.Config()->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
                dstRowSpec->FillAttrNode(attrs[YqlRowSpecAttribute], nativeYtTypeCompatibility, rowSpecCompactForm);
                NYT::TNode columnGroupsSpec;
                if (options.Config()->OptimizeFor.Get(cluster).GetOrElse(NYT::OF_LOOKUP_ATTR) != NYT::OF_LOOKUP_ATTR) {
                    if (auto setting = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::ColumnGroups)) {
                        columnGroupsSpec = NYT::NodeFromYsonString(setting->Tail().Content());
                    }
                }
                if (!append || !attrs.HasKey("schema") || !columnGroupsSpec.IsUndefined()) {
                    attrs["schema"] = RowSpecToYTSchema(spec[YqlRowSpecAttribute], nativeYtTypeCompatibility, columnGroupsSpec).ToNode();
                }
                TOFStream ofAttr(destFilePath + ".attr");
                ofAttr.Write(NYT::NodeToYsonString(attrs, NYson::EYsonFormat::Pretty));
            }

            if (isAnonymous) {
                session->DeleteAtFinalize(options.Config(), cluster, destFilePath);
            }

            res.SetSuccess();
        }
        catch (const TNodeException& e) {
            res.SetException(e, exprCtx.GetPosition(e.Pos()));
        }
        catch (const yexception& e) {
            res.SetException(e, exprCtx.GetPosition(node->Pos()));
        }
        return MakeFuture(res);
    }

    TFuture<TCommitResult> Commit(TCommitOptions&& options) final {
        Y_UNUSED(options);
        TCommitResult res;
        res.SetSuccess();
        return MakeFuture(res);
    }

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) final {
        TDropTrackablesResult res;
        try {
            TSession* session = GetSession(options);
            // check for overrides from command line
            if (session->KeepTempTables_) {
                res.SetSuccess();
                return MakeFuture(res);
            }

            for (const auto& i : options.Pathes()) {

                const TString& cluster = i.Cluster;
                const TString& path = i.Path;

                auto tmpPath = Services_->GetTablePath(cluster, path, true);

                session->CancelDeleteAtFinalize(cluster, tmpPath);

                NFs::Remove(tmpPath);
                NFs::Remove(tmpPath + ".attr");
            }
            res.SetSuccess();
        }
        catch (const yexception& e) {
            res.SetException(e);
        }
        return MakeFuture(res);
    }

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) final {
        bool onlyCached = false;
        return MakeFuture(DoPathStat(std::move(options), onlyCached));
    }

    TPathStatResult TryPathStat(TPathStatOptions&& options) final {
        bool onlyCached = true;
        return DoPathStat(std::move(options), onlyCached);
    }

    bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const final {
        Y_UNUSED(url);
        Y_UNUSED(cluster);
        Y_UNUSED(path);
        return false;
    }

    TString GetClusterServer(const TString& cluster) const final {
        return cluster;
    }

    NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder) const final {
        Y_UNUSED(sessionId);
        Y_UNUSED(cluster);
        Y_UNUSED(epoch);
        Y_UNUSED(tmpFolder);
        return NYT::TRichYPath().Path(table);
    }

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        Y_UNUSED(sessionId);
        Y_UNUSED(cluster);
        auto realTableName = NYql::TransformPath(tmpFolder, table, true, "");
        realTableName = NYT::AddPathPrefix(realTableName, NYT::TConfig::Get()->Prefix);
        NYT::TRichYPath res{realTableName};
        res.TransactionId(TGUID());
        return res;
    }

    TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) final {
        try {
            TString cluster = options.Cluster();
            auto outTable = options.OutTable();
            TSession* session = GetSession(options);

            NYT::TNode attrs = NYT::TNode::CreateMap();
            for (auto& a: options.OutTable().Meta->Attrs) {
                attrs[a.first] = a.second;
            }
            const auto nativeYtTypeCompatibility = options.Config()->NativeYtTypeCompatibility.Get(TString{cluster}).GetOrElse(NTCF_LEGACY);
            const bool rowSpecCompactForm = options.Config()->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
            options.OutTable().RowSpec->FillAttrNode(attrs[YqlRowSpecAttribute], nativeYtTypeCompatibility, rowSpecCompactForm);
            NYT::TNode rowSpecYson;
            options.OutTable().RowSpec->FillCodecNode(rowSpecYson);
            attrs["schema"] = RowSpecToYTSchema(rowSpecYson, nativeYtTypeCompatibility).ToNode();

            NYT::TNode outSpec = NYT::TNode::CreateList();
            outSpec.Add(NYT::TNode::CreateMap()(TString{YqlRowSpecAttribute}, rowSpecYson));
            outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, std::move(outSpec));

            TFullResultTableResult res;

            TString name = TStringBuilder() << "tmp/" << GetGuidAsString(session->RandomProvider_->GenGuid());
            TString path = Services_->GetTablePath(cluster, name, true);

            res.Server = cluster;
            res.Path = path;
            res.RefName = name;
            res.CodecSpec = NYT::NodeToYsonString(outSpec);
            res.TableAttrs = NYT::NodeToYsonString(attrs);

            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TFullResultTableResult>();
        }
    }

    TString GetDefaultClusterName() const final {
        return {};
    }

    void SetStatUploader(IStatUploader::TPtr statUploader) final {
        Y_UNUSED(statUploader);
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqYtFileMkqlCompilers(compiler);
    }

    TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) override {
        const TString tmpFolder = GetTablesTmpFolder(*options.Config());
        auto res = TGetTablePartitionsResult();
        TVector<NYT::TRichYPath> paths;
        for (const auto& pathInfo: options.Paths()) {
            const auto tablePath = TransformPath(tmpFolder, pathInfo->Table->Name, pathInfo->Table->IsTemp, options.SessionId());
            NYT::TRichYPath richYtPath{NYT::AddPathPrefix(tablePath, NYT::TConfig::Get()->Prefix)};
            pathInfo->FillRichYPath(richYtPath);  // n.b. throws exception, if there is no RowSpec (we assume it is always there)
            paths.push_back(std::move(richYtPath));
        }
        res.Partitions.Partitions.push_back({});
        res.Partitions.Partitions.back().TableRanges = std::move(paths);
        res.SetSuccess();
        return res;
    }

    void AddCluster(const TYtClusterConfig&) override {
    }

private:
    static NYT::TNode LoadTableAttrs(const TString& path) {
        NYT::TNode attrs = NYT::TNode::CreateMap();
        if (NFs::Exists(path + ".attr")) {
            attrs = NYT::NodeFromYsonString(TIFStream(path + ".attr").ReadAll());
        };
        return attrs;
    }

    static void LoadTableMetaInfo(const TTableReq& req, const TString& path, TYtTableMetaInfo& info) {
        NYT::TNode attrs = LoadTableAttrs(path);

        TransferTableAttributes(attrs, [&info] (const TString& name, const TString& val) {
            info.Attrs[name] = val;
        });

        if (attrs.HasKey(YqlDynamicAttribute)) {
            info.IsDynamic = attrs[YqlDynamicAttribute].AsBool();
        }

        if (attrs.HasKey(YqlTypeAttribute)) {
            auto type = attrs[YqlTypeAttribute];
            YQL_ENSURE(type.AsString() == YqlTypeView);
            info.SqlView = TIFStream(path).ReadAll();
            auto attrVer = type.Attributes()["syntax_version"];
            info.SqlViewSyntaxVersion = attrVer.IsUndefined() ? 1 : attrVer.AsInt64();
            info.CanWrite = false;
            return;
        }

        info.CanWrite = true;
        info.YqlCompatibleScheme = ValidateTableSchema(
            req.Table(), attrs, req.IgnoreYamrDsv(), req.IgnoreWeakSchema()
        );

        if (attrs.AsMap().contains("schema_mode") && attrs["schema_mode"].AsString() == "weak") {
            info.Attrs["schema_mode"] = attrs["schema_mode"].AsString();
        }

        NYT::TNode schemaAttrs;
        if (req.ForceInferSchema() && req.InferSchemaRows() > 0) {
            info.Attrs.erase(YqlRowSpecAttribute);
            if (!req.Intents().HasFlags(
                TYtTableIntent::Override | TYtTableIntent::Append | TYtTableIntent::Drop | TYtTableIntent::Flush)) {
                auto list = LoadTableContent(path);
                if (!list.AsList().empty()) {
                    auto inferedSchemaAttrs = GetSchemaFromAttributes(attrs, true, req.IgnoreWeakSchema());
                    inferedSchemaAttrs[INFER_SCHEMA_ATTR_NAME] = InferSchemaFromSample(list, req.Table(), req.InferSchemaRows());
                    info.InferredScheme = true;
                    schemaAttrs = std::move(inferedSchemaAttrs);
                }
            }
        } else {
            if (info.YqlCompatibleScheme) {
                schemaAttrs = GetSchemaFromAttributes(attrs, false, req.IgnoreWeakSchema());
            }
            else if (!info.Attrs.contains(YqlRowSpecAttribute)
                && req.InferSchemaRows() > 0
                && !req.Intents().HasFlags(TYtTableIntent::Override | TYtTableIntent::Append | TYtTableIntent::Drop | TYtTableIntent::Flush)) {
                auto list = LoadTableContent(path);
                if (!list.AsList().empty()) {
                    schemaAttrs[INFER_SCHEMA_ATTR_NAME] = InferSchemaFromSample(list, req.Table(), req.InferSchemaRows());
                    info.InferredScheme = true;
                }
            }
        }

        if (!schemaAttrs.IsUndefined()) {
            for (auto& item: schemaAttrs.AsMap()) {
                info.Attrs[item.first] = NYT::NodeToYsonString(item.second, NYson::EYsonFormat::Text);
            }
        }
    }

    static void LoadTableStatInfo(const TString& path, TYtTableStatInfo& info) {
        NYT::TNode inputList = LoadTableContent(path);
        info.RecordsCount = inputList.AsList().size();
        if (!info.IsEmpty()) {
            info.DataSize = TFileStat(path).Size;
            info.ChunkCount = 1;
        }
    }

    static NYT::TNode LoadTableContent(const TString& path) {
        NYT::TNode inputList = NYT::TNode::CreateList();
        if (TFileStat(path).Size) {
            TIFStream input(path);
            NYT::TNodeBuilder builder(&inputList);
            NYson::TYsonParser parser(&builder, &input, ::NYson::EYsonType::ListFragment);
            parser.Parse();
        }
        return inputList;
    }

    bool ExecuteResult(TSession& session, NYson::TYsonWriter& writer, TExprBase input, TExprContext& exprCtx,
        TResOrPullOptions&& options, const TVector<TString>& columns) const
    {
        TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
            Services_->GetFunctionRegistry()->SupportsSizedAllocators());
        alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
        auto secureParamsProvider = MakeSimpleSecureParamsProvider(options.SecureParams());
        TVector<TFileLinkPtr> externalFiles;
        TFileYtLambdaBuilder builder(alloc, session,
            MakeFunctionRegistry(*Services_->GetFunctionRegistry(), options.UserDataBlocks(), Services_->GetFileStorage(), externalFiles), secureParamsProvider.get());
        auto data = builder.BuildLambda(*MkqlCompiler_, input.Ptr(), exprCtx);
        auto transform = TFileTransformProvider(Services_, options.UserDataBlocks());
        data = builder.TransformAndOptimizeProgram(data, transform);

        TExploringNodeVisitor explorer;
        auto nodeFactory = GetYtFileFullFactory(Services_);
        auto compGraph = builder.BuildGraph(nodeFactory, options.UdfValidateMode(),
            NUdf::EValidatePolicy::Exception, options.OptLLVM(), EGraphPerProcess::Multi, explorer, data, {data.GetNode()});
        const TBindTerminator bind(compGraph->GetTerminator());
        compGraph->Prepare();

        TYsonExecuteResOrPull resultData(options.FillSettings().RowsLimitPerWrite,
            options.FillSettings().AllResultsBytesLimit, MakeMaybe(columns));

        resultData.WriteValue(compGraph->GetValue(), data.GetStaticType());
        auto dataRes = resultData.Finish();

        writer.OnKeyedItem("Data");
        writer.OnRaw(options.FillSettings().Discard ? "#" : dataRes);

        return resultData.IsTruncated();
    }

    bool ExecutePull(TSession& session, NYson::TYsonWriter& writer, TPull pull, TExprContext& exprCtx,
        TResOrPullOptions&& options, const TVector<TString>& columns) const
    {
        bool truncated = false;
        bool writeRef = NCommon::HasResOrPullOption(pull.Ref(), "ref");

        if (!writeRef) {
            truncated = ExecuteResult(session, writer, pull, exprCtx, std::move(options), columns);
            writeRef = truncated && NCommon::HasResOrPullOption(pull.Ref(), "autoref");
        }

        if (writeRef && !options.FillSettings().Discard) {
            auto cluster = GetClusterName(pull.Input());
            writer.OnKeyedItem("Ref");
            writer.OnBeginList();
            for (auto& tableInfo: GetInputTableInfos(pull.Input())) {
                writer.OnListItem();
                if (tableInfo->IsTemp) {
                    auto outPath = Services_->GetTablePath(cluster, tableInfo->Name, true);
                    session.CancelDeleteAtFinalize(TString{cluster}, outPath);
                }
                NYql::WriteTableReference(writer, YtProviderName, cluster, tableInfo->Name, tableInfo->IsTemp, columns);
            }
            writer.OnEndList();
        }

        return truncated;
    }

    TVector<std::pair<TString, TYtTableStatInfo::TPtr>> ExecuteOpWithOutput(TSession& session,
        const TExprNode::TPtr& node, TExprContext& exprCtx,
        TRunOptions&& options) const
    {
        TYtOutputOpBase op(node);

        auto cluster = TString{op.DataSink().Cluster().Value()};
        TVector<TString> outTablePaths;
        TVector<TYtOutTableInfo> outTableInfos;
        for (auto table: op.Output()) {
            TString name = TStringBuilder() << "tmp/" << GetGuidAsString(session.RandomProvider_->GenGuid());

            outTablePaths.push_back(Services_->GetTablePath(cluster, name, true));

            outTableInfos.emplace_back(table);
            outTableInfos.back().Name = name;
        }

        TFsQueryCacheItem queryCacheItem(*options.Config(), cluster, Services_->GetTmpDir(), options.OperationHash(), outTablePaths);
        if (!queryCacheItem.Lookup(FakeQueue_)) {
            TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(),
                Services_->GetFunctionRegistry()->SupportsSizedAllocators());
            alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            auto secureParamsProvider = MakeSimpleSecureParamsProvider(options.SecureParams());
            TVector<TFileLinkPtr> externalFiles;
            TFileYtLambdaBuilder builder(alloc, session,
                MakeFunctionRegistry(*Services_->GetFunctionRegistry(), options.UserDataBlocks(), Services_->GetFileStorage(), externalFiles), secureParamsProvider.get());
            auto data = builder.BuildLambda(*MkqlCompiler_, node, exprCtx);
            auto transform = TFileTransformProvider(Services_, options.UserDataBlocks());
            data = builder.TransformAndOptimizeProgram(data, transform);

            TExploringNodeVisitor explorer;
            auto nodeFactory = GetYtFileFullFactory(Services_);
            auto compGraph = builder.BuildGraph(nodeFactory, options.UdfValidateMode(),
                NUdf::EValidatePolicy::Exception, options.OptLLVM(), EGraphPerProcess::Multi, explorer, data, {data.GetNode()});
            const TBindTerminator bind(compGraph->GetTerminator());
            compGraph->Prepare();

            WriteOutTables(builder, options.Config(), session, cluster, outTableInfos, compGraph.Get());
            queryCacheItem.Store();
        }

        TVector<std::pair<TString, TYtTableStatInfo::TPtr>> outStat;
        for (size_t i: xrange(outTableInfos.size())) {
            TYtTableStatInfo::TPtr statInfo = MakeIntrusive<TYtTableStatInfo>();
            statInfo->Id = outTableInfos[i].Name;
            LoadTableStatInfo(outTablePaths.at(i), *statInfo);

            outStat.emplace_back(statInfo->Id, statInfo);
        }
        return outStat;
    }

    TVector<std::pair<TString, TYtTableStatInfo::TPtr>> ExecuteTouch(const TYtSettings::TConstPtr& config, TSession& session, const TYtTouch& op) const {
        auto cluster = op.DataSink().Cluster().StringValue();
        TVector<std::pair<TString, TYtTableStatInfo::TPtr>> outStat;
        for (auto table: op.Output()) {
            TString name = TStringBuilder() << "tmp/" << GetGuidAsString(session.RandomProvider_->GenGuid());

            TYtOutTableInfo tableInfo(table);
            tableInfo.Name = name;
            WriteOutTable(config, session, cluster, tableInfo, {});

            TYtTableStatInfo::TPtr statInfo = MakeIntrusive<TYtTableStatInfo>();
            statInfo->Id = name;
            LoadTableStatInfo(Services_->GetTablePath(cluster, name, true), *statInfo);

            outStat.emplace_back(statInfo->Id, statInfo);
        }

        return outStat;
    }

    TVector<std::pair<TString, TYtTableStatInfo::TPtr>> ExecutePrepare(const TPrepareOptions& options, const TSession& session, const TYtOutputOpBase& op) const {
        const auto cluster = op.DataSink().Cluster().StringValue();
        YQL_ENSURE(op.Output().Size() == 1U);

        const TString name = TStringBuilder() << "tmp/" << GetGuidAsString(session.RandomProvider_->GenGuid());
        const auto path = Services_->GetTablePath(cluster, name, true);

        TFsQueryCacheItem queryCacheItem(*options.Config(), cluster, Services_->GetTmpDir(), options.OperationHash(), path);
        if (queryCacheItem.Lookup(FakeQueue_)) {
            TYtTableStatInfo::TPtr statInfo = MakeIntrusive<TYtTableStatInfo>();
            statInfo->Id = name;
            LoadTableStatInfo(path, *statInfo);

            return {{statInfo->Id, std::move(statInfo)}};
        }

        TYtOutTableInfo tableInfo(op.Output().Item(0));
        tableInfo.Name = name;
        WriteOutTable(options.Config(), const_cast<TSession&>(session), cluster, tableInfo, {});
        return {{name, nullptr}};
    }

    void ExecuteDrop(const TExprNode::TPtr& node) const {
        TYtDropTable op(node);
        auto table = op.Table();
        bool isAnonymous = NYql::HasSetting(table.Settings().Ref(), EYtSettingType::Anonymous);
        auto path = Services_->GetTablePath(op.DataSink().Cluster().Value(), table.Name().Value(), isAnonymous, true);

        NFs::Remove(path);
        NFs::Remove(path + ".attr");
    }

    void WriteOutTables(TLambdaBuilder& builder, const TYtSettings::TConstPtr& config, TSession& session, const TString& cluster,
        const TVector<TYtOutTableInfo>& outTableInfos, IComputationGraph* compGraph) const
    {
        NYT::TNode outSpec = NYT::TNode::CreateList();
        for (auto table: outTableInfos) {
            table.RowSpec->FillCodecNode(outSpec.Add()[YqlRowSpecAttribute]);
        }
        outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, std::move(outSpec));

        TVector<TString> outTableContent = GetFileWriteResult(
            builder.GetTypeEnvironment(),
            builder.GetFunctionRegistry(),
            compGraph->GetContext(),
            compGraph->GetValue(),
            outSpec);
        YQL_ENSURE(outTableInfos.size() == outTableContent.size());

        for (size_t i: xrange(outTableInfos.size())) {
            WriteOutTable(config, session, cluster, outTableInfos[i], outTableContent[i]);
        }
    }

    void WriteOutTable(const TYtSettings::TConstPtr& config, TSession& session, const TString& cluster,
        const TYtOutTableInfo& outTableInfo, TStringBuf binaryYson) const
    {
        auto outPath = Services_->GetTablePath(cluster, outTableInfo.Name, true);
        session.DeleteAtFinalize(config, cluster, outPath);
        if (binaryYson) {
            TMemoryInput in(binaryYson);
            TOFStream of(outPath);
            TDoubleHighPrecisionYsonWriter writer(&of, ::NYson::EYsonType::ListFragment);
            NYson::TYsonParser parser(&writer, &in, ::NYson::EYsonType::ListFragment);
            parser.Parse();
        }
        else {
            YQL_ENSURE(TFile(outPath, CreateAlways | WrOnly).IsOpen(), "Failed to create " << outPath.Quote() << " file");
        }

        {
            NYT::TNode attrs = NYT::TNode::CreateMap();
            for (auto& a: outTableInfo.Meta->Attrs) {
                attrs[a.first] = a.second;
            }
            const auto nativeYtTypeCompatibility = config->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
            const bool rowSpecCompactForm = config->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
            const bool optimizeForScan = config->OptimizeFor.Get(cluster).GetOrElse(NYT::EOptimizeForAttr::OF_LOOKUP_ATTR) != NYT::EOptimizeForAttr::OF_LOOKUP_ATTR;
            outTableInfo.RowSpec->FillAttrNode(attrs[YqlRowSpecAttribute], nativeYtTypeCompatibility, rowSpecCompactForm);
            NYT::TNode rowSpecYson;
            outTableInfo.RowSpec->FillCodecNode(rowSpecYson);

            attrs["schema"] = RowSpecToYTSchema(rowSpecYson, nativeYtTypeCompatibility, optimizeForScan ? outTableInfo.GetColumnGroups() : NYT::TNode{}).ToNode();
            TOFStream ofAttr(outPath + ".attr");
            NYson::TYsonWriter writer(&ofAttr, NYson::EYsonFormat::Pretty, ::NYson::EYsonType::Node);
            NYT::TNodeVisitor visitor(&writer);
            visitor.Visit(attrs);
        }
    }

    TSet<TString>& GetColumnarStatHistory(NYT::TRichYPath ytPath) {
        YQL_ENSURE(ytPath.Columns_.Defined());
        ytPath.Columns_.Clear();
        return ColumnarStatHistory[NYT::NodeToCanonicalYsonString(NYT::PathToNode(ytPath), NYson::EYsonFormat::Text)];
    }

    TPathStatResult DoPathStat(TPathStatOptions&& options, bool onlyCached) {
        TPathStatResult res;
        res.DataSize.reserve(options.Paths().size());

        auto extractSysColumns = [] (NYT::TRichYPath& ytPath) -> TVector<TString> {
            TVector<TString> res;
            if (ytPath.Columns_) {
                auto it = std::remove_if(
                    ytPath.Columns_->Parts_.begin(),
                    ytPath.Columns_->Parts_.end(),
                    [] (const TString& col) { return col.StartsWith(YqlSysColumnPrefix); }
                );
                res.assign(it, ytPath.Columns_->Parts_.end());
                ytPath.Columns_->Parts_.erase(it, ytPath.Columns_->Parts_.end());
            }
            return res;
        };

        for (auto& req: options.Paths()) {
            auto path = Services_->GetTablePath(options.Cluster(), req.Path().Path_, req.IsTemp());

            const NYT::TNode attrs = LoadTableAttrs(path);
            bool inferSchema = attrs.HasKey("infer_schema") && attrs["infer_schema"].AsBool();

            res.DataSize.push_back(0);
            auto ytPath = req.Path();
            if (auto sysColumns = extractSysColumns(ytPath)) {
                NYT::TNode inputList = LoadTableContent(path);
                auto records = inputList.AsList().size();
                records = GetUsedRows(ytPath, records).GetOrElse(records);
                for (auto col: sysColumns) {
                    auto size = 0;
                    if (col == YqlSysColumnNum || col == YqlSysColumnRecord) {
                        size = sizeof(ui64);
                    } else if (col == YqlSysColumnIndex) {
                        size = sizeof(ui32);
                    } else if (col == YqlSysColumnPath && !req.IsTemp()) {
                        size = req.Path().Path_.size();
                    }
                    res.DataSize.back() += size * records;
                }
            }

            if (ytPath.Columns_ && !inferSchema) {
                TSet<TString>& columnarStatsHistory = GetColumnarStatHistory(ytPath);

                if (onlyCached) {
                    bool allColumnsPresent = AllOf(ytPath.Columns_->Parts_, [&](const auto& c) {
                        return columnarStatsHistory.contains(c);
                    });

                    if (!allColumnsPresent) {
                        return res;
                    }
                }

                TIFStream in(path);
                TStringStream out;
                TSet<TStringBuf> columns(ytPath.Columns_->Parts_.begin(), ytPath.Columns_->Parts_.end());
                THashMap<TStringBuf, TStringBuf> renames;
                if (ytPath.RenameColumns_) {
                    renames.insert(ytPath.RenameColumns_->begin(), ytPath.RenameColumns_->end());
                }

                TBinaryYsonWriter writer(&out, ::NYson::EYsonType::ListFragment);
                TColumnFilteringConsumer filter(&writer, columns, renames);
                NYson::TYsonParser parser(&filter, &in, ::NYson::EYsonType::ListFragment);
                parser.Parse();

                for (auto& c : columns) {
                    columnarStatsHistory.insert(TString(c));
                }
                res.DataSize.back() += out.Str().size();
            } else {
                res.DataSize.back() += TFileStat(path).Size;
            }
        }
        res.SetSuccess();
        return res;
    }


private:
    TYtFileServices::TPtr Services_;
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> MkqlCompiler_;
    THashMap<TString, TSession> Sessions;
    bool* EmulateOutputForMultirunPtr;
    THashMap<TString, TSet<TString>> ColumnarStatHistory;
    TAsyncQueue::TPtr FakeQueue_;
};

} // NFile

IYtGateway::TPtr CreateYtFileGateway(const NFile::TYtFileServices::TPtr& services, bool* emulateOutputForMultirunPtr) {
    return new NFile::TYtFileGateway(services, emulateOutputForMultirunPtr);
}

} // NYql
