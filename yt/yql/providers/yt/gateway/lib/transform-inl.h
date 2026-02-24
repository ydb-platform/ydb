#pragma once

#include "transform.h"

#include <yt/yql/providers/yt/lib/dump_helpers/yql_yt_dump_helpers.h>
#include <yt/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yt/yql/providers/yt/codec/yt_codec.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/streams/brotli/brotli.h>

#include <util/system/env.h>
#include <util/folder/path.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;

template<typename TExecContextPtr>
TGatewayTransformer<TExecContextPtr>::TGatewayTransformer(
    TExecContextPtr execCtx,
    TYtSettings::TConstPtr settings,
    const TString& optLLVM,
    TUdfModulesTable udfModules,
    IUdfResolver::TPtr udfResolver,
    ITableDownloaderFunc downloader,
    TProgramBuilder& builder,
    TMaybe<ui32> publicId
)
    : ExecCtx_(execCtx)
    , Settings_(std::move(settings))
    , UdfModules_(std::move(udfModules))
    , UdfResolver_(std::move(udfResolver))
    , Downloader_(downloader)
    , PgmBuilder_(builder)
    , PublicId_(publicId)
    , ForceLocalTableContent_(false)
    , TableContentFlag_(std::make_shared<bool>(false))
    , RemoteExecutionFlag_(std::make_shared<bool>(false))
    , UntrustedUdfFlag_(std::make_shared<bool>(false))
    , UsedMem_(std::make_shared<ui64>(ui64(0)))
    , JobFileAliases_(std::make_shared<THashMap<TString, TString>>())
    , JobUdfs_(std::make_shared<THashMap<TString, TString>>())
    , UniqFiles_(std::make_shared<THashMap<TString, TString>>())
    , RemoteFiles_(std::make_shared<TVector<NYT::TRichYPath>>())
    , LocalFiles_(std::make_shared<TVector<std::pair<TString, TLocalFileInfo>>>())
    , DeferredUdfFiles_(std::make_shared<TVector<std::pair<TString, TLocalFileInfo>>>())
{
    if (optLLVM != "OFF") {
        *UsedMem_ = 128_MB;
    }

    for (const auto& f: ExecCtx_->UserFiles_->GetFiles()) {
        if (f.second.IsPgExt || f.second.IsPgCatalog) {
            AddFile(f.second.IsPgCatalog ? TString(NCommon::PgCatalogFileName) : "", f.second);
        }
    }
}

template<typename TExecContextPtr>
TCallableVisitFunc TGatewayTransformer<TExecContextPtr>::operator()(TInternName internName) {
    auto name = internName.Str();
    const bool small = name.SkipPrefix("Small");
    if (name == TYtTableContent::CallableName() || name == TYtBlockTableContent::CallableName()) {
        bool useBlocks = (name == TYtBlockTableContent::CallableName());

        *TableContentFlag_ = true;
        *RemoteExecutionFlag_ = *RemoteExecutionFlag_ || !small;

        if (EPhase::Content == Phase_ || EPhase::All == Phase_) {
            return [&, name, useBlocks](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

                TTableDownloaderOptions downloaderOptions{
                    .ForceLocalTableContent = ForceLocalTableContent_,
                    .PublicId = PublicId_
                };

                const TString cluster = ExecCtx_->Cluster_;
                const TString tmpFolder = GetTablesTmpFolder(*Settings_, cluster);


                TListLiteral* groupList = AS_VALUE(TListLiteral, callable.GetInput(0));
                YQL_ENSURE(groupList->GetItemsCount() == 1);
                TListLiteral* tableList = AS_VALUE(TListLiteral, groupList->GetItems()[0]);

                TVector<TRemoteYtTable> tables;
                for (ui32 i = 0; i < tableList->GetItemsCount(); ++i) {
                    TTupleLiteral* tuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[i]);
                    YQL_ENSURE(tuple->GetValuesCount() == 8, "Expect 8 elements in the Tuple item");

                    NYT::TRichYPath richYPath;
                    NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tuple->GetValue(0))->AsValue().AsStringRef())));
                    YQL_ENSURE(richYPath.Cluster_ && !richYPath.Cluster_->empty());

                    YQL_ENSURE(*richYPath.Cluster_ == cluster,
                        "Reading from remote cluster is not supported in " << name << ", runtime cluster = " << cluster.Quote() <<
                        ", input[" << i << "] cluster = " << richYPath.Cluster_->Quote());
                    richYPath.Cluster_.Clear();
                    tables.emplace_back(TRemoteYtTable{.RichPath = richYPath});
                }

                auto deliveryMode = ForceLocalTableContent_ ? ETableContentDeliveryMode::File : Settings_->TableContentDeliveryMode.Get(cluster).GetOrElse(ETableContentDeliveryMode::Native);
                bool useSkiff = !useBlocks && Settings_->TableContentUseSkiff.Get(cluster).GetOrElse(DEFAULT_USE_SKIFF);
                const bool ensureOldTypesOnly = !useSkiff;
                const ui64 maxChunksForNativeDelivery = Settings_->TableContentMaxChunksForNativeDelivery.Get().GetOrElse(1000ul);
                const ui64 localDataSizeLimit = Settings_->_LocalTableContentLimit.Get().GetOrElse(DEFAULT_LOCAL_TABLE_CONTENT_LIMIT);
                TString contentTmpFolder = ForceLocalTableContent_ ? TString() : Settings_->TableContentTmpFolder.Get(cluster).GetOrElse(TString());
                if (contentTmpFolder.StartsWith("//")) {
                    contentTmpFolder = contentTmpFolder.substr(2);
                }
                if (contentTmpFolder.EndsWith('/')) {
                    contentTmpFolder.remove(contentTmpFolder.length() - 1);
                }

                THashMap<TString, ui32> structColumns;
                if (useSkiff) {
                    YQL_ENSURE(!useBlocks);
                    auto itemType = AS_TYPE(TListType, callable.GetType()->GetReturnType())->GetItemType();
                    TStructType* itemTypeStruct = AS_TYPE(TStructType, itemType);
                    if (itemTypeStruct->GetMembersCount() == 0) {
                        useSkiff = false; // TODO: YT-12235
                    } else {
                        for (ui32 index = 0; index < itemTypeStruct->GetMembersCount(); ++index) {
                            structColumns.emplace(itemTypeStruct->GetMemberName(index), index);
                        }
                    }
                }
                downloaderOptions.StructColumns = structColumns;

                TString uniqueId = GetGuidAsString(ExecCtx_->BaseSession_->RandomProvider_->GenGuid());

                NYT::TNode specNode = NYT::TNode::CreateMap();
                NYT::TNode& tablesNode = specNode[YqlIOSpecTables];
                NYT::TNode& registryNode = specNode[YqlIOSpecRegistry];
                ui64 totalDataSize = 0;
                THashMap<TString, TString> uniqSpecs;
                for (ui32 i = 0; i < tableList->GetItemsCount(); ++i) {
                    TTupleLiteral* tuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[i]);
                    YQL_ENSURE(tuple->GetValuesCount() == 8, "Expect 8 elements in the Tuple item");

                    TString refName = TStringBuilder() << "$table" << uniqSpecs.size();
                    TString specStr = TString(AS_VALUE(TDataLiteral, tuple->GetValue(2))->AsValue().AsStringRef());
                    const auto specNode = NYT::NodeFromYsonString(specStr);

                    NYT::TRichYPath& richYPath = tables[i].RichPath;

                    const bool isTemporary = AS_VALUE(TDataLiteral, tuple->GetValue(1))->AsValue().Get<bool>();
                    const bool isAnonymous = AS_VALUE(TDataLiteral, tuple->GetValue(5))->AsValue().Get<bool>();
                    const ui32 epoch = AS_VALUE(TDataLiteral, tuple->GetValue(6))->AsValue().Get<ui32>();

                    tables[i].TableOptions = TYtTableOptions{.IsTemporary = isTemporary, .IsAnonymous = isAnonymous, .Epoch = epoch};

                    auto tablePath = TransformPath(tmpFolder, richYPath.Path_, isTemporary, ExecCtx_->BaseSession_->UserName_);

                    auto res = uniqSpecs.emplace(specStr, refName);
                    if (res.second) {
                        registryNode[refName] = specNode;
                    }
                    else {
                        refName = res.first->second;
                    }
                    tablesNode.Add(refName);
                    if (useBlocks) {
                        NYT::TNode formatNode("arrow");
                        tables[i].Format = formatNode;
                    } else if (useSkiff) {
                        tables[i].Format = SingleTableSpecToInputSkiff(specNode, structColumns, false, false, false);
                    } else {
                        if (ensureOldTypesOnly && specNode.HasKey(YqlRowSpecAttribute)) {
                            EnsureSpecDoesntUseNativeYtTypes(specNode, tablePath, true);
                        }
                        NYT::TNode formatNode("yson");
                        formatNode.Attributes()["format"] = "binary";
                        tables[i].Format = formatNode;
                    }

                    const ui64 chunkCount = AS_VALUE(TDataLiteral, tuple->GetValue(3))->AsValue().Get<ui64>();
                    if (chunkCount > maxChunksForNativeDelivery) {
                        deliveryMode = ETableContentDeliveryMode::File;
                        YQL_CLOG(DEBUG, ProviderYt) << "Switching to file delivery mode, because table "
                            << tablePath.Quote() << " has too many chunks: " << chunkCount;
                    }

                    totalDataSize += AS_VALUE(TDataLiteral, tuple->GetValue(7))->AsValue().Get<ui64>();
                }

                downloaderOptions.Tables = tables;

                const bool localTableContent = ETableContentDeliveryMode::File == deliveryMode && !contentTmpFolder;
                if (localTableContent && totalDataSize > localDataSizeLimit) {
                    YQL_LOG_CTX_THROW TErrorException(TIssuesIds::DEFAULT_ERROR)
                        << "Tables are too big for local table content";
                }

                TMaybe<TSamplingConfig> samplingConfig = Nothing();
                TTupleLiteral* samplingTuple = AS_VALUE(TTupleLiteral, callable.GetInput(1));
                if (samplingTuple->GetValuesCount() != 0) {
                    YQL_ENSURE(samplingTuple->GetValuesCount() == 3);
                    double samplingPercent = AS_VALUE(TDataLiteral, samplingTuple->GetValue(0))->AsValue().Get<double>();
                    ui64 samplingSeed = AS_VALUE(TDataLiteral, samplingTuple->GetValue(1))->AsValue().Get<ui64>();
                    bool isSystemSampling = AS_VALUE(TDataLiteral, samplingTuple->GetValue(2))->AsValue().Get<bool>();
                    samplingConfig = TSamplingConfig{
                        .SamplingPercent = samplingPercent,
                        .SamplingSeed = samplingSeed, .
                        IsSystemSampling = isSystemSampling
                    };
                }
                downloaderOptions.SamplingConfig = samplingConfig;
                downloaderOptions.UniqueId = uniqueId;
                downloaderOptions.DeliveryMode = deliveryMode;

                auto downloaderResult = Downloader_(downloaderOptions).GetValueSync();

                for (auto& richPath: downloaderResult.RemoteFiles) {
                    RemoteFiles_->emplace_back(richPath);
                }

                for (auto& dstFileName: downloaderResult.LocalFiles) {
                    LocalFiles_->emplace_back(dstFileName, TLocalFileInfo{TString(), false});
                }

                TCallableBuilder call(env,
                    TStringBuilder() << name << TStringBuf("Job"),
                    callable.GetType()->GetReturnType());
                if (useBlocks) {
                    call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(uniqueId));
                    call.Add(PgmBuilder_.NewDataLiteral(tableList->GetItemsCount()));
                    call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(specNode)));
                    call.Add(PgmBuilder_.NewDataLiteral(ETableContentDeliveryMode::File == deliveryMode)); // use compression
                    call.Add(callable.GetInput(2)); // length
                } else {
                    call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(uniqueId));
                    call.Add(PgmBuilder_.NewDataLiteral(tableList->GetItemsCount()));
                    call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(specNode)));
                    call.Add(PgmBuilder_.NewDataLiteral(useSkiff));
                    call.Add(PgmBuilder_.NewDataLiteral(ETableContentDeliveryMode::File == deliveryMode)); // use compression
                    call.Add(callable.GetInput(2)); // length
                }

                return TRuntimeNode(call.Build(), false);
            };
        }
    }

    if (EPhase::Other == Phase_ || EPhase::All == Phase_) {
        if (name == TStringBuf("Udf") || name == TStringBuf("ScriptUdf")) {
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& /*env*/) {
                YQL_ENSURE(callable.GetInputsCount() > 0, "Expected at least one argument");
                const TString udfName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                const auto moduleName = ModuleName(udfName);

                *UntrustedUdfFlag_ = *UntrustedUdfFlag_ ||
                    callable.GetType()->GetName() == TStringBuf("ScriptUdf") ||
                    !ExecCtx_->FunctionRegistry_->IsLoadedUdfModule(moduleName) ||
                    moduleName == TStringBuf("Geo");

                if (moduleName.StartsWith("SystemPython")) {
                    *RemoteExecutionFlag_ = true;
                }

                const auto udfPath = FindUdfPath(moduleName);
                if (!udfPath.StartsWith(NMiniKQL::StaticModulePrefix)) {
                    const auto fileInfo = ExecCtx_->UserFiles_->GetFile(udfPath);
                    YQL_ENSURE(fileInfo, "Unknown udf path " << udfPath);
                    AddFile(udfPath, *fileInfo, FindUdfPrefix(moduleName));
                }

                if (moduleName == TStringBuf("Geo")) {
                    if (const auto fileInfo = ExecCtx_->UserFiles_->GetFile("/home/geodata6.bin")) {
                        AddFile("./geodata6.bin", *fileInfo);
                    }
                }

                return TRuntimeNode(&callable, false);
            };
        }

        if (name == TStringBuf("FilePath") || name == TStringBuf("FileContent") || name == TStringBuf("FolderPath")) {
            if (name == TStringBuf("FolderPath")) {
                *RemoteExecutionFlag_ = true;
            }
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
                const TString fileName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                const TString fullFileName = TUserDataStorage::MakeFullName(fileName);

                auto callableName = callable.GetType()->GetName();
                if (callableName == TStringBuf("FolderPath")) {
                    TVector<TString> files;
                    if (!ExecCtx_->UserFiles_->FindFolder(fullFileName, files)) {
                        ythrow yexception() << "Folder not found: " << fullFileName;
                    }
                    for (const auto& x : files) {
                        auto fileInfo = ExecCtx_->UserFiles_->GetFile(x);
                        YQL_ENSURE(fileInfo, "File not found: " << x);
                        AddFile(x, *fileInfo);
                    }
                } else {
                    auto fileInfo = ExecCtx_->UserFiles_->GetFile(fullFileName);
                    YQL_ENSURE(fileInfo, "File not found: " << fullFileName);
                    AddFile(fileName, *fileInfo);
                }

                TStringBuilder jobCallable;
                if (callableName == TStringBuf("FolderPath")) {
                    jobCallable << "FilePath";
                } else {
                    jobCallable << callableName;
                }

                TCallableBuilder builder(env, jobCallable << TStringBuf("Job"),
                    callable.GetType()->GetReturnType(), false);
                builder.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(fullFileName));
                return TRuntimeNode(builder.Build(), false);
            };
        }

        if (name == TYtTablePath::CallableName()
            || name == TYtTableIndex::CallableName()
            || name == TYtTableRecord::CallableName()
            || name == TYtIsKeySwitch::CallableName()
            || name == TYtRowNumber::CallableName())
        {
            *RemoteExecutionFlag_ = true;
        }
    }

    return TCallableVisitFunc();
}

template<typename TExecContextPtr>
bool TGatewayTransformer<TExecContextPtr>::HasSecondPhase() {
    YQL_ENSURE(EPhase::Other == Phase_);
    if (!*TableContentFlag_) {
        return false;
    }
    ForceLocalTableContent_ = CanExecuteInternally();
    Phase_ = EPhase::Content;
    return true;
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::ApplyJobProps(TYqlJobBase& job) {
    for (auto& x: *JobFileAliases_) {
        job.AddFileAlias(x.first, x.second);
    }
    JobFileAliases_->clear();

    for (auto& x: *JobUdfs_) {
        job.AddUdfModule(x.first, x.second);
    }
    JobUdfs_->clear();
    UniqFiles_->clear();
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::AddFile(TString alias,
            TUserFiles::TFileInfo fileInfo, const TString& udfPrefix) {
    if (alias.StartsWith('/')) {
        alias = alias.substr(1);
    }
    if (alias.StartsWith(TStringBuf("home/"))) {
        alias = alias.substr(TStringBuf("home/").length());
    }

    HandleQContextCapture(fileInfo, alias);

    TString basename;
    if (fileInfo.Path) {
        // Pass only unique files to YT
        auto insertRes = UniqFiles_->insert({fileInfo.Path->GetMd5(), fileInfo.Path->GetPath()});
        TString filePath;
        if (insertRes.second) {
            filePath = fileInfo.Path->GetPath();
            *UsedMem_ += fileInfo.InMemorySize;
            if (fileInfo.IsUdf) {
                DeferredUdfFiles_->emplace_back(filePath, TLocalFileInfo{fileInfo.Path->GetMd5(), fileInfo.BypassArtifactCache});
            } else {
                LocalFiles_->emplace_back(filePath, TLocalFileInfo{fileInfo.Path->GetMd5(), fileInfo.BypassArtifactCache});
            }
        } else {
            filePath = insertRes.first->second;
        }

        basename = TFsPath(filePath).GetName();
        if (alias && alias != basename) {
            JobFileAliases_->insert({alias, basename});
        }
        TString fileAlias = (alias && alias != basename) ? alias : basename;
        ProcessLocalFileForDump(fileAlias, filePath);

    } else {
        *RemoteExecutionFlag_ = true;
        TString filePath = NYT::AddPathPrefix(fileInfo.RemotePath, NYT::TConfig::Get()->Prefix);
        NYT::TRichYPath remoteFile(filePath);
        if (alias) {
            remoteFile.FileName(alias);
            filePath = alias;
        } else {
            alias = TFsPath(filePath).GetName();
        }
        auto insertRes = UniqFiles_->insert({alias, remoteFile.Path_});
        if (insertRes.second) {
            ProcessRemoteFileForDump(alias, remoteFile);
            RemoteFiles_->push_back(remoteFile.Executable(true).BypassArtifactCache(fileInfo.BypassArtifactCache));
            CalculateRemoteMemoryUsage(fileInfo.RemoteMemoryFactor, remoteFile);
        } else {
            YQL_ENSURE(remoteFile.Path_ == insertRes.first->second, "Duplicate file alias " << alias.Quote()
                << " for different files " << remoteFile.Path_.Quote() << " and " << insertRes.first->second.Quote());
        }
        basename = TFsPath(filePath).GetName();
    }

    if (fileInfo.IsUdf) {
        if (alias) {
            JobUdfs_->insert({"./" + alias, udfPrefix});
        } else {
            JobUdfs_->insert({"./" + basename, udfPrefix});
        }
    }
}

template<typename TExecContextPtr>
TTransformerFiles TGatewayTransformer<TExecContextPtr>::GetTransformerFiles() {
    TTransformerFiles transformerFiles{.RemoteFiles = *RemoteFiles_, .LocalFiles = *LocalFiles_, .DeferredUdfFiles = *DeferredUdfFiles_, .JobUdfs = *JobUdfs_};
    return transformerFiles;
}

template<typename TExecContextPtr>
TString TGatewayTransformer<TExecContextPtr>::FindUdfPath(const TStringBuf moduleName) const {
    if (auto udfInfo = UdfModules_.FindPtr(moduleName)) {
        return TUserDataStorage::MakeFullName(udfInfo->FileAlias);
    }

    TMaybe<TFilePathWithMd5> udfPathWithMd5 = UdfResolver_->GetSystemModulePath(moduleName);
    YQL_ENSURE(udfPathWithMd5.Defined());
    return TFsPath(udfPathWithMd5->Path).GetName();
}

template<typename TExecContextPtr>
TString TGatewayTransformer<TExecContextPtr>::FindUdfPrefix(const TStringBuf moduleName) const {
    if (auto udfInfo = UdfModules_.FindPtr(moduleName)) {
        return udfInfo->Prefix;
    }
    return TString();
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::CalculateRemoteMemoryUsage(double, const NYT::TRichYPath&) {
    return;
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::HandleQContextCapture(TUserFiles::TFileInfo&, const TString&) {
    return;
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::ProcessLocalFileForDump(const TString&, const TString&) {
    return;
}

template<typename TExecContextPtr>
void TGatewayTransformer<TExecContextPtr>::ProcessRemoteFileForDump(const TString&, const NYT::TRichYPath&) {
    return;
}

} // NYql
