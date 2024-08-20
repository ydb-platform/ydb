#include "yql_yt_transform.h"

#include <ydb/library/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

namespace NNative {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYT;
using namespace NNodes;

TGatewayTransformer::TGatewayTransformer(const TExecContextBase& execCtx, TYtSettings::TConstPtr settings, const TString& optLLVM,
    TUdfModulesTable udfModules, IUdfResolver::TPtr udfResolver, TTransactionCache::TEntry::TPtr entry,
    TProgramBuilder& builder, TTempFiles& tmpFiles, TMaybe<ui32> publicId)
    : ExecCtx_(execCtx)
    , Settings_(std::move(settings))
    , UdfModules_(std::move(udfModules))
    , UdfResolver_(std::move(udfResolver))
    , Entry_(std::move(entry))
    , PgmBuilder_(builder)
    , TmpFiles_(tmpFiles)
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

    for (const auto& f: ExecCtx_.UserFiles_->GetFiles()) {
        if (f.second.IsPgExt || f.second.IsPgCatalog) {
            AddFile(f.second.IsPgCatalog ? TString(NCommon::PgCatalogFileName) : "", f.second);
        }
    }
}

TCallableVisitFunc TGatewayTransformer::operator()(TInternName name) {
    if (name == TYtTableContent::CallableName()) {

        *TableContentFlag_ = true;
        *RemoteExecutionFlag_ = *RemoteExecutionFlag_ || !Settings_->TableContentLocalExecution.Get().GetOrElse(DEFAULT_TABLE_CONTENT_LOCAL_EXEC);

        if (EPhase::Content == Phase_ || EPhase::All == Phase_) {
            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

                const TString cluster(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                const TString& server = ExecCtx_.Clusters_->GetServer(cluster);
                const TString tmpFolder = GetTablesTmpFolder(*Settings_);
                TTransactionCache::TEntry::TPtr entry = ExecCtx_.Session_->TxCache_.GetEntry(server);
                auto tx = entry->Tx;

                auto deliveryMode = ForceLocalTableContent_ ? ETableContentDeliveryMode::File : Settings_->TableContentDeliveryMode.Get(cluster).GetOrElse(ETableContentDeliveryMode::Native);
                bool useSkiff = Settings_->TableContentUseSkiff.Get(cluster).GetOrElse(DEFAULT_USE_SKIFF);
                const bool ensureOldTypesOnly = !useSkiff;
                const ui64 maxChunksForNativeDelivery = Settings_->TableContentMaxChunksForNativeDelivery.Get().GetOrElse(1000ul);
                TString contentTmpFolder = ForceLocalTableContent_ ? TString() : Settings_->TableContentTmpFolder.Get(cluster).GetOrElse(TString());
                if (contentTmpFolder.StartsWith("//")) {
                    contentTmpFolder = contentTmpFolder.substr(2);
                }
                if (contentTmpFolder.EndsWith('/')) {
                    contentTmpFolder.remove(contentTmpFolder.length() - 1);
                }

                TString uniqueId = GetGuidAsString(ExecCtx_.Session_->RandomProvider_->GenGuid());

                TListLiteral* groupList = AS_VALUE(TListLiteral, callable.GetInput(1));
                YQL_ENSURE(groupList->GetItemsCount() == 1);
                TListLiteral* tableList = AS_VALUE(TListLiteral, groupList->GetItems()[0]);

                NYT::TNode specNode = NYT::TNode::CreateMap();
                NYT::TNode& tablesNode = specNode[YqlIOSpecTables];
                NYT::TNode& registryNode = specNode[YqlIOSpecRegistry];
                THashMap<TString, TString> uniqSpecs;
                TVector<NYT::TRichYPath> richPaths;
                TVector<NYT::TNode> formats;

                THashMap<TString, ui32> structColumns;
                if (useSkiff) {
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

                for (ui32 i = 0; i < tableList->GetItemsCount(); ++i) {
                    TTupleLiteral* tuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[i]);
                    YQL_ENSURE(tuple->GetValuesCount() == 7, "Expect 7 elements in the Tuple item");

                    TString refName = TStringBuilder() << "$table" << uniqSpecs.size();
                    TString specStr = TString(AS_VALUE(TDataLiteral, tuple->GetValue(2))->AsValue().AsStringRef());
                    const auto specNode = NYT::NodeFromYsonString(specStr);

                    NYT::TRichYPath richYPath;
                    NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tuple->GetValue(0))->AsValue().AsStringRef())));
                    const bool isTemporary = AS_VALUE(TDataLiteral, tuple->GetValue(1))->AsValue().Get<bool>();
                    const bool isAnonymous = AS_VALUE(TDataLiteral, tuple->GetValue(5))->AsValue().Get<bool>();
                    const ui32 epoch = AS_VALUE(TDataLiteral, tuple->GetValue(6))->AsValue().Get<ui32>();

                    auto tablePath = TransformPath(tmpFolder, richYPath.Path_, isTemporary, ExecCtx_.Session_->UserName_);

                    auto res = uniqSpecs.emplace(specStr, refName);
                    if (res.second) {
                        registryNode[refName] = specNode;
                    }
                    else {
                        refName = res.first->second;
                    }
                    tablesNode.Add(refName);
                    if (useSkiff) {
                        formats.push_back(SingleTableSpecToInputSkiff(specNode, structColumns, false, false, false));
                    } else {
                        if (ensureOldTypesOnly && specNode.HasKey(YqlRowSpecAttribute)) {
                            EnsureSpecDoesntUseNativeYtTypes(specNode, tablePath, true);
                        }
                        NYT::TNode formatNode("yson");
                        formatNode.Attributes()["format"] = "binary";
                        formats.push_back(formatNode);
                    }

                    if (isTemporary && !isAnonymous) {
                        richYPath.Path_ = NYT::AddPathPrefix(tablePath, NYT::TConfig::Get()->Prefix);
                    } else {
                        auto p = entry->Snapshots.FindPtr(std::make_pair(tablePath, epoch));
                        YQL_ENSURE(p, "Table " << tablePath << " has no snapshot");
                        richYPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p));
                    }
                    richPaths.push_back(richYPath);

                    const ui64 chunkCount = AS_VALUE(TDataLiteral, tuple->GetValue(3))->AsValue().Get<ui64>();
                    if (chunkCount > maxChunksForNativeDelivery) {
                        deliveryMode = ETableContentDeliveryMode::File;
                        YQL_CLOG(DEBUG, ProviderYt) << "Switching to file delivery mode, because table "
                            << tablePath.Quote() << " has too many chunks: " << chunkCount;
                    }
                }

                for (size_t i = 0; i < richPaths.size(); ++i) {
                    NYT::TRichYPath richYPath = richPaths[i];

                    TString richYPathDesc = NYT::NodeToYsonString(NYT::PathToNode(richYPath));
                    TString fileName = TStringBuilder() << uniqueId << '_' << i;

                    if (ETableContentDeliveryMode::Native == deliveryMode) {
                        richYPath.Format(formats[i]);
                        richYPath.FileName(fileName);
                        RemoteFiles_->push_back(richYPath);

                        YQL_CLOG(DEBUG, ProviderYt) << "Passing table " << richYPathDesc << " as remote file "
                            << fileName.Quote();
                    }
                    else {
                        NYT::TTableReaderOptions readerOptions;
                        readerOptions.CreateTransaction(false);

                        auto readerTx = tx;
                        if (richYPath.TransactionId_) {
                            readerTx = entry->GetSnapshotTx(*richYPath.TransactionId_);
                            richYPath.TransactionId_.Clear();
                        }

                        TTupleLiteral* samplingTuple = AS_VALUE(TTupleLiteral, callable.GetInput(2));
                        if (samplingTuple->GetValuesCount() != 0) {
                            YQL_ENSURE(samplingTuple->GetValuesCount() == 3);
                            double samplingPercent = AS_VALUE(TDataLiteral, samplingTuple->GetValue(0))->AsValue().Get<double>();
                            ui64 samplingSeed = AS_VALUE(TDataLiteral, samplingTuple->GetValue(1))->AsValue().Get<ui64>();
                            bool isSystemSampling = AS_VALUE(TDataLiteral, samplingTuple->GetValue(2))->AsValue().Get<bool>();
                            if (!isSystemSampling) {
                                NYT::TNode spec = NYT::TNode::CreateMap();
                                spec["sampling_rate"] = samplingPercent / 100.;
                                if (samplingSeed) {
                                    spec["sampling_seed"] = static_cast<i64>(samplingSeed);
                                }
                                readerOptions.Config(spec);
                            }
                        }

                        TRawTableReaderPtr reader;
                        const int lastAttempt = NYT::TConfig::Get()->ReadRetryCount - 1;
                        for (int attempt = 0; attempt <= lastAttempt; ++attempt) {
                            try {
                                reader = readerTx->CreateRawReader(richYPath, NYT::TFormat(formats[i]), readerOptions);
                                break;
                            } catch (const NYT::TErrorResponse& e) {
                                YQL_CLOG(ERROR, ProviderYt) << "Error creating reader for " << richYPathDesc << ": " << e.what();
                                // Already retried inside CreateRawReader
                                throw;
                            } catch (const yexception& e) {
                                YQL_CLOG(ERROR, ProviderYt) << "Error creating reader for " << richYPathDesc << ": " << e.what();
                                if (attempt == lastAttempt) {
                                    throw;
                                }
                                NYT::NDetail::TWaitProxy::Get()->Sleep(NYT::TConfig::Get()->RetryInterval);
                            }
                        }

                        if (contentTmpFolder) {
                            entry->GetRoot()->Create(contentTmpFolder, NT_MAP,
                                TCreateOptions().Recursive(true).IgnoreExisting(true));

                            auto remotePath = TString(contentTmpFolder).append('/').append(fileName);

                            while (true) {
                                try {
                                    auto out = tx->CreateFileWriter(TRichYPath(remotePath));
                                    TBrotliCompress compressor(out.Get(), Settings_->TableContentCompressLevel.Get(cluster).GetOrElse(8));
                                    TransferData(reader.Get(), &compressor);
                                    compressor.Finish();
                                    out->Finish();
                                } catch (const yexception& e) {
                                    YQL_CLOG(ERROR, ProviderYt) << "Error transferring " << richYPathDesc << " to " << remotePath << ": " << e.what();
                                    if (reader->Retry(Nothing(), Nothing(), std::make_exception_ptr(e))) {
                                        continue;
                                    }
                                    throw;
                                }
                                break;
                            }
                            entry->DeleteAtFinalize(remotePath);
                            YQL_CLOG(DEBUG, ProviderYt) << "Passing table " << richYPathDesc << " as remote file " << remotePath.Quote();

                            RemoteFiles_->push_back(NYT::TRichYPath(NYT::AddPathPrefix(remotePath, NYT::TConfig::Get()->Prefix)).FileName(fileName));

                        } else {
                            TString outPath = TmpFiles_.AddFile(fileName);

                            if (PublicId_) {
                                auto progress = TOperationProgress(TString(YtProviderName), *PublicId_,
                                    TOperationProgress::EState::InProgress, "Preparing table content");
                                ExecCtx_.Session_->ProgressWriter_(progress);
                            }
                            while (true) {
                                try {
                                    TOFStream out(outPath);
                                    out.SetFinishPropagateMode(false);
                                    out.SetFlushPropagateMode(false);
                                    TBrotliCompress compressor(&out, Settings_->TableContentCompressLevel.Get(cluster).GetOrElse(8));
                                    TransferData(reader.Get(), &compressor);
                                    compressor.Finish();
                                    out.Finish();
                                } catch (const TIoException& e) {
                                    YQL_CLOG(ERROR, ProviderYt) << "Error reading " << richYPathDesc << ": " << e.what();
                                    // Don't retry IO errors
                                    throw;
                                } catch (const yexception& e) {
                                    YQL_CLOG(ERROR, ProviderYt) << "Error reading " << richYPathDesc << ": " << e.what();
                                    if (reader->Retry(Nothing(), Nothing(), std::make_exception_ptr(e))) {
                                        continue;
                                    }
                                    throw;
                                }
                                break;
                            }
                            YQL_CLOG(DEBUG, ProviderYt) << "Passing table " << richYPathDesc << " as file "
                                << fileName.Quote() << " (size=" << TFileStat(outPath).Size << ')';

                            LocalFiles_->emplace_back(outPath, TLocalFileInfo{TString(), false});
                        }
                    }
                }

                TCallableBuilder call(env,
                    TStringBuilder() << TYtTableContent::CallableName() << TStringBuf("Job"),
                    callable.GetType()->GetReturnType());

                call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(uniqueId));
                call.Add(PgmBuilder_.NewDataLiteral(tableList->GetItemsCount()));
                call.Add(PgmBuilder_.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(specNode)));
                call.Add(PgmBuilder_.NewDataLiteral(useSkiff));
                call.Add(PgmBuilder_.NewDataLiteral(ETableContentDeliveryMode::File == deliveryMode)); // use compression
                call.Add(callable.GetInput(3)); // length
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
                    !ExecCtx_.FunctionRegistry_->IsLoadedUdfModule(moduleName) ||
                    moduleName == TStringBuf("Geo");

                if (moduleName.StartsWith("SystemPython")) {
                    *RemoteExecutionFlag_ = true;
                }

                const auto udfPath = FindUdfPath(moduleName);
                if (!udfPath.StartsWith(NMiniKQL::StaticModulePrefix)) {
                    const auto fileInfo = ExecCtx_.UserFiles_->GetFile(udfPath);
                    YQL_ENSURE(fileInfo, "Unknown udf path " << udfPath);
                    AddFile(udfPath, *fileInfo, FindUdfPrefix(moduleName));
                }

                if (moduleName == TStringBuf("Geo")) {
                    if (const auto fileInfo = ExecCtx_.UserFiles_->GetFile("/home/geodata6.bin")) {
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
                    if (!ExecCtx_.UserFiles_->FindFolder(fullFileName, files)) {
                        ythrow yexception() << "Folder not found: " << fullFileName;
                    }
                    for (const auto& x : files) {
                        auto fileInfo = ExecCtx_.UserFiles_->GetFile(x);
                        YQL_ENSURE(fileInfo, "File not found: " << x);
                        AddFile(x, *fileInfo);
                    }
                } else {
                    auto fileInfo = ExecCtx_.UserFiles_->GetFile(fullFileName);
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

bool TGatewayTransformer::HasSecondPhase() {
    YQL_ENSURE(EPhase::Other == Phase_);
    if (!*TableContentFlag_) {
        return false;
    }
    ForceLocalTableContent_ = CanExecuteInternally();
    Phase_ = EPhase::Content;
    return true;
}

void TGatewayTransformer::ApplyJobProps(TYqlJobBase& job) {
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

void TGatewayTransformer::ApplyUserJobSpec(NYT::TUserJobSpec& spec, bool localRun) {
    if (!RemoteFiles_->empty()) {
        YQL_ENSURE(!localRun, "Unexpected remote files");
        for (auto& file: *RemoteFiles_) {
            spec.AddFile(file);
        }
    }
    bool fakeChecksum = (GetEnv("YQL_LOCAL") == "1");  // YQL-15353
    for (auto& file: *LocalFiles_) {
        TAddLocalFileOptions opts;
        if (!fakeChecksum && file.second.Hash) {
            opts.MD5CheckSum(file.second.Hash);
        }
        opts.BypassArtifactCache(file.second.BypassArtifactCache);
        spec.AddLocalFile(file.first, opts);
    }
    const TString binTmpFolder = Settings_->BinaryTmpFolder.Get().GetOrElse(TString());
    if (localRun || !binTmpFolder) {
        for (auto& file: *DeferredUdfFiles_) {
            TAddLocalFileOptions opts;
            if (!fakeChecksum && file.second.Hash) {
                opts.MD5CheckSum(file.second.Hash);
            }
            YQL_ENSURE(TFileStat(file.first).Size != 0);
            opts.BypassArtifactCache(file.second.BypassArtifactCache);
            spec.AddLocalFile(file.first, opts);
        }
    } else {
        const TDuration binExpiration = Settings_->BinaryExpirationInterval.Get().GetOrElse(TDuration());
        auto entry = GetEntry();
        for (auto& file: *DeferredUdfFiles_) {
            YQL_ENSURE(TFileStat(file.first).Size != 0);
            auto snapshot = entry->GetBinarySnapshot(binTmpFolder, file.second.Hash, file.first, binExpiration);
            spec.AddFile(TRichYPath(snapshot.first).TransactionId(snapshot.second).FileName(TFsPath(file.first).GetName()).Executable(true).BypassArtifactCache(file.second.BypassArtifactCache));
        }
    }
    RemoteFiles_->clear();
    LocalFiles_->clear();
    DeferredUdfFiles_->clear();
}

NYT::ITransactionPtr TGatewayTransformer::GetTx() {
    return GetEntry()->Tx;
}

TTransactionCache::TEntry::TPtr TGatewayTransformer::GetEntry() {
    if (!Entry_) {
        Entry_ = ExecCtx_.GetOrCreateEntry(Settings_);
    }
    return Entry_;
}

void TGatewayTransformer::AddFile(TString alias,
            const TUserFiles::TFileInfo& fileInfo, const TString& udfPrefix) {
    if (alias.StartsWith('/')) {
        alias = alias.substr(1);
    }
    if (alias.StartsWith(TStringBuf("home/"))) {
        alias = alias.substr(TStringBuf("home/").length());
    }

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

    } else {
        *RemoteExecutionFlag_ = true;
        TString filePath = NYT::AddPathPrefix(fileInfo.RemotePath, NYT::TConfig::Get()->Prefix);
        TRichYPath remoteFile(filePath);
        if (alias) {
            remoteFile.FileName(alias);
            filePath = alias;
        } else {
            alias = TFsPath(filePath).GetName();
        }
        auto insertRes = UniqFiles_->insert({alias, remoteFile.Path_});
        if (insertRes.second) {
            RemoteFiles_->push_back(remoteFile.Executable(true).BypassArtifactCache(fileInfo.BypassArtifactCache));
            if (fileInfo.RemoteMemoryFactor > 0.) {
                *UsedMem_ += fileInfo.RemoteMemoryFactor * GetUncompressedFileSize(GetTx(), remoteFile.Path_).GetOrElse(ui64(1) << 10);
            }
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

TString TGatewayTransformer::FindUdfPath(const TStringBuf moduleName) const {
    if (auto udfInfo = UdfModules_.FindPtr(moduleName)) {
        return TUserDataStorage::MakeFullName(udfInfo->FileAlias);
    }

    TMaybe<TFilePathWithMd5> udfPathWithMd5 = UdfResolver_->GetSystemModulePath(moduleName);
    YQL_ENSURE(udfPathWithMd5.Defined());
    return TFsPath(udfPathWithMd5->Path).GetName();
}

TString TGatewayTransformer::FindUdfPrefix(const TStringBuf moduleName) const {
    if (auto udfInfo = UdfModules_.FindPtr(moduleName)) {
        return udfInfo->Prefix;
    }
    return TString();
}

} // NNative

} // NYql
