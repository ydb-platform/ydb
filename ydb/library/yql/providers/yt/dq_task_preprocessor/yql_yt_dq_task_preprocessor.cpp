#include "yql_yt_dq_task_preprocessor.h"

#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/interface/io.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/hash.h>
#include <util/generic/guid.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>
#include <util/system/env.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/stream/mem.h>

#include <string_view>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

class TYtFullResultWriter: public IDqFullResultWriter {
public:
    TYtFullResultWriter(IFunctionRegistry::TPtr funcRegistry, NYT::ITransactionPtr tx, const TString& path, const TString& spec)
        : FuncRegistry_(std::move(funcRegistry))
        , Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("DqFullResultWriter")
        , HolderFactory(Alloc.Ref(), MemInfo, FuncRegistry_.Get())
        , CodecContext(TypeEnv, *FuncRegistry_, &HolderFactory)
        , Specs()
    {
        Specs.SetUseSkiff("");
        Specs.Init(CodecContext, spec);
        OutStream = tx->CreateRawWriter(NYT::TRichYPath{path}, Specs.MakeOutputFormat(), NYT::TTableWriterOptions());
        TableWriter = MakeHolder<TMkqlWriterImpl>(OutStream, 4_MB);
        TableWriter->SetSpecs(Specs);
        Alloc.Release();
    }

    ~TYtFullResultWriter() {
        try {
            Abort();
        } catch (...) {
        }
        Alloc.Acquire();
    };

    void AddRow(const NUdf::TUnboxedValuePod& row) override {
        YQL_ENSURE(!Finished);
        try {
            TableWriter->AddRow(row);
            ++RowCount;
        } catch (const NYT::TErrorResponse& e) {
            TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
            throw yexception() << errMsg;
        }
    }

    void Finish() override {
        if (!Finished) {
            try {
                TableWriter->Finish();
            } catch (const NYT::TErrorResponse& e) {
                TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
                throw yexception() << errMsg;
            }
            Finished = true;
        }
    }

    void Abort() override {
        if (!Finished) {
            try {
                TableWriter->Abort();
                OutStream->Abort();
            } catch (const NYT::TErrorResponse& e) {
                TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
                throw yexception() << errMsg;
            }
            Finished = true;
        }
    }

    ui64 GetRowCount() const override {
        return RowCount;
    }

private:
    const IFunctionRegistry::TPtr FuncRegistry_;
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    NCommon::TCodecContext CodecContext;
    TMkqlIOSpecs Specs;
    NYT::TRawTableWriterPtr OutStream;
    THolder<TMkqlWriterImpl> TableWriter;
    ui64 RowCount{0};
    bool Finished = false;
};

class TFileFullResultWriter: public IDqFullResultWriter {
public:
    TFileFullResultWriter(IFunctionRegistry::TPtr funcRegistry, const TString& path, const TString& spec, const TString& attrs)
        : Path(path)
        , Attrs(attrs)
        , FuncRegistry_(std::move(funcRegistry))
        , Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("DqFullResultWriter")
        , HolderFactory(Alloc.Ref(), MemInfo, FuncRegistry_.Get())
        , CodecContext(TypeEnv, *FuncRegistry_, &HolderFactory)
        , Specs()
    {
        Specs.Init(CodecContext, spec);
        TableWriter = MakeHolder<TMkqlWriterImpl>(OutStream, 1, 4_MB);
        TableWriter->SetSpecs(Specs);
        Alloc.Release();
    }

    ~TFileFullResultWriter() {
        Alloc.Acquire();
    };

    void AddRow(const NUdf::TUnboxedValuePod& row) override {
        YQL_ENSURE(!Finished);
        TableWriter->AddRow(row);
        ++RowCount;
    }

    void Finish() override {
        if (!Finished) {
            TableWriter->Finish();
            if (auto binaryYson = OutStream.Str()) {
                TMemoryInput in(binaryYson);
                TOFStream of(Path);
                TDoubleHighPrecisionYsonWriter writer(&of, ::NYson::EYsonType::ListFragment);
                NYson::TYsonParser parser(&writer, &in, ::NYson::EYsonType::ListFragment);
                parser.Parse();
            }
            else {
                YQL_ENSURE(TFile(Path, CreateAlways | WrOnly).IsOpen(), "Failed to create " << Path.Quote() << " file");
            }

            {
                NYT::TNode attrs = NYT::NodeFromYsonString(Attrs);
                TOFStream ofAttr(Path + ".attr");
                NYson::TYsonWriter writer(&ofAttr, NYson::EYsonFormat::Pretty, ::NYson::EYsonType::Node);
                NYT::TNodeVisitor visitor(&writer);
                visitor.Visit(attrs);
            }

            Finished = true;
        }
    }

    void Abort() override {
        Finished = true;
    }

    ui64 GetRowCount() const override {
        return RowCount;
    }

private:
    const TString Path;
    const TString Attrs;
    const IFunctionRegistry::TPtr FuncRegistry_;
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    NCommon::TCodecContext CodecContext;
    TMkqlIOSpecs Specs;
    TStringStream OutStream;
    THolder<TMkqlWriterImpl> TableWriter;
    ui64 RowCount{0};
    bool Finished = false;
};

} // unnamed

class TYtDqTaskPreprocessor : public IDqTaskPreprocessor {
public:
    explicit TYtDqTaskPreprocessor(bool ytEmulationMode, IFunctionRegistry::TPtr funcRegistry)
        : YtEmulationMode_(ytEmulationMode)
        , FuncRegistry_(std::move(funcRegistry))
    {
    }

    TYtDqTaskPreprocessor(TYtDqTaskPreprocessor&&) = default;
    TYtDqTaskPreprocessor& operator=(TYtDqTaskPreprocessor&&) = default;

    THashMap<TString, TString> GetTaskParams(const THashMap<TString, TString>& graphParams, const THashMap<TString, TString>& secureParams) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("YtDqTaskPreprocessor"), __FUNCTION__);
        THashMap<TString, TString> result;

        GraphParams_ = graphParams;
        SecureParams_ = secureParams;
        if (YtEmulationMode_) {
            YQL_LOG(DEBUG) << "No nested transactions are created in YT emulation mode, skipping";
            result["yt.write.tx"] = GetGuidAsString(TGUID());
        } else if (auto p = graphParams.FindPtr("yt.write")) {
            try {
                auto params = NYT::NodeFromYsonString(*p);
                auto rootTxId = params["root_tx"].AsString();
                auto server = params["server"].AsString();
                auto token = params["token"].AsString();
                auto& client = Clients_[std::make_pair(server, token)];
                NYT::TCreateClientOptions opts;
                if (token) {
                    opts.Token(secureParams.Value(token, ""));
                }
                client = NYT::CreateClient(server, std::move(opts));
                auto rootTx = client->AttachTransaction(GetGuid(rootTxId));
                auto subTx = rootTx->StartTransaction();
                auto subTxId = GetGuidAsString(subTx->GetId());
                TFailureInjector::Reach("expire_tx", [&] {
                    subTx->Abort();
                    subTx = rootTx->StartTransaction(NYT::TStartTransactionOptions().Timeout(TDuration::Seconds(1)).AutoPingable(false));
                    subTxId = GetGuidAsString(subTx->GetId());
                    ::Sleep(TDuration::Seconds(1));
                });

                YQL_LOG(DEBUG) << "Cluster " << server << ", creating nested " << subTxId << " for " << rootTxId;
                result["yt.write.tx"] = subTxId;
                WriteSubTx_ = std::move(subTx);
            }
            catch (const NYT::TErrorResponse& e) {
                TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
                throw yexception() << errMsg;
            }
        }
        return result;
    }

    void Finish(bool success) override {
        GraphParams_.clear();
        SecureParams_.clear();
        try {
            if (WriteSubTx_) {
                if (success) {
                    YQL_LOG(DEBUG) << "Committing " << GetGuidAsString(WriteSubTx_->GetId());
                    TFailureInjector::Reach("fail_commit", [] { throw yexception() << "fail_commit"; });
                    WriteSubTx_->Commit();
                } else {
                    YQL_LOG(DEBUG) << "Aborting " << GetGuidAsString(WriteSubTx_->GetId());
                    WriteSubTx_->Abort();
                }
                WriteSubTx_.Reset();
            }
            if (FullResultSubTx_) {
                if (success) {
                    YQL_LOG(DEBUG) << "Committing " << GetGuidAsString(FullResultSubTx_->GetId());
                    TFailureInjector::Reach("full_result_fail_commit", [] { throw yexception() << "full_result_fail_commit"; });
                    FullResultSubTx_->Commit();
                } else {
                    YQL_LOG(DEBUG) << "Aborting " << GetGuidAsString(FullResultSubTx_->GetId());
                    FullResultSubTx_->Abort();
                }
                FullResultSubTx_.Reset();
            }
            Clients_.clear();
        } catch (const NYT::TErrorResponse& e) {
            TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
            throw yexception() << errMsg;
        }
    }

    THolder<IDqFullResultWriter> CreateFullResultWriter() override {
        if (auto p = GraphParams_.FindPtr("yt.full_result_table")) {
            try {
                auto params = NYT::NodeFromYsonString(*p);
                if (YtEmulationMode_) {
                    return MakeHolder<TFileFullResultWriter>(FuncRegistry_, params["path"].AsString(), params["codecSpec"].AsString(), params["tableAttrs"].AsString());
                } else {
                    auto server = params["server"].AsString();
                    auto token = params["token"].AsString();
                    auto& client = Clients_[std::make_pair(server, token)];
                    if (!client) {
                        NYT::TCreateClientOptions opts;
                        if (token) {
                            opts.Token(SecureParams_.Value(token, ""));
                        }
                        client = NYT::CreateClient(server, std::move(opts));
                    }

                    NYT::ITransactionPtr subTx;
                    NYT::IClientBasePtr parentTx = client;
                    if (params.HasKey("root_tx")) {
                        auto rootTx = client->AttachTransaction(GetGuid(params["root_tx"].AsString()));
                        subTx = rootTx->StartTransaction();
                        if (params.HasKey("external_tx")) {
                            parentTx = client->AttachTransaction(GetGuid(params["external_tx"].AsString()));
                        }
                    } else if (params.HasKey("external_tx")) {
                        parentTx = client->AttachTransaction(GetGuid(params["external_tx"].AsString()));
                        subTx = parentTx->StartTransaction();
                    } else {
                        subTx = client->StartTransaction();
                    }

                    auto path = params["path"].AsString();
                    CreateParents({path}, parentTx);

                    subTx->Create(path, NYT::NT_TABLE,
                        NYT::TCreateOptions().Force(true).Attributes(NYT::NodeFromYsonString(params["tableAttrs"].AsString()))
                    );

                    TFailureInjector::Reach("full_result_fail_create", [&] { throw yexception() << "full_result_fail_create"; });

                    FullResultSubTx_ = subTx;

                    return MakeHolder<TYtFullResultWriter>(FuncRegistry_, subTx, path, params["codecSpec"].AsString());
                }
            }
            catch (const NYT::TErrorResponse& e) {
                TString errMsg = GetEnv("YQL_DETERMINISTIC_MODE") ? e.GetError().ShortDescription() : TString(e.what());
                throw yexception() << errMsg;
            }
        }

        return {};
    }

private:
    THashMap<TString, TString> GraphParams_;
    THashMap<TString, TString> SecureParams_;
    THashMap<std::pair<TString, TString>, NYT::IClientPtr> Clients_;
    NYT::ITransactionPtr WriteSubTx_;
    NYT::ITransactionPtr FullResultSubTx_;

    const bool YtEmulationMode_;
    const IFunctionRegistry::TPtr FuncRegistry_;
};

TDqTaskPreprocessorFactory CreateYtDqTaskPreprocessorFactory(bool ytEmulationMode, IFunctionRegistry::TPtr funcRegistry) {
    return [=]() {
        return new TYtDqTaskPreprocessor(ytEmulationMode, funcRegistry);
    };
}

} // namespace NYql::NDq
