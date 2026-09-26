#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_udf.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_udf_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_udf.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <util/generic/yexception.h>

#include <atomic>
#include <fstream>
#include <sstream>

namespace NYdb::inline Dev::NUdf {
namespace {

Ydb::Udf::ModuleType ToProto(EModuleType type) {
    return static_cast<Ydb::Udf::ModuleType>(type);
}

Ydb::Udf::ModuleKind ToProto(EModuleKind kind) {
    return static_cast<Ydb::Udf::ModuleKind>(kind);
}

Ydb::Udf::WriteMode ToProto(EWriteMode mode) {
    return static_cast<Ydb::Udf::WriteMode>(mode);
}

//! Spelled out rather than cast: an older client talking to a newer server gets
//! values it has no name for, and a cast would put them into an enum that
//! nothing downstream — least of all a switch over it — expects.
EModuleKind FromProto(Ydb::Udf::ModuleKind kind) {
    switch (kind) {
        case Ydb::Udf::WASM:
            return EModuleKind::Wasm;
        case Ydb::Udf::NATIVE:
            return EModuleKind::Native;
        default:
            return EModuleKind::Unspecified;
    }
}

EModuleType FromProto(Ydb::Udf::ModuleType type) {
    switch (type) {
        case Ydb::Udf::MODULE:
            return EModuleType::Module;
        case Ydb::Udf::LIBRARY:
            return EModuleType::Library;
        default:
            return EModuleType::Unspecified;
    }
}

ECompileStatus FromProto(Ydb::Udf::CompileStatus status) {
    switch (status) {
        case Ydb::Udf::PENDING:
            return ECompileStatus::Pending;
        case Ydb::Udf::COMPILING:
            return ECompileStatus::Compiling;
        case Ydb::Udf::READY:
            return ECompileStatus::Ready;
        case Ydb::Udf::FAILED:
            return ECompileStatus::Failed;
        default:
            return ECompileStatus::Unspecified;
    }
}

TModuleInfo FromProto(const Ydb::Udf::ModuleInfo& proto) {
    TModuleInfo info;
    info.Name = proto.name();
    info.Type = FromProto(proto.module_type());
    info.Kind = FromProto(proto.module_kind());
    info.Uid = proto.uid();
    info.Md5 = proto.md5();
    info.Size = proto.size();
    info.Version = proto.version();
    info.CreatedAt = ProtoTimestampToInstant(proto.created_at());
    return info;
}

TPlatformCompileStatus FromProto(const Ydb::Udf::PlatformCompileStatus& proto) {
    TPlatformCompileStatus status;
    status.CpuSpec = proto.cpu_spec();
    status.Status = FromProto(proto.status());
    status.CompileError = proto.compile_error();
    return status;
}

void FillUploadParams(Ydb::Udf::UploadModuleParams& params, const TUploadModuleSettings& settings) {
    if (!settings.ManifestJson_.empty()) {
        params.set_manifest_json(TStringType{settings.ManifestJson_});
    }
    params.set_write_mode(ToProto(settings.WriteMode_));
    if (!settings.ExpectedUid_.empty()) {
        params.set_expected_uid(TStringType{settings.ExpectedUid_});
    }
    if (settings.Version_) {
        params.set_version(*settings.Version_);
    }
    if (!settings.ExpectedMd5_.empty()) {
        params.set_expected_md5(TStringType{settings.ExpectedMd5_});
    }
}

//! One-shot BIDI UploadModule: connect, post the response read, write metadata
//! and data, half-close, and answer from the single response. Owns its input
//! stream until all callbacks complete; only one data chunk is in flight.
class TUploadModuleSession: public std::enable_shared_from_this<TUploadModuleSession> {
public:
    using TService = Ydb::Udf::V1::UdfService;
    using TRequest = Ydb::Udf::UploadModuleChunk;
    using TResponse = Ydb::Udf::UploadModuleResponse;
    using IProcessor = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>;

    TUploadModuleSession(
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbState,
        std::shared_ptr<std::istream> input,
        uint64_t size,
        TUploadModuleSettings settings)
        : Connections_(std::move(connections))
        , DbDriverState_(std::move(dbState))
        , Input_(std::move(input))
        , Size_(size)
        , Settings_(std::move(settings))
        , Promise_(NThreading::NewPromise<TUploadModuleResult>())
    {
    }

    TAsyncUploadModuleResult Start() {
        Connections_->StartBidirectionalStream<TService, TRequest, TResponse>(
            [self = shared_from_this()](TPlainStatus status, IProcessor::TPtr processor) {
                self->OnConnect(std::move(status), std::move(processor));
            },
            &TService::Stub::AsyncUploadModule,
            DbDriverState_,
            TRpcRequestSettings::Make(Settings_));
        return Promise_.GetFuture();
    }

private:
    //! `Processor_` is set once, before any callback can run, and never cleared:
    //! the write chain and the response read are in flight at the same time, and
    //! whichever of them finishes the call must not pull the processor out from
    //! under the other. Cancelling it is enough — a write on a cancelled stream
    //! comes straight back with CANCELLED.
    void Fail(TPlainStatus status) {
        if (Done_.exchange(true)) {
            return;
        }
        if (Processor_) {
            Processor_->Cancel();
        }
        Promise_.SetValue(TUploadModuleResult(TStatus(std::move(status)), Ydb::Udf::UploadModuleResult()));
    }

    void OnConnect(TPlainStatus status, IProcessor::TPtr processor) {
        if (!status.Ok() || !processor) {
            if (status.Ok()) {
                status = TPlainStatus::Internal("Upload stream returned no processor");
            }
            Fail(std::move(status));
            return;
        }
        Processor_ = std::move(processor);
        // The read is posted before the first write. The server answers the
        // moment it can tell the upload will be refused — a bad kind, a name
        // that clashes with the manifest — without waiting for the body, and a
        // response that arrives with no read outstanding is lost: all that would
        // be left to report is a write that failed on a stream the server had
        // already finished.
        ReadResponse();
        WriteMetadata();
    }

    void WriteMetadata() {
        TRequest chunk;
        auto* metadata = chunk.mutable_metadata();
        auto& params = *metadata->mutable_params();
        params = MakeOperationRequest<Ydb::Udf::UploadModuleParams>(Settings_);
        FillUploadParams(params, Settings_);
        metadata->set_total_size(Size_);

        Processor_->Write(std::move(chunk), [self = shared_from_this()](NYdbGrpc::TGrpcStatus&& grpcStatus) {
            if (!grpcStatus.Ok()) {
                return;
            }
            self->Offset_ = 0;
            self->ScheduleNextData();
        });
    }

    //! A failed write is not reported from here: it means the stream is gone,
    //! and why it is gone is what the pending read is about to say. Writing just
    //! stops.
    void ScheduleNextData() {
        Connections_->PostToResponseQueue([self = shared_from_this()] {
            self->WriteNextDataOrDone();
        });
    }

    void WriteNextDataOrDone() {
        if (Done_) {
            return;
        }
        TRequest chunk;
        try {
            if (Offset_ == Size_) {
                if (Input_->peek() != std::char_traits<char>::eof() || Input_->bad()) {
                    ythrow yexception() << "Module file changed or failed while being read";
                }
                Processor_->WritesDone([](NYdbGrpc::TGrpcStatus&&) {});
                return;
            }
            const size_t chunkSize = Max<size_t>(1, Settings_.ChunkSize_);
            const size_t size = Min<uint64_t>(chunkSize, Size_ - Offset_);
            std::string data(size, '\0');
            Input_->read(data.data(), size);
            if (Input_->bad() || static_cast<size_t>(Input_->gcount()) != size) {
                ythrow yexception() << "Module file ended early or failed while being read";
            }
            chunk.set_data(data.data(), data.size());
            Offset_ += size;
        } catch (const std::exception& ex) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(NYdb::NIssue::TIssue(ex.what()));
            Fail(TPlainStatus(EStatus::CLIENT_INTERNAL_ERROR, std::move(issues)));
            return;
        }

        Processor_->Write(std::move(chunk), [self = shared_from_this()](NYdbGrpc::TGrpcStatus&& grpcStatus) {
            if (!grpcStatus.Ok()) {
                return;
            }
            self->ScheduleNextData();
        });
    }

    void ReadResponse() {
        Processor_->Read(&Response_, [self = shared_from_this()](NYdbGrpc::TGrpcStatus&& grpcStatus) {
            if (!grpcStatus.Ok()) {
                self->Fail(TPlainStatus(grpcStatus));
                return;
            }
            self->CompleteFromResponse();
        });
    }

    void CompleteFromResponse() {
        if (Done_.exchange(true)) {
            return;
        }

        NYdb::NIssue::TIssues issues;
        NYdb::NIssue::IssuesFromMessage(Response_.operation().issues(), issues);
        TPlainStatus plain(
            static_cast<EStatus>(Response_.operation().status()),
            std::move(issues));

        Ydb::Udf::UploadModuleResult result;
        if (Response_.operation().has_result()) {
            Response_.operation().result().UnpackTo(&result);
        }

        Promise_.SetValue(TUploadModuleResult(TStatus(std::move(plain)), std::move(result)));
        // Finishing while the body is still being written is the normal case for
        // a response the server sent early; the processor half-closes the write
        // side itself once the write in flight comes back.
        Processor_->Finish([](NYdbGrpc::TGrpcStatus&&) {});
    }

private:
    std::shared_ptr<TGRpcConnectionsImpl> Connections_;
    TDbDriverStatePtr DbDriverState_;
    std::shared_ptr<std::istream> Input_;
    const uint64_t Size_;
    TUploadModuleSettings Settings_;
    NThreading::TPromise<TUploadModuleResult> Promise_;
    IProcessor::TPtr Processor_;
    TResponse Response_;
    uint64_t Offset_ = 0;
    //! Read by the write callbacks and written by the read callback, which gRPC
    //! may run on different threads at the same time.
    std::atomic<bool> Done_ = false;
};

} // namespace

TUploadModuleResult::TUploadModuleResult(TStatus&& status, Ydb::Udf::UploadModuleResult&& proto)
    : TStatus(std::move(status))
    , Name_(proto.name())
    , Uid_(proto.uid())
    , Md5_(proto.md5())
    , Size_(proto.size())
    , ReplacedExisting_(proto.replaced_existing())
{
}

const std::string& TUploadModuleResult::GetName() const {
    CheckStatusOk("TUploadModuleResult::GetName");
    return Name_;
}

const std::string& TUploadModuleResult::GetUid() const {
    CheckStatusOk("TUploadModuleResult::GetUid");
    return Uid_;
}

const std::string& TUploadModuleResult::GetMd5() const {
    CheckStatusOk("TUploadModuleResult::GetMd5");
    return Md5_;
}

uint64_t TUploadModuleResult::GetSize() const {
    CheckStatusOk("TUploadModuleResult::GetSize");
    return Size_;
}

bool TUploadModuleResult::GetReplacedExisting() const {
    CheckStatusOk("TUploadModuleResult::GetReplacedExisting");
    return ReplacedExisting_;
}

TListModulesResult::TListModulesResult(TStatus&& status, Ydb::Udf::ListModulesResult&& proto)
    : TStatus(std::move(status))
    , NextPageToken_(proto.next_page_token())
{
    Modules_.reserve(proto.modules_size());
    for (const auto& moduleInfo : proto.modules()) {
        Modules_.push_back(FromProto(moduleInfo));
    }
}

const std::vector<TModuleInfo>& TListModulesResult::GetModules() const {
    CheckStatusOk("TListModulesResult::GetModules");
    return Modules_;
}

const std::string& TListModulesResult::GetNextPageToken() const {
    CheckStatusOk("TListModulesResult::GetNextPageToken");
    return NextPageToken_;
}

TDescribeModuleResult::TDescribeModuleResult(TStatus&& status, Ydb::Udf::DescribeModuleResult&& proto)
    : TStatus(std::move(status))
    , ManifestJson_(proto.manifest_json())
{
    if (proto.has_module_info()) {
        ModuleInfo_ = FromProto(proto.module_info());
    }
    Platforms_.reserve(proto.platforms_size());
    for (const auto& platform : proto.platforms()) {
        Platforms_.push_back(FromProto(platform));
    }
}

const TModuleInfo& TDescribeModuleResult::GetModule() const {
    CheckStatusOk("TDescribeModuleResult::GetModule");
    return ModuleInfo_;
}

const std::string& TDescribeModuleResult::GetManifestJson() const {
    CheckStatusOk("TDescribeModuleResult::GetManifestJson");
    return ManifestJson_;
}

const std::vector<TPlatformCompileStatus>& TDescribeModuleResult::GetPlatforms() const {
    CheckStatusOk("TDescribeModuleResult::GetPlatforms");
    return Platforms_;
}

class TUdfClient::TImpl: public TClientImplCommon<TUdfClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncUploadModuleResult UploadModule(std::string body, const TUploadModuleSettings& settings) {
        const auto size = body.size();
        auto session = std::make_shared<TUploadModuleSession>(
            Connections_, DbDriverState_,
            std::make_shared<std::istringstream>(std::move(body)), size, settings);
        return session->Start();
    }

    TAsyncUploadModuleResult UploadModuleFromFile(const std::string& path, const TUploadModuleSettings& settings) {
        auto input = std::make_shared<std::ifstream>(path, std::ios::binary | std::ios::ate);
        const auto length = input->tellg();
        if (!*input || length < 0 || !input->seekg(0)) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(NYdb::NIssue::TIssue("Cannot read module file: " + path));
            auto promise = NThreading::NewPromise<TUploadModuleResult>();
            promise.SetValue(TUploadModuleResult(
                TStatus(TPlainStatus(EStatus::CLIENT_INTERNAL_ERROR, std::move(issues))), {}));
            return promise.GetFuture();
        }
        auto session = std::make_shared<TUploadModuleSession>(
            Connections_, DbDriverState_, std::move(input), static_cast<uint64_t>(length), settings);
        return session->Start();
    }

    TAsyncStatus DeleteModule(const std::string& name, const TDeleteModuleSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Udf::DeleteModuleRequest>(settings);
        request.set_name(TStringType{name});
        request.set_module_type(ToProto(settings.Type_));
        if (settings.Kind_ != EModuleKind::Unspecified) {
            request.set_module_kind(ToProto(settings.Kind_));
        }
        if (!settings.ExpectedUid_.empty()) {
            request.set_expected_uid(TStringType{settings.ExpectedUid_});
        }

        return RunSimple<Ydb::Udf::V1::UdfService, Ydb::Udf::DeleteModuleRequest, Ydb::Udf::DeleteModuleResponse>(
            std::move(request),
            &Ydb::Udf::V1::UdfService::Stub::AsyncDeleteModule,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncListModulesResult ListModules(const TListModulesSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Udf::ListModulesRequest>(settings);
        request.set_type_filter(ToProto(settings.TypeFilter_));
        if (settings.KindFilter_ != EModuleKind::Unspecified) {
            request.set_kind_filter(ToProto(settings.KindFilter_));
        }
        if (settings.PageSize_) {
            request.set_page_size(*settings.PageSize_);
        }
        if (!settings.PageToken_.empty()) {
            request.set_page_token(TStringType{settings.PageToken_});
        }

        auto promise = NThreading::NewPromise<TListModulesResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Udf::ListModulesResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            promise.SetValue(TListModulesResult(TStatus(std::move(status)), std::move(result)));
        };

        Connections_->RunDeferred<Ydb::Udf::V1::UdfService, Ydb::Udf::ListModulesRequest, Ydb::Udf::ListModulesResponse>(
            std::move(request),
            extractor,
            &Ydb::Udf::V1::UdfService::Stub::AsyncListModules,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeModuleResult DescribeModule(const std::string& name, const TDescribeModuleSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Udf::DescribeModuleRequest>(settings);
        request.set_name(TStringType{name});

        auto promise = NThreading::NewPromise<TDescribeModuleResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Udf::DescribeModuleResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            promise.SetValue(TDescribeModuleResult(TStatus(std::move(status)), std::move(result)));
        };

        Connections_->RunDeferred<Ydb::Udf::V1::UdfService, Ydb::Udf::DescribeModuleRequest, Ydb::Udf::DescribeModuleResponse>(
            std::move(request),
            extractor,
            &Ydb::Udf::V1::UdfService::Stub::AsyncDescribeModule,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TUdfClient::TUdfClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncUploadModuleResult TUdfClient::UploadModule(std::string body, const TUploadModuleSettings& settings) {
    return Impl_->UploadModule(std::move(body), settings);
}

TAsyncUploadModuleResult TUdfClient::UploadModuleFromFile(const std::string& path, const TUploadModuleSettings& settings) {
    return Impl_->UploadModuleFromFile(path, settings);
}

TAsyncStatus TUdfClient::DeleteModule(const std::string& name, const TDeleteModuleSettings& settings) {
    return Impl_->DeleteModule(name, settings);
}

TAsyncListModulesResult TUdfClient::ListModules(const TListModulesSettings& settings) {
    return Impl_->ListModules(settings);
}

TAsyncDescribeModuleResult TUdfClient::DescribeModule(const std::string& name, const TDescribeModuleSettings& settings) {
    return Impl_->DescribeModule(name, settings);
}

} // namespace NYdb::inline Dev::NUdf
