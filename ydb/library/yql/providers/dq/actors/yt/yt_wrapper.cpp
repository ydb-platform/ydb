#include "yt_wrapper.h"

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>

#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/digest/md5/md5.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/stream/fwd.h>
#include <util/stream/buffered.h>
#include <util/system/mutex.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/core/ytree/convert.h>

using namespace NYql;
using namespace NYT;
using namespace NYT::NApi;
using namespace NActors;

namespace NYql {
    struct TRequest: public NYT::TRefCounted {
        const TActorId SelfId;
        const TActorId Sender;
        TActorSystem* Ctx;
        const ui64 RequestId;

        TRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : SelfId(selfId)
            , Sender(sender)
            , Ctx(ctx)
            , RequestId(requestId)
        { }

        void Complete(IEventBase* ev);
    };

    struct TWriteFileRequest: public TRequest {
        IClientPtr Client;
        IFileWriterPtr FileWriter;
        TFile File;
        i64 FileSize = -1;
        TVector<char> Buffer;
        TString NodePathTmp;
        TString NodePath;
        TFileWriterOptions WriterOptions;
        TString Digest;
        i64 Offset = 0;

        TWriteFileRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : TRequest(selfId, sender, ctx, requestId)
        { }


        TFuture<void> WriteNext() {
            const ui64 chunkSize = 64 * 1024 * 1024;
            Buffer.resize(chunkSize);

            i64 dataSize;
            try {
                if (FileSize < 0) {
                    FileSize = File.GetLength();
                }
                dataSize = File.Pread(&Buffer[0], Buffer.size(), Offset);
                Offset += dataSize;
            } catch (const std::exception& ex) {
                return MakeFuture(TErrorOr<void>(ex));
            }

            if (dataSize == 0) {
                YQL_CLOG(DEBUG, ProviderDq) << "Closing writer";
                return FileWriter->Close()
                    .Apply(BIND([self = MakeWeak(this)]() {
                        YQL_CLOG(DEBUG, ProviderDq) << "Write finished";
                        auto this_ = self.Lock();
                        if (!this_) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }

                        return this_->Client->GetNode(this_->NodePathTmp + "/@md5")
                            .Apply(BIND([self](const TErrorOr<NYT::NYson::TYsonString>& err) {
                            auto req = self.Lock();
                            if (!req) {
                                return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                            }
                            if (err.IsOK() && req->Digest == NYTree::ConvertTo<TString>(err.Value())) {
                                return VoidFuture;
                            }

                            return MakeFuture(TErrorOr<void>(yexception() << "wrong checksum"));
                        }));
                    }));
            } else {
                YQL_CLOG(DEBUG, ProviderDq) << "Writing chunk " << Offset << "/" << FileSize;
                return FileWriter->Write(TSharedRef(&Buffer[0], dataSize, nullptr))
                    .Apply(BIND([self = MakeWeak(this)]() mutable {
                        auto this_ = self.Lock();
                        if (!this_) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        return this_->WriteNext();
                    }).AsyncVia(Client->GetConnection()->GetInvoker()));
            }
        }

        TFuture<void> WriteFile()
        {
            auto& remotePath = NodePathTmp;
            YQL_CLOG(INFO, ProviderDq) << "Start writing file to " << remotePath;
            FileWriter = Client->CreateFileWriter(remotePath, WriterOptions);

            return FileWriter->Open().Apply(BIND([self = MakeWeak(this)]() mutable {
                auto this_ = self.Lock();
                if (!this_) {
                    return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                }
                return this_->WriteNext();
            }));
        }
    };

    struct TReadFileRequest;
    void ReadNext(TWeakPtr<TReadFileRequest> request);

    struct TReadFileRequest: public TRequest {
        IClientPtr Client;
        TString LocalPath;
        IFileReaderPtr Reader;
        std::shared_ptr<TFileOutput> Output;
        TString Digest;
        MD5 Md5;

        TReadFileRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : TRequest(selfId, sender, ctx, requestId)
        { }

        TFuture<void> ReadFile()
        {
            auto pos = LocalPath.rfind('/');
            if (pos != TString::npos) {
                auto dirName = LocalPath.substr(0, pos);
                if (!dirName.empty()) {
                    NFs::MakeDirectoryRecursive(dirName, NFs::FP_NONSECRET_FILE, false);
                }
            }

            Output = std::make_shared<TFileOutput>(LocalPath);
            return BIND([self = MakeWeak(this)]() {
                ReadNext(self);
            }).AsyncVia(Client->GetConnection()->GetInvoker()).Run();
        }
    };

    void ReadNext(TWeakPtr<TReadFileRequest> request)
    {
        auto lockedRequest = [&] () {
            auto this_ = request.Lock();
            if (!this_) {
                ythrow yexception() << "request complete";
            }
            return this_;
        };

        while(true) {
            TFuture<TSharedRef> part = lockedRequest()->Reader->Read();
            auto blob = NYT::NConcurrency::WaitFor(part).ValueOrThrow();

            auto this_ = lockedRequest();
            if (!this_) {
                ythrow yexception() << "request complete";
            }

            YQL_CLOG(DEBUG, ProviderDq) << "Store " << blob.Size() << " bytes to " << this_->LocalPath;
            if (blob.Size() == 0) {
                TString buf;
                buf.ReserveAndResize(32);
                this_->Md5.End(buf.begin());

                if (buf == this_->Digest) {
                    this_->Output.reset();
                    return;
                } else {
                    ythrow yexception() << "md5 mismatch";
                }
            }

            this_->Md5.Update(blob.Begin(), blob.Size());
            this_->Output->Write(blob.Begin(), blob.Size());
        }
    }

    using TRequestPtr = NYT::TIntrusivePtr<TRequest>;

    struct TEvComplete
        : NActors::TEventLocal<TEvComplete, TDqEvents::ES_OTHER1> {
        TEvComplete() = default;
        TEvComplete(const TRequestPtr& req)
            : Request(req)
        { }

        const TRequestPtr Request;
    };

    void TRequest::Complete(IEventBase* ev) {
        Ctx->Send(Sender, ev);
        Ctx->Send(SelfId, new TEvComplete(NYT::MakeStrong(this)));
    }

    class TYtWrapper: public TActor<TYtWrapper> {
    public:
        static constexpr char ActorName[] = "YT_WRAPPER";

        TYtWrapper(const IClientPtr& client)
            : TActor(&TYtWrapper::Handler)
            , Client(client)
        { }

    private:
        STRICT_STFUNC(Handler, {
            HFunc(TEvStartOperation, OnStartOperation)
            HFunc(TEvGetOperation, OnGetOperation)
            HFunc(TEvListOperations, OnListOperations)
            HFunc(TEvGetJob, OnGetJob)
            HFunc(TEvWriteFile, OnFileWrite)
            HFunc(TEvReadFile, OnReadFile)
            HFunc(TEvListNode, OnListNode)
            HFunc(TEvSetNode, OnSetNode)
            HFunc(TEvGetNode, OnGetNode)
            HFunc(TEvRemoveNode, OnRemoveNode)
            HFunc(TEvCreateNode, OnCreateNode)
            HFunc(TEvStartTransaction, OnStartTransaction)
            HFunc(TEvPrintJobStderr, OnPrintJobStderr)
            HFunc(TEvComplete, OnComplete)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
        });

        THashSet<TRequestPtr> Requests;

        void PassAway() override {
            Requests.clear();
            IActor::PassAway();
        }

        template<typename T>
        TWeakPtr<T> NewRequest(ui64 id, TActorId sender, const TActorContext& ctx) {
            auto req = New<T>(SelfId(), sender, ctx.ExecutorThread.ActorSystem, id);
            Requests.emplace(req);
            return NYT::MakeWeak(req);
        }

        void OnComplete(TEvComplete::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto req = ev->Get()->Request;
            Requests.erase(req);
        }

        void OnFileWrite(TEvWriteFile::TPtr& ev, const TActorContext& ctx) {
            TFile file = std::move(std::get<0>(*ev->Get()));
            NYPath::TRichYPath remotePath = std::get<1>(*ev->Get());
            THashMap<TString, NYT::TNode> attributes = std::get<2>(*ev->Get());
            TFileWriterOptions writerOptions = std::get<3>(*ev->Get());
            auto requestId = ev->Get()->RequestId;

            auto nodePathTmp = remotePath.GetPath() + ".tmp";
            auto nodePath = remotePath.GetPath();
            auto request = NewRequest<TWriteFileRequest>(requestId, ev->Sender, ctx);
            writerOptions.ComputeMD5 = true;

            try {
                YQL_CLOG(INFO, ProviderDq) << "Creating node " << remotePath.GetPath();

                Y_ENSURE(file.IsOpen());

                TString digest;

                if (writerOptions.ComputeMD5) {
                    char buf[32768];
                    MD5 md5;
                    i64 size, offset = 0;
                    while ((size = file.Pread(buf, sizeof(buf), offset)) > 0) {
                        md5.Update(buf, size);
                        offset += size;
                    }
                    digest.ReserveAndResize(32);
                    md5.End(digest.begin());
                }

                if (auto req = request.Lock()) {
                    req->Client = Client;
                    req->File = std::move(file);
                    req->NodePathTmp = nodePathTmp;
                    req->NodePath = nodePath;
                    req->WriterOptions = writerOptions;
                    req->Digest = digest;
                }

                YT_UNUSED_FUTURE(Client->GetNode(nodePath + "/@md5")
                    .Apply(BIND([request, attributes, digest](const TErrorOr<NYT::NYson::TYsonString>& err) mutable {
                        auto req = request.Lock();
                        if (!req) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        if (err.IsOK() && digest == NYTree::ConvertTo<TString>(err.Value())) {
                            YQL_CLOG(INFO, ProviderDq) << "File already uploaded";
                            try {
                                YT_UNUSED_FUTURE(req->Client->SetNode(req->NodePath + "/@yql_last_update",
                                    NYT::NYson::TYsonString(
                                        NYT::NodeToYsonString(NYT::TNode(ToString(TInstant::Now()))
                                    ))));
                            } catch (...) { }
                            return VoidFuture;
                        } else if (err.IsOK() || err.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                            TCreateNodeOptions options;
                            options.Recursive = true;
                            options.IgnoreExisting = true;

                            if (err.IsOK()) {
                                YQL_CLOG(INFO, ProviderDq) << digest << "!=" << NYTree::ConvertTo<TString>(err.Value());
                            } else {
                                YQL_CLOG(ERROR, ProviderDq) << ToString(err);
                            }

                            return req->Client->CreateNode(req->NodePathTmp, NObjectClient::EObjectType::File, options).As<void>()
                                .Apply(BIND([request, attributes] () {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    TVector<NYT::TFuture<void>> futures;
                                    futures.reserve(attributes.size());
                                    for (const auto& [k, v]: attributes) {
                                        futures.push_back(
                                            req->Client->SetNode(
                                                req->NodePathTmp + "/@" + k,
                                                NYT::NYson::TYsonString(NYT::NodeToYsonString(v)),
                                                NYT::NApi::TSetNodeOptions()));
                                    }
                                    return NYT::AllSucceeded(futures).As<void>();
                                }))
                                .Apply(BIND([request]() mutable {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    return req->WriteFile();
                                }))
                                .Apply(BIND([request] () {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    auto moveOptions = NYT::NApi::TMoveNodeOptions();
                                    moveOptions.Force = true;
                                    return req->Client->MoveNode(req->NodePathTmp, req->NodePath, moveOptions).As<void>();
                                }));
                        }

                        err.ThrowOnError();

                        return VoidFuture;
                    }))
                    .Apply(BIND([request, requestId](const TErrorOr<void>& err)
                    {
                        if (auto req = request.Lock()) {
                            req->Complete(new TEvWriteFileResponse(requestId, err));
                        }
                    })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvWriteFileResponse(requestId, ex));
                }
            }
        }

        void OnReadFile(TEvReadFile::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TReadFileRequest>(requestId, ev->Sender, ctx);

            try {
                NYPath::TRichYPath remotePath = std::get<0>(*ev->Get());
                TFileReaderOptions readerOptions = std::get<2>(*ev->Get());
                if (auto req = request.Lock()) {
                    req->LocalPath = std::get<1>(*ev->Get());
                    req->Client = Client;
                }

                auto nodePath = remotePath.GetPath();

               YT_UNUSED_FUTURE(Client->GetNode(nodePath + "/@md5")
                    .Apply(BIND([request, nodePath, readerOptions](const TErrorOr<NYT::NYson::TYsonString>& err) mutable {
                        auto req = request.Lock();
                        if (!req) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        if (!err.IsOK()) {
                            return MakeFuture(TErrorOr<void>(yexception() << "failed to get md5"));
                        }
                        auto digest = NYTree::ConvertTo<TString>(err.Value());
                        req->Digest = digest;

                        return req->Client->CreateFileReader(nodePath, readerOptions)
                            .Apply(BIND([request](const IFileReaderPtr& reader) {
                                auto req = request.Lock();
                                if (!req) {
                                    return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                }
                                req->Reader = reader;
                                return req->ReadFile();
                            }));
                    }))
                    .Apply(BIND([request, requestId](const TErrorOr<void>& err) {
                        if (auto req = request.Lock()) {
                            req->Complete(new TEvReadFileResponse(requestId, err));
                        }
                    })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvReadFileResponse(requestId, ex));
                }
            }
        }

        void OnStartOperation(TEvStartOperation::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                NScheduler::EOperationType type = std::get<0>(*ev->Get());
                auto spec = NYT::NYson::TYsonString(std::get<1>(*ev->Get()));
                TStartOperationOptions options = std::get<2>(*ev->Get());

                Client->StartOperation(type, spec, options).Subscribe(BIND([=](const TErrorOr<NScheduler::TOperationId>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvStartOperationResponse(requestId, result));
                    }
                }));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvStartOperationResponse(requestId, ex));
                }
            }
        }

        void OnGetOperation(TEvGetOperation::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto operationId = std::get<0>(*ev->Get());
                auto options = std::get<1>(*ev->Get());

                YT_UNUSED_FUTURE(Client->GetOperation(operationId, options).Apply(BIND([=](const TErrorOr<TOperation>& result) {
                    return NYT::NYson::ConvertToYsonString(result.ValueOrThrow()).ToString();
                }))
                .Apply(BIND([=](const TErrorOr<TString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetOperationResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvGetOperationResponse(requestId, ex));
                }
            }
        }

        void OnListOperations(TEvListOperations::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto options = std::get<0>(*ev->Get());

                YT_UNUSED_FUTURE(Client->ListOperations(options).Apply(BIND([=](const TErrorOr<NYT::NApi::TListOperationsResult>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvListOperationsResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvListOperationsResponse(requestId, ex));
                }
            }
        }

        void OnGetJob(TEvGetJob::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto operationId = std::get<0>(*ev->Get());
                auto jobId = std::get<1>(*ev->Get());
                auto options = std::get<2>(*ev->Get());

                YT_UNUSED_FUTURE(Client->GetJob(operationId, jobId, options).Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                    return result.ValueOrThrow().ToString();
                }))
                .Apply(BIND([=](const TErrorOr<TString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetJobResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvGetJobResponse(requestId, ex));
                }
            }
        }

        void OnListNode(TEvListNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                YT_UNUSED_FUTURE(Client->ListNode(path, options)
                    .Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                        return result.ValueOrThrow().ToString();
                    }))
                    .Apply(BIND([=](const TErrorOr<TString>& result) {
                        if (auto req = request.Lock()) {
                            req->Complete(new TEvListNodeResponse(requestId, result));
                        }
                    })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvListNodeResponse(requestId, ex));
                }
            }
        }

        void OnSetNode(TEvSetNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto value = std::get<1>(*ev->Get());
            auto options = std::get<2>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->SetNode(path, value, options)
                .Apply(BIND([=](const TErrorOr<void>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvSetNodeResponse(requestId, result));
                    }
                })));
        }

        void OnGetNode(TEvGetNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->GetNode(path, options)
                .Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetNodeResponse(requestId, result));
                    }
                })));
        }

        void OnRemoveNode(TEvRemoveNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->RemoveNode(path, options)
                .Apply(BIND([=](const TErrorOr<void>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvRemoveNodeResponse(requestId, result));
                    }
                })));
        }

        void OnCreateNode(TEvCreateNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto type = std::get<1>(*ev->Get());
            auto options = std::get<2>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->CreateNode(path, type, options)
                .Apply(BIND([=](const TErrorOr<NYT::NCypressClient::TNodeId>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvCreateNodeResponse(requestId, result));
                    }
                })));
        }

        void OnStartTransaction(TEvStartTransaction::TPtr& ev, const TActorContext& ctx) {
            auto type = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->StartTransaction(type, options)
                .Apply(BIND([=](const TErrorOr<ITransactionPtr>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvStartTransactionResponse(requestId, result));
                    }
                })));
        }

        void OnPrintJobStderr(TEvPrintJobStderr::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto operationId = std::get<0>(*ev->Get());

            YQL_CLOG(DEBUG, ProviderDq) << "Printing stderr of operation " << ToString(operationId);

            YT_UNUSED_FUTURE(Client->ListJobs(operationId)
                .Apply(BIND([operationId, client = MakeWeak(Client)](const TListJobsResult& result) {
                    if (auto cli = client.Lock()) {
                        for (const auto& job : result.Jobs) {
                            YQL_CLOG(DEBUG, ProviderDq) << "Printing stderr (" << ToString(operationId) << "," << ToString(job.Id) << ")";

                            YT_UNUSED_FUTURE(cli->GetJobStderr(operationId, job.Id)
                                .Apply(BIND([jobId = job.Id, operationId](const TSharedRef& data) {
                                    YQL_CLOG(DEBUG, ProviderDq)
                                        << "Stderr ("
                                        << ToString(operationId) << ","
                                        << ToString(jobId) << ")"
                                        << TString(data.Begin(), data.Size());
                                })));
                        }
                    }
                })));
        }

        IClientPtr Client;
    };

    IActor* CreateYtWrapper(const IClientPtr& client) {
        return new TYtWrapper(client);
    }
}
