#include "mon_blob_range_page.h"
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonBlobRangePage
        : public NMonitoring::IMonPage
    {
        class TRequestActor
            : public TActorBootstrapped<TRequestActor>
        {
            const ui32 GroupId;
            std::unique_ptr<TEvBlobStorage::TEvRange> Query;
            NThreading::TPromise<std::unique_ptr<TEvBlobStorage::TEvRangeResult>> Promise;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_GET_ACTOR;
            }

            TRequestActor(ui32 groupId, ui64 tabletId, const TLogoBlobID& from, const TLogoBlobID& to,
                    NThreading::TPromise<std::unique_ptr<TEvBlobStorage::TEvRangeResult>> promise)
                : GroupId(groupId)
                , Query(std::make_unique<TEvBlobStorage::TEvRange>(tabletId, from, to, false, TInstant::Max(), true))
                , Promise(std::move(promise))
            {}

            void Bootstrap(const TActorContext& ctx) {
                SendToBSProxy(ctx, GroupId, Query.release());
                Become(&TThis::StateFunc);
            }

            void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& ctx) {
                Promise.SetValue(std::unique_ptr<TEvBlobStorage::TEvRangeResult>(ev->Release().Release()));
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvBlobStorage::TEvRangeResult, Handle)
            )
        };

    private:
        TActorSystem *ActorSystem;

    public:
        TMonBlobRangePage(const TString& path, TActorSystem *actorSystem)
            : IMonPage(path)
            , ActorSystem(actorSystem)
        {}

        void Output(NMonitoring::IMonHttpRequest& request) override {
            IOutputStream& out = request.Output();

            // parse HTTP request
            const TCgiParameters& params = request.GetParams();

            auto generateError = [&](const TString& msg) {
                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << msg << "\r\n";
            };

            ui32 groupId = 0;
            if (!params.Has("groupId")) {
                return generateError("Missing groupId parameter");
            } else if (!TryFromString(params.Get("groupId"), groupId)) {
                return generateError("Failed to parse groupId parameter -- must be an integer");
            }

            ui64 tabletId = 0;
            if (!params.Has("tabletId")) {
                return generateError("Missing tabletId parameter");
            } else if (!TryFromString(params.Get("tabletId"), tabletId)) {
                return generateError("Failed to parse tabletId parameter -- must be an integer");
            }

            ui32 json = 0;
            TLogoBlobID from, to;
            TString errorExplanation;
            if (!params.Has("from")) {
                return generateError("Missing from parameter");
            } else if (!TLogoBlobID::Parse(from, params.Get("from"), errorExplanation)) {
                return generateError("Failed to parse from parameter -- " + errorExplanation);
            } else if (from != from.FullID()) {
                return generateError("Desired blob must be full one");
            }
            if (!params.Has("to")) {
                return generateError("Missing to parameter");
            } else if (!TLogoBlobID::Parse(to, params.Get("to"), errorExplanation)) {
                return generateError("Failed to parse to parameter -- " + errorExplanation);
            } else if (to != to.FullID()) {
                return generateError("Desired blob must be full one");
            }
            if (params.Has("json")) {
                if (!TryFromString(params.Get("json"), json)) {
                    return generateError("Failed to parse json parameter -- must be an integer");
                }
            }

            // create promise & future to obtain query result
            auto promise = NThreading::NewPromise<std::unique_ptr<TEvBlobStorage::TEvRangeResult>>();
            auto future = promise.GetFuture();

            // register and start actor
            ActorSystem->Register(new TRequestActor(groupId, tabletId, from, to, std::move(promise)));

            // wait for query to complete
            future.Wait();

            // obtain result
            const std::unique_ptr<TEvBlobStorage::TEvRangeResult>& result = future.GetValue();

            if (json) {
                out << NMonitoring::HTTPOKJSON;

                NJson::TJsonValue root;
                root["Status"] = NKikimrProto::EReplyStatus_Name(result->Status);
                if (result->ErrorReason) {
                    root["ErrorReason"] = result->ErrorReason;
                }
                NJson::TJsonValue blobs(NJson::JSON_ARRAY);
                for (const auto& blob : result->Responses) {
                    blobs.AppendValue(blob.Id.ToString());
                }
                root["Blobs"] = blobs;

                NJsonWriter::TBuf buf;
                buf.WriteJsonValue(&root);
                out << buf.Str();

                return;
            }

            out << NMonitoring::HTTPOKHTML;

            HTML(out) {
                out << "<!DOCTYPE html>\n";
                HTML_TAG() {
                    HEAD() {
                        out << "<title>Blob Range Index Query</title>\n";
                        out << "<link rel='stylesheet' href='https://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css'>\n";
                        out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/jquery/2.1.3/jquery.min.js'></script>\n";
                        out << "<script language='javascript' type='text/javascript' src='https://yastatic.net/bootstrap/3.3.1/js/bootstrap.min.js'></script>\n";
                        out << "<style type=\"text/css\">\n";
                        out << ".table-nonfluid { width: auto; }\n";
                        out << ".narrow-line50 {line-height: 50%}\n";
                        out << ".narrow-line60 {line-height: 60%}\n";
                        out << ".narrow-line70 {line-height: 70%}\n";
                        out << ".narrow-line80 {line-height: 80%}\n";
                        out << ".narrow-line90 {line-height: 90%}\n";
                        out << "</style>\n";
                    }
                    BODY() {
                        DIV_CLASS("panel panel-info") {
                            DIV_CLASS("panel-heading") {
                                out << "Blob Data";
                            }
                            DIV_CLASS("panel-body") {
                                DIV() {
                                    out << "Status: ";
                                    STRONG() {
                                        out << NKikimrProto::EReplyStatus_Name(result->Status);
                                    }
                                }

                                if (result->ErrorReason) {
                                    DIV() {
                                        out << "ErrorReason: ";
                                        STRONG() {
                                            out << result->ErrorReason;
                                        }
                                    }
                                }

                                TABLE() {
                                    TABLEHEAD() {
                                        TABLER() {
                                            TABLEH() {
                                                out << "LogoBlobId";
                                            }
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto& blob : result->Responses) {
                                            TABLER() {
                                                TABLED() {
                                                    out << blob.Id.ToString();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

} // anon

NMonitoring::IMonPage *CreateMonBlobRangePage(const TString& path, TActorSystem *actorSystem) {
    return new TMonBlobRangePage(path, actorSystem);
}

} // NKikimr
