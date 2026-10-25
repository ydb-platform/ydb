#include "restore_part_fixture.h"
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/blobstorage/other/mon_restore_part.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <google/protobuf/text_format.h>

#include <util/string/strip.h>

namespace {

    struct TRequest : THttpRequestMock {
        TCgiParameters PostParameters;
        HTTP_METHOD Method = HTTP_METHOD_POST;

        TRequest() { Path = "/restore_part"; }

        HTTP_METHOD GetMethod() const override { return Method; }
        const TCgiParameters& GetPostParams() const override { return PostParameters; }
    };

    TString Http(TTestActorSystem& runtime, TRequest& request, ui32 nodeId = 1) {
        const auto actorId = runtime.Register(CreateMonRestorePartActor(), nodeId);
        const auto edge = runtime.AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        NMonitoring::TMonService2HttpRequest monRequest(nullptr, &request, nullptr, nullptr, "", nullptr);
        runtime.Send(new IEventHandle(actorId, edge, new NMon::TEvHttpInfo(monRequest, 123), 0, 456),
            edge.NodeId());
        auto result = runtime.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(edge);
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->SubRequestId, 123);
        const TString answer = result->Get()->Answer;
        runtime.DestroyActor(actorId);
        return answer;
    }

    std::vector<NKikimrBlobStorage::TEvVRestorePartResult> RestoreBatch(TTestActorSystem& runtime, const TString& text, const TString& inflight = {},
            const TString& httpStatus = "200 OK", const TString& timeout = "30", ui32 nodeId = 1) {
        TRequest request;
        // The handler must read POST parameters, even when query parameters disagree.
        request.CgiParameters.InsertUnescaped("requests", "invalid query parameter");
        request.PostParameters.InsertUnescaped("requests", text);
        if (inflight) {
            request.PostParameters.InsertUnescaped("inflight", inflight);
        }
        request.PostParameters.InsertUnescaped("timeout", timeout);
        const TString answer = Http(runtime, request, nodeId);
        UNIT_ASSERT_C(answer.StartsWith("HTTP/1.1 " + httpStatus + "\r\n"), answer);
        UNIT_ASSERT_STRING_CONTAINS(answer, "Content-Type: text/plain; charset=utf-8\r\n");
        const auto pos = answer.find("\r\n\r\n");
        UNIT_ASSERT_C(pos != TString::npos, answer);
        TStringBuf body = TStringBuf(answer).SubStr(pos + 4);
        UNIT_ASSERT(body.EndsWith('\n'));
        TStringBuf requests = text;
        std::vector<NKikimrBlobStorage::TEvVRestorePartResult> results;
        while (body) {
            auto line = body.NextTok('\n');
            UNIT_ASSERT_C(!line.empty(), answer);
            if (httpStatus != "400 Bad Request") {
                TStringBuf request;
                do {
                    request = StripString(requests.NextTok('\n'));
                } while (!request && requests);
                const size_t separator = request.find_first_of(" \t");
                UNIT_ASSERT_VALUES_EQUAL(line.NextTok(' '), request.SubStr(0, separator));
                UNIT_ASSERT_VALUES_EQUAL(line.NextTok(' '), StripString(request.SubStr(separator)));
            }
            UNIT_ASSERT_C(NProtoBuf::TextFormat::ParseFromString(TString(line), &results.emplace_back()), line);
            UNIT_ASSERT(results.back().HasStatus());
            UNIT_ASSERT(results.back().HasResponseOrigin());
        }
        UNIT_ASSERT(!results.empty());
        return results;
    }

    NKikimrBlobStorage::TEvVRestorePartResult RestorePartRequest(TTestActorSystem& runtime, ui32 nodeId, const TString& vdiskId, const TString& blob,
            const TString& httpStatus = "200 OK", const TString& timeout = "30") {
        auto results = RestoreBatch(runtime, vdiskId + " " + blob, {}, httpStatus, timeout, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        return results.front();
    }

    struct TFixture : NKikimr::NRestorePartTest::TBlobFixture {
        NKikimrBlobStorage::TEvVRestorePartResult RestorePart(ui32 subgroupIdx, const TString& blob, const TString& httpStatus = "200 OK",
                const TString& timeout = "30") {
            const auto vdiskId = VDisk(subgroupIdx);
            const auto nodeId = Info->GetActorId(TVDiskIdShort(vdiskId)).NodeId();
            return RestorePartRequest(*Env.Runtime, nodeId, vdiskId.ToString(), blob, httpStatus, timeout);
        }

    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(MonRestorePart) {
    Y_UNIT_TEST(MultilinePost) {
        TFixture f;
        f.PutDataParts();
        const TString text = TStringBuilder() << "\r\n " << f.VDisk(0) << "\t" << TLogoBlobID(f.FullId, 1)
            << " \r\n\t\n" << f.VDisk(4) << " " << TLogoBlobID(f.FullId, 5)
            << "\n" << f.VDisk(5) << " " << TLogoBlobID(f.FullId, 6) << "\n";
        const auto results = RestoreBatch(*f.Env.Runtime, text, "1", "200 OK", "1000");
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(results[0].GetStatus(), NKikimrProto::ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(results[1].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(results[2].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
        UNIT_ASSERT_VALUES_EQUAL(f.Read(6, 5), f.Parts.Parts[5].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(BatchInflightAndCookies) {
        TFixture f;
        const auto line = f.VDisk(4).ToString() + " " + TLogoBlobID(f.FullId, 5).ToString();
        const TString errorReason = "missing part\n\"quoted\"\r\n";
        std::set<ui64> active;
        std::set<ui64> cookies;
        TActorId sender;
        TActorId target;
        ui32 numSent = 0;
        auto reply = [&](ui64 cookie, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            auto result = std::make_unique<TEvVRestorePartResult>();
            result->Record.SetStatus(status);
            result->Record.SetResponseOrigin(NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR);
            if (status == NKikimrProto::NODATA) {
                result->Record.SetErrorReason(errorReason);
            }
            f.Env.Runtime->Send(new IEventHandle(sender, target, result.release(), 0, cookie), target.NodeId());
        };
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart) {
                UNIT_ASSERT(cookies.insert(ev->Cookie).second);
                UNIT_ASSERT(ev->Cookie);
                active.insert(ev->Cookie);
                UNIT_ASSERT(active.size() <= 2);
                ++numSent;
                sender = ev->Sender;
                target = ev->Recipient;
                if (numSent == 2) {
                    reply(2, NKikimrProto::NODATA);
                    reply(2, NKikimrProto::NODATA); // duplicate must not free another inflight slot
                } else if (numSent == 3) {
                    reply(3);
                    reply(1);
                } else if (numSent == 4) {
                    reply(4);
                }
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePartResult) {
                active.erase(ev->Cookie);
            }
            return true;
        };
        const auto results = RestoreBatch(*f.Env.Runtime, line + "\n" + line + "\n" + line + "\n" + line, "2", "404 Not Found");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(numSent, 4);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(results[0].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ui32(results[0].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR));
        UNIT_ASSERT_VALUES_EQUAL(results[1].GetStatus(), NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(results[1].GetErrorReason(), errorReason);
        UNIT_ASSERT_VALUES_EQUAL(results[2].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(results[3].GetStatus(), NKikimrProto::OK);
    }

    Y_UNIT_TEST(BatchTimeoutPreservesCompleted) {
        TFixture f;
        f.PutDataParts();
        const auto line = f.VDisk(4).ToString() + " " + TLogoBlobID(f.FullId, 5).ToString();
        ui32 sent = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != TEvBlobStorage::EvVRestorePart || ++sent == 1;
        };
        const auto results = RestoreBatch(*f.Env.Runtime, line + "\n" + line + "\n" + line, {}, "504 Gateway Timeout", "1");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(sent, 2);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(results[0].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(results[1].GetStatus(), NKikimrProto::DEADLINE);
        UNIT_ASSERT_VALUES_EQUAL(results[2].GetStatus(), NKikimrProto::DEADLINE);
        UNIT_ASSERT_VALUES_EQUAL(ui32(results[0].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR));
        UNIT_ASSERT_VALUES_EQUAL(ui32(results[1].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::MON_RESTORE_PART_REQUEST_ACTOR));
        UNIT_ASSERT_VALUES_EQUAL(ui32(results[2].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::MON_RESTORE_PART_REQUEST_ACTOR));
    }

    Y_UNIT_TEST(BatchResolutionErrors) {
        TFixture f;
        f.PutDataParts();
        auto unknown = f.VDisk(0);
        unknown.VDisk = Max<ui8>();
        auto stale = f.VDisk(0);
        ++stale.GroupGeneration;
        const auto blob = TLogoBlobID(f.FullId, 5).ToString();
        ui32 lookups = 0;
        ui32 requests = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            // Count the original SendData submission, before pipe and BSC forwarding.
            lookups += ev->Type == NSysView::TEvSysView::EvGetVSlotsRequest
                && ev->GetTypeRewrite() == TEvTabletPipe::EvSend
                && ev->Recipient == ev->GetRecipientRewrite();
            requests += ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart;
            return true;
        };
        const auto results = RestoreBatch(*f.Env.Runtime, unknown.ToString() + " " + blob + "\n"
            + stale.ToString() + " " + blob + "\n" + f.VDisk(4).ToString() + " " + blob,
            "1025", "404 Not Found");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(lookups, 1);
        UNIT_ASSERT_VALUES_EQUAL(requests, 1);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(results[0].GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(results[1].GetStatus(), NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(results[2].GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ui32(results[0].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::MON_RESTORE_PART_REQUEST_ACTOR));
    }

    Y_UNIT_TEST(BatchDeliveryFailures) {
        TFixture f;
        const auto disconnected = f.Info->GetActorId(TVDiskIdShort(f.VDisk(4)));
        const auto undelivered = f.Info->GetActorId(TVDiskIdShort(f.VDisk(5)));
        const auto healthy = f.Info->GetActorId(TVDiskIdShort(f.VDisk(6)));
        UNIT_ASSERT_VALUES_UNEQUAL(disconnected.NodeId(), undelivered.NodeId());
        UNIT_ASSERT_VALUES_UNEQUAL(disconnected.NodeId(), healthy.NodeId());
        ui32 ingressNode = 1;
        while (ingressNode == disconnected.NodeId() || ingressNode == undelivered.NodeId()
                || ingressNode == healthy.NodeId()) {
            ++ingressNode;
        }
        std::set<ui64> sent;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvVRestorePart) {
                return true;
            }
            sent.insert(ev->Cookie);
            const auto sender = ev->Sender;
            if (ev->Cookie == 2) {
                f.Env.Runtime->Send(new IEventHandle(sender, {},
                    new TEvInterconnect::TEvNodeDisconnected(disconnected.NodeId())), ingressNode);
                f.Env.Runtime->Send(new IEventHandle(sender, undelivered,
                    new TEvents::TEvUndelivered(TEvBlobStorage::EvVRestorePart,
                        TEvents::TEvUndelivered::ReasonActorUnknown), 0, 2), ingressNode);
            } else if (ev->Cookie == 4) {
                auto result = std::make_unique<TEvVRestorePartResult>();
                result->Record.SetStatus(NKikimrProto::OK);
                result->Record.SetResponseOrigin(NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR);
                f.Env.Runtime->Send(new IEventHandle(sender, healthy, result.release(), 0, 4), healthy.NodeId());
            }
            return false;
        };
        const auto blob = TLogoBlobID(f.FullId, 5).ToString();
        const auto results = RestoreBatch(*f.Env.Runtime, f.VDisk(4).ToString() + " " + blob + "\n"
            + f.VDisk(5).ToString() + " " + blob + "\n" + f.VDisk(4).ToString() + " " + blob + "\n"
            + f.VDisk(6).ToString() + " " + blob, "2", "503 Service Unavailable", "30", ingressNode);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 3);
        UNIT_ASSERT(!sent.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 4);
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(results[i].GetStatus(), NKikimrProto::NOTREADY);
        }
        UNIT_ASSERT_VALUES_EQUAL(results[3].GetStatus(), NKikimrProto::OK);
    }

    Y_UNIT_TEST(InvalidBatch) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto line = TString("[80000000:1:0:0:0] [1:1:1:0:0:1024:1]");
        bool restoreSeen = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            restoreSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart
                || ev->GetTypeRewrite() == NSysView::TEvSysView::EvGetVSlotsRequest;
            return true;
        };
        for (const TString& text : {TString(), TString(" \n\r\n"), line + " extra", line + "\ninvalid"}) {
            const auto results = RestoreBatch(runtime, text, "1", "400 Bad Request");
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(results[0].GetStatus(), NKikimrProto::ERROR);
            UNIT_ASSERT(!results[0].GetErrorReason().empty());
            UNIT_ASSERT_VALUES_EQUAL(ui32(results[0].GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::MON_RESTORE_PART_REQUEST_ACTOR));
        }
        for (const TString& inflight : {"0", "4294967296", "invalid"}) {
            UNIT_ASSERT_VALUES_EQUAL(RestoreBatch(runtime, line, inflight, "400 Bad Request").front().GetStatus(), NKikimrProto::ERROR);
        }
        runtime.FilterFunction = nullptr;
        UNIT_ASSERT(!restoreSeen);
    }

    Y_UNIT_TEST(CrossNodeRequest) {
        TFixture f;
        f.PutDataParts();
        const auto target = f.Info->GetActorId(TVDiskIdShort(f.VDisk(4)));
        const ui32 ingressNode = target.NodeId() == 1 ? 2 : 1;
        bool requestSeen = false;
        bool resultSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), ingressNode);
                UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, target);
                const auto& record = ev->Get<TEvVRestorePart>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(record.GetVDiskID()), f.VDisk(4));
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), TLogoBlobID(f.FullId, 5));
                requestSeen = true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePartResult) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), target.NodeId());
                UNIT_ASSERT_VALUES_EQUAL(ev->Recipient.NodeId(), ingressNode);
                resultSeen = true;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(*f.Env.Runtime, ingressNode, f.VDisk(4).ToString(),
            TLogoBlobID(f.FullId, 5).ToString()).GetStatus(), NKikimrProto::OK);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(requestSeen);
        UNIT_ASSERT(resultSeen);
        f.Read(5, 4);
    }

    Y_UNIT_TEST(ResolveVDiskWithoutLocalGroupInfo) {
        TFixture f;
        f.PutDataParts();
        std::set<ui32> vdiskNodes;
        for (ui32 i = 0; i < f.Info->GetTotalVDisksNum(); ++i) {
            vdiskNodes.insert(f.Info->GetActorId(i).NodeId());
        }
        ui32 ingressNode = 1;
        while (vdiskNodes.contains(ingressNode)) {
            ++ingressNode;
        }
        UNIT_ASSERT(ingressNode <= f.Env.Settings.NodeCount);
        const auto edge = f.Env.Runtime->AllocateEdgeActor(ingressNode);
        f.Env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(ingressNode), edge,
            new TEvNodeWardenQueryGroupInfo(f.Info->GroupID.GetRawId())), ingressNode);
        auto cached = f.Env.WaitForEdgeActorEvent<TEvNodeWardenGroupInfo>(edge);
        UNIT_ASSERT(!cached->Get()->Record.HasGroup());
        UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(*f.Env.Runtime, ingressNode, f.VDisk(4).ToString(),
            TLogoBlobID(f.FullId, 5).ToString()).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(VSlotsLookupTimeout) {
        TFixture f;
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != NSysView::TEvSysView::EvGetVSlotsResponse;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "504 Gateway Timeout", "1")
            .GetStatus(), NKikimrProto::DEADLINE);
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(VDiskGenerationMismatch) {
        TFixture f;
        auto vdiskId = f.VDisk(0);
        ++vdiskId.GroupGeneration;
        bool restorePartSent = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            restorePartSent |= ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart;
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(*f.Env.Runtime, 1, vdiskId.ToString(), TLogoBlobID(f.FullId, 1).ToString(),
            "409 Conflict").GetStatus(), NKikimrProto::RACE);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!restorePartSent);
    }

    Y_UNIT_TEST(InvalidParameters) {
        TTestActorSystem runtime(1);
        runtime.Start();
        for (const TString& blob : {TString(), TString("invalid"), TString("[1:1:1:0:0:1024:17]"),
                TString("[1:1:1:0:0:999999999:1]"), TString("[1:1:1:256:0:1024:1]")}) {
            UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(runtime, 1, "[80000000:1:0:0:0]", blob, "400 Bad Request").GetStatus(), NKikimrProto::ERROR);
        }
        for (const TString& timeout : {"0", "-1", "4294967296", "invalid"}) {
            UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(runtime, 1, "[80000000:1:0:0:0]", "[1:1:1:0:0:1024:1]", "400 Bad Request", timeout)
                .GetStatus(), NKikimrProto::ERROR);
        }
        for (const TString& vdisk : {TString(), TString("invalid"), TString("[100000000:1:0:0:0]"),
                TString("[1:1:256:0:0]"), TString("[1:0:0:0:0]")}) {
            UNIT_ASSERT_VALUES_EQUAL(RestorePartRequest(runtime, 1, vdisk, "[1:1:1:0:0:1024:1]", "400 Bad Request")
                .GetStatus(), NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(RejectUnsupportedMethods) {
        TTestActorSystem runtime(1);
        runtime.Start();
        bool restoreSeen = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            restoreSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart
                || ev->GetTypeRewrite() == NSysView::TEvSysView::EvGetVSlotsRequest;
            return true;
        };
        TRequest request;
        for (const auto method : {HTTP_METHOD_GET, HTTP_METHOD_PUT}) {
            request.Method = method;
            UNIT_ASSERT_STRING_CONTAINS(Http(runtime, request), "405 Method Not Allowed\r\nAllow: POST\r\n");
        }
        runtime.FilterFunction = nullptr;
        UNIT_ASSERT(!restoreSeen);
    }
}
