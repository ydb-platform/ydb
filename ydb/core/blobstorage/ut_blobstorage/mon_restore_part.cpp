#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/blobstorage/other/mon_restore_part.h>
#include <ydb/core/blobstorage/vdisk/scrub/blob_recovery.h>
#include <ydb/core/sys_view/common/events.h>

#include <library/cpp/json/json_reader.h>

namespace {

    struct TFixture {
        TEnvironmentSetup Env;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TString Data;
        const TLogoBlobID FullId;
        TDataPartSet Parts;

        TFixture(TBlobStorageGroupType type = TBlobStorageGroupType::Erasure4Plus2Block,
                ui32 size = 1024, bool encryption = false)
            : Env(TEnvironmentSetup::TSettings{
                .NodeCount = 9,
                .VDiskReplPausedAtStart = true,
                .Erasure = type,
                .Encryption = encryption,
                .ConfigPreprocessor = [encryption](ui32, TNodeWardenConfig& config) {
                    if (encryption) {
                        const ui8 keyData[32] = "MonRestorePart test key";
                        config.TenantKey = TEncryptionKey{
                            .Key = {keyData, sizeof(keyData)},
                            .Version = 1,
                            .Id = "tenant key",
                        };
                    }
                },
            })
            , Data(size, 'x')
            , FullId(1, 1, 1, 0, size, 0)
        {
            Env.CreateBoxAndPool(1, 1);
            Env.Sim(TDuration::Minutes(1));
            const auto groups = Env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            Info = Env.GetGroupInfo(groups.front());
            Info->Type.SplitData(TErasureType::CrcModeNone, Data, Parts);
        }

        TVDiskID VDisk(ui32 subgroupIdx) const {
            return Info->GetVDiskInSubgroup(subgroupIdx, FullId.Hash());
        }

        void Put(ui32 partId, ui32 subgroupIdx) {
            Env.PutBlob(VDisk(subgroupIdx), TLogoBlobID(FullId, partId),
                Parts.Parts[partId - 1].OwnedString.ConvertToString());
        }

        void PutDataParts() {
            for (ui32 i = 1; i <= Info->Type.DataParts(); ++i) {
                Put(i, i - 1);
            }
        }

        TString Read(ui32 partId, ui32 subgroupIdx, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            const TVDiskID vdiskId = VDisk(subgroupIdx);
            TString data;
            Env.WithQueueId(vdiskId, NKikimrBlobStorage::GetFastRead, [&](TActorId queueId) {
                const auto edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
                    {TLogoBlobID(FullId, partId)});
                Env.Runtime->Send(new IEventHandle(queueId, edge, query.release()), edge.NodeId());
                auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                const auto& record = result->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResult(0).GetStatus(), status);
                data = result->Get()->GetBlobData(record.GetResult(0)).ConvertToString();
            });
            return data;
        }

        NJson::TJsonValue RestorePartRequest(ui32 nodeId, const TString& vdiskId, const TString& blob,
                const TString& httpStatus = "200 OK", const TString& timeout = "30") {
            const auto actorId = Env.Runtime->Register(CreateMonRestorePartActor(), nodeId);
            const auto edge = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            THttpRequestMock request;
            request.CgiParameters.InsertUnescaped("VDiskId", vdiskId);
            if (blob) {
                request.CgiParameters.InsertUnescaped("BlobId", blob);
            }
            request.CgiParameters.InsertUnescaped("timeout", timeout);
            NMonitoring::TMonService2HttpRequest monRequest(nullptr, &request, nullptr, nullptr, "", nullptr);
            Env.Runtime->Send(new IEventHandle(actorId, edge, new NMon::TEvHttpInfo(monRequest, 123), 0, 456),
                edge.NodeId());
            auto result = Env.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(edge);
            UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->SubRequestId, 123);
            const TString& answer = result->Get()->Answer;
            UNIT_ASSERT_C(answer.StartsWith("HTTP/1.1 " + httpStatus + "\r\n"), answer);
            const auto pos = answer.find("\r\n\r\n");
            UNIT_ASSERT_C(pos != TString::npos, answer);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(answer).SubStr(pos + 4), &json, true));
            Env.Runtime->DestroyActor(actorId);
            return json;
        }

        NJson::TJsonValue RestorePart(ui32 subgroupIdx, const TString& blob, const TString& httpStatus = "200 OK",
                const TString& timeout = "30") {
            const auto vdiskId = VDisk(subgroupIdx);
            const auto nodeId = Info->GetActorId(TVDiskIdShort(vdiskId)).NodeId();
            auto json = RestorePartRequest(nodeId, vdiskId.ToString(), blob, httpStatus, timeout);
            UNIT_ASSERT_VALUES_EQUAL(json["VDiskId"].GetString(), vdiskId.ToString());
            return json;
        }
    };

    void TestRestorePart(ui32 subgroupIdx, ui32 size) {
        TFixture f(TBlobStorageGroupType::Erasure4Plus2Block, size);
        f.PutDataParts();
        f.Read(5, subgroupIdx, NKikimrProto::NODATA);
        ui32 puts = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            // The operation must not send a group-level TEvGet through DSProxy.
            UNIT_ASSERT_VALUES_UNEQUAL(ev->GetTypeRewrite(), ui32(TEvBlobStorage::EvGet));
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), TLogoBlobID(f.FullId, 5));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(record.GetVDiskID()), f.VDisk(subgroupIdx));
                ++puts;
            }
            return true;
        };
        const TLogoBlobID partId(f.FullId, 5);
        auto result = f.RestorePart(subgroupIdx, partId.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(result["BlobId"].GetString(), partId.ToString());
        UNIT_ASSERT_VALUES_EQUAL(puts, 1);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, subgroupIdx), f.Parts.Parts[4].OwnedString.ConvertToString());
        // RestorePart for part 5 must leave the other missing parity untouched.
        f.Read(6, 5, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(subgroupIdx, partId.ToString())["Status"].GetString(), "OK");
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(MonRestorePart) {
    Y_UNIT_TEST(GroupScope) {
        TFixture f;
        f.PutDataParts();
        const TScopeId acceptedScope(100, 200);
        const TScopeId foreignScope(100, 201);
        NKikimrBlobStorage::TConfigRequest config;
        auto* read = config.AddCommand()->MutableReadStoragePool();
        read->SetBoxId(1);
        read->AddName(f.Env.StoragePoolName);
        auto response = f.Env.Invoke(config);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);
        NKikimrBlobStorage::TDefineStoragePool pool = response.GetStatus(0).GetStoragePool(0);
        pool.MutableScopeId()->SetX1(acceptedScope.first);
        pool.MutableScopeId()->SetX2(acceptedScope.second);
        config.Clear();
        config.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = f.Env.Invoke(config);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        f.Env.Sim(TDuration::Seconds(10));
        f.Info = f.Env.GetGroupInfo(f.Info->GroupID.GetRawId());

        auto restorePart = [&](const TScopeId& originScopeId) {
            const auto vdiskId = f.VDisk(4);
            const auto target = f.Info->GetActorId(TVDiskIdShort(vdiskId));
            const auto edge = f.Env.Runtime->AllocateEdgeActor(target.NodeId(), __FILE__, __LINE__);
            auto request = std::make_unique<TEvVRestorePart>();
            VDiskIDFromVDiskID(vdiskId, request->Record.MutableVDiskID());
            LogoBlobIDFromLogoBlobID(TLogoBlobID(f.FullId, 5), request->Record.MutableBlobID());
            const auto deadline = f.Env.Runtime->GetClock() + TDuration::Seconds(30);
            request->Record.SetDeadline(deadline.GetValue());
            IEventHandle ev(target, edge, request.release(), 0, 456);
            // Emulate an incoming Interconnect event with an authenticated OriginScopeId.
            f.Env.Runtime->Send(new IEventHandle(TActorId(), ev.Type, ev.Flags, target, edge,
                ev.ReleaseChainBuffer(), ev.Cookie, originScopeId, {}), target.NodeId());
            auto result = f.Env.WaitForEdgeActorEvent<TEvVRestorePartResult>(edge, true, deadline + TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
            return result;
        };

        bool recoverSeen = false;
        bool putSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            recoverSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob;
            putSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        auto denied = restorePart(foreignScope);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(denied->Get()->Record.GetStatus(), NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(denied->Get()->Record.GetErrorReason(), "access denied");
        UNIT_ASSERT(!recoverSeen);
        UNIT_ASSERT(!putSeen);
        f.Read(5, 4, NKikimrProto::NODATA);

        UNIT_ASSERT_VALUES_EQUAL(restorePart(acceptedScope)->Get()->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
        // Locally generated TEvVRestorePart must also remain available through /restore_part.
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString())["Status"].GetString(), "OK");
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
                const auto* request = ev->Get<TEvVRestorePart>();
                UNIT_ASSERT(request->IsSerializable());
                NKikimrBlobStorage::TEvVRestorePart wire;
                UNIT_ASSERT(wire.ParseFromString(request->Record.SerializeAsString()));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(wire.GetVDiskID()), f.VDisk(4));
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(wire.GetBlobID()), TLogoBlobID(f.FullId, 5));
                requestSeen = true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePartResult) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), target.NodeId());
                UNIT_ASSERT_VALUES_EQUAL(ev->Recipient.NodeId(), ingressNode);
                UNIT_ASSERT(ev->Get<TEvVRestorePartResult>()->IsSerializable());
                resultSeen = true;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePartRequest(ingressNode, f.VDisk(4).ToString(),
            TLogoBlobID(f.FullId, 5).ToString())["Status"].GetString(), "OK");
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePartRequest(ingressNode, f.VDisk(4).ToString(),
            TLogoBlobID(f.FullId, 5).ToString())["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(VSlotsLookupTimeout) {
        TFixture f;
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != NSysView::TEvSysView::EvGetVSlotsResponse;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "504 Gateway Timeout", "1")
            ["Status"].GetString(), "DEADLINE");
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(UnknownVDiskId) {
        TFixture f;
        auto vdiskId = f.VDisk(0);
        vdiskId.VDisk = Max<ui8>();
        bool restorePartSent = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            restorePartSent |= ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart;
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePartRequest(1, vdiskId.ToString(), TLogoBlobID(f.FullId, 1).ToString(),
            "404 Not Found")["Status"].GetString(), "NOT_FOUND");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!restorePartSent);
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePartRequest(1, vdiskId.ToString(), TLogoBlobID(f.FullId, 1).ToString(),
            "409 Conflict")["Status"].GetString(), "RACE");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!restorePartSent);
    }

    Y_UNIT_TEST(MultipartReplyBeforeLastRequiredPart) {
        TFixture f;
        // Three parts share one handoff VDisk; the fourth arrives from another VDisk last.
        for (ui32 part = 1; part <= 3; ++part) {
            f.Put(part, 6);
        }
        f.Put(4, 3);
        std::set<TVDiskIdShort> responded;
        std::unique_ptr<IEventHandle> delayed;
        ui32 delayedNodeId = 0;
        bool released = false;
        bool multipartSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvVGetResult) {
                return true;
            }
            const auto& record = ev->Get<TEvBlobStorage::TEvVGetResult>()->Record;
            if (!record.ResultSize() || LogoBlobIDFromLogoBlobID(record.GetResult(0).GetBlobID()).FullID() != f.FullId) {
                return true;
            }
            const auto vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
            if (vdisk == f.VDisk(3) && !released) {
                UNIT_ASSERT(!delayed);
                delayedNodeId = nodeId;
                delayed = std::move(ev);
            } else if (vdisk != f.VDisk(3)) {
                responded.emplace(vdisk);
                multipartSeen |= vdisk == f.VDisk(6) && record.ResultSize() == 3;
            }
            if (delayed && responded.size() == 6) {
                released = true;
                f.Env.Runtime->Schedule(TDuration::MilliSeconds(1), delayed.release(), nullptr, delayedNodeId);
            }
            return bool(ev);
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString())["Status"].GetString(), "OK");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(multipartSeen);
        UNIT_ASSERT(released);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(PreserveKeepBelowSoftBarrier) {
        TFixture f;
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                ev->Get<TEvBlobStorage::TEvVPut>()->Record.SetIssueKeepFlag(true);
            }
            return true;
        };
        f.PutDataParts();
        f.Env.Runtime->FilterFunction = nullptr;
        for (ui32 i = 0; i < f.Info->GetTotalVDisksNum(); ++i) {
            const auto vdisk = f.Info->GetVDiskId(i);
            f.Env.WithQueueId(vdisk, NKikimrBlobStorage::PutTabletLog, [&](TActorId queueId) {
                const auto edge = f.Env.Runtime->AllocateEdgeActor(queueId.NodeId());
                auto request = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(f.FullId.TabletID(),
                    f.FullId.Generation(), 1, f.FullId.Channel(), true, f.FullId.Generation(), f.FullId.Step(),
                    false, nullptr, nullptr, vdisk, TInstant::Max());
                f.Env.Runtime->Send(new IEventHandle(queueId, edge, request.release()), edge.NodeId());
                auto result = f.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            });
        }
        f.Env.Sim(TDuration::Seconds(10));
        bool keepSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                UNIT_ASSERT(record.GetIssueKeepFlag());
                keepSeen = true;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString())["Status"].GetString(), "OK");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(keepSeen);
        f.Env.CompactVDisk(f.Info->GetActorId(TVDiskIdShort(f.VDisk(4))));
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(MissingPart) {
        TestRestorePart(4, 1024);
    }

    Y_UNIT_TEST(MissingHugePart) {
        TestRestorePart(4, 1024 * 1024);
    }

    Y_UNIT_TEST(Handoff) {
        TestRestorePart(6, 1024);
    }

    Y_UNIT_TEST(Mirror3dc) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc);
        f.Put(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString())["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 0), f.Data);
    }

    Y_UNIT_TEST(Mirror3of4MetadataPart) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        f.Put(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(2, TLogoBlobID(f.FullId, 3).ToString())["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(f.Read(3, 2), "");
    }

    Y_UNIT_TEST(Mirror3of4MetadataOnlySource) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        const ui32 source = f.Info->Type.BlobSubgroupSize() - 1;
        const TLogoBlobID partId(f.FullId, 3);
        f.Env.PutBlob(f.VDisk(source), partId, TString());
        f.Read(3, 2, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(2, partId.ToString())["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(f.Read(3, 2), "");
    }

    Y_UNIT_TEST(Mirror3of4MetadataCannotRestoreData) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        const ui32 source = f.Info->Type.BlobSubgroupSize() - 1;
        f.Env.PutBlob(f.VDisk(source), TLogoBlobID(f.FullId, 3), TString());
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "404 Not Found")
            ["Status"].GetString(), "NODATA");
        f.Read(1, 0, NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(Mirror3dcWithoutConnectionQuorum) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc);
        const auto target = f.VDisk(0);
        ui32 source = 1;
        while (f.VDisk(source).FailRealm != target.FailRealm) {
            ++source;
        }
        f.Put(1, source);
        const auto queueId = f.Env.CreateQueueActor(target, NKikimrBlobStorage::GetFastRead, 123,
            f.Info->GetActorId(TVDiskIdShort(target)).NodeId());
        // Leave BSController running so the HTTP actor can resolve VDiskId through VSlots.
        for (ui32 i = 0; i < f.Info->GetTotalVDisksNum(); ++i) {
            const auto vdiskId = f.Info->GetVDiskId(i);
            if (vdiskId.FailRealm != target.FailRealm) {
                const auto actorId = f.Info->GetActorId(i);
                f.Env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, actorId, {}, nullptr, 0),
                    actorId.NodeId());
            }
        }
        f.Env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString())["Status"].GetString(), "OK");
        const auto edge = f.Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
        auto get = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(target, TInstant::Max(),
            NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {TLogoBlobID(f.FullId, 1)});
        f.Env.Runtime->Send(new IEventHandle(queueId, edge, get.release()), edge.NodeId());
        auto result = f.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.ResultSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetResult(0).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetBlobData(result->Get()->Record.GetResult(0)).ConvertToString(), f.Data);
    }

    Y_UNIT_TEST(EncryptedGroup) {
        TFixture f(TBlobStorageGroupType::Erasure4Plus2Block, 1024, true);
        f.Env.PutBlob(f.Info->GroupID.GetRawId(), f.FullId, f.Data);
        // The first encrypted TEvPut advances GroupGeneration; UpdateSystemViews publishes it every 5 seconds.
        f.Env.Sim(TDuration::Seconds(10));
        f.Info = f.Env.GetGroupInfo(f.Info->GroupID.GetRawId());
        const TString encryptedPart = f.Read(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString())["Status"].GetString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 0), encryptedPart);
    }

    Y_UNIT_TEST(InvalidParameters) {
        TFixture f;
        for (const TString& blob : {TString(), TString("invalid"), f.FullId.ToString(),
                TString("[1:1:1:0:0:1024:7]"), TString("[1:1:1:0:0:1024:17]"),
                TString("[1:1:1:0:0:999999999:1]"), TString("[1:1:1:256:0:1024:1]")}) {
            UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, blob, "400 Bad Request")["Status"].GetString(), "ERROR");
        }
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 2).ToString(), "400 Bad Request")
            ["Status"].GetString(), "ERROR");
        for (const TString& timeout : {"0", "301", "invalid"}) {
            UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "400 Bad Request", timeout)
                ["Status"].GetString(), "ERROR");
        }
        for (const TString& vdisk : {TString(), TString("invalid"), TString("[100000000:1:0:0:0]"),
                TString("[1:1:256:0:0]"), TString("[1:0:0:0:0]")}) {
            UNIT_ASSERT_VALUES_EQUAL(f.RestorePartRequest(1, vdisk, TLogoBlobID(f.FullId, 1).ToString(), "400 Bad Request")
                ["Status"].GetString(), "ERROR");
        }
    }

    Y_UNIT_TEST(StaleVDiskId) {
        TFixture f;
        auto vdisk = f.VDisk(0);
        ++vdisk.GroupGeneration;
        bool putSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart) {
                // Emulate a generation change after the HTTP actor resolved VDiskId.
                ev->Get<TEvVRestorePart>()->Record.MutableVDiskID()->SetGroupGeneration(vdisk.GroupGeneration);
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                putSeen = true;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "409 Conflict")
            ["Status"].GetString(), "RACE");
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!putSeen);
    }

    Y_UNIT_TEST(NoData) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "404 Not Found")
            ["Status"].GetString(), "NODATA");
        f.Read(1, 0, NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(WriteFailure) {
        TFixture f;
        f.PutDataParts();
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPutResult) {
                ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.SetStatus(NKikimrProto::OUT_OF_SPACE);
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString(), "500 Internal Server Error")
            ["Status"].GetString(), "OUT_OF_SPACE");
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(WriteTimeout) {
        TFixture f;
        f.PutDataParts();
        bool putIssued = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                putIssued = true;
                return false;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString(), "504 Gateway Timeout", "1")
            ["Status"].GetString(), "DEADLINE");
        UNIT_ASSERT(putIssued);
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(GenerationRace) {
        TFixture f;
        f.PutDataParts();
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                auto* vdiskId = ev->Get<TEvBlobStorage::TEvVPut>()->Record.MutableVDiskID();
                vdiskId->SetGroupGeneration(vdiskId->GetGroupGeneration() + 1);
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5).ToString(), "409 Conflict")
            ["Status"].GetString(), "RACE");
        f.Env.Runtime->FilterFunction = nullptr;
        f.Read(5, 4, NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(RecoveryTimeout) {
        TFixture f;
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != TEvBlobStorage::EvRecoverBlobResult;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1).ToString(), "504 Gateway Timeout", "1")
            ["Status"].GetString(), "DEADLINE");
        f.Env.Runtime->FilterFunction = nullptr;
    }
}
