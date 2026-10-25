#include "restore_part_fixture.h"

#include <ydb/core/blobstorage/vdisk/scrub/blob_recovery.h>

namespace {

    using TFixture = NKikimr::NRestorePartTest::TBlobFixture;

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
        auto result = f.RestorePart(subgroupIdx, partId);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ui32(result.GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR));
        UNIT_ASSERT_VALUES_EQUAL(puts, 1);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, subgroupIdx), f.Parts.Parts[4].OwnedString.ConvertToString());
        // RestorePart for part 5 must leave the other missing parity untouched.
        f.Read(6, 5, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(subgroupIdx, partId).GetStatus(), NKikimrProto::ALREADY);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(VRestorePart) {
    Y_UNIT_TEST(AnyPartOnPrimary) {
        TFixture f;
        f.PutDataParts();
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, f.FullId).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(AnyPartOnHandoffWithoutEnoughDataToReconstruct) {
        TFixture f;
        f.Put(4, 3);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(6, f.FullId).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(4, 6), f.Parts.Parts[3].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(AnyPartAlreadyLocal) {
        TFixture f;
        f.Put(5, 6);
        bool recoverSeen = false;
        bool putSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            recoverSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob;
            putSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(6, f.FullId).GetStatus(), NKikimrProto::ALREADY);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!recoverSeen);
        UNIT_ASSERT(!putSeen);
    }

    Y_UNIT_TEST(ExplicitPartAlreadyLocal) {
        TFixture f;
        f.Put(5, 4);
        bool recoverSeen = false;
        bool putSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            recoverSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob;
            putSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        const auto result = f.RestorePart(4, TLogoBlobID(f.FullId, 5));
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::ALREADY);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!recoverSeen);
        UNIT_ASSERT(!putSeen);
    }

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
        UNIT_ASSERT_VALUES_EQUAL(ui32(denied->Get()->Record.GetResponseOrigin()), ui32(NKikimrBlobStorage::TEvVRestorePartResult::SKELETON_FRONT));
        UNIT_ASSERT(!recoverSeen);
        UNIT_ASSERT(!putSeen);
        f.Read(5, 4, NKikimrProto::NODATA);

        UNIT_ASSERT_VALUES_EQUAL(restorePart(acceptedScope)->Get()->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
        // Locally generated TEvVRestorePart must also remain available in a scoped group.
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5)).GetStatus(), NKikimrProto::ALREADY);
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5)).GetStatus(), NKikimrProto::OK);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(keepSeen);
        f.Env.CompactVDisk(f.Info->GetActorId(TVDiskIdShort(f.VDisk(4))));
        UNIT_ASSERT_VALUES_EQUAL(f.Read(5, 4), f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(MissingPart) {
        TestRestorePart(4, 1024);
    }

    Y_UNIT_TEST(MissingHugePart) {
        TestRestorePart(4, 4 * 1024 * 1024);
    }

    Y_UNIT_TEST(Handoff) {
        TestRestorePart(6, 1024);
    }

    Y_UNIT_TEST(Mirror3of4RejectsMisplacedDataParts) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        bool ioSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            ioSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVGet
                || ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob
                || ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        for (ui32 target = 0; target < 4; ++target) {
            const ui32 part = 2 - target % 2;
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, TLogoBlobID(f.FullId, part)).GetStatus(),
                NKikimrProto::ERROR, "target# " << target << " part# " << part);
        }
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!ioSeen);

        // An existing misplaced part must not satisfy a PartId 0 request.
        f.Put(1, 1);
        f.Put(2, 7);
        ui32 puts = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), TLogoBlobID(f.FullId, 2));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(record.GetVDiskID()), f.VDisk(1));
                ++puts;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(1, f.FullId).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(2, 1), f.Data);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(1, f.FullId).GetStatus(), NKikimrProto::ALREADY);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(puts, 1);
    }

    Y_UNIT_TEST(Mirror3of4AnyPartOnEverySubgroupPosition) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        f.Put(2, 7);
        for (ui32 target = 0; target < 7; ++target) {
            const ui32 part = target < 4 ? target % 2 + 1 : 1;
            f.Read(part, target, NKikimrProto::NODATA);
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, f.FullId).GetStatus(),
                NKikimrProto::OK, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(f.Read(part, target), f.Data, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, f.FullId).GetStatus(),
                NKikimrProto::ALREADY, "target# " << target);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(7, f.FullId).GetStatus(), NKikimrProto::ALREADY);
        // An explicit request must restore the missing part despite another local part.
        f.Read(1, 7, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(7, TLogoBlobID(f.FullId, 1)).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 7), f.Data);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(7, TLogoBlobID(f.FullId, 1)).GetStatus(), NKikimrProto::ALREADY);
    }

    Y_UNIT_TEST(Mirror3of4MetadataAlreadyLocal) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        for (ui32 target = 0; target < f.Info->Type.BlobSubgroupSize(); ++target) {
            f.Env.PutBlob(f.VDisk(target), TLogoBlobID(f.FullId, 3), TString());
        }
        bool recoverySeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            recoverySeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob
                || ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        for (ui32 target = 0; target < f.Info->Type.BlobSubgroupSize(); ++target) {
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, f.FullId).GetStatus(),
                NKikimrProto::ALREADY, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, TLogoBlobID(f.FullId, 3)).GetStatus(),
                NKikimrProto::ALREADY, "target# " << target);
        }
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!recoverySeen);
    }

    Y_UNIT_TEST(Mirror3dc) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc);
        f.Put(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1)).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 0), f.Data);
    }

    Y_UNIT_TEST(Mirror3dcAnyPartOnEverySubgroupPosition) {
        const TBlobStorageGroupType type(TBlobStorageGroupType::ErasureMirror3dc);
        for (ui32 target = 0; target < type.BlobSubgroupSize(); ++target) {
            TFixture f(type);
            const ui32 source = (target + 1) % type.BlobSubgroupSize();
            const ui32 part = target % 3 + 1;
            f.Put(source % 3 + 1, source);
            f.Read(part, target, NKikimrProto::NODATA);
            ui32 puts = 0;
            f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                    const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                    UNIT_ASSERT_VALUES_EQUAL_C(LogoBlobIDFromLogoBlobID(record.GetBlobID()),
                        TLogoBlobID(f.FullId, part), "target# " << target);
                    UNIT_ASSERT_VALUES_EQUAL_C(VDiskIDFromVDiskID(record.GetVDiskID()),
                        f.VDisk(target), "target# " << target);
                    ++puts;
                }
                return true;
            };
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, f.FullId).GetStatus(),
                NKikimrProto::OK, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(puts, 1, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(f.Read(part, target), f.Data, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, f.FullId).GetStatus(),
                NKikimrProto::ALREADY, "target# " << target);
            UNIT_ASSERT_VALUES_EQUAL_C(puts, 1, "target# " << target);
            f.Env.Runtime->FilterFunction = nullptr;
        }
    }

    Y_UNIT_TEST(Mirror3dcAnyPartIgnoresMisplacedLocalPart) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc);
        // Generic ingress permits this historical misplaced part on a handoff VDisk,
        // but mirror-3-dc requires part 2 at subgroup position 4.
        f.Put(1, 4);
        f.Put(3, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 4), f.Data);
        f.Read(2, 4, NKikimrProto::NODATA);
        ui32 puts = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), TLogoBlobID(f.FullId, 2));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(record.GetVDiskID()), f.VDisk(4));
                ++puts;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, f.FullId).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(puts, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(2, 4), f.Data);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, f.FullId).GetStatus(), NKikimrProto::ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(puts, 1);
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(Mirror3dcRejectsPartFromAnotherRealm) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc);
        ui32 reads = 0;
        ui32 recoveries = 0;
        ui32 puts = 0;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            reads += ev->GetTypeRewrite() == TEvBlobStorage::EvVGet;
            recoveries += ev->GetTypeRewrite() == TEvBlobStorage::EvRecoverBlob;
            puts += ev->GetTypeRewrite() == TEvBlobStorage::EvVPut;
            return true;
        };
        for (ui32 target = 0; target < f.Info->Type.BlobSubgroupSize(); ++target) {
            for (ui32 part = 1; part <= f.Info->Type.TotalPartCount(); ++part) {
                if (part != target % 3 + 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(f.RestorePart(target, TLogoBlobID(f.FullId, part)).GetStatus(),
                        NKikimrProto::ERROR, "target# " << target << " part# " << part);
                }
            }
        }
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(reads, 0);
        UNIT_ASSERT_VALUES_EQUAL(recoveries, 0);
        UNIT_ASSERT_VALUES_EQUAL(puts, 0);
    }

    Y_UNIT_TEST_TWIN(Mirror3dcWithoutConnectionQuorum, corrupted) {
        TFixture f(TBlobStorageGroupType::ErasureMirror3dc, corrupted ? 1024 * 1024 : 1024);
        const auto target = f.VDisk(0);
        ui32 source = 1;
        while (f.VDisk(source).FailRealm != target.FailRealm) {
            ++source;
        }
        f.Put(1, source);
        if (corrupted) {
            f.Put(1, 0);
            const auto actorId = f.Info->GetActorId(TVDiskIdShort(target));
            TIntrusivePtr<TPDiskMockState> state;
            for (const auto& [pdiskId, ptr] : f.Env.PDiskMockStates) {
                if (std::get<0>(pdiskId) == actorId.NodeId()) {
                    UNIT_ASSERT(!state);
                    state = ptr;
                }
            }
            UNIT_ASSERT(state);
            const auto layout = f.Env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult,
                TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            ui32 corruptedParts = 0;
            for (const auto& item : layout->Layout) {
                if (item.BlobId == f.FullId) {
                    UNIT_ASSERT(item.RecordType == TEvBlobStorage::TEvCaptureVDiskLayoutResult::ERecordType::HugeBlob);
                    const auto& location = item.Location;
                    state->SetCorruptedArea(location.ChunkIdx, location.Offset, location.Offset + location.Size, true);
                    ++corruptedParts;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(corruptedParts, 1);
        }
        const auto queueId = f.Env.CreateQueueActor(target, NKikimrBlobStorage::GetFastRead, 123,
            f.Info->GetActorId(TVDiskIdShort(target)).NodeId());
        // Stop only VDisks outside the target realm.
        for (ui32 i = 0; i < f.Info->GetTotalVDisksNum(); ++i) {
            const auto vdiskId = f.Info->GetVDiskId(i);
            if (vdiskId.FailRealm != target.FailRealm) {
                const auto actorId = f.Info->GetActorId(i);
                f.Env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, actorId, {}, nullptr, 0),
                    actorId.NodeId());
            }
        }
        f.Env.Sim(TDuration::Seconds(10));
        bool corruptedReadSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult) {
                for (const auto& item : ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult()) {
                    corruptedReadSeen |= item.GetStatus() == NKikimrProto::CORRUPTED;
                }
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1)).GetStatus(), NKikimrProto::OK);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL(corruptedReadSeen, corrupted);
        const auto edge = f.Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
        auto get = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(target, TInstant::Max(),
            NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
            {TLogoBlobID(f.FullId, 1)});
        get->SuppressCorruptedBlobRestoration = true;
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
        // Allow the generation change triggered by the first encrypted TEvPut to reach the VDisks.
        f.Env.Sim(TDuration::Seconds(10));
        f.Info = f.Env.GetGroupInfo(f.Info->GroupID.GetRawId());
        const TString encryptedPart = f.Read(1, 0);
        UNIT_ASSERT_VALUES_UNEQUAL(encryptedPart, f.Parts.Parts[0].OwnedString.ConvertToString());
        f.Read(1, 6, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(6, TLogoBlobID(f.FullId, 1)).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Read(1, 6), encryptedPart);
    }

    Y_UNIT_TEST(StaleVDiskId) {
        TFixture f;
        auto vdisk = f.VDisk(0);
        ++vdisk.GroupGeneration;
        bool putSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart) {
                // Emulate a stale request arriving after the VDisk generation changed.
                ev->Get<TEvVRestorePart>()->Record.MutableVDiskID()->SetGroupGeneration(vdisk.GroupGeneration);
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                putSeen = true;
            }
            return true;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1))
            .GetStatus(), NKikimrProto::RACE);
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(!putSeen);
    }

    Y_UNIT_TEST(NoData) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1))
            .GetStatus(), NKikimrProto::NODATA);
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5))
            .GetStatus(), NKikimrProto::OUT_OF_SPACE);
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5), TDuration::Seconds(1))
            .GetStatus(), NKikimrProto::DEADLINE);
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
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(4, TLogoBlobID(f.FullId, 5))
            .GetStatus(), NKikimrProto::RACE);
        f.Env.Runtime->FilterFunction = nullptr;
        f.Read(5, 4, NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(RecoveryTimeout) {
        TFixture f;
        f.Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != TEvBlobStorage::EvRecoverBlobResult;
        };
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 1), TDuration::Seconds(1))
            .GetStatus(), NKikimrProto::DEADLINE);
        f.Env.Runtime->FilterFunction = nullptr;
    }

    Y_UNIT_TEST(InvalidPartId) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 2)).GetStatus(), NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.RestorePart(0, TLogoBlobID(f.FullId, 7)).GetStatus(), NKikimrProto::ERROR);
    }
}
