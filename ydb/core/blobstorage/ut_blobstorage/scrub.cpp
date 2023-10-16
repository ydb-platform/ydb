#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(BlobScrubbing) {

    ui32 QueueClientId = 0;

    using TLayout = TEvBlobStorage::TEvCaptureVDiskLayoutResult;

    void Populate(TEnvironmentSetup& env, TVDiskID vdiskId, TActorId vdiskActorId, std::map<TLogoBlobID, TString>& data,
            std::unique_ptr<TLayout>& layout) {
        const TActorId& sender = env.Runtime->AllocateEdgeActor(vdiskActorId.NodeId());

        std::vector<TString> blobs;
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 size = RandomNumber(10u) == 0 ? RandomNumber<ui32>(3 << 20) + 1048576 : (RandomNumber<ui32>(65536) + 1); // 1 byte - 4 MB
            TString data = TString::Uninitialized(size);
            memset(data.Detach(), RandomNumber<ui8>(), size);
            blobs.push_back(data);
        }

        for (ui32 i = 1;; ++i) {
            const TString& data = blobs[RandomNumber(blobs.size())];
            TLogoBlobID id(1, 1, i, 0, data.size(), 0);
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, vdiskId.GroupID, ev.release());
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            Cerr << "res# " << res->Get()->ToString() << Endl;

            if (i % 500 == 0) {
                env.CompactVDisk(vdiskActorId, true /*freshOnly*/);

                bool done = false;
                for (;;) {
                    env.Runtime->Send(new IEventHandle(vdiskActorId, sender, new TEvBlobStorage::TEvCaptureVDiskLayout),
                        sender.NodeId());
                    auto res = env.WaitForEdgeActorEvent<TLayout>(sender, false);
                    layout.reset(res->Release().Release());
                    ui32 num0 = 0, num1_16 = 0, num17 = 0, num18 = 0;
                    for (const auto& item : layout->Layout) {
                        if (item.Database == TLayout::EDatabase::LogoBlobs && item.RecordType == TLayout::ERecordType::IndexRecord) {
                            if (item.Level == 0) {
                                ++num0;
                            } else if (item.Level < 17) {
                                ++num1_16;
                            } else if (item.Level == 17) {
                                ++num17;
                            } else if (item.Level == 18) {
                                ++num18;
                            } else {
                                Y_ABORT("unexpected level");
                            }
                        }
                    }
                    Cerr << "num0# " << num0 << " num1..16# " << num1_16 << " num17# " << num17 << " num18# " << num18 << Endl;
                    if (num0 > 3 && num1_16 && (num17 > 1 || num18 > 1)) {
                        done = true;
                        break;
                    }
                    if (num0 < 20) {
                        break;
                    }
                    env.Sim(TDuration::Seconds(10));
                }
                if (done) {
                    break;
                }
            }
        }
        env.Runtime->DestroyActor(sender);

        TActorId queueId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000);
        TLogoBlobID fromId = TLogoBlobID(1, 0, 0, 0, 0, 0);
        TLogoBlobID toId = TLogoBlobID(1, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
            TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
        for (;;) {
            const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
            auto ev = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, {}, {}, fromId, toId, 1000);
            env.Runtime->Send(new IEventHandle(queueId, sender, ev.release()), sender.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender);
            auto& r = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), NKikimrProto::OK);
            for (const auto& item : r.GetResult()) {
                UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
                const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                fromId = id;
                auto ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, {}, {}, {id});
                const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
                env.Runtime->Send(new IEventHandle(queueId, sender, ev.release()), sender.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender);
                auto& r = res->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), NKikimrProto::OK);
                for (const auto& item : r.GetResult()) {
                    if (item.GetStatus() == NKikimrProto::OK) {
                        const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                        const TString buffer = res->Get()->GetBlobData(item).ConvertToString();
                        const TString& hash = MD5::Calc(buffer);
                        data.emplace(id, hash);
                        Cerr << "BlobId# " << id.ToString() << " hash# " << hash << " size# " << buffer.size() << Endl;
                    }
                }
            }
            if (!r.GetIsRangeOverflow()) {
                break;
            }
        }
    }

    void Validate(TEnvironmentSetup& env, const TVDiskID& vdiskId, std::map<TLogoBlobID, TString>& data,
            TBlobStorageGroupType type, const std::set<TLogoBlobID>& blobIdsToValidate) {
        const TActorId& queueId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, ++QueueClientId);
        const TActorId& edge = env.Runtime->AllocateEdgeActor(queueId.NodeId());

        std::unique_ptr<TEvBlobStorage::TEvVGet> ev;
        ui32 total = 0;
        ui32 numMsgs = 0;
        ui32 numBlobs = 0;
        for (const auto& key : blobIdsToValidate) {
            const ui32 partSize = type.PartSize(key);
            const ui32 size = partSize + BlobProtobufHeaderMaxSize;
            if (size + total > 60000000) {
                env.Runtime->Send(new IEventHandle(queueId, edge, ev.release()), edge.NodeId());
                total = 0;
                ++numMsgs;
            }
            if (!ev) {
                ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead);
            }
            ev->AddExtremeQuery(key, 0, 0);
            total += size;
            ++numBlobs;
        }
        if (ev) {
            env.Runtime->Send(new IEventHandle(queueId, edge, ev.release()), edge.NodeId());
            ++numMsgs;
        }
        while (numMsgs--) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
            auto& r = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), NKikimrProto::OK);
            for (const auto& result : r.GetResult()) {
                const TLogoBlobID& key = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrProto::OK, "Id# " << key);
                UNIT_ASSERT_EQUAL(MD5::Calc(res->Get()->GetBlobData(result).ConvertToString()), data.at(key));
                --numBlobs;
            }
        }
        UNIT_ASSERT(!numBlobs);

        env.Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, queueId, edge, nullptr, 0), 1);
        env.Runtime->DestroyActor(edge);
    }

    void ScrubTest(TBlobStorageGroupType erasure) {
        SetRandomSeed(1);
        TEnvironmentSetup env(false, erasure);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        auto info = env.GetGroupInfo(groups[0]);
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);

        std::map<TLogoBlobID, TString> data;
        std::unique_ptr<TLayout> layout;
        Populate(env, vdiskId, vdiskActorId, data, layout);

        env.SetScrubPeriodicity(TDuration::Seconds(60));
        env.Sim(TDuration::Seconds(60));

        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
        auto it = env.PDiskMockStates.find(std::make_pair(nodeId, pdiskId));
        Y_ABORT_UNLESS(it != env.PDiskMockStates.end());
        TPDiskMockState::TPtr snapshot = it->second->Snapshot();

        std::map<ui32, std::vector<const TLayout::TLayoutRecord*>> indexes;
        std::map<ui64, std::vector<const TLayout::TLayoutRecord*>> inplaceBlobs;
        std::map<ui64, std::vector<const TLayout::TLayoutRecord*>> hugeBlobs;
        std::map<ui32, std::vector<TLogoBlobID>> blobsPerChunk;
        std::map<TLogoBlobID, TDiskPart> locations;
        for (const auto& item : layout->Layout) {
            if (item.Database != TLayout::EDatabase::LogoBlobs) {
                continue;
            }
            switch (item.RecordType) {
                case TLayout::ERecordType::IndexRecord:
                    if (item.Level == 0) {
                        indexes[0].push_back(&item);
                    } else if (item.Level < 17) {
                        indexes[1].push_back(&item);
                    } else {
                        indexes[2].push_back(&item);
                    }
                    break;

                case TLayout::ERecordType::InplaceBlob:
                    inplaceBlobs[item.SstId].push_back(&item);
                    blobsPerChunk[item.Location.ChunkIdx].push_back(item.BlobId);
                    locations.emplace(item.BlobId, item.Location);
                    break;

                case TLayout::ERecordType::HugeBlob:
                    hugeBlobs[item.SstId].push_back(&item);
                    break;
            }
        }

        enum class ECheckpoint : ui32 {
            BROKEN_CHUNK_L0,
            BROKEN_INDEX_L0,
            BROKEN_CHUNK_L1_8,
            BROKEN_INDEX_L1_8,
            BROKEN_CHUNK_L17,
            BROKEN_INDEX_L17,
            BROKEN_INPLACE_BLOB,
            BROKEN_HUGE_BLOB,
        };

        const ui32 brokenChunks = 2;
        const ui32 brokenIndices = 2;
        const ui32 brokenBlobs = 10;

        std::map<ECheckpoint, std::pair<ui32, ui32>> checkpoints{
            {ECheckpoint::BROKEN_CHUNK_L0, {0, brokenChunks}},
            {ECheckpoint::BROKEN_INDEX_L0, {0, brokenIndices}},
            {ECheckpoint::BROKEN_CHUNK_L1_8, {0, brokenChunks}},
            {ECheckpoint::BROKEN_INDEX_L1_8, {0, brokenIndices}},
            {ECheckpoint::BROKEN_CHUNK_L17, {0, brokenChunks}},
            {ECheckpoint::BROKEN_INDEX_L17, {0, brokenIndices}},
            {ECheckpoint::BROKEN_INPLACE_BLOB, {0, brokenBlobs}},
            {ECheckpoint::BROKEN_HUGE_BLOB, {0, brokenBlobs}},
        };

        ui32 passedCheckpoints = 0; // from TEvScrubNotify

        for (ui32 iter = 0;; ++iter) {
            // wait for VDisks to start
            env.WaitForVDiskToGetRunning(vdiskId, vdiskActorId);

            Cerr << Endl;

            auto corrupt = [&](const TDiskPart& location, TString name) {
                Cerr << "*** iter# " << iter << " corrupting " << name << " location# " << location.ToString() << Endl;
                it->second->SetCorruptedArea(location.ChunkIdx, location.Offset, location.Offset + location.Size, true);
            };

            std::set<ui64> brokenSstIds;

            std::set<TLogoBlobID> blobIdsToValidate;

            auto pickIndexRecord = [&](ui32 i) {
                const std::vector<const TLayout::TLayoutRecord*>& recs = indexes[i];
                return recs[RandomNumber(recs.size())];
            };

            auto breakChunk = [&](ui32 i) {
                const auto& r = *pickIndexRecord(i);
                brokenSstIds.insert(r.SstId);
                const TDiskPart location(r.Location.ChunkIdx, 0, r.Location.Offset + r.Location.Size);
                corrupt(location, "chunk");
                for (const TLogoBlobID& id : blobsPerChunk[location.ChunkIdx]) {
                    const auto& blobLocation = locations.at(id);
                    if (location.Offset + location.Size > blobLocation.Offset && blobLocation.Offset + blobLocation.Size > location.Offset) {
                        blobIdsToValidate.insert(id);
                    }
                }
            };

            auto breakIndex = [&](ui32 i) {
                const auto& r = *pickIndexRecord(i);
                brokenSstIds.insert(r.SstId);
                corrupt(r.Location, "index");
                auto& v = blobsPerChunk[r.Location.ChunkIdx];
                blobIdsToValidate.insert(v.begin(), v.end());
            };

            auto pickBlob = [&](const auto& blobs) {
                std::vector<const TLayout::TLayoutRecord*> recs;
                for (ui64 sstId : brokenSstIds) {
                    recs.insert(recs.end(), inplaceBlobs[sstId].begin(), inplaceBlobs[sstId].end());
                }
                if (recs.empty()) {
                    for (const auto& [key, value] : blobs) {
                        recs.insert(recs.end(), value.begin(), value.end());
                    }
                }
                Y_ABORT_UNLESS(!recs.empty());
                return recs[RandomNumber(recs.size())];
            };

            auto breakInplaceBlob = [&] {
                const auto& r = *pickBlob(inplaceBlobs);
                corrupt(r.Location, TStringBuilder() << "inplace blob# " << r.BlobId.ToString());
                blobIdsToValidate.insert(r.BlobId);
            };

            auto breakHugeBlob = [&] {
                const auto& r = *pickBlob(hugeBlobs);
                corrupt(r.Location, TStringBuilder() << "huge blob# " << r.BlobId.ToString());
                blobIdsToValidate.insert(r.BlobId);
            };

            std::vector<ECheckpoint> v;
            for (const auto& [key, value] : checkpoints) {
                for (ui32 i = 0; i < value.second - value.first; ++i) {
                    v.push_back(key);
                }
            }
            for (ui32 i = 0; i < v.size(); ++i) {
                std::swap(v[i], v[i + RandomNumber(v.size() - i)]);
            }
            const ui32 num = 1 + RandomNumber(Min<ui32>(v.size(), 10));
            for (ui32 i = 0; i < num; ++i) {
                ++checkpoints[v[i]].first;
                switch (v[i]) {
                    case ECheckpoint::BROKEN_CHUNK_L0: breakChunk(0); break;
                    case ECheckpoint::BROKEN_INDEX_L0: breakIndex(0); break;
                    case ECheckpoint::BROKEN_CHUNK_L1_8: breakChunk(1); break;
                    case ECheckpoint::BROKEN_INDEX_L1_8: breakIndex(1); break;
                    case ECheckpoint::BROKEN_CHUNK_L17: breakChunk(2); break;
                    case ECheckpoint::BROKEN_INDEX_L17: breakIndex(2); break;
                    case ECheckpoint::BROKEN_INPLACE_BLOB: breakInplaceBlob(); break;
                    case ECheckpoint::BROKEN_HUGE_BLOB: breakHugeBlob(); break;
                }
            }

            Cerr << Endl;

            // wait for full scrub cycle to finish
            for (;;) {
                const ui32 nodeId = vdiskActorId.NodeId();
                const TActorId& edge = runtime->AllocateEdgeActor(nodeId);
                const ui64 cookie = RandomNumber<ui64>();
                runtime->Send(new IEventHandle(TEvBlobStorage::EvScrubAwait, 0, vdiskActorId, edge, nullptr, cookie), nodeId);
                auto ev = env.WaitForEdgeActorEvent<TEvScrubNotify>(edge);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
                passedCheckpoints |= ev->Get()->Checkpoints;
                if (ev->Get()->Success) {
                    break;
                }
            }

            // terminate peer disks
            for (ui32 i = 1; i < info->GetTotalVDisksNum(); ++i) {
                const TActorId& actorId = info->GetActorId(i);
                Cerr << "*** terminating peer disk# " << actorId.ToString() << Endl;
                runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0), actorId.NodeId());
            }

            Cerr << "*** blobIdsToValidate.size# " << blobIdsToValidate.size() << Endl;

            Validate(env, vdiskId, data, info->Type, blobIdsToValidate);

            env.Cleanup();
            env.Initialize();

            Validate(env, vdiskId, data, info->Type, blobIdsToValidate);

            Cerr << Endl;
            Cerr << "*** iter# " << iter << " checkpoints";
            for (const auto& [key, value] : checkpoints) {
                Cerr << " " << (ui32)key << ":" << value.first << "/" << value.second;
            }
            Cerr << Endl << Endl;

            bool done = passedCheckpoints == TEvScrubNotify::ALL;
            for (const auto& [key, value] : checkpoints) {
                const auto& [current, target] = value;
                if (current < target) {
                    done = false;
                }
            }
            if (done) {
                break;
            }

            // restore PDisk snapshot and restart the system
            env.Cleanup();
            it->second = snapshot->Snapshot();
            env.Initialize();
        }
    }

    Y_UNIT_TEST(mirror3of4) {
        ScrubTest(TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(mirror3dc) {
        ScrubTest(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(block42) {
        ScrubTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }

}
