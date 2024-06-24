#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <library/cpp/digest/crc32c/crc32c.h>

Y_UNIT_TEST_SUITE(BlobPatching) {

    void SendPut(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPut>(blobId, data, TInstant::Max());

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});

        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvPutResult);
        TEvBlobStorage::TEvPutResult *putResult = handle->Get<TEvBlobStorage::TEvPutResult>();
        UNIT_ASSERT_EQUAL(putResult->Status, status);
    };

    void SendGet(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> getQueries{new TEvBlobStorage::TEvGet::TQuery[1]};
        getQueries[0].Id = blobId;
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvGet>(getQueries, 1, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead);
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Buffer.ConvertToString(), data);
    };

    void SendPatch(const TTestInfo &test, const TLogoBlobID &originalBlobId, const TLogoBlobID &patchedBlobId, ui32 mask,
            const TVector<TEvBlobStorage::TEvPatch::TDiff> &diffs, NKikimrProto::EReplyStatus status)
    {
        TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffArr{new TEvBlobStorage::TEvPatch::TDiff[diffs.size()]};
        for (ui32 idx = 0; idx < diffs.size(); ++idx) {
            diffArr[idx] = diffs[idx];
        }
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPatch>(test.Info->GroupID.GetRawId(), originalBlobId, patchedBlobId,
                mask, std::move(diffArr), diffs.size(), TInstant::Max());
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvPatchResult);
        TEvBlobStorage::TEvPatchResult *patchResult = handle->Get<TEvBlobStorage::TEvPatchResult>();
        UNIT_ASSERT_EQUAL(patchResult->Status, status);
    };

    TString ApplyDiffs(const TString &source, const TVector<TEvBlobStorage::TEvPatch::TDiff> &diffs) {
        TString result = TString::Uninitialized(source.size());
        Copy(source.begin(), source.end(), result.begin());
        for (auto &diff : diffs) {
            Copy(diff.Buffer.begin(), diff.Buffer.end(), result.begin() + diff.Offset);
        }
        return result;
    };

    void MakePatchingTest(TString erasure) {
        TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);
        std::unique_ptr<IEventHandle> handle;

        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        SendGet(test, originalBlobId, data, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs1(1);
        diffs1[0].Set("b", 0);
        TString patchedData1 = ApplyDiffs(data, diffs1);
        TLogoBlobID patchedBlobId1(1, 1, 1, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId1, 0,
                test.Info->GroupID.GetRawId(), test.Info->GroupID.GetRawId());
        SendPatch(test, originalBlobId, patchedBlobId1, 0, diffs1, NKikimrProto::OK);
        SendGet(test, patchedBlobId1, patchedData1, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs2(2);
        diffs2[0].Set("b", 0);
        diffs2[1].Set("b", 99);
        TString patchedData2 = ApplyDiffs(data, diffs2);
        TLogoBlobID patchedBlobId2(1, 1, 2, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId2, 0,
                test.Info->GroupID.GetRawId(), test.Info->GroupID.GetRawId());
        SendPatch(test, originalBlobId, patchedBlobId2, 0, diffs2, NKikimrProto::OK);
        SendGet(test, patchedBlobId2, patchedData2, NKikimrProto::OK);

        TLogoBlobID patchedBlobId3(1, 1, 3, 0, size, 0);
        TLogoBlobID truePatchedBlobId3(1, 1, 3, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &truePatchedBlobId3, TLogoBlobID::MaxCookie,
                test.Info->GroupID.GetRawId(), test.Info->GroupID.GetRawId());
        UNIT_ASSERT(patchedBlobId3 != truePatchedBlobId3);
        NKikimrProto::EReplyStatus statusWhenNotMatchingCookie = (erasure == "block-4-2" ? NKikimrProto::ERROR : NKikimrProto::OK);
        SendPatch(test, originalBlobId, patchedBlobId3, TLogoBlobID::MaxCookie, diffs2, statusWhenNotMatchingCookie);
        SendPatch(test, originalBlobId, truePatchedBlobId3, TLogoBlobID::MaxCookie, diffs2, NKikimrProto::OK);
        SendGet(test, truePatchedBlobId3, patchedData2, NKikimrProto::OK);
    }

    void MakeStressPatchingTest(TString erasure) {
        TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);
        std::unique_ptr<IEventHandle> handle;

        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        SendGet(test, originalBlobId, data, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs(50);
        for (ui32 idx = 0; idx < diffs.size(); ++idx) {
            diffs[idx].Set("b", idx * 2);
        }
        TString patchedData = ApplyDiffs(data, diffs);

        constexpr ui32 patchCount = 1'000;
        for (ui32 patchIdx = 0; patchIdx < patchCount; ++patchIdx) {
            TLogoBlobID patchedBlobId(1, 1, patchIdx + 1, 0, size, 0);
            TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId, TLogoBlobID::MaxCookie,
                    test.Info->GroupID.GetRawId(), test.Info->GroupID.GetRawId());
            SendPatch(test, originalBlobId, patchedBlobId, 0, diffs, NKikimrProto::OK);
            SendGet(test, patchedBlobId, patchedData, NKikimrProto::OK);
        }
    }


    Y_UNIT_TEST(Mirror3of4) {
        MakePatchingTest("mirror-3of4");
    }

    Y_UNIT_TEST(Mirror3dc) {
        MakePatchingTest("mirror-3-dc");
    }

    Y_UNIT_TEST(Mirror3) {
        MakePatchingTest("mirror-3");
    }

    Y_UNIT_TEST(Block42) {
        MakePatchingTest("block-4-2");
    }

    Y_UNIT_TEST(None) {
        MakePatchingTest("none");
    }


    Y_UNIT_TEST(StressMirror3of4) {
        MakeStressPatchingTest("mirror-3of4");
    }

    Y_UNIT_TEST(StressMirror3dc) {
        MakeStressPatchingTest("mirror-3-dc");
    }

    Y_UNIT_TEST(StressMirror3) {
        MakeStressPatchingTest("mirror-3");
    }

    Y_UNIT_TEST(StressBlock42) {
        MakeStressPatchingTest("block-4-2");
    }

    Y_UNIT_TEST(StressNone) {
        MakeStressPatchingTest("none");
    }

    Y_UNIT_TEST(DiffsWithIncorectPatchedBlobPartId) {
        TEnvironmentSetup env(true);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        env.CommenceReplication();
        auto groups = env.GetGroups();
        auto info = env.GetGroupInfo(groups[0]);
        constexpr ui32 size = 100;
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);

        env.WithQueueId(info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob, [&](TActorId queueId) {
            TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId());
            constexpr ui32 diffCount = 100;
            for (ui32 diffIdx = 0; diffIdx < diffCount; ++diffIdx) {
                TLogoBlobID patchedBlobId(1, 1, diffIdx + 1, 0, size, 0);
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> ev = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(
                        originalBlobId, patchedBlobId, info->GetVDiskId(0), 0, TInstant::Max(), 0);
                runtime->Send(new IEventHandle(queueId, edge, ev.release()), queueId.NodeId());
            }
            for (ui32 diffIdx = 0; diffIdx < diffCount; ++diffIdx) {
                auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPatchResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetStatus(), NKikimrProto::ERROR);
            }
        });
    }

    Y_UNIT_TEST(PatchBlock42) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        }};
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(1, 2);
        env.Sim(TDuration::Seconds(60));

        std::vector<ui32> groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

        std::unordered_set<ui32> stoppedNodes;
        std::unordered_set<ui32> runningNodes;
        std::vector<TLogoBlobID> putBlobs;
        std::unordered_map<TLogoBlobID, ui32> digest;
        ui64 tabletId = 1'000'000'000;
        ui32 generation = 1;
        ui32 step = 1;
        ui8 channel = 5;

        ui32 numPatchedBlobs = 0;

        for (ui32 nodeId : runtime->GetNodes()) {
            if (nodeId != 1) {
                runningNodes.insert(nodeId);
            }
        }

        auto stopNode = [&] {
            UNIT_ASSERT(runningNodes.size());
            auto it = runningNodes.begin();
            std::advance(it, RandomNumber(runningNodes.size()));
            env.StopNode(*it);
            Cerr << "*** stopped node " << *it << Endl;
            stoppedNodes.insert(runningNodes.extract(it));
        };

        auto startNode = [&] {
            UNIT_ASSERT(stoppedNodes.size());
            auto it = stoppedNodes.begin();
            std::advance(it, RandomNumber(stoppedNodes.size()));
            env.StartNode(*it);
            Cerr << "*** started node " << *it << Endl;
            runningNodes.insert(stoppedNodes.extract(it));
        };

        auto sim = [&](TActorId edgeId) {
            std::unique_ptr<IEventHandle> res;
            auto *edge = dynamic_cast<TTestActorSystem::TEdgeActor*>(runtime->GetActor(edgeId));
            Y_VERIFY(edge);
            edge->WaitForEvent(&res);

            bool stopped = false;
            bool started = false;

            while (!res) {
                bool iteration = true;
                runtime->Sim([&] { return std::exchange(iteration, false); });
                if (stoppedNodes.size() && RandomNumber(1000u) == 0 && !started) {
                    startNode();
                    started = true;
                } else if (stoppedNodes.size() < 2 && RandomNumber(1000u) == 0 && !stopped) {
                    stopNode();
                    stopped = true;
                }
            }

            edge->StopWaitingForEvent();
            return res;
        };

        auto dumpContent = [&](std::string_view data) -> TString {
            TStringStream s;
            s << '[';

            const ui32 step = 4096;
            for (ui32 offset = 0; offset < data.size(); offset += step) {
                auto fragment = data.substr(offset, step);
                if (offset) {
                    s << ' ';
                }
                s << Sprintf("%08" PRIx32, Crc32c(fragment.data(), fragment.size()));
            }
            s << ']';
            return s.Str();
        };

        auto putBlob = [&] {
            const TActorId sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            ui32 count = 1 + RandomNumber(3u);
            std::unordered_map<TLogoBlobID, TString> content;
            for (ui32 n = 0; n < count; ++n) {
                const ui32 size = 16384;
                const TLogoBlobID id(tabletId, generation, step, channel, size, 0);
                ++step;
                TString data = FastGenDataForLZ4(size, id.Hash());
                content[id] = dumpContent(data);
                digest.emplace(id, Crc32c(data.data(), data.size()));
                runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groups[0], new TEvBlobStorage::TEvPut(id, TRcBuf(data), TInstant::Max()));
                });
            }
            for (ui32 n = 0; n < count; ++n) {
                auto res = sim(sender);
                auto *m = res->Get<TEvBlobStorage::TEvPutResult>();
                if (m->Status == NKikimrProto::OK) {
                    Cerr << "*** put blob Id# " << m->Id << " content# " << content[m->Id] << Endl;
                    putBlobs.push_back(m->Id);
                }
            }
            runtime->DestroyActor(sender);
        };

        auto restoreBlob = [&] {
            const TLogoBlobID blobId = putBlobs[RandomNumber(putBlobs.size())];
            const TActorId sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groups[0], new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
            if (res->Get()->Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
                Cerr << "*** restore blob Id# " << blobId << Endl;
            }
        };

        auto patchBlob = [&] {
            const TLogoBlobID originalId = putBlobs[RandomNumber(putBlobs.size())];
            const ui32 targetGroup = groups[RandomNumber(groups.size())];
            const TActorId sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            TLogoBlobID patchedId(tabletId, generation, step, channel, originalId.BlobSize(), 0);
            ++step;
            const bool success = TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalId, &patchedId, 0xff,
                groups[0], targetGroup);
            UNIT_ASSERT(targetGroup != groups[0] || success);
            using TDiff = TEvBlobStorage::TEvPatch::TDiff;
            std::vector<TDiff> diffs;

            TString data;
            {
                const TActorId sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
                runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groups[0], new TEvBlobStorage::TEvGet(originalId, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
                if (res->Get()->Status != NKikimrProto::OK) {
                    return;
                }
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
                data = res->Get()->Responses[0].Buffer.ConvertToString();
                Cerr << "*** got " << originalId << " content# " << dumpContent(data) << Endl;
                UNIT_ASSERT_VALUES_EQUAL(data.size(), originalId.BlobSize());
                UNIT_ASSERT(digest.contains(originalId));
                UNIT_ASSERT_VALUES_EQUAL(Crc32c(data.data(), data.size()), digest[originalId]);
            }

            for (ui32 offset = 0; offset < originalId.BlobSize(); offset += 4096) {
                if (RandomNumber(2u) || (offset + 4096 == originalId.BlobSize() && diffs.empty())) {
                    TString chunk = FastGenDataForLZ4(4096, patchedId.Hash());
                    TDiff diff;
                    diff.Set(TRcBuf(chunk), offset);
                    diffs.push_back(diff);
                    memcpy(data.Detach() + offset, chunk.data(), chunk.size());
                }
            }
            digest.emplace(patchedId, Crc32c(data.data(), data.size()));
            runtime->WrapInActorContext(sender, [&] {
                TArrayHolder<TDiff> ptr(new TDiff[diffs.size()]);
                std::copy(diffs.begin(), diffs.end(), ptr.Get());
                SendToBSProxy(sender, targetGroup, new TEvBlobStorage::TEvPatch(groups[0], originalId, patchedId,
                    0xff, std::move(ptr), diffs.size(), TInstant::Max()));
            });
            auto res = sim(sender);
            if (res->Get<TEvBlobStorage::TEvPatchResult>()->Status == NKikimrProto::OK) {
                Cerr << "*** patched OriginalId# " << originalId << " to PatchedId# " << patchedId <<
                    " content# " << dumpContent(data) << Endl;
                ++numPatchedBlobs;
                if (targetGroup == groups[0]) {
                    putBlobs.push_back(patchedId);
                }
            }
        };

        while (putBlobs.size() < 1000 && numPatchedBlobs < 10000) {
            const ui32 canStop = stoppedNodes.size() < 2 ? 10 : 0;
            const ui32 canStart = stoppedNodes.size() ? 10 : 0;
            const ui32 canPut = putBlobs.size() < 1000 ? 100 : 0;
            const ui32 canRestore = putBlobs.size() ? 50 : 0;
            const ui32 canPatch = putBlobs.size() ? 2000 : 0;
            const ui32 canWait = 100;
            i32 w = RandomNumber(canStop + canStart + canPut + canPatch + canWait);
            if ((w -= canStop) < 0) {
                stopNode();
                env.Sim(TDuration::Seconds(5));
            } else if ((w -= canStart) < 0) {
                startNode();
                env.Sim(TDuration::Seconds(5));
            } else if ((w -= canPut) < 0) {
                putBlob();
            } else if ((w -= canRestore) < 0) {
                restoreBlob();
            } else if ((w -= canPatch) < 0) {
                patchBlob();
            } else if ((w -= canWait) < 0) {
                env.Sim(TDuration::Seconds(20));
            } else {
                UNIT_FAIL("unexpected scenario");
            }
        }

        for (const ui32 nodeId : stoppedNodes) {
            env.StartNode(nodeId);
        }

        auto info = env.GetGroupInfo(groups[0]);
        std::vector<TActorId> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            queues.push_back(env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000));
        }

        const TActorId sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        auto checkBlob = [&](TLogoBlobID id) {
            Cerr << "*** checking blob " << id << Endl;

            std::vector<TString> parts(info->Type.TotalPartCount());
            ui32 mask = 0;
            TSubgroupPartLayout layout;
            ui32 writtenParts = 0;

            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID& vdiskId = info->GetVDiskId(i);
                runtime->Send(new IEventHandle(queues[i], sender, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId,
                    TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                    Nothing(), {{id}}).release()), sender.NodeId());

                auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, false);
                auto& record = r->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                for (auto& res : record.GetResult()) {
                    if (res.GetStatus() == NKikimrProto::OK) {
                        TString buffer = r->Get()->GetBlobData(res).ConvertToString();
                        const TLogoBlobID id(LogoBlobIDFromLogoBlobID(res.GetBlobID()));
                        const ui32 partIdx = id.PartId() - 1;
                        if (parts[partIdx]) {
                            UNIT_ASSERT_VALUES_EQUAL(parts[partIdx], buffer);
                        }
                        parts[partIdx] = std::move(buffer);
                        layout.AddItem(info->GetIdxInSubgroup(vdiskId, id.Hash()), partIdx, info->Type);
                        mask |= 1 << partIdx;
                        ++writtenParts;
                    }
                }
            }

            Cerr << "    writtenParts# " << writtenParts << Endl;

            TDataPartSet ps;
            ps.FullDataSize = id.BlobSize();
            ps.PartsMask = mask;
            ps.Parts.resize(parts.size());
            for (ui32 i = 0; i < parts.size(); ++i) {
                if (mask >> i & 1) {
                    ps.Parts[i].ResetToWhole(TRope(parts[i]));
                }
            }

            TRope outBuffer;
            info->Type.RestoreData(TErasureType::CrcModeNone, ps, outBuffer, false, true, false);
            UNIT_ASSERT(digest.contains(id));
            UNIT_ASSERT_VALUES_EQUAL(outBuffer.size(), id.BlobSize());
            TString s = outBuffer.ConvertToString();
            UNIT_ASSERT_VALUES_EQUAL(Crc32c(s.data(), s.size()), digest[id]);

            ps = {};
            info->Type.SplitData(TErasureType::CrcModeNone, outBuffer, ps);
            for (ui32 i = 0; i < parts.size(); ++i) {
                if (mask >> i & 1) {
                    auto& part = ps.Parts[i];
                    UNIT_ASSERT_VALUES_EQUAL(parts[i], TStringBuf(part.Bytes, part.Size));
                }
            }

            UNIT_ASSERT_EQUAL(info->GetQuorumChecker().GetBlobState(layout, {&info->GetTopology()}), TBlobStorageGroupInfo::EBS_FULL);
        };

        for (const TLogoBlobID& id : putBlobs) {
            checkBlob(id);
        }
    }
}

