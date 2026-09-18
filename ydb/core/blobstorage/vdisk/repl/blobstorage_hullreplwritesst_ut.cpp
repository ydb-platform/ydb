#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_hullreplwritesst.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

using namespace NKikimr;

std::shared_ptr<TReplCtx> CreateReplCtx(TVector<TVDiskID>& vdisks, const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
    for (const auto& vdisk : info->GetVDisks()) {
        vdisks.push_back(info->GetVDiskId(vdisk.OrderNumber));
    }
    auto baseInfo = TVDiskConfig::TBaseInfo::SampleForTests();
    baseInfo.VDiskIdShort = TVDiskIdShort(vdisks[0]);
    auto vdiskCfg = MakeIntrusive<TVDiskConfig>(baseInfo);
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
        nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
    auto hugeBlobCtx = std::make_shared<THugeBlobCtx>(nullptr, vctx->EffectiveAddHeader);
    auto dsk = MakeIntrusive<TPDiskParams>(ui8(1), 1u, 128u << 20, 4096u, 0u, 1000000000u, 1000000000u, 65536u, 65536u, 65536u,
            NPDisk::DEVICE_TYPE_UNKNOWN);
    auto pdiskCtx = std::make_shared<TPDiskCtx>(dsk, TActorId(), TString());
    auto replCtx = std::make_shared<TReplCtx>(
        vctx,
        nullptr,
        pdiskCtx,
        hugeBlobCtx,
        4097,
        nullptr,
        info,
        TActorId(),
        vdiskCfg,
        std::make_shared<std::atomic_uint64_t>());
    return replCtx;
}

TVDiskContextPtr CreateVDiskContext(const TBlobStorageGroupInfo& info) {
    return MakeIntrusive<TVDiskContext>(TActorId(), info.PickTopology(), new ::NMonitoring::TDynamicCounters(), TVDiskID(),
        nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
}

TIntrusivePtr<THullCtx> CreateHullCtx(const TBlobStorageGroupInfo& info, ui32 chunkSize, ui32 compWorthReadSize) {
    auto baseInfo = TVDiskConfig::TBaseInfo::SampleForTests();
    return MakeIntrusive<THullCtx>(CreateVDiskContext(info), MakeIntrusive<TVDiskConfig>(baseInfo), chunkSize, compWorthReadSize, true, true, true, true, 1u,
        1u, 2.0, 0.5, TDuration::Minutes(5), TDuration::Seconds(1), true);
}

TIntrusivePtr<THullDs> CreateHullDs(const TBlobStorageGroupInfo& info) {
    auto hullDs = MakeIntrusive<THullDs>(CreateHullCtx(info, 128 << 20, 256 * 1024));
    hullDs->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(TLevelIndexSettings(hullDs->HullCtx, 0, 0, 0, TDuration::Zero(),
        0, false, false), std::make_shared<TRopeArena>(TRopeArenaBackend::Allocate));
    return hullDs;
}

Y_UNIT_TEST_SUITE(HullReplWriteSst) {
    void RunSstWriter(TBlobStorageGroupType::EErasureSpecies species, ui32 maxRecords = 0,
            TErasureType::ECrcMode crc = TErasureType::CrcModeNone) {
        TVector<TVDiskID> vdisks;
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(species);
        auto replCtx = CreateReplCtx(vdisks, groupInfo);
        auto hullDs = CreateHullDs(*groupInfo);
        TReplSstStreamWriter writer(replCtx, hullDs);
        ui64 index = 1;
        ui32 chunkIdx = 1;
        std::deque<TLogoBlobID> checkQ;
        std::map<TLogoBlobID, std::pair<NMatrix::TVectorType, TRope>> expected;
        ui32 seenParts = 0;
        ui32 commits = 0;
        TReallyFastRng32 rng(0x82557);
        auto random = [&](ui32 bound) { return maxRecords ? rng() % bound : RandomNumber(bound); };
        std::map<ui32, TString> chunks;
        TRopeArena arena(TRopeArenaBackend::Allocate);
        std::deque<std::unique_ptr<NPDisk::TEvChunkWrite>> writeMsgs;
        auto handleWrite = [&](NPDisk::TEvChunkWrite& m) {
            TString& data = chunks[m.ChunkIdx];
            if (!data) {
                data = TString::Uninitialized(hullDs->HullCtx->ChunkSize);
            }
            ui32 offset = m.Offset;
            for (ui32 i = 0; i < m.PartsPtr->Size(); ++i) {
                const auto& [ptr, size] = (*m.PartsPtr)[i];
                Y_ABORT_UNLESS(offset + size <= data.size());
                if (ptr) {
                    memcpy(data.Detach() + offset, ptr, size);
                } else {
                    memset(data.Detach() + offset, 0, size);
                }
                offset += size;
            }
            NPDisk::TEvChunkWriteResult res(NKikimrProto::OK, m.ChunkIdx, m.PartsPtr, m.Cookie, {}, {});
            writer.Apply(&res);
        };
        auto read = [&](const TDiskPart& p) {
            const auto it = chunks.find(p.ChunkIdx);
            Y_ABORT_UNLESS(it != chunks.end());
            const TString& s = it->second;
            return s.substr(p.Offset, p.Size);
        };
        for (THPTimer timer; maxRecords ? !commits : TDuration::Seconds(timer.Passed()) < TDuration::Minutes(5); ) {
            if (!writeMsgs.empty() && random(1000u) == 999) {
                const size_t index = random(writeMsgs.size());
                handleWrite(*writeMsgs[index]);
                writeMsgs.erase(writeMsgs.begin() + index);
            }
            switch (writer.GetState()) {
                case TReplSstStreamWriter::EState::STOPPED:
                    writer.Begin();
                    break;

                case TReplSstStreamWriter::EState::COLLECT: {
                    if (maxRecords && index > maxRecords) {
                        writer.Finish();
                        break;
                    }
                    // generate random size for this blob
                    const ui32 size = 1 + random(256u);
                    TString data = TString::Uninitialized(size);
                    memset(data.Detach(), '*', size);

                    // generate blob id
                    TLogoBlobID id(index++, 1, 1, 0, size, 0, 0, crc);
                    TDataPartSet encoded;
                    groupInfo->Type.SplitData(crc, data, encoded);

                    // find possible local part for current vdisk
                    TRope rope;
                    NMatrix::TVectorType vec(0, groupInfo->GetTopology().GType.TotalPartCount());
                    for (ui32 partIdx = 0; partIdx < vec.GetSize(); ++partIdx) {
                        if (TIngress::CreateIngressWithLocal(&groupInfo->GetTopology(), replCtx->VCtx->ShortSelfVDisk,
                                TLogoBlobID(id, partIdx + 1))) {
                            vec.Set(partIdx);
                            rope = TDiskBlob::Create(size, partIdx + 1, vec.GetSize(),
                                TRope(encoded.Parts[partIdx].OwnedString), arena, replCtx->GetAddHeader());
                            seenParts |= 1u << partIdx;
                            break;
                        }
                    }

                    // write it
                    TReplSstStreamWriter::TRecoveredBlobInfo info(id, std::move(rope), false, vec);
                    const TRope originalRecord = info.Data;
                    const bool success = writer.AddRecoveredBlob(info);
                    if (success) {
                        checkQ.push_back(info.Id);
                        if (maxRecords) {
                            expected.emplace(info.Id, std::make_pair(vec, originalRecord));
                        }
                    }
                    break;
                }

                case TReplSstStreamWriter::EState::PDISK_MESSAGE_PENDING: {
                    auto msg = writer.GetPendingPDiskMsg();
                    Y_ABORT_UNLESS(msg);
                    if (auto *x = dynamic_cast<NPDisk::TEvChunkReserve*>(msg.get())) {
                        NPDisk::TEvChunkReserveResult res(NKikimrProto::OK, {});
                        for (ui32 i = 0; i < x->SizeChunks; ++i) {
                            res.ChunkIds.push_back(chunkIdx++);
                        }
                        writer.Apply(&res);
                    } else if (auto *x = dynamic_cast<NPDisk::TEvChunkWrite*>(msg.get())) {
                        if (random(100u) >= 95) {
                            handleWrite(*x);
                        } else {
                            writeMsgs.emplace_back(static_cast<NPDisk::TEvChunkWrite*>(msg.release()));
                        }
                    } else {
                        Y_ABORT();
                    }
                    break;
                }

                case TReplSstStreamWriter::EState::NOT_READY: {
                    Y_ABORT_UNLESS(!writeMsgs.empty());
                    const size_t index = random(writeMsgs.size());
                    handleWrite(*writeMsgs[index]);
                    writeMsgs.erase(writeMsgs.begin() + index);
                    break;
                }

                case TReplSstStreamWriter::EState::COMMIT_PENDING: {
                    auto msg = writer.GetPendingCommitMsg();
                    Cerr << "commit" << Endl;
                    for (const ui32 chunk : msg->ChunksToCommit) {
                        Cerr << "chunk# " << chunk << Endl;
                    }
                    for (const auto& addition : msg->Essence.LogoBlobsAdditions) {
                        Cerr << addition.Sst->LastPartAddr.ToString() << Endl;
                        TString data = read(addition.Sst->LastPartAddr);
                        TIdxDiskPlaceHolder ph = *(reinterpret_cast<const TIdxDiskPlaceHolder*>(data.data() + data.size()) - 1);
                        UNIT_ASSERT_VALUES_EQUAL(ph.MagicNumber, ui32(TIdxDiskPlaceHolder::Signature));
                        data.resize(data.size() - sizeof(ph));
                        for (TDiskPart link = ph.PrevPart; link.ChunkIdx; ) {
                            TString chain = read(link);
                            auto *x = reinterpret_cast<const TIdxDiskLinker*>(chain.data() + chain.size()) - 1;
                            link = x->PrevPart;
                            data = chain + data;
                        }
                        const char *ptr = data.data();
                        const char *end = ptr + data.size();
                        ui32 numItems = 0;
                        while (ptr < end) {
                            auto *key = reinterpret_cast<const TKeyLogoBlob*>(ptr);
                            auto *memrec = reinterpret_cast<const TMemRecLogoBlob*>(key + 1);
                            ptr = reinterpret_cast<const char*>(memrec + 1);
                            UNIT_ASSERT(ptr <= end);
                            UNIT_ASSERT(!checkQ.empty());
                            UNIT_ASSERT_VALUES_EQUAL(key->LogoBlobID(), checkQ.front());
                            checkQ.pop_front();
                            if (maxRecords) {
                                const auto it = expected.find(key->LogoBlobID());
                                UNIT_ASSERT(it != expected.end());
                                UNIT_ASSERT_EQUAL(memrec->GetLocalParts(groupInfo->Type), it->second.first);
                                TDiskDataExtractor extractor;
                                memrec->GetDiskData(&extractor, nullptr);
                                const TDiskPart& location = extractor.SwearOne();
                                UNIT_ASSERT_VALUES_EQUAL(location.Size, TDiskBlob::CalculateBlobSize(
                                    groupInfo->Type, key->LogoBlobID(), it->second.first, false));
                                UNIT_ASSERT_EQUAL(TRope(read(location)), it->second.second);
                                expected.erase(it);
                            }
                            ++numItems;
                        }
                        Cerr << numItems << Endl;
                        UNIT_ASSERT_VALUES_EQUAL(numItems, ph.Info.ItemsWithInplacedData);
                        UNIT_ASSERT_VALUES_EQUAL(0, ph.Info.ItemsWithHugeData);
                    }
                    UNIT_ASSERT(checkQ.empty());
                    for (const ui32 chunk : msg->ChunksToCommit) {
                        const ui32 num = chunks.erase(chunk);
                        UNIT_ASSERT_VALUES_EQUAL(num, 1);
                    }
                    writer.ApplyCommit();
                    ++commits;
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
        if (maxRecords) {
            UNIT_ASSERT(expected.empty());
            UNIT_ASSERT_VALUES_EQUAL(seenParts, (1u << groupInfo->Type.TotalPartCount()) - 1);
            UNIT_ASSERT(!replCtx->GetAddHeader());
            UNIT_ASSERT(!hullDs->HullCtx->AddHeader);
        }
    }

    Y_UNIT_TEST(Basic) {
        RunSstWriter(TBlobStorageGroupType::ErasureMirror3);
    }

    Y_UNIT_TEST(HeaderlessBlock82) {
        RunSstWriter(TBlobStorageGroupType::Erasure8Plus2Block, 512);
    }

    Y_UNIT_TEST(HeaderlessWholePartCrcBlock82) {
        RunSstWriter(TBlobStorageGroupType::Erasure8Plus2Block, 512, TErasureType::CrcModeWholePart);
    }
}
