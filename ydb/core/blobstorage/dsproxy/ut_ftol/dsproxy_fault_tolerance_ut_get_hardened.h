#pragma once

#include "defs.h"
#include "dsproxy_fault_tolerance_ut_base.h"

namespace NKikimr {

class TGetHardenedFaultToleranceTest : public TFaultToleranceTestBase<TGetHardenedFaultToleranceTest> {
    ui32 Index = 1;
    TString Data = "hello, world!";
    struct TBlobInfo {
        TBlobStorageGroupInfo::TGroupVDisks DisksWrittenTo;
        TSubgroupPartLayout Layout;
        bool AlmostWritten;
        bool Unwritten;
    };
    std::map<TLogoBlobID, TBlobInfo> BlobsWritten;

public:
    using TFaultToleranceTestBase::TFaultToleranceTestBase;

    void RunTestAction() {
        const bool wide = Info->Type.GetErasure() == TBlobStorageGroupType::Erasure8Plus2Block;
        if (wide) {
            Data = TString::Uninitialized(4097);
            char* bytes = Data.Detach();
            for (ui32 i = 0; i < Data.size(); ++i) {
                bytes[i] = char((i * 137 + i / 251) % 256);
            }
        }
        WriteParts();

        enum EState {
            ST_OK,
            ST_ERR,
            ST_LOST,
        };

        ui32 blobsWrittenFull = 0;
        ui32 blobsWrittenAlmostFull = 0;
        ui32 blobsUnwritten = 0;
        for (const auto& [id, blobInfo] : BlobsWritten) {
            if (blobInfo.Unwritten) {
                blobsUnwritten++;
            } else {
                ++(blobInfo.AlmostWritten ? blobsWrittenAlmostFull : blobsWrittenFull);
            }
        }

        const ui32 numDisks = Info->GetTotalVDisksNum();
        std::vector<std::vector<EState>> wideStates;
        if (wide) {
            // 1 healthy + 24 single + 264 double error/lost combinations, and representative triples.
            wideStates.emplace_back(numDisks, ST_OK);
            for (ui32 first = 0; first < numDisks; ++first) {
                for (EState a : {ST_ERR, ST_LOST}) {
                    auto single = std::vector<EState>(numDisks, ST_OK);
                    single[first] = a;
                    wideStates.push_back(single);
                    for (ui32 second = 0; second < first; ++second) {
                        for (EState b : {ST_ERR, ST_LOST}) {
                            auto pair = single;
                            pair[second] = b;
                            wideStates.push_back(std::move(pair));
                        }
                    }
                }
            }
            for (ui32 mask : {0x007u, 0x103u, 0x301u, 0xc01u, 0xd00u}) {
                for (EState state : {ST_ERR, ST_LOST}) {
                    auto triple = std::vector<EState>(numDisks, ST_OK);
                    for (ui32 i = 0; i < numDisks; ++i) {
                        if (mask >> i & 1) {
                            triple[i] = state;
                        }
                    }
                    wideStates.push_back(std::move(triple));
                }
            }
        }
        ui32 nIter = 0;
        for (const auto& [id, blobInfo] : BlobsWritten) {
            if (nIter % TestPartCount != TestPartIdx) {
                nIter++;
                continue;
            }
            Cerr << "iteration# " << nIter++ << " BlobsWritten# " << BlobsWritten.size()
                << " blobsWrittenFull# " << blobsWrittenFull << " blobsWrittenAlmostFull# " << blobsWrittenAlmostFull
                << " blobsUnwritten# " << blobsUnwritten
                << Endl;

            std::vector<EState> states(numDisks, ST_OK);
            size_t wideStateIndex = 0;
            for (;;) {
                // send queries
                ui32 responsesPending = 0;
                for (ui32 i = 0; i < states.size(); ++i) {
                    const EState state = states[i];
                    Send(Info->GetActorId(i), TEvVMockCtlRequest::CreateErrorModeRequest(state == ST_ERR, state == ST_LOST));
                    ++responsesPending;
                }
                while (responsesPending--) {
                    WaitForSpecificEvent<TEvVMockCtlResponse>(&TGetHardenedFaultToleranceTest::ProcessUnexpectedEvent);
                }

                // check whether the group fits the fail model
                TBlobStorageGroupInfo::TGroupVDisks failedDisks(&Info->GetTopology());
                TBlobStorageGroupInfo::TGroupVDisks lostDisks(&Info->GetTopology());
                bool isErrorDisksPresent = false;
                for (ui32 diskIdx = 0; diskIdx < states.size(); ++diskIdx) {
                    if (states[diskIdx] != ST_OK) {
                        failedDisks |= {&Info->GetTopology(), Info->GetVDiskId(diskIdx)};
                        if (states[diskIdx] == ST_ERR) {
                            isErrorDisksPresent = true;
                        }
                    }
                    if (states[diskIdx] == ST_LOST) {
                        lostDisks |= {&Info->GetTopology(), Info->GetVDiskId(diskIdx)};
                    }
                }
                const bool fitsFailModel = Info->GetQuorumChecker().CheckFailModelForGroup(failedDisks);

                TString data;
                const NKikimrProto::EReplyStatus status = GetBlob(id, false, &data,
                        blobInfo.AlmostWritten || blobInfo.Unwritten);
                bool failure = false;
                if (status == NKikimrProto::OK) {
                    if (!blobInfo.AlmostWritten && !blobInfo.Unwritten) {
                        UNIT_ASSERT_VALUES_EQUAL(data, Data);
                    }
                } else if (fitsFailModel && !blobInfo.AlmostWritten && !blobInfo.Unwritten) {
                    failure = true;
                } else if (status == NKikimrProto::NODATA) {
                    bool restorable = false;
                    // see if there is any chance to restore this blob?
                    TBlobStorageGroupInfo::TOrderNums orderNums;
                    Info->GetTopology().PickSubgroup(id.Hash(), orderNums);
                    TSubgroupPartLayout layout(blobInfo.Layout);
                    for (ui32 idxInSubgroup = 0; idxInSubgroup < Info->Type.BlobSubgroupSize(); ++idxInSubgroup) {
                        if (states[orderNums[idxInSubgroup]] == ST_LOST) {
                            for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
                                layout.ClearItem(idxInSubgroup, partIdx, Info->Type);
                            }
                        }
                    }
                    switch (Info->Type.GetErasure()) {
                        case TBlobStorageGroupType::ErasureMirror3dc:
                            if (layout.GetDisksWithPart(0) || layout.GetDisksWithPart(1) || layout.GetDisksWithPart(2)) {
                                restorable = true;
                            }
                            break;

                        case TBlobStorageGroupType::ErasureMirror3of4:
                            if (layout.GetDisksWithPart(0) || layout.GetDisksWithPart(1)) {
                                restorable = true;
                            }
                            break;

                        default: {
                            ui32 numDistinctParts = 0;
                            for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
                                if (layout.GetDisksWithPart(partIdx)) {
                                    ++numDistinctParts;
                                }
                            }
                            if (numDistinctParts >= Info->Type.MinimalRestorablePartCount()) {
                                restorable = true;
                            }
                            break;
                        }
                    }
                    if (!blobInfo.AlmostWritten && !blobInfo.Unwritten) {
                        // it's okay if proxy returns DATA for fully written blob with excessive failures, but it
                        // must reply with ERROR in phantom check mode
                        if (Info->GetQuorumChecker().CheckFailModelForGroup(lostDisks)) {
                            failure = true;
                            UNIT_ASSERT(restorable);
                        } else if (const NKikimrProto::EReplyStatus status = GetBlob(id, false, &data, true); status == NKikimrProto::NODATA) {
                            // Cerr << "PhantomCheck mode status# " << NKikimrProto::EReplyStatus_Name(status) << Endl;
                            failure = restorable;
                        }
                    } else {
                        failure = restorable;
                    }
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::ERROR);
                    if (blobInfo.Unwritten && !isErrorDisksPresent) {
                        failure = true;
                    }
                }
                if (failure) {
                    static char letters[] = {'O', 'E', 'L'};

                    TStringBuilder state;
                    for (ui32 i = 0; i < states.size(); ++i) {
                        state << letters[states[states.size() - i - 1]];
                    }

                    TStringBuilder subgroupState;
                    TBlobStorageGroupInfo::TOrderNums orderNums;
                    Info->GetTopology().PickSubgroup(id.Hash(), orderNums);
                    for (ui32 i = 0; i < states.size(); ++i) {
                        subgroupState << letters[states[orderNums[states.size() - i - 1]]];
                    }

                    TStringBuilder reducedLayout;
                    reducedLayout << "{";
                    for (ui32 diskIdx = 0; diskIdx < Info->Type.BlobSubgroupSize(); ++diskIdx) {
                        if (diskIdx) {
                            reducedLayout << " ";
                        }
                        for (ui32 partIdx = Info->Type.TotalPartCount(); partIdx--; ) {
                            const ui32 orderNum = orderNums[diskIdx];
                            const EState state = states[orderNum];
                            const bool written = blobInfo.Layout.GetDisksWithPart(partIdx) >> diskIdx & 1;
                            reducedLayout << (
                                state == ST_OK ? (written ? "1" : "0") :
                                state == ST_ERR ? (written ? "E" : "e") :
                                state == ST_LOST ? (written ? "L" : "0") : "?"
                            );
                        }
                    }
                    reducedLayout << "}";

                    Cerr << "state# " << state
                        << " writtenTo# " << blobInfo.DisksWrittenTo.ToString()
                        << " subgroupState# " << subgroupState
                        << " subgroupLayout# " << blobInfo.Layout.ToString(Info->Type)
                        << " reducedLayout# " << reducedLayout
                        << " almostWritten# " << (blobInfo.AlmostWritten ? "Y" : "N")
                        << " unritten# " << (blobInfo.Unwritten ? "Y" : "N")
                        << " fitsFailModel# " << (fitsFailModel ? "Y" : "N")
                        << " status# " << NKikimrProto::EReplyStatus_Name(status)
                        << Endl;
                    UNIT_ASSERT(false);
                }

                // The wide corpus is deterministic and bounded; do not enumerate 3^12 states per layout.
                if (wide) {
                    if (++wideStateIndex == wideStates.size()) {
                        break;
                    }
                    states = wideStates[wideStateIndex];
                    continue;
                }
                // advance to next option
                bool carry = true;
                for (ui32 i = 0; carry && i < numDisks; ++i) {
                    switch (states[i]) {
                        case ST_OK:
                            states[i] = ST_ERR;
                            break;

                        case ST_ERR:
                            states[i] = blobInfo.DisksWrittenTo[i] ? ST_LOST : ST_OK;
                            break;

                        case ST_LOST:
                            states[i] = ST_OK;
                            break;
                    }
                    carry = states[i] == ST_OK;
                }
                if (carry) {
                    break;
                }
            }
        }
    }

    void GenerateBlob(TLogoBlobID *id, TDataPartSet *parts) {
        TString data = Data;
        *id = TLogoBlobID(1, 1, Index++, 0, data.size(), 0);
        char *buffer = data.Detach();
        Encrypt(buffer, buffer, 0, data.size(), *id, *Info);
        *parts = {};
        Info->Type.SplitData(TBlobStorageGroupType::CrcModeNone, data, *parts);
    }

    void WriteParts() {
        const TBlobStorageGroupType type = Info->Type;
        const TBlobStorageGroupInfo::TTopology *topology = &Info->GetTopology();
        const TBlobStorageGroupInfo::IQuorumChecker& checker = topology->GetQuorumChecker();

        auto writeLayout = [&](const TSubgroupPartLayout& layout) {
            bool almostWritten = false;
            bool unwritten = false;

            if (checker.GetBlobState(layout, {topology}) == TBlobStorageGroupInfo::EBS_FULL) {
                almostWritten = false; // blob is fully written
            } else {
                // try to add one more part to make it fully written, if possible
                bool found = false;
                for (ui32 idxInSubgroup = 0; !found && idxInSubgroup < type.BlobSubgroupSize(); ++idxInSubgroup) {
                    for (ui32 partIdx = 0; !found && partIdx < type.TotalPartCount(); ++partIdx) {
                        if (layout.GetDisksWithPart(partIdx) & 1 << idxInSubgroup) {
                            continue;
                        }
                        bool match = false;
                        switch (type.GetErasure()) {
                            case TBlobStorageGroupType::ErasureMirror3dc:
                                match = partIdx == idxInSubgroup % 3;
                                break;

                            case TBlobStorageGroupType::ErasureMirror3of4:
                                match = partIdx == (idxInSubgroup & 1) || partIdx == 2;
                                break;

                            default:
                                match = idxInSubgroup < type.TotalPartCount()
                                    ? idxInSubgroup == partIdx // only specific part on main
                                    : true; // any part on handoff
                                break;
                        }
                        if (match) {
                            TSubgroupPartLayout temp(layout);
                            temp.AddItem(idxInSubgroup, partIdx, type);
                            if (checker.GetBlobState(temp, {topology}) == TBlobStorageGroupInfo::EBS_FULL) {
                                found = true;
                            }
                        }
                    }
                }
                if (found) {
                    almostWritten = true;
                } else if (type.ErasureFamily() == TBlobStorageGroupType::ErasureParityBlock) {
                    i32 count = 0;
                    for (ui32 partIdx = 0; partIdx < type.TotalPartCount(); ++partIdx) {
                        if (layout.GetDisksWithPart(partIdx)) {
                            count++;
                        }
                    }
                    if (count < static_cast<i32>(type.MinimalRestorablePartCount())) {
                        unwritten = true;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }

            TLogoBlobID id;
            TDataPartSet parts;
            GenerateBlob(&id, &parts);

            TBlobStorageGroupInfo::TOrderNums orderNums;
            Info->GetTopology().PickSubgroup(id.Hash(), orderNums);

            TBlobStorageGroupInfo::TGroupVDisks disksWrittenTo(topology);

            layout.ForEachPartOfDisk(type, [&](ui32 partIdx, ui32 idxInSubgroup) {
                const TLogoBlobID partId(id, partIdx + 1);
                const NKikimrProto::EReplyStatus res = PutToVDisk(orderNums[idxInSubgroup], partId,
                    parts.Parts[partIdx].OwnedString.ConvertToString());
                UNIT_ASSERT_VALUES_EQUAL(res, NKikimrProto::OK);
                disksWrittenTo |= {topology, topology->GetVDiskId(orderNums[idxInSubgroup])};
            });

            BlobsWritten.emplace(id, TBlobInfo{.DisksWrittenTo = disksWrittenTo, .Layout = layout, .AlmostWritten = almostWritten, .Unwritten = unwritten});
        };
        if (type.GetErasure() != TBlobStorageGroupType::Erasure8Plus2Block) {
            TSubgroupPartLayout::GeneratePossibleLayouts(type, 1, writeLayout);
            return;
        }

        TSubgroupPartLayout full;
        const ui32 parts = type.TotalPartCount();
        for (ui32 part = 0; part < parts; ++part) {
            full.AddItem(part, part, type);
        }
        writeLayout(full);
        for (ui32 first = 0; first < parts; ++first) {
            auto absent = full;
            absent.ClearItem(first, first, type);
            writeLayout(absent);
            for (ui32 handoff = 0; handoff < type.Handoff(); ++handoff) {
                auto moved = absent;
                moved.AddItem(parts + handoff, first, type);
                writeLayout(moved);
            }
            for (ui32 second = 0; second < first; ++second) {
                for (bool swap : {false, true}) {
                    auto moved = absent;
                    moved.ClearItem(second, second, type);
                    moved.AddItem(parts + swap, first, type);
                    moved.AddItem(parts + !swap, second, type);
                    writeLayout(moved);
                }
            }
        }
        // Match GeneratePossibleLayouts(type, 1): at most one part per handoff.
        // The phantom oracle below counts distinct parts; with dense handoffs
        // that can exceed the independent replicas used by PhantomCheck.
        // Dense layouts have a separate DSProxyBlock82 regression.
        ui64 seed = 0x82422026;
        for (ui32 iteration = 0; iteration < 64; ++iteration) {
            TSubgroupPartLayout layout;
            auto next = [&] {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                return seed;
            };
            for (ui32 part = 0; part < parts; ++part) {
                if (next() & 1) {
                    layout.AddItem(part, part, type);
                }
            }
            for (ui32 handoff = 0; handoff < type.Handoff(); ++handoff) {
                const ui32 part = next() % (parts + 1);
                if (part < parts) {
                    layout.AddItem(parts + handoff, part, type);
                }
            }
            writeLayout(layout);
        }
    }
};

} // NKikimr
