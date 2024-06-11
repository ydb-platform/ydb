#pragma once

#include "defs.h"
#include "dsproxy_fault_tolerance_ut_base.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

#include <util/random/fast.h>

namespace NKikimr {

class TRangeFaultToleranceTest : public TFaultToleranceTestBase<TRangeFaultToleranceTest> {
public:
    using TFaultToleranceTestBase::TFaultToleranceTestBase;

    void Check(ui64 tabletId, const TBlobStorageGroupInfo::TGroupVDisks& disks,
            NKikimrProto::EReplyStatus defaultExpectedStatus = NKikimrProto::OK) {
        // Cerr << (TStringBuilder() << "]] " << disks.ToString() << Endl);
        for (ui32 generation = 1; generation <= 4; ++generation) {
            NKikimrProto::EReplyStatus expectedStatus = defaultExpectedStatus;
            TVector<TEvBlobStorage::TEvRangeResult::TResponse> expectedResponse;

            ui32 statusMap = generation - 1;

            CTEST << "tabletId# " << tabletId << " generation# " << generation << " statusMap#";

            for (ui32 step = 1; step <= 2; ++step) {
                TString buffer = Sprintf("%256s/%" PRIu64 "/%" PRIu32 "/%" PRIu32, "kikimr", tabletId, generation, step);
                TStringBuilder b;
                for (int i = 0; i < 1024; ++i) {
                    b << 'a';
                }
                buffer += b;
                TLogoBlobID id(tabletId, generation, step, 0 /*channel*/, buffer.size(), 0);
                if (defaultExpectedStatus == NKikimrProto::OK) {
                    UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::OK, PutWithResult(id, buffer, TEvBlobStorage::TEvPut::TacticMaxThroughput)->Status);
                } else {
                    PutWithResult(id, buffer, TEvBlobStorage::TEvPut::TacticMaxThroughput);
                }

                TBlobStorageGroupInfo::TVDiskIds vdisks;
                TBlobStorageGroupInfo::TServiceIds services;
                Info->PickSubgroup(id.Hash(), &vdisks, &services);

                enum {
                    Lost,
                    Unconfirmed,
                    Max
                };
                const ui32 blobStatus = statusMap % Max;
                statusMap /= Max;

                switch (blobStatus) {
                    case Lost:
                        CTEST << " Lost";
                        // delete blobs from selected disks -- the data is missing, but the metadata is still here
                        Delete(id, disks, false);
                        break;

                    case Unconfirmed:
                        CTEST << " Unconfirmed";
                        // wipe out blobs from selected disks, including any metadata
                        Delete(id, disks, true);
                        break;

                    default:
                        Y_ABORT();
                }

                // now collect information about actually written replicas
                ui32 writtenPartsMask = 0;
                TSubgroupPartLayout layout;
                for (ui32 i = 0; i < vdisks.size(); ++i) {
                    auto event = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdisks[i], TInstant::Max(),
                        NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {i}, {id});
                    GetActorContext().Send(services[i], event.release());
                }
                for (ui32 i = 0; i < vdisks.size(); ++i) {
                    auto event = WaitForSpecificEvent<TEvBlobStorage::TEvVGetResult>(&TRangeFaultToleranceTest::ProcessUnexpectedEvent);
                    // Cerr << (TStringBuilder() << "]] Get: " << event->Get()->ToString() << Endl);
                    if (event->Get()->Record.GetStatus() == NKikimrProto::OK) {
                        for (const auto& item : event->Get()->Record.GetResult()) {
                            if (item.GetStatus() == NKikimrProto::OK) {
                                TLogoBlobID partId(LogoBlobIDFromLogoBlobID(item.GetBlobID()));
                                Y_ABORT_UNLESS(partId.PartId() > 0);
                                Y_ABORT_UNLESS(partId.FullID() == id);
                                writtenPartsMask |= 1 << (partId.PartId() - 1);
                                layout.AddItem(event->Get()->Record.GetCookie(), partId.PartId() - 1, Info->Type);
                            }
                        }
                    }
                }

                // figure out if it is possible to restore this blob
                const bool restorable = Info->GetQuorumChecker().GetBlobState(layout, {&Info->GetTopology()}) &
                    TBlobStorageGroupInfo::EBSF_RECOVERABLE;
                if (restorable) {
                    CTEST << "/Y";
                    expectedResponse.emplace_back(id, buffer);
                } else {
                    CTEST << "/N(" << writtenPartsMask << ")";
                    switch (blobStatus) {
                        case Lost:
                            // FIXME: uncomment when KIKIMR-2271 is done
//                            expectedStatus = NKikimrProto::ERROR;
                            break;

                        case Unconfirmed:
                            // this blob will not fit into result at all
                            break;
                    }
                }
            }

            CTEST << Endl;

            auto query = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, TLogoBlobID(tabletId, generation, 0, 0, 0, 0),
                    TLogoBlobID(tabletId, generation, Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                    false, TInstant::Max());
            SendToBSProxy(GetActorContext(), Info->GroupID, query.release());
            auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvRangeResult>(&TRangeFaultToleranceTest::ProcessUnexpectedEvent);
            CTEST << resp->Get()->ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Status, expectedStatus);
            if (expectedStatus == NKikimrProto::OK) {
                auto it1 = resp->Get()->Responses.begin();
                auto it2 = expectedResponse.begin();

                while (it1 != resp->Get()->Responses.end() && it2 != expectedResponse.end()) {
                    if (it1->Id < it2->Id) {
                        ++it1;
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(it1->Id, it2->Id);
                        UNIT_ASSERT_VALUES_EQUAL(it1->Buffer, it2->Buffer);
                        ++it1;
                        ++it2;
                    }
                }
                UNIT_ASSERT(it2 == expectedResponse.end());
            }
        }
    }

    void RunTestAction() {
        ui64 tabletId = 100500;
        for (const auto& disks : FaultsFittingFailModel) {
            Check(tabletId++, disks);
        }
        for (const auto& disks : FaultsExceedingFailModel) {
            Check(tabletId++, disks);
        }
        for (const auto& disks : FaultsFittingFailModel) {
            SetFailedDisks(disks);
            Check(tabletId++, TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology()));
        }
        for (const auto& disks : FaultsExceedingFailModel) {
            SetFailedDisks(disks);
            Check(tabletId++, TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology()), NKikimrProto::ERROR);
        }
    }
};

} // NKikimr
