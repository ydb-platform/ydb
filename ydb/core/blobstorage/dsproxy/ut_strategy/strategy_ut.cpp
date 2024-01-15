#include <ydb/core/blobstorage/dsproxy/dsproxy_blackboard.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_restore.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

using namespace NActors;
using namespace NKikimr;

#define Ctest Cnull

class TGroupModel {
    TBlobStorageGroupInfo& Info;

    struct TNotYet {};

    struct TDiskState {
        bool InErrorState = false;
        std::map<TLogoBlobID, std::variant<TNotYet, TRope>> Blobs;
    };

    std::vector<TDiskState> DiskStates;

public:
    TGroupModel(TBlobStorageGroupInfo& info)
        : Info(info)
        , DiskStates(Info.GetTotalVDisksNum())
    {
        for (auto& disk : DiskStates) {
            disk.InErrorState = RandomNumber(2 * DiskStates.size()) == 0;
        }
    }

    TBlobStorageGroupInfo::TGroupVDisks GetFailedDisks() const {
        TBlobStorageGroupInfo::TGroupVDisks res = &Info.GetTopology();
        for (ui32 i = 0; i < DiskStates.size(); ++i) {
            if (DiskStates[i].InErrorState) {
                res |= {&Info.GetTopology(), Info.GetVDiskId(i)};
            }
        }
        return res;
    }

    void ProcessBlackboardRequests(TBlackboard& blackboard) {
        for (ui32 i = 0; i < blackboard.GroupDiskRequests.DiskRequestsForOrderNumber.size(); ++i) {
            auto& r = blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[i];
            Y_ABORT_UNLESS(i < DiskStates.size());
            auto& disk = DiskStates[i];
            for (auto& get : r.GetsToSend) {
                Ctest << "orderNumber# " << i << " get Id# " << get.Id;
                if (disk.InErrorState) {
                    Ctest << " ERROR";
                    blackboard.AddErrorResponse(get.Id, i);
                } else if (auto it = disk.Blobs.find(get.Id); it == disk.Blobs.end()) {
                    Ctest << " NODATA";
                    blackboard.AddNoDataResponse(get.Id, i);
                } else {
                    std::visit(TOverloaded{
                        [&](TNotYet&) {
                            Ctest << " NOT_YET";
                            blackboard.AddNotYetResponse(get.Id, i);
                        },
                        [&](TRope& buffer) {
                            Ctest << " OK";
                            size_t begin = Min<size_t>(get.Shift, buffer.size());
                            size_t end = Min<size_t>(buffer.size(), begin + get.Size);
                            TRope data(buffer.begin() + begin, buffer.begin() + end);
                            blackboard.AddResponseData(get.Id, i, get.Shift, std::move(data));
                        }
                    }, it->second);
                }
                Ctest << Endl;
            }
            r.GetsToSend.clear();
            for (auto& put : r.PutsToSend) {
                Ctest << "orderNumber# " << i << " put Id# " << put.Id;
                if (disk.InErrorState) {
                    Ctest << " ERROR";
                    blackboard.AddErrorResponse(put.Id, i);
                } else {
                    Ctest << " OK";
                    disk.Blobs[put.Id] = std::move(put.Buffer);
                    blackboard.AddPutOkResponse(put.Id, i);
                }
                Ctest << Endl;
            }
            r.PutsToSend.clear();
        }
    }
};

template<typename T>
void RunStrategyTest(TBlobStorageGroupType type) {
    TBlobStorageGroupInfo info(type);
    info.Ref();
    TGroupQueues groupQueues(info.GetTopology());
    groupQueues.Ref();

    std::unordered_map<TString, std::tuple<EStrategyOutcome, TString>> transitions;

    for (ui32 iter = 0; iter < 1'000'000; ++iter) {
        Ctest << "iteration# " << iter << Endl;

        TBlackboard blackboard(&info, &groupQueues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead);
        TString data(1000, 'x');
        TLogoBlobID id(1'000'000'000, 1, 1, 0, data.size(), 0);
        std::vector<TRope> parts(type.TotalPartCount());
        ErasureSplit(TBlobStorageGroupType::CrcModeNone, type, TRope(data), parts);
        blackboard.RegisterBlobForPut(id);
        for (ui32 i = 0; i < parts.size(); ++i) {
            blackboard.AddPartToPut(id, i, TRope(parts[i]));
        }
        blackboard[id].Whole.Data.Write(0, TRope(data));

        TLogContext logCtx(NKikimrServices::BS_PROXY, false);
        logCtx.SuppressLog = true;

        TGroupModel model(info);

        auto sureFailedDisks = model.GetFailedDisks();
        auto failedDisks = sureFailedDisks;

        auto& state = blackboard[id];
        for (ui32 idxInSubgroup = 0; idxInSubgroup < type.BlobSubgroupSize(); ++idxInSubgroup) {
            for (ui32 partIdx = 0; partIdx < type.TotalPartCount(); ++partIdx) {
                if (!type.PartFits(partIdx + 1, idxInSubgroup)) {
                    continue;
                }
                const ui32 orderNumber = state.Disks[idxInSubgroup].OrderNumber;
                const TLogoBlobID partId(id, partIdx + 1);
                auto& item = state.Disks[idxInSubgroup].DiskParts[partIdx];
                TBlobStorageGroupInfo::TGroupVDisks diskMask = {&info.GetTopology(), info.GetVDiskId(orderNumber)};
                if (sureFailedDisks & diskMask) {
                    if (RandomNumber(5u) == 0) {
                        blackboard.AddErrorResponse(partId, orderNumber);
                    }
                } else {
                    switch (RandomNumber(100u)) {
                        case 0:
                            blackboard.AddErrorResponse(partId, orderNumber);
                            break;

                        case 1:
                            blackboard.AddNoDataResponse(partId, orderNumber);
                            break;

                        case 2:
                            blackboard.AddNotYetResponse(partId, orderNumber);
                            break;

                        case 3:
                            blackboard.AddResponseData(partId, orderNumber, 0, TRope(parts[partIdx]));
                            break;
                    }
                }
                if (item.Situation == TBlobState::ESituation::Error) {
                    failedDisks |= diskMask;
                }
            }
        }

        Ctest << "initial state# " << state.ToString() << Endl;

        for (;;) {
            T strategy;

            TString state = blackboard[id].ToString();

            auto outcome = blackboard.RunStrategy(logCtx, strategy);

            TString nextState = blackboard[id].ToString();
            if (const auto [it, inserted] = transitions.try_emplace(state, std::make_tuple(outcome, nextState)); !inserted) {
                Y_ABORT_UNLESS(it->second == std::make_tuple(outcome, nextState));
            }

            if (outcome == EStrategyOutcome::IN_PROGRESS) {
                auto temp = blackboard.RunStrategy(logCtx, strategy);
                UNIT_ASSERT_EQUAL(temp, outcome);
                UNIT_ASSERT_VALUES_EQUAL(blackboard[id].ToString(), nextState);
            }

            if (outcome == EStrategyOutcome::DONE) {
                Y_ABORT_UNLESS(info.GetQuorumChecker().CheckFailModelForGroup(sureFailedDisks));
                break;
            } else if (outcome == EStrategyOutcome::ERROR) {
                Y_ABORT_UNLESS(!info.GetQuorumChecker().CheckFailModelForGroup(failedDisks));
                break;
            } else if (outcome != EStrategyOutcome::IN_PROGRESS) {
                Y_ABORT("unexpected EStrategyOutcome");
            }

            model.ProcessBlackboardRequests(blackboard);
        }
    }
}

Y_UNIT_TEST_SUITE(DSProxyStrategyTest) {

    Y_UNIT_TEST(Restore_block42) {
        RunStrategyTest<TRestoreStrategy>(TBlobStorageGroupType::Erasure4Plus2Block);
    }

}
