#include <ydb/core/blobstorage/dsproxy/dsproxy_blackboard.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_restore.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_m3dc_restore.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>
#include <util/generic/overloaded.h>

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
        for (auto& get : blackboard.GroupDiskRequests.GetsPending) {
            auto& disk = DiskStates[get.OrderNumber];
            Ctest << "orderNumber# " << get.OrderNumber << " get Id# " << get.Id;
            if (disk.InErrorState) {
                Ctest << " ERROR";
                blackboard.AddErrorResponse(get.Id, get.OrderNumber);
            } else if (auto it = disk.Blobs.find(get.Id); it == disk.Blobs.end()) {
                Ctest << " NODATA";
                blackboard.AddNoDataResponse(get.Id, get.OrderNumber);
            } else {
                std::visit(TOverloaded{
                    [&](TNotYet&) {
                        Ctest << " NOT_YET";
                        blackboard.AddNotYetResponse(get.Id, get.OrderNumber);
                    },
                    [&](TRope& buffer) {
                        Ctest << " OK";
                        size_t begin = Min<size_t>(get.Shift, buffer.size());
                        size_t end = Min<size_t>(buffer.size(), begin + get.Size);
                        TRope data(buffer.begin() + begin, buffer.begin() + end);
                        blackboard.AddResponseData(get.Id, get.OrderNumber, get.Shift, std::move(data));
                    }
                }, it->second);
            }
            Ctest << Endl;
        }
        blackboard.GroupDiskRequests.GetsPending.clear();

        for (auto& put : blackboard.GroupDiskRequests.PutsPending) {
            auto& disk = DiskStates[put.OrderNumber];
            Ctest << "orderNumber# " << put.OrderNumber << " put Id# " << put.Id;
            if (disk.InErrorState) {
                Ctest << " ERROR";
                blackboard.AddErrorResponse(put.Id, put.OrderNumber);
            } else {
                Ctest << " OK";
                disk.Blobs[put.Id] = std::move(put.Buffer);
                blackboard.AddPutOkResponse(put.Id, put.OrderNumber);
            }
            Ctest << Endl;
        }
        blackboard.GroupDiskRequests.PutsPending.clear();
    }
};

template<typename T>
void RunStrategyTest(TBlobStorageGroupType type) {
    TBlobStorageGroupInfo info(type);
    info.Ref();
    TGroupQueues groupQueues(info.GetTopology());
    groupQueues.Ref();

    std::unordered_map<TString, std::tuple<EStrategyOutcome, TString>> transitions;

    TString data(1000, 'x');
    TLogoBlobID id(1'000'000'000, 1, 1, 0, data.size(), 0);
    std::vector<TRope> parts(type.TotalPartCount());

    for (ui32 iter = 0; iter < 100'000; ++iter) {
        Ctest << "iteration# " << iter << Endl;

        TBlackboard blackboard(&info, &groupQueues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead);
        ErasureSplit(TBlobStorageGroupType::CrcModeNone, type, TRope(data), parts);
        blackboard.RegisterBlobForPut(id, 0);
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

struct TGetQuery {
    ui32 OrderNumber;
    TLogoBlobID Id;
    ui32 Shift;
    ui32 Size;

    auto AsTuple() const { return std::make_tuple(OrderNumber, Id, Shift, Size); }
    friend bool operator ==(const TGetQuery& x, const TGetQuery& y) { return x.AsTuple() == y.AsTuple(); }
    friend bool operator <(const TGetQuery& x, const TGetQuery& y) { return x.AsTuple() < y.AsTuple(); }
};

struct TPutQuery {
    ui32 OrderNumber;
    TLogoBlobID Id;

    auto AsTuple() const { return std::make_tuple(OrderNumber, Id); }
    friend bool operator ==(const TPutQuery& x, const TPutQuery& y) { return x.AsTuple() == y.AsTuple(); }
    friend bool operator <(const TPutQuery& x, const TPutQuery& y) { return x.AsTuple() < y.AsTuple(); }
};

using TOperation = std::variant<TGetQuery, TPutQuery>;

void RunTestLevel(const TBlobStorageGroupInfo& info, TBlackboard& blackboard,
        const std::function<EStrategyOutcome(TBlackboard&)>& runStrategies, const TLogoBlobID& id,
        std::vector<TOperation>& stock, TSubgroupPartLayout presenceMask, bool nonWorkingDomain,
        std::set<TOperation>& context, ui32& terminals) {
    // see which operations we can add to the stock
    const size_t stockSizeOnEntry = stock.size();
    for (auto& get : blackboard.GroupDiskRequests.GetsPending) {
        stock.push_back(TGetQuery{get.OrderNumber, get.Id, get.Shift, get.Size});
        const bool inserted = context.insert(stock.back()).second;
        UNIT_ASSERT(inserted);
    }
    blackboard.GroupDiskRequests.GetsPending.clear();
    for (auto& put : blackboard.GroupDiskRequests.PutsPending) {
        stock.push_back(TPutQuery{put.OrderNumber, put.Id});
        const bool inserted = context.insert(stock.back()).second;
        UNIT_ASSERT(inserted);
    }
    blackboard.GroupDiskRequests.PutsPending.clear();
    UNIT_ASSERT(!stock.empty());

    bool canIssuePuts = true;
    for (size_t i = 0; i < stock.size(); ++i) {
        if (std::holds_alternative<TGetQuery>(stock[i])) {
            canIssuePuts = false;
            break;
        }
    }

    // try every single operation in stock
    for (size_t i = 0; i < stock.size(); ++i) {
        if (!canIssuePuts && std::holds_alternative<TPutQuery>(stock[i])) {
            continue;
        }
        if (auto *get = std::get_if<TGetQuery>(&stock[i]); get && context.contains(TPutQuery{get->OrderNumber, get->Id})) {
            continue;
        }

        std::swap(stock[i], stock.back());
        TOperation operation = std::move(stock.back());
        stock.pop_back();

        TBlackboard branch(blackboard);
        TSubgroupPartLayout myPresenceMask(presenceMask);

        std::visit(TOverloaded{
            [&](const TGetQuery& op) {
                const ui32 idxInSubgroup = info.GetTopology().GetIdxInSubgroup(info.GetVDiskId(op.OrderNumber), id.Hash());
                if (nonWorkingDomain && idxInSubgroup % 3 == 2) {
                    branch.AddErrorResponse(op.Id, op.OrderNumber);
                } else if (myPresenceMask.GetDisksWithPart(op.Id.PartId() - 1) >> idxInSubgroup & 1) {
                    const ui32 blobSize = op.Id.BlobSize();
                    const ui32 shift = Min(op.Shift, blobSize);
                    const ui32 size = Min(op.Size ? op.Size : Max<ui32>(), blobSize - shift);
                    branch.AddResponseData(op.Id, op.OrderNumber, shift, TRope(TString(size, 'X')));
                } else {
                    branch.AddNoDataResponse(op.Id, op.OrderNumber);
                }
            },
            [&](const TPutQuery& op) {
                const ui32 idxInSubgroup = info.GetTopology().GetIdxInSubgroup(info.GetVDiskId(op.OrderNumber), id.Hash());
                if (nonWorkingDomain && idxInSubgroup % 3 == 2) {
                    branch.AddErrorResponse(op.Id, op.OrderNumber);
                } else {
                    myPresenceMask.AddItem(idxInSubgroup, op.Id.PartId() - 1, info.Type);
                    branch.AddPutOkResponse(op.Id, op.OrderNumber);
                }
            }
        }, operation);

        auto outcome = runStrategies(branch);
        UNIT_ASSERT(outcome != EStrategyOutcome::ERROR);
        if (outcome == EStrategyOutcome::DONE) {
            TBlobStorageGroupInfo::TOrderNums nums;
            info.GetTopology().PickSubgroup(id.Hash(), nums);
            UNIT_ASSERT(info.GetQuorumChecker().GetBlobState(myPresenceMask, {&info.GetTopology()}) == TBlobStorageGroupInfo::EBS_FULL);
            ++terminals;
        } else {
            RunTestLevel(info, branch, runStrategies, id, stock, myPresenceMask, nonWorkingDomain, context, terminals);
        }

        stock.push_back(std::move(operation));
        std::swap(stock[i], stock.back());
    }

    // revert stock
    for (size_t i = stockSizeOnEntry; i < stock.size(); ++i) {
        const size_t n = context.erase(stock[i]);
        UNIT_ASSERT(n);
    }
    stock.resize(stockSizeOnEntry);
}

Y_UNIT_TEST_SUITE(DSProxyStrategyTest) {

    Y_UNIT_TEST(Restore_block42) {
        RunStrategyTest<TRestoreStrategy>(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(Restore_mirror3dc) {
        THPTimer timer;
        const TBlobStorageGroupType type(TBlobStorageGroupType::ErasureMirror3dc);

        TBlobStorageGroupInfo info(type, 1, 3, 3);
        info.Ref();
        TGroupQueues groupQueues(info.GetTopology());
        groupQueues.Ref();

        std::vector<TOperation> stock;

        TLogContext logCtx(NKikimrServices::BS_PROXY, false);
        logCtx.SuppressLog = true;

        auto runStrategies = [&](TBlackboard& blackboard) {
            return blackboard.RunStrategy(logCtx, TMirror3dcGetWithRestoreStrategy());
        };

        const ui32 base = RandomNumber(512u);
        for (ui32 i = 0; i < 512; ++i) {
            const ui32 diskMask = (base + i) % 512;
            for (bool nonWorkingDomain : {false, true}) {
                TBlackboard blackboard(&info, &groupQueues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead);

                const TLogoBlobID id(1'000'000'000, 1, 1, 0, 1000, 0);
                TSubgroupPartLayout presenceMask;
                blackboard.AddNeeded(id, 0, id.BlobSize());
                bool partsAvailable = false;
                for (ui32 idxInSubgroup = 0; idxInSubgroup < 9; ++idxInSubgroup) {
                    if (diskMask >> idxInSubgroup & 1 && (!nonWorkingDomain || idxInSubgroup % 3 != 2)) {
                        presenceMask.AddItem(idxInSubgroup, idxInSubgroup % 3, info.Type);
                        partsAvailable = true;
                    }
                }
                if (!partsAvailable) {
                    continue;
                }

                Cerr << "diskMask# " << diskMask << " nonWorkingDomain# " << nonWorkingDomain;

                auto outcome = runStrategies(blackboard);
                UNIT_ASSERT(outcome == EStrategyOutcome::IN_PROGRESS);

                std::set<TOperation> context;
                ui32 terminals = 0;
                RunTestLevel(info, blackboard, runStrategies, id, stock, presenceMask, nonWorkingDomain, context, terminals);
                Cerr << " " << terminals << Endl;

                if (TDuration::Seconds(timer.Passed()) >= TDuration::Minutes(5)) {
                    return;
                }
            }
        }
    }

}
