#include "blobstorage_replrecoverymachine.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {
std::shared_ptr<TReplCtx> MakeBlock82ReplContext(ui32 threshold) {
    const auto info = MakeIntrusive<TBlobStorageGroupInfo>(TErasureType::Erasure8Plus2Block);
    const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    const auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
        info->GetVDiskId(0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
    auto config = MakeIntrusive<TVDiskConfig>(TVDiskConfig::TBaseInfo::SampleForTests());
    auto huge = std::make_shared<THugeBlobCtx>(nullptr, vctx->EffectiveAddHeader);
    return std::make_shared<TReplCtx>(vctx, nullptr, nullptr, huge, threshold, nullptr, info,
        TActorId(), config, std::make_shared<std::atomic_uint64_t>());
}

struct TRecoveryResult {
    ui32 Recovered = 0;
    ui32 Unrecovered = 0;
    void AddUnreplicatedBlobRecord(const NRepl::TRecoveryMachine::TPartSet&, TIngress, bool) {
        ++Unrecovered;
    }
    void DropUnreplicatedBlobRecord(const TLogoBlobID&) {
        ++Recovered;
    }
    void AddPhantomBlobRecord(const NRepl::TRecoveryMachine::TPartSet&, TIngress, NMatrix::TVectorType) {
        UNIT_FAIL("unexpected phantom");
    }
};
}

Y_UNIT_TEST_SUITE(ReplicationBlock82) {
    Y_UNIT_TEST(NullHullUsesImmutableHeaderPolicy) {
        const auto ctx = MakeBlock82ReplContext(4097);
        UNIT_ASSERT(!ctx->HullCtx);
        UNIT_ASSERT(ctx->VDiskCfg->AddHeader);
        UNIT_ASSERT(!ctx->VCtx->EffectiveAddHeader);
        UNIT_ASSERT(!ctx->HugeBlobCtx->AddHeader);
        UNIT_ASSERT(!ctx->GetAddHeader());
        ctx->VDiskCfg->AddHeader = false;
        UNIT_ASSERT(!ctx->GetAddHeader());
        ctx->VDiskCfg->AddHeader = true;
        UNIT_ASSERT(!ctx->GetAddHeader());
    }

    Y_UNIT_TEST(AllSingleDoubleLossesAndUnrecoverableTriples) {
        const TBlobStorageGroupType type(TErasureType::Erasure8Plus2Block);
        std::vector<ui32> losses;
        for (ui32 mask = 1; mask < 1024; ++mask) {
            if (std::popcount(mask) <= 2) {
                losses.push_back(mask);
            }
        }
        losses.insert(losses.end(), {7, 0x301, 0x380});
        for (const ui32 threshold : {1u, Max<ui32>()}) {
            const bool huge = threshold == 1;
            const auto ctx = MakeBlock82ReplContext(threshold);
            for (const auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
                for (const ui32 size : {257u, 8193u}) {
                    const TString data(size, 'x');
                    std::array<TRope, 10> encoded;
                    ErasureSplit(crc, type, TRope(data), encoded, nullptr, GetDefaultRcBufAllocator());
                    for (const ui32 loss : losses) {
                        const TLogoBlobID id(1, 1, loss, 0, size, 0, 0, crc);
                        auto info = MakeIntrusive<TEvReplFinished::TInfo>();
                        info->WorkUnitsPlanned = Max<ui64>();
                        NRepl::TRecoveryMachine machine(ctx, info);
                        NMatrix::TVectorType wanted(0, 10);
                        NRepl::TRecoveryMachine::TPartSet available(id, type);
                        for (ui8 p = 0; p < 10; ++p) {
                            if (loss & (1u << p)) {
                                wanted.Set(p);
                            } else {
                                // Include copies obtained from both handoff positions.
                                const ui32 disk = p % 3 == 0 ? 10 : p % 3 == 1 ? 11 : p;
                                available.AddData(disk, TLogoBlobID(id, p + 1), NKikimrProto::OK,
                                    TRope(encoded[p]));
                            }
                        }
                        machine.AddTask(id, wanted, false, TIngress());
                        NRepl::TRecoveryMachine::TRecoveredBlobsQueue output;
                        TRecoveryResult result;
                        machine.Recover(available, output, result);
                        if (std::popcount(loss) > 2) {
                            UNIT_ASSERT(output.empty());
                            UNIT_ASSERT_VALUES_EQUAL(result.Recovered, 0);
                            UNIT_ASSERT_VALUES_EQUAL(result.Unrecovered, 1);
                            continue;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(result.Recovered, 1);
                        UNIT_ASSERT_VALUES_EQUAL(result.Unrecovered, 0);
                        UNIT_ASSERT_VALUES_EQUAL(output.size(), huge ? std::popcount(loss) : 1);
                        NMatrix::TVectorType recovered(0, 10);
                        while (!output.empty()) {
                            const auto& record = output.front();
                            const auto parts = huge ? NMatrix::TVectorType::MakeOneHot(record.Id.PartId() - 1, 10)
                                : record.LocalParts;
                            UNIT_ASSERT_VALUES_EQUAL(record.IsHugeBlob, huge);
                            UNIT_ASSERT_VALUES_EQUAL(record.Data.GetSize(),
                                TDiskBlob::CalculateBlobSize(type, id, parts, false));
                            TDiskBlob blob(&record.Data, parts, type, id);
                            for (auto p = blob.begin(); p != blob.end(); ++p) {
                                UNIT_ASSERT_EQUAL(p.GetPart(), encoded[p.GetPartId() - 1]);
                            }
                            recovered |= parts;
                            output.pop();
                        }
                        UNIT_ASSERT_EQUAL(recovered, wanted);
                    }
                }
            }
        }
    }
}
}
