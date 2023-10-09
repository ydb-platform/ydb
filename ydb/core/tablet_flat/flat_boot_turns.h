#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_boot_switch.h"

#include <ydb/core/util/pb.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TTurns final: public NBoot::IStep {
    public:
        TTurns(IStep *owner)
            : IStep(owner, NBoot::EStep::Turns)
            , Codec(NBlockCodecs::Codec("lz4fast"))
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() noexcept override
        {
            for (auto slot: xrange(Back->Switches.size()))
                if (const auto &largeGlobId = Back->Switches[slot].LargeGlobId)
                    Pending += Spawn<TLoadBlobs>(largeGlobId, slot);

            Flush();
        }

        void HandleStep(TIntrusivePtr<IStep> step) noexcept override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(Pending);

            Assign(load->Cookie, load->Plain());
            Flush();
        }

    private:
        void Flush() noexcept
        {
            Process();

            if (!Pending && Handled >= Back->Switches.size()) Env->Finish(this);
        }

        void Assign(ui32 slot, TArrayRef<const char> body) noexcept
        {
            Y_ABORT_UNLESS(slot < Back->Switches.size(), "Invalid switch index");

            auto &entry = Back->Switches[slot];
            auto index = TCookie(entry.LargeGlobId.Lead.Cookie()).Index();

            Y_ABORT_UNLESS(entry.LargeGlobId, "Assigning TSwitch entry w/o valid TLargeGlobId");

            if (index != TCookie::EIdx::TurnLz4) {
                Apply(entry, body);
            } else {
                Apply(entry, Codec->Decode(body));
            }
        }

        void Apply(TSwitch &entry, TArrayRef<const char> body) noexcept
        {
            TProtoBox<NKikimrExecutorFlat::TTablePartSwitch> proto(body);

            entry.Init(proto);

            if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Back) << " switching packs"
                    << ", table " << entry.Table << ", bundles [";

                for (const auto &one : entry.Bundles) {
                    logl << " " << one.LargeGlobIds[0].Lead;
                    if (one.Epoch != NTable::TEpoch::Max()) {
                        logl << "{epoch " << one.Epoch << "}";
                    }
                }

                for (const auto &one : entry.Moves) {
                    logl << " " << one.Label;
                    if (one.RebasedEpoch != NTable::TEpoch::Max()) {
                        logl << "{epoch " << one.RebasedEpoch << "}";
                    }
                    logl << " from table " << one.SourceTable;
                }

                logl << " ]";
            }

            if (proto.HasTableSnapshoted())
                Back->SetTableEdge(proto.GetTableSnapshoted());
        }

        void Process() noexcept
        {
            for (; Handled < Back->Switches.size(); Handled++) {
                auto &front = Back->Switches[Handled];

                if (!front.Loaded())
                    return;

                for (auto &txStatusId : front.LeavingTxStatus) {
                    auto it = TxStatus.find(txStatusId);
                    if (it == TxStatus.end()) {
                        Y_Fail("Part switch has removal for an unknown tx status " << txStatusId);
                    }
                    it->second->Load = false;
                    TxStatus.erase(it);
                    LeavingTxStatus.insert(txStatusId);
                }

                for (auto &txStatus : front.TxStatus) {
                    if (!txStatus.DataId) {
                        Y_Fail("Part switch has tx status without data id");
                    }
                    const auto &txStatusId = txStatus.DataId.Lead;
                    if (TxStatus.contains(txStatusId)) {
                        Y_Fail("Part switch has a duplicate tx status " << txStatusId);
                    }
                    if (LeavingTxStatus.contains(txStatusId)) {
                        Y_Fail("Part switch has a removed tx status " << txStatusId);
                    }
                    TxStatus[txStatusId] = &txStatus;
                }

                for (auto &bundleId : front.Leaving) {
                    auto it = Bundles.find(bundleId);
                    if (it == Bundles.end()) {
                        Y_Fail("Part switch has removal for an unknown bundle " << bundleId);
                    }
                    it->second->Load = false;
                    Bundles.erase(it);
                    Leaving.insert(bundleId);
                }

                for (auto &change : front.Changes) {
                    auto *bundle = Bundles.Value(change.Label, nullptr);
                    if (!bundle) {
                        Y_Fail("Part switch has changes for an unknown bundle " << change.Label);
                    }
                    bundle->Legacy = std::move(change.Legacy);
                    bundle->Opaque = std::move(change.Opaque);
                    bundle->Deltas.clear();
                }

                for (auto &delta : front.Deltas) {
                    auto *bundle = Bundles.Value(delta.Label, nullptr);
                    if (!bundle) {
                        Y_Fail("Part switch has delta for an unknown bundle " << delta.Label);
                    }
                    bundle->Deltas.push_back(std::move(delta.Delta));
                }

                for (auto &bundle : front.Bundles) {
                    if (!bundle.LargeGlobIds) {
                        Y_Fail("Part switch has bundle without page collections");
                    }
                    const auto &bundleId = bundle.LargeGlobIds[0].Lead;
                    if (Bundles.contains(bundleId)) {
                        Y_Fail("Part switch has a duplicate bundle " << bundleId);
                    }
                    if (Leaving.contains(bundleId)) {
                        Y_Fail("Part switch has a removed bundle" << bundleId);
                    }
                    Bundles[bundleId] = &bundle;
                }

                auto *compaction = Logic->Result().Comp.Get();

                front.MovedBundles.resize(front.Moves.size());
                for (size_t index = 0; index < front.Moves.size(); ++index) {
                    const auto &move = front.Moves[index];

                    auto *source = Bundles.Value(move.Label, nullptr);
                    if (!source) {
                        Y_Fail("Part switch has move for an unknown bundle " << move.Label);
                    }

                    if (compaction) {
                        auto &snapshot = compaction->Snapshots[move.SourceTable];
                        snapshot.State.PartLevels.erase(move.Label);
                    }

                    // Make a copy of the source bundle state, changing epoch
                    auto &bundle = front.MovedBundles[index];
                    bundle = std::move(*source);
                    if (move.RebasedEpoch != NTable::TEpoch::Max()) {
                        bundle.Epoch = move.RebasedEpoch;
                    }

                    // Don't load the old bundle state
                    source->Load = false;

                    // Change pointer to the new bundle
                    Bundles[move.Label] = &bundle;
                }

                if (compaction) {
                    auto &snapshot = compaction->Snapshots[front.Table];

                    // N.B.: schema is reflected first during commit
                    if (front.CompactionChanges.Strategy != NKikimrSchemeOp::CompactionStrategyUnset &&
                        front.CompactionChanges.Strategy != snapshot.Strategy)
                    {
                        snapshot.State.PartLevels.clear();
                        snapshot.State.StateSnapshot.clear();
                        snapshot.Strategy = front.CompactionChanges.Strategy;
                    }

                    for (const auto &label : front.Leaving) {
                        snapshot.State.PartLevels.erase(label);
                    }

                    for (const auto &bundle : front.Bundles) {
                        const auto &label = bundle.LargeGlobIds[0].Lead;
                        snapshot.State.PartLevels[label] = front.Level;
                    }

                    for (const auto &kv : front.CompactionChanges.KeyValues) {
                        if (kv.second) {
                            snapshot.State.StateSnapshot[kv.first] = kv.second;
                        } else {
                            snapshot.State.StateSnapshot.erase(kv.first);
                        }
                    }
                }

                std::exchange(front.LeavingTxStatus, { });
                std::exchange(front.Leaving, { });
                std::exchange(front.Changes, { });
                std::exchange(front.Deltas, { });
                std::exchange(front.Moves, { });
            }
        }

    private:
        const NBlockCodecs::ICodec *Codec = nullptr;
        TLeft Pending;
        ui64 Handled = 0;
        THashSet<TLogoBlobID> Leaving; /* Dropped bundles */
        THashMap<TLogoBlobID, TSwitch::TBundle*> Bundles;
        THashSet<TLogoBlobID> LeavingTxStatus; /* Dropped tx status */
        THashMap<TLogoBlobID, TSwitch::TTxStatus*> TxStatus;
    };
}
}
}
