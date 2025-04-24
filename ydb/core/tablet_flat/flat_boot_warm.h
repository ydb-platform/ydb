#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_bio_events.h"
#include "flat_dbase_naked.h"
#include "flat_mem_blobs.h"
#include "flat_sausage_fetch.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TMemTable final: public NBoot::IStep {
        using TBlobs = NTable::NMem::TBlobs;
        using TLargeGlobId = NPageCollection::TLargeGlobId;

    public:
        TMemTable(IStep *owner) : IStep(owner, NBoot::EStep::MemTable) { }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() override
        {
            for (auto it: Back->DatabaseImpl->Scheme->Tables) {
                const auto &wrap = Back->DatabaseImpl->Get(it.first, true);

                for (auto &mem : wrap->GetMemTables()) {
                    size_t size = mem->GetBlobs()->Size();
                    if (size > 0) {
                        ui64 base = ui64(States.size()) << 32;
                        auto& state = States.emplace_back();
                        state.Blobs = const_cast<TBlobs*>(mem->GetBlobs());
                        state.Pages.resize(size);
                        ui32 page = 0;
                        for (auto it = state.Blobs->Iterator(); it.IsValid(); it.Next()) {
                            Y_DEBUG_ABORT_UNLESS(page < state.Pages.size(),
                                "Unexpected memtable blobs instability during boot");
                            ui64 cookie = base | page++;
                            Pending += Spawn<TLoadBlobs>(TLargeGlobId(it->GId.Group, it->GId.Logo), cookie);
                            ++state.Pending;
                        }
                    }
                }
            }

            if (!Pending) {
                Env->Finish(this);
            }
        }

        void HandleStep(TIntrusivePtr<IStep> step) override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(Pending);

            size_t index = load->Cookie >> 32;
            Y_ENSURE(index < States.size());
            auto& state = States[index];
            Y_ENSURE(state.Pending > 0);

            ui32 page = ui32(load->Cookie);
            Y_ENSURE(page < state.Pages.size());
            Y_ENSURE(!state.Pages[page].Data);

            state.Pages[page].PageId = page;
            state.Pages[page].Data = load->PlainData();

            if (!--state.Pending) {
                state.Blobs->Assign(state.Pages);
                state.Blobs = nullptr;
                state.Pages.clear();
            }

            if (!Pending) {
                Env->Finish(this);
            }
        }

    private:
        struct TLoadState {
            TBlobs* Blobs;
            TVector<NPageCollection::TLoadedPage> Pages;
            size_t Pending = 0;
        };

    private:
        TDeque<TLoadState> States;
        TLeft Pending;
    };
}
}
}
