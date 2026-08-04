#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/mailbox.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <memory>

namespace {

using namespace NActors;

constexpr size_t MaxActors = 16;
constexpr size_t MaxOperations = 512;

class TFuzzActor final : public TActor<TFuzzActor> {
public:
    TFuzzActor()
        : TActor(&TFuzzActor::StateFunc)
    {}

private:
    void StateFunc(TAutoPtr<IEventHandle>&) {}
};

struct TActorSlot {
    ui64 LocalId = 0;
    IActor* Actor = nullptr;
    THashSet<ui64> Aliases;
};

enum class EMailboxState {
    Free,
    Locked,
    Unlocked,
};

std::unique_ptr<IEventHandle> MakeEvent(ui64 cookie, ui64 recipientLocalId) {
    return std::make_unique<IEventHandle>(
        TActorId(1, recipientLocalId),
        TActorId(1, 1000000 + cookie),
        new TEvents::TEvPing,
        0,
        cookie);
}

void RemoveObserved(TVector<ui64>& outstanding, ui64 cookie) {
    auto it = std::find(outstanding.begin(), outstanding.end(), cookie);
    Y_ABORT_UNLESS(it != outstanding.end());
    outstanding.erase(it);
}

void DrainMailbox(TMailbox& mailbox, TVector<ui64>& outstanding) {
    while (auto ev = mailbox.Pop()) {
        RemoveObserved(outstanding, ev->Cookie);
    }
}

bool HasActor(const TVector<TActorSlot>& actors, ui64 localId) {
    return std::any_of(actors.begin(), actors.end(), [localId](const TActorSlot& slot) {
        return slot.LocalId == localId;
    });
}

void CheckActors(TMailbox& mailbox, const TVector<TActorSlot>& actors) {
    size_t seen = 0;
    mailbox.ForEach([&](ui64 localId, IActor* actor) {
        auto it = std::find_if(actors.begin(), actors.end(), [localId, actor](const TActorSlot& slot) {
            return slot.LocalId == localId && slot.Actor == actor;
        });
        Y_ABORT_UNLESS(it != actors.end());
        ++seen;
    });
    Y_ABORT_UNLESS(seen == actors.size());

    for (const auto& slot : actors) {
        Y_ABORT_UNLESS(mailbox.FindActor(slot.LocalId) == slot.Actor);
        for (ui64 alias : slot.Aliases) {
            Y_ABORT_UNLESS(mailbox.FindAlias(alias) == slot.Actor);
        }
    }
}

void RunMailboxFuzz(FuzzedDataProvider& fdp) {
    TMailbox mailbox;
    EMailboxState state = EMailboxState::Free;
    TVector<ui64> outstanding;
    TVector<TActorSlot> actors;
    ui64 nextCookie = 1;

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 14)) {
            case 0: {
                if (state == EMailboxState::Free) {
                    mailbox.LockFromFree();
                    state = EMailboxState::Locked;
                }
                break;
            }

            case 1: {
                if (state == EMailboxState::Unlocked && mailbox.TryLock()) {
                    state = EMailboxState::Locked;
                }
                break;
            }

            case 2: {
                if (state == EMailboxState::Locked && outstanding.empty()) {
                    if (mailbox.TryUnlock()) {
                        state = EMailboxState::Unlocked;
                    }
                } else if (state == EMailboxState::Locked) {
                    Y_ABORT_UNLESS(!mailbox.TryUnlock());
                }
                break;
            }

            case 3: {
                const ui64 localId = fdp.ConsumeIntegralInRange<ui64>(1, 64);
                auto ev = MakeEvent(nextCookie++, localId);
                const ui64 cookie = ev->Cookie;
                const EMailboxPush result = mailbox.Push(ev);
                if (result == EMailboxPush::Free) {
                    Y_ABORT_UNLESS(ev);
                } else {
                    Y_ABORT_UNLESS(!ev);
                    if (result == EMailboxPush::Locked) {
                        state = EMailboxState::Locked;
                    }
                    outstanding.push_back(cookie);
                }
                break;
            }

            case 4: {
                if (state == EMailboxState::Locked) {
                    const ui64 localId = fdp.ConsumeIntegralInRange<ui64>(1, 64);
                    auto ev = MakeEvent(nextCookie++, localId);
                    const ui64 cookie = ev->Cookie;
                    mailbox.PushFront(std::move(ev));
                    outstanding.push_back(cookie);
                }
                break;
            }

            case 5: {
                if (state != EMailboxState::Unlocked) {
                    if (auto ev = mailbox.Pop()) {
                        RemoveObserved(outstanding, ev->Cookie);
                    }
                }
                break;
            }

            case 6: {
                if (state != EMailboxState::Unlocked) {
                    mailbox.CountMailboxEvents(fdp.ConsumeIntegralInRange<ui64>(1, 64),
                        fdp.ConsumeIntegralInRange<ui32>(0, 64));
                }
                break;
            }

            case 7: {
                if (state == EMailboxState::Locked) {
                    mailbox.LockToFree();
                    state = EMailboxState::Free;
                }
                break;
            }

            case 8: {
                if (actors.size() < MaxActors) {
                    const ui64 localId = fdp.ConsumeIntegralInRange<ui64>(1, 128);
                    if (!HasActor(actors, localId)) {
                        IActor* actor = new TFuzzActor;
                        mailbox.AttachActor(localId, actor);
                        actors.push_back({localId, actor, {}});
                    }
                }
                break;
            }

            case 9: {
                if (!actors.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, actors.size() - 1);
                    TActorSlot slot = std::move(actors[index]);
                    IActor* detached = mailbox.DetachActor(slot.LocalId);
                    Y_ABORT_UNLESS(detached == slot.Actor);
                    delete detached;
                    actors.erase(actors.begin() + index);
                }
                break;
            }

            case 10: {
                if (!actors.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, actors.size() - 1);
                    const ui64 alias = fdp.ConsumeIntegralInRange<ui64>(129, 512);
                    if (!mailbox.FindAlias(alias) && !HasActor(actors, alias)) {
                        mailbox.AttachAlias(alias, actors[index].Actor);
                        actors[index].Aliases.insert(alias);
                    }
                }
                break;
            }

            case 11: {
                TVector<size_t> withAliases;
                for (size_t i = 0; i < actors.size(); ++i) {
                    if (!actors[i].Aliases.empty()) {
                        withAliases.push_back(i);
                    }
                }
                if (!withAliases.empty()) {
                    TActorSlot& slot = actors[withAliases[fdp.ConsumeIntegralInRange<size_t>(0, withAliases.size() - 1)]];
                    auto it = slot.Aliases.begin();
                    std::advance(it, fdp.ConsumeIntegralInRange<size_t>(0, slot.Aliases.size() - 1));
                    const ui64 alias = *it;
                    Y_ABORT_UNLESS(mailbox.DetachAlias(alias) == slot.Actor);
                    slot.Aliases.erase(alias);
                }
                break;
            }

            case 12: {
                if (!actors.empty()) {
                    mailbox.EnableStats();
                    mailbox.AddElapsedCycles(fdp.ConsumeIntegral<ui16>());
                    mailbox.GetElapsedCycles();
                    mailbox.GetElapsedSeconds();
                }
                break;
            }

            case 13: {
                CheckActors(mailbox, actors);
                break;
            }

            case 14: {
                if (state != EMailboxState::Unlocked) {
                    DrainMailbox(mailbox, outstanding);
                }
                break;
            }
        }

        Y_ABORT_UNLESS(outstanding.size() <= nextCookie);
        CheckActors(mailbox, actors);
    }

    if (state == EMailboxState::Unlocked) {
        Y_ABORT_UNLESS(mailbox.TryLock());
        state = EMailboxState::Locked;
    }
    DrainMailbox(mailbox, outstanding);
    Y_ABORT_UNLESS(outstanding.empty());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunMailboxFuzz(fdp);
    return 0;
}
