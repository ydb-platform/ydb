#pragma once
#include "defs.h"

namespace NKikimr {

static const ui32 Primes[128] = {
    104743, 105023, 105359, 105613,
    104759, 105031, 105361, 105619,
    104761, 105037, 105367, 105649,
    104773, 105071, 105373, 105653,
    104779, 105097, 105379, 105667,
    104789, 105107, 105389, 105673,
    104801, 105137, 105397, 105683,
    104803, 105143, 105401, 105691,
    104827, 105167, 105407, 105701,
    104831, 105173, 105437, 105727,
    104849, 105199, 105449, 105733,
    104851, 105211, 105467, 105751,
    104869, 105227, 105491, 105761,
    104879, 105229, 105499, 105767,
    104891, 105239, 105503, 105769,
    104911, 105251, 105509, 105817,
    104917, 105253, 105517, 105829,
    104933, 105263, 105527, 105863,
    104947, 105269, 105529, 105871,
    104953, 105277, 105533, 105883,
    104959, 105319, 105541, 105899,
    104971, 105323, 105557, 105907,
    104987, 105331, 105563, 105913,
    104999, 105337, 105601, 105929,
    105019, 105341, 105607, 105943,
    105953, 106261, 106487, 106753,
    105967, 106273, 106501, 106759,
    105971, 106277, 106531, 106781,
    105977, 106279, 106537, 106783,
    105983, 106291, 106541, 106787,
    105997, 106297, 106543, 106801,
    106013, 106303, 106591, 106823,
};

struct TStateStorageRingWalker {
    static auto Select(ui32 hash, ui32 sz, ui32 nToSelect)
    {
        std::vector<ui32> rings;
        rings.resize(nToSelect);
        std::unordered_set<ui32> ringsUsed;
        const ui32 delta = Primes[hash % 128];
        ui32 a = hash + delta;
        Y_DEBUG_ABORT_UNLESS(delta > sz);
        for (ui32 i : xrange(nToSelect)) {
            a += delta;
            rings[i] = a % sz;
            ringsUsed.insert(rings[i]);
        }
        if (ringsUsed.size() != nToSelect) {
            std::unordered_set<ui32> duplicates;
            for (ui32 i : xrange(nToSelect)) {
                if (!duplicates.insert(rings[i]).second) {
                    ui32 proposedRing = rings[i];
                    while (ringsUsed.count(proposedRing) > 0) {
                        proposedRing = (proposedRing + 1) % nToSelect;
                    }
                    rings[i] = proposedRing;
                    ringsUsed.insert(proposedRing);
                    duplicates.insert(proposedRing);
                }
            }
        }
        return rings;
    }
};
}
