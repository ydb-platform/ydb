#include "blobstorage_groupinfo_partlayout.h"
#include "blobstorage_groupinfo_sets.h"

#include <library/cpp/testing/unittest/registar.h>

#include <array>

#if defined(_unix_)
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace NKikimr;

namespace {

constexpr auto Species = TBlobStorageGroupType::Erasure8Plus2Block;
using TRows = std::array<ui32, 10>;

bool Augment(ui32 part, const TRows& rows, std::array<i32, 12>& owner, ui32& seen) {
    for (ui32 disk = 0; disk != owner.size(); ++disk) {
        const ui32 bit = 1u << disk;
        if ((rows[part] & bit) && !(seen & bit)) {
            seen |= bit;
            if (owner[disk] < 0 || Augment(owner[disk], rows, owner, seen)) {
                owner[disk] = part;
                return true;
            }
        }
    }
    return false;
}

ui32 MatchOracle(const TRows& rows, ui32 parts) {
    std::array<i32, 12> owner;
    owner.fill(-1);
    ui32 count = 0;
    for (ui32 part = 0; part != parts; ++part) {
        ui32 seen = 0;
        count += Augment(part, rows, owner, seen);
    }
    return count;
}

TSubgroupPartLayout MakeLayout(const TRows& rows, const TBlobStorageGroupType& type) {
    TSubgroupPartLayout layout;
    for (ui32 part = 0; part != type.TotalPartCount(); ++part) {
        for (ui32 disk = 0; disk != type.BlobSubgroupSize(); ++disk) {
            if (rows[part] & (1u << disk)) {
                layout.AddItem(disk, part, type);
            }
        }
    }
    return layout;
}

TRows ValidRows(ui32 main, ui32 handoff0, ui32 handoff1) {
    TRows rows{};
    for (ui32 part = 0; part != 10; ++part) {
        rows[part] = ((main >> part & 1) << part)
            | ((handoff0 >> part & 1) << 10) | ((handoff1 >> part & 1) << 11);
    }
    return rows;
}

ui32 Random(ui32& state) {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
}

#if defined(_unix_)
template<class F>
void AssertAborts(F&& action) {
    const pid_t pid = fork();
    UNIT_ASSERT(pid >= 0);
    if (!pid) {
        const struct rlimit noCore{0, 0};
        setrlimit(RLIMIT_CORE, &noCore);
        signal(SIGABRT, SIG_DFL);
        if (!freopen("/dev/null", "w", stderr)) {
            _exit(100);
        }
        action();
        _exit(0);
    }
    int status = 0;
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result < 0 && errno == EINTR);
    UNIT_ASSERT_VALUES_EQUAL(result, pid);
    UNIT_ASSERT(WIFSIGNALED(status));
    UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
}
#endif

} // anonymous namespace

Y_UNIT_TEST_SUITE(SubgroupWideLayout) {
    Y_UNIT_TEST(CellsAcrossBothWords) {
        const TBlobStorageGroupType type(Species);
        const TSubgroupPartLayout zero;
        UNIT_ASSERT_VALUES_EQUAL(zero.CountDistinctParts(type), 0);
        UNIT_ASSERT_VALUES_EQUAL(zero.CountEffectiveReplicas(type), 0);
        for (ui32 cell : {0, 63, 64, 119}) {
            const ui32 part = cell / 12, disk = cell % 12;
            TSubgroupPartLayout layout;
            layout.AddItem(disk, part, type);
            UNIT_ASSERT(layout != zero);
            for (ui32 i = 0; i != 10; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(i), i == part ? 1u << disk : 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(layout.CountDistinctParts(type), 1);
            layout.ClearItem(disk, part, type);
            UNIT_ASSERT(layout == zero);
        }
    }

    Y_UNIT_TEST(CrossWordRowMaskMergeAndIteration) {
        const TBlobStorageGroupType type(Species);
        TSubgroupPartLayout layout, high;
        for (ui32 disk = 0; disk != 12; ++disk) {
            (disk < 4 ? layout : high).AddItem(disk, 5, type);
        }
        layout.Merge(high, type);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(5), 0xfff);
        layout.AddItem(0, 0, type);
        layout.AddItem(11, 9, type);
        layout.Mask(5, 0xa59);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(5), 0xa59);
        layout.Mask(5, 0x30f);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(5), 0x209);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(9), 0x800);
        TRows visited{};
        layout.ForEachPartOfDisk(type, [&](ui32 part, ui32 disk) {
            UNIT_ASSERT(!(visited[part] & (1u << disk)));
            visited[part] |= 1u << disk;
        });
        UNIT_ASSERT(MakeLayout(visited, type) == layout);
        UNIT_ASSERT_VALUES_EQUAL(layout.ToString(type),
            "{000000000001 000000000000 000000000000 000000000000 000000000000 "
            "001000001001 000000000000 000000000000 000000000000 100000000000}");
        layout.Mask(5, 0);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(5), 0);
        UNIT_ASSERT_VALUES_EQUAL(layout.CountDistinctParts(type), 2);
    }

    Y_UNIT_TEST(EffectiveReplicaAdversariesAndInvolvedDisks) {
        TBlobStorageGroupInfo info(Species, 2, 12);
        const auto& type = info.Type;
        UNIT_ASSERT_VALUES_EQUAL(type.BlobSubgroupSize(), 12);
        UNIT_ASSERT_VALUES_EQUAL(type.Handoff(), 2);
        for (const auto& item : std::array<std::array<ui32, 4>, 8>{{
                {0x3ff, 0, 0, 10}, {0, 0x3ff, 0, 1}, {0, 0x3ff, 0x3ff, 2},
                {0x3fc, 3, 0, 9}, {0x3fc, 3, 1, 10}, {0x3fc, 1, 1, 9},
                {0x1ff, 0x200, 0, 10}, {0xff, 0x300, 0x100, 10}}}) {
            const auto rows = ValidRows(item[0], item[1], item[2]);
            const auto layout = MakeLayout(rows, type);
            UNIT_ASSERT_VALUES_EQUAL(layout.CountEffectiveReplicas(type), item[3]);
            UNIT_ASSERT_VALUES_EQUAL(layout.CountEffectiveReplicas(type), MatchOracle(rows, 10));
            ui32 diskMask = 0;
            for (ui32 row : rows) {
                diskMask |= row;
            }
            UNIT_ASSERT(layout.GetInvolvedDisks(&info.GetTopology())
                == TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), diskMask));
        }
    }

    Y_UNIT_TEST(TenThousandSeededValidLayouts) {
        const TBlobStorageGroupType type(Species);
        ui32 cases = 0;
        for (ui32 seed : {0x13579bdfu, 0x2468ace1u, 0x9e3779b9u, 0xdeadbeefu}) {
            ui32 state = seed;
            for (ui32 iteration = 0; iteration != 2500; ++iteration) {
                ui32 main = Random(state) & 0x3ff;
                ui32 h0 = Random(state) & 0x3ff;
                ui32 h1 = Random(state) & 0x3ff;
                if (iteration % 4 == 0) {
                    main = 0x3ff & ~(1u << (Random(state) % 10)) & ~(1u << (Random(state) % 10));
                } else if (iteration % 4 == 1) {
                    h0 &= 1u << (Random(state) % 10);
                    h1 &= 1u << (Random(state) % 10);
                }
                const auto rows = ValidRows(main, h0, h1);
                const auto layout = MakeLayout(rows, type);
                UNIT_ASSERT_VALUES_EQUAL_C(layout.CountEffectiveReplicas(type), MatchOracle(rows, 10),
                    "seed# " << seed << " iteration# " << iteration << " layout# " << layout.ToString(type));
                ++cases;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(cases, 10000);
    }

    Y_UNIT_TEST(OldSpeciesIncludingTenDiskSubgroup) {
        for (ui32 species = 0; species != 19; ++species) {
            const TBlobStorageGroupType type{TBlobStorageGroupType::EErasureSpecies(species)};
            TSubgroupPartLayout layout;
            for (ui32 part = 0; part != type.TotalPartCount(); ++part) {
                layout.AddItem(type.BlobSubgroupSize() - 1, part, type);
                UNIT_ASSERT_VALUES_EQUAL(layout.GetDisksWithPart(part), 1u << (type.BlobSubgroupSize() - 1));
            }
            UNIT_ASSERT_VALUES_EQUAL(layout.CountDistinctParts(type), type.TotalPartCount());
            if (species != TBlobStorageGroupType::ErasureMirror3dc
                    && species != TBlobStorageGroupType::ErasureMirror3of4) {
                TRows rows{};
                for (ui32 part = 0; part != type.TotalPartCount(); ++part) {
                    rows[part] = 1u << part;
                    if (type.Handoff()) {
                        rows[part] |= 1u << (type.BlobSubgroupSize() - 1);
                    }
                }
                const auto valid = MakeLayout(rows, type);
                UNIT_ASSERT_VALUES_EQUAL(valid.CountEffectiveReplicas(type), MatchOracle(rows, type.TotalPartCount()));
            }
        }
    }

    Y_UNIT_TEST(LayoutGeneratorHasBoundedWideDigitCoverage) {
        const TBlobStorageGroupType type(Species);
        ui32 mainOnly = 0;
        TSubgroupPartLayout::GeneratePossibleLayouts(type, 0, [&](const auto& layout) {
            for (ui32 part = 0; part != 10; ++part) {
                UNIT_ASSERT(!(layout.GetDisksWithPart(part) & (3u << 10)));
            }
            ++mainOnly;
        });
        UNIT_ASSERT_VALUES_EQUAL(mainOnly, 1024);

        // Main disks are the ten low mixed-radix digits. Stop immediately after
        // the first handoff digit reaches 256; exhaustively visiting 2^30 is forbidden.
        ui32 visited = 0;
        bool wideDigitSeen = false;
        TSubgroupPartLayout::GeneratePossibleLayouts(type, 10, [&](const auto& layout) {
            wideDigitSeen |= bool(layout.GetDisksWithPart(8) & (1u << 10));
            ++visited;
        }, 256 * 1024 + 1);
        UNIT_ASSERT_VALUES_EQUAL(visited, 256 * 1024 + 1);
        UNIT_ASSERT(wideDigitSeen);
    }

#if defined(_unix_)
    Y_UNIT_TEST(ReservedRowAndInvalidDiskAreRejected) {
        const TBlobStorageGroupType type(Species);
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.AddItem(0, 10, type);
        });
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.AddItem(12, 0, type);
        });
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.ClearItem(0, 10, type);
        });
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.GetDisksWithPart(10);
        });
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.Mask(10, 0);
        });
        AssertAborts([&] {
            TSubgroupPartLayout layout;
            layout.Mask(0, 1u << 12);
        });
    }
#endif
}
