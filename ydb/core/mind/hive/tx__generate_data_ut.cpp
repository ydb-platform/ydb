#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxGenerateTestData : public TTransactionBase<THive> {
public:
    TTxGenerateTestData(THive *hive, uint64_t seed)
        : TBase(hive)
        , Seed(seed)
    {}

    TTxType GetTxType() const override { return -1; }

    const std::vector<TString> STORAGE_POOLS = {"def1", "def2", "def3", "def4", "def5", "def6", "def7"};
    static constexpr size_t NUM_TABLETS = 250'000;
    static constexpr size_t NUM_GROUPS = 30'000;

    uint64_t Seed;

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        std::mt19937 engine(Seed);
        TTabletId nextTabletId = 0x10000 + NUM_TABLETS * Seed;
        std::uniform_int_distribution<size_t> getNumChannels(1, 10);
        std::uniform_int_distribution<TStorageGroupId> getGroup(1, NUM_GROUPS);


        for (size_t i = 0; i < NUM_TABLETS; ++i) {
            auto tabletId = nextTabletId++;
            db.Table<Schema::Tablet>().Key(tabletId).Update<Schema::Tablet::KnownGeneration>(1);
            auto numChannels = getNumChannels(engine);
            for (size_t channel = 0; channel < numChannels; ++channel) {
                auto groupId = getGroup(engine);
                auto storagePool = STORAGE_POOLS[(groupId - 1) * STORAGE_POOLS.size() / NUM_GROUPS];
                db.Table<Schema::TabletChannel>().Key(tabletId, channel).Update<Schema::TabletChannel::StoragePool>(storagePool);
                db.Table<Schema::TabletChannelGen>().Key(tabletId, channel, 1).Update<Schema::TabletChannelGen::Group>(groupId);
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* THive::CreateGenerateTestData(uint64_t seed) {
    return new TTxGenerateTestData(this, seed);
}

} // NHive
} // NKikimr
