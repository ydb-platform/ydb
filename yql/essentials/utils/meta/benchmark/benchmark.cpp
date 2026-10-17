#include <yql/essentials/utils/meta/hash.h>

#include <benchmark/benchmark.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

struct TPet {
    TString Name;
    ui64 Age = 0;
};

struct TPerson {
    TString Name;
    ui64 Age = 0;
    TVector<TPerson> Friends;
    TMaybe<TPet> Pet;
};

struct TManualPetHash {
    size_t operator()(const TPet& pet) const {
        return CombineHashes(THash<TString>{}(pet.Name), THash<ui64>{}(pet.Age));
    }
};

struct TManualPersonHash {
    size_t operator()(const TPerson& person) const {
        size_t hash = THash<TString>{}(person.Name);
        hash = CombineHashes(hash, THash<ui64>{}(person.Age));

        size_t friendsHash = 0;
        for (const auto& friendPerson : person.Friends) {
            friendsHash = CombineHashes(friendsHash, TManualPersonHash{}(friendPerson));
        }
        hash = CombineHashes(hash, friendsHash);

        const size_t petHash = person.Pet ? CombineHashes(size_t{1}, TManualPetHash{}(*person.Pet)) : size_t{0};
        return CombineHashes(hash, petHash);
    }
};

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(TPet, (Name)(Age));
YQL_DEFINE_REFLECTING(TPerson, (Name)(Age)(Friends)(Pet));

} // namespace NYql::NReflection

YQL_DERIVE_HASH(TPet);
YQL_DERIVE_HASH(TPerson);

namespace {

TPerson MakePerson() {
    return {
        .Name = "John",
        .Age = 30,
        .Friends = {
            {
                .Name = "Mary",
                .Age = 25,
                .Friends = {},
                .Pet = Nothing(),
            },
            {
                .Name = "Bob",
                .Age = 40,
                .Friends = {},
                .Pet = TPet{
                    .Name = "Kitty",
                    .Age = 2,
                },
            },
        },
    };
}

} // namespace

void HashManual(benchmark::State& state) {
    const auto person = MakePerson();

    for (const auto _ : state) {
        size_t hash = TManualPersonHash{}(person);
        benchmark::DoNotOptimize(hash);
    }
}

void HashReflecting(benchmark::State& state) {
    const auto person = MakePerson();

    for (const auto _ : state) {
        size_t hash = THash<TPerson>{}(person);
        benchmark::DoNotOptimize(hash);
    }
}

BENCHMARK(HashManual);
BENCHMARK(HashReflecting);
