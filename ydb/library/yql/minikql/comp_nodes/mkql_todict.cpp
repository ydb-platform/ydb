#include "mkql_todict.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_list_adapter.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/utils/cast.h>
#include <ydb/library/yql/utils/hash.h>

#include <algorithm>
#include <unordered_map>
#include <optional>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class THashedMultiMapAccumulator {
    using TMapType = TValuesDictHashMap;

    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    bool IsTuple;
    std::shared_ptr<TValuePacker> Packer;
    const NUdf::IHash* Hash;
    const NUdf::IEquate* Equate;

    TMapType Map;

public:
    static constexpr bool IsSorted = false;

    THashedMultiMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyType(keyType), KeyTypes(keyTypes), IsTuple(isTuple), Hash(hash), Equate(equate)
        , Map(0, TValueHasher(KeyTypes, isTuple, hash), TValueEqual(KeyTypes, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        auto it = Map.find(key);
        if (it == Map.end()) {
            it = Map.emplace(std::move(key), Ctx.HolderFactory.NewVectorHolder()).first;
        }
        it->second.Push(std::move(payload));
    }

    NUdf::TUnboxedValue Build()
    {
        const auto filler = [this](TValuesDictHashMap& targetMap) {
            targetMap = std::move(Map);
        };

        return Ctx.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

class THashedMapAccumulator {
    using TMapType = TValuesDictHashMap;

    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    const bool IsTuple;
    std::shared_ptr<TValuePacker> Packer;
    const NUdf::IHash* Hash;
    const NUdf::IEquate* Equate;

    TMapType Map;

public:
    static constexpr bool IsSorted = false;

    THashedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyType(keyType), KeyTypes(keyTypes), IsTuple(isTuple), Hash(hash), Equate(equate)
        , Map(0, TValueHasher(KeyTypes, isTuple, hash), TValueEqual(KeyTypes, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        Map.emplace(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build()
    {
        const auto filler = [this](TMapType& targetMap) {
            targetMap = std::move(Map);
        };

        return Ctx.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

template<typename T, bool OptionalKey>
class THashedSingleFixedMultiMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    TComputationContext& Ctx;
    const TKeyTypes& KeyTypes;
    TMapType Map;
    TUnboxedValueVector NullPayloads;
    NUdf::TUnboxedValue CurrentEmptyVectorForInsert;
public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedMultiMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyTypes(keyTypes), Map(0, TMyHash<T>(), TMyEquals<T>()) {
        Y_UNUSED(keyType);
        Y_UNUSED(payloadType);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.reserve(itemsCountHint);
        CurrentEmptyVectorForInsert = Ctx.HolderFactory.NewVectorHolder();
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayloads.emplace_back(std::move(payload));
                return;
            }
        }
        auto insertInfo = Map.emplace(key.Get<T>(), CurrentEmptyVectorForInsert);
        if (insertInfo.second) {
            CurrentEmptyVectorForInsert = Ctx.HolderFactory.NewVectorHolder();
        }
        insertInfo.first->second.Push(payload.Release());
    }

    NUdf::TUnboxedValue Build() {
        std::optional<NUdf::TUnboxedValue> nullPayload;
        if (NullPayloads.size()) {
            nullPayload = Ctx.HolderFactory.VectorAsVectorHolder(std::move(NullPayloads));
        }
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map), std::move(nullPayload));
    }
};

template<typename T, bool OptionalKey>
class THashedSingleFixedMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    TComputationContext& Ctx;
    TMapType Map;
    std::optional<NUdf::TUnboxedValue> NullPayload;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Map(0, TMyHash<T>(), TMyEquals<T>())
    {
        Y_UNUSED(keyType);
        Y_UNUSED(payloadType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload.emplace(std::move(payload));
                return;
            }
        }
        Map.emplace(key.Get<T>(), std::move(payload));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayload));
    }
};

class THashedSetAccumulator {
    using TSetType = TValuesDictHashSet;

    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    bool IsTuple;
    std::shared_ptr<TValuePacker> Packer;
    TSetType Set;
    const NUdf::IHash* Hash;
    const NUdf::IEquate* Equate;

public:
    static constexpr bool IsSorted = false;

    THashedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyType(keyType), KeyTypes(keyTypes), IsTuple(isTuple), Set(0, TValueHasher(KeyTypes, isTuple, hash),
        TValueEqual(KeyTypes, isTuple, equate)), Hash(hash), Equate(equate)
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Set.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key)
    {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        Set.emplace(std::move(key));
    }

    NUdf::TUnboxedValue Build()
    {
        const auto filler = [this](TSetType& targetSet) {
            targetSet = std::move(Set);
        };

        return Ctx.HolderFactory.CreateDirectHashedSetHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedSetAccumulator {
    using TSetType = TValuesDictHashSingleFixedSet<T>;

    TComputationContext& Ctx;
    TSetType Set;
    bool HasNull = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Set(0, TMyHash<T>(), TMyEquals<T>())
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key)
    {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull = true;
                return;
            }
        }
        Set.emplace(key.Get<T>());
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedSetHolder<T, OptionalKey>(std::move(Set), HasNull);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactSetAccumulator {
    using TSetType = TValuesDictHashSingleFixedCompactSet<T>;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TSetType Set;
    bool HasNull = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Set(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key)
    {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull = true;
                return;
            }
        }
        Set.Insert(key.Get<T>());
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactSetHolder<T, OptionalKey>(std::move(Set), HasNull);
    }
};

class THashedCompactSetAccumulator {
    using TSetType = TValuesDictHashCompactSet;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TSetType Set;
    TType *KeyType;
    std::shared_ptr<TValuePacker> KeyPacker;

public:
    static constexpr bool IsSorted = false;

    THashedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Set(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR, TSmallValueHash(), TSmallValueEqual())
        , KeyType(keyType), KeyPacker(std::make_shared<TValuePacker>(true, keyType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key)
    {
        Set.Insert(AddSmallValue(Pool, KeyPacker->Pack(key)));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedCompactSetHolder(std::move(Set), std::move(Pool), KeyType, &Ctx);
    }
};

template <bool Multi>
class THashedCompactMapAccumulator;

template <>
class THashedCompactMapAccumulator<false> {
    using TMapType = TValuesDictHashCompactMap;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TMapType Map;
    TType *KeyType, *PayloadType;
    std::shared_ptr<TValuePacker> KeyPacker, PayloadPacker;

public:
    static constexpr bool IsSorted = false;

    THashedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType(keyType), PayloadType(payloadType)
        , KeyPacker(std::make_shared<TValuePacker>(true, keyType))
        , PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        Map.InsertNew(AddSmallValue(Pool, KeyPacker->Pack(key)), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedCompactMapHolder(std::move(Map), std::move(Pool), KeyType, PayloadType, &Ctx);
    }
};

template <>
class THashedCompactMapAccumulator<true> {
    using TMapType = TValuesDictHashCompactMultiMap;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TMapType Map;
    TType *KeyType, *PayloadType;
    std::shared_ptr<TValuePacker> KeyPacker, PayloadPacker;

public:
    static constexpr bool IsSorted = false;

    THashedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType(keyType), PayloadType(payloadType)
        , KeyPacker(std::make_shared<TValuePacker>(true, keyType))
        , PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        Map.Insert(AddSmallValue(Pool, KeyPacker->Pack(key)), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedCompactMultiMapHolder(std::move(Map), std::move(Pool), KeyType, PayloadType, &Ctx);
    }
};

template <typename T, bool OptionalKey, bool Multi>
class THashedSingleFixedCompactMapAccumulator;

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, false> {
    using TMapType = TValuesDictHashSingleFixedCompactMap<T>;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TMapType Map;
    std::optional<ui64> NullPayload;
    TType* PayloadType;
    std::shared_ptr<TValuePacker> PayloadPacker;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType(payloadType), PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload = AddSmallValue(Pool, PayloadPacker->Pack(payload));
                return;
            }
        }
        Map.InsertNew(key.Get<T>(), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayload), std::move(Pool), PayloadType, &Ctx);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, true> {
    using TMapType = TValuesDictHashSingleFixedCompactMultiMap<T>;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TMapType Map;
    std::vector<ui64> NullPayloads;
    TType* PayloadType;
    std::shared_ptr<TValuePacker> PayloadPacker;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), Pool(&Ctx.HolderFactory.GetPagePool()), Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType(payloadType), PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(keyType);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayloads.push_back(AddSmallValue(Pool, PayloadPacker->Pack(payload)));
                return;
            }
        }
        Map.Insert(key.Get<T>(), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build()
    {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactMultiMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayloads), std::move(Pool), PayloadType, &Ctx);
    }
};

class TSortedSetAccumulator {
    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    bool IsTuple;
    const NUdf::ICompare* Compare;
    const NUdf::IEquate* Equate;

    std::optional<TGenericPresortEncoder> Packer;
    TUnboxedValueVector Items;

public:
    static constexpr bool IsSorted = true;

    TSortedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyType(keyType), KeyTypes(keyTypes), IsTuple(isTuple), Compare(compare), Equate(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer.emplace(KeyType);
        }

        Items.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key)
    {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key));
    }

    NUdf::TUnboxedValue Build()
    {
        const TSortedSetFiller filler = [this](TUnboxedValueVector& values) {
            std::stable_sort(Items.begin(), Items.end(), TValueLess(KeyTypes, IsTuple, Compare));
            Items.erase(std::unique(Items.begin(), Items.end(), TValueEqual(KeyTypes, IsTuple, Equate)), Items.end());
            values = std::move(Items);
        };

        return Ctx.HolderFactory.CreateDirectSortedSetHolder(filler, KeyTypes, IsTuple,
            EDictSortMode::SortedUniqueAscending, true, Packer ? KeyType : nullptr, Compare, Equate);
    }
};

template<bool IsMulti>
class TSortedMapAccumulator;

template<>
class TSortedMapAccumulator<false> {
    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    bool IsTuple;
    const NUdf::ICompare* Compare;
    const NUdf::IEquate* Equate;
    std::optional<TGenericPresortEncoder> Packer;

    TKeyPayloadPairVector Items;

public:
    static constexpr bool IsSorted = true;

    TSortedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
    const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx)
        , KeyType(keyType)
        , KeyTypes(keyTypes)
        , IsTuple(isTuple)
        , Compare(compare)
        , Equate(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer.emplace(KeyType);
        }

        Y_UNUSED(payloadType);
        Items.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build()
    {
        const TSortedDictFiller filler = [this](TKeyPayloadPairVector& values) {
            values = std::move(Items);
        };

        return Ctx.HolderFactory.CreateDirectSortedDictHolder(filler, KeyTypes, IsTuple, EDictSortMode::RequiresSorting,
            true, Packer ? KeyType : nullptr, Compare, Equate);
    }
};

template<>
class TSortedMapAccumulator<true> {
    TComputationContext& Ctx;
    TType* KeyType;
    const TKeyTypes& KeyTypes;
    bool IsTuple;
    const NUdf::ICompare* Compare;
    const NUdf::IEquate* Equate;
    std::optional<TGenericPresortEncoder> Packer;
    TKeyPayloadPairVector Items;

public:
    static constexpr bool IsSorted = true;

    TSortedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
        const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx), KeyType(keyType), KeyTypes(keyTypes), IsTuple(isTuple), Compare(compare), Equate(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer.emplace(KeyType);
        }

        Y_UNUSED(payloadType);
        Items.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload)
    {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build()
    {
        const TSortedDictFiller filler = [this](TKeyPayloadPairVector& values) {
            std::stable_sort(Items.begin(), Items.end(), TKeyPayloadPairLess(KeyTypes, IsTuple, Compare));

            TKeyPayloadPairVector groups;
            groups.reserve(Items.size());
            if (!Items.empty()) {
                TDefaultListRepresentation currentList(std::move(Items.begin()->second));
                auto lastKey = std::move(Items.begin()->first);
                TValueEqual eqPredicate(KeyTypes, IsTuple, Equate);
                for (auto it = Items.begin() + 1; it != Items.end(); ++it) {
                    if (eqPredicate(lastKey, it->first)) {
                        currentList = currentList.Append(std::move(it->second));
                    }
                    else {
                        auto payload = Ctx.HolderFactory.CreateDirectListHolder(std::move(currentList));
                        groups.emplace_back(std::move(lastKey), std::move(payload));
                        currentList = TDefaultListRepresentation(std::move(it->second));
                        lastKey =  std::move(it->first);
                    }
                }

                auto payload = Ctx.HolderFactory.CreateDirectListHolder(std::move(currentList));
                groups.emplace_back(std::move(lastKey), std::move(payload));
            }

            values = std::move(groups);
        };

        return Ctx.HolderFactory.CreateDirectSortedDictHolder(filler, KeyTypes, IsTuple,
            EDictSortMode::SortedUniqueAscending, true, Packer ? KeyType : nullptr, Compare, Equate);
    }
};

template <typename TSetAccumulator, bool IsStream>
class TSetWrapper : public TMutableComputationNode<TSetWrapper<TSetAccumulator, IsStream>> {
    typedef TMutableComputationNode<TSetWrapper<TSetAccumulator, IsStream>> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
            IComputationNode* const key, TSetAccumulator&& setAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input(std::move(input))
            , Item(item)
            , Key(key)
            , SetAccum(std::move(setAccum))
            , Ctx(ctx) {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (auto status = Input.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item->SetValue(Ctx, std::move(item));
                        SetAccum.Add(Key->GetValue(Ctx));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = SetAccum.Build();
                        Finished = true;
                        return NUdf::EFetchStatus::Ok;
                    }
                    case NUdf::EFetchStatus::Yield: {
                        return NUdf::EFetchStatus::Yield;
                    }
                }
            }
        }

        NUdf::TUnboxedValue Input;
        IComputationExternalNode* const Item;
        IComputationNode* const Key;
        TSetAccumulator SetAccum;
        TComputationContext& Ctx;
        bool Finished = false;
    };

    TSetWrapper(TComputationMutables& mutables, TType* keyType, IComputationNode* list, IComputationExternalNode* item,
        IComputationNode* key, ui64 itemsCountHint)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType(keyType)
        , List(list)
        , Item(item)
        , Key(key)
        , ItemsCountHint(itemsCountHint)
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TSetAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TSetAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if constexpr (IsStream) {
            return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Item, Key,
                TSetAccumulator(KeyType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
                    ctx, ItemsCountHint), ctx);
        }

        const auto& list = List->GetValue(ctx);
        auto itemsCountHint = ItemsCountHint;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength())
                itemsCountHint = size;
            else
                return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        TSetAccumulator accumulator(KeyType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
            ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
            [this, &accumulator, &ctx] (NUdf::TUnboxedValue&& item) {
                Item->SetValue(ctx, std::move(item));
                accumulator.Add(Key->GetValue(ctx));
            }
        );

        return accumulator.Build().Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->Own(Item);
        this->DependsOn(Key);
    }

    TType* const KeyType;
    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const Key;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

#ifndef MKQL_DISABLE_CODEGEN
template <class TLLVMBase>
class TLLVMFieldsStructureStateWithAccum: public TLLVMBase {
private:
    using TBase = TLLVMBase;
    llvm::PointerType* StructPtrType;
protected:
    using TBase::Context;
public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StructPtrType); //accumulator
        return result;
    }

    llvm::Constant* GetAccumulator() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    TLLVMFieldsStructureStateWithAccum(llvm::LLVMContext& context)
        : TBase(context)
        , StructPtrType(PointerType::getUnqual(StructType::get(context))) {
    }
};
#endif

template <typename TSetAccumulator>
class TSqueezeSetFlowWrapper : public TStatefulFlowCodegeneratorNode<TSqueezeSetFlowWrapper<TSetAccumulator>> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeSetFlowWrapper<TSetAccumulator>>;
public:
    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        TState(TMemoryUsageInfo* memInfo, TSetAccumulator&& setAccum)
            : TBase(memInfo), SetAccum(std::move(setAccum)) {}

        NUdf::TUnboxedValuePod Build() {
            return SetAccum.Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum.Add(value);
        }

    private:
        TSetAccumulator SetAccum;
    };

    TSqueezeSetFlowWrapper(TComputationMutables& mutables, TType* keyType,
        IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, ui64 itemsCountHint)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , Flow(flow)
        , Item(item)
        , Key(key)
        , ItemsCountHint(itemsCountHint)
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TSetAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TSetAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state.Release();
        } else if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (auto item = Flow->GetValue(ctx); item.IsYield()) {
                return item.Release();
            } else if (item.IsFinish()) {
                const auto dict = statePtr->Build();
                state = std::move(item);
                return dict;
            } else {
                Item->SetValue(ctx, std::move(item));
                statePtr->Insert(Key->GetValue(ctx).Release());
            }
        }
        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItemArg, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        TLLVMFieldsStructureStateWithAccum<TLLVMFieldsStructure<TComputationValue<TState>>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSqueezeSetFlowWrapper<TSetAccumulator>::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto plus = BasicBlock::Create(context, "plus", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        result->addIncoming(GetFinish(context), block);

        BranchInst::Create(over, more, IsFinish(state, block), block);

        block = more;

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto key = GetNodeValue(Key, ctx, block);

        const auto insert = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Insert));

        const auto keyArg = WrapArgumentForWindows(key, ctx, block);

        const auto insType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), keyArg->getType()}, false);
        const auto insPtr = CastInst::Create(Instruction::IntToPtr, insert, PointerType::getUnqual(insType), "insert", block);
        CallInst::Create(insType, insPtr, {stateArg, keyArg}, "", block);

        BranchInst::Create(more, block);

        block = done;

        const auto build = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Build));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {stateArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            const auto dict = CallInst::Create(funType, funcPtr, {stateArg}, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), ptr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            CallInst::Create(funType, funcPtr, {stateArg, ptr}, "", block);
            const auto dict = new LoadInst(valueType, ptr, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        }

        new StoreInst(item, statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(TSetAccumulator(KeyType, KeyTypes, IsTuple, Encoded,
            Compare.Get(), Equate.Get(), Hash.Get(), ctx, ItemsCountHint));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, Item);
            this->DependsOn(flow, Key);
        }
    }

    TType* const KeyType;
    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const Key;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

template <typename TSetAccumulator>
class TSqueezeSetWideWrapper : public TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper<TSetAccumulator>> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper<TSetAccumulator>>;
public:
    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        TState(TMemoryUsageInfo* memInfo, TSetAccumulator&& setAccum)
            : TBase(memInfo), SetAccum(std::move(setAccum)) {}

        NUdf::TUnboxedValuePod Build() {
            return SetAccum.Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum.Add(value);
        }

    private:
        TSetAccumulator SetAccum;
    };

    TSqueezeSetWideWrapper(TComputationMutables& mutables, TType* keyType,
        IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key, ui64 itemsCountHint)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , Flow(flow)
        , Items(std::move(items))
        , Key(key)
        , ItemsCountHint(itemsCountHint)
        , PasstroughKey(GetPasstroughtMap(TComputationNodePtrVector{Key}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TSetAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TSetAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state.Release();
        } else if (!state.HasValue()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Key == Items[i] || Items[i]->GetDependencesCount() > 0U)
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (const auto result = Flow->FetchValues(ctx, fields)) {
                case EFetchResult::One:
                    statePtr->Insert(Key->GetValue(ctx).Release());
                    continue;
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                case EFetchResult::Finish: {
                    const auto dict = statePtr->Build();
                    state = NUdf::TUnboxedValuePod::MakeFinish();
                    return dict;
                }
            }
        }
        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        TLLVMFieldsStructureStateWithAccum<TLLVMFieldsStructure<TComputationValue<TState>>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSqueezeSetWideWrapper<TSetAccumulator>::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto plus = BasicBlock::Create(context, "plus", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        result->addIncoming(GetFinish(context), block);

        BranchInst::Create(over, more, IsFinish(state, block), block);

        block = more;

         const auto getres = GetNodeValues(Flow, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!PasstroughKey) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
        }

        const auto key = PasstroughKey ? getres.second[*PasstroughKey](ctx, block) : GetNodeValue(Key, ctx, block);

        const auto insert = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Insert));

        const auto keyArg = WrapArgumentForWindows(key, ctx, block);

        const auto insType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), keyArg->getType()}, false);
        const auto insPtr = CastInst::Create(Instruction::IntToPtr, insert, PointerType::getUnqual(insType), "insert", block);
        CallInst::Create(insType, insPtr, {stateArg, keyArg}, "", block);

        BranchInst::Create(more, block);

        block = done;

        const auto build = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Build));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {stateArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            const auto dict = CallInst::Create(funType, funcPtr, {stateArg}, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), ptr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            CallInst::Create(funType, funcPtr, {stateArg, ptr}, "", block);
            const auto dict = new LoadInst(valueType, ptr, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        }

        new StoreInst(GetFinish(context), statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(TSetAccumulator(KeyType, KeyTypes, IsTuple, Encoded,
            Compare.Get(), Equate.Get(), Hash.Get(), ctx, ItemsCountHint));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TSqueezeSetWideWrapper::Own, flow, std::placeholders::_1));
            this->DependsOn(flow, Key);
        }
    }

    TType* const KeyType;
    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const Key;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    const std::optional<size_t> PasstroughKey;

    const ui32 WideFieldsIndex;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

template <typename TMapAccumulator, bool IsStream>
class TMapWrapper : public TMutableComputationNode<TMapWrapper<TMapAccumulator, IsStream>> {
    typedef TMutableComputationNode<TMapWrapper<TMapAccumulator, IsStream>> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
            IComputationNode* const key, IComputationNode* const payload, TMapAccumulator&& mapAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input(std::move(input))
            , Item(item)
            , Key(key)
            , Payload(payload)
            , MapAccum(std::move(mapAccum))
            , Ctx(ctx) {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (auto status = Input.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item->SetValue(Ctx, std::move(item));
                        MapAccum.Add(Key->GetValue(Ctx), Payload->GetValue(Ctx));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = MapAccum.Build();
                        Finished = true;
                        return NUdf::EFetchStatus::Ok;
                    }
                    case NUdf::EFetchStatus::Yield: {
                        return NUdf::EFetchStatus::Yield;
                    }
                }
            }
        }

        NUdf::TUnboxedValue Input;
        IComputationExternalNode* const Item;
        IComputationNode* const Key;
        IComputationNode* const Payload;
        TMapAccumulator MapAccum;
        TComputationContext& Ctx;
        bool Finished = false;
    };

    TMapWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType, IComputationNode* list, IComputationExternalNode* item,
        IComputationNode* key, IComputationNode* payload, ui64 itemsCountHint)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , List(list)
        , Item(item)
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TMapAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TMapAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if constexpr (IsStream) {
            return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Item, Key, Payload,
                TMapAccumulator(KeyType, PayloadType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
                    ctx, ItemsCountHint), ctx);
        }

        const auto& list = List->GetValue(ctx);

        auto itemsCountHint = ItemsCountHint;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength())
                itemsCountHint = size;
            else
                return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        TMapAccumulator accumulator(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
            Compare.Get(), Equate.Get(), Hash.Get(), ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
            [this, &accumulator, &ctx] (NUdf::TUnboxedValue&& item) {
                Item->SetValue(ctx, std::move(item));
                accumulator.Add(Key->GetValue(ctx), Payload->GetValue(ctx));
            }
        );

        return accumulator.Build().Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->Own(Item);
        this->DependsOn(Key);
        this->DependsOn(Payload);
    }

    TType* const KeyType;
    TType* PayloadType;
    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const Key;
    IComputationNode* const Payload;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

template <typename TMapAccumulator>
class TSqueezeMapFlowWrapper : public TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper<TMapAccumulator>> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper<TMapAccumulator>>;
public:
    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        TState(TMemoryUsageInfo* memInfo, TMapAccumulator&& mapAccum)
            : TBase(memInfo), MapAccum(std::move(mapAccum)) {}

        NUdf::TUnboxedValuePod Build() {
            return MapAccum.Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum.Add(key, value);
        }

    private:
        TMapAccumulator MapAccum;
    };

    TSqueezeMapFlowWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
        IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, IComputationNode* payload,
        ui64 itemsCountHint)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , Flow(flow)
        , Item(item)
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TMapAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TMapAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (auto item = Flow->GetValue(ctx); item.IsYield()) {
                return item.Release();
            } else if (item.IsFinish()) {
                const auto dict = statePtr->Build();
                state = std::move(item);
                return dict;
            } else {
                Item->SetValue(ctx, std::move(item));
                statePtr->Insert(Key->GetValue(ctx).Release(), Payload->GetValue(ctx).Release());
            }
        }
        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItemArg, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        TLLVMFieldsStructureStateWithAccum<TLLVMFieldsStructure<TComputationValue<TState>>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSqueezeMapFlowWrapper<TMapAccumulator>::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto plus = BasicBlock::Create(context, "plus", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        result->addIncoming(GetFinish(context), block);

        BranchInst::Create(over, more, IsFinish(state, block), block);

        block = more;

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto key = GetNodeValue(Key, ctx, block);
        const auto payload = GetNodeValue(Payload, ctx, block);

        const auto insert = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Insert));

        const auto keyArg = WrapArgumentForWindows(key, ctx, block);
        const auto payloadArg = WrapArgumentForWindows(payload, ctx, block);

        const auto insType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), keyArg->getType(), payloadArg->getType()}, false);
        const auto insPtr = CastInst::Create(Instruction::IntToPtr, insert, PointerType::getUnqual(insType), "insert", block);
        CallInst::Create(insType, insPtr, {stateArg, keyArg, payloadArg}, "", block);

        BranchInst::Create(more, block);

        block = done;

        const auto build = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Build));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {stateArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            const auto dict = CallInst::Create(funType, funcPtr, {stateArg}, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), ptr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            CallInst::Create(funType, funcPtr, {stateArg, ptr}, "", block);
            const auto dict = new LoadInst(valueType, ptr, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        }

        new StoreInst(item, statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(TMapAccumulator(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
            Compare.Get(), Equate.Get(), Hash.Get(), ctx, ItemsCountHint));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, Item);
            this->DependsOn(flow, Key);
            this->DependsOn(flow, Payload);
        }
    }

    TType* const KeyType;
    TType* PayloadType;
    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const Key;
    IComputationNode* const Payload;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

template <typename TMapAccumulator>
class TSqueezeMapWideWrapper : public TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper<TMapAccumulator>> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper<TMapAccumulator>>;
public:
    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        TState(TMemoryUsageInfo* memInfo, TMapAccumulator&& mapAccum)
            : TBase(memInfo), MapAccum(std::move(mapAccum)) {}

        NUdf::TUnboxedValuePod Build() {
            return MapAccum.Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum.Add(key, value);
        }

    private:
        TMapAccumulator MapAccum;
    };

    TSqueezeMapWideWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
        IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key, IComputationNode* payload,
        ui64 itemsCountHint)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , Flow(flow)
        , Items(std::move(items))
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
        , PasstroughKey(GetPasstroughtMap(TComputationNodePtrVector{Key}, Items).front())
        , PasstroughPayload(GetPasstroughtMap(TComputationNodePtrVector{Payload}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && TMapAccumulator::IsSorted ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !TMapAccumulator::IsSorted ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (!state.HasValue()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Key == Items[i] || Payload == Items[i] || Items[i]->GetDependencesCount() > 0U)
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (const auto result = Flow->FetchValues(ctx, fields)) {
                case EFetchResult::One:
                    statePtr->Insert(Key->GetValue(ctx).Release(), Payload->GetValue(ctx).Release());
                    continue;
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                case EFetchResult::Finish: {
                    const auto dict = statePtr->Build();
                    state = NUdf::TUnboxedValuePod::MakeFinish();
                    return dict;
                }
            }
        }
        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        TLLVMFieldsStructureStateWithAccum<TLLVMFieldsStructure<TComputationValue<TState>>> fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSqueezeMapWideWrapper<TMapAccumulator>::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto plus = BasicBlock::Create(context, "plus", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        result->addIncoming(GetFinish(context), block);

        BranchInst::Create(over, more, IsFinish(state, block), block);

        block = more;

        const auto getres = GetNodeValues(Flow, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!(PasstroughKey && PasstroughPayload)) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
        }

        const auto key = PasstroughKey ? getres.second[*PasstroughKey](ctx, block) : GetNodeValue(Key, ctx, block);
        const auto payload = PasstroughPayload ? getres.second[*PasstroughPayload](ctx, block) : GetNodeValue(Payload, ctx, block);

        const auto insert = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Insert));

        const auto keyArg = WrapArgumentForWindows(key, ctx, block);
        const auto payloadArg = WrapArgumentForWindows(payload, ctx, block);

        const auto insType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), keyArg->getType(), payloadArg->getType()}, false);
        const auto insPtr = CastInst::Create(Instruction::IntToPtr, insert, PointerType::getUnqual(insType), "insert", block);
        CallInst::Create(insType, insPtr, {stateArg, keyArg, payloadArg}, "", block);

        BranchInst::Create(more, block);

        block = done;

        const auto build = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Build));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {stateArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            const auto dict = CallInst::Create(funType, funcPtr, {stateArg}, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), ptr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, build, PointerType::getUnqual(funType), "build", block);
            CallInst::Create(funType, funcPtr, {stateArg, ptr}, "", block);
            const auto dict = new LoadInst(valueType, ptr, "dict", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(dict, block);
        }

        new StoreInst(GetFinish(context), statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(TMapAccumulator(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
            Compare.Get(), Equate.Get(), Hash.Get(), ctx, ItemsCountHint));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TSqueezeMapWideWrapper::Own, flow, std::placeholders::_1));
            this->DependsOn(flow, Key);
            this->DependsOn(flow, Payload);
        }
    }


    TType* const KeyType;
    TType* PayloadType;
    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const Key;
    IComputationNode* const Payload;
    const ui64 ItemsCountHint;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    const std::optional<size_t> PasstroughKey;
    const std::optional<size_t> PasstroughPayload;

    mutable std::vector<NUdf::TUnboxedValue*> Fields;
    const ui32 WideFieldsIndex;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

template <typename TAccumulator>
IComputationNode* WrapToSet(TCallable& callable, const TNodeLocator& nodeLocator, TComputationMutables& mutables) {
    const auto keyType = callable.GetInput(callable.GetInputsCount() - 5U).GetStaticType();
    const auto itemsCountHint = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 1U))->AsValue().Get<ui64>();

    const auto flow = LocateNode(nodeLocator, callable, 0U);
    const auto keySelector = LocateNode(nodeLocator, callable, callable.GetInputsCount() - 5U);

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate_n(args.begin(), width, [&](){ return LocateExternalNode(nodeLocator, callable, ++index); });

        return new TSqueezeSetWideWrapper<TAccumulator>(mutables, keyType, wide, std::move(args), keySelector, itemsCountHint);
    }

    const auto itemArg = LocateExternalNode(nodeLocator, callable, 1U);
    const auto type = callable.GetInput(0U).GetStaticType();

    if (type->IsList()) {
        return new TSetWrapper<TAccumulator, false>(mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
    }
    if (type->IsFlow()) {
        return new TSqueezeSetFlowWrapper<TAccumulator>(mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
    }
    if (type->IsStream()) {
        return new TSetWrapper<TAccumulator, true>(mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
    }

    THROW yexception() << "Expected list, flow or stream.";
}

template <typename TAccumulator>
IComputationNode* WrapToMap(TCallable& callable, const TNodeLocator& nodeLocator, TComputationMutables& mutables) {
    const auto keyType = callable.GetInput(callable.GetInputsCount() - 5U).GetStaticType();
    const auto payloadType = callable.GetInput(callable.GetInputsCount() - 4U).GetStaticType();

    const auto itemsCountHint = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 1U))->AsValue().Get<ui64>();

    const auto flow = LocateNode(nodeLocator, callable, 0U);
    const auto keySelector = LocateNode(nodeLocator, callable, callable.GetInputsCount() - 5U);
    const auto payloadSelector = LocateNode(nodeLocator, callable, callable.GetInputsCount() - 4U);

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate(args.begin(), args.end(), [&](){ return LocateExternalNode(nodeLocator, callable, ++index); });

        return new TSqueezeMapWideWrapper<TAccumulator>(mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint);
    }

    const auto itemArg = LocateExternalNode(nodeLocator, callable, 1U);
    const auto type = callable.GetInput(0U).GetStaticType();

    if (type->IsList()) {
        return new TMapWrapper<TAccumulator, false>(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
    }
    if (type->IsFlow()) {
        return new TSqueezeMapFlowWrapper<TAccumulator>(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
    }
    if (type->IsStream()) {
        return new TMapWrapper<TAccumulator, true>(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
    }

    THROW yexception() << "Expected list, flow or stream.";
}

template <bool IsList>
IComputationNode* WrapToSortedDictInternal(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 6U, "Expected six or more args.");

    const auto type = callable.GetInput(0U).GetStaticType();
    if constexpr (IsList) {
        MKQL_ENSURE(type->IsList(), "Expected list.");
    } else {
        MKQL_ENSURE(type->IsFlow() || type->IsStream(), "Expected flow or stream.");
    }

    const auto keyType = callable.GetInput(callable.GetInputsCount() - 5U).GetStaticType();
    const auto payloadType = callable.GetInput(callable.GetInputsCount() - 4U).GetStaticType();

    const auto multiData = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 3U));
    const bool isMulti = multiData->AsValue().Get<bool>();
    const auto itemsCountHint = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 1U))->AsValue().Get<ui64>();

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto keySelector = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() -5U);
    const auto payloadSelector = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() -4U);

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate(args.begin(), args.end(), [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        if (!isMulti && payloadType->IsVoid()) {
            return new TSqueezeSetWideWrapper<TSortedSetAccumulator>(ctx.Mutables, keyType, wide, std::move(args), keySelector, itemsCountHint);
        } else if (isMulti) {
            return new TSqueezeMapWideWrapper<TSortedMapAccumulator<true>>(ctx.Mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint);
        } else {
            return new TSqueezeMapWideWrapper<TSortedMapAccumulator<false>>(ctx.Mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint);
        }
    }

    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1U);
    if (!isMulti && payloadType->IsVoid()) {
        if (type->IsList()) {
            return new TSetWrapper<TSortedSetAccumulator, false>(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
        }
        if (type->IsFlow()) {
            return new TSqueezeSetFlowWrapper<TSortedSetAccumulator>(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
        }
        if (type->IsStream()) {
            return new TSetWrapper<TSortedSetAccumulator, true>(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint);
        }
    } else if (isMulti) {
        if (type->IsList()) {
            return new TMapWrapper<TSortedMapAccumulator<true>, false>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper<TSortedMapAccumulator<true>>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
        if (type->IsStream()) {
            return new TMapWrapper<TSortedMapAccumulator<true>, true>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
    } else {
        if (type->IsList()) {
            return new TMapWrapper<TSortedMapAccumulator<false>, false>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper<TSortedMapAccumulator<false>>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
        if (type->IsStream()) {
            return new TMapWrapper<TSortedMapAccumulator<false>, true>(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint);
        }
    }

    THROW yexception() << "Expected list, flow or stream.";
}

template <bool IsList>
IComputationNode* WrapToHashedDictInternal(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 6U, "Expected six or more args.");

    const auto type = callable.GetInput(0U).GetStaticType();
    if constexpr (IsList) {
        MKQL_ENSURE(type->IsList(), "Expected list.");
    } else {
        MKQL_ENSURE(type->IsFlow() || type->IsStream(), "Expected flow or stream.");
    }

    const auto keyType = callable.GetInput(callable.GetInputsCount() - 5U).GetStaticType();
    const auto payloadType = callable.GetInput(callable.GetInputsCount() - 4U).GetStaticType();
    const bool multi = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 3U))->AsValue().Get<bool>();
    const bool isCompact = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 2U))->AsValue().Get<bool>();
    const auto payloadSelectorNode = callable.GetInput(callable.GetInputsCount() - 4U);

    const bool isOptional = keyType->IsOptional();
    const auto unwrappedKeyType = isOptional ? AS_TYPE(TOptionalType, keyType)->GetItemType() : keyType;

    if (!multi && payloadType->IsVoid()) {
        if (isCompact) {
            if (unwrappedKeyType->IsData()) {
#define USE_HASHED_SINGLE_FIXED_COMPACT_SET(xType, xLayoutType) \
                case NUdf::TDataType<xType>::Id: \
                    if (isOptional) { \
                        return WrapToSet< \
                            THashedSingleFixedCompactSetAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    } else { \
                        return WrapToSet< \
                            THashedSingleFixedCompactSetAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    }

                switch (AS_TYPE(TDataType, unwrappedKeyType)->GetSchemeType()) {
                    KNOWN_FIXED_VALUE_TYPES(USE_HASHED_SINGLE_FIXED_COMPACT_SET)
                }
#undef USE_HASHED_SINGLE_FIXED_COMPACT_SET
            }

            return WrapToSet<THashedCompactSetAccumulator>(callable, ctx.NodeLocator, ctx.Mutables);
        }

        if (unwrappedKeyType->IsData()) {
#define USE_HASHED_SINGLE_FIXED_SET(xType, xLayoutType) \
            case NUdf::TDataType<xType>::Id: \
                if (isOptional) { \
                    return WrapToSet< \
                        THashedSingleFixedSetAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
               } else { \
                   return WrapToSet< \
                       THashedSingleFixedSetAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
               }

            switch (AS_TYPE(TDataType, unwrappedKeyType)->GetSchemeType()) {
                KNOWN_FIXED_VALUE_TYPES(USE_HASHED_SINGLE_FIXED_SET)
            }
#undef USE_HASHED_SINGLE_FIXED_SET
        }
        return WrapToSet<THashedSetAccumulator>(callable, ctx.NodeLocator, ctx.Mutables);
    }

    if (isCompact) {
        if (unwrappedKeyType->IsData()) {
#define USE_HASHED_SINGLE_FIXED_COMPACT_MAP(xType, xLayoutType) \
                case NUdf::TDataType<xType>::Id: \
                    if (multi) { \
                        if (isOptional) { \
                            return WrapToMap< \
                                THashedSingleFixedCompactMapAccumulator<xLayoutType, true, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
                        } else { \
                            return WrapToMap< \
                                THashedSingleFixedCompactMapAccumulator<xLayoutType, false, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
                        } \
                    } else { \
                        if (isOptional) { \
                            return WrapToMap< \
                                THashedSingleFixedCompactMapAccumulator<xLayoutType, true, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
                        } else { \
                            return WrapToMap< \
                                THashedSingleFixedCompactMapAccumulator<xLayoutType, false, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
                        } \
                    }

            switch (AS_TYPE(TDataType, unwrappedKeyType)->GetSchemeType()) {
                KNOWN_FIXED_VALUE_TYPES(USE_HASHED_SINGLE_FIXED_COMPACT_MAP)
            }
#undef USE_HASHED_SINGLE_FIXED_COMPACT_MAP
        }

        if (multi) {
            return WrapToMap<THashedCompactMapAccumulator<true>>(callable, ctx.NodeLocator, ctx.Mutables);
        } else {
            return WrapToMap<THashedCompactMapAccumulator<false>>(callable, ctx.NodeLocator, ctx.Mutables);
        }
    }

    if (unwrappedKeyType->IsData()) {
#define USE_HASHED_SINGLE_FIXED_MAP(xType, xLayoutType) \
            case NUdf::TDataType<xType>::Id: \
                if (multi) { \
                    if (isOptional) { \
                        return WrapToMap< \
                            THashedSingleFixedMultiMapAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    } else { \
                        return WrapToMap< \
                            THashedSingleFixedMultiMapAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    } \
                } else { \
                    if (isOptional) { \
                        return WrapToMap< \
                            THashedSingleFixedMapAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    } else { \
                        return WrapToMap< \
                            THashedSingleFixedMapAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
                    } \
                }

        switch (AS_TYPE(TDataType, unwrappedKeyType)->GetSchemeType()) {
            KNOWN_FIXED_VALUE_TYPES(USE_HASHED_SINGLE_FIXED_MAP)
        }
#undef USE_HASHED_SINGLE_FIXED_MAP
    }

    if (multi) {
        return WrapToMap<THashedMultiMapAccumulator>(callable, ctx.NodeLocator, ctx.Mutables);
    } else {
        return WrapToMap<THashedMapAccumulator>(callable, ctx.NodeLocator, ctx.Mutables);
    }
}

}

IComputationNode* WrapToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToSortedDictInternal<true>(callable, ctx);
}

IComputationNode* WrapToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal<true>(callable, ctx);
}

IComputationNode* WrapSqueezeToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToSortedDictInternal<false>(callable, ctx);
}

IComputationNode* WrapSqueezeToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal<false>(callable, ctx);
}

}
}
