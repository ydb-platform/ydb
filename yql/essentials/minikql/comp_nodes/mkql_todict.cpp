#include "mkql_todict.h"

#include <yql/essentials/minikql/computation/mkql_computation_list_adapter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_llvm_base.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/utils/cast.h>
#include <yql/essentials/utils/hash.h>

#include <algorithm>
#include <unordered_map>
#include <optional>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class ISetAccumulator {
public:
    virtual ~ISetAccumulator() = default;
    virtual void Add(NUdf::TUnboxedValue&& key) = 0;
    virtual NUdf::TUnboxedValue Build() = 0;
};

class ISetAccumulatorFactory {
public:
    virtual ~ISetAccumulatorFactory() = default;
    virtual bool IsSorted() const = 0;
    virtual std::unique_ptr<ISetAccumulator> Create(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                                    const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx,
                                                    ui64 itemsCountHint) const = 0;
};

class IMapAccumulator {
public:
    virtual ~IMapAccumulator() = default;
    virtual void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) = 0;
    virtual NUdf::TUnboxedValue Build() = 0;
};

class IMapAccumulatorFactory {
public:
    virtual ~IMapAccumulatorFactory() = default;
    virtual bool IsSorted() const = 0;
    virtual std::unique_ptr<IMapAccumulator> Create(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                                    const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint) const = 0;
};

template <typename T>
class TSetAccumulatorFactory: public ISetAccumulatorFactory {
public:
    bool IsSorted() const final {
        return T::IsSorted;
    }

    std::unique_ptr<ISetAccumulator> Create(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx,
                                            ui64 itemsCountHint) const {
        return std::make_unique<T>(keyType, keyTypes, isTuple, encoded, compare, equate, hash, ctx, itemsCountHint);
    }
};

template <typename T>
class TMapAccumulatorFactory: public IMapAccumulatorFactory {
public:
    bool IsSorted() const final {
        return T::IsSorted;
    }

    std::unique_ptr<IMapAccumulator> Create(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx,
                                            ui64 itemsCountHint) const {
        return std::make_unique<T>(keyType, payloadType, keyTypes, isTuple, encoded, compare, equate, hash, ctx, itemsCountHint);
    }
};

class THashedMultiMapAccumulator: public IMapAccumulator {
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
        : Ctx(ctx)
        , KeyType(keyType)
        , KeyTypes(keyTypes)
        , IsTuple(isTuple)
        , Hash(hash)
        , Equate(equate)
        , Map(0, TValueHasher(KeyTypes, isTuple, hash), TValueEqual(KeyTypes, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        auto it = Map.find(key);
        if (it == Map.end()) {
            it = Map.emplace(std::move(key), Ctx.HolderFactory.NewVectorHolder()).first;
        }
        it->second.Push(std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TValuesDictHashMap& targetMap) {
            targetMap = std::move(Map);
        };

        return Ctx.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

class THashedMapAccumulator: public IMapAccumulator {
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
        : Ctx(ctx)
        , KeyType(keyType)
        , KeyTypes(keyTypes)
        , IsTuple(isTuple)
        , Hash(hash)
        , Equate(equate)
        , Map(0, TValueHasher(KeyTypes, isTuple, hash), TValueEqual(KeyTypes, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        Map.emplace(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TMapType& targetMap) {
            targetMap = std::move(Map);
        };

        return Ctx.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMultiMapAccumulator: public IMapAccumulator {
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
        : Ctx(ctx)
        , KeyTypes(keyTypes)
        , Map(0, TMyHash<T>(), TMyEquals<T>())
    {
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
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

    NUdf::TUnboxedValue Build() final {
        std::optional<NUdf::TUnboxedValue> nullPayload;
        if (NullPayloads.size()) {
            nullPayload = Ctx.HolderFactory.VectorAsVectorHolder(std::move(NullPayloads));
        }
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map), std::move(nullPayload));
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMapAccumulator: public IMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    TComputationContext& Ctx;
    TMapType Map;
    std::optional<NUdf::TUnboxedValue> NullPayload;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                     const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx)
        , Map(0, TMyHash<T>(), TMyEquals<T>())
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload.emplace(std::move(payload));
                return;
            }
        }
        Map.emplace(key.Get<T>(), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayload));
    }
};

class THashedSetAccumulator: public ISetAccumulator {
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
        : Ctx(ctx)
        , KeyType(keyType)
        , KeyTypes(keyTypes)
        , IsTuple(isTuple)
        , Set(0, TValueHasher(KeyTypes, isTuple, hash),
              TValueEqual(KeyTypes, isTuple, equate))
        , Hash(hash)
        , Equate(equate)
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer = std::make_shared<TValuePacker>(true, keyType);
        }

        Set.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if (Packer) {
            key = MakeString(Packer->Pack(key));
        }

        Set.emplace(std::move(key));
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TSetType& targetSet) {
            targetSet = std::move(Set);
        };

        return Ctx.HolderFactory.CreateDirectHashedSetHolder(filler, KeyTypes, IsTuple, true, Packer ? KeyType : nullptr, Hash, Equate);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashSingleFixedSet<T>;

    TComputationContext& Ctx;
    TSetType Set;
    bool HasNull = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                     const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx)
        , Set(0, TMyHash<T>(), TMyEquals<T>())
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

    void Add(NUdf::TUnboxedValue&& key) final {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull = true;
                return;
            }
        }
        Set.emplace(key.Get<T>());
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedSetHolder<T, OptionalKey>(std::move(Set), HasNull);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashSingleFixedCompactSet<T>;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TSetType Set;
    bool HasNull = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Set(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
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

    void Add(NUdf::TUnboxedValue&& key) final {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull = true;
                return;
            }
        }
        Set.Insert(key.Get<T>());
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactSetHolder<T, OptionalKey>(std::move(Set), HasNull);
    }
};

class THashedCompactSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashCompactSet;

    TComputationContext& Ctx;
    TPagedArena Pool;
    TSetType Set;
    TType* KeyType;
    std::shared_ptr<TValuePacker> KeyPacker;

public:
    static constexpr bool IsSorted = false;

    THashedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                 const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Set(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR, TSmallValueHash(), TSmallValueEqual())
        , KeyType(keyType)
        , KeyPacker(std::make_shared<TValuePacker>(true, keyType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        Set.Insert(AddSmallValue(Pool, KeyPacker->Pack(key)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedCompactSetHolder(std::move(Set), std::move(Pool), KeyType, &Ctx);
    }
};

template <bool Multi>
class THashedCompactMapAccumulator;

template <>
class THashedCompactMapAccumulator<false>: public IMapAccumulator {
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
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType(keyType)
        , PayloadType(payloadType)
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        Map.InsertNew(AddSmallValue(Pool, KeyPacker->Pack(key)), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedCompactMapHolder(std::move(Map), std::move(Pool), KeyType, PayloadType, &Ctx);
    }
};

template <>
class THashedCompactMapAccumulator<true>: public IMapAccumulator {
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
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType(keyType)
        , PayloadType(payloadType)
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        Map.Insert(AddSmallValue(Pool, KeyPacker->Pack(key)), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedCompactMultiMapHolder(std::move(Map), std::move(Pool), KeyType, PayloadType, &Ctx);
    }
};

template <typename T, bool OptionalKey, bool Multi>
class THashedSingleFixedCompactMapAccumulator;

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, false>: public IMapAccumulator {
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
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType(payloadType)
        , PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload = AddSmallValue(Pool, PayloadPacker->Pack(payload));
                return;
            }
        }
        Map.InsertNew(key.Get<T>(), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayload), std::move(Pool), PayloadType, &Ctx);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, true>: public IMapAccumulator {
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
        : Ctx(ctx)
        , Pool(&Ctx.HolderFactory.GetPagePool())
        , Map(Ctx.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType(payloadType)
        , PayloadPacker(std::make_shared<TValuePacker>(false, payloadType))
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayloads.push_back(AddSmallValue(Pool, PayloadPacker->Pack(payload)));
                return;
            }
        }
        Map.Insert(key.Get<T>(), AddSmallValue(Pool, PayloadPacker->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx.HolderFactory.CreateDirectHashedSingleFixedCompactMultiMapHolder<T, OptionalKey>(std::move(Map), std::move(NullPayloads), std::move(Pool), PayloadType, &Ctx);
    }
};

class TSortedSetAccumulator: public ISetAccumulator {
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

        Items.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key));
    }

    NUdf::TUnboxedValue Build() final {
        const TSortedSetFiller filler = [this](TUnboxedValueVector& values) {
            std::stable_sort(Items.begin(), Items.end(), TValueLess(KeyTypes, IsTuple, Compare));
            Items.erase(std::unique(Items.begin(), Items.end(), TValueEqual(KeyTypes, IsTuple, Equate)), Items.end());
            values = std::move(Items);
        };

        return Ctx.HolderFactory.CreateDirectSortedSetHolder(filler, KeyTypes, IsTuple,
                                                             EDictSortMode::SortedUniqueAscending, true, Packer ? KeyType : nullptr, Compare, Equate);
    }
};

template <bool IsMulti>
class TSortedMapAccumulator;

template <>
class TSortedMapAccumulator<false>: public IMapAccumulator {
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const TSortedDictFiller filler = [this](TKeyPayloadPairVector& values) {
            values = std::move(Items);
        };

        return Ctx.HolderFactory.CreateDirectSortedDictHolder(filler, KeyTypes, IsTuple, EDictSortMode::RequiresSorting,
                                                              true, Packer ? KeyType : nullptr, Compare, Equate);
    }
};

template <>
class TSortedMapAccumulator<true>: public IMapAccumulator {
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

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer) {
            key = MakeString(Packer->Encode(key, false));
        }

        Items.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
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
                    } else {
                        auto payload = Ctx.HolderFactory.CreateDirectListHolder(std::move(currentList));
                        groups.emplace_back(std::move(lastKey), std::move(payload));
                        currentList = TDefaultListRepresentation(std::move(it->second));
                        lastKey = std::move(it->first);
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

class TSetWrapper: public TMutableComputationNode<TSetWrapper> {
    typedef TMutableComputationNode<TSetWrapper> TBaseComputation;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
                     IComputationNode* const key, std::unique_ptr<ISetAccumulator>&& setAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input(std::move(input))
            , Item(item)
            , Key(key)
            , SetAccum(std::move(setAccum))
            , Ctx(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (Input.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item->SetValue(Ctx, std::move(item));
                        SetAccum->Add(Key->GetValue(Ctx));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = SetAccum->Build();
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
        const std::unique_ptr<ISetAccumulator> SetAccum;
        TComputationContext& Ctx;
        bool Finished = false;
    };

    TSetWrapper(TComputationMutables& mutables, TType* keyType, IComputationNode* list, IComputationExternalNode* item,
                IComputationNode* key, ui64 itemsCountHint, bool isStream, std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType(keyType)
        , List(list)
        , Item(item)
        , Key(key)
        , ItemsCountHint(itemsCountHint)
        , IsStream(isStream)
        , Factory(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (IsStream) {
            return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Item, Key,
                                                          Factory->Create(KeyType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
                                                                          ctx, ItemsCountHint), ctx);
        }

        const auto& list = List->GetValue(ctx);
        auto itemsCountHint = ItemsCountHint;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength()) {
                itemsCountHint = size;
            } else {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        auto acc = Factory->Create(KeyType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
                                   ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
                                        [this, &acc, &ctx](NUdf::TUnboxedValue&& item) {
                                            Item->SetValue(ctx, std::move(item));
                                            acc->Add(Key->GetValue(ctx));
                                        });

        return acc->Build().Release();
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
    const bool IsStream;
    const std::unique_ptr<ISetAccumulatorFactory> Factory;
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
        result.emplace_back(StructPtrType); // accumulator
        return result;
    }

    llvm::Constant* GetAccumulator() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    TLLVMFieldsStructureStateWithAccum(llvm::LLVMContext& context)
        : TBase(context)
        , StructPtrType(PointerType::getUnqual(StructType::get(context)))
    {
    }
};
#endif

class TSqueezeSetFlowWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeSetFlowWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeSetFlowWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<ISetAccumulator>&& setAccum)
            : TBase(memInfo)
            , SetAccum(std::move(setAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return SetAccum->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum->Add(value);
        }

    private:
        const std::unique_ptr<ISetAccumulator> SetAccum;
    };

    TSqueezeSetFlowWrapper(TComputationMutables& mutables, TType* keyType,
                           IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, ui64 itemsCountHint,
                           std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , Flow(flow)
        , Item(item)
        , Key(key)
        , ItemsCountHint(itemsCountHint)
        , Factory(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TSqueezeSetFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
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

        BranchInst::Create(over, more, IsFinish(state, block, context), block);

        block = more;

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto key = GetNodeValue(Key, ctx, block);

        EmitFunctionCall<&TState::Insert>(Type::getVoidTy(context), {stateArg, key}, ctx, block);

        BranchInst::Create(more, block);

        block = done;

        const auto dict = EmitFunctionCall<&TState::Build>(valueType, {stateArg}, ctx, block);
        UnRefBoxed(state, ctx, block);
        result->addIncoming(dict, block);

        new StoreInst(item, statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Factory->Create(KeyType, KeyTypes, IsTuple, Encoded,
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
    const std::unique_ptr<ISetAccumulatorFactory> Factory;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

class TSqueezeSetWideWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<ISetAccumulator>&& setAccum)
            : TBase(memInfo)
            , SetAccum(std::move(setAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return SetAccum->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum->Add(value);
        }

    private:
        const std::unique_ptr<ISetAccumulator> SetAccum;
    };

    TSqueezeSetWideWrapper(TComputationMutables& mutables, TType* keyType,
                           IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key,
                           ui64 itemsCountHint, std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , Flow(flow)
        , Items(std::move(items))
        , Key(key)
        , ItemsCountHint(itemsCountHint)
        , Factory(std::move(factory))
        , PasstroughKey(GetPasstroughtMap(TComputationNodePtrVector{Key}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items.size(); ++i) {
                if (Key == Items[i] || Items[i]->GetDependentsCount() > 0U) {
                    fields[i] = &Items[i]->RefValue(ctx);
                }
            }

            switch (Flow->FetchValues(ctx, fields)) {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TSqueezeSetWideWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
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

        BranchInst::Create(over, more, IsFinish(state, block, context), block);

        block = more;

        const auto getres = GetNodeValues(Flow, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!PasstroughKey) {
            for (auto i = 0U; i < Items.size(); ++i) {
                if (Items[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
                }
            }
        }

        const auto key = PasstroughKey ? getres.second[*PasstroughKey](ctx, block) : GetNodeValue(Key, ctx, block);

        EmitFunctionCall<&TState::Insert>(Type::getVoidTy(context), {stateArg, key}, ctx, block);

        BranchInst::Create(more, block);

        block = done;

        const auto dict = EmitFunctionCall<&TState::Build>(valueType, {stateArg}, ctx, block);
        UnRefBoxed(state, ctx, block);
        result->addIncoming(dict, block);

        new StoreInst(GetFinish(context), statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Factory->Create(KeyType, KeyTypes, IsTuple, Encoded,
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
    const std::unique_ptr<ISetAccumulatorFactory> Factory;
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

class TMapWrapper: public TMutableComputationNode<TMapWrapper> {
    typedef TMutableComputationNode<TMapWrapper> TBaseComputation;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
                     IComputationNode* const key, IComputationNode* const payload, std::unique_ptr<IMapAccumulator>&& mapAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input(std::move(input))
            , Item(item)
            , Key(key)
            , Payload(payload)
            , MapAccum(std::move(mapAccum))
            , Ctx(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (Input.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item->SetValue(Ctx, std::move(item));
                        MapAccum->Add(Key->GetValue(Ctx), Payload->GetValue(Ctx));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = MapAccum->Build();
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
        const std::unique_ptr<IMapAccumulator> MapAccum;
        TComputationContext& Ctx;
        bool Finished = false;
    };

    TMapWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType, IComputationNode* list, IComputationExternalNode* item,
                IComputationNode* key, IComputationNode* payload, ui64 itemsCountHint, bool isStream, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , List(list)
        , Item(item)
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
        , IsStream(isStream)
        , Factory(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (IsStream) {
            return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Item, Key, Payload,
                                                          Factory->Create(KeyType, PayloadType, KeyTypes, IsTuple, Encoded, Compare.Get(), Equate.Get(), Hash.Get(),
                                                                          ctx, ItemsCountHint), ctx);
        }

        const auto& list = List->GetValue(ctx);

        auto itemsCountHint = ItemsCountHint;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength()) {
                itemsCountHint = size;
            } else {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        auto acc = Factory->Create(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
                                   Compare.Get(), Equate.Get(), Hash.Get(), ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
                                        [this, &acc, &ctx](NUdf::TUnboxedValue&& item) {
                                            Item->SetValue(ctx, std::move(item));
                                            acc->Add(Key->GetValue(ctx), Payload->GetValue(ctx));
                                        });

        return acc->Build().Release();
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
    const bool IsStream;
    const std::unique_ptr<IMapAccumulatorFactory> Factory;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

class TSqueezeMapFlowWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<IMapAccumulator>&& mapAccum)
            : TBase(memInfo)
            , MapAccum(std::move(mapAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return MapAccum->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum->Add(key, value);
        }

    private:
        const std::unique_ptr<IMapAccumulator> MapAccum;
    };

    TSqueezeMapFlowWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
                           IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, IComputationNode* payload,
                           ui64 itemsCountHint, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , Flow(flow)
        , Item(item)
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
        , Factory(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TSqueezeMapFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
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

        BranchInst::Create(over, more, IsFinish(state, block, context), block);

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

        EmitFunctionCall<&TState::Insert>(Type::getVoidTy(context), {stateArg, key, payload}, ctx, block);

        BranchInst::Create(more, block);

        block = done;

        const auto dict = EmitFunctionCall<&TState::Build>(valueType, {stateArg}, ctx, block);
        UnRefBoxed(state, ctx, block);
        result->addIncoming(dict, block);

        new StoreInst(item, statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Factory->Create(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
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
    const std::unique_ptr<IMapAccumulatorFactory> Factory;
    TKeyTypes KeyTypes;
    bool IsTuple;
    bool Encoded;
    bool UseIHash;

    NUdf::ICompare::TPtr Compare;
    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

class TSqueezeMapWideWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<IMapAccumulator>&& mapAccum)
            : TBase(memInfo)
            , MapAccum(std::move(mapAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return MapAccum->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum->Add(key, value);
        }

    private:
        const std::unique_ptr<IMapAccumulator> MapAccum;
    };

    TSqueezeMapWideWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
                           IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key, IComputationNode* payload,
                           ui64 itemsCountHint, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , Flow(flow)
        , Items(std::move(items))
        , Key(key)
        , Payload(payload)
        , ItemsCountHint(itemsCountHint)
        , Factory(std::move(factory))
        , PasstroughKey(GetPasstroughtMap(TComputationNodePtrVector{Key}, Items).front())
        , PasstroughPayload(GetPasstroughtMap(TComputationNodePtrVector{Payload}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {
        GetDictionaryKeyTypes(KeyType, KeyTypes, IsTuple, Encoded, UseIHash);

        Compare = UseIHash && Factory->IsSorted() ? MakeCompareImpl(KeyType) : nullptr;
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash && !Factory->IsSorted() ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items.size(); ++i) {
                if (Key == Items[i] || Payload == Items[i] || Items[i]->GetDependentsCount() > 0U) {
                    fields[i] = &Items[i]->RefValue(ctx);
                }
            }

            switch (Flow->FetchValues(ctx, fields)) {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TSqueezeMapWideWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
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

        BranchInst::Create(over, more, IsFinish(state, block, context), block);

        block = more;

        const auto getres = GetNodeValues(Flow, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!(PasstroughKey && PasstroughPayload)) {
            for (auto i = 0U; i < Items.size(); ++i) {
                if (Items[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
                }
            }
        }

        const auto key = PasstroughKey ? getres.second[*PasstroughKey](ctx, block) : GetNodeValue(Key, ctx, block);
        const auto payload = PasstroughPayload ? getres.second[*PasstroughPayload](ctx, block) : GetNodeValue(Payload, ctx, block);

        EmitFunctionCall<&TState::Insert>(Type::getVoidTy(context), {stateArg, key, payload}, ctx, block);

        BranchInst::Create(more, block);

        block = done;

        const auto dict = EmitFunctionCall<&TState::Build>(valueType, {stateArg}, ctx, block);
        UnRefBoxed(state, ctx, block);
        result->addIncoming(dict, block);

        new StoreInst(GetFinish(context), statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Factory->Create(KeyType, PayloadType, KeyTypes, IsTuple, Encoded,
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
    const std::unique_ptr<IMapAccumulatorFactory> Factory;
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

    auto factory = std::make_unique<TSetAccumulatorFactory<TAccumulator>>();

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate_n(args.begin(), width, [&]() { return LocateExternalNode(nodeLocator, callable, ++index); });

        return new TSqueezeSetWideWrapper(mutables, keyType, wide, std::move(args), keySelector, itemsCountHint, std::move(factory));
    }

    const auto itemArg = LocateExternalNode(nodeLocator, callable, 1U);
    const auto type = callable.GetInput(0U).GetStaticType();

    if (type->IsList()) {
        return new TSetWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, false, std::move(factory));
    }
    if (type->IsFlow()) {
        return new TSqueezeSetFlowWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, std::move(factory));
    }
    if (type->IsStream()) {
        return new TSetWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, true, std::move(factory));
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

    auto factory = std::make_unique<TMapAccumulatorFactory<TAccumulator>>();
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate(args.begin(), args.end(), [&]() { return LocateExternalNode(nodeLocator, callable, ++index); });

        return new TSqueezeMapWideWrapper(mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint, std::move(factory));
    }

    const auto itemArg = LocateExternalNode(nodeLocator, callable, 1U);
    const auto type = callable.GetInput(0U).GetStaticType();

    if (type->IsList()) {
        return new TMapWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, false, std::move(factory));
    }
    if (type->IsFlow()) {
        return new TSqueezeMapFlowWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, std::move(factory));
    }
    if (type->IsStream()) {
        return new TMapWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, true, std::move(factory));
    }

    THROW yexception() << "Expected list, flow or stream.";
}

IComputationNode* WrapToSortedDictInternal(TCallable& callable, const TComputationNodeFactoryContext& ctx, bool isList) {
    MKQL_ENSURE(callable.GetInputsCount() >= 6U, "Expected six or more args.");

    const auto type = callable.GetInput(0U).GetStaticType();
    if (isList) {
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
    const auto keySelector = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() - 5U);
    const auto payloadSelector = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() - 4U);

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto width = callable.GetInputsCount() - 6U;
        TComputationExternalNodePtrVector args(width, nullptr);
        auto index = 0U;
        std::generate(args.begin(), args.end(), [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        if (!isMulti && payloadType->IsVoid()) {
            return new TSqueezeSetWideWrapper(ctx.Mutables, keyType, wide, std::move(args), keySelector, itemsCountHint,
                                              std::make_unique<TSetAccumulatorFactory<TSortedSetAccumulator>>());
        } else if (isMulti) {
            return new TSqueezeMapWideWrapper(ctx.Mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint,
                                              std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<true>>>());
        } else {
            return new TSqueezeMapWideWrapper(ctx.Mutables, keyType, payloadType, wide, std::move(args), keySelector, payloadSelector, itemsCountHint,
                                              std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<false>>>());
        }
    }

    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1U);
    if (!isMulti && payloadType->IsVoid()) {
        auto factory = std::make_unique<TSetAccumulatorFactory<TSortedSetAccumulator>>();
        if (type->IsList()) {
            return new TSetWrapper(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint,
                                   false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeSetFlowWrapper(ctx.Mutables, keyType, flow, itemArg, keySelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TSetWrapper(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint,
                                   true, std::move(factory));
        }
    } else if (isMulti) {
        auto factory = std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<true>>>();
        if (type->IsList()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   true, std::move(factory));
        }
    } else {
        auto factory = std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<false>>>();
        if (type->IsList()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   true, std::move(factory));
        }
    }

    THROW yexception() << "Expected list, flow or stream.";
}

IComputationNode* WrapToHashedDictInternal(TCallable& callable, const TComputationNodeFactoryContext& ctx, bool isList) {
    MKQL_ENSURE(callable.GetInputsCount() >= 6U, "Expected six or more args.");

    const auto type = callable.GetInput(0U).GetStaticType();
    if (isList) {
        MKQL_ENSURE(type->IsList(), "Expected list.");
    } else {
        MKQL_ENSURE(type->IsFlow() || type->IsStream(), "Expected flow or stream.");
    }

    const auto keyType = callable.GetInput(callable.GetInputsCount() - 5U).GetStaticType();
    const auto payloadType = callable.GetInput(callable.GetInputsCount() - 4U).GetStaticType();
    const bool multi = AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 3U))->AsValue().Get<bool>();

    // Compact structures rely on the TAlignedPagePool invariant that every allocated page is aligned to POOL_PAGE_SIZE.
    // However, this invariant does not hold when PROFILE_MEMORY_ALLOCATIONS is enabled
    const bool isCompact = TAlignedPagePool::IsDefaultAllocatorUsed() ? false : AS_VALUE(TDataLiteral, callable.GetInput(callable.GetInputsCount() - 2U))->AsValue().Get<bool>();

    const auto payloadSelectorNode = callable.GetInput(callable.GetInputsCount() - 4U);

    const bool isOptional = keyType->IsOptional();
    const auto unwrappedKeyType = isOptional ? AS_TYPE(TOptionalType, keyType)->GetItemType() : keyType;

    if (!multi && payloadType->IsVoid()) {
        if (isCompact) {
            if (unwrappedKeyType->IsData()) {
#define USE_HASHED_SINGLE_FIXED_COMPACT_SET(xType, xLayoutType)                                                        \
    case NUdf::TDataType<xType>::Id:                                                                                   \
        if (isOptional) {                                                                                              \
            return WrapToSet<                                                                                          \
                THashedSingleFixedCompactSetAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables);  \
        } else {                                                                                                       \
            return WrapToSet<                                                                                          \
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
#define USE_HASHED_SINGLE_FIXED_SET(xType, xLayoutType)                                                         \
    case NUdf::TDataType<xType>::Id:                                                                            \
        if (isOptional) {                                                                                       \
            return WrapToSet<                                                                                   \
                THashedSingleFixedSetAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables);  \
        } else {                                                                                                \
            return WrapToSet<                                                                                   \
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
#define USE_HASHED_SINGLE_FIXED_COMPACT_MAP(xType, xLayoutType)                                                                   \
    case NUdf::TDataType<xType>::Id:                                                                                              \
        if (multi) {                                                                                                              \
            if (isOptional) {                                                                                                     \
                return WrapToMap<                                                                                                 \
                    THashedSingleFixedCompactMapAccumulator<xLayoutType, true, true>>(callable, ctx.NodeLocator, ctx.Mutables);   \
            } else {                                                                                                              \
                return WrapToMap<                                                                                                 \
                    THashedSingleFixedCompactMapAccumulator<xLayoutType, false, true>>(callable, ctx.NodeLocator, ctx.Mutables);  \
            }                                                                                                                     \
        } else {                                                                                                                  \
            if (isOptional) {                                                                                                     \
                return WrapToMap<                                                                                                 \
                    THashedSingleFixedCompactMapAccumulator<xLayoutType, true, false>>(callable, ctx.NodeLocator, ctx.Mutables);  \
            } else {                                                                                                              \
                return WrapToMap<                                                                                                 \
                    THashedSingleFixedCompactMapAccumulator<xLayoutType, false, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
            }                                                                                                                     \
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
#define USE_HASHED_SINGLE_FIXED_MAP(xType, xLayoutType)                                                                  \
    case NUdf::TDataType<xType>::Id:                                                                                     \
        if (multi) {                                                                                                     \
            if (isOptional) {                                                                                            \
                return WrapToMap<                                                                                        \
                    THashedSingleFixedMultiMapAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables);  \
            } else {                                                                                                     \
                return WrapToMap<                                                                                        \
                    THashedSingleFixedMultiMapAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables); \
            }                                                                                                            \
        } else {                                                                                                         \
            if (isOptional) {                                                                                            \
                return WrapToMap<                                                                                        \
                    THashedSingleFixedMapAccumulator<xLayoutType, true>>(callable, ctx.NodeLocator, ctx.Mutables);       \
            } else {                                                                                                     \
                return WrapToMap<                                                                                        \
                    THashedSingleFixedMapAccumulator<xLayoutType, false>>(callable, ctx.NodeLocator, ctx.Mutables);      \
            }                                                                                                            \
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

} // namespace

IComputationNode* WrapToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToSortedDictInternal(callable, ctx, true);
}

IComputationNode* WrapToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal(callable, ctx, true);
}

IComputationNode* WrapSqueezeToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToSortedDictInternal(callable, ctx, false);
}

IComputationNode* WrapSqueezeToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal(callable, ctx, false);
}

} // namespace NMiniKQL
} // namespace NKikimr
