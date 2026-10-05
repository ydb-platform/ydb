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

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

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
                                            ui64 itemsCountHint) const override {
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
                                            ui64 itemsCountHint) const override {
        return std::make_unique<T>(keyType, payloadType, keyTypes, isTuple, encoded, compare, equate, hash, ctx, itemsCountHint);
    }
};

class THashedMultiMapAccumulator: public IMapAccumulator {
    using TMapType = TValuesDictHashMap;

    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    bool IsTuple_;
    std::shared_ptr<TValuePacker> Packer_;
    const NUdf::IHash* Hash_;
    const NUdf::IEquate* Equate_;

    TMapType Map_;

public:
    static constexpr bool IsSorted = false;

    THashedMultiMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                               const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Hash_(hash)
        , Equate_(equate)
        , Map_(0, TValueHasher(KeyTypes_, isTuple, hash), TValueEqual(KeyTypes_, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer_ = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer_) {
            key = MakeString(Packer_->Pack(key));
        }

        auto it = Map_.find(key);
        if (it == Map_.end()) {
            it = Map_.emplace(std::move(key), Ctx_.HolderFactory.NewVectorHolder()).first;
        }
        it->second.Push(payload);
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TValuesDictHashMap& targetMap) {
            targetMap = std::move(Map_);
        };

        return Ctx_.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes_, IsTuple_, /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Hash_, Equate_);
    }
};

class THashedMapAccumulator: public IMapAccumulator {
    using TMapType = TValuesDictHashMap;

    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    const bool IsTuple_;
    std::shared_ptr<TValuePacker> Packer_;
    const NUdf::IHash* Hash_;
    const NUdf::IEquate* Equate_;

    TMapType Map_;

public:
    static constexpr bool IsSorted = false;

    THashedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Hash_(hash)
        , Equate_(equate)
        , Map_(0, TValueHasher(KeyTypes_, isTuple, hash), TValueEqual(KeyTypes_, isTuple, equate))
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer_ = std::make_shared<TValuePacker>(true, keyType);
        }

        Y_UNUSED(payloadType);
        Map_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer_) {
            key = MakeString(Packer_->Pack(key));
        }

        Map_.emplace(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TMapType& targetMap) {
            targetMap = std::move(Map_);
        };

        return Ctx_.HolderFactory.CreateDirectHashedDictHolder(filler, KeyTypes_, IsTuple_, /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Hash_, Equate_);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMultiMapAccumulator: public IMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    TComputationContext& Ctx_;
    const TKeyTypes& KeyTypes_;
    TMapType Map_;
    TUnboxedValueVector NullPayloads_;
    NUdf::TUnboxedValue CurrentEmptyVectorForInsert_;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedMultiMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyTypes_(keyTypes)
        , Map_(0, TMyHash<T>(), TMyEquals<T>())
    {
        Y_UNUSED(keyType);
        Y_UNUSED(payloadType);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.reserve(itemsCountHint);
        CurrentEmptyVectorForInsert_ = Ctx_.HolderFactory.NewVectorHolder();
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayloads_.emplace_back(std::move(payload));
                return;
            }
        }
        auto insertInfo = Map_.emplace(key.Get<T>(), CurrentEmptyVectorForInsert_);
        if (insertInfo.second) {
            CurrentEmptyVectorForInsert_ = Ctx_.HolderFactory.NewVectorHolder();
        }
        insertInfo.first->second.Push(payload.Release());
    }

    NUdf::TUnboxedValue Build() final {
        std::optional<NUdf::TUnboxedValue> nullPayload;
        if (!NullPayloads_.empty()) {
            nullPayload = Ctx_.HolderFactory.VectorAsVectorHolder(std::move(NullPayloads_));
        }
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map_), std::move(nullPayload));
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMapAccumulator: public IMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    TComputationContext& Ctx_;
    TMapType Map_;
    std::optional<NUdf::TUnboxedValue> NullPayload_;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                     const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Map_(0, TMyHash<T>(), TMyEquals<T>())
    {
        Y_UNUSED(keyType);
        Y_UNUSED(payloadType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload_.emplace(std::move(payload));
                return;
            }
        }
        Map_.emplace(key.Get<T>(), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedMapHolder<T, OptionalKey>(std::move(Map_), std::move(NullPayload_));
    }
};

class THashedSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashSet;

    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    bool IsTuple_;
    std::shared_ptr<TValuePacker> Packer_;
    TSetType Set_;
    const NUdf::IHash* Hash_;
    const NUdf::IEquate* Equate_;

public:
    static constexpr bool IsSorted = false;

    THashedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Set_(0, TValueHasher(KeyTypes_, isTuple, hash),
               TValueEqual(KeyTypes_, isTuple, equate))
        , Hash_(hash)
        , Equate_(equate)
    {
        Y_UNUSED(compare);
        if (encoded) {
            Packer_ = std::make_shared<TValuePacker>(true, keyType);
        }

        Set_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if (Packer_) {
            key = MakeString(Packer_->Pack(key));
        }

        Set_.emplace(std::move(key));
    }

    NUdf::TUnboxedValue Build() final {
        const auto filler = [this](TSetType& targetSet) {
            targetSet = std::move(Set_);
        };

        return Ctx_.HolderFactory.CreateDirectHashedSetHolder(filler, KeyTypes_, IsTuple_, /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Hash_, Equate_);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashSingleFixedSet<T>;

    TComputationContext& Ctx_;
    TSetType Set_;
    bool HasNull_ = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                     const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Set_(0, TMyHash<T>(), TMyEquals<T>())
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull_ = true;
                return;
            }
        }
        Set_.emplace(key.Get<T>());
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedSetHolder<T, OptionalKey>(std::move(Set_), HasNull_);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashSingleFixedCompactSet<T>;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TSetType Set_;
    bool HasNull_ = false;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Set_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if constexpr (OptionalKey) {
            if (!key) {
                HasNull_ = true;
                return;
            }
        }
        Set_.Insert(key.Get<T>());
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedCompactSetHolder<T, OptionalKey>(std::move(Set_), HasNull_);
    }
};

class THashedCompactSetAccumulator: public ISetAccumulator {
    using TSetType = TValuesDictHashCompactSet;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TSetType Set_;
    TType* KeyType_;
    std::shared_ptr<TValuePacker> KeyPacker_;

public:
    static constexpr bool IsSorted = false;

    THashedCompactSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                 const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Set_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR, TSmallValueHash(), TSmallValueEqual())
        , KeyType_(keyType)
        , KeyPacker_(std::make_shared<TValuePacker>(true, keyType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Set_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        Set_.Insert(AddSmallValue(Pool_, KeyPacker_->Pack(key)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedCompactSetHolder(std::move(Set_), std::move(Pool_), KeyType_, &Ctx_);
    }
};

template <bool Multi>
class THashedCompactMapAccumulator;

template <>
class THashedCompactMapAccumulator<false>: public IMapAccumulator {
    using TMapType = TValuesDictHashCompactMap;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TMapType Map_;
    TType *KeyType_, *PayloadType_;
    std::shared_ptr<TValuePacker> KeyPacker_, PayloadPacker_;

public:
    static constexpr bool IsSorted = false;

    THashedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                 const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Map_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
        , KeyPacker_(std::make_shared<TValuePacker>(true, keyType))
        , PayloadPacker_(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        Map_.InsertNew(AddSmallValue(Pool_, KeyPacker_->Pack(key)), AddSmallValue(Pool_, PayloadPacker_->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedCompactMapHolder(std::move(Map_), std::move(Pool_), KeyType_, PayloadType_, &Ctx_);
    }
};

template <>
class THashedCompactMapAccumulator<true>: public IMapAccumulator {
    using TMapType = TValuesDictHashCompactMultiMap;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TMapType Map_;
    TType *KeyType_, *PayloadType_;
    std::shared_ptr<TValuePacker> KeyPacker_, PayloadPacker_;

public:
    static constexpr bool IsSorted = false;

    THashedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                 const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Map_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
        , KeyPacker_(std::make_shared<TValuePacker>(true, keyType))
        , PayloadPacker_(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        Map_.Insert(AddSmallValue(Pool_, KeyPacker_->Pack(key)), AddSmallValue(Pool_, PayloadPacker_->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedCompactMultiMapHolder(std::move(Map_), std::move(Pool_), KeyType_, PayloadType_, &Ctx_);
    }
};

template <typename T, bool OptionalKey, bool Multi>
class THashedSingleFixedCompactMapAccumulator;

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, false>: public IMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedCompactMap<T>;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TMapType Map_;
    std::optional<ui64> NullPayload_;
    TType* PayloadType_;
    std::shared_ptr<TValuePacker> PayloadPacker_;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Map_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType_(payloadType)
        , PayloadPacker_(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyType);
        Y_UNUSED(keyTypes);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayload_ = AddSmallValue(Pool_, PayloadPacker_->Pack(payload));
                return;
            }
        }
        Map_.InsertNew(key.Get<T>(), AddSmallValue(Pool_, PayloadPacker_->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedCompactMapHolder<T, OptionalKey>(std::move(Map_), std::move(NullPayload_), std::move(Pool_), PayloadType_, &Ctx_);
    }
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapAccumulator<T, OptionalKey, true>: public IMapAccumulator {
    using TMapType = TValuesDictHashSingleFixedCompactMultiMap<T>;

    TComputationContext& Ctx_;
    TPagedArena Pool_;
    TMapType Map_;
    std::vector<ui64> NullPayloads_;
    TType* PayloadType_;
    std::shared_ptr<TValuePacker> PayloadPacker_;

public:
    static constexpr bool IsSorted = false;

    THashedSingleFixedCompactMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                                            const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , Pool_(&Ctx_.HolderFactory.GetPagePool())
        , Map_(Ctx_.HolderFactory.GetPagePool(), itemsCountHint / COMPACT_HASH_MAX_LOAD_FACTOR)
        , PayloadType_(payloadType)
        , PayloadPacker_(std::make_shared<TValuePacker>(false, payloadType))
    {
        Y_UNUSED(keyTypes);
        Y_UNUSED(keyType);
        Y_UNUSED(isTuple);
        Y_UNUSED(encoded);
        Y_UNUSED(compare);
        Y_UNUSED(equate);
        Y_UNUSED(hash);
        Map_.SetMaxLoadFactor(COMPACT_HASH_MAX_LOAD_FACTOR);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if constexpr (OptionalKey) {
            if (!key) {
                NullPayloads_.push_back(AddSmallValue(Pool_, PayloadPacker_->Pack(payload)));
                return;
            }
        }
        Map_.Insert(key.Get<T>(), AddSmallValue(Pool_, PayloadPacker_->Pack(payload)));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateDirectHashedSingleFixedCompactMultiMapHolder<T, OptionalKey>(std::move(Map_), std::move(NullPayloads_), std::move(Pool_), PayloadType_, &Ctx_);
    }
};

class TSortedSetAccumulator: public ISetAccumulator {
    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    bool IsTuple_;
    const NUdf::ICompare* Compare_;
    const NUdf::IEquate* Equate_;

    std::optional<TGenericPresortEncoder> Packer_;
    TUnboxedValueVector Items_;

public:
    static constexpr bool IsSorted = true;

    TSortedSetAccumulator(TType* keyType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Compare_(compare)
        , Equate_(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer_.emplace(KeyType_);
        }

        Items_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key) final {
        if (Packer_) {
            key = MakeString(Packer_->Encode(key, /*desc=*/false));
        }

        Items_.emplace_back(std::move(key));
    }

    NUdf::TUnboxedValue Build() final {
        const TSortedSetFiller filler = [this](TUnboxedValueVector& values) {
            std::stable_sort(Items_.begin(), Items_.end(), TValueLess(KeyTypes_, IsTuple_, Compare_));
            Items_.erase(std::unique(Items_.begin(), Items_.end(), TValueEqual(KeyTypes_, IsTuple_, Equate_)), Items_.end());
            values = std::move(Items_);
        };

        return Ctx_.HolderFactory.CreateDirectSortedSetHolder(filler, KeyTypes_, IsTuple_,
                                                              EDictSortMode::SortedUniqueAscending, /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Compare_, Equate_);
    }
};

template <bool IsMulti>
class TSortedMapAccumulator;

template <>
class TSortedMapAccumulator<false>: public IMapAccumulator {
    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    bool IsTuple_;
    const NUdf::ICompare* Compare_;
    const NUdf::IEquate* Equate_;
    std::optional<TGenericPresortEncoder> Packer_;

    TKeyPayloadPairVector Items_;

public:
    static constexpr bool IsSorted = true;

    TSortedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Compare_(compare)
        , Equate_(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer_.emplace(KeyType_);
        }

        Y_UNUSED(payloadType);
        Items_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer_) {
            key = MakeString(Packer_->Encode(key, /*desc=*/false));
        }

        Items_.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const TSortedDictFiller filler = [this](TKeyPayloadPairVector& values) {
            values = std::move(Items_);
        };

        return Ctx_.HolderFactory.CreateDirectSortedDictHolder(filler, KeyTypes_, IsTuple_, EDictSortMode::RequiresSorting,
                                                               /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Compare_, Equate_);
    }
};

template <>
class TSortedMapAccumulator<true>: public IMapAccumulator {
    TComputationContext& Ctx_;
    TType* KeyType_;
    const TKeyTypes& KeyTypes_;
    bool IsTuple_;
    const NUdf::ICompare* Compare_;
    const NUdf::IEquate* Equate_;
    std::optional<TGenericPresortEncoder> Packer_;
    TKeyPayloadPairVector Items_;

public:
    static constexpr bool IsSorted = true;

    TSortedMapAccumulator(TType* keyType, TType* payloadType, const TKeyTypes& keyTypes, bool isTuple, bool encoded,
                          const NUdf::ICompare* compare, const NUdf::IEquate* equate, const NUdf::IHash* hash, TComputationContext& ctx, ui64 itemsCountHint)
        : Ctx_(ctx)
        , KeyType_(keyType)
        , KeyTypes_(keyTypes)
        , IsTuple_(isTuple)
        , Compare_(compare)
        , Equate_(equate)
    {
        Y_UNUSED(hash);
        if (encoded) {
            Packer_.emplace(KeyType_);
        }

        Y_UNUSED(payloadType);
        Items_.reserve(itemsCountHint);
    }

    void Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& payload) final {
        if (Packer_) {
            key = MakeString(Packer_->Encode(key, /*desc=*/false));
        }

        Items_.emplace_back(std::move(key), std::move(payload));
    }

    NUdf::TUnboxedValue Build() final {
        const TSortedDictFiller filler = [this](TKeyPayloadPairVector& values) {
            std::stable_sort(Items_.begin(), Items_.end(), TKeyPayloadPairLess(KeyTypes_, IsTuple_, Compare_));

            TKeyPayloadPairVector groups;
            groups.reserve(Items_.size());
            if (!Items_.empty()) {
                TDefaultListRepresentation currentList(std::move(Items_.begin()->second));
                auto lastKey = std::move(Items_.begin()->first);
                TValueEqual eqPredicate(KeyTypes_, IsTuple_, Equate_);
                for (auto it = Items_.begin() + 1; it != Items_.end(); ++it) {
                    if (eqPredicate(lastKey, it->first)) {
                        currentList = currentList.Append(std::move(it->second));
                    } else {
                        auto payload = Ctx_.HolderFactory.CreateDirectListHolder(std::move(currentList));
                        groups.emplace_back(std::move(lastKey), std::move(payload));
                        currentList = TDefaultListRepresentation(std::move(it->second));
                        lastKey = std::move(it->first);
                    }
                }

                auto payload = Ctx_.HolderFactory.CreateDirectListHolder(std::move(currentList));
                groups.emplace_back(std::move(lastKey), std::move(payload));
            }

            values = std::move(groups);
        };

        return Ctx_.HolderFactory.CreateDirectSortedDictHolder(filler, KeyTypes_, IsTuple_,
                                                               EDictSortMode::SortedUniqueAscending, /*eagerFill=*/true, Packer_ ? KeyType_ : nullptr, Compare_, Equate_);
    }
};

class TSetWrapper: public TMutableComputationNode<TSetWrapper> {
    using TBaseComputation = TMutableComputationNode<TSetWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
                     IComputationNode* const key, std::unique_ptr<ISetAccumulator>&& setAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input_(std::move(input))
            , Item_(item)
            , Key_(key)
            , SetAccum_(std::move(setAccum))
            , Ctx_(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished_) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (Input_.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item_->SetValue(Ctx_, std::move(item));
                        SetAccum_->Add(Key_->GetValue(Ctx_));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = SetAccum_->Build();
                        Finished_ = true;
                        return NUdf::EFetchStatus::Ok;
                    }
                    case NUdf::EFetchStatus::Yield: {
                        return NUdf::EFetchStatus::Yield;
                    }
                }
            }
        }

        NUdf::TUnboxedValue Input_;
        IComputationExternalNode* const Item_;
        IComputationNode* const Key_;
        const std::unique_ptr<ISetAccumulator> SetAccum_;
        TComputationContext& Ctx_;
        bool Finished_ = false;
    };

    TSetWrapper(TComputationMutables& mutables, TType* keyType, IComputationNode* list, IComputationExternalNode* item,
                IComputationNode* key, ui64 itemsCountHint, bool isStream, std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType_(keyType)
        , List_(list)
        , Item_(item)
        , Key_(key)
        , ItemsCountHint_(itemsCountHint)
        , IsStream_(isStream)
        , Factory_(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (IsStream_) {
            return ctx.HolderFactory.Create<TStreamValue>(List_->GetValue(ctx), Item_, Key_,
                                                          Factory_->Create(KeyType_, KeyTypes_, IsTuple_, Encoded_, Compare_.Get(), Equate_.Get(), Hash_.Get(),
                                                                           ctx, ItemsCountHint_), ctx);
        }

        const auto& list = List_->GetValue(ctx);
        auto itemsCountHint = ItemsCountHint_;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength()) {
                itemsCountHint = size;
            } else {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        auto acc = Factory_->Create(KeyType_, KeyTypes_, IsTuple_, Encoded_, Compare_.Get(), Equate_.Get(), Hash_.Get(),
                                    ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
                                        [this, &acc, &ctx](NUdf::TUnboxedValue&& item) {
                                            Item_->SetValue(ctx, std::move(item));
                                            acc->Add(Key_->GetValue(ctx));
                                        });

        return acc->Build().Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(List_);
        this->Own(Item_);
        this->DependsOn(Key_);
    }

    TType* const KeyType_;
    IComputationNode* const List_;
    IComputationExternalNode* const Item_;
    IComputationNode* const Key_;
    const ui64 ItemsCountHint_;
    const bool IsStream_;
    const std::unique_ptr<ISetAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
};

#ifndef MKQL_DISABLE_CODEGEN
template <class TLLVMBase>
class TLLVMFieldsStructureStateWithAccum: public TLLVMBase {
private:
    using TBase = TLLVMBase;
    llvm::PointerType* StructPtrType_;

protected:
    using TBase::GetContext;

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StructPtrType_); // accumulator
        return result;
    }

    llvm::Constant* GetAccumulator() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
    }

    explicit TLLVMFieldsStructureStateWithAccum(llvm::LLVMContext& context)
        : TBase(context)
        , StructPtrType_(PointerType::getUnqual(StructType::get(context)))
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
            , SetAccum_(std::move(setAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return SetAccum_->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum_->Add(value);
        }

    private:
        const std::unique_ptr<ISetAccumulator> SetAccum_;
    };

    TSqueezeSetFlowWrapper(TComputationMutables& mutables, TType* keyType,
                           IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, ui64 itemsCountHint,
                           std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType_(keyType)
        , Flow_(flow)
        , Item_(item)
        , Key_(key)
        , ItemsCountHint_(itemsCountHint)
        , Factory_(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (auto item = Flow_->GetValue(ctx); item.IsYield()) {
                return item.Release();
            } else if (item.IsFinish()) {
                const auto dict = statePtr->Build();
                state = std::move(item);
                return dict;
            } else {
                Item_->SetValue(ctx, std::move(item));
                statePtr->Insert(Key_->GetValue(ctx).Release());
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        MKQL_ENSURE(codegenItemArg, "Item_ must be codegenerator node.");

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

        const auto item = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto key = GetNodeValue(Key_, ctx, block);

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
        state = ctx.HolderFactory.Create<TState>(Factory_->Create(KeyType_, KeyTypes_, IsTuple_, Encoded_,
                                                                  Compare_.Get(), Equate_.Get(), Hash_.Get(), ctx, ItemsCountHint_));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            this->Own(flow, Item_);
            this->DependsOn(flow, Key_);
        }
    }

    TType* const KeyType_;
    IComputationNode* const Flow_;
    IComputationExternalNode* const Item_;
    IComputationNode* const Key_;
    const ui64 ItemsCountHint_;
    const std::unique_ptr<ISetAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
};

class TSqueezeSetWideWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeSetWideWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<ISetAccumulator>&& setAccum)
            : TBase(memInfo)
            , SetAccum_(std::move(setAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return SetAccum_->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod value) {
            SetAccum_->Add(value);
        }

    private:
        const std::unique_ptr<ISetAccumulator> SetAccum_;
    };

    TSqueezeSetWideWrapper(TComputationMutables& mutables, TType* keyType,
                           IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key,
                           ui64 itemsCountHint, std::unique_ptr<ISetAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType_(keyType)
        , Flow_(flow)
        , Items_(std::move(items))
        , Key_(key)
        , ItemsCountHint_(itemsCountHint)
        , Factory_(std::move(factory))
        , PasstroughKey_(GetPasstroughtMap(TComputationNodePtrVector{Key_}, Items_).front())
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items_.size(); ++i) {
                if (Key_ == Items_[i] || Items_[i]->GetDependentsCount() > 0U) {
                    fields[i] = &Items_[i]->RefValue(ctx);
                }
            }

            switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::One:
                    statePtr->Insert(Key_->GetValue(ctx).Release());
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
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
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

        const auto getres = GetNodeValues(Flow_, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!PasstroughKey_) {
            for (size_t i = 0; i < Items_.size(); ++i) {
                if (Items_[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items_[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
                }
            }
        }

        const auto key = PasstroughKey_ ? getres.second[*PasstroughKey_](ctx, block) : GetNodeValue(Key_, ctx, block);

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
        state = ctx.HolderFactory.Create<TState>(Factory_->Create(KeyType_, KeyTypes_, IsTuple_, Encoded_,
                                                                  Compare_.Get(), Equate_.Get(), Hash_.Get(), ctx, ItemsCountHint_));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TSqueezeSetWideWrapper::Own, flow, std::placeholders::_1));
            this->DependsOn(flow, Key_);
        }
    }

    TType* const KeyType_;
    IComputationWideFlowNode* const Flow_;
    const TComputationExternalNodePtrVector Items_;
    IComputationNode* const Key_;
    const ui64 ItemsCountHint_;
    const std::unique_ptr<ISetAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    const std::optional<size_t> PasstroughKey_;

    const ui32 WideFieldsIndex_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
};

class TMapWrapper: public TMutableComputationNode<TMapWrapper> {
    using TBaseComputation = TMutableComputationNode<TMapWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, IComputationExternalNode* const item,
                     IComputationNode* const key, IComputationNode* const payload, std::unique_ptr<IMapAccumulator>&& mapAccum, TComputationContext& ctx)
            : TComputationValue<TStreamValue>(memInfo)
            , Input_(std::move(input))
            , Item_(item)
            , Key_(key)
            , Payload_(payload)
            , MapAccum_(std::move(mapAccum))
            , Ctx_(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished_) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                NUdf::TUnboxedValue item;
                switch (Input_.Fetch(item)) {
                    case NUdf::EFetchStatus::Ok: {
                        Item_->SetValue(Ctx_, std::move(item));
                        MapAccum_->Add(Key_->GetValue(Ctx_), Payload_->GetValue(Ctx_));
                        break; // and continue
                    }
                    case NUdf::EFetchStatus::Finish: {
                        result = MapAccum_->Build();
                        Finished_ = true;
                        return NUdf::EFetchStatus::Ok;
                    }
                    case NUdf::EFetchStatus::Yield: {
                        return NUdf::EFetchStatus::Yield;
                    }
                }
            }
        }

        NUdf::TUnboxedValue Input_;
        IComputationExternalNode* const Item_;
        IComputationNode* const Key_;
        IComputationNode* const Payload_;
        const std::unique_ptr<IMapAccumulator> MapAccum_;
        TComputationContext& Ctx_;
        bool Finished_ = false;
    };

    TMapWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType, IComputationNode* list, IComputationExternalNode* item,
                IComputationNode* key, IComputationNode* payload, ui64 itemsCountHint, bool isStream, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
        , List_(list)
        , Item_(item)
        , Key_(key)
        , Payload_(payload)
        , ItemsCountHint_(itemsCountHint)
        , IsStream_(isStream)
        , Factory_(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (IsStream_) {
            return ctx.HolderFactory.Create<TStreamValue>(List_->GetValue(ctx), Item_, Key_, Payload_,
                                                          Factory_->Create(KeyType_, PayloadType_, KeyTypes_, IsTuple_, Encoded_, Compare_.Get(), Equate_.Get(), Hash_.Get(),
                                                                           ctx, ItemsCountHint_), ctx);
        }

        const auto& list = List_->GetValue(ctx);

        auto itemsCountHint = ItemsCountHint_;
        if (list.HasFastListLength()) {
            if (const auto size = list.GetListLength()) {
                itemsCountHint = size;
            } else {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        auto acc = Factory_->Create(KeyType_, PayloadType_, KeyTypes_, IsTuple_, Encoded_,
                                    Compare_.Get(), Equate_.Get(), Hash_.Get(), ctx, itemsCountHint);

        TThresher<false>::DoForEachItem(list,
                                        [this, &acc, &ctx](NUdf::TUnboxedValue&& item) {
                                            Item_->SetValue(ctx, std::move(item));
                                            acc->Add(Key_->GetValue(ctx), Payload_->GetValue(ctx));
                                        });

        return acc->Build().Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(List_);
        this->Own(Item_);
        this->DependsOn(Key_);
        this->DependsOn(Payload_);
    }

    TType* const KeyType_;
    TType* PayloadType_;
    IComputationNode* const List_;
    IComputationExternalNode* const Item_;
    IComputationNode* const Key_;
    IComputationNode* const Payload_;
    const ui64 ItemsCountHint_;
    const bool IsStream_;
    const std::unique_ptr<IMapAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
};

class TSqueezeMapFlowWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapFlowWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<IMapAccumulator>&& mapAccum)
            : TBase(memInfo)
            , MapAccum_(std::move(mapAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return MapAccum_->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum_->Add(key, value);
        }

    private:
        const std::unique_ptr<IMapAccumulator> MapAccum_;
    };

    TSqueezeMapFlowWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
                           IComputationNode* flow, IComputationExternalNode* item, IComputationNode* key, IComputationNode* payload,
                           ui64 itemsCountHint, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
        , Flow_(flow)
        , Item_(item)
        , Key_(key)
        , Payload_(payload)
        , ItemsCountHint_(itemsCountHint)
        , Factory_(std::move(factory))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (auto item = Flow_->GetValue(ctx); item.IsYield()) {
                return item.Release();
            } else if (item.IsFinish()) {
                const auto dict = statePtr->Build();
                state = std::move(item);
                return dict;
            } else {
                Item_->SetValue(ctx, std::move(item));
                statePtr->Insert(Key_->GetValue(ctx).Release(), Payload_->GetValue(ctx).Release());
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        MKQL_ENSURE(codegenItemArg, "Item_ must be codegenerator node.");

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

        const auto item = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto key = GetNodeValue(Key_, ctx, block);
        const auto payload = GetNodeValue(Payload_, ctx, block);

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
        state = ctx.HolderFactory.Create<TState>(Factory_->Create(KeyType_, PayloadType_, KeyTypes_, IsTuple_, Encoded_,
                                                                  Compare_.Get(), Equate_.Get(), Hash_.Get(), ctx, ItemsCountHint_));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            this->Own(flow, Item_);
            this->DependsOn(flow, Key_);
            this->DependsOn(flow, Payload_);
        }
    }

    TType* const KeyType_;
    TType* PayloadType_;
    IComputationNode* const Flow_;
    IComputationExternalNode* const Item_;
    IComputationNode* const Key_;
    IComputationNode* const Payload_;
    const ui64 ItemsCountHint_;
    const std::unique_ptr<IMapAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
};

class TSqueezeMapWideWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeMapWideWrapper>;

public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

    public:
        TState(TMemoryUsageInfo* memInfo, std::unique_ptr<IMapAccumulator>&& mapAccum)
            : TBase(memInfo)
            , MapAccum_(std::move(mapAccum))
        {
        }

        NUdf::TUnboxedValuePod Build() {
            return MapAccum_->Build().Release();
        }

        void Insert(NUdf::TUnboxedValuePod key, NUdf::TUnboxedValuePod value) {
            MapAccum_->Add(key, value);
        }

    private:
        const std::unique_ptr<IMapAccumulator> MapAccum_;
    };

    TSqueezeMapWideWrapper(TComputationMutables& mutables, TType* keyType, TType* payloadType,
                           IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* key, IComputationNode* payload,
                           ui64 itemsCountHint, std::unique_ptr<IMapAccumulatorFactory> factory)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , KeyType_(keyType)
        , PayloadType_(payloadType)
        , Flow_(flow)
        , Items_(std::move(items))
        , Key_(key)
        , Payload_(payload)
        , ItemsCountHint_(itemsCountHint)
        , Factory_(std::move(factory))
        , PasstroughKey_(GetPasstroughtMap(TComputationNodePtrVector{Key_}, Items_).front())
        , PasstroughPayload_(GetPasstroughtMap(TComputationNodePtrVector{Payload_}, Items_).front())
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
    {
        GetDictionaryKeyTypes(KeyType_, KeyTypes_, IsTuple_, Encoded_, UseIHash_);

        Compare_ = UseIHash_ && Factory_->IsSorted() ? MakeCompareImpl(KeyType_) : nullptr;
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ && !Factory_->IsSorted() ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            for (auto i = 0U; i < Items_.size(); ++i) {
                if (Key_ == Items_[i] || Payload_ == Items_[i] || Items_[i]->GetDependentsCount() > 0U) {
                    fields[i] = &Items_[i]->RefValue(ctx);
                }
            }

            switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::One:
                    statePtr->Insert(Key_->GetValue(ctx).Release(), Payload_->GetValue(ctx).Release());
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
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
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

        const auto getres = GetNodeValues(Flow_, ctx, block);

        result->addIncoming(GetYield(context), block);

        const auto action = SwitchInst::Create(getres.first, plus, 2U, block);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Finish)), done);
        action->addCase(ConstantInt::get(Type::getInt32Ty(context), i32(EFetchResult::Yield)), over);

        block = plus;

        if (!(PasstroughKey_ && PasstroughPayload_)) {
            for (size_t i = 0; i < Items_.size(); ++i) {
                if (Items_[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items_[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
                }
            }
        }

        const auto key = PasstroughKey_ ? getres.second[*PasstroughKey_](ctx, block) : GetNodeValue(Key_, ctx, block);
        const auto payload = PasstroughPayload_ ? getres.second[*PasstroughPayload_](ctx, block) : GetNodeValue(Payload_, ctx, block);

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
        state = ctx.HolderFactory.Create<TState>(Factory_->Create(KeyType_, PayloadType_, KeyTypes_, IsTuple_, Encoded_,
                                                                  Compare_.Get(), Equate_.Get(), Hash_.Get(), ctx, ItemsCountHint_));
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TSqueezeMapWideWrapper::Own, flow, std::placeholders::_1));
            this->DependsOn(flow, Key_);
            this->DependsOn(flow, Payload_);
        }
    }

    TType* const KeyType_;
    TType* PayloadType_;
    IComputationWideFlowNode* const Flow_;
    const TComputationExternalNodePtrVector Items_;
    IComputationNode* const Key_;
    IComputationNode* const Payload_;
    const ui64 ItemsCountHint_;
    const std::unique_ptr<IMapAccumulatorFactory> Factory_;
    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool Encoded_;
    bool UseIHash_;

    const std::optional<size_t> PasstroughKey_;
    const std::optional<size_t> PasstroughPayload_;

    mutable std::vector<NUdf::TUnboxedValue*> Fields_;
    const ui32 WideFieldsIndex_;

    NUdf::ICompare::TPtr Compare_;
    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
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
        return new TSetWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, /*isStream=*/false, std::move(factory));
    }
    if (type->IsFlow()) {
        return new TSqueezeSetFlowWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, std::move(factory));
    }
    if (type->IsStream()) {
        return new TSetWrapper(mutables, keyType, flow, itemArg, keySelector, itemsCountHint, /*isStream=*/true, std::move(factory));
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
        return new TMapWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, /*isStream=*/false, std::move(factory));
    }
    if (type->IsFlow()) {
        return new TSqueezeMapFlowWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, std::move(factory));
    }
    if (type->IsStream()) {
        return new TMapWrapper(mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint, /*isStream=*/true, std::move(factory));
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
                                   /*isStream=*/false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeSetFlowWrapper(ctx.Mutables, keyType, flow, itemArg, keySelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TSetWrapper(ctx.Mutables, keyType, flow, itemArg, keySelector, itemsCountHint,
                                   /*isStream=*/true, std::move(factory));
        }
    } else if (isMulti) {
        auto factory = std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<true>>>();
        if (type->IsList()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   /*isStream=*/false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   /*isStream=*/true, std::move(factory));
        }
    } else {
        auto factory = std::make_unique<TMapAccumulatorFactory<TSortedMapAccumulator<false>>>();
        if (type->IsList()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   /*isStream=*/false, std::move(factory));
        }
        if (type->IsFlow()) {
            return new TSqueezeMapFlowWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector,
                                              itemsCountHint, std::move(factory));
        }
        if (type->IsStream()) {
            return new TMapWrapper(ctx.Mutables, keyType, payloadType, flow, itemArg, keySelector, payloadSelector, itemsCountHint,
                                   /*isStream=*/true, std::move(factory));
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
    return WrapToSortedDictInternal(callable, ctx, /*isList=*/true);
}

IComputationNode* WrapToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal(callable, ctx, /*isList=*/true);
}

IComputationNode* WrapSqueezeToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToSortedDictInternal(callable, ctx, /*isList=*/false);
}

IComputationNode* WrapSqueezeToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapToHashedDictInternal(callable, ctx, /*isList=*/false);
}

} // namespace NKikimr::NMiniKQL
