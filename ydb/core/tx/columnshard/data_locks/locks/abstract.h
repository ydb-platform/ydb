#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/string.h>
#include <util/generic/hash_set.h>

#include <optional>
#include <bitset>

namespace NKikimr::NOlap {
class TPortionInfo;
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataLocks {

enum class ELockCategory {
    Generic,
    ReadOnly,
    NewTx,
    MAX
};

class TLockCategories {
private:
    std::bitset<static_cast<ui64>(ELockCategory::MAX)> Categories;
public:
    TLockCategories() {
    }
    TLockCategories(std::initializer_list<ELockCategory> categories) {
        for (auto c: categories) {
            Categories.set(static_cast<ui64>(c));
        }
    }
    bool operator[](ELockCategory c) {
        return Categories[static_cast<ui64>(c)];
    }
};


class TLockFilter {
    enum class EType {
        Excluding, 
        Including
    };

    EType Type;
    std::variant<
        TLockCategories,
        THashSet<TString>
    > Filter;
private:
    TLockFilter(EType type, std::variant<TLockCategories, THashSet<TString>>&& filter)
        : Type(type)
        , Filter(std::move(filter))
    {}
public:
    TLockFilter() {} //by default exclude nothing
    static TLockFilter Only(std::initializer_list<ELockCategory> categories) {
        return TLockFilter{EType::Including, TLockCategories{categories}};
    }
    static TLockFilter AllBut(std::initializer_list<ELockCategory> categories) {
        return TLockFilter{EType::Excluding, categories};
    }
    static TLockFilter Only(THashSet<TString>&& names) {
        return TLockFilter{EType::Including, std::move(names)};
    }
    static TLockFilter AllBut(THashSet<TString>&& names) {
        return TLockFilter{EType::Excluding, std::move(names)};
    }
public:
    bool operator()(const TString& name, const ELockCategory category) const;
};

template<typename T>
concept IsLocable = 
    std::same_as<T, TPortionInfo> || 
    std::same_as<T, TGranuleMeta> ||
    std::same_as<T, ui64> //table pathId
; 

class ILock {
private:
    YDB_READONLY_DEF(TString, LockName);
    YDB_READONLY_DEF(ELockCategory, LockCategory);
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const TLockFilter& filter) const = 0;
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const TLockFilter& filter) const = 0;
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const TLockFilter& filter) const = 0;
    virtual bool DoIsEmpty() const = 0;
public:
    ILock(const TString& name, ELockCategory category)
        : LockName(name)
        , LockCategory(category)
    {
    }

    virtual ~ILock() = default;

    template <IsLocable T>
    std::optional<TString> IsLocked(const T& obj, const TLockFilter& filter) const {
        if (filter(LockName, LockCategory)) {
            return DoIsLocked(obj, filter);
        } else {
            return std::nullopt;
        }
    }

    bool IsEmpty() const {
        return DoIsEmpty();
    }
};

}