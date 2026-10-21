#pragma once

#include <util/string/builder.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

#include <expected>

namespace NSQLTranslationV1 {

template <class T>
class TYqlNamespace final: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TYqlNamespace>;

    explicit TYqlNamespace(TPtr parent = nullptr)
        : Parent_(std::move(parent))
    {
    }

    [[nodiscard]]
    TMaybe<T> Exchange(TStringBuf name, T d) {
        auto [it, isInserted] = Values_.try_emplace(name, std::move(d));
        if (isInserted) {
            return Nothing();
        }

        return std::exchange(it->second, std::move(d));
    }

    const T* Get(TStringBuf name) {
        const T* value = GetLocal(name);
        if (value) {
            return value;
        }

        if (!Parent_) {
            return nullptr;
        }

        return Parent_->Get(name);
    }

    void ForEachTopLevelUnused(std::invocable<const T&> auto f) const {
        for (const auto& [name, value] : Values_) {
            if (!Used_.contains(name)) {
                f(value);
            }
        }
    }

    static TPtr Fork(TPtr parent = nullptr) {
        return new TYqlNamespace(std::move(parent));
    }

private:
    const T* GetLocal(TStringBuf name) {
        const T* value = Values_.FindPtr(name);
        if (value) {
            Used_.emplace(name);
        }
        return value;
    }

    THashMap<TString, T> Values_;
    THashSet<TString> Used_;
    TPtr Parent_;
};

} // namespace NSQLTranslationV1
