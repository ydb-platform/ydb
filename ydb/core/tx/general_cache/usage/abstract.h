#pragma once
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr::NGeneralCache::NPublic {

template <typename TPolicy>
class TErrorAddresses {
private:
    THashMap<typename TPolicy::TAddress, TString> Errors;

public:
    TString GetErrorMessage() const {
        TStringBuilder sb;
        sb << '[';
        for (const auto& [_, error] : Errors) {
            sb << error << ';';
        }
        sb << ']';
        return sb;
    }

    THashMap<typename TPolicy::TAddress, TString> GetErrors() const {
        return Errors;
    }

    bool HasErrors() const {
        return !Errors.empty();
    }

    TErrorAddresses(THashMap<typename TPolicy::TAddress, TString>&& errors)
        : Errors(std::move(errors))
    {
    }
};

template <class TPolicy>
class ICallback {
private:
    mutable bool IsDone = false;

    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;

    virtual void DoOnResultReady(
        THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses, TErrorAddresses<TPolicy>&& errorAddresses) = 0;
    virtual bool DoIsAborted() const = 0;

public:
    virtual ~ICallback() {
        AFL_VERIFY(IsDone);
    }

    bool IsAborted() const {
        if (DoIsAborted()) {
            IsDone = true;
            return true;
        }
        return false;
    }
    void OnResultReady(
        THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses, TErrorAddresses<TPolicy>&& errorAddresses) {
        AFL_VERIFY(!IsDone);
        DoOnResultReady(std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
        IsDone = true;
    }
};

}   // namespace NKikimr::NGeneralCache::NPublic
