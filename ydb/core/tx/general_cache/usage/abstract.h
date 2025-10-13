#pragma once
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
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;

    virtual void DoOnResultReady(
        THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses, TErrorAddresses<TPolicy>&& errorAddresses) = 0;
    virtual bool DoIsAborted() const = 0;

public:
    virtual ~ICallback() = default;

    bool IsAborted() const {
        return DoIsAborted();
    }
    void OnResultReady(
        THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses, TErrorAddresses<TPolicy>&& errorAddresses) {
        DoOnResultReady(std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
    }
};

}   // namespace NKikimr::NGeneralCache::NPublic
