#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/manager/object.h>

namespace NKikimr::NMetadata::NContainer {

class TObjectSnapshotBase {
public:
    using TPtr = std::shared_ptr<TObjectSnapshotBase>;

private:
    YDB_ACCESSOR(TInstant, LastHistoryInstant, TInstant::Zero());

public:
    TObjectSnapshotBase() = default;
    TObjectSnapshotBase(TInstant historyInstant)
        : LastHistoryInstant(historyInstant) {
    }

    virtual TString GetTypeId() const = 0;

    virtual ~TObjectSnapshotBase() = default;
};

template <NModifications::MetadataObject TObject>
class TObjectSnapshot: public TObjectSnapshotBase {
private:
    YDB_READONLY_DEF(TObject, Metadata);

public:
    TString GetTypeId() const override {
        return TObject::GetTypeId();
    }

    TObjectSnapshot(TObject metadata, TInstant historyInstant)
        : TObjectSnapshotBase(historyInstant)
        , Metadata(std::move(metadata)) {
    }
};

class TAbstractObjectContainer {
private:
    using TObjectPtr = std::shared_ptr<TObjectSnapshotBase>;

    THashMap<TString, TObjectPtr> Objects;

protected:
    bool MatchElementTypeId(const TString& objectType) {
        return Objects.empty() || Objects.begin()->second->GetTypeId() == objectType;
    }

public:
    bool EmplaceAbstractVerified(const TString& objectId, TObjectPtr object) {
        AFL_VERIFY(object);
        if (!Objects.empty()) {
            AFL_VERIFY(MatchElementTypeId(object->GetTypeId()))("container", Objects.begin()->second->GetTypeId())(
                "emplaced", object->GetTypeId());
        }
        return Objects.emplace(objectId, std::move(object)).second;
    }

    bool Contains(const TString& objectId) const {
        return Objects.contains(objectId);
    }

    std::shared_ptr<TObjectSnapshotBase> FindObjectAbstract(const TString& objectId) const {
        auto findObject = Objects.FindPtr(objectId);
        if (!findObject) {
            return {};
        }
        return *findObject;
    }

    bool Erase(const TString& objectId) {
        return Objects.erase(objectId);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (auto it = Objects.begin(); it != Objects.end(); ++it) {
            if (it != Objects.begin()) {
                sb << ", ";
            }
            sb << it->first << ": <" << it->second->GetTypeId() << ">";
        }
        sb << "}";
        return sb;
    }
};

template <NModifications::MetadataObject TObject>
class TObjectContainer: public TAbstractObjectContainer {
public:
    using TSnapshotPtr = std::shared_ptr<TObjectSnapshot<TObject>>;
    using TSelf = TObjectContainer<TObject>;

    TSnapshotPtr FindObject(const TString& objectId) const {
        auto findObject = FindObjectAbstract(objectId);
        if (!findObject) {
            return nullptr;
        }
        auto resultPtr = std::dynamic_pointer_cast<TObjectSnapshot<TObject>>(findObject);
        AFL_VERIFY(!!resultPtr);
        return resultPtr;
    }

    bool Emplace(const TString& objectId, TSnapshotPtr snapshot) {
        return EmplaceAbstractVerified(objectId, std::move(snapshot));
    }

    static std::shared_ptr<TSelf> ConvertFromAbstractVerified(const std::shared_ptr<TAbstractObjectContainer>& container) {
        auto resultPtr = static_pointer_cast<TSelf>(container);
        AFL_VERIFY(resultPtr->MatchElementTypeId(TObject::GetTypeId()));
        return resultPtr;
    }
};

}   // namespace NKikimr::NMetadata::NContainer
