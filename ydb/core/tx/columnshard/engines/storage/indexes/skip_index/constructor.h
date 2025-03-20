#pragma once

#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/constructor.h>

namespace NKikimr::NOlap::NIndexes {

class TSkipIndexConstructor: public TColumnIndexConstructor {
private:
    using TBase = TColumnIndexConstructor;

public:
    TSkipIndexConstructor() = default;
};

class TSkipBitmapIndexConstructor: public TSkipIndexConstructor {
private:
    using TBase = TSkipIndexConstructor;
    std::shared_ptr<IBitsStorageConstructor> BitsStorageConstructor;

protected:
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    template <class TProto>
    TConclusionStatus DeserializeFromProtoBitsStorageOnly(const TProto& proto) {
        if (!proto.HasBitsStorage()) {
            BitsStorageConstructor = IBitsStorageConstructor::GetDefault();
        } else {
            BitsStorageConstructor =
                std::shared_ptr<IBitsStorageConstructor>(IBitsStorageConstructor::TFactory::Construct(proto.GetBitsStorage().GetClassName()));
            if (!BitsStorageConstructor) {
                return TConclusionStatus::Fail(
                    "bits_storage_type '" + proto.GetBitsStorage().GetClassName() +
                    "' is unknown for bitset storage factory: " + JoinSeq(",", IBitsStorageConstructor::TFactory::GetRegisteredKeys()));
            }
            return BitsStorageConstructor->DeserializeFromProto(proto.GetBitsStorage());
        }
        return TConclusionStatus::Success();
    }

    template <class TProto>
    void SerializeToProtoBitsStorageOnly(TProto& proto) const {
        *proto.MutableBitsStorage() = GetBitsStorageConstructor()->SerializeToProto();
    }

    template <class TProto>
    TConclusionStatus DeserializeFromProtoImpl(const TProto& proto) {
        auto conclusion = DeserializeFromProtoBitsStorageOnly(proto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return TBase::DeserializeFromProtoImpl(proto);
    }

    template <class TProto>
    void SerializeToProtoImpl(TProto& proto) const {
        proto.MutableBitsStorage()->SetClassName(GetBitsStorageConstructor()->GetClassName());
        TBase::SerializeToProtoImpl(proto);
    }

public:
    const std::shared_ptr<IBitsStorageConstructor>& GetBitsStorageConstructor() const {
        AFL_VERIFY(!!BitsStorageConstructor);
        return BitsStorageConstructor;
    }

    TSkipBitmapIndexConstructor() = default;
};

}   // namespace NKikimr::NOlap::NIndexes
