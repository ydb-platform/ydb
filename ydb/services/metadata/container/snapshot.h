#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/container/container.h>
#include <ydb/services/metadata/manager/object.h>

namespace NKikimr::NMetadata::NContainer {

class TSnapshotBase: public NFetcher::ISnapshot {
protected:
    TAbstractObjectContainer Objects;

protected:
    virtual TString DoSerializeToString() const override {
        return Objects.DebugString();
    }

public:
    const TAbstractObjectContainer& GetObjects() & {
        return Objects;
    }

    TAbstractObjectContainer GetObjects() && {
        return std::move(Objects);
    }
};

template <typename TObject>
class TSnapshot: public TSnapshotBase {
private:
    class THistoryDecoderWrapper: public NMetadata::NInternal::TDecoderBase {
    private:
        using TBaseDecoder = TObject::TDecoder;

        YDB_READONLY(i32, HistoryInstantIdx, -1);
        TBaseDecoder BaseDecoder;

    public:
        static inline const TString HistoryInstant = "historyInstant";

        THistoryDecoderWrapper(const Ydb::ResultSet& rawData)
            : BaseDecoder(rawData) {
            HistoryInstantIdx = GetFieldIndex(rawData, HistoryInstant);
        }

        const TBaseDecoder& GetBaseDecoder() const {
            return BaseDecoder;
        }
    };

protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override {
        AFL_VERIFY(rawData.result_sets().size() == 1);
        const auto& objectsData = rawData.result_sets()[0];
        THistoryDecoderWrapper decoder(rawData);
        for (auto&& r : objectsData.rows()) {
            TObject object;
            if (!object.DeserializeFromRecord(decoder.GetBaseDecoder(), r)) {
                ALS_WARN(NKikimrServices::METADATA_PROVIDER) << "cannot parse object: " << TypeName<TObject>();
                return false;
            }
            ui64 historyInstantUs;
            if (!decoder.Read(decoder.GetHistoryInstantIdx(), historyInstantUs, r)) {
                return false;
            }
            Objects.EmplaceAbstractVerified(
                object.GetId(), std::make_shared<TObjectSnapshot<TObject>>(std::move(object), TInstant::MicroSeconds(historyInstantUs)));
        }
        return true;
    }
};

template <NModifications::MetadataObject TObject>
class TSnapshotsFetcher: public NFetcher::TSnapshotsFetcher<TSnapshot<TObject>> {
private:
    using TBase = NFetcher::TSnapshotsFetcher<TSnapshot<TObject>>;

protected:
    std::vector<IClassBehaviour::TPtr> DoGetManagers() const override {
        auto manager = NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(TObject::GetTypeId()));
        AFL_VERIFY(manager);
        return { manager };
    }

    TSnapshot<TObject> CreateSnapshot(const TInstant /*actuality*/) const override {
        return std::make_shared<TSnapshot<TObject>>();
    }

    TString GetComponentId() const override {
        return TBase::GetComponentId() + "-extended-fetcher";
    }
};

}   // namespace NKikimr::NMetadata::NContainer
