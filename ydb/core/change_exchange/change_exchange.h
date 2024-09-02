#pragma once

#include "change_record.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <memory>
#include <variant>

namespace NKikimr {

namespace NDataShard {
    class TChangeRecord;
}

namespace NReplication::NService {
    class TChangeRecord;
}

namespace NBackup::NImpl {
    class TChangeRecord;
}

struct IChangeRecordContainer {
    virtual ~IChangeRecordContainer() = default;
    virtual TString Out() const = 0;
};

template <typename T>
struct TBaseChangeRecordContainer: public IChangeRecordContainer {
    TVector<typename T::TPtr> Records;

    TBaseChangeRecordContainer() = default;

    explicit TBaseChangeRecordContainer(TVector<typename T::TPtr>&& records)
        : Records(std::move(records))
    {}

    TString Out() const override {
        return TStringBuilder() << "[" << JoinSeq(",", Records) << "]";
    }
};

template <typename T>
struct TChangeRecordContainer {};

}

namespace NKikimr::NChangeExchange {

struct TEvChangeExchange {
    enum EEv {
        // Enqueue for sending
        EvEnqueueRecords = EventSpaceBegin(TKikimrEvents::ES_CHANGE_EXCHANGE),
        // Request change record(s) by id
        EvRequestRecords,
        // Change record(s)
        EvRecords,
        // Remove change record(s) from local database
        EvRemoveRecords,
        // Already removed records that the sender should forget about
        EvForgetRecods,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CHANGE_EXCHANGE));

    struct TEvEnqueueRecords: public TEventLocal<TEvEnqueueRecords, EvEnqueueRecords> {
        struct TRecordInfo {
            ui64 Order;
            TPathId PathId;
            ui64 BodySize;

            TRecordInfo(ui64 order, const TPathId& pathId, ui64 bodySize);

            void Out(IOutputStream& out) const;
        };

        TVector<TRecordInfo> Records;

        explicit TEvEnqueueRecords(const TVector<TRecordInfo>& records);
        explicit TEvEnqueueRecords(TVector<TRecordInfo>&& records);
        TString ToString() const override;
    };

    struct TEvRequestRecords: public TEventLocal<TEvRequestRecords, EvRequestRecords> {
        struct TRecordInfo {
            ui64 Order;
            ui64 BodySize;

            TRecordInfo(ui64 order, ui64 bodySize = 0);

            bool operator<(const TRecordInfo& rhs) const;
            void Out(IOutputStream& out) const;
        };

        TVector<TRecordInfo> Records;

        explicit TEvRequestRecords(const TVector<TRecordInfo>& records);
        explicit TEvRequestRecords(TVector<TRecordInfo>&& records);
        TString ToString() const override;
    };

    struct TEvRemoveRecords: public TEventLocal<TEvRemoveRecords, EvRemoveRecords> {
        TVector<ui64> Records;

        explicit TEvRemoveRecords(const TVector<ui64>& records);
        explicit TEvRemoveRecords(TVector<ui64>&& records);
        TString ToString() const override;
    };

    struct TEvRecords: public TEventLocal<TEvRecords, EvRecords> {
        using TChangeRecordVector = std::variant<
            std::shared_ptr<TChangeRecordContainer<NDataShard::TChangeRecord>>,
            std::shared_ptr<TChangeRecordContainer<NReplication::NService::TChangeRecord>>,
            std::shared_ptr<TChangeRecordContainer<NBackup::NImpl::TChangeRecord>>
        >;

        TChangeRecordVector Records;

        explicit TEvRecords(TChangeRecordVector&& records);
        TString ToString() const override;

        template <typename T>
        static TEvRecords* New(TVector<typename T::TPtr>&& records) {
            return new TEvRecords(std::make_shared<TChangeRecordContainer<T>>(std::move(records)));
        }

        template <typename T>
        inline auto& GetRecords() {
            return std::get<std::shared_ptr<TChangeRecordContainer<T>>>(Records)->Records;
        }
    };

    struct TEvForgetRecords: public TEventLocal<TEvForgetRecords, EvForgetRecods> {
        TVector<ui64> Records;

        explicit TEvForgetRecords(const TVector<ui64>& records);
        explicit TEvForgetRecords(TVector<ui64>&& records);
        TString ToString() const override;
    };

}; // TEvChangeExchange

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NChangeExchange::TEvChangeExchange::TEvRequestRecords::TRecordInfo, o, x) {
    return x.Out(o);
}
