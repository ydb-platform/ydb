#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

class TUploadStatus {
private:
    YDB_READONLY_DEF(Ydb::StatusIds::StatusCode, Code);
    YDB_READONLY_DEF(std::optional<TString>, Subcode);
    YDB_READONLY_DEF(std::optional<TString>, ErrorMessage);

public:
    enum class ECustomSubcode {
        DISK_QUOTA_EXCEEDED,
        DELIVERY_PROBLEM,
    };

public:
    TUploadStatus(const Ydb::StatusIds::StatusCode code)
        : Code(code) {
    }
    TUploadStatus(const Ydb::StatusIds::StatusCode code, const TString& errorMessage)
        : Code(code)
        , ErrorMessage(errorMessage) {
        AFL_VERIFY(code != Ydb::StatusIds::SUCCESS);
    }
    TUploadStatus(const Ydb::StatusIds::StatusCode code, const ECustomSubcode& subcode, const TString& errorMessage)
        : Code(code)
        , Subcode(ToString(subcode))
        , ErrorMessage(errorMessage) {
        AFL_VERIFY(code != Ydb::StatusIds::SUCCESS);
    }
    TUploadStatus(const NSchemeCache::TSchemeCacheNavigate::EStatus status);
    TUploadStatus(const NKikimrTxDataShard::TError::EKind status, const TString& errorDescription);

    struct THasher {
        ui64 operator()(const TUploadStatus& object) const {
            return MultiHash(object.GetCode(), object.GetSubcode().has_value(), object.GetSubcode().value_or(""));
        }
    };

    bool operator==(const TUploadStatus& other) const {
        return Code == other.Code && Subcode == other.Subcode;
    }

    TString GetCodeString() const {
        return Ydb::StatusIds::StatusCode_Name(Code);
    }
};

class TUploadCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::THistogramPtr ReplyDuration;

    NMonitoring::TDynamicCounters::TCounterPtr RowsCount;
    NMonitoring::THistogramPtr PackageSizeRecordsByRecords;
    NMonitoring::THistogramPtr PackageSizeCountByRecords;

    NMonitoring::THistogramPtr PreparingDuration;
    NMonitoring::THistogramPtr WritingDuration;
    NMonitoring::THistogramPtr CommitDuration;
    NMonitoring::THistogramPtr PrepareReplyDuration;

    THashMap<TUploadStatus, NMonitoring::TDynamicCounters::TCounterPtr, TUploadStatus::THasher> CodesCount;

    NMonitoring::TDynamicCounters::TCounterPtr GetCodeCounter(const TUploadStatus& status);

public:
    TUploadCounters();

    class TGuard: TMoveOnly {
    private:
        TMonotonic Start = TMonotonic::Now();
        std::optional<TMonotonic> WritingStarted;
        std::optional<TMonotonic> CommitStarted;
        std::optional<TMonotonic> CommitFinished;
        std::optional<TMonotonic> ReplyFinished;
        TUploadCounters& Owner;

    public:
        TGuard(const TMonotonic start, TUploadCounters& owner)
            : Start(start)
            , Owner(owner) {
        }

        void OnWritingStarted() {
            WritingStarted = TMonotonic::Now();
            Owner.PreparingDuration->Collect((*WritingStarted - Start).MilliSeconds());
        }

        void OnCommitStarted() {
            CommitStarted = TMonotonic::Now();
            AFL_VERIFY(WritingStarted);
            Owner.WritingDuration->Collect((*CommitStarted - *WritingStarted).MilliSeconds());
        }

        void OnCommitFinished() {
            CommitFinished = TMonotonic::Now();
            AFL_VERIFY(CommitStarted);
            Owner.CommitDuration->Collect((*CommitFinished - *CommitStarted).MilliSeconds());
        }

        void OnReply(const TUploadStatus& status) {
            ReplyFinished = TMonotonic::Now();
            if (CommitFinished) {
                Owner.PrepareReplyDuration->Collect((*ReplyFinished - *CommitFinished).MilliSeconds());
            }
            Owner.ReplyDuration->Collect((*ReplyFinished - Start).MilliSeconds());
            Owner.GetCodeCounter(status)->Add(1);
        }
    };

    TGuard BuildGuard(const TMonotonic start) {
        return TGuard(start, *this);
    }

    void OnRequest(const ui64 rowsCount) const {
        RequestsCount->Add(1);
        RowsCount->Add(rowsCount);
        PackageSizeRecordsByRecords->Collect((i64)rowsCount, rowsCount);
        PackageSizeCountByRecords->Collect(rowsCount);
    }
};

}   // namespace NKikimr
