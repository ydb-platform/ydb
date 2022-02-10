#include "restore_compat.h"

#include <ydb/library/backup/query_builder.h>
#include <ydb/library/backup/query_uploader.h>

namespace NYdb {
namespace NDump {

using namespace NBackup;
using namespace NTable;

namespace {

class TDataWriter;

class TDataAccumulator: public NPrivate::IDataAccumulator, protected TQueryBuilder {
    friend class TDataWriter;

    ui64 RecordSize(const TString& line) const {
        return line.size();
    }

    ui64 RecordRequestUnitsX2(const TString& line) const {
        const ui64 kbytes = (RecordSize(line) + 1_KB - 1) / 1_KB;
        return UseBulkUpsert ? kbytes : 4 * kbytes;
    }

    static ui64 CalcRequestUnits(ui64 requestUnitsX2) {
        return (requestUnitsX2 + 1) / 2;
    }

public:
    explicit TDataAccumulator(
            const TString& path,
            const TTableDescription& desc,
            const TRestoreSettings& settings)
        : TQueryBuilder(path, desc.GetColumns())
        , UseBulkUpsert(settings.Mode_ == TRestoreSettings::EMode::BulkUpsert)
        , RowsPerRequest(settings.RowsPerRequest_)
        , BytesPerRequest(settings.BytesPerRequest_)
        , RequestUnitsPerRequest(settings.RequestUnitsPerRequest_)
        , Rows(0)
        , Bytes(0)
        , RequestUnitsX2(0)
    {
        Begin();
    }

    bool Fits(const TString& line) const override {
        if (RowsPerRequest > 0 && (Rows + 1) > RowsPerRequest) {
            return Rows == 0;
        }

        if (BytesPerRequest > 0 && (Bytes + RecordSize(line)) > BytesPerRequest) {
            return Bytes == 0;
        }

        if (RequestUnitsPerRequest > 0 && CalcRequestUnits(RequestUnitsX2 + RecordRequestUnitsX2(line)) > RequestUnitsPerRequest) {
            return RequestUnitsX2 == 0;
        }

        return true;
    }

    void Feed(TString&& line) override {
        AddLine(line);

        ++Rows;
        Bytes += RecordSize(line);
        RequestUnitsX2 += RecordRequestUnitsX2(line);
    }

    bool Ready(bool force) const override {
        if (RowsPerRequest > 0 && Rows >= RowsPerRequest) {
            return true;
        }

        if (BytesPerRequest > 0 && Bytes >= BytesPerRequest) {
            return true;
        }

        if (RequestUnitsPerRequest > 0 && CalcRequestUnits(RequestUnitsX2) >= RequestUnitsPerRequest) {
            return true;
        }

        if (force && (Rows || Bytes || RequestUnitsX2)) {
            return true;
        }

        return false;
    }

    TString GetData(bool) override {
        Rows = 0;
        Bytes = 0;
        RequestUnitsX2 = 0;

        return {}; // Writer gets data directly from accumulator
    }

private:
    const bool UseBulkUpsert;
    const ui64 RowsPerRequest; // 0 for inf
    const ui64 BytesPerRequest; // 0 for inf
    const ui64 RequestUnitsPerRequest; // 0 for inf

    ui64 Rows;
    ui64 Bytes;
    ui64 RequestUnitsX2;

}; // TDataAccumulator

class TDataWriter: public NPrivate::IDataWriter {
public:
    explicit TDataWriter(
            const TString& path,
            TTableClient& tableClient,
            NPrivate::IDataAccumulator* accumulator,
            const TRestoreSettings& settings)
        : Path(path)
        , TableClient(tableClient)
        , Accumulator(dynamic_cast<TDataAccumulator*>(accumulator))
        , UseBulkUpsert(settings.Mode_ == TRestoreSettings::EMode::BulkUpsert)
    {
        Y_ENSURE(Accumulator);

        TUploader::TOptions opts;
        opts.InFly = settings.InFly_;
        opts.Rate = settings.RateLimiterSettings_.Rate_;
        opts.Interval = settings.RateLimiterSettings_.Interval_;
        opts.ReactionTime = settings.RateLimiterSettings_.ReactionTime_;

        Uploader = MakeHolder<TUploader>(opts, TableClient, Accumulator->GetQueryString());
    }

    bool Push(TString&&) override {
        bool ok;

        if (UseBulkUpsert) {
            ok = Uploader->Push(Path, Accumulator->EndAndGetResultingValue());
        } else {
            ok = Uploader->Push(Accumulator->EndAndGetResultingParams());
        }

        if (ok) {
            Accumulator->Begin();
        }

        return ok;
    }

    void Wait() override {
        Uploader->WaitAllJobs();
    }

private:
    const TString Path;
    TTableClient& TableClient;
    TDataAccumulator* Accumulator;
    const bool UseBulkUpsert;
    THolder<TUploader> Uploader;

}; // TDataWriter

} // anonymous

NPrivate::IDataAccumulator* CreateCompatAccumulator(
        const TString& path,
        const TTableDescription& desc,
        const TRestoreSettings& settings) {
    return new TDataAccumulator(path, desc, settings);
}

NPrivate::IDataWriter* CreateCompatWriter(
        const TString& path,
        TTableClient& tableClient,
        NPrivate::IDataAccumulator* accumulator,
        const TRestoreSettings& settings) {
    return new TDataWriter(path, tableClient, accumulator, settings);
}

} // NDump
} // NYdb
