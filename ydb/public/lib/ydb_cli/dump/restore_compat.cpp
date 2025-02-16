#include "restore_compat.h"

#include <ydb/library/backup/query_builder.h>
#include <ydb/library/backup/query_uploader.h>

namespace NYdb::NDump {

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

    EStatus Check(const NPrivate::TLine& line) const override {
        if (RowsPerRequest > 0 && (Rows + 1) > RowsPerRequest) {
            return Rows == 0 ? OK : FULL;
        }

        if (BytesPerRequest > 0 && (Bytes + RecordSize(line)) > BytesPerRequest) {
            return Bytes == 0 ? OK : FULL;
        }

        if (RequestUnitsPerRequest > 0 && CalcRequestUnits(RequestUnitsX2 + RecordRequestUnitsX2(line)) > RequestUnitsPerRequest) {
            return RequestUnitsX2 == 0 ? OK : FULL;
        }

        return OK;
    }

    void Feed(NPrivate::TLine&& line) override {
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

    NPrivate::TBatch GetData(bool) override {
        Rows = 0;
        Bytes = 0;
        RequestUnitsX2 = 0;

        // Writer gets data directly from accumulator
        NPrivate::TBatch batch;
        batch.SetOriginAccumulator(this);
        return batch; 
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
            const NPrivate::IDataAccumulator* accumulator,
            const TRestoreSettings& settings)
        : Path(path)
        , TableClient(tableClient)
        , UseBulkUpsert(settings.Mode_ == TRestoreSettings::EMode::BulkUpsert)
    {   
        const auto* dataAccumulator = dynamic_cast<const TDataAccumulator*>(accumulator);
        Y_ENSURE(dataAccumulator);

        TUploader::TOptions opts;
        opts.InFly = settings.MaxInFlight_;
        opts.Rate = settings.RateLimiterSettings_.Rate_;
        opts.Interval = settings.RateLimiterSettings_.Interval_;
        opts.ReactionTime = settings.RateLimiterSettings_.ReactionTime_;

        Uploader = MakeHolder<TUploader>(opts, TableClient, dataAccumulator->GetQueryString());
    }

    bool Push(NPrivate::TBatch&& batch) override {
        auto* accumulator = dynamic_cast<TDataAccumulator*>(batch.GetOriginAccumulator());
        Y_ENSURE(accumulator);

        bool ok;
        if (UseBulkUpsert) {
            ok = Uploader->Push(Path, accumulator->EndAndGetResultingValue());
        } else {
            ok = Uploader->Push(accumulator->EndAndGetResultingParams());
        }

        if (ok) {
            accumulator->Begin();
        }

        return ok;
    }

    void Wait() override {
        Uploader->WaitAllJobs();
    }

private:
    const TString Path;
    TTableClient& TableClient;
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
        const NPrivate::IDataAccumulator* accumulator,
        const TRestoreSettings& settings) {
    return new TDataWriter(path, tableClient, accumulator, settings);
}

} // NYdb::NDump
