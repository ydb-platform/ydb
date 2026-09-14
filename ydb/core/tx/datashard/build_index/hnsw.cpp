#include "common_helper.h"
#include "kmeans_helper.h"

#include <ydb/core/base/hnsw.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NDataShard {
namespace {

namespace NHnsw = NTableIndex::NHnsw;

struct TSourceRow {
    TSerializedCellVec Key;
    TSerializedCellVec Values;
};

struct TEvHnswBuilt : TEventLocal<TEvHnswBuilt, TEvents::ES_PRIVATE> {
    TVector<TSourceRow> Rows;
    std::unique_ptr<NHnsw::TBuilder> Graph;
    TString Error;
};

class THnswBuildActor : public TActorBootstrapped<THnswBuildActor> {
    TActorId Owner;
    THolder<TEvHnswBuilt> Result = MakeHolder<TEvHnswBuilt>();
    std::shared_ptr<NKikimr::NKMeans::IClusters> Distance;
    ui32 EmbeddingColumn;
    size_t Next = 0;

public:
    THnswBuildActor(TActorId owner, TVector<TSourceRow> rows, NHnsw::TSettings settings,
        ui64 parent, const Ydb::Table::VectorIndexSettings& vectorSettings, ui32 embeddingColumn)
        : Owner(owner)
        , EmbeddingColumn(embeddingColumn)
    {
        Result->Rows = std::move(rows);
        Distance = NKikimr::NKMeans::CreateClusters(vectorSettings, 0, Result->Error);
        if (Distance) {
            Result->Graph = std::make_unique<NHnsw::TBuilder>(settings, parent,
                [distance = Distance](TStringBuf a, TStringBuf b) { return distance->CalcDistance(a, b); });
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        BuildBatch();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Wakeup, BuildBatch);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void BuildBatch() {
        try {
            if (Result->Error.empty()) {
                const size_t end = Min(Next + 16, Result->Rows.size());
                for (; Next < end; ++Next) {
                    const auto& row = Result->Rows[Next];
                    const auto embedding = row.Values.GetCells().at(EmbeddingColumn).AsBuf();
                    Y_ENSURE(Distance->IsExpectedFormat(embedding), "Invalid vector in HNSW build input");
                    Result->Graph->Add(NHnsw::EncodeSourceKey(row.Key.GetCells().Slice(1)), TString(embedding));
                }
                if (Next != Result->Rows.size()) {
                    Send(SelfId(), new TEvents::TEvWakeup());
                    return;
                }
            }
        } catch (const std::exception& e) {
            Result->Error = e.what();
        }
        Send(Owner, Result.Release());
        PassAway();
    }
};

class THnswBuildScan : public TActor<THnswBuildScan>, public IActorExceptionHandler, public NTable::IScan {
    IDriver* Driver = nullptr;
    NKikimrTxDataShard::TEvBuildIndexCreateRequest Request;
    TActorId ResponseActor;
    TActorId Worker;
    TLead Lead;
    TBatchRowsUploader Uploader;
    TBufferData* Buffer;
    NHnsw::TSettings Settings;
    ui32 EmbeddingColumn = 0;
    ui64 Parent = 0;
    ui64 LeafBytes = 0;
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    TVector<TSourceRow> Rows;
    std::optional<TSourceRow> Pending;
    std::unique_ptr<NHnsw::TBuilder> Graph;
    size_t NextUpload = 0;
    bool ExhaustedInput = false;

public:
    static constexpr auto ActorActivityType() { return NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR; }

    THnswBuildScan(const TUserTable& table, const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request,
        TActorId responseActor)
        : TActor(&TThis::StateWork)
        , Request(request)
        , ResponseActor(responseActor)
        , Uploader(request.GetDatabaseName(), request.GetScanSettings())
        , Settings(NHnsw::GetSettings(request.GetHnswSettings()))
    {
        Settings.Validate();
        auto aligned = [](const TSerializedCellVec& bound) {
            const auto cells = bound.GetCells();
            return cells.size() <= 1 || std::all_of(cells.begin(), cells.end(), [](const TCell& cell) { return cell.IsNull(); });
        };
        Y_ENSURE(aligned(table.Range.From) && aligned(table.Range.To),
            "HNSW build input must not be split inside a parent");
        Y_ENSURE(table.KeyColumnIds.size() > 1
            && table.Columns.at(table.KeyColumnIds[0]).Name == NTableIndex::NKMeans::ParentColumn,
            "HNSW build requires a posting table ordered by parent");
        auto types = std::make_shared<NTxProxy::TUploadTypes>();
        auto addType = [&](const char* name, Ydb::Type::PrimitiveTypeId typeId) {
            Ydb::Type type;
            type.set_type_id(typeId);
            types->emplace_back(name, type);
        };
        addType(NTableIndex::NKMeans::ParentColumn, Ydb::Type::UINT64);
        addType(NHnsw::RecordTypeColumn, Ydb::Type::UINT8);
        addType(NHnsw::NodeIdColumn, Ydb::Type::UINT64);
        addType(NHnsw::KeyColumn, Ydb::Type::STRING);
        addType(NHnsw::SourceKeyColumn, Ydb::Type::STRING);
        addType(NHnsw::NeighborsColumn, Ydb::Type::STRING);
        addType(NHnsw::NodeLevelColumn, Ydb::Type::UINT32);
        addType(NHnsw::EntryIdColumn, Ydb::Type::UINT64);
        addType(NHnsw::MaxLevelColumn, Ydb::Type::UINT32);
        addType(NHnsw::BaseCountColumn, Ydb::Type::UINT64);
        addType(NHnsw::FormatVersionColumn, Ydb::Type::UINT32);
        TTags tags;
        bool embeddingFound = false;
        for (const auto& [id, column] : table.Columns) {
            if (column.Name == NTableIndex::NKMeans::ParentColumn) continue;
            if (column.Name == request.GetHnswEmbeddingColumn()) {
                EmbeddingColumn = tags.size();
                embeddingFound = true;
            }
            tags.push_back(id);
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(column.Type, type);
            types->emplace_back(column.Name, type);
        }
        Y_ENSURE(embeddingFound, "Missing HNSW embedding column");
        TSerializedTableRange range;
        range.Load(request.GetKeyRange());
        Lead = NKMeans::CreateLeadFrom(range.ToTableRange());
        Lead.SetTags(tags);
        Buffer = Uploader.AddDestination(request.GetTargetName(), types);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) override {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        Driver = driver;
        Uploader.SetOwner(SelfId());
        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64) override { lead = Lead; return EScan::Feed; }
    EScan PageFault() override { return EScan::Feed; }
    void Describe(IOutputStream& out) const override { out << "THnswBuildScan " << Request.GetId(); }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);
        TSourceRow source{TSerializedCellVec(key), TSerializedCellVec(*row)};
        const ui64 parent = key.at(0).AsValue<ui64>();
        if (!Rows.empty() && parent != Parent) {
            Pending = std::move(source);
            StartBuild();
            return EScan::Sleep;
        }
        AddRow(std::move(source));
        return EScan::Feed;
    }

    EScan Exhausted() override {
        ExhaustedInput = true;
        if (Rows.empty()) return EScan::Final;
        StartBuild();
        return EScan::Sleep;
    }

    void AddRow(TSourceRow row) {
        const ui64 bytes = row.Key.GetBuffer().size() + row.Values.GetBuffer().size()
            + (row.Key.GetCells().size() + row.Values.GetCells().size()) * sizeof(TCell)
            + 2 * sizeof(TSourceRow) + 64; // vector capacity and string allocation overhead
        Y_ENSURE(Rows.size() < Settings.MaxNodes && bytes < Settings.MaxBytes - LeafBytes,
            "HNSW leaf exceeds its build limit; increase K-means clusters or levels");
        LeafBytes += bytes;
        Parent = row.Key.GetCells()[0].AsValue<ui64>();
        Rows.push_back(std::move(row));
    }

    void StartBuild() {
        auto settings = Settings;
        settings.MaxBytes -= LeafBytes;
        Worker = Register(new THnswBuildActor(SelfId(), std::move(Rows), settings, Parent,
            Request.GetHnswVectorSettings(), EmbeddingColumn), TMailboxType::HTSwap, AppData()->BatchPoolId);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHnswBuilt, Handle);
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, RetryUpload);
        }
    }

    void Handle(TEvHnswBuilt::TPtr& ev) {
        Worker = {};
        Y_ENSURE(ev->Get()->Error.empty(), ev->Get()->Error);
        Rows = std::move(ev->Get()->Rows);
        Graph = std::move(ev->Get()->Graph);
        NextUpload = 0;
        UploadNext();
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        Uploader.Handle(ev);
        if (Uploader.IsSuccess()) {
            UploadNext();
        } else if (auto retry = Uploader.GetRetryAfter()) {
            Schedule(*retry, new TEvents::TEvWakeup());
        } else {
            Driver->Touch(EScan::Final);
        }
    }

    void RetryUpload() { Uploader.RetryUpload(); }

    void UploadNext() {
        const auto& meta = Graph->GetMeta();
        while (NextUpload <= Rows.size()) {
            const bool isMeta = NextUpload == 0;
            TVector<TCell> key{TCell::Make(Parent), TCell::Make(isMeta ? NHnsw::MetaRecord : NHnsw::NodeRecord),
                TCell::Make(ui64(NextUpload)), TCell("", 0)};
            TVector<TCell> values(7);
            TString sourceKey, neighbors;
            if (isMeta) {
                values[3] = TCell::Make(meta.EntryId);
                values[4] = TCell::Make(meta.Level);
                values[5] = TCell::Make(meta.Count);
                values[6] = TCell::Make(NHnsw::FormatVersion);
                values.resize(7 + Rows.front().Values.GetCells().size());
            } else {
                const auto& node = Graph->GetNodes()[NextUpload - 1];
                sourceKey = NHnsw::EncodeSourceKey(Rows[NextUpload - 1].Key.GetCells().Slice(1));
                neighbors = NHnsw::EncodeNeighbors(node.Neighbors);
                values[0] = TCell(sourceKey);
                values[1] = TCell(neighbors);
                values[2] = TCell::Make(ui32(node.Neighbors.size() - 1));
                auto cells = Rows[NextUpload - 1].Values.GetCells();
                values.insert(values.end(), cells.begin(), cells.end());
            }
            Buffer->AddRow(key, values, Rows.back().Key.GetCells());
            ++NextUpload;
            if (Uploader.ShouldWaitUpload()) return;
        }
        if (!Uploader.CanFinish()) return;
        // Checkpoint only after every NODE and META of the parent is durable.
        auto progress = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        FillScanResponseCommonFields(*progress, Request.GetId(), Request.GetTabletId(),
            {Request.GetSeqNoGeneration(), Request.GetSeqNoRound()});
        progress->Record.SetStatus(NKikimrIndexBuilder::IN_PROGRESS);
        progress->Record.SetLastKeyAck(Rows.back().Key.GetBuffer());
        Send(ResponseActor, std::move(progress));
        Rows = TVector<TSourceRow>{};
        Graph.reset();
        LeafBytes = 0;
        if (Pending) {
            AddRow(std::move(*Pending));
            Pending.reset();
        }
        Driver->Touch(ExhaustedInput ? EScan::Final : EScan::Feed);
    }

    bool OnUnhandledException(const std::exception& e) override {
        if (!Driver) return false;
        Driver->Throw(e);
        return true;
    }

    TAutoPtr<IDestructable> Finish(const std::exception& e) override {
        Uploader.AddIssue(e);
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        if (Worker) Send(Worker, new TEvents::TEvPoison());
        auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        FillScanResponseCommonFields(*response, Request.GetId(), Request.GetTabletId(),
            {Request.GetSeqNoGeneration(), Request.GetSeqNoRound()});
        Uploader.Finish(response->Record, status);
        response->Record.SetRowsDelta(ReadRows);
        response->Record.SetBytesDelta(ReadBytes);
        Send(ResponseActor, std::move(response));
        Driver = nullptr;
        PassAway();
        return nullptr;
    }
};

}

NTable::IScan* CreateHnswBuildScan(const TUserTable& table,
    const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request, TActorId responseActor)
{
    return new THnswBuildScan(table, request, responseActor);
}

}
