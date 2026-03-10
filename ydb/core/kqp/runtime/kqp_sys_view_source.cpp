#include "kqp_sys_view_source.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;

namespace {

#define LOG_PREFIX "TKqpSysViewSource "
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LOG_PREFIX << SelfId() << " " << msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LOG_PREFIX << SelfId() << " " << msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LOG_PREFIX << SelfId() << " " << msg)

class TKqpSysViewSource : public TActorBootstrapped<TKqpSysViewSource>, public IDqComputeActorAsyncInput {
public:
    TKqpSysViewSource(
        const NKikimrKqp::TKqpSysViewSourceSettings* settings,
        TIntrusivePtr<TProtoArenaHolder> arena,
        const TActorId& computeActorId,
        ui64 inputIndex,
        const THolderFactory& holderFactory,
        std::shared_ptr<TScopedAlloc> alloc,
        TIntrusivePtr<TKqpCounters> counters)
        : Settings(settings)
        , Arena(std::move(arena))
        , ComputeActorId(computeActorId)
        , InputIndex(inputIndex)
        , HolderFactory(holderFactory)
        , Alloc(std::move(alloc))
        , Counters(std::move(counters))
    {
    }

    void Bootstrap() {
        LOG_D("Bootstrap");

        const auto& table = Settings->GetTable();
        TTableId tableId(table.GetOwnerId(), table.GetTableId(), table.GetSysView());
        TString tablePath = Settings->GetTablePath();
        TString database = Settings->GetDatabase();
        bool reverse = Settings->GetReverse();

        TMaybe<NKikimrSysView::TSysViewDescription> sysViewDescription;
        if (Settings->HasSysViewDescription()) {
            sysViewDescription = Settings->GetSysViewDescription();
            // If SourceObject is missing, set it from the table's PathId so the scan actor
            // can navigate the scheme cache to find the domain and SysViewProcessor.
            if (!sysViewDescription->HasSourceObject()) {
                tableId.PathId.ToProto(sysViewDescription->MutableSourceObject());
            }
        }

        // Build columns
        Columns.reserve(Settings->ColumnsSize());
        for (const auto& col : Settings->GetColumns()) {
            TKqpComputeContextBase::TColumn column;
            column.Tag = col.GetId();
            column.Type = NScheme::TypeInfoFromProto(col.GetTypeId(), col.GetTypeInfo());
            Columns.push_back(column);
            ColumnTypes.push_back(column.Type);
        }

        // Build key ranges
        TVector<TSerializedTableRange> ranges;
        for (const auto& protoRange : Settings->GetKeyRanges()) {
            ranges.emplace_back(protoRange);
        }

        if (ranges.empty()) {
            // Full scan — construct a range with NULL cells in From so scan actors
            // don't interpret empty From as +inf.
            ui32 keyColumnCount = Settings->GetKeyColumns().size();
            TVector<TCell> fromCells(keyColumnCount ? keyColumnCount : 1); // at least one NULL cell
            ranges.emplace_back(TSerializedTableRange(fromCells, true, TConstArrayRef<TCell>(), true));
        }

        // Deserialize user token for access control checks in scan actors
        TIntrusiveConstPtr<NACLib::TUserToken> userToken;
        if (Settings->HasUserToken()) {
            userToken = MakeIntrusive<NACLib::TUserToken>(Settings->GetUserToken());
        }

        auto scanActor = NSysView::CreateSystemViewScan(
            SelfId(), 0, database, sysViewDescription,
            tableId, tablePath,
            ranges, Columns, std::move(userToken), reverse);

        if (!scanActor) {
            auto issue = TStringBuilder()
                << "Failed to create system view scan, table id: " << tableId;
            LOG_E(issue);
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex,
                TIssues({TIssue(issue)}),
                NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        }

        ScanActorId = Register(scanActor.Release());
        LOG_D("Registered scan actor: " << ScanActorId);

        // Send initial ack to start data flow
        Send(ScanActorId, new TEvKqpCompute::TEvScanDataAck(BufferSize));

        Become(&TKqpSysViewSource::StateFunc);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpCompute::TEvScanInitActor, Handle);
            hFunc(TEvKqpCompute::TEvScanData, Handle);
            hFunc(TEvKqpCompute::TEvScanError, Handle);
            default:
                LOG_W("Unexpected event: " << ev->GetTypeRewrite());
        }
    }

private:
    void Handle(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        LOG_D("Got scan init actor event");
        // Respond with ack so the scan actor knows we're ready
        Send(ev->Sender, new TEvKqpCompute::TEvScanDataAck(BufferSize));
    }

    void Handle(TEvKqpCompute::TEvScanData::TPtr& ev) {
        auto& msg = *ev->Get();

        LOG_D("Got scan data, rows: " << msg.Rows.size()
            << ", finished: " << msg.Finished
            << ", from: " << ev->Sender);

        if (!msg.Rows.empty()) {
            auto guard = Guard(*Alloc);
            for (auto& row : msg.Rows) {
                BufferedRows.push_back(std::move(row));
            }
        }

        if (msg.ArrowBatch && msg.ArrowBatch->num_rows() > 0) {
            LOG_W("Arrow batches not supported in sys view source, ignoring");
        }

        if (msg.Finished) {
            ScanFinished = true;
        }

        // Notify compute actor that we have data
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvKqpCompute::TEvScanError::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        TIssues issues;
        Ydb::StatusIds::StatusCode status = msg.GetStatus();
        IssuesFromMessage(msg.GetIssues(), issues);

        LOG_E("Got scan error, status: " << Ydb::StatusIds::StatusCode_Name(status)
            << ", issues: " << issues.ToOneLineString());

        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues,
            NYql::NDq::YdbStatusToDqStatus(status, NYql::NDq::EStatusCompatibilityLevel::WithUnauthorized)));
    }

    // IDqComputeActorAsyncInput implementation
    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueBatch& batch,
        TMaybe<TInstant>& watermark,
        bool& finished,
        i64 freeSpace) override
    {
        Y_UNUSED(watermark);
        i64 totalSize = 0;

        if (BufferedRows.empty()) {
            finished = ScanFinished;
            if (!ScanFinished && ScanActorId) {
                Send(ScanActorId, new TEvKqpCompute::TEvScanDataAck(BufferSize));
            }
            return 0;
        }

        while (!BufferedRows.empty() && totalSize < freeSpace) {
            auto& row = BufferedRows.front();

            NUdf::TUnboxedValue* items = nullptr;
            auto rowValue = HolderFactory.CreateDirectArrayHolder(Columns.size(), items);

            for (ui32 i = 0; i < Columns.size(); ++i) {
                if (i < row.size()) {
                    items[i] = NMiniKQL::GetCellValue(row[i], ColumnTypes[i]);
                }
            }

            i64 rowSize = 0;
            for (const auto& cell : row) {
                rowSize += cell.Size();
            }
            totalSize += std::max(rowSize, (i64)8);

            batch.push_back(std::move(rowValue));
            BufferedRows.pop_front();
        }

        finished = ScanFinished && BufferedRows.empty();

        // Request more data if we still have room and scan is not finished
        if (!ScanFinished && ScanActorId) {
            Send(ScanActorId, new TEvKqpCompute::TEvScanDataAck(BufferSize));
        }

        return totalSize;
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    void LoadState(const TSourceState&) override {}

    void PassAway() override {
        if (ScanActorId) {
            Send(ScanActorId, new TEvents::TEvPoison);
            ScanActorId = {};
        }

        {
            auto guard = Guard(*Alloc);
            BufferedRows.clear();
        }

        TActorBootstrapped<TKqpSysViewSource>::PassAway();
    }

private:
    const NKikimrKqp::TKqpSysViewSourceSettings* Settings;
    TIntrusivePtr<TProtoArenaHolder> Arena;
    const TActorId ComputeActorId;
    const ui64 InputIndex;
    const THolderFactory& HolderFactory;
    std::shared_ptr<TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;

    TActorId ScanActorId;
    TSmallVec<TKqpComputeContextBase::TColumn> Columns;
    TVector<NScheme::TTypeInfo> ColumnTypes;

    TDeque<TOwnedCellVec> BufferedRows;
    bool ScanFinished = false;

    TDqAsyncStats IngressStats;

    static constexpr ui64 BufferSize = 8_MB;
};

} // anonymous namespace

void RegisterKqpSysViewSource(TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSource<NKikimrKqp::TKqpSysViewSourceSettings>(
        TString(KqpSysViewSourceName),
        [counters](const NKikimrKqp::TKqpSysViewSourceSettings* settings, TDqAsyncIoFactory::TSourceArguments&& args) {
            auto* actor = new TKqpSysViewSource(
                settings, args.Arena, args.ComputeActorId, args.InputIndex,
                args.HolderFactory, args.Alloc, counters);
            return std::make_pair<IDqComputeActorAsyncInput*, IActor*>(actor, actor);
        });
}

} // namespace NKikimr::NKqp
