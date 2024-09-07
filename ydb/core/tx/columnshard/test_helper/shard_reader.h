#pragma once
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <optional>

namespace NKikimr::NTxUT {

class TShardReader : TNonCopyable {
private:
    TTestBasicRuntime& Runtime;
    const ui64 TabletId;
    const ui64 PathId;
    const NOlap::TSnapshot Snapshot;
    std::optional<NActors::TActorId> ScanActorId;
    std::optional<int> Finished;
    THashMap<TString, ui64> ResultStats;
    std::optional<NKikimrSSA::TProgram> ProgramProto;
    std::optional<TString> SerializedProgram;
    YDB_ACCESSOR(bool, Reverse, false);
    YDB_ACCESSOR(ui32, Limit, 0);
    std::vector<TString> ReplyColumns;
    std::vector<TSerializedTableRange> Ranges;

    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildStartEvent() const {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(PathId);
        ev->Record.MutableSnapshot()->SetStep(Snapshot.GetPlanStep());
        ev->Record.MutableSnapshot()->SetTxId(Snapshot.GetTxId());

        ev->Record.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
        ev->Record.SetTxId(Snapshot.GetTxId());

        ev->Record.SetReverse(Reverse);
        ev->Record.SetItemsLimit(Limit);

        ev->Record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);

        auto protoRanges = ev->Record.MutableRanges();
        protoRanges->Reserve(Ranges.size());
        for (auto& range : Ranges) {
            auto newRange = protoRanges->Add();
            range.Serialize(*newRange);
        }

        if (ProgramProto) {
            NKikimrSSA::TOlapProgram olapProgram;
            {
                TString programBytes;
                TStringOutput stream(programBytes);
                ProgramProto->SerializeToArcadiaStream(&stream);
                olapProgram.SetProgram(programBytes);
            }
            {
                TString programBytes;
                TStringOutput stream(programBytes);
                olapProgram.SerializeToArcadiaStream(&stream);
                ev->Record.SetOlapProgram(programBytes);
            }
            ev->Record.SetOlapProgramType(
                NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS
            );
        } else if (SerializedProgram) {
            ev->Record.SetOlapProgram(*SerializedProgram);
            ev->Record.SetOlapProgramType(
                NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS
            );
        }

        return ev;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> ResultBatches;
    YDB_READONLY(ui32, IterationsCount, 0);
public:
    ui64 GetReadStat(const TString& paramName) const {
        AFL_VERIFY(IsCorrectlyFinished());
        auto it = ResultStats.find(paramName);
        AFL_VERIFY(it != ResultStats.end());
        return it->second;
    }

    ui64 GetReadBytes() const {
        return GetReadStat("committed_bytes") + GetReadStat("inserted_bytes") + GetReadStat("compacted_bytes");
    }

    void AddRange(const TSerializedTableRange& r) {
        Ranges.emplace_back(r);
    }

    ui32 GetRecordsCount() const {
        AFL_VERIFY(IsFinished());
        auto r = GetResult();
        return r ? r->num_rows() : 0;
    }

    TShardReader& SetReplyColumns(const std::vector<TString>& replyColumns) {
        AFL_VERIFY(!SerializedProgram);
        if (!ProgramProto) {
            ProgramProto = NKikimrSSA::TProgram();
        }
        for (auto&& command : *ProgramProto->MutableCommand()) {
            if (command.HasProjection()) {
                NKikimrSSA::TProgram::TProjection proj;
                for (auto&& i : replyColumns) {
                    proj.AddColumns()->SetName(i);
                }
                *command.MutableProjection() = proj;
                return *this;
            }
        }
        {
            auto* command = ProgramProto->AddCommand();
            NKikimrSSA::TProgram::TProjection proj;
            for (auto&& i : replyColumns) {
                proj.AddColumns()->SetName(i);
            }
            *command->MutableProjection() = proj;
        }
        return *this;
    }

    TShardReader& SetReplyColumnIds(const std::vector<ui32>& replyColumnIds) {
        AFL_VERIFY(!SerializedProgram);
        if (!ProgramProto) {
            ProgramProto = NKikimrSSA::TProgram();
        }
        for (auto&& command : *ProgramProto->MutableCommand()) {
            if (command.HasProjection()) {
                NKikimrSSA::TProgram::TProjection proj;
                for (auto&& i : replyColumnIds) {
                    proj.AddColumns()->SetId(i);
                }
                *command.MutableProjection() = proj;
                return *this;
            }
        }
        {
            auto* command = ProgramProto->AddCommand();
            NKikimrSSA::TProgram::TProjection proj;
            for (auto&& i : replyColumnIds) {
                proj.AddColumns()->SetId(i);
            }
            *command->MutableProjection() = proj;
        }
        return *this;
    }

    TShardReader& SetProgram(const NKikimrSSA::TProgram& p) {
        AFL_VERIFY(!ProgramProto);
        AFL_VERIFY(!SerializedProgram);
        ProgramProto = p;
        return *this;
    }

    TShardReader& SetProgram(const TString& serializedProgram) {
        AFL_VERIFY(!ProgramProto);
        AFL_VERIFY(!SerializedProgram);
        SerializedProgram = serializedProgram;
        return *this;
    }

    TShardReader(TTestBasicRuntime& runtime, const ui64 tabletId, const ui64 pathId, const NOlap::TSnapshot& snapshot)
        : Runtime(runtime)
        , TabletId(tabletId)
        , PathId(pathId)
        , Snapshot(snapshot) {

    }

    bool IsFinished() const {
        return !!Finished;
    }

    bool IsCorrectlyFinished() const {
        return IsFinished() && *Finished == 1;
    }

    bool IsError() const {
        return IsFinished() && *Finished == -1;
    }

    bool InitializeScanner() {
        AFL_VERIFY(!ScanActorId);
        const TActorId sender = Runtime.AllocateEdgeActor();
        ForwardToTablet(Runtime, TabletId, sender, BuildStartEvent().release());
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEvents<NKqp::TEvKqpCompute::TEvScanInitActor, NKqp::TEvKqpCompute::TEvScanError>(handle);
        if (auto* evSuccess = std::get<0>(event)) {
            AFL_VERIFY(evSuccess);
            auto& msg = evSuccess->Record;
            ScanActorId = ActorIdFromProto(msg.GetScanActorId());
            return true;
        } else if (auto* evError = std::get<1>(event)) {
            Finished = -1;
        } else {
            AFL_VERIFY(false);
        }
        return false;
    }

    void Ack() {
        AFL_VERIFY(!Finished);
        AFL_VERIFY(ScanActorId);
        Runtime.Send(*ScanActorId, *ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(8 * 1024 * 1024, 0, 1));
        ++IterationsCount;
    }

    bool Receive() {
        AFL_VERIFY(!Finished);
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEvents<NKqp::TEvKqpCompute::TEvScanData, NKqp::TEvKqpCompute::TEvScanError>(handle);
        if (auto* evData = std::get<0>(event)) {
            auto b = evData->ArrowBatch;
            if (b) {
                ResultBatches.push_back(NArrow::ToBatch(b, true));
                NArrow::TStatusValidator::Validate(ResultBatches.back()->ValidateFull());
            } else {
                AFL_VERIFY(evData->Finished);
            }
            if (evData->Finished) {
                AFL_VERIFY(evData->StatsOnFinished);
                ResultStats = evData->StatsOnFinished->GetMetrics();
                Finished = 1;
            }
        } else if (auto* evError = std::get<1>(event)) {
            Finished = -1;
        } else {
            AFL_VERIFY(false);
        }
        return !Finished;
    }

    std::shared_ptr<arrow::RecordBatch> ReadAll() {
        if (InitializeScanner()) {
            Ack();
            return ContinueReadAll();
        }
        return GetResult();
    }

    std::shared_ptr<arrow::RecordBatch> ContinueReadAll() {
        while (Receive()) {
            Ack();
        }
        return GetResult();
    }

    std::shared_ptr<arrow::RecordBatch> GetResult() const {
        AFL_VERIFY(!!Finished);
        if (IsError()) {
            return nullptr;
        }
        if (ResultBatches.empty()) {
            return nullptr;
        } else {
            auto result = NArrow::CombineBatches(ResultBatches);
            NArrow::TStatusValidator::Validate(result->ValidateFull());
            return result;
        }
    }
};

} //namespace NKikimr::NTxUT
