#include "shard_reader.h"

#include <ydb/library/yql/dq/actors/dq.h>

namespace NKikimr::NTxUT {

std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> TShardReader::BuildStartEvent() const {
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
        ev->Record.SetOlapProgramType(NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS);
    } else if (SerializedProgram) {
        ev->Record.SetOlapProgram(*SerializedProgram);
        ev->Record.SetOlapProgramType(NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS);
    }

    return ev;
}

NKikimr::NTxUT::TShardReader& TShardReader::SetReplyColumnIds(const std::vector<ui32>& replyColumnIds) {
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

void TShardReader::Abort(const TString &reason) {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(ScanActorId);
    Runtime.Send(*ScanActorId, *ScanActorId,
                 new NKqp::TEvKqp::TEvAbortExecution(
                     NYql::NDqProto::StatusIds::CANCELLED, reason));
    // The scan actor processes TEvAbortExecution by calling Finish(ExternalAbort)
    // and PassAway() WITHOUT sending TEvScanError to the edge actor. Therefore
    // a subsequent Receive() would block forever in GrabEdgeEvents. Mark the
    // reader as aborted immediately so any further Receive()/Ack() trips the
    // AFL_VERIFY(!Finished) assertion instead of hanging the test.
    Finished = -1;
}

} // namespace NKikimr::NTxUT
