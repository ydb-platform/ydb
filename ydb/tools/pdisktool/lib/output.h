#pragma once

#include "session.h"

#include <google/protobuf/message.h>

namespace NKikimr::NPDiskTool {

bool PrintMessage(const google::protobuf::Message& msg, bool json, IOutputStream& out);
void PrintIssues(const TIssueLog& issues, IOutputStream& err);

void FillOwnersProto(const TParsedSysLog& state, NKikimr::NPdiskTool::TOwnersResult& proto, bool withChunks);
void PrintOwnersText(const NKikimr::NPdiskTool::TOwnersResult& proto, IOutputStream& out);

void FillChunksProto(const TParsedSysLog& state, NKikimr::NPdiskTool::TChunksResult& proto);
void PrintChunksText(const NKikimr::NPdiskTool::TChunksResult& proto, IOutputStream& out);

void FillLogChunksProto(const TLogScanResult& log, const TParsedSysLog& state, NKikimr::NPdiskTool::TLogChunksResult& proto);
void PrintLogChunksText(const NKikimr::NPdiskTool::TLogChunksResult& proto, IOutputStream& out);

void FillSysLogProto(const TSysLogReadResult& raw, const TParsedSysLog& state, NKikimr::NPdiskTool::TSysLogResult& proto);
void PrintSysLogText(const NKikimr::NPdiskTool::TSysLogResult& proto, IOutputStream& out);

void FillParseLogProto(const TLogScanResult& log, ui32 ownerFilter, NKikimr::NPdiskTool::TParseLogResult& proto);
void PrintParseLogText(const NKikimr::NPdiskTool::TParseLogResult& proto, IOutputStream& out);

void PrintBlobsText(const NKikimr::NPdiskTool::TBlobsResult& proto, IOutputStream& out);
void PrintBarriersText(const NKikimr::NPdiskTool::TBarriersResult& proto, IOutputStream& out);
void PrintBlocksText(const NKikimr::NPdiskTool::TBlocksResult& proto, IOutputStream& out);
void PrintVerifyText(const NKikimr::NPdiskTool::TVerifyResult& proto, IOutputStream& out);

TString HexDump(const void* data, ui32 size);

} // namespace NKikimr::NPDiskTool
