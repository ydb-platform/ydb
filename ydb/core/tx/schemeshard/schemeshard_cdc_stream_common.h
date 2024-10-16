#pragma once

namespace NKikimr {

struct TPathId;

namespace NSchemeShard {

struct TOperationContext;

} // namespace NSchemeShard

} // namespace NKikimr

namespace NKikimrTxDataShard {

class TCreateCdcStreamNotice;

} // namespace NKikimrTxDataShard

namespace NKikimr::NSchemeShard::NCdc {

void FillNotice(const TPathId& pathId, TOperationContext& context, NKikimrTxDataShard::TCreateCdcStreamNotice& notice);

} // namespace NKikimr::NSchemeShard::NCdc
