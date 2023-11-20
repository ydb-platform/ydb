#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

    ui64 ResolvePqTablet(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId);

    TVector<std::pair<TString, TString>> GetPqRecords(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId);

} // namespace NKikimr
