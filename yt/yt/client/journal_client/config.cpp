#include "config.h"
namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

void TChunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_data_size_read_threshold", &TThis::ReplicaDataSizeReadThreshold)
        .Default(1_MB);

    registrar.Parameter("slow_path_delay", &TThis::SlowPathDelay)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
