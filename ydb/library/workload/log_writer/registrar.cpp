#include "log_writer.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NLogWriter {

TWorkloadFactory::TRegistrator<TLogWriterWorkloadParams> Registrar("log_writer");

} // namespace NLogWriter

} // namespace NYdbWorkload
