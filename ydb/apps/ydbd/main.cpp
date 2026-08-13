#include "export.h"
#include <ydb/core/driver_lib/run/main.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/schemeshard/schemeshard_operation_factory.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/base/events_writer.h>
#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/pdisk_io/aio.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yaml_config/yaml_config.h>

#include <util/string/cast.h>
#include <util/system/env.h>

#ifdef _linux_
#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>
#endif

#include <limits>

namespace {

constexpr TStringBuf TCMallocMaxPerCpuCacheSizeEnv = "YDB_TCMALLOC_MAX_PER_CPU_CACHE_SIZE_BYTES";
constexpr TStringBuf TCMallocMaxTotalThreadCacheSizeEnv = "YDB_TCMALLOC_MAX_TOTAL_THREAD_CACHE_SIZE_BYTES";

bool ConfigureTCMallocMaxPerCpuCacheSizeFromEnvironment() {
    const auto value = TryGetEnv(TString{TCMallocMaxPerCpuCacheSizeEnv});
    if (!value || value->empty()) {
        return true;
    }

    ui64 bytes = 0;
    if (!TryFromString(*value, bytes) || bytes > static_cast<ui64>(std::numeric_limits<i32>::max())) {
        Cerr << "Invalid " << TCMallocMaxPerCpuCacheSizeEnv << " value '" << *value
             << "': expected 0 or an integer number of bytes in range [1, "
             << std::numeric_limits<i32>::max() << "]" << Endl;
        return false;
    }

    if (bytes == 0) {
        return true;
    }

#ifdef _linux_
    const auto requestedBytes = static_cast<i32>(bytes);
    tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(requestedBytes);
    const auto configuredBytes = tcmalloc::MallocExtension::GetMaxPerCpuCacheSize();
    if (configuredBytes != requestedBytes) {
        Cerr << "Failed to set TCMalloc maximum per-CPU cache size to " << requestedBytes
             << " bytes: allocator reports " << configuredBytes << " bytes" << Endl;
        return false;
    }

    Cerr << "Configured TCMalloc maximum per-CPU cache size to " << configuredBytes
         << " bytes from " << TCMallocMaxPerCpuCacheSizeEnv << Endl;
    return true;
#else
    Cerr << TCMallocMaxPerCpuCacheSizeEnv << " is supported only by Linux ydbd builds"
         << Endl;
    return false;
#endif
}

bool ConfigureTCMallocMaxTotalThreadCacheSizeFromEnvironment() {
    const auto value = TryGetEnv(TString{TCMallocMaxTotalThreadCacheSizeEnv});
    if (!value || value->empty()) {
        return true;
    }

    ui64 bytes = 0;
    if (!TryFromString(*value, bytes) || bytes > static_cast<ui64>(std::numeric_limits<i64>::max())) {
        Cerr << "Invalid " << TCMallocMaxTotalThreadCacheSizeEnv << " value '" << *value
             << "': expected 0 or an integer number of bytes in range [1, "
             << std::numeric_limits<i64>::max() << "]" << Endl;
        return false;
    }

    if (bytes == 0) {
        return true;
    }

#ifdef _linux_
    const auto requestedBytes = static_cast<i64>(bytes);
    tcmalloc::MallocExtension::SetMaxTotalThreadCacheBytes(requestedBytes);
    const auto configuredBytes = tcmalloc::MallocExtension::GetMaxTotalThreadCacheBytes();
    if (configuredBytes != requestedBytes) {
        Cerr << "Failed to set TCMalloc maximum total thread cache size to " << requestedBytes
             << " bytes: allocator reports " << configuredBytes << " bytes" << Endl;
        return false;
    }

    Cerr << "Configured TCMalloc maximum total thread cache size to " << configuredBytes
         << " bytes from " << TCMallocMaxTotalThreadCacheSizeEnv << Endl;
    return true;
#else
    Cerr << TCMallocMaxTotalThreadCacheSizeEnv << " is supported only by Linux ydbd builds"
         << Endl;
    return false;
#endif
}

bool ConfigureTCMallocFromEnvironment() {
    return ConfigureTCMallocMaxPerCpuCacheSizeFromEnvironment()
        && ConfigureTCMallocMaxTotalThreadCacheSizeFromEnvironment();
}

} // anonymous namespace

int main(int argc, char **argv) {
    SetupTerminateHandler();

    if (!ConfigureTCMallocFromEnvironment()) {
        return EXIT_FAILURE;
    }

    auto factories = std::make_shared<NKikimr::TModuleFactories>();
    factories->DataShardExportFactory = std::make_shared<TDataShardExportFactory>();
    factories->CreateTicketParser = NKikimr::CreateTicketParser;
    factories->FolderServiceFactory = NKikimr::NFolderService::CreateFolderServiceActor;
    factories->IoContextFactory = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
    factories->DataStreamsAuthFactory = std::make_shared<NKikimr::NHttpProxy::TIamAuthFactory>();
    factories->AdditionalComputationNodeFactories = { NYql::GetPgFactory() };
    factories->SqsAuthFactory = std::make_shared<NKikimr::NSQS::TMultiAuthFactory>();
    factories->SqsEventsWriterFactory = std::make_shared<TSqsEventsWriterFactory>();
    factories->SchemeOperationFactory.reset(NKikimr::NSchemeShard::DefaultOperationFactory());
    factories->ConfigSwissKnife = NKikimr::NYamlConfig::CreateDefaultConfigSwissKnife();
    factories->TransferWriterFactory = std::make_shared<NKikimr::NReplication::NTransfer::TTransferWriterFactory>();

    return ParameterizedMain(argc, argv, std::move(factories));
}
