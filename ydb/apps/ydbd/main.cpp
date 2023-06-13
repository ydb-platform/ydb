#include "sqs.h"
#include "export.h"
#include <ydb/core/driver_lib/run/main.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/ymq/actor/auth_factory.h>
#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/core/http_proxy/auth_factory.h>


int main(int argc, char **argv) {
    SetupTerminateHandler();

    auto factories = std::make_shared<NKikimr::TModuleFactories>();
    factories->DataShardExportFactory = std::make_shared<TDataShardExportFactory>();
    factories->CreateTicketParser = NKikimr::CreateTicketParser;
    factories->FolderServiceFactory = NKikimr::NFolderService::CreateMockFolderServiceAdapterActor;
    factories->IoContextFactory = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
    factories->SqsAuthFactory = std::make_shared<NKikimr::NSQS::TAuthFactory>();
    factories->DataStreamsAuthFactory = std::make_shared<NKikimr::NHttpProxy::TIamAuthFactory>();
    factories->AdditionalComputationNodeFactories = { NYql::GetPgFactory() };

    return ParameterizedMain(argc, argv, std::move(factories));
}
