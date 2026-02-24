#include "yql_yt_tvm_helpers.h"
#include <util/stream/file.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/string/strip.h>
#include <util/system/fs.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

TMaybe<TString> GetTvmServiceTicketFromHeaders(const THttpHeaders& headers) {
    auto serviceTicket = std::find_if(
        headers.Begin(),
        headers.End(),
        [](const auto& h) { return h.Name() == "X-Ya-Service-Ticket"; }
    );
    if (serviceTicket == headers.End()) {
       return Nothing();
    }
    return serviceTicket->Value();
}

void CheckTvmServiceTicket(const THttpHeaders& headers, IFmrTvmClient::TPtr tvmClient, const std::vector<TTvmId> allowedSourceTvmIds) {
    if (tvmClient) {
        auto workerServiceTicket = GetTvmServiceTicketFromHeaders(headers);
        if (!workerServiceTicket) {
            ythrow yexception() << "Tvm service ticket is not passed in headers";
        }

        tvmClient->CheckTvmServiceTicket(*workerServiceTicket, allowedSourceTvmIds);
    }
}

TMaybe<TFmrTvmSpec> ParseFmrTvmSpec(const TMaybe<TString>& tvmSpecFilePath) {
    if (!tvmSpecFilePath.Defined()) {
        return Nothing();
    }
    TFileInput tvmSpecFile(*tvmSpecFilePath);
    auto tvmConfig = NYT::NodeFromYsonStream(&tvmSpecFile);
    return TFmrTvmSpec{
        .WorkerTvmAlias = tvmConfig["worker"]["tvm_alias"].AsString(),
        .CoordinatorTvmAlias = tvmConfig["coordinator"]["tvm_alias"].AsString(),
        .TableDataServiceTvmAlias = tvmConfig["table_data_service"]["tvm_alias"].AsString(),
        .WorkerTvmId = tvmConfig["worker"]["tvm_id"].AsUint64(),
        .CoordinatorTvmId = tvmConfig["coordinator"]["tvm_id"].AsUint64(),
        .TableDataServiceTvmId = tvmConfig["table_data_service"]["tvm_id"].AsUint64(),
    };
}

TMaybe<TString> ParseFmrTvmSecretFile(const TMaybe<TString>& tvmSecretFilePath) {
    if (!tvmSecretFilePath.Defined()) {
        return Nothing();
    }
    YQL_ENSURE(NFs::Exists(*tvmSecretFilePath), "Tvm secret file should exist");
    return StripStringRight(TFileInput(*tvmSecretFilePath).ReadLine());
}

} // namespace NYql::NFmr
