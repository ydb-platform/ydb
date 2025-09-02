#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace NJson;
using namespace NKikimrSchemeOp;
using namespace NYql;

namespace {

bool Convert(const TColumnDescription& in, Ydb::Table::ColumnMeta& out, TIssues& issues) {
    try {
        FillColumnDescription(out, in);
    } catch (const std::exception& ex) {
        issues.AddIssue(TStringBuilder() << "Unable to fill the column description, error: " << ex.what());
        return false;
    }
    return true;
}

bool ConvertContent(
    const TString& sourceType,
    const TString& in,
    google::protobuf::Map<TProtoStringType, TProtoStringType>& out,
    TIssues& issues
) {
    const auto externalSourceFactory = NExternalSource::CreateExternalSourceFactory({}, nullptr, 50000, nullptr, false, false, true, NYql::GetAllExternalDataSourceTypes());
    try {
        const auto source = externalSourceFactory->GetOrCreate(sourceType);
        for (const auto& [key, items] : source->GetParameters(in)) {
            TJsonValue json(EJsonValueType::JSON_ARRAY);
            for (const auto& item : items) {
                json.AppendValue(item);
            }
            out[to_upper(key)] = WriteJson(json, false);
        }
    } catch (...) {
        issues.AddIssue(TStringBuilder() << "Cannot unpack the content of an external table of type: " << sourceType
            << ", error: " << CurrentExceptionMessage()
        );
        return false;
    }
    return true;
}

std::optional<Ydb::Table::DescribeExternalTableResult> Convert(
    const TDirEntry& inSelf,
    const TExternalTableDescription& inDesc,
    TIssues& issues
) {
    Ydb::Table::DescribeExternalTableResult out;
    ConvertDirectoryEntry(inSelf, out.mutable_self(), true);

    out.set_source_type(inDesc.GetSourceType());
    out.set_data_source_path(inDesc.GetDataSourcePath());
    out.set_location(inDesc.GetLocation());
    for (const auto& column : inDesc.GetColumns()) {
        if (!Convert(column, *out.add_columns(), issues)) {
            return std::nullopt;
        }
    }
    if (!ConvertContent(inDesc.GetSourceType(), inDesc.GetContent(), *out.mutable_content(), issues)) {
        return std::nullopt;
    }
    return out;
}

}

using TEvDescribeExternalTableRequest = TGrpcRequestOperationCall<
    Ydb::Table::DescribeExternalTableRequest,
    Ydb::Table::DescribeExternalTableResponse
>;

class TDescribeExternalTableRPC : public TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest>;

public:

    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

private:

    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(GetProtoRequest()->path());

        Send(MakeTxProxyID(), ev.release());
        Become(&TDescribeExternalTableRPC::StateDescribeScheme);
    }

    STATEFN(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto& pathDescription = record.GetPathDescription();

        if (record.HasReason()) {
            Request_->RaiseIssue(TIssue(record.GetReason()));
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeExternalTable) {
                    Request_->RaiseIssue(TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()
                    ));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                TIssues issues;
                const auto description = Convert(pathDescription.GetSelf(), pathDescription.GetExternalTableDescription(), issues);
                if (!description) {
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
                    return;
                }
                return ReplyWithResult(
                    Ydb::StatusIds::SUCCESS,
                    *description,
                    ctx
                );
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);

            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);

            case NKikimrScheme::StatusNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);

            default:
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }
    }
};

void DoDescribeExternalTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeExternalTableRPC(p.release()));
}

}
