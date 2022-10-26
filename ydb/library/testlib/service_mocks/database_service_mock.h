#pragma once

#include <cloud/bitbucket/private-api/yandex/cloud/priv/ydb/v1/database_service.grpc.pb.h>
#include <util/string/join.h>
#include <util/string/vector.h>

class TDatabaseServiceMock : public yandex::cloud::priv::ydb::v1::DatabaseService::Service {
public:
    THashMap<TString, yandex::cloud::priv::ydb::v1::Database> PersistentDatabases;
    THashMap<TString, yandex::cloud::priv::ydb::v1::Database> TemporaryDatabases;
    TString Identity;

    TMaybe<grpc::Status> CheckAuthorization(grpc::ServerContext* context) {
        if (!Identity.empty()) {
            auto[reqIdBegin, reqIdEnd] = context->client_metadata().equal_range("authorization");
            UNIT_ASSERT_C(reqIdBegin != reqIdEnd, "Authorization is expected.");
            if (Identity != TStringBuf(reqIdBegin->second.cbegin(), reqIdBegin->second.cend())) {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                                    TStringBuilder() << "Access for user " << Identity << " is forbidden");
            }
        }

        return Nothing();
    }

    virtual grpc::Status GetByPath(grpc::ServerContext* context,
                                   const yandex::cloud::priv::ydb::v1::GetDatabaseByPathRequest* request,
                                   yandex::cloud::priv::ydb::v1::Database* response) override
    {
        auto status = CheckAuthorization(context);
        auto parts = SplitString(request->path(), "/");
        Y_ENSURE(parts.size() >= 3);
        TString canonizedPath = "/" + JoinRange("/", parts.begin(), parts.begin() + 3);

        if (auto itPersistent = PersistentDatabases.find(canonizedPath); itPersistent != PersistentDatabases.end()) {
            *response = itPersistent->second;
            return grpc::Status::OK;
        } else {
            auto it = TemporaryDatabases.find(canonizedPath);
            if (it == TemporaryDatabases.end()) {
                return grpc::Status(grpc::StatusCode::NOT_FOUND, TStringBuilder() << " database with name " << request->path() << " not found");
            } else {
                *response = it->second;
                return grpc::Status::OK;
            }
        }
    }

    virtual grpc::Status ListAll(grpc::ServerContext* context,
                                 const yandex::cloud::priv::ydb::v1::ListAllDatabasesRequest* request,
                                 yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse* response) override
    {
        auto status = CheckAuthorization(context);
        if (status.Defined()) {
            return *status;
        }

        if (PersistentDatabases.empty()) {
            return grpc::Status::OK;
        }
        auto it = PersistentDatabases.begin();
        if (!request->page_token().empty()) {
            it = PersistentDatabases.find(request->page_token());
        }
        Y_ENSURE(it != PersistentDatabases.end());
        *response->add_databases() = it->second;
        it++;
        if (it != PersistentDatabases.end()) {
            response->set_next_page_token(it->first);
        }
        return grpc::Status::OK;
    }

};

