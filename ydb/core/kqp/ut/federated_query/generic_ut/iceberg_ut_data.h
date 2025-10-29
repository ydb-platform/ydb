#pragma once 

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/defaults.h>

namespace NTestUtils {

struct TTestData;
class TIcebergTestData final {
    friend struct TTestData;

public:    
    enum EAuthType : int {
        AuthBasic = 1,
        AuthSa = 2,
        AuthToken = 3
    }; 

    struct TAuth {
        EAuthType Type;
        TString Id;
        TString Value;
    };

public:
    TIcebergTestData(TAuth auth, const TString& dataSourceName, const TString& database, bool UseTls);

    NYql::TGenericDataSourceInstance CreateDataSourceForHadoop();

    NYql::TGenericDataSourceInstance CreateDataSourceForHiveMetastore();

    void ExecuteCreateHiveMetastoreExternalDataSource(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr);

    void ExecuteCreateHadoopExternalDataSource(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr);

private: 
    TString CreateAuthSection();

    TString CreateQuery(const TString& catalogSection);

    void ExecuteQuery(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr, const TString& query);

private: 
    const TAuth Auth_;
    const TString DataSourceName_;
    const TString Database_;
    const bool UseTls_;
};

TIcebergTestData CreateIcebergBasic(const TString& dataSourceName = NYql::NConnector::NTest::DEFAULT_DATA_SOURCE_NAME, 
                                    const TString& database = NYql::NConnector::NTest::DEFAULT_DATABASE, 
                                    const TString& userName = NYql::NConnector::NTest::DEFAULT_LOGIN,
                                    const TString& password = NYql::NConnector::NTest::DEFAULT_PASSWORD);

TIcebergTestData CreateIcebergToken(const TString& dataSourceName = NYql::NConnector::NTest::DEFAULT_DATA_SOURCE_NAME, 
                                    const TString& database = NYql::NConnector::NTest::DEFAULT_DATABASE, 
                                    const TString& token = NYql::NConnector::NTest::DEFAULT_PASSWORD);

TIcebergTestData CreateIcebergSa(const TString& dataSourceName = NYql::NConnector::NTest::DEFAULT_DATA_SOURCE_NAME, 
                                 const TString& database = NYql::NConnector::NTest::DEFAULT_DATABASE, 
                                 const TString& token = NYql::NConnector::NTest::DEFAULT_PASSWORD);

std::shared_ptr<NYql::ISecuredServiceAccountCredentialsFactory> CreateCredentialsFactory(const TString& token = NYql::NConnector::NTest::DEFAULT_PASSWORD);

} // NTestUtils
