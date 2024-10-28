#include "dg_customer_demographics.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_customer_demographics.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorCustomerDemographics::TTpcDSGeneratorCustomerDemographics(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CUSTOMER_DEMOGRAPHICS)
{}

void TTpcDSGeneratorCustomerDemographics::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_CUSTOMER_DEMOGRAPHICS_TBL> customerDemoList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_customer_demographics(&customerDemoList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_CUSTOMER_DEMOGRAPHICS_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cd_demo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_gender);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_marital_status);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_education_status);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_purchase_estimate);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_credit_rating);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_employed_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_college_count);
    writer.Write(customerDemoList);
};

}
