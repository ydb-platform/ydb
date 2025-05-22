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
    TTpcdsCsvItemWriter<W_CUSTOMER_DEMOGRAPHICS_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cd_demo_sk, CD_DEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_gender, CD_GENDER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_marital_status, CD_MARITAL_STATUS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_education_status, CD_EDUCATION_STATUS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_purchase_estimate, CD_PURCHASE_ESTIMATE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cd_credit_rating, CD_CREDIT_RATING);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_count, CD_DEP_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_employed_count, CD_DEP_EMPLOYED_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cd_dep_college_count, CD_DEP_COLLEGE_COUNT);

    TVector<W_CUSTOMER_DEMOGRAPHICS_TBL> customerDemoList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_customer_demographics(&customerDemoList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(customerDemoList);
};

}
