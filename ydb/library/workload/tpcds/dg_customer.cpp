#include "dg_customer.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_customer.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorCustomer::TTpcDSGeneratorCustomer(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CUSTOMER)
{}

void TTpcDSGeneratorCustomer::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_CUSTOMER_TBL> customerList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_customer(&customerList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_CUSTOMER_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_customer_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_addr_sk);
    CSV_WRITER_REGISTER_FIELD(writer, "c_first_shipto_date_sk", c_first_shipto_date_id);
    CSV_WRITER_REGISTER_FIELD(writer, "c_first_sales_date_sk", c_first_sales_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_salutation);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_first_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_last_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, c_preferred_cust_flag);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_day);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_month);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_year);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_birth_country);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_login);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_email_address);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_last_review_date);
    writer.Write(customerList);
};

}
