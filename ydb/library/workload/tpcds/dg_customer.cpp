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
    TTpcdsCsvItemWriter<W_CUSTOMER_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_customer_sk, C_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_customer_id, C_CUSTOMER_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_cdemo_sk, C_CURRENT_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_hdemo_sk, C_CURRENT_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, c_current_addr_sk, C_CURRENT_ADDR_SK);
    CSV_WRITER_REGISTER_FIELD(writer, "c_first_shipto_date_sk", c_first_shipto_date_id, C_FIRST_SHIPTO_DATE_ID);
    CSV_WRITER_REGISTER_FIELD(writer, "c_first_sales_date_sk", c_first_sales_date_id, C_FIRST_SALES_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_salutation, C_SALUTATION);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_first_name, C_FIRST_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_last_name, C_LAST_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, c_preferred_cust_flag, C_PREFERRED_CUST_FLAG);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_day, C_BIRTH_DAY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_month, C_BIRTH_MONTH);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_birth_year, C_BIRTH_YEAR);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_birth_country, C_BIRTH_COUNTRY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_login, C_LOGIN);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, c_email_address, C_EMAIL_ADDRESS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, c_last_review_date, C_LAST_REVIEW_DATE);

    TVector<W_CUSTOMER_TBL> customerList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_customer(&customerList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(customerList);
};

}
