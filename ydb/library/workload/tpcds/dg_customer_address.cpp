#include "dg_customer_address.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_customer_address.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorCustomerAddress::TTpcDSGeneratorCustomerAddress(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CUSTOMER_ADDRESS)
{}

void TTpcDSGeneratorCustomerAddress::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_CUSTOMER_ADDRESS_TBL> customerAddressList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_customer_address(&customerAddressList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_CUSTOMER_ADDRESS_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "ca_address_sk", ca_addr_sk);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "ca_address_id", ca_addr_id);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, ca, ca_address);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, ca_location_type);
    writer.Write(customerAddressList);
};

}
