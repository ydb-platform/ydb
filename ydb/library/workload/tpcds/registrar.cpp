#include "tpcds.h"
#include "dg_call_center.h"
#include "dg_catalog_sales.h"
#include "dg_catalog_page.h"
#include "dg_customer.h"
#include "dg_customer_address.h"
#include "dg_customer_demographics.h"
#include "dg_date_dim.h"
#include "dg_household_demographics.h"
#include "dg_income_band.h"
#include "dg_inventory.h"
#include "dg_item.h"
#include "dg_promotion.h"
#include "dg_reason.h"
#include "dg_ship_mode.h"
#include "dg_store.h"
#include "dg_store_sales.h"
#include "dg_time_dim.h"
#include "dg_warehouse.h"
#include "dg_web_page.h"
#include "dg_web_sales.h"
#include "dg_web_site.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TTpcdsWorkloadParams> TpcdsRegistrar("tpcds");

const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCallCenter> TTpcDSGeneratorCallCenter::Registrar("call_center");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCatalogSales> TTpcDSGeneratorCatalogSales::Registrar("catalog_sales");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCatalogPage> TTpcDSGeneratorCatalogPage::Registrar("catalog_page");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCustomer> TTpcDSGeneratorCustomer::Registrar("customer");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCustomerAddress> TTpcDSGeneratorCustomerAddress::Registrar("customer_address");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorCustomerDemographics> TTpcDSGeneratorCustomerDemographics::Registrar("customer_demographics");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorDateDim> TTpcDSGeneratorDateDim::Registrar("date_dim");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorHouseholdDemographics> TTpcDSGeneratorHouseholdDemographics::Registrar("household_demo");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorIncomeBand> TTpcDSGeneratorIncomeBand::Registrar("income_band");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorInventory> TTpcDSGeneratorInventory::Registrar("inventory");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorItem> TTpcDSGeneratorItem::Registrar("item");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorPromotion> TTpcDSGeneratorPromotion::Registrar("promotion");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorReason> TTpcDSGeneratorReason::Registrar("reason");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorShipMode> TTpcDSGeneratorShipMode::Registrar("ship_mode");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorStore> TTpcDSGeneratorStore::Registrar("store");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorStoreSales> TTpcDSGeneratorStoreSales::Registrar("store_sales");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorTimeDim> TTpcDSGeneratorTimeDim::Registrar("time_dim");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorWarehouse> TTpcDSGeneratorWarehouse::Registrar("warehouse");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorWebSales> TTpcDSGeneratorWebSales::Registrar("web_sales");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorWebPage> TTpcDSGeneratorWebPage::Registrar("web_page");
const TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TTpcDSGeneratorWebSite> TTpcDSGeneratorWebSite::Registrar("web_site");

}