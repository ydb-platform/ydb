#include "stock.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <cmath>
#include <format>
#include <iomanip>
#include <string>
#include <thread>
#include <random>

namespace {
uint64_t getOrderId() {
    static thread_local std::mt19937_64 generator;
    generator.seed(Now().MicroSeconds() + std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::uniform_int_distribution<uint64_t> distribution(1, UINT64_MAX);
    return distribution(generator);
}
}

namespace NYdbWorkload {

TStockWorkloadGenerator::TStockWorkloadGenerator(const TStockWorkloadParams* params)
    : TBase(params)
    , DbPath(params->DbPath)
    , Rd()
    , Gen(Rd())
    , RandExpDistrib(1.6)
    , CustomerIdGenerator(1, MAX_CUSTOMERS)
    , ProductIdGenerator(1, params->ProductCount)
{
    Gen.seed(Now().MicroSeconds());
}

std::string TStockWorkloadGenerator::GetDDLQueries() const {
    std::string stockPartitionsDdl = "";
    std::string ordersPartitionsDdl = "WITH (READ_REPLICAS_SETTINGS = \"per_az:1\")";
    std::string orderLinesPartitionsDdl = "";
    if (Params.PartitionsByLoad) {
        stockPartitionsDdl = std::format(R"(WITH (
              AUTO_PARTITIONING_BY_LOAD = ENABLED
            , AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {0}
        ))", Params.MinPartitions);
        ordersPartitionsDdl = std::format(R"(WITH (
              READ_REPLICAS_SETTINGS = "per_az:1"
            , AUTO_PARTITIONING_BY_LOAD = ENABLED
            , AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {0}
            , AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
            , UNIFORM_PARTITIONS = {0}
        ))", Params.MinPartitions);
        orderLinesPartitionsDdl = std::format(R"(WITH (
              AUTO_PARTITIONING_BY_LOAD = ENABLED
            , AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {0}
            , AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
            , UNIFORM_PARTITIONS = {0}
        ))", Params.MinPartitions);
    }

    std::string changefeeds = "";
    if (Params.EnableCdc) {
        changefeeds = std::format(R"(ALTER TABLE `{0}/orders` ADD CHANGEFEED `updates` WITH (
              FORMAT = 'JSON'
            , MODE = 'UPDATES'
        );)", DbPath);
    }

    return std::format(R"(--!syntax_v1
        CREATE TABLE `{0}/stock`(product Utf8, quantity Int64, PRIMARY KEY(product)) {1};
        CREATE TABLE `{0}/orders`(id Uint64, customer Utf8, created Datetime, processed Datetime, PRIMARY KEY(id), INDEX ix_cust GLOBAL ON (customer, created) COVER (processed)) {2};
        CREATE TABLE `{0}/orderLines`(id_order Uint64, product Utf8, quantity Int64, PRIMARY KEY(id_order, product)) {3};
        {4}
    )", DbPath, stockPartitionsDdl, ordersPartitionsDdl, orderLinesPartitionsDdl, changefeeds);
}

TQueryInfoList TStockWorkloadGenerator::GetInitialData() {
    TQueryInfoList res;
    res.push_back(FillStockData());
    for (size_t i = 0; i < Params.OrderCount; ++i) {
        auto queryInfos = InsertRandomOrder();
        res.insert(res.end(), queryInfos.begin(), queryInfos.end());
    }
    return res;
}

TVector<std::string> TStockWorkloadGenerator::GetCleanPaths() const {
    return {"stock", "orders", "orderLines"};
}

TQueryInfo TStockWorkloadGenerator::FillStockData() const {
    std::string query = R"(--!syntax_v1
        DECLARE $stocks AS List<Struct<product:Utf8,quantity:Int64>>;
        INSERT INTO `stock`(product, quantity) SELECT product, quantity from AS_TABLE( $stocks );
    )";

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < Params.ProductCount; ++i) {
        char productName[8] = "";
        std::snprintf(productName, sizeof(productName), "p%.6zu", i);
        rows.AddListItem()
                .BeginStruct()
                .AddMember("product").Utf8(productName)
                .AddMember("quantity").Int64(Params.Quantity)
                .EndStruct();
    }
    rows.EndList();

    NYdb::TParamsBuilder paramsBuilder;
    paramsBuilder.AddParam("$stocks", rows.Build());

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfoList TStockWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::InsertRandomOrder:
            return InsertRandomOrder();
        case EType::SubmitRandomOrder:
            return SubmitRandomOrder();
        case EType::SubmitSameOrder:
            return SubmitSameOrder();
        case EType::GetRandomCustomerHistory:
            return GetRandomCustomerHistory();
        case EType::GetCustomerHistory:
            return GetCustomerHistory();
        default:
            return TQueryInfoList();
    }
}


TVector<IWorkloadQueryGenerator::TWorkloadType> TStockWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::InsertRandomOrder), "add-rand-order", "Inserts orders with random ID without their processing");
    result.emplace_back(static_cast<int>(EType::SubmitRandomOrder), "put-rand-order", "Submit random orders with processing");
    result.emplace_back(static_cast<int>(EType::SubmitSameOrder), "put-same-order", "Submit orders with same products with processing");
    result.emplace_back(static_cast<int>(EType::GetRandomCustomerHistory), "rand-user-hist", "Selects orders of random customer");
    result.emplace_back(static_cast<int>(EType::GetCustomerHistory), "user-hist", "Selects orders of 10000th customer");
    return result;
}

TQueryInfo TStockWorkloadGenerator::InsertOrder(const uint64_t orderID, const std::string& customer, const TProductsQuantity& products) {
    std::string query = R"(--!syntax_v1
        DECLARE $ido AS UInt64;
        DECLARE $cust as Utf8;
        DECLARE $lines AS List<Struct<product:Utf8,quantity:Int64>>;
        DECLARE $time AS DateTime;
        INSERT INTO `orders`(id, customer, created) values ($ido, $cust, $time);
        UPSERT INTO `orderLines`(id_order, product, quantity) SELECT $ido, product, quantity from AS_TABLE( $lines );
    )";

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (auto const& [product, quantity] : products) {
        rows.AddListItem()
                .BeginStruct()
                .AddMember("product").Utf8(product.c_str())
                .AddMember("quantity").Int64(quantity)
                .EndStruct();
    }
    rows.EndList();

    NYdb::TParamsBuilder paramsBuilder;
    paramsBuilder
        .AddParam("$ido")
            .Uint64(orderID)
            .Build()
        .AddParam("$cust")
            .Utf8(customer.c_str())
            .Build()
        .AddParam("$time")
            .Datetime(Now())
            .Build()
        .AddParam("$lines", rows.Build());

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfo TStockWorkloadGenerator::ExecuteOrder(const uint64_t orderID) {
    std::string query = R"(--!syntax_v1
        DECLARE $ido AS UINT64;
        DECLARE $time AS DateTime;
        $prods = SELECT * FROM orderLines as p WHERE p.id_order = $ido;
        $cnt = SELECT count(*) FROM $prods;
        $newq = SELECT p.product AS product, COALESCE(s.quantity,0)-p.quantity AS quantity
                FROM   $prods as p LEFT JOIN stock AS s on s.product = p.product;
        $check = SELECT count(*) as cntd FROM $newq as q where q.quantity >= 0;
        UPSERT INTO stock SELECT product, quantity FROM $newq where $check=$cnt;
        $upo = SELECT id, $time as tm FROM orders WHERE id = $ido and $check = $cnt;
        UPSERT INTO orders SELECT id, tm as processed FROM $upo;
        SELECT * from $newq as q where q.quantity < 0
    )";

    NYdb::TParamsBuilder paramsBuilder;
    paramsBuilder
        .AddParam("$ido")
            .Uint64(orderID)
            .Build()
        .AddParam("$time")
            .Datetime(Now())
            .Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

TQueryInfo TStockWorkloadGenerator::SelectCustomerHistory(const std::string& customerId, const unsigned int limit) {
    std::string query = R"(--!syntax_v1
        DECLARE $cust as Utf8;
        DECLARE $limit as UInt32;
        select id, customer, created
        from orders view ix_cust
        where customer = $cust
        order by customer desc, created desc
        limit $limit;
    )";

    NYdb::TParamsBuilder paramsBuilder;
    paramsBuilder
        .AddParam("$cust")
            .Utf8(customerId.c_str())
            .Build()
        .AddParam("$limit")
            .Uint32(limit)
            .Build();

    return TQueryInfo(query, paramsBuilder.Build());
}

std::string TStockWorkloadGenerator::GetCustomerId() {
    return "Name" + std::to_string(CustomerIdGenerator(Gen));
}

unsigned int TStockWorkloadGenerator::GetProductCountInOrder() {
    unsigned int productCountInOrder = 0;
    while (productCountInOrder == 0) {
        productCountInOrder = std::abs(std::round(RandExpDistrib(Gen) * 2));
    }
    return productCountInOrder;
}

TStockWorkloadGenerator::TProductsQuantity TStockWorkloadGenerator::GenerateOrder(unsigned int productCountInOrder, int quantity) {
    TProductsQuantity products;
    for (unsigned i = 0; i < productCountInOrder; ++i) {
        char productName[8] = "";
        std::snprintf(productName, sizeof(productName), "p%.6i", ProductIdGenerator(Gen));
        products.emplace(productName, quantity);
    }
    return products;
}

TQueryInfoList TStockWorkloadGenerator::InsertRandomOrder() {
    uint64_t orderID = getOrderId();
    auto customer = GetCustomerId();
    auto productCountInOrder = GetProductCountInOrder();
    auto products = GenerateOrder(productCountInOrder, 1);
    return TQueryInfoList(1, InsertOrder(orderID, customer, products));
}

TQueryInfoList TStockWorkloadGenerator::SubmitRandomOrder() {
    TQueryInfoList res;

    uint64_t orderID = getOrderId();
    auto customer = GetCustomerId();
    auto productCountInOrder = GetProductCountInOrder();
    auto products = GenerateOrder(productCountInOrder, 1);

    res.push_back(InsertOrder(orderID, customer, products));
    res.push_back(ExecuteOrder(orderID));
    return res;
}

TQueryInfoList TStockWorkloadGenerator::SubmitSameOrder() {
    TQueryInfoList res;

    uint64_t orderID = getOrderId();
    auto customer = GetCustomerId();

    char productName[8] = "";
    TProductsQuantity products;
    for (unsigned i = 0; i < Params.ProductCount; ++i) {
        std::snprintf(productName, sizeof(productName), "p%.6i", i);
        products.emplace(productName, 1);
    }
    res.push_back(InsertOrder(orderID, customer, products));
    res.push_back(ExecuteOrder(orderID));
    return res;
}

TQueryInfoList TStockWorkloadGenerator::GetRandomCustomerHistory() {
    TQueryInfoList res;

    auto customer = GetCustomerId();
    res.push_back(SelectCustomerHistory(customer, Params.Limit));
    return res;
}

TQueryInfoList TStockWorkloadGenerator::GetCustomerHistory() {
    TQueryInfoList res;

    auto customer = "Name" + std::to_string(MAX_CUSTOMERS);
    res.push_back(SelectCustomerHistory(customer, Params.Limit));
    return res;
}

void TStockWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption('p', "products", "Product count. Value in 1..500 000.")
            .DefaultValue(100).StoreResult(&ProductCount);
        opts.AddLongOption('q', "quantity", "Quantity of each product in stock.")
            .DefaultValue(1000).StoreResult(&Quantity);
        opts.AddLongOption('o', "orders", "Initial orders count.")
            .DefaultValue(100).StoreResult(&OrderCount);
        opts.AddLongOption("min-partitions", "Minimum partitions for tables.")
            .DefaultValue(40).StoreResult(&MinPartitions);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
            .DefaultValue(true).StoreResult(&PartitionsByLoad);
        opts.AddLongOption("enable-cdc", "Create changefeeds on tables.")
            .DefaultValue(false).StoreTrue(&EnableCdc).Hidden();
        break;
    case TWorkloadParams::ECommandType::Run:
        switch (static_cast<TStockWorkloadGenerator::EType>(workloadType)) {
        case TStockWorkloadGenerator::EType::InsertRandomOrder:
        case TStockWorkloadGenerator::EType::SubmitRandomOrder:
        case TStockWorkloadGenerator::EType::SubmitSameOrder:
            opts.AddLongOption('p', "products", "Products count to use in workload.")
                .DefaultValue(100).StoreResult(&ProductCount);
            break;
        case TStockWorkloadGenerator::EType::GetRandomCustomerHistory:
        case TStockWorkloadGenerator::EType::GetCustomerHistory:
            opts.AddLongOption('l', "limit", "Number of last orders to select.")
                .DefaultValue(10).StoreResult(&Limit);
            break;
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TStockWorkloadParams::CreateGenerator() const {
    return MakeHolder<TStockWorkloadGenerator>(this);
}

TString TStockWorkloadParams::GetWorkloadName() const {
    return "stock";
}

}
