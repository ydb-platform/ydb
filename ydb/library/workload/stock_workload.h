#pragma once

#include "workload_query_generator.h"

#include <cctype>

#include <random>

namespace NYdbWorkload {

struct TStockWorkloadParams : public TWorkloadParams {
    size_t ProductCount = 0;
    size_t Quantity = 0;
    size_t OrderCount = 0;
    unsigned int MinPartitions = 0;
    unsigned int Limit = 0;
    bool PartitionsByLoad = true;
    bool EnableCdc = false;
};

class TStockWorkloadGenerator : public IWorkloadQueryGenerator {
public:

    static TStockWorkloadGenerator* New(const TStockWorkloadParams* params) {
        return new TStockWorkloadGenerator(params);
    }

    virtual ~TStockWorkloadGenerator() {}

    std::string GetDDLQueries() const override;

    TQueryInfoList GetInitialData() override;

    std::string GetCleanDDLQueries() const override;

    TQueryInfoList GetWorkload(int type) override;

    TStockWorkloadParams* GetParams() override;

    enum class EType {
        InsertRandomOrder,
        SubmitRandomOrder,
        SubmitSameOrder,
        GetRandomCustomerHistory,
        GetCustomerHistory,
        MaxType
    };

private:
    static const unsigned int MAX_CUSTOMERS = 10000; // We will have just 10k customers

    TQueryInfoList InsertRandomOrder();
    TQueryInfoList SubmitRandomOrder();
    TQueryInfoList SubmitSameOrder();
    TQueryInfoList GetRandomCustomerHistory();
    TQueryInfoList GetCustomerHistory();

    using TProductsQuantity = std::map<std::string, int64_t>;
    TQueryInfo InsertOrder(const uint64_t orderID, const std::string& customer, const TProductsQuantity& products);
    TQueryInfo ExecuteOrder(const uint64_t orderID);
    TQueryInfo SelectCustomerHistory(const std::string& customerId, const unsigned int limit);

    std::string GetCustomerId();
    unsigned int GetProductCountInOrder();
    TProductsQuantity GenerateOrder(unsigned int productCountInOrder, int quantity);

    TStockWorkloadGenerator(const TStockWorkloadParams* params);

    TQueryInfo FillStockData() const;

    std::string DbPath;
    TStockWorkloadParams Params;

    std::random_device Rd;
    std::mt19937_64 Gen;
    std::exponential_distribution<> RandExpDistrib;
    std::uniform_int_distribution<unsigned int> CustomerIdGenerator;
    std::uniform_int_distribution<unsigned int> ProductIdGenerator;
};

} // namespace NYdbWorkload
