#include "terminal.h"

#include <ydb/library/workload/tpcc/util.h>
#include <ydb/library/workload/tpcc/procedures/new_order.h>
#include <ydb/library/workload/tpcc/procedures/payment.h>
#include <ydb/library/workload/tpcc/procedures/order_status.h>
#include <ydb/library/workload/tpcc/procedures/delivery.h>
#include <ydb/library/workload/tpcc/procedures/stock_level.h>


using namespace NYdb;
using namespace NTable;

namespace NYdbWorkload {
namespace NTPCC {

TTerminal::TTerminal(const TRunParams& params,
                     i32 warehouseId, i32 terminalId, TLog& log,
                     std::shared_ptr<TTableClient> tableClient,
                     std::shared_ptr<TTaskScheduler> taskScheduler,
                     std::shared_ptr<TThreadPool> threadPool,
                     std::shared_ptr<std::deque<TTerminal*>> readyTerminals,
                     TMutex& readyQueueMutex,
                     i32 seed)
    : Params(params)
    , Log(log)
    , FastRetryOption(params.RetryCount,
                      TDuration::Zero(),
                      TDuration::Zero(),
                      TDuration::Zero(),
                      TDuration::MilliSeconds(EWorkloadConstants::TPCC_FAST_RETRY_EXP_MULTIPLIER))
    , SlowRetryOption(params.RetryCount,
                      TDuration::Zero(),
                      TDuration::Zero(),
                      TDuration::Zero(),
                      TDuration::MilliSeconds(EWorkloadConstants::TPCC_SLOW_RETRY_EXP_MULTIPLIER))
    , TableClient(tableClient)
    , WarehouseId(warehouseId)
    , TerminalId(terminalId)
    , ThreadPool(threadPool)
    , Scheduler(taskScheduler)
    , ReadyTerminals(readyTerminals)
    , ReadyQueueMutex(readyQueueMutex)
    , Rng(seed, 0)
{
    for (i32 i = 0; i < EProcedureType::COUNT; i++) {
        Statistics.push_back(std::make_unique<TStatistics>(60000, 2));
    }
}

void TTerminal::Process(void*) {
    if (WorkloadEnded.load()) {
        return;
    }

    if (Resource.get() == nullptr) {
        GetProcedureResource();

        AddToScheduler(GetPreExecutionWait());
        return;
    }

    if (!ProcedureStarted) {
        StartTime = Now();
        ProcedureStarted = true;
    }

    Procedure->Run(Resource).Apply([this](const NThreading::TFuture<void>& future) {
        try {
            future.GetValue();

            if (Procedure->Finished()) {
                SuccessProcedure();
            } else {
                AddToThreadPool();
            }
        } catch (const TErrorException& ex) {
            if (Params.DebugMode) {
                TStringStream stream;
                stream << "Terminal ID=" << TerminalId << ": ";
                stream << "Procedure step (" << Procedure->GetType() << "): " << ex;
                Log.Write(TLOG_ERR, stream.Str());
            }

            EStatus status = ex.GetStatus();
            auto retry = HandleException(status);
            if (retry.Defined()) {
                RetryProcedure(*retry.Get(), status);
            } else {
                TStringStream stream;
                stream << ex;
                FailWithMessage(std::move(stream.Str()), status);
            }
        } catch (const std::exception& ex) {
            if (Params.DebugMode) {
                TStringStream stream;
                stream << "Terminal ID=" << TerminalId << ": ";
                stream << "Procedure step (" << Procedure->GetType() << "): " << ex.what();
                Log.Write(TLOG_ERR, stream.Str());
            }

            if (std::string(ex.what()).contains("EXPECTED")) {
                SuccessProcedure();
                return;
            }
            
            EStatus status = EStatus::CLIENT_INTERNAL_ERROR;
            auto retry = HandleException(status);
            if (retry.Defined()) {
                RetryProcedure(*retry.Get(), status);
            } else {
                FailWithMessage(ex.what(), status);
            }
        } catch (...) {
            FailWithMessage("Failed with an unknown error", EStatus::CLIENT_INTERNAL_ERROR);
        }
    });
}

const std::vector<std::unique_ptr<TStatistics>>& TTerminal::GetStatistics() {
    return Statistics;
}

void TTerminal::NotifyAboutWorkloadStart() {
    WorkloadStarted.store(true);
}

void TTerminal::NotifyAboutWorkloadEnd() {
    WorkloadEnded.store(true);
}

i32 TTerminal::GetTerminalId() {
    return TerminalId;
}

void TTerminal::GetProcedureResource() {
    switch (Procedure->GetType()) {
        case EProcedureType::NewOrder:
            return GetNewOrderResource();
        case EProcedureType::Payment:
            return GetPaymentResource();
        case EProcedureType::OrderStatus:
            return GetOrderStatusResource();
        case EProcedureType::Delivery:
            return GetDeliveryResource();
        case EProcedureType::StockLevel:
            return GetStockLevelResource();
        default:
            FailWithMessage("Non-existent procedure", EStatus::CLIENT_INTERNAL_ERROR);
    }
}

void TTerminal::GetNewOrderResource() {
    auto* resource = new TNewOrderProcedure::NewOrderResource();
    resource->WarehouseId = WarehouseId;
    resource->DistrictId = UniformRandom32(1, EWorkloadConstants::TPCC_DIST_PER_WH, Rng);
    resource->CustomerId = GetNonUniformCustomerId(Params.CustomerIdDelta, Rng);
    resource->ItemCount = UniformRandom32(5, 15, Rng);
    resource->ItemIds.assign(resource->ItemCount, -1);
    resource->SupplierWarehouseIds.assign(resource->ItemCount, -1);
    resource->OrderQuantities.assign(resource->ItemCount, -1);
    resource->AllLocal = 1;

    for (i32 i = 0; i < resource->ItemCount; ++i) {
        resource->ItemIds[i] = GetNonUniformItemId(Params.OrderLineItemIdDelta, Rng);

        if (UniformRandom32(1, 100, Rng) > 1) {
            resource->SupplierWarehouseIds[i] = WarehouseId;
        } else {
            do {
                resource->SupplierWarehouseIds[i] = UniformRandom32(1, Params.Warehouses, Rng);
            } while (resource->SupplierWarehouseIds[i] == WarehouseId && Params.Warehouses > 1);
            
            resource->AllLocal = 0;
        }
        
        resource->OrderQuantities[i] = UniformRandom32(1, 10, Rng);
    }

    if (UniformRandom32(1, 100, Rng) == 1) {
        resource->ItemIds[resource->ItemCount - 1] = EWorkloadConstants::TPCC_INVALID_ITEM_ID;
    }

    Resource.reset(resource);
}

void TTerminal::GetPaymentResource() {
    auto* resource = new TPaymentProcedure::PaymentResource();
    resource->WarehouseId = WarehouseId;
    resource->WarehouseCount = Params.Warehouses;
    resource->DistrictId = UniformRandom32(1, EWorkloadConstants::TPCC_DIST_PER_WH, Rng);
    resource->PaymentAmount = UniformRandom32(100, 500000, Rng) / 100.0;
    resource->CustomerIdDelta = Params.CustomerIdDelta;

    i32 randomX = UniformRandom32(1, 100, Rng);
    if (randomX <= 85) {
        resource->CustomerWhId = resource->WarehouseId;
        resource->CustomerDistId = resource->DistrictId;
    } else {
        resource->CustomerDistId = UniformRandom32(1, EWorkloadConstants::TPCC_DIST_PER_WH, Rng);
        do {
            resource->CustomerWhId = UniformRandom32(1, resource->WarehouseCount, Rng);
        } while (resource->CustomerWhId == resource->WarehouseId && resource->WarehouseCount > 1);
    }
    
    Resource.reset(resource);
}

void TTerminal::GetDeliveryResource() {
    auto* resource = new TDeliveryProcedure::DeliveryResource();
    resource->WarehouseId = WarehouseId;
    resource->WarehouseCount = Params.Warehouses;
    resource->CarrierId = UniformRandom32(1, 10, Rng);
    
    Resource.reset(resource);
}

void TTerminal::GetOrderStatusResource() {
    auto* resource = new TOrderStatusProcedure::OrderStatusResource();
    resource->WarehouseId = WarehouseId;
    resource->DistrictId = UniformRandom32(1, EWorkloadConstants::TPCC_DIST_PER_WH, Rng);
    resource->CustomerIdDelta = Params.CustomerIdDelta;
    
    Resource.reset(resource);
}

void TTerminal::GetStockLevelResource() {
    auto* resource = new TStockLevelProcedure::StockLevelResource();
    resource->WarehouseId = WarehouseId;
    resource->DistrictId = UniformRandom32(1, EWorkloadConstants::TPCC_DIST_PER_WH, Rng);
    resource->Threshold = UniformRandom32(10, 20, Rng);
    
    Resource.reset(resource);
}

TTerminal::ERetryType TTerminal::GetRetryType(EStatus status) {
    switch (status) {
        case EStatus::BAD_SESSION:
        case EStatus::ABORTED:
            return INSTANT_RETRY;

        case EStatus::CLIENT_CANCELLED:
        case EStatus::CLIENT_INTERNAL_ERROR:
        case EStatus::SESSION_BUSY:
        case EStatus::TRANSPORT_UNAVAILABLE:
        case EStatus::UNAVAILABLE:
        case EStatus::UNDETERMINED:
            return FAST_RETRY;

        case EStatus::NOT_FOUND:
        case EStatus::OVERLOADED:
        case EStatus::CLIENT_RESOURCE_EXHAUSTED:
        default:
            return SLOW_RETRY;
    }
}

TMaybe<TInstant> TTerminal::HandleException(EStatus status) {
    ERetryType retryType = GetRetryType(status);
    
    TInstant expire;
    RetryCount++;
    if (retryType == FAST_RETRY && RetryCount <= ui32(Params.RetryCount)) {
        expire = Now() + FastRetryOption.GetTimeToSleep(RetryCount - 1);
    } else if (retryType == SLOW_RETRY && RetryCount <= ui32(Params.RetryCount)) {
        expire = Now() + SlowRetryOption.GetTimeToSleep(RetryCount - 1);
    } else if (retryType == INSTANT_RETRY && RetryCount <= ui32(Params.InstanyRetryCount)) {
        expire = Now();
    } else {
        return TMaybe<TInstant>();
    }

    return TMaybe<TInstant>(expire);
}

void TTerminal::SuccessProcedure() {
    if (WorkloadStarted.load()) {
        Statistics[Procedure->GetType()]->Successes++;
        Statistics[Procedure->GetType()]->Hist.RecordValue((Now() - StartTime).MilliSeconds());
    }

    FinishAndGetNextProcedure();
}

void TTerminal::ProcedureFailed(EStatus status) {
    if (WorkloadStarted.load()) {
        Statistics[Procedure->GetType()]->Failes++;
        Statistics[Procedure->GetType()]->FailesWithStatus[status]++;
    }

    FinishAndGetNextProcedure();
}

void TTerminal::RetryProcedure(TInstant expire, EStatus status) {
    if (WorkloadStarted.load()) {
        Statistics[Procedure->GetType()]->Retries++;
        Statistics[Procedure->GetType()]->RetriesWithStatus[status]++;
    }

    Procedure->StartOver();
    AddToScheduler(expire);
}

void TTerminal::FailWithMessage(TString&& message, EStatus status) {
    if (Params.DebugMode || Params.ErrorMode) {
        TStringStream stream;
        stream << "Terminal ID=" << TerminalId << ": ";
        stream << Procedure->GetType() << ": " << Procedure->GetStageName() << ": ";
        stream << "Retry failed[" << status << "]: " << message;
        Log.Write(TLOG_ERR, stream.Str());
    }

    ProcedureFailed(status);
}

void TTerminal::FinishAndGetNextProcedure() {
    TInstant expire = GetPostExecutionWait();

    NextProcedure();
    
    AddToScheduler(expire);
}

void TTerminal::AddToScheduler(TInstant expire) {
    if (WorkloadEnded.load()) {
        return;
    }

    Scheduler->Add(MakeIntrusive<TSchedulerTerminalTask>(*this), expire);

    if (Params.DebugMode) {
        TStringStream stream;
        stream << "Terminal ID=" << TerminalId << ": " << Procedure->GetType() << " added to Scheduler";
        Log.Write(TLOG_DEBUG, stream.Str());
    }
}

void TTerminal::AddToThreadPool() {
    if (WorkloadEnded.load()) {
        return;
    }

    try {
        ThreadPool->SafeAdd(this);
    } catch (const std::exception& ex) {
        TStringStream stream;
        stream << "Couldn't add task to ThreadPool: " << ex.what(); 
        FailWithMessage(std::move(stream.Str()), EStatus::CLIENT_INTERNAL_ERROR);
    } catch (...) {
        FailWithMessage("Couldn't add task to ThreadPool", EStatus::CLIENT_INTERNAL_ERROR);
    }
}

void TTerminal::Start() {
    NextProcedure();
    AddToThreadPool();
}

TInstant TTerminal::GetPreExecutionWait() {
    ui32 durationS;
    switch (Procedure->GetType()) {
        case EProcedureType::NewOrder:
            durationS = EWorkloadConstants::TPCC_NEWORDER_PRE_EXEC_WAIT;
            break;
        case EProcedureType::Payment:
            durationS = EWorkloadConstants::TPCC_PAYMENT_PRE_EXEC_WAIT;
            break;
        case EProcedureType::OrderStatus:
            durationS = EWorkloadConstants::TPCC_ORDERSTATUS_PRE_EXEC_WAIT;
            break;
        case EProcedureType::Delivery:
            durationS = EWorkloadConstants::TPCC_DELIVERY_PRE_EXEC_WAIT;
            break;
        case EProcedureType::StockLevel:
            durationS = EWorkloadConstants::TPCC_STOCKLEVEL_PRE_EXEC_WAIT;
            break;
        default:
            FailWithMessage("Non-existent procedure", EStatus::CLIENT_INTERNAL_ERROR);
            return TInstant::Max();
    }
    return Now() + TDuration::MilliSeconds(durationS);
}

TInstant TTerminal::GetPostExecutionWait() {
    ui32 durationS;
    switch (Procedure->GetType()) {
        case EProcedureType::NewOrder:
            durationS = EWorkloadConstants::TPCC_NEWORDER_POST_EXEC_WAIT;
            break;
        case EProcedureType::Payment:
            durationS = EWorkloadConstants::TPCC_PAYMENT_POST_EXEC_WAIT;
            break;
        case EProcedureType::OrderStatus:
            durationS = EWorkloadConstants::TPCC_ORDERSTATUS_POST_EXEC_WAIT;
            break;
        case EProcedureType::Delivery:
            durationS = EWorkloadConstants::TPCC_DELIVERY_POST_EXEC_WAIT;
            break;
        case EProcedureType::StockLevel:
            durationS = EWorkloadConstants::TPCC_STOCKLEVEL_POST_EXEC_WAIT;
            break;
        default:
            FailWithMessage("Non-existent procedure", EStatus::CLIENT_INTERNAL_ERROR);
            return TInstant::Max();
    }
    return Now() + TDuration::MilliSeconds(durationS);
}

void TTerminal::NextProcedure() {
    Resource.reset();
    Procedure.reset();
    RetryCount = 0;
    ProcedureStarted = false;

    ui32 weight = TPCC_NEWORDER_WEIGHT + TPCC_PAYMENT_WEIGHT + TPCC_ORDERSTATUS_WEIGHT
                  + TPCC_DELIVERY_WEIGHT + TPCC_STOCKLEVEL_WEIGHT;
    ui32 random = UniformRandom32(1, weight, Rng);
    
    if (random <= EWorkloadConstants::TPCC_NEWORDER_WEIGHT && Params.OnlyOneType == -1 || Params.OnlyOneType == EProcedureType::NewOrder) {
        Procedure.reset(new TNewOrderProcedure(TableClient, *this, Log, Params.DebugMode, Rng()));
        return;
    } else {
        random -= EWorkloadConstants::TPCC_NEWORDER_WEIGHT;
    }

    if (random <= EWorkloadConstants::TPCC_PAYMENT_WEIGHT && Params.OnlyOneType == -1 || Params.OnlyOneType == EProcedureType::Payment) {
        Procedure.reset(new TPaymentProcedure(TableClient, *this, Log, Params.DebugMode, Rng()));
        return;
    } else {
        random -= EWorkloadConstants::TPCC_PAYMENT_WEIGHT;
    }

    if (random <= EWorkloadConstants::TPCC_ORDERSTATUS_WEIGHT && Params.OnlyOneType == -1 || Params.OnlyOneType == EProcedureType::OrderStatus) {
        Procedure.reset(new TOrderStatusProcedure(TableClient, *this, Log, Params.DebugMode, Rng()));
        return;
    } else {
        random -= EWorkloadConstants::TPCC_ORDERSTATUS_WEIGHT;
    }

    if (random <= EWorkloadConstants::TPCC_DELIVERY_WEIGHT && Params.OnlyOneType == -1 || Params.OnlyOneType == EProcedureType::Delivery) {
        Procedure.reset(new TDeliveryProcedure(TableClient, *this, Log, Params.DebugMode, Rng()));
        return;
    } else {
        random -= EWorkloadConstants::TPCC_DELIVERY_WEIGHT;
    }

    if (random <= EWorkloadConstants::TPCC_STOCKLEVEL_WEIGHT && Params.OnlyOneType == -1 || Params.OnlyOneType == EProcedureType::StockLevel) {
        Procedure.reset(new TStockLevelProcedure(TableClient, *this, Log, Params.DebugMode, Rng()));
        return;
    } else {
        random -= EWorkloadConstants::TPCC_STOCKLEVEL_WEIGHT;
    }
}


}
}
