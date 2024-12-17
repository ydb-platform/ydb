#pragma once

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <util/str_stl.h>
#include <util/system/mutex.h>
#include <util/generic/hash.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class IAbortable
    : public TThrRefBase
{
public:
    virtual void Abort() = 0;
    virtual TString GetType() const = 0;
};

using IAbortablePtr = ::TIntrusivePtr<IAbortable>;

////////////////////////////////////////////////////////////////////////////////

class TTransactionAbortable
    : public IAbortable
{
public:
    TTransactionAbortable(const TClientContext& context, const TTransactionId& transactionId);
    void Abort() override;
    TString GetType() const override;

private:
    TClientContext Context_;
    TTransactionId TransactionId_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationAbortable
    : public IAbortable
{
public:
    TOperationAbortable(
        IRawClientPtr rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        const TOperationId& operationId);
    void Abort() override;
    TString GetType() const override;

private:
    const IRawClientPtr RawClient_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TOperationId OperationId_;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortableRegistry
    : public TThrRefBase
{
public:
    TAbortableRegistry() = default;
    static ::TIntrusivePtr<TAbortableRegistry> Get();

    void AbortAllAndBlockForever();
    void Add(const TGUID& id, IAbortablePtr abortable);
    void Remove(const TGUID& id);

private:
    THashMap<TGUID, IAbortablePtr> ActiveAbortables_;
    TMutex Lock_;
    bool Running_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
