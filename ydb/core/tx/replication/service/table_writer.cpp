#include "table_writer.h"
#include "worker.h"

#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NService {

class TLocalTableWriter
    : public TActor<TLocalTableWriter>
    , public NChangeExchange::TBaseChangeSender
    , public NChangeExchange::IChangeSenderResolver
    , private NSchemeCache::TSchemeCacheHelpers
{
    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    void LogCritAndLeave(const TString& error) {
        Y_UNUSED(error);
        Leave();
    }

    void LogWarnAndRetry(const TString& error) {
        Y_UNUSED(error);
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc("writer", subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TThis::LogCritAndLeave, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TThis::LogCritAndLeave, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TThis::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    static TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.ShardId);
        }

        return result;
    }

    TActorId GetChangeServer() const override {
        return SelfId();
    }

    void Resolve() override {
        ResolveTable();
    }

    bool IsResolving() const override {
        return Resolving;
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->GetPartitions();
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        ResolveTable();
    }

    void ResolveTable() {
        Resolving = true;

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(PathId, TNavigate::OpTable));
        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& [_, column] : entry.Columns) {
            if (column.KeyOrder < 0) {
                continue;
            }

            if (keyColumnTypes.size() <= static_cast<ui32>(column.KeyOrder)) {
                keyColumnTypes.resize(column.KeyOrder + 1);
            }

            keyColumnTypes[column.KeyOrder] = column.PType;
        }

        KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            GetFullRange(keyColumnTypes.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>()
        );

        ResolveKeys();
    }

    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(KeyDesc));
        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            return LogWarnAndRetry("Empty partitions");
        }

        const bool versionChanged = !TableVersion || TableVersion != entry.GeneralVersion;
        TableVersion = entry.GeneralVersion;

        KeyDesc = std::move(entry.KeyDescription);
        CreateSenders(MakePartitionIds(KeyDesc->GetPartitions()), versionChanged);

        Send(Worker, new TEvWorker::TEvHandshake());
        Resolving = false;
    }

    IActor* CreateSender(ui64 partitionId) override {
        Y_UNUSED(partitionId);
        return nullptr;
    }

    ui64 GetPartitionId(NChangeExchange::IChangeRecord::TPtr record) const override {
        Y_UNUSED(record);
        return 0;
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        Worker = ev->Sender;
        // TODO: enqueue records
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev) {
        Y_UNUSED(ev);
        // TODO: send records
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        ProcessRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        ForgetRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        OnGone(ev->Get()->PartitionId);
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void Leave() {
        Send(Worker, new TEvents::TEvGone());
        PassAway();
    }

    void PassAway() override {
        KillSenders();
        TActor::PassAway();
    }

public:
    explicit TLocalTableWriter(const TPathId& tablePathId)
        : TActor(&TThis::StateWork)
        , TBaseChangeSender(this, this, tablePathId)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRequestRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvForgetRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvReady, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvGone, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, ResolveTable);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId Worker;
    ui64 TableVersion = 0;
    THolder<TKeyDesc> KeyDesc;
    bool Resolving = false;

}; // TLocalTableWriter

IActor* CreateLocalTableWriter(const TPathId& tablePathId) {
    return new TLocalTableWriter(tablePathId);
}

}
