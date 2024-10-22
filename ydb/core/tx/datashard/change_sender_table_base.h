#pragma once

#include "change_exchange_helpers.h"

#include <ydb/core/change_exchange/util.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tx/scheme_cache/helpers.h>

namespace NKikimr::NDataShard {

using namespace NTable;

#define DEFINE_STATE_INTRO \
    public: \
        struct TStateTag {}; \
    private: \
        const TDerived* AsDerived() const { \
            return static_cast<const TDerived*>(this); \
        } \
        TDerived* AsDerived() { \
            return static_cast<TDerived*>(this); \
        } \
        TStringBuf GetLogPrefix() const { \
            return AsDerived()->GetLogPrefix(); \
        }

#define USE_STATE(STATE) \
    friend class T ## STATE ## State; \
    STATEFN(State ## STATE) { \
        return T ## STATE ## State::State(ev); \
    } \
    bool Is ## STATE ## State() const { \
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::State ## STATE); \
    }

template <typename TDerived>
class TResolveUserTableState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveUserTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(AsDerived()->UserTableId, TNavigate::OpTable));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveUserTable);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, ResolveUserTable);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleUserTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->UserTableId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!AsDerived()->CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        for (const auto& [tag, column] : entry.Columns) {
            Y_DEBUG_ABORT_UNLESS(!AsDerived()->MainColumnToTag.contains(column.Name));
            AsDerived()->MainColumnToTag.emplace(column.Name, tag);
        }

        AsDerived()->NextState(TStateTag{});
    }
};

template <typename TDerived>
class TResolveTargetTableState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveTargetTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(AsDerived()->TargetTablePathId, TNavigate::OpTable));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveTargetTable);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, OnRetry);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void OnRetry() {
        AsDerived()->OnRetry(TStateTag{});
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleTargetTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->TargetTablePathId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!AsDerived()->CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        if (AsDerived()->TargetTableVersion && AsDerived()->TargetTableVersion == entry.Self->Info.GetVersion().GetGeneralVersion()) {
            AsDerived()->CreateSenders();
            return AsDerived()->Serve();
        }

        AsDerived()->TagMap.clear();
        TVector<NScheme::TTypeInfo> keyColumnTypes;

        for (const auto& [tag, column] : entry.Columns) {
            auto it = AsDerived()->MainColumnToTag.find(column.Name);
            Y_ABORT_UNLESS(it != AsDerived()->MainColumnToTag.end());

            Y_DEBUG_ABORT_UNLESS(!AsDerived()->TagMap.contains(it->second));
            AsDerived()->TagMap.emplace(it->second, tag);

            if (column.KeyOrder < 0) {
                continue;
            }

            if (keyColumnTypes.size() <= static_cast<ui32>(column.KeyOrder)) {
                keyColumnTypes.resize(column.KeyOrder + 1);
            }

            keyColumnTypes[column.KeyOrder] = column.PType;
        }

        AsDerived()->KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            AsDerived()->GetFullRange(keyColumnTypes.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>()
        );

        AsDerived()->SetPartitionResolver(CreateDefaultPartitionResolver(*AsDerived()->KeyDesc.Get()));
        AsDerived()->NextState(TStateTag{});
    }
};

template <typename TDerived>
class TResolveKeysState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(AsDerived()->KeyDesc));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveKeys);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, OnRetry);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void OnRetry() {
        AsDerived()->OnRetry(TStateTag{});
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleKeys TEvTxProxySchemeCache::TEvResolveKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->TargetTablePathId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            LOG_W("Empty partitions list"
                << ": entry# " << entry.ToString(*AppData()->TypeRegistry));
            return AsDerived()->Retry();
        }

        AsDerived()->TargetTableVersion = entry.GeneralVersion;
        AsDerived()->KeyDesc = std::move(entry.KeyDescription);
        AsDerived()->CreateSenders(NChangeExchange::MakePartitionIds(AsDerived()->KeyDesc->GetPartitions()));

        AsDerived()->NextState(TStateTag{});
    }
};

template <typename TDerived>
struct TSchemeChecksMixin
    : virtual private NSchemeCache::TSchemeCacheHelpers
{
    const TDerived* AsDerived() const {
        return static_cast<const TDerived*>(this);
    }

    TDerived* AsDerived() {
        return static_cast<TDerived*>(this);
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc(AsDerived()->CurrentStateName(), subject, std::forward<Args>(args)..., std::bind(failFunc, AsDerived(), std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TDerived::LogCritAndRetry, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TDerived::LogCritAndRetry, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TDerived::LogCritAndRetry, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TDerived::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TDerived::LogWarnAndRetry, entry, expected);
    }

};

enum class ETableChangeSenderType {
    AsyncIndex,
    IncrementalRestore,
};

IActor* CreateTableChangeSenderShard(
    const TActorId& parent,
    const TDataShardId& dataShard,
    ui64 shardId,
    const TPathId& targetTablePathId,
    const TMap<TTag, TTag>& tagMap,
    ETableChangeSenderType type);

} // namespace NKikimr::NDataShard
