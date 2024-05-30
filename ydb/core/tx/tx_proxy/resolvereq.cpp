#include "resolvereq.h"

#include <ydb/core/engine/mkql_proto.h>

#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NTxProxy {

namespace {

    enum class EParseRangeKeyExp {
        NONE,
        TO_NULL
    };

    bool ParseRangeKey(
            const NKikimrMiniKQL::TParams& proto,
            TConstArrayRef<NScheme::TTypeInfo> keyTypes,
            TSerializedCellVec& buf,
            EParseRangeKeyExp exp,
            TVector<TString>& unresolvedKeys)
    {
        TVector<TCell> key;
        TVector<TString> memoryOwner;
        if (proto.HasValue()) {
            if (!proto.HasType()) {
                unresolvedKeys.push_back("No type was specified in the range key tuple");
                return false;
            }

            auto& value = proto.GetValue();
            auto& type = proto.GetType();
            TString errStr;
            bool res = NMiniKQL::CellsFromTuple(&type, value, keyTypes, {}, true, key, errStr, memoryOwner);
            if (!res) {
                unresolvedKeys.push_back("Failed to parse range key tuple: " + errStr);
                return false;
            }
        }

        switch (exp) {
            case EParseRangeKeyExp::TO_NULL:
                key.resize(keyTypes.size());
                break;
            case EParseRangeKeyExp::NONE:
                break;
        }

        buf = TSerializedCellVec(key);
        return true;
    }

} // namespace

    bool TEvResolveTablesResponse::CheckDomainLocality() const {
        NSchemeCache::TDomainInfo::TPtr domainInfo;

        for (const auto& entry : Tables) {
            if (entry.KeyDescription->TableId.IsSystemView() ||
                TSysTables::IsSystemTable(entry.KeyDescription->TableId))
            {
                continue;
            }

            Y_ABORT_UNLESS(entry.DomainInfo);

            if (!domainInfo) {
                domainInfo = entry.DomainInfo;
                continue;
            }

            if (domainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                return false;
            }
        }

        return true;
    }

    NSchemeCache::TDomainInfo::TPtr TEvResolveTablesResponse::FindDomainInfo() const {
        for (const auto& entry : Tables) {
            if (entry.DomainInfo) {
                return entry.DomainInfo;
            }
        }

        return nullptr;
    }

    class TResolveTablesActor : public TActorBootstrapped<TResolveTablesActor> {
    public:
        TResolveTablesActor(
                TActorId owner,
                ui64 txId,
                const TTxProxyServices& services,
                TVector<TResolveTableRequest> tables,
                const TString& databaseName)
            : Owner(owner)
            , TxId(txId)
            , Services(services)
            , DatabaseName(databaseName)
        {
            Tables.reserve(tables.size());
            for (auto& req : tables) {
                auto& res = Tables.emplace_back();
                res.TablePath = std::move(req.TablePath);
                res.KeyRange = std::move(req.KeyRange);
            }
        }

        void Bootstrap(const TActorContext& ctx) {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            request->DatabaseName = DatabaseName;

            for (auto& table : Tables) {
                auto& entry = request->ResultSet.emplace_back();
                entry.Path = SplitPath(table.TablePath);
                entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
                entry.ShowPrivatePath = true;
            }

            ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
            Become(&TThis::StateWaitResolve);
        }

    private:
        STFUNC(StateWaitResolve) {
            TRACE_EVENT(NKikimrServices::TX_PROXY);
            switch (ev->GetTypeRewrite()) {
                HFuncTraced(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolve);
                HFuncTraced(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
                CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            }
        }

        void HandleResolve(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            if (Cancelled) {
                return Die(ctx);
            }

            NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

            LOG_LOG_S_SAMPLED_BY(ctx, (resp->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
                    NKikimrServices::TX_PROXY, TxId,
                    "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                    << " HANDLE EvNavigateKeySetResult TResolveTablesActor marker# P1 ErrorCount# "
                    << resp->ErrorCount);

            if (resp->ErrorCount > 0) {
                TStringBuilder builder;
                builder << "unresolved tables: ";
                bool first = true;
                for (size_t index = 0; index < Tables.size(); ++index) {
                    auto& table = Tables[index];
                    auto& entry = resp->ResultSet[index];
                    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                        if (first) {
                            first = false;
                        } else {
                            builder << ", ";
                        }
                        builder << table.TablePath;
                    }
                }
                const TString errorExplanation = builder;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errorExplanation));

                UnresolvedKeys.push_back(errorExplanation);
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
            }

            auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
            request->DatabaseName = DatabaseName;

            for (size_t index = 0; index < Tables.size(); ++index) {
                auto& table = Tables[index];
                auto& entry = resp->ResultSet[index];

                table.TableId = entry.TableId;
                table.IsColumnTable = (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

                TVector<NScheme::TTypeInfo> keyColumnTypes(entry.Columns.size());
                TVector<TKeyDesc::TColumnOp> columns(entry.Columns.size());
                size_t keySize = 0;
                size_t no = 0;

                for (auto& kv : entry.Columns) {
                    auto& col = kv.second;

                    if (col.KeyOrder != -1) {
                        keyColumnTypes[col.KeyOrder] = col.PType;
                        ++keySize;
                    }

                    columns[no].Column = col.Id;
                    columns[no].Operation = TKeyDesc::EColumnOperation::Read;
                    columns[no].ExpectedType = col.PType;
                    ++no;
                }

                // Parse range.
                TConstArrayRef<NScheme::TTypeInfo> keyTypes(keyColumnTypes.data(), keySize);

                const bool fromInclusive = table.KeyRange.GetFromInclusive();
                const bool toInclusive = table.KeyRange.GetToInclusive();
                const EParseRangeKeyExp fromExpand = (
                    table.KeyRange.HasFrom()
                        ? (fromInclusive ? EParseRangeKeyExp::TO_NULL : EParseRangeKeyExp::NONE)
                        : EParseRangeKeyExp::TO_NULL);
                const EParseRangeKeyExp toExpand = (
                    table.KeyRange.HasTo()
                        ? (toInclusive ? EParseRangeKeyExp::NONE : EParseRangeKeyExp::TO_NULL)
                        : EParseRangeKeyExp::NONE);

                if (!ParseRangeKey(table.KeyRange.GetFrom(), keyTypes,
                                table.FromValues, fromExpand, UnresolvedKeys) ||
                    !ParseRangeKey(table.KeyRange.GetTo(), keyTypes,
                                table.ToValues, toExpand, UnresolvedKeys))
                {
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR, "could not parse key string"));
                    return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, ctx);
                }

                TTableRange range(
                        table.FromValues.GetCells(), fromInclusive,
                        table.ToValues.GetCells(), toInclusive);

                if (range.IsEmptyRange({keyTypes.begin(), keyTypes.end()})) {
                    const TString errorExplanation = "empty range requested";
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::EMPTY_OP_RANGE, errorExplanation));
                    UnresolvedKeys.push_back(errorExplanation);
                    return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, ctx);
                }

                table.KeyDescription = MakeHolder<TKeyDesc>(entry.TableId, range, TKeyDesc::ERowOperation::Read, keyTypes, columns);

                request->ResultSet.emplace_back(std::move(table.KeyDescription));
            }

            WallClockResolveStarted = Now();

            ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySet(request));

            // This actor must not die until ResolveKeySet completes
            ResolvingKeys = true;
        }

        void TryToInvalidateTable(TTableId tableId, const TActorContext& ctx) {
            const bool notYetInvalidated = InvalidatedTables.insert(tableId).second;
            if (notYetInvalidated) {
                ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(tableId, TActorId()));
            }
        }

        void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev, const TActorContext& ctx) {
            ResolvingKeys = false;

            if (Cancelled) {
                return Die(ctx);
            }

            NSchemeCache::TSchemeCacheRequest* request = ev->Get()->Request.Get();

            LOG_LOG_S_SAMPLED_BY(ctx, (request->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR),
                    NKikimrServices::TX_PROXY, TxId,
                    "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                    << " HANDLE EvResolveKeySetResult TResolveTablesActor marker# P2 ErrorCount# " << request->ErrorCount);

            WallClockResolved = Now();

            if (request->ErrorCount > 0) {
                bool gotHardResolveError = false;
                for (const auto& x : request->ResultSet) {
                    if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                        TryToInvalidateTable(x.KeyDescription->TableId, ctx);

                        TStringStream ss;
                        switch (x.Status) {
                            case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                                gotHardResolveError = true;
                                ss << "table not exists: " << x.KeyDescription->TableId;
                                break;
                            case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                                gotHardResolveError = true;
                                ss << "type check error: " << x.KeyDescription->TableId;
                                break;
                            default:
                                ss << "unresolved table: " << x.KeyDescription->TableId << ". Status: " << x.Status;
                                break;
                        }

                        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, ss.Str()));
                        UnresolvedKeys.push_back(ss.Str());
                    }
                }

                if (gotHardResolveError) {
                    return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
                } else {
                    return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, ctx);
                }
            }

            for (size_t index = 0; index < Tables.size(); ++index) {
                auto& table = Tables[index];
                auto& entry = request->ResultSet[index];
                table.KeyDescription = std::move(entry.KeyDescription);
                table.DomainInfo = entry.DomainInfo;
            }

            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved, NKikimrIssues::TStatusIds::SUCCESS, ctx);
        }

        void HandlePoison(const TActorContext& ctx) {
            Cancelled = true;

            if (ResolvingKeys) {
                // Die cannot be called while ResolveKeySet in progress
                return;
            }

            Die(ctx);
        }

        void ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, const TActorContext& ctx) {
            auto reply = MakeHolder<TEvResolveTablesResponse>(status, code);

            reply->WallClockResolveStarted = WallClockResolveStarted;
            reply->WallClockResolved = WallClockResolved;
            reply->Tables = std::move(Tables);
            reply->UnresolvedKeys = std::move(UnresolvedKeys);
            reply->Issues = IssueManager.GetIssues();

            ctx.Send(Owner, reply.Release());
            Die(ctx);
        }

    private:
        const TActorId Owner;
        const ui64 TxId;
        const TTxProxyServices& Services;
        const TString DatabaseName;

        TVector<TResolveTableResponse> Tables;

        TInstant WallClockResolveStarted;
        TInstant WallClockResolved;

        TVector<TString> UnresolvedKeys;
        NYql::TIssueManager IssueManager;
        TTablePathHashSet InvalidatedTables;

        bool ResolvingKeys = false;
        bool Cancelled = false;
    };

    IActor* CreateResolveTablesActor(
            TActorId owner,
            ui64 txId,
            const TTxProxyServices& services,
            TVector<TResolveTableRequest> tables,
            const TString& databaseName)
    {
        return new TResolveTablesActor(owner, txId, services, std::move(tables), databaseName);
    }

} // namespace NTxProxy
} // namespace NKikimr
