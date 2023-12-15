#pragma once
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {
namespace NDataShardReadTableTest {

    class TReadTableDriver : public TActorBootstrapped<TReadTableDriver> {
    public:
        enum EEv {
            EvBegin = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvReady,
            EvNext,
            EvResult,
        };

        struct TEvReady : public TEventLocal<TEvReady, EvReady> {
            // empty
        };

        struct TEvNext : public TEventLocal<TEvNext, EvNext> {
            // empty
        };

        struct TEvResult : public TEventLocal<TEvResult, EvResult> {
            TString Result;
            bool Finished;
            bool IsError;

            TEvResult(TString result, bool finished, bool isError)
                : Result(std::move(result))
                , Finished(finished)
                , IsError(isError)
            { }
        };

    public:
        TReadTableDriver(const TActorId edge, const NTxProxy::TReadTableSettings& settings)
            : Edge(edge)
            , Settings(settings)
        { }

        void Bootstrap(const TActorContext& ctx) {
            auto settings = Settings;
            settings.Owner = ctx.SelfID;
            Worker = ctx.RegisterWithSameMailbox(NTxProxy::CreateReadTableSnapshotWorker(settings));
            Become(&TThis::StateWork);
        }

        void NotifyReady(const TActorContext& ctx) {
            if (!Ready) {
                Ready = true;
                ctx.Send(Edge, new TEvReady);
            }
        }

        STRICT_STFUNC(StateWork,
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle)
            HFunc(TEvTxProcessing::TEvStreamQuotaRequest, Handle)
            HFunc(TEvTxProcessing::TEvStreamQuotaRelease, Handle)
            HFunc(TEvNext, Handle)
        )

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            const auto* msg = ev->Get();

            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
            switch (status) {
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResponseData: {
                    const auto rsData = msg->Record.GetSerializedReadTableResponse();
                    Ydb::ResultSet rsParsed;
                    Y_ABORT_UNLESS(rsParsed.ParseFromString(rsData));
                    NYdb::TResultSet rs(rsParsed);
                    auto& columns = rs.GetColumnsMeta();
                    NYdb::TResultSetParser parser(rs);
                    TStringBuilder result;
                    while (parser.TryNextRow()) {
                        for (size_t idx = 0; idx < columns.size(); ++idx) {
                            if (idx > 0) {
                                result << ", ";
                            }
                            result << columns[idx].Name << " = ";
                            PrintValue(result, parser.ColumnParser(idx));
                        }
                        result << Endl;
                    }
                    NotifyReady(ctx);
                    ctx.Send(Edge, new TEvResult(result, false, false));
                    break;
                }
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete: {
                    NotifyReady(ctx);
                    ctx.Send(Edge, new TEvResult({ }, true, false));
                    return Die(ctx);
                }
                default: {
                    NotifyReady(ctx);
                    ctx.Send(Edge, new TEvResult(TStringBuilder() << "ERROR: " << status << Endl, true, true));
                    return Die(ctx);
                }
            }
        }

        void Handle(TEvTxProcessing::TEvStreamQuotaRequest::TPtr& ev, const TActorContext& ctx) {
            const auto* msg = ev->Get();

            auto& req = QuotaRequests.emplace_back();
            req.Sender = ev->Sender;
            req.Cookie = ev->Cookie;
            req.TxId = msg->Record.GetTxId();

            NotifyReady(ctx);
            SendQuotas(ctx);
        }

        void Handle(TEvTxProcessing::TEvStreamQuotaRelease::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
        }

        void Handle(TEvNext::TPtr&, const TActorContext& ctx) {
            ++QuotaAvailable;
            SendQuotas(ctx);
        }

        void SendQuotas(const TActorContext& ctx) {
            while (QuotaRequests && QuotaAvailable > 0) {
                auto& req = QuotaRequests.front();

                auto response = MakeHolder<TEvTxProcessing::TEvStreamQuotaResponse>();
                response->Record.SetTxId(req.TxId);
                response->Record.SetMessageSizeLimit(1);
                response->Record.SetReservedMessages(1);

                ctx.Send(req.Sender, response.Release(), 0, req.Cookie);

                QuotaRequests.pop_front();
                --QuotaAvailable;
            }
        }

        static void PrintValue(TStringBuilder& out, NYdb::TValueParser& parser) {
            switch (parser.GetKind()) {
            case NYdb::TTypeParser::ETypeKind::Optional:
                parser.OpenOptional();
                if (parser.IsNull()) {
                    out << "NULL";
                } else {
                    PrintValue(out, parser);
                }
                parser.CloseOptional();
                break;

            case NYdb::TTypeParser::ETypeKind::Primitive:
                PrintPrimitive(out, parser);
                break;

            default:
                Y_ABORT("Unhandled");
            }
        }

        static void PrintPrimitive(TStringBuilder& out, const NYdb::TValueParser& parser) {
            switch (parser.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Uint64:
                out << parser.GetUint64();
                break;
            case NYdb::EPrimitiveType::Uint32:
                out << parser.GetUint32();
                break;
            case NYdb::EPrimitiveType::Utf8:
                out << parser.GetUtf8();
                break;
            case NYdb::EPrimitiveType::Timestamp:
                out << parser.GetTimestamp();
                break;

            default:
                Y_ABORT("Unhandled");
            }
        }

    private:
        struct TPendingRequest {
            TActorId Sender;
            ui64 Cookie;
            ui64 TxId;
        };

    private:
        const TActorId Edge;
        const NTxProxy::TReadTableSettings Settings;
        TActorId Worker;
        bool Ready = false;

        TDeque<TPendingRequest> QuotaRequests;
        size_t QuotaAvailable = 0;
    };

    struct TReadTableState {
        TTestActorRuntime& Runtime;
        TActorId Edge;
        TActorId Driver;
        TString LastResult;
        TStringBuilder Result;
        bool Finished = false;
        bool IsError = false;

        TReadTableState(Tests::TServer::TPtr server, const NTxProxy::TReadTableSettings& settings)
            : Runtime(*server->GetRuntime())
            , Edge(Runtime.AllocateEdgeActor())
            , Driver(Runtime.Register(new TReadTableDriver(Edge, settings)))
        {
            Runtime.EnableScheduleForActor(Driver);
            auto ev = Runtime.GrabEdgeEventRethrow<TReadTableDriver::TEvReady>(Edge);
            Y_UNUSED(ev);
        }

        bool Next() {
            if (!Finished) {
                Runtime.Send(new IEventHandle(Driver, TActorId(), new TReadTableDriver::TEvNext()), 0, true);
                auto ev = Runtime.GrabEdgeEventRethrow<TReadTableDriver::TEvResult>(Edge);
                LastResult = ev->Get()->Result;
                Finished = ev->Get()->Finished;
                IsError = ev->Get()->IsError;
                Result << LastResult;
            }

            return !Finished;
        }

        TString All() {
            while (Next()) {
                // nothing
            }

            return Result;
        }
    };

    inline NTxProxy::TReadTableSettings MakeReadTableSettings(const TString& path, bool ordered = false, ui64 maxRows = Max<ui64>()) {
        NTxProxy::TReadTableSettings settings;
        settings.TablePath = path;
        settings.Ordered = ordered;
        settings.MaxRows = maxRows;
        return settings;
    }

} // namespace NDataShardReadTableTest
} // namespace NKikimr
