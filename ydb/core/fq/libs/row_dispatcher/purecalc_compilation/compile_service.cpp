#include "compile_service.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher {

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCompileFinished = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvCompileFinished : public NActors::TEventLocal<TEvCompileFinished, EvCompileFinished> {
        TEvCompileFinished(NActors::TActorId requestActor, ui64 requestId)
            : RequestActor(requestActor)
            , RequestId(requestId)
        {}

        const NActors::TActorId RequestActor;
        const ui64 RequestId;
    };
};

class TPurecalcCompileActor : public NActors::TActorBootstrapped<TPurecalcCompileActor> {
public:
    TPurecalcCompileActor(NActors::TActorId owner, NYql::NPureCalc::IProgramFactoryPtr factory, TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr request)
        : Owner(owner)
        , Factory(factory)
        , LogPrefix(TStringBuilder() << "TPurecalcCompileActor " << request->Sender << " [id " << request->Cookie << "]: ")
        , Request(std::move(request))
    {}

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_COMPILE_ACTOR";

    void Bootstrap() {
        Y_DEFER {
            Finish();
        };

        LOG_ROW_DISPATCHER_TRACE("Started compile request");
        IProgramHolder::TPtr programHolder = std::move(Request->Get()->ProgramHolder);

        TStatus status = TStatus::Success();
        try {
            programHolder->CreateProgram(Factory);
        } catch (const NYql::NPureCalc::TCompileError& error) {
            status = TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Compile issues: " << error.GetIssues())
                .AddIssue(TStringBuilder() << "Final yql: " << error.GetYql())
                .AddParentIssue(TStringBuilder() << "Failed to compile purecalc program");
        } catch (...) {
            status = TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to compile purecalc program, got unexpected exception: " << CurrentExceptionMessage());
        }

        if (status.IsFail()) {
            LOG_ROW_DISPATCHER_ERROR("Compilation failed for request");
            Send(Request->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(status.GetStatus(), status.GetErrorDescription()), 0, Request->Cookie);
        } else {
            LOG_ROW_DISPATCHER_TRACE("Compilation completed for request");
            Send(Request->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(programHolder)), 0, Request->Cookie);
        }
    }

private:
    void Finish() {
        Send(Owner, new TEvPrivate::TEvCompileFinished(Request->Sender, Request->Cookie));
        PassAway();
    }

private:
    const NActors::TActorId Owner;
    const NYql::NPureCalc::IProgramFactoryPtr Factory;
    const TString LogPrefix;

    TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr Request;
};

class TPurecalcCompileService : public NActors::TActor<TPurecalcCompileService> {
    using TBase = NActors::TActor<TPurecalcCompileService>;

    struct TCounters {
        const NMonitoring::TDynamicCounterPtr Counters;

        NMonitoring::TDynamicCounters::TCounterPtr ActiveCompileActors;
        NMonitoring::TDynamicCounters::TCounterPtr CompileQueueSize;

        explicit TCounters(NMonitoring::TDynamicCounterPtr counters)
            : Counters(counters)
        {
            Register();
        }

    private:
        void Register() {
            ActiveCompileActors = Counters->GetCounter("ActiveCompileActors", false);
            CompileQueueSize = Counters->GetCounter("CompileQueueSize", false);
        }
    };

public:
    TPurecalcCompileService(const NConfig::TCompileServiceConfig& config, NMonitoring::TDynamicCounterPtr counters)
        : TBase(&TPurecalcCompileService::StateFunc)
        , Config(config)
        , InFlightLimit(Config.GetParallelCompilationLimit() ? Config.GetParallelCompilationLimit() : 1)
        , LogPrefix("TPurecalcCompileService: ")
        , Counters(counters)
    {}

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_COMPILE_SERVICE";

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
        hFunc(TEvRowDispatcher::TEvPurecalcCompileAbort, Handle)
        hFunc(TEvPrivate::TEvCompileFinished, Handle);
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        const auto requestActor = ev->Sender;
        const ui64 requestId = ev->Cookie;
        LOG_ROW_DISPATCHER_TRACE("Add to compile queue request with id " << requestId << " from " << requestActor);

        // Remove old compile request
        RemoveRequest(requestActor, requestId);

        // Add new request
        RequestsQueue.emplace_back(std::move(ev));
        Y_ENSURE(RequestsIndex.emplace(std::make_pair(requestActor, requestId), --RequestsQueue.end()).second);
        Counters.CompileQueueSize->Inc();

        StartCompilation();
    }

    void Handle(TEvRowDispatcher::TEvPurecalcCompileAbort::TPtr& ev) {
        LOG_ROW_DISPATCHER_TRACE("Abort compile request with id " << ev->Cookie << " from " << ev->Sender);

        RemoveRequest(ev->Sender, ev->Cookie);
    }

    void Handle(TEvPrivate::TEvCompileFinished::TPtr& ev) {
        LOG_ROW_DISPATCHER_TRACE("Compile finished for request with id " << ev->Get()->RequestId << " from " << ev->Get()->RequestActor);

        InFlightCompilations.erase(ev->Sender);
        Counters.ActiveCompileActors->Dec();

        StartCompilation();
    }

private:
    void RemoveRequest(NActors::TActorId requestActor, ui64 requestId) {
        const auto it = RequestsIndex.find(std::make_pair(requestActor, requestId));
        if (it == RequestsIndex.end()) {
            return;
        }

        RequestsQueue.erase(it->second);
        RequestsIndex.erase(it);
        Counters.CompileQueueSize->Dec();
    }

    void StartCompilation() {
        while (!RequestsQueue.empty() && InFlightCompilations.size() < InFlightLimit) {
            auto request = std::move(RequestsQueue.front());
            RemoveRequest(request->Sender, request->Cookie);

            const auto factory = GetOrCreateFactory(request->Get()->Settings);
            const auto compileActor = Register(new TPurecalcCompileActor(SelfId(), factory, std::move(request)));
            Y_ENSURE(InFlightCompilations.emplace(compileActor).second);
            Counters.ActiveCompileActors->Inc();
        }
    }

    NYql::NPureCalc::IProgramFactoryPtr GetOrCreateFactory(const TPurecalcCompileSettings& settings) {
        const auto it = ProgramFactories.find(settings);
        if (it != ProgramFactories.end()) {
            return it->second;
        }
        auto options = NYql::NPureCalc::TProgramFactoryOptions();
        options.SetLLVMSettings(settings.EnabledLLVM ? "ON" : "OFF");
        options.UseAntlr4 = false;
        return ProgramFactories.emplace(settings, NYql::NPureCalc::MakeProgramFactory(options)).first->second;
    }

private:
    const NConfig::TCompileServiceConfig Config;
    const ui64 InFlightLimit;
    const TString LogPrefix;

    std::list<TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr> RequestsQueue;
    THashMap<std::pair<NActors::TActorId, ui64>, std::list<TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr>::iterator> RequestsIndex;
    std::unordered_set<NActors::TActorId> InFlightCompilations;

    std::map<TPurecalcCompileSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;

    const TCounters Counters;
};

}  // anonymous namespace

NActors::IActor* CreatePurecalcCompileService(const NConfig::TCompileServiceConfig& config, NMonitoring::TDynamicCounterPtr counters) {
    return new TPurecalcCompileService(config, counters);
}

}  // namespace NFq::NRowDispatcher
