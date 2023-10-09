#include "all.h"
#include "kill_action.h"
#include "log_shuttle.h"
#include "preprocessor.h"
#include "sleep_action.h"
#include "stderr_writer.h"
#include "google/protobuf/repeated_field.h"

#include <util/generic/map.h>
#include <util/random/random.h>

#include <functional>

namespace NLWTrace {
#ifndef LWTRACE_DISABLE

// Define static strings for name of each parameter type
#define FOREACH_PARAMTYPE_MACRO(n, t, v)       \
    const char* TParamType<t>::NameString = n; \
    /**/
    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
    FOR_NIL_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO

#endif

    void TProbeRegistry::AddProbesList(TProbe** reg) {
        TGuard<TMutex> g(Mutex);
        if (reg == nullptr) {
            return;
        }
        for (TProbe** i = reg; *i != nullptr; i++) {
            AddProbeNoLock(new TStaticBox(*i));
        }
    }

    void TProbeRegistry::AddProbe(const TBoxPtr& box) {
        TGuard<TMutex> g(Mutex);
        AddProbeNoLock(box);
    }

    void TProbeRegistry::RemoveProbe(TProbe* probe) {
        TGuard<TMutex> g(Mutex);
        RemoveProbeNoLock(probe);
    }

    void TProbeRegistry::AddProbeNoLock(const TBoxPtr& box) {
        TProbe* probe = box->GetProbe();
        if (Probes.contains(probe)) {
            return; // silently skip probe double registration
        }
        TIds::key_type key(probe->Event.GetProvider(), probe->Event.Name);
        Y_ABORT_UNLESS(Ids.count(key) == 0, "duplicate provider:probe pair %s:%s", key.first.data(), key.second.data());
        Probes.emplace(probe, box);
        Ids.insert(key);
    }

    void TProbeRegistry::RemoveProbeNoLock(TProbe* probe) {
        auto iter = Probes.find(probe);
        if (iter != Probes.end()) {
            TIds::key_type key(probe->Event.GetProvider(), probe->Event.Name);
            Ids.erase(key);
            Probes.erase(iter);
        } else {
            // silently skip probe double unregistration
        }
    }

    TAtomic* GetVariablePtr(TSession::TTraceVariables& traceVariables, const TString& name) {
        TSession::TTraceVariables::iterator it = traceVariables.find(name);
        if (it == traceVariables.end()) {
            TAtomicBase zero = 0;
            traceVariables[name] = zero;
            return &traceVariables[name];
        }
        return &((*it).second);
    }

    typedef enum {
        OT_LITERAL = 0,
        OT_PARAMETER = 1,
        OT_VARIABLE = 2
    } EOperandType;

    template <class T, EOperandType>
    class TOperand;

    template <class T>
    class TOperand<T, OT_LITERAL> {
    private:
        T ImmediateValue;

    public:
        TOperand(TSession::TTraceVariables&, const TString&, const TString& value, size_t) {
            ImmediateValue = TParamConv<T>::FromString(value);
        }
        const T& Get(const TParams&) {
            return ImmediateValue;
        }
    };

    template <class T>
    class TOperand<T, OT_PARAMETER> {
    private:
        size_t Idx;

    public:
        TOperand(TSession::TTraceVariables&, const TString&, const TString&, size_t idx) {
            Idx = idx;
        }

        const T& Get(const TParams& params) {
            return params.Param[Idx].template Get<T>();
        }
    };

    template <class T>
    class TOperand<T, OT_VARIABLE> {
    private:
        TAtomic* Variable;

    public:
        TOperand(TSession::TTraceVariables& traceVariables, const TString& name, const TString&, size_t) {
            Variable = GetVariablePtr(traceVariables, name);
        }

        const T Get(const TParams&) {
            return (T)AtomicGet(*Variable);
        }

        void Set(const T& value) {
            AtomicSet(*Variable, value);
        }

        void Inc() {
            AtomicIncrement(*Variable);
        }

        void Dec() {
            AtomicDecrement(*Variable);
        }

        void Add(const TAtomicBase value) {
            AtomicAdd(*Variable, value);
        }

        void Sub(const TAtomicBase value) {
            AtomicSub(*Variable, value);
        }
    };

    template <>
    class TOperand<TCheck, OT_VARIABLE> {
    private:
        TAtomic* Variable;

    public:
        TOperand(TSession::TTraceVariables& traceVariables, const TString& name, const TString&, size_t) {
            Variable = GetVariablePtr(traceVariables, name);
        }

        const TCheck Get(const TParams&) {
            return TCheck(AtomicGet(*Variable));
        }

        void Set(const TCheck& value) {
            AtomicSet(*Variable, value.Value);
        }

        void Add(const TCheck& value) {
            AtomicAdd(*Variable, value.Value);
        }

        void Sub(const TCheck value) {
            AtomicSub(*Variable, value.Value);
        }

        void Inc() {
            AtomicIncrement(*Variable);
        }

        void Dec() {
            AtomicDecrement(*Variable);
        }
    };

    template <>
    class TOperand<TString, OT_VARIABLE> {
    private:
        TString Dummy;

    public:
        TOperand(TSession::TTraceVariables&, const TString&, const TString&, size_t) {
        }

        const TString Get(const TParams&) {
            return Dummy;
        }

        void Set(const TString&) {
        }
    };

    template <>
    class TOperand<TSymbol, OT_VARIABLE> {
    private:
        TSymbol Dummy;

    public:
        TOperand(TSession::TTraceVariables&, const TString&, const TString&, size_t) {
        }

        const TSymbol Get(const TParams&) {
            return Dummy;
        }

        void Set(const TSymbol&) {
        }
    };

    // IOperandGetter: hide concrete EOperandType, to save compilation time
    template <class T>
    struct IOperandGetter {
        virtual const T Get(const TParams& params) = 0;
        virtual ~IOperandGetter() {
        }
    };

    template <class T, EOperandType TParam>
    class TOperandGetter: public IOperandGetter<T> {
    private:
        TOperand<T, TParam> Op;

    public:
        TOperandGetter(const TOperand<T, TParam>& op)
            : Op(op)
        {
        }

        const T Get(const TParams& params) override {
            return Op.Get(params);
        }
    };

    template <class T>
    class TReceiver: public TOperand<T, OT_VARIABLE> {
    public:
        TReceiver(TSession::TTraceVariables& traceVariables, const TString& name)
            : TOperand<T, OT_VARIABLE>(traceVariables, name, nullptr, 0)
        {
        }
    };

    template <class TP, class TPredicate>
    static bool CmpFunc(TP a, TP b) {
        return TPredicate()(a, b);
    }

    template <class TP, class TFunc, EOperandType TLhs, EOperandType TRhs>
    class TOperatorExecutor: public IExecutor {
    private:
        bool InvertCompare;
        TOperand<TP, TLhs> Lhs;
        TOperand<TP, TRhs> Rhs;

        bool DoExecute(TOrbit&, const TParams& params) override {
            return TFunc()(Lhs.Get(params), Rhs.Get(params)) != InvertCompare;
        }

    public:
        TOperatorExecutor(const TOperand<TP, TLhs>& lhs, const TOperand<TP, TRhs>& rhs, bool invertCompare)
            : InvertCompare(invertCompare)
            , Lhs(lhs)
            , Rhs(rhs)
        {
        }
    };

    template <class TR, class TP>
    struct TAddEq {
        void operator()(TR& x, TP y) const {
            x.Add(y);
        }
    };
    template <class TR, class TP>
    struct TSubEq {
        void operator()(TR& x, TP y) const {
            x.Sub(y);
        }
    };
    template <class TR>
    struct TInc {
        void operator()(TR& x) const {
            x.Inc();
        }
    };
    template <class TR>
    struct TDec {
        void operator()(TR& x) const {
            x.Dec();
        }
    };

    template <class TP, class TFunc>
    class TUnaryInplaceStatementExecutor: public IExecutor {
    private:
        TFunc Func;
        TReceiver<TP> Receiver;

        bool DoExecute(TOrbit&, const TParams&) override {
            Func(Receiver);
            return true;
        }

    public:
        TUnaryInplaceStatementExecutor(TReceiver<TP>& receiver)
            : Receiver(receiver)
        {
        }
    };

    template <class TP, class TFunc, EOperandType TParam>
    class TBinaryInplaceStatementExecutor: public IExecutor {
    private:
        TFunc Func;
        TReceiver<TP> Receiver;
        TOperand<TP, TParam> Param;

        bool DoExecute(TOrbit&, const TParams& params) override {
            Func(Receiver, Param.Get(params));
            return true;
        }

    public:
        TBinaryInplaceStatementExecutor(TReceiver<TP>& receiver, const TOperand<TP, TParam>& param)
            : Receiver(receiver)
            , Param(param)
        {
        }
    };

    template <class TP, class TFunc, EOperandType TFirstParam>
    class TBinaryStatementExecutor: public IExecutor {
    private:
        TFunc Func;
        TReceiver<TP> Receiver;
        TOperand<TP, TFirstParam> FirstParam;

        bool DoExecute(TOrbit&, const TParams& params) override {
            Receiver.Set(Func(Receiver.Get(params), FirstParam.Get(params)));
            return true;
        }

    public:
        TBinaryStatementExecutor(TReceiver<TP>& receiver, const TOperand<TP, TFirstParam>& firstParam)
            : Receiver(receiver)
            , FirstParam(firstParam)
        {
        }
    };

    template <class TP, class TFunc>
    class TTernaryStatementExecutor: public IExecutor {
    private:
        TFunc Func;
        TReceiver<TP> Receiver;

        TAutoPtr<IOperandGetter<TP>> FirstParam;
        TAutoPtr<IOperandGetter<TP>> SecondParam;

        bool DoExecute(TOrbit&, const TParams& params) override {
            Receiver.Set(Func(FirstParam->Get(params), SecondParam->Get(params)));
            return true;
        }

    public:
        TTernaryStatementExecutor(const TReceiver<TP>& receiver,
                                  TAutoPtr<IOperandGetter<TP>> firstParam,
                                  TAutoPtr<IOperandGetter<TP>> secondParam)
            : Receiver(receiver)
            , FirstParam(firstParam)
            , SecondParam(secondParam)
        {
        }
    };

    template <class TLog>
    class TLogActionExecutor: public IExecutor {
    private:
        bool LogParams;
        bool LogTimestamp;
        intptr_t* MaxRecords;
        TAtomic Records;
        TProbe* Probe;
        TLog* Log;

        bool DoExecute(TOrbit&, const TParams& params) override {
            if (MaxRecords != nullptr) {
                while (true) {
                    intptr_t a = AtomicGet(Records);
                    if (a >= *MaxRecords) {
                        return true;
                    }
                    if (AtomicCas(&Records, a + 1, a)) {
                        Write(params);
                        return true;
                    }
                }
            } else {
                Write(params);
                return true;
            }
        }

        void Write(const TParams& params) {
            typename TLog::TAccessor la(*Log);
            if (typename TLog::TItem* item = la.Add()) {
                item->Probe = Probe;
                if (LogParams) {
                    if ((item->SavedParamsCount = Probe->Event.Signature.ParamCount) > 0) {
                        Probe->Event.Signature.CloneParams(item->Params, params);
                    }
                } else {
                    item->SavedParamsCount = 0;
                }
                if (LogTimestamp) {
                    item->Timestamp = TInstant::Now();
                }
                item->TimestampCycles = GetCycleCount();
            }
        }

    public:
        TLogActionExecutor(TProbe* probe, const TLogAction& action, TLog* log)
            : LogParams(!action.GetDoNotLogParams())
            , LogTimestamp(action.GetLogTimestamp())
            , MaxRecords(action.GetMaxRecords() ? new intptr_t(action.GetMaxRecords()) : nullptr)
            , Records(0)
            , Probe(probe)
            , Log(log)
        {
        }

        ~TLogActionExecutor() override {
            delete MaxRecords;
        }
    };

    class TSamplingExecutor: public IExecutor {
    private:
        double SampleRate;

    public:
        explicit TSamplingExecutor(double sampleRate)
            : SampleRate(sampleRate)
        {}

        bool DoExecute(TOrbit&, const TParams&) override {
            return RandomNumber<double>() < SampleRate;
        }
    };

    typedef struct {
        EOperandType Type;
        size_t ParamIdx;
    } TArgumentDescription;

    using TArgumentList = TVector<TArgumentDescription>;

    template <class T>
    void ParseArguments(const T& op, const TSignature& signature, const TString& exceptionPrefix, size_t expectedArgumentCount, TArgumentList& arguments) {
        arguments.clear();
        size_t firstParamIdx = size_t(-1);
        for (size_t argumentIdx = 0; argumentIdx < op.ArgumentSize(); ++argumentIdx) {
            const TArgument& arg = op.GetArgument(argumentIdx);
            TArgumentDescription operand;
            operand.ParamIdx = size_t(-1);
            if (arg.GetVariable()) {
                operand.Type = OT_VARIABLE;
            } else if (arg.GetValue()) {
                operand.Type = OT_LITERAL;
            } else if (arg.GetParam()) {
                operand.Type = OT_PARAMETER;
                operand.ParamIdx = signature.FindParamIndex(arg.GetParam());
                if (operand.ParamIdx == size_t(-1)) {
                    ythrow yexception() << exceptionPrefix
                                        << " argument #" << argumentIdx << " param '" << arg.GetParam()
                                        << "' doesn't exist";
                }
                if (firstParamIdx == size_t(-1)) {
                    firstParamIdx = operand.ParamIdx;
                } else {
                    if (strcmp(signature.ParamTypes[firstParamIdx], signature.ParamTypes[operand.ParamIdx]) != 0) {
                        ythrow yexception() << exceptionPrefix
                                            << " param types do not match";
                    }
                }
            } else {
                ythrow yexception() << exceptionPrefix
                                    << " argument #" << argumentIdx
                                    << " is empty";
            }
            arguments.push_back(operand);
        }
        if (arguments.size() != expectedArgumentCount) {
            ythrow yexception() << exceptionPrefix
                                << " incorrect number of arguments (" << arguments.size()
                                << " present, " << expectedArgumentCount << " expected)";
        }
    }

    template <class TArg1, class TArg2>
    struct TTraceSecondArg {
        // implementation of deprecated std::project2nd
        TArg1 operator()(const TArg1&, const TArg2& y) const {
            return y;
        }
    };

    void TSession::InsertExecutor(
        TTraceVariables& traceVariables, size_t bi, const TPredicate* pred,
        const NProtoBuf::RepeatedPtrField<TAction>& actions, TProbe* probe,
        const bool destructiveActionsAllowed,
        const TCustomActionFactory& customActionFactory) {
#ifndef LWTRACE_DISABLE
        THolder<IExecutor> exec;
        IExecutor* last = nullptr;
        TArgumentList arguments;
        if (pred) {
            double sampleRate = pred->GetSampleRate();
            if (sampleRate != 0.0) {
                if (!(0.0 < sampleRate && sampleRate <= 1.0)) {
                    ythrow yexception() << "probe '" << probe->Event.Name << "' block #" << bi + 1 << " sampling operator"
                                        << " invalid sample rate " << sampleRate << ", expected [0;1]";
                }
                exec.Reset(new TSamplingExecutor(sampleRate));
                last = exec.Get();
            }

            for (size_t i = 0; i < pred->OperatorsSize(); i++) {
                const TOperator& op = pred->GetOperators(i);
                TString exceptionPrefix;
                TStringOutput exceptionPrefixOutput(exceptionPrefix);
                exceptionPrefixOutput << "probe '" << probe->Event.Name << "' block #" << bi + 1 << " operator #" << i + 1;
                ParseArguments<TOperator>(op, probe->Event.Signature, exceptionPrefix, 2, arguments);
                THolder<IExecutor> opExec;

                TArgumentDescription arg0 = arguments.at(0);
                TArgumentDescription arg1 = arguments.at(1);

                const char* tName0 = arg0.ParamIdx == size_t(-1) ? nullptr : probe->Event.Signature.ParamTypes[arg0.ParamIdx];
                const char* tName1 = arg1.ParamIdx == size_t(-1) ? nullptr : probe->Event.Signature.ParamTypes[arg1.ParamIdx];

                TString var0 = op.GetArgument(0).GetVariable();
                TString var1 = op.GetArgument(1).GetVariable();

                TString val0 = op.GetArgument(0).GetValue();
                TString val1 = op.GetArgument(1).GetValue();

#define FOREACH_OPERAND_TYPE_RT(n, t, v, fn, lt, rt)                                    \
    if (rt == arg1.Type) {                                                              \
        TOperand<t, rt> rhs(traceVariables, var1, val1, arg1.ParamIdx);                 \
        opExec.Reset(new TOperatorExecutor<t, fn<t>, lt, rt>(lhs, rhs, invertCompare)); \
        break;                                                                          \
    }

#define FOREACH_OPERAND_TYPE_LT(n, t, v, fn, lt)                        \
    if (lt == arg0.Type) {                                              \
        TOperand<t, lt> lhs(traceVariables, var0, val0, arg0.ParamIdx); \
        FOREACH_RIGHT_TYPE(FOREACH_OPERAND_TYPE_RT, n, t, v, fn, lt)    \
    }

#define FOREACH_PARAMTYPE_MACRO(n, t, v, fn)                                                                                  \
    if ((arg0.ParamIdx == size_t(-1) || strcmp(tName0, n) == 0) && (arg1.ParamIdx == size_t(-1) || strcmp(tName1, n) == 0)) { \
        FOREACH_LEFT_TYPE(FOREACH_OPERAND_TYPE_LT, n, t, v, fn);                                                              \
    }

                bool invertCompare = EqualToOneOf(op.GetType(), OT_NE, OT_GE, OT_LE);

                switch (op.GetType()) {
                    case OT_EQ:
                    case OT_NE:
                        FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO, std::equal_to);
                        break;
                    case OT_LT:
                    case OT_GE:
                        FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO, std::less);
                        break;
                    case OT_GT:
                    case OT_LE:
                        FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO, std::greater);
                        break;
                    default:
                        ythrow yexception() << exceptionPrefix
                                            << " has not supported operator type #" << int(op.GetType());
                }

#undef FOREACH_OPERAND_TYPE_RT
#undef FOREACH_OPERAND_TYPE_LT
#undef FOREACH_PARAMTYPE_MACRO

                if (!opExec) {
                    ythrow yexception() << exceptionPrefix
                                        << " has not supported left param #" << arg0.ParamIdx + 1 << " type '"
                                        << (arg0.ParamIdx != size_t(-1) ? probe->Event.Signature.ParamTypes[arg0.ParamIdx] : "?")
                                        << "', or right param #" << arg0.ParamIdx + 1 << " type '"
                                        << (arg1.ParamIdx != size_t(-1) ? probe->Event.Signature.ParamTypes[arg1.ParamIdx] : "?")
                                        << "'";
                }

                if (!exec) {
                    exec.Reset(opExec.Release());
                    last = exec.Get();
                } else {
                    last->SetNext(opExec.Release());
                    last = last->GetNext();
                }
            }
        }

        for (int i = 0; i < actions.size(); ++i) {
            const TAction& action = actions.Get(i);
            THolder<IExecutor> actExec;
            if (action.HasPrintToStderrAction()) {
                actExec.Reset(new TStderrActionExecutor(probe));
            } else if (action.HasLogAction()) {
                if (Query.GetLogDurationUs()) {
                    actExec.Reset(new TLogActionExecutor<TDurationLog>(probe, action.GetLogAction(), &DurationLog));
                } else {
                    actExec.Reset(new TLogActionExecutor<TCyclicLog>(probe, action.GetLogAction(), &CyclicLog));
                }
            } else if (action.HasRunLogShuttleAction()) {
                if (Query.GetLogDurationUs()) {
                    actExec.Reset(new TRunLogShuttleActionExecutor<TDurationDepot>(TraceIdx, action.GetRunLogShuttleAction(), &DurationDepot, &LastTrackId, &LastSpanId));
                } else {
                    actExec.Reset(new TRunLogShuttleActionExecutor<TCyclicDepot>(TraceIdx, action.GetRunLogShuttleAction(), &CyclicDepot, &LastTrackId, &LastSpanId));
                }
            } else if (action.HasEditLogShuttleAction()) {
                if (Query.GetLogDurationUs()) {
                    actExec.Reset(new TEditLogShuttleActionExecutor<TDurationDepot>(TraceIdx, action.GetEditLogShuttleAction()));
                } else {
                    actExec.Reset(new TEditLogShuttleActionExecutor<TCyclicDepot>(TraceIdx, action.GetEditLogShuttleAction()));
                }
            } else if (action.HasDropLogShuttleAction()) {
                if (Query.GetLogDurationUs()) {
                    actExec.Reset(new TDropLogShuttleActionExecutor<TDurationDepot>(TraceIdx, action.GetDropLogShuttleAction()));
                } else {
                    actExec.Reset(new TDropLogShuttleActionExecutor<TCyclicDepot>(TraceIdx, action.GetDropLogShuttleAction()));
                }
            } else if (action.HasCustomAction()) {
                THolder<TCustomActionExecutor> customExec(customActionFactory.Create(probe, action.GetCustomAction(), this));
                if (customExec) {
                    if (!customExec->IsDestructive() || destructiveActionsAllowed) {
                        actExec.Reset(customExec.Release());
                    } else {
                        ythrow yexception() << "probe '" << probe->Event.Name << "block #" << bi + 1 << " action #" << i + 1
                                            << " contains destructive CustomAction, but destructive actions are disabled."
                                            << " Please, consider using --unsafe-lwtrace command line parameter.";
                    }
                } else {
                    ythrow yexception() << "probe '" << probe->Event.Name << "block #" << bi + 1 << " action #" << i + 1
                                        << " contains unregistered CustomAction '" << action.GetCustomAction().GetName() << "'";
                }
            } else if (action.HasKillAction()) {
                if (destructiveActionsAllowed) {
                    actExec.Reset(new NPrivate::TKillActionExecutor(probe));
                } else {
                    ythrow yexception() << "probe '" << probe->Event.Name << "block #" << bi + 1 << " action #" << i + 1
                                        << " contains destructive KillAction, but destructive actions are disabled."
                                        << " Please, consider using --unsafe-lwtrace command line parameter.";
                }
            } else if (action.HasSleepAction()) {
                if (destructiveActionsAllowed) {
                    const TSleepAction& sleepAction = action.GetSleepAction();
                    if (sleepAction.GetNanoSeconds()) {
                        ui64 nanoSeconds = sleepAction.GetNanoSeconds();
                        actExec.Reset(new NPrivate::TSleepActionExecutor(probe, nanoSeconds));
                    } else {
                        ythrow yexception() << "probe '" << probe->Event.Name << "block #" << bi + 1 << " action #" << i + 1
                                            << " SleepAction missing parameter 'NanoSeconds'";
                    }
                } else {
                    ythrow yexception() << "probe '" << probe->Event.Name << "block #" << bi + 1 << " action #" << i + 1
                                        << " contains destructive SleepAction, but destructive actions are disabled."
                                        << " Please, consider using --unsafe-lwtrace command line parameter.";
                }
            } else if (action.HasStatementAction()) {
                const TStatementAction& statement = action.GetStatementAction();
                TString exceptionPrefix;
                TStringOutput exceptionPrefixOutput(exceptionPrefix);
                exceptionPrefixOutput << "probe '" << probe->Event.Name << "' block #" << bi + 1 << " action #" << i + 1;
                size_t expectedArgumentCount = 3;
                if (statement.GetType() == ST_MOV || statement.GetType() == ST_ADD_EQ || statement.GetType() == ST_SUB_EQ) {
                    expectedArgumentCount = 2;
                } else if (statement.GetType() == ST_INC || statement.GetType() == ST_DEC) {
                    expectedArgumentCount = 1;
                }
                ParseArguments<TStatementAction>(statement, probe->Event.Signature, exceptionPrefix, expectedArgumentCount, arguments);

                TArgumentDescription arg0 = (expectedArgumentCount <= 0) ? TArgumentDescription() : arguments.at(0);
                TArgumentDescription arg1 = (expectedArgumentCount <= 1) ? TArgumentDescription() : arguments.at(1);
                TArgumentDescription arg2 = (expectedArgumentCount <= 2) ? TArgumentDescription() : arguments.at(2);

                TString var0 = (expectedArgumentCount <= 0) ? "" : statement.GetArgument(0).GetVariable();
                TString var1 = (expectedArgumentCount <= 1) ? "" : statement.GetArgument(1).GetVariable();
                TString var2 = (expectedArgumentCount <= 2) ? "" : statement.GetArgument(2).GetVariable();

                TString val0 = (expectedArgumentCount <= 0) ? "" : statement.GetArgument(0).GetValue();
                TString val1 = (expectedArgumentCount <= 1) ? "" : statement.GetArgument(1).GetValue();
                TString val2 = (expectedArgumentCount <= 2) ? "" : statement.GetArgument(2).GetValue();

                const char* tName1 = (expectedArgumentCount <= 1 || arg1.ParamIdx == size_t(-1))
                    ? nullptr : probe->Event.Signature.ParamTypes[arg1.ParamIdx];
                const char* tName2 = (expectedArgumentCount <= 2 || arg2.ParamIdx == size_t(-1))
                    ? nullptr : probe->Event.Signature.ParamTypes[arg2.ParamIdx];

                if (arg0.Type == OT_VARIABLE) {
                    switch (statement.GetType()) {
#define PARSE_UNARY_INPLACE_STATEMENT_MACRO(n, t, v, fn)                   \
    {                                                                      \
        typedef TUnaryInplaceStatementExecutor<t, fn<TReceiver<t>>> TExec; \
        TReceiver<t> receiver(traceVariables, var0);                       \
        actExec.Reset(new TExec(receiver));                                \
        break;                                                             \
    }

#define PARSE_BINARY_INPLACE_STATEMENT_MACRO2(n, t, v, fn, ft)                     \
    if (arg1.Type == ft) {                                                         \
        typedef TBinaryInplaceStatementExecutor<t, fn<TReceiver<t>, t>, ft> TExec; \
        TOperand<t, ft> firstParam(traceVariables, var1, val1, arg1.ParamIdx);     \
        actExec.Reset(new TExec(receiver, firstParam));                            \
        break;                                                                     \
    }

#define PARSE_BINARY_INPLACE_STATEMENT_MACRO(n, t, v, fn)                       \
    if (arg1.ParamIdx == size_t(-1) || strcmp(tName1, n) == 0) {                \
        TReceiver<t> receiver(traceVariables, var0);                            \
        FOREACH_RIGHT_TYPE(PARSE_BINARY_INPLACE_STATEMENT_MACRO2, n, t, v, fn); \
    }

#define PARSE_BINARY_STATEMENT_MACRO2(n, t, v, fn, ft)                         \
    if (arg1.Type == ft) {                                                     \
        typedef TBinaryStatementExecutor<t, fn<t, t>, ft> TExec;               \
        TOperand<t, ft> firstParam(traceVariables, var1, val1, arg1.ParamIdx); \
        actExec.Reset(new TExec(receiver, firstParam));                        \
        break;                                                                 \
    }

#define PARSE_BINARY_STATEMENT_MACRO(n, t, v, fn)                       \
    if (arg1.ParamIdx == size_t(-1) || strcmp(tName1, n) == 0) {        \
        TReceiver<t> receiver(traceVariables, var0);                    \
        FOREACH_RIGHT_TYPE(PARSE_BINARY_STATEMENT_MACRO2, n, t, v, fn); \
    }

#define CREATE_OPERAND_GETTER_N(N, type, arg_type)                                                                                       \
    if (arg##N.Type == arg_type) {                                                                                                       \
        operand##N.Reset(new TOperandGetter<type, arg_type>(TOperand<type, arg_type>(traceVariables, var##N, val##N, arg##N.ParamIdx))); \
    }

#define TERNARY_ON_TYPE(n, t, v, fn)                                                                                          \
    if ((arg1.ParamIdx == size_t(-1) || strcmp(tName1, n) == 0) && (arg2.ParamIdx == size_t(-1) || strcmp(tName2, n) == 0)) { \
        TAutoPtr<IOperandGetter<t>> operand1, operand2;                                                                       \
        FOREACH_LEFT_TYPE(CREATE_OPERAND_GETTER_N, 1, t);                                                                     \
        FOREACH_RIGHT_TYPE(CREATE_OPERAND_GETTER_N, 2, t);                                                                    \
        if (operand1 && operand2) {                                                                                           \
            actExec.Reset(new TTernaryStatementExecutor<t, fn<t>>(                                                            \
                TReceiver<t>(traceVariables, var0),                                                                           \
                operand1,                                                                                                     \
                operand2));                                                                                                   \
        }                                                                                                                     \
        break;                                                                                                                \
    }

#define IMPLEMENT_TERNARY_STATEMENT(fn) FOR_MATH_PARAMTYPE(TERNARY_ON_TYPE, fn)

                        case ST_INC:
                            FOR_MATH_PARAMTYPE(PARSE_UNARY_INPLACE_STATEMENT_MACRO, TInc);
                            break;
                        case ST_DEC:
                            FOR_MATH_PARAMTYPE(PARSE_UNARY_INPLACE_STATEMENT_MACRO, TDec);
                            break;
                        case ST_MOV:
                            FOR_MATH_PARAMTYPE(PARSE_BINARY_STATEMENT_MACRO, TTraceSecondArg);
                            break;
                        case ST_ADD_EQ:
                            FOR_MATH_PARAMTYPE(PARSE_BINARY_INPLACE_STATEMENT_MACRO, TAddEq);
                            break;
                        case ST_SUB_EQ:
                            FOR_MATH_PARAMTYPE(PARSE_BINARY_INPLACE_STATEMENT_MACRO, TSubEq);
                            break;
                        case ST_ADD:
                            IMPLEMENT_TERNARY_STATEMENT(std::plus)
                            break;
                        case ST_SUB:
                            IMPLEMENT_TERNARY_STATEMENT(std::minus)
                            break;
                        case ST_MUL:
                            IMPLEMENT_TERNARY_STATEMENT(std::multiplies)
                            break;
                        case ST_DIV:
                            IMPLEMENT_TERNARY_STATEMENT(std::divides)
                            break;
                        case ST_MOD:
                            IMPLEMENT_TERNARY_STATEMENT(std::modulus)
                            break;
                        default:
                            ythrow yexception() << "block #" << bi + 1 << " action #" << i + 1
                                                << " has not supported statement type #" << int(statement.GetType());
                    }
                }
                if (!actExec) {
                    ythrow yexception() << "block #" << bi + 1 << " action #" << i + 1
                                        << " can't create action";
                }
#undef CREATE_OPERAND_GETTER_N
#undef TERNARY_ON_TYPE
#undef IMPLEMENT_TERNARY_STATEMENT
#undef PARSE_TERNARY_STATEMENT_MACRO
#undef PARSE_BINARY_STATEMENT_MACRO
#undef PARSE_BINARY_INPLACE_STATEMENT_MACRO
#undef PARSE_UNARY_INPLACE_STATEMENT_MACRO
            } else {
                ythrow yexception() << "block #" << bi + 1 << " action #" << i + 1
                                    << " has not supported action '" << action.ShortDebugString() << "'";
            }
            if (!exec) {
                exec.Reset(actExec.Release());
                last = exec.Get();
            } else {
                last->SetNext(actExec.Release());
                last = last->GetNext();
            }
        }

        if (!probe->Attach(exec.Get())) {
            ythrow yexception() << "block #" << bi + 1
                                << " cannot be attached to probe '" << probe->Event.Name << "': no free slots";
        }
        Probes.push_back(std::make_pair(probe, exec.Release()));
#else
        Y_UNUSED(bi);
        Y_UNUSED(pred);
        Y_UNUSED(actions);
        Y_UNUSED(probe);
        Y_UNUSED(destructiveActionsAllowed);
        Y_UNUSED(traceVariables);
        Y_UNUSED(customActionFactory);
#endif
    }

    TSession::TSession(ui64 traceIdx,
                   TProbeRegistry& registry,
                   const TQuery& query,
                   const bool destructiveActionsAllowed,
                   const TCustomActionFactory& customActionFactory)
        : StartTime(TInstant::Now())
        , TraceIdx(traceIdx)
        , Registry(registry)
        , StoreDuration(TDuration::MicroSeconds(query.GetLogDurationUs() * 11 / 10)) // +10% to try avoid truncation while reading multiple threads/traces
        , ReadDuration(TDuration::MicroSeconds(query.GetLogDurationUs()))
        , CyclicLog(query.GetPerThreadLogSize() ? query.GetPerThreadLogSize() : 1000)
        , DurationLog(StoreDuration)
        , CyclicDepot(query.GetPerThreadLogSize() ? query.GetPerThreadLogSize() : 1000)
        , DurationDepot(StoreDuration)
        , LastTrackId(0)
        , LastSpanId(0)
        , Attached(true)
        , Query(query)
    {
        try {
            for (size_t bi = 0; bi < query.BlocksSize(); bi++) {
                const TBlock& block = query.GetBlocks(bi);
                if (!block.HasProbeDesc()) {
                    ythrow yexception() << "block #" << bi + 1 << " has no probe description";
                }
                const TProbeDesc& pdesc = block.GetProbeDesc();
                const TPredicate* pred = block.HasPredicate() ? &block.GetPredicate() : nullptr;
                if (block.ActionSize() < 1) {
                    ythrow yexception() << "block #" << bi + 1 << " has no action";
                }
                const NProtoBuf::RepeatedPtrField<TAction>& actions = block.action();
                if (pdesc.GetName() && pdesc.GetProvider()) {
                    TProbeRegistry::TProbesAccessor probes(Registry);
                    bool found = false;
                    for (auto& kv : probes) {
                        TProbe* probe = kv.first;
                        if (probe->Event.Name == pdesc.GetName() && probe->Event.GetProvider() == pdesc.GetProvider()) {
                            InsertExecutor(TraceVariables, bi, pred, actions, probe, destructiveActionsAllowed, customActionFactory);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ythrow yexception() << "block #" << bi + 1 << " has no matching probe with name '"
                                            << pdesc.GetName() << "' provider '" << pdesc.GetProvider() << "'";
                    }
                } else if (pdesc.GetGroup()) {
                    bool found = false;
                    TProbeRegistry::TProbesAccessor probes(Registry);
                    for (auto& kv : probes) {
                        TProbe* probe = kv.first;
                        for (const char* const* gi = probe->Event.Groups; *gi != nullptr; gi++) {
                            if (*gi == pdesc.GetGroup()) {
                                InsertExecutor(TraceVariables, bi, pred, actions, probe, destructiveActionsAllowed, customActionFactory);
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found) {
                        ythrow yexception() << "block #" << bi + 1
                                            << " has no matching probes for group '" << pdesc.GetGroup() << "'";
                    }
                } else {
                    ythrow yexception() << "block #" << bi + 1 << " has bad probe description: name '" << pdesc.GetName()
                                        << "' provider '" << pdesc.GetProvider()
                                        << "' group '" << pdesc.GetGroup() << "'";
                }
            }
        } catch (...) {
            Destroy();
            throw;
        }
    }

    void TSession::Destroy() {
        Detach();
        for (auto& probe : Probes) {
            delete probe.second;
        }
    }

    TSession::~TSession() {
        Destroy();
    }

    void TSession::Detach() {
        if (Attached) {
            for (auto& p : Probes) {
                TProbe* probe = p.first;
                IExecutor* exec = p.second;
                probe->Detach(exec);
            }
            Attached = false;
        }
    }

    size_t TSession::GetEventsCount() const {
        return CyclicLog.GetEventsCount() + DurationLog.GetEventsCount() + CyclicDepot.GetEventsCount() + DurationDepot.GetEventsCount();
    }

    size_t TSession::GetThreadsCount() const {
        return CyclicLog.GetThreadsCount() + DurationLog.GetThreadsCount() + CyclicDepot.GetThreadsCount() + DurationDepot.GetThreadsCount();
    }

    class TReadToProtobuf {
    private:
        TMap<TThread::TId, TVector<TLogItem>> Items;

    public:
        void ToProtobuf(TLogPb& pb) const {
            TSet<TProbe*> probes;
            ui64 eventsCount = 0;
            for (auto kv : Items) {
                TThreadLogPb* tpb = pb.AddThreadLogs();
                tpb->SetThreadId(kv.first);
                for (TLogItem& item : kv.second) {
                    item.ToProtobuf(*tpb->AddLogItems());
                    probes.insert(item.Probe);
                    eventsCount++;
                }
            }
            pb.SetEventsCount(eventsCount);
            for (TProbe* probe : probes) {
                probe->Event.ToProtobuf(*pb.AddEvents());
            }
        }

        void Push(TThread::TId tid, const TLogItem& item) {
            // Avoid any expansive operations in Push(), because it executes under lock and blocks program being traced
            Items[tid].push_back(item);
        }
    };

    void TSession::ToProtobuf(TLogPb& pb) const {
        TReadToProtobuf reader;
        ReadItems(reader);
        reader.ToProtobuf(pb);
        pb.MutableQuery()->CopyFrom(Query);
        pb.SetCrtTime(TInstant::Now().GetValue());
    }

    TManager::TManager(TProbeRegistry& registry, bool allowDestructiveActions)
        : Registry(registry)
        , DestructiveActionsAllowed(allowDestructiveActions)
        , SerializingExecutor(new TRunLogShuttleActionExecutor<TCyclicDepot>(0, {}, nullptr, nullptr, nullptr))
    {
    }

    TManager::~TManager() {
        for (auto& trace : Traces) {
            delete trace.second;
        }
    }

    bool TManager::HasTrace(const TString& id) const {
        TGuard<TMutex> g(Mtx);
        return Traces.contains(id);
    }

    const TSession* TManager::GetTrace(const TString& id) const {
        TGuard<TMutex> g(Mtx);
        TTraces::const_iterator it = Traces.find(id);
        if (it == Traces.end()) {
            ythrow yexception() << "trace id '" << id << "' is not used";
        } else {
            return it->second;
        }
    }

    void TManager::New(const TString& id, const TQuery& query) {
        TGuard<TMutex> g(Mtx);
        if (Traces.find(id) == Traces.end()) {
            TSession* trace = new TSession(++LastTraceIdx, Registry, query, GetDestructiveActionsAllowed(), CustomActionFactory);
            Traces[id] = trace;
        } else {
            ythrow yexception() << "trace id '" << id << "' is already used";
        }
    }

    void TManager::Delete(const TString& id) {
        TGuard<TMutex> g(Mtx);
        TTraces::iterator it = Traces.find(id);
        if (it == Traces.end()) {
            ythrow yexception() << "trace id '" << id << "' is not used";
        } else {
            delete it->second;
            Traces.erase(it);
        }
    }

    void TManager::Stop(const TString& id) {
        TGuard<TMutex> g(Mtx);
        TTraces::iterator it = Traces.find(id);
        if (it == Traces.end()) {
            ythrow yexception() << "trace id '" << id << "' is not used";
        } else {
            it->second->Detach();
        }
    }
}
