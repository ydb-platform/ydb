#include "mon_lwtrace.h"

#include <algorithm>
#include <iterator>

#include <google/protobuf/text_format.h>
#include <library/cpp/lwtrace/mon/analytics/all.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/resource_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <util/string/escape.h>
#include <util/system/condvar.h>
#include <util/system/execpath.h>
#include <util/system/hostname.h>

using namespace NMonitoring;

#define WWW_CHECK(cond, ...) \
    do { \
        if (!(cond)) { \
            ythrow yexception() << Sprintf(__VA_ARGS__); \
        } \
    } while (false) \
    /**/

#define WWW_HTML_INNER(out) HTML(out) WITH_SCOPED(tmp, TScopedHtmlInner(out))
#define WWW_HTML(out) out << NMonitoring::HTTPOKHTML; WWW_HTML_INNER(out)

namespace NLwTraceMonPage {

struct TTrackLogRefs {
    struct TItem {
        const TThread::TId ThreadId;
        const NLWTrace::TLogItem* Ptr;

        TItem()
            : ThreadId(0)
            , Ptr(nullptr)
        {}

        TItem(TThread::TId tid, const NLWTrace::TLogItem& ref)
            : ThreadId(tid)
            , Ptr(&ref)
        {}

        TItem(const TItem& o)
            : ThreadId(o.ThreadId)
            , Ptr(o.Ptr)
        {}

        operator const NLWTrace::TLogItem&() const { return *Ptr; }
    };

    using TItems = TVector<TItem>;
    TItems Items;

    TTrackLogRefs() {}

    TTrackLogRefs(const TTrackLogRefs& other)
        : Items(other.Items)
    {}

    void Clear()
    {
        Items.clear();
    }

    ui64 GetTimestampCycles() const
    {
        return Items.empty()? 0: Items.front().Ptr->GetTimestampCycles();
    }
};

//
// Templates to treat both NLWTrace::TLogItem and NLWTrace::TTrackLog in the same way (e.g. in TLogQuery)
//

template <class TLog>
struct TLogTraits {};

template <>
struct TLogTraits<NLWTrace::TLogItem> {
    using TLog = NLWTrace::TLogItem;
    using const_iterator = const NLWTrace::TLogItem*;
    using const_reverse_iterator = const NLWTrace::TLogItem*;

    static const_iterator begin(const TLog& log) { return &log; }
    static const_iterator end(const TLog& log) { return &log + 1; }
    static const_reverse_iterator rbegin(const TLog& log) { return &log; }
    static const_reverse_iterator rend(const TLog& log) { return &log + 1; }
    static bool empty(const TLog&) { return false; }
    static const NLWTrace::TLogItem& front(const TLog& log) { return log; }
    static const NLWTrace::TLogItem& back(const TLog& log) { return log; }
};

template <>
struct TLogTraits<NLWTrace::TTrackLog> {
    using TLog = NLWTrace::TTrackLog;
    using const_iterator = NLWTrace::TTrackLog::TItems::const_iterator;
    using const_reverse_iterator = NLWTrace::TTrackLog::TItems::const_reverse_iterator;

    static const_iterator begin(const TLog& log) { return log.Items.begin(); }
    static const_iterator end(const TLog& log) { return log.Items.end(); }
    static const_reverse_iterator rbegin(const TLog& log) { return log.Items.rbegin(); }
    static const_reverse_iterator rend(const TLog& log) { return log.Items.rend(); }
    static bool empty(const TLog& log) { return log.Items.empty(); }
    static const NLWTrace::TLogItem& front(const TLog& log) { return log.Items.front(); }
    static const NLWTrace::TLogItem& back(const TLog& log) { return log.Items.back(); }
};

template <>
struct TLogTraits<TTrackLogRefs> {
    using TLog = TTrackLogRefs;
    using const_iterator = TTrackLogRefs::TItems::const_iterator;
    using const_reverse_iterator = TTrackLogRefs::TItems::const_reverse_iterator;

    static const_iterator begin(const TLog& log) { return log.Items.begin(); }
    static const_iterator end(const TLog& log) { return log.Items.end(); }
    static const_reverse_iterator rbegin(const TLog& log) { return log.Items.rbegin(); }
    static const_reverse_iterator rend(const TLog& log) { return log.Items.rend(); }
    static bool empty(const TLog& log) { return log.Items.empty(); }
    static const NLWTrace::TLogItem& front(const TLog& log) { return log.Items.front(); }
    static const NLWTrace::TLogItem& back(const TLog& log) { return log.Items.back(); }
};

/*
 * Log Query Language:
 * Data expressions:
 *  1) myparam[0], myparam[-1] // the first and the last myparam in any probe/provider
 *  2) myparam // the first (the same as [0])
 *  3) PROVIDER..myparam // any probe with myparam in PROVIDER
 *  4) MyProbe._elapsedMs // Ms since track begin for the first MyProbe event
 *  5) PROVIDER.MyProbe._sliceUs // Us since previous event in track for the first PROVIDER.MyProbe event
 */

struct TLogQuery {
private:
    enum ESpecialParam {
        NotSpecial = 0,
        // The last '*' can be one of: Ms, Us, Ns, Min, Hours, (blank means seconds)
        // '*Time' can be one of: RTime (since cut ts for given dataset), NTime (negative time since now), Time (since machine start)
        TrackDuration = 1,  // _track*
        TrackBeginTime = 2, // _begin*Time*
        TrackEndTime = 3,   // _end*Time*
        ElapsedDuration = 4,  // _elapsed*
        SliceDuration = 5,   // _slice*
        ThreadTime = 6,      // _thr*Time*
    };

    template <class TLog, class TTr = TLogTraits<TLog>>
    struct TExecuteQuery;

    template <class TLog, class TTr>
    friend struct TExecuteQuery;

    template <class TLog, class TTr>
    struct TExecuteQuery {
        const TLogQuery& Query;
        const TLog* Log = nullptr;
        bool Reversed;

        i64 Skip;
        const NLWTrace::TLogItem* Item = nullptr;
        typename TTr::const_iterator FwdIter;
        typename TTr::const_reverse_iterator RevIter;

        NLWTrace::TTypedParam Result;

        explicit TExecuteQuery(const TLogQuery& query, const TLog& log)
            : Query(query)
            , Log(&log)
            , Reversed(Query.Index < 0)
            , Skip(Reversed? -Query.Index - 1: Query.Index)
            , FwdIter()
            , RevIter()
        {}

        void ExecuteQuery()
        {
            if (!Reversed) {
                for (auto i = TTr::begin(*Log), e = TTr::end(*Log); i != e; ++i) {
                    if (FwdIteration(i)) {
                        return;
                    }
                }
            } else {
                for (auto i = TTr::rbegin(*Log), e = TTr::rend(*Log); i != e; ++i) {
                    if (RevIteration(i)) {
                        return;
                    }
                }
            }
        }

        bool FwdIteration(typename TTr::const_iterator it)
        {
            FwdIter = it;
            Item = &*it;
            return ProcessItem();
        }

        bool RevIteration(typename TTr::const_reverse_iterator it)
        {
            RevIter = it;
            Item = &*it;
            return ProcessItem();
        }

        bool ProcessItem()
        {
            if (Query.Provider && Query.Provider != Item->Probe->Event.GetProvider()) {
                return false;
            }
            if (Query.Probe && Query.Probe != Item->Probe->Event.Name) {
                return false;
            }
            switch (Query.SpecialParam) {
            case NotSpecial:
                if (Item->Probe->Event.Signature.FindParamIndex(Query.ParamName) != size_t(-1)) {
                    break; // param found
                } else {
                    return false; // param not found
                }
            case TrackDuration: Y_ABORT();
            case TrackBeginTime: Y_ABORT();
            case TrackEndTime: Y_ABORT();
            case ElapsedDuration: break;
            case SliceDuration: break;
            case ThreadTime: break;
            }
            if (Skip > 0) {
                Skip--;
                return false;
            }
            switch (Query.SpecialParam) {
            case NotSpecial:
                Result = NLWTrace::TTypedParam(Item->GetParam(Query.ParamName));
                return true;
            case TrackDuration: Y_ABORT();
            case TrackBeginTime: Y_ABORT();
            case TrackEndTime: Y_ABORT();
            case ElapsedDuration:
                Result = NLWTrace::TTypedParam(Query.Duration(
                    Log->GetTimestampCycles(),
                    Item->GetTimestampCycles()));
                return true;
            case SliceDuration:
                Result = NLWTrace::TTypedParam(Query.Duration(
                    PrevOrSame().GetTimestampCycles(),
                    Item->GetTimestampCycles()));
                return true;
            case ThreadTime:
                Result = NLWTrace::TTypedParam(Query.Instant(Item->GetTimestampCycles()));
                return true;
            }
            return true;
        }

        const NLWTrace::TLogItem& PrevOrSame() const
        {
            if (!Reversed) {
                auto i = FwdIter;
                if (i != TTr::begin(*Log)) {
                    i--;
                }
                return *i;
            } else {
                auto j = RevIter + 1;
                if (j == TTr::rend(*Log)) {
                    return *RevIter;
                }
                return *j;
            }
        }
    };

    TString Text;
    TString Provider;
    TString Probe;
    TString ParamName;
    ESpecialParam SpecialParam = NotSpecial;
    i64 Index = 0;
    double TimeUnitSec = 1.0;
    i64 ZeroTs = 0;
    i64 RTimeZeroTs = 0;
    i64 NTimeZeroTs = 0;

public:
    TLogQuery() {}

    explicit TLogQuery(const TString& text)
        : Text(text)
    {
        try {
            if (!Text.empty()) {
                ParseQuery(Text);
            }
        } catch (...) {
            ythrow yexception()
                << EncodeHtmlPcdata(CurrentExceptionMessage())
                << " while parsing track log query: "
                << Text;
        }
    }

    operator bool() const
    {
        return !Text.empty();
    }

    template <class TLog>
    NLWTrace::TTypedParam ExecuteQuery(const TLog& log) const
    {
        using TTr = TLogTraits<TLog>;

        WWW_CHECK(Text, "execute of empty log query");
        if (TTr::empty(log)) {
            return NLWTrace::TTypedParam();
        }

        if (SpecialParam == TrackDuration) {
            return Duration(
                log.GetTimestampCycles(),
                TTr::back(log).GetTimestampCycles());
        } else if (SpecialParam == TrackBeginTime) {
            return Instant(log.GetTimestampCycles());
        } else if (SpecialParam == TrackEndTime) {
            return Instant(TTr::back(log).GetTimestampCycles());
        }

        TExecuteQuery<TLog, TTr> exec(*this, log);
        exec.ExecuteQuery();
        return exec.Result;
    }

private:
    NLWTrace::TTypedParam Duration(ui64 ts1, ui64 ts2) const
    {
        double sec = NHPTimer::GetSeconds(ts1 < ts2? ts2 - ts1: 0);
        return NLWTrace::TTypedParam(sec / TimeUnitSec);
    }

    NLWTrace::TTypedParam Instant(ui64 ts) const
    {
        double sec = NHPTimer::GetSeconds(i64(ts) - ZeroTs);
        return NLWTrace::TTypedParam(sec / TimeUnitSec);
    }

    void ParseQuery(const TString& s)
    {
        auto parts = SplitString(s, ".");
        WWW_CHECK(parts.size() <= 3, "too many name specifiers");
        ParseParamSelector(parts.back());
        if (parts.size() >= 2) {
            ParseProbeSelector(parts[parts.size() - 2]);
        }
        if (parts.size() >= 3) {
            ParseProviderSelector(parts[parts.size() - 3]);
        }
    }

    void ParseParamSelector(const TString& s)
    {
        size_t bracket = s.find('[');
        if (bracket == TString::npos) {
            ParseParamName(s);
            Index = 0;
        } else {
            ParseParamName(s.substr(0, bracket));
            size_t bracket2 = s.find(']', bracket);
            WWW_CHECK(bracket2 != TString::npos, "closing braket ']' is missing");
            Index = FromString<i64>(s.substr(bracket + 1, bracket2 - bracket - 1));
        }
    }

    void ParseParamName(const TString& s)
    {
        ParamName = s;
        TString paramName = s;

        const static TVector<std::pair<TString, ESpecialParam>> specials = {
            { "_track", TrackDuration },
            { "_begin", TrackBeginTime },
            { "_end", TrackEndTime },
            { "_elapsed", ElapsedDuration },
            { "_slice", SliceDuration },
            { "_thr", ThreadTime }
        };

        // Check for special params
        SpecialParam = NotSpecial;
        for (const auto& p : specials) {
            if (paramName.StartsWith(p.first)) {
                SpecialParam = p.second;
                paramName.erase(0, p.first.size());
                break;
            }
        }

        if (SpecialParam == NotSpecial) {
            return;
        }

        const static TVector<std::pair<TString, double>> timeUnits = {
            { "Ms", 1e-3 },
            { "Us", 1e-6 },
            { "Ns", 1e-9 },
            { "Min", 60.0 },
            { "Hours", 3600.0 }
        };

        // Parse units for special params
        TimeUnitSec = 1.0;
        for (const auto& p : timeUnits) {
            if (paramName.EndsWith(p.first)) {
                TimeUnitSec = p.second;
                paramName.erase(paramName.size() - p.first.size());
                break;
            }
        }

        if (SpecialParam == ThreadTime ||
                SpecialParam == TrackBeginTime ||
                SpecialParam == TrackEndTime)
        {
            // Parse time zero for special instant params
            const TVector<std::pair<TString, i64>> timeZeros = {
                { "RTime", RTimeZeroTs },
                { "NTime", NTimeZeroTs },
                { "Time", 0ll }
            };
            ZeroTs = -1;
            for (const auto& p : timeZeros) {
                if (paramName.EndsWith(p.first)) {
                    ZeroTs = p.second;
                    paramName.erase(paramName.size() - p.first.size());
                    break;
                }
            }
            WWW_CHECK(ZeroTs != -1, "wrong special param name (postfix '*Time' required): %s", s.data());
        }

        WWW_CHECK(paramName.empty(), "wrong special param name: %s", s.data());
    }

    void ParseProbeSelector(const TString& s)
    {
        Probe = s;
    }

    void ParseProviderSelector(const TString& s)
    {
        Provider = s;
    }
};

using TVariants = TVector<std::pair<TString, TString>>;
using TTags = TSet<TString>;

TString GetProbeName(const NLWTrace::TProbe* probe, const char* sep = ".")
{
    return TString(probe->Event.GetProvider()) + sep + probe->Event.Name;
}

struct TAdHocTraceConfig {
    NLWTrace::TQuery Cfg;

    TAdHocTraceConfig() {} // Invalid config

    TAdHocTraceConfig(ui16 logSize, ui64 logDurationUs, bool logShuttle)
    {
        auto block = Cfg.AddBlocks(); // Create one block to distinguish valid config
        if (logSize) {
            Cfg.SetPerThreadLogSize(logSize);
        }
        if (logDurationUs) {
            Cfg.SetLogDurationUs(logDurationUs);
        }
        if (logShuttle) {
            block->AddAction()->MutableRunLogShuttleAction();
        }
    }

    TAdHocTraceConfig(const TString& provider, const TString& probe, ui16 logSize = 0, ui64 logDurationUs = 0, bool logShuttle = false)
        : TAdHocTraceConfig(logSize, logDurationUs, logShuttle)
    {
        auto block = Cfg.MutableBlocks(0);
        auto pdesc = block->MutableProbeDesc();
        pdesc->SetProvider(provider);
        pdesc->SetName(probe);
    }

    explicit TAdHocTraceConfig(const TString& group, ui16 logSize = 0, ui64 logDurationUs = 0, bool logShuttle = false)
        : TAdHocTraceConfig(logSize, logDurationUs, logShuttle)
    {
        auto block = Cfg.MutableBlocks(0);
        auto pdesc = block->MutableProbeDesc();
        pdesc->SetGroup(group);
    }

    TString Id() const
    {
        TStringStream ss;
        for (size_t blockIdx = 0; blockIdx < Cfg.BlocksSize(); blockIdx++) {
            if (!ss.Str().empty()) {
                ss << "/";
            }
            auto block = Cfg.GetBlocks(blockIdx);
            auto pdesc = block.GetProbeDesc();
            if (pdesc.GetProvider()) {
                ss << "." << pdesc.GetProvider() << "." << pdesc.GetName();
            } else if (pdesc.GetGroup()) {
                ss << ".Group." << pdesc.GetGroup();
            }
            // TODO[serxa]: handle predicate
            for (size_t actionIdx = 0; actionIdx < block.ActionSize(); actionIdx++) {
                const NLWTrace::TAction& action = block.GetAction(actionIdx);
                if (action.HasRunLogShuttleAction()) {
                    auto ls = action.GetRunLogShuttleAction();
                    ss << ".alsr";
                    if (ls.GetIgnore()) {
                        ss << "-i";
                    }
                    if (ls.GetShuttlesCount()) {
                        ss << "-s" << ls.GetShuttlesCount();
                    }
                    if (ls.GetMaxTrackLength()) {
                        ss << "-t" << ls.GetMaxTrackLength();
                    }
                } else if (action.HasEditLogShuttleAction()) {
                    auto ls = action.GetEditLogShuttleAction();
                    ss << ".alse";
                    if (ls.GetIgnore()) {
                        ss << "-i";
                    }
                } else if (action.HasDropLogShuttleAction()) {
                    ss << ".alsd";
                }
            }
        }
        if (Cfg.GetPerThreadLogSize()) {
            ss << ".l" << Cfg.GetPerThreadLogSize();
        }
        if (Cfg.GetLogDurationUs()) {
            ui64 logDurationUs = Cfg.GetLogDurationUs();
            if (logDurationUs % (60 * 1000 * 1000) == 0)
                ss << ".d" << logDurationUs / (60 * 1000 * 1000) << "m";
            if (logDurationUs % (1000 * 1000) == 0)
                ss << ".d" << logDurationUs / (1000 * 1000) << "s";
            else if (logDurationUs % 1000 == 0)
                ss << ".d" << logDurationUs / 1000 << "ms";
            else
                ss << ".d" << logDurationUs << "us";
        }
        return ss.Str();
    }

    bool IsValid() const
    {
        return Cfg.BlocksSize() > 0;
    }

    NLWTrace::TQuery Query() const
    {
        if (!IsValid()) {
            ythrow yexception() << "invalid adhoc trace config";
        }
        return Cfg;
    }

    bool ParseId(const TString& id)
    {
        if (IsAdHocId(id)) {
            for (const TString& block : SplitString(id, "/")) {
                if (block.empty()) {
                    continue;
                }
                size_t cutPos = (block[0] == '.'? 1: 0);
                TVector<TString> parts = SplitString(block.substr(cutPos), ".");
                WWW_CHECK(parts.size() >= 2, "too few parts in adhoc trace id '%s' block '%s'", id.data(), block.data());
                auto blockPb = Cfg.AddBlocks();
                auto pdescPb = blockPb->MutableProbeDesc();
                if (parts[0] == "Group") {
                    pdescPb->SetGroup(parts[1]);
                } else {
                    pdescPb->SetProvider(parts[0]);
                    pdescPb->SetName(parts[1]);
                }
                bool defaultAction = true;
                for (auto i = parts.begin() + 2, e = parts.end(); i != e; ++i) {
                    const TString& part = *i;
                    if (part.empty()) {
                        continue;
                    }
                    switch (part[0]) {
                    case 'l': Cfg.SetPerThreadLogSize(FromString<ui16>(part.substr(1))); break;
                    case 'd': Cfg.SetLogDurationUs(ParseDuration(part.substr(1))); break;
                    case 's': blockPb->MutablePredicate()->SetSampleRate(1.0 / Max<ui64>(1, FromString<ui64>(part.substr(1)))); break;
                    case 'p': ParsePredicate(blockPb->MutablePredicate()->AddOperators(), part.substr(1)); break;
                    case 'a': ParseAction(blockPb->AddAction(), part.substr(1)); defaultAction = false; break;
                    default: WWW_CHECK(false, "unknown adhoc trace part type '%s' in '%s'", part.data(), id.data());
                    }
                }
                if (defaultAction) {
                    blockPb->AddAction()->MutableLogAction();
                }
            }
            return true;
        }
        return false;
    }
private:
    static bool IsAdHocId(const TString& id)
    {
        return !id.empty() && id[0] == '.';
    }

    void ParsePredicate(NLWTrace::TOperator* op, const TString& p)
    {
        size_t sign = p.find_first_of("=!><");
        WWW_CHECK(sign != TString::npos, "wrong predicate format in adhoc trace: %s", p.data());
        op->AddArgument()->SetParam(p.substr(0, sign));
        size_t value = sign + 1;
        switch (p[sign]) {
        case '=':
            op->SetType(NLWTrace::OT_EQ);
            break;
        case '!': {
            WWW_CHECK(p.size() > sign + 1, "wrong predicate operator format in adhoc trace: %s", p.data());
            WWW_CHECK(p[sign + 1] == '=', "wrong predicate operator format in adhoc trace: %s", p.data());
            value++;
            op->SetType(NLWTrace::OT_NE);
            break;
        }
        case '<': {
            WWW_CHECK(p.size() > sign + 1, "wrong predicate operator format in adhoc trace: %s", p.data());
            if (p[sign + 1] == '=') {
                value++;
                op->SetType(NLWTrace::OT_LE);
            } else {
                op->SetType(NLWTrace::OT_LT);
            }
            break;
        }
        case '>': {
            WWW_CHECK(p.size() > sign + 1, "wrong predicate operator format in adhoc trace: %s", p.data());
            if (p[sign + 1] == '=') {
                value++;
                op->SetType(NLWTrace::OT_GE);
            } else {
                op->SetType(NLWTrace::OT_GT);
            }
            break;
        }
        default: WWW_CHECK(false, "wrong predicate operator format in adhoc trace: %s", p.data());
        }
        op->AddArgument()->SetValue(p.substr(value));
    }

    void ParseAction(NLWTrace::TAction* action, const TString& a)
    {
        // NOTE: checks for longer action names should go first, your captain.
        if (a.substr(0, 3) == "lsr") {
            auto pb = action->MutableRunLogShuttleAction();
            for (const TString& opt : SplitString(a.substr(3), "-")) {
                if (!opt.empty()) {
                    switch (opt[0]) {
                    case 'i': pb->SetIgnore(true); break;
                    case 's': pb->SetShuttlesCount(FromString<ui64>(opt.substr(1))); break;
                    case 't': pb->SetMaxTrackLength(FromString<ui64>(opt.substr(1))); break;
                    default: WWW_CHECK(false, "unknown adhoc trace log shuttle opt '%s' in '%s'", opt.data(), a.data());
                    }
                }
            }
        } else if (a.substr(0, 3) == "lse") {
            auto pb = action->MutableEditLogShuttleAction();
            for (const TString& opt : SplitString(a.substr(3), "-")) {
                if (!opt.empty()) {
                    switch (opt[0]) {
                    case 'i': pb->SetIgnore(true); break;
                    default: WWW_CHECK(false, "unknown adhoc trace log shuttle opt '%s' in '%s'", opt.data(), a.data());
                    }
                }
            }
        } else if (a.substr(0, 3) == "lsd") {
            action->MutableDropLogShuttleAction();
        } else if (a.substr(0, 1) == "l") {
            auto pb = action->MutableLogAction();
            for (const TString& opt : SplitString(a.substr(1), "-")) {
                if (!opt.empty()) {
                    switch (opt[0]) {
                    case 't': pb->SetLogTimestamp(true); break;
                    case 'r': pb->SetMaxRecords(FromString<ui32>(opt.substr(1))); break;
                    default: WWW_CHECK(false, "unknown adhoc trace log opt '%s' in '%s'", opt.data(), a.data());
                    }
                }
            }
        } else {
            WWW_CHECK(false, "wrong action format in adhoc trace: %s", a.data());
        }
    }

    static ui64 ParseDuration(const TString& s)
    {
        if (s.substr(s.length() - 2) == "us")
            return FromString<ui64>(s.substr(0, s.length() - 2));
        if (s.substr(s.length() - 2) == "ms")
            return FromString<ui64>(s.substr(0, s.length() - 2)) * 1000;
        if (s.substr(s.length() - 1) == "s")
            return FromString<ui64>(s.substr(0, s.length() - 1)) * 1000 * 1000;
        if (s.substr(s.length() - 1) == "m")
            return FromString<ui64>(s.substr(0, s.length() - 1)) * 60 * 1000 * 1000;
        else
            return FromString<ui64>(s);
    }
};

// Class that maintains one thread iff there are adhoc traces and cleans'em by deadlines
class TTraceCleaner {
private:
    NLWTrace::TManager* TraceMngr;
    TAtomic Quit = 0;

    TMutex Mtx;
    TCondVar WakeCondVar;
    THashMap<TString, TInstant> Deadlines;
    volatile bool ThreadIsRunning = false;
    THolder<TThread> Thread;
public:
    TTraceCleaner(NLWTrace::TManager* traceMngr)
        : TraceMngr(traceMngr)
    {}

    ~TTraceCleaner()
    {
        AtomicSet(Quit, 1);
        WakeCondVar.Signal();
        // TThread dtor joins thread
    }

    // Returns deadline for specified trace id or zero
    TInstant GetDeadline(const TString& id) const
    {
        TGuard<TMutex> g(Mtx);
        auto iter = Deadlines.find(id);
        return iter != Deadlines.end()? iter->second: TInstant::Zero();
    }

    // Postpone deletion of specified trace for specified timeout
    void Postpone(const TString& id, TDuration timeout, bool allowLowering)
    {
        TGuard<TMutex> g(Mtx);
        TInstant newDeadline = TInstant::Now() + timeout;
        if (Deadlines[id] < newDeadline) {
            Deadlines[id] = newDeadline;
        } else if (allowLowering) { // Deadline lowering requires wake
            Deadlines[id] = newDeadline;
            WakeCondVar.Signal();
        }
        if (newDeadline != TInstant::Max() && !ThreadIsRunning) {
            // Note that dtor joins previous thread if any
            Thread.Reset(new TThread(ThreadProc, this));
            Thread->Start();
            ThreadIsRunning = true;
        }
    }

    // Forget about specified trace deletion
    void Forget(const TString& id)
    {
        TGuard<TMutex> g(Mtx);
        Deadlines.erase(id);
        WakeCondVar.Signal(); // in case thread is not required any more
    }
private:
    void Exec()
    {
        while (!AtomicGet(Quit)) {
            TGuard<TMutex> g(Mtx);

            // Delete all timed out traces
            TInstant now = TInstant::Now();
            TInstant nextDeadline = TInstant::Max();
            for (auto i = Deadlines.begin(), e = Deadlines.end(); i != e;) {
                const TString& id = i->first;
                TInstant deadline = i->second;
                if (deadline < now) {
                    try {
                        TraceMngr->Delete(id);
                    } catch (...) {
                        // already deleted
                    }
                    Deadlines.erase(i++);
                } else {
                    nextDeadline = Min(nextDeadline, deadline);
                    ++i;
                }
            }

            // Stop thread if there is no more work
            if (Deadlines.empty() || nextDeadline == TInstant::Max()) {
                ThreadIsRunning = false;
                break;
            }

            // Wait until next deadline or quit
            WakeCondVar.WaitD(Mtx, nextDeadline);
        }
    }

    static void* ThreadProc(void* _this)
    {
        TString name = "LWTraceCleaner";
        // Copy-pasted from kikimr/core/util/thread.h
#if defined(_linux_)
        TStringStream linuxName;
        linuxName << TStringBuf(GetExecPath()).RNextTok('/') << "." << name;
        TThread::SetCurrentThreadName(linuxName.Str().data());
#else
        TThread::SetCurrentThreadName(name.data());
#endif
        static_cast<TTraceCleaner*>(_this)->Exec();
        return nullptr;
    }
};

class TChromeTrace {
private:
    TMultiMap<double, TString> TraceEvents;
    THashMap<TThread::TId, TString> Tids;
public:
    void Add(TThread::TId tid, ui64 tsCycles, const TString& ph, const TString& cat,
             const NLWTrace::TLogItem* argsItem = nullptr,
             const TString& name = TString(), const TString& id = TString())
    {
        auto tidIter = Tids.find(tid);
        if (tidIter == Tids.end()) {
            tidIter = Tids.emplace(tid, ToString(Tids.size() + 1)).first;
        }
        const TString& shortId = tidIter->second;
        double ts = Timestamp(tsCycles);
        TraceEvents.emplace(ts, Event(shortId, ts, ph, cat, argsItem, name, id));
    }

    void Output(IOutputStream& os)
    {
        os << "{\"traceEvents\":[";
        bool first = true;
        for (auto kv : TraceEvents) {
            if (!first) {
                os << ",\n";
            }
            os << kv.second;
            first = false;
        }
        os << "]}";
    }

private:
    static TString Event(const TString& tid, double ts, const TString& ph, const TString& cat,
                        const NLWTrace::TLogItem* argsItem,
                        const TString& name, const TString& id)
    {
        TStringStream ss;
        pid_t pid = 1;
        ss << "{\"pid\":" << pid
           << ",\"tid\":" << tid
           << ",\"ts\":" << Sprintf("%lf", ts)
           << ",\"ph\":\"" << ph << "\""
           << ",\"cat\":\"" << cat << "\"";
        if (name) {
            ss << ",\"name\":\"" << name << "\"";
        }
        if (id) {
            ss << ",\"id\":\"" << id << "\"";
        }
        if (argsItem && argsItem->SavedParamsCount > 0) {
            ss << ",\"args\":{";
            TString paramValues[LWTRACE_MAX_PARAMS];
            argsItem->Probe->Event.Signature.SerializeParams(argsItem->Params, paramValues);
            bool first = true;
            for (size_t pi = 0; pi < argsItem->SavedParamsCount; pi++, first = false) {
                if (!first) {
                    ss << ",";
                }
                ss << "\"" << TString(argsItem->Probe->Event.Signature.ParamNames[pi]) << "\":"
                      "\"" << paramValues[pi] << "\"";
            }
            ss << "}";
        }
        ss << "}";
        return ss.Str();
    }

    static double Timestamp(ui64 cycles)
    {
        return double(cycles) * 1000000.0 / NHPTimer::GetClockRate();
    }
};

TString MakeUrl(const TCgiParameters& e, const THashMap<TString, TString>& values)
{
    TStringStream ss;
    bool first = true;
    for (const auto& [k, v] : e) {
        if (values.find(k) == values.end()) {
            ss << (first? "?": "&") << k << "=" << v;
            first = false;
        }
    }
    for (const auto& [k, v] : values) {
        ss << (first? "?": "&") << k << "=" << v;
        first = false;
    }
    return ss.Str();
}

TString MakeUrl(const TCgiParameters& e, const TString& key, const TString& value, bool keep = false)
{
    TStringStream ss;
    bool first = true;
    for (const auto& kv : e) {
        if (keep || kv.first != key) {
            ss << (first? "?": "&") << kv.first << "=" << kv.second;
            first = false;
        }
    }
    ss << (first? "?": "&") << key << "=" << value;
    return ss.Str();
}

TString MakeUrlAdd(const TCgiParameters& e, const TString& key, const TString& value)
{
    TStringStream ss;
    bool first = true;
    for (const auto& kv : e) {
        ss << (first? "?": "&") << kv.first << "=" << kv.second;
        first = false;
    }
    ss << (first? "?": "&") << key << "=" << value;
    return ss.Str();
}

TString MakeUrlReplace(const TCgiParameters& e, const TString& key, const TString& oldValue, const TString& newValue)
{
    TStringStream ss;
    bool first = true;
    bool inserted = false;
    for (const auto& kv : e) {
        if (kv.first == key && (kv.second == oldValue || kv.second == newValue)) {
            if (!inserted) {
                inserted = true;
                ss << (first? "?": "&") << key << "=" << newValue;
                first = false;
            }
        } else {
            ss << (first? "?": "&") << kv.first << "=" << kv.second;
            first = false;
        }
    }
    if (!inserted) {
        ss << (first? "?": "&") << key << "=" << newValue;
    }
    return ss.Str();
}

TString MakeUrlErase(const TCgiParameters& e, const TString& key, const TString& value)
{
    TStringStream ss;
    bool first = true;
    for (const auto& kv : e) {
        if (kv.first != key || kv.second != value) {
            ss << (first? "?": "&") << kv.first << "=" << kv.second;
            first = false;
        }
    }
    return ss.Str();
}

TString EscapeSubvalue(const TString& s)
{
    TString ret;
    ret.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        if (c == ':') {
            ret.append("^c");
        } else if (c == '^') {
            ret.append("^^");
        } else {
            ret.append(c);
        }
    }
    return ret;
}

TString UnescapeSubvalue(const TString& s)
{
    TString ret;
    ret.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        if (c == '^' && i + 1 < s.size()) {
            char c2 = s[++i];
            if (c2 == 'c') {
                ret.append(':');
            } else if (c2 == '^') {
                ret.append('^');
            } else {
                ret.append(c);
                ret.append(c2);
            }
        } else {
            ret.append(c);
        }
    }
    return ret;
}

TVector<TString> Subvalues(const TCgiParameters& e, const TString& key)
{
    if (!e.Has(key)) {
        return TVector<TString>();
    } else {
        TVector<TString> ret;
        for (const TString& s : SplitString(e.Get(key), ":", 0, KEEP_EMPTY_TOKENS)) {
            ret.push_back(UnescapeSubvalue(s));
        }
        if (ret.empty()) {
            ret.push_back("");
        }
        return ret;
    }
}

TString ParseTagsOut(const TString& taggedStr, TTags& tags)
{
    auto vec = SplitString(taggedStr, "-");
    if (vec.empty()) {
        return "";
    }
    auto iter = vec.begin();
    TString value = *iter++;
    for (;iter != vec.end(); ++iter) {
        tags.insert(*iter);
    }
    return value;
}

TString JoinTags(TTags tags) {
    return JoinStrings(TVector<TString>(tags.begin(), tags.end()), "-");
}

TString MakeValue(const TVector<TString>& subvalues)
{
    TVector<TString> subvaluesEsc;
    for (const TString& s : subvalues) {
        subvaluesEsc.push_back(EscapeSubvalue(s));
    }
    return JoinStrings(subvaluesEsc, ":");
}

TString MakeUrlAddSub(const TCgiParameters& e, const TString& key, const TString& subvalue)
{
    const TString& value = e.Get(key);
    auto subvalues = Subvalues(e, key);
    subvalues.push_back(subvalue);
    return MakeUrlReplace(e, key, value, MakeValue(subvalues));
}

TString MakeUrlReplaceSub(const TCgiParameters& e, const TString& key, const TString& oldSubvalue, const TString& newSubvalue)
{
    const TString& value = e.Get(key);
    auto subvalues = Subvalues(e, key);
    auto iter = std::find(subvalues.begin(), subvalues.end(), oldSubvalue);
    if (iter != subvalues.end()) {
        *iter = newSubvalue;
    } else {
        subvalues.push_back(newSubvalue);
    }
    return MakeUrlReplace(e, key, value, MakeValue(subvalues));
}

TString MakeUrlEraseSub(const TCgiParameters& e, const TString& key, const TString& subvalue)
{
    const TString& value = e.Get(key);
    auto subvalues = Subvalues(e, key);
    auto iter = std::find(subvalues.begin(), subvalues.end(), subvalue);
    if (iter != subvalues.end()) {
        subvalues.erase(iter);
    }
    if (subvalues.empty()) {
        return MakeUrlErase(e, key, value);
    } else {
        return MakeUrlReplace(e, key, value, MakeValue(subvalues));
    }
}

template <bool sub> TString UrlAdd(const TCgiParameters& e, const TString& key, const TString& value);
template <> TString UrlAdd<false>(const TCgiParameters& e, const TString& key, const TString& value) {
    return MakeUrlAdd(e, key, value);
}
template <> TString UrlAdd<true>(const TCgiParameters& e, const TString& key, const TString& value) {
    return MakeUrlAddSub(e, key, value);
}

template <bool sub> TString UrlReplace(const TCgiParameters& e, const TString& key, const TString& oldValue, const TString& newValue);
template <> TString UrlReplace<false>(const TCgiParameters& e, const TString& key, const TString& oldValue, const TString& newValue) {
    return MakeUrlReplace(e, key, oldValue, newValue);
}
template <> TString UrlReplace<true>(const TCgiParameters& e, const TString& key, const TString& oldValue, const TString& newValue) {
    return MakeUrlReplaceSub(e, key, oldValue, newValue);
}

template <bool sub> TString UrlErase(const TCgiParameters& e, const TString& key, const TString& value);
template <> TString UrlErase<false>(const TCgiParameters& e, const TString& key, const TString& value) {
    return MakeUrlErase(e, key, value);
}
template <> TString UrlErase<true>(const TCgiParameters& e, const TString& key, const TString& value) {
    return MakeUrlEraseSub(e, key, value);
}

void OutputCommonHeader(IOutputStream& out)
{
    out << NResource::Find("lwtrace/mon/static/header.html") << Endl;
}

void OutputCommonFooter(IOutputStream& out)
{
    out << NResource::Find("lwtrace/mon/static/footer.html") << Endl;
}

struct TScopedHtmlInner {
    explicit TScopedHtmlInner(IOutputStream& str)
        : Str(str)
    {
        Str << "<!DOCTYPE html>\n"
               "<html>";
        HTML(str) {
            HEAD() { OutputCommonHeader(Str); }
        }
        Str << "<body>";
    }

    ~TScopedHtmlInner()
    {
        OutputCommonFooter(Str);
        Str << "</body></html>";
    }

    inline operator bool () const noexcept { return true; }

    IOutputStream &Str;
};

TString NavbarHeader()
{
    return "<div class=\"navbar-header\">"
             "<a class=\"navbar-brand\" href=\"?mode=\">LWTrace</a>"
           "</div>";
}

struct TSelectorsContainer {
    TSelectorsContainer(IOutputStream& str)
        : Str(str)
    {
        Str << "<nav id=\"selectors-container\" class=\"navbar navbar-default\">"
               "<div class=\"container-fluid\">"
            << NavbarHeader() <<
               "<div class=\"navbar-text\" style=\"margin-top:12px;margin-bottom:10px\">";
    }

    ~TSelectorsContainer() {
        try {
            Str <<
                "</div>"
                "<div class=\"container-fluid\">"
                    "<div class=\"pull-right\">"
                        "<button id=\"download-btn\""
                        " type=\"button\" style=\"display: inline-block;margin:7px\""
                        " title=\"Chromium trace (load it in chrome://tracing/)\""
                        " class=\"btn btn-default hidden\">"
                            "<span class=\"glyphicon glyphicon-download-alt\"></span>"
                        "</button>"
                    "</div>"
                "</div>"
            "</div></nav>";
        } catch(...) {}
    }

    IOutputStream& Str;
};

struct TNullContainer {
    TNullContainer(IOutputStream&) {}
};

class TPageGenBase: public std::exception {};
template <class TContainer = TNullContainer>
class TPageGen: public TPageGenBase {
private:
    TString Content;
    TString HttpResponse;
public:
    void BuildResponse()
    {
        TStringStream ss;
        WWW_HTML(ss) {
            TContainer container(ss);
            ss << Content;
        }
        HttpResponse = ss.Str();
    }

    explicit TPageGen(const TString& content = TString())
        : Content(content)
    {
        BuildResponse();
    }

    void Append(const TString& moreContent)
    {
        Content.append(moreContent);
        BuildResponse();
    }

    void Prepend(const TString& moreContent)
    {
        Content.prepend(moreContent);
        BuildResponse();
    }

    virtual const char* what() const noexcept { return HttpResponse.data(); }
    operator bool() const { return !Content.empty(); }
};

enum EStyleFlags {
    // bit 1
    Link = 0x0,
    Button = 0x1,

    // bit 2
    NonErasable = 0x0,
    Erasable = 0x2,

    // bit 3-4
    Medium = 0x0,
    Large = 0x4,
    Small = 0x8,
    ExtraSmall = 0xC,
    SizeMask = 0xC,

    // bit 5
    NoCaret = 0x0,
    Caret = 0x10,

    // bit 6
    SimpleValue = 0x0,
    CompositeValue = 0x20
};

template <ui64 flags>
TString BtnClass() {
    if ((flags & SizeMask) == Large) {
        return "btn btn-lg";
    } else if ((flags & SizeMask) == Small) {
        return "btn btn-sm";
    } else if ((flags & SizeMask) == ExtraSmall) {
        return "btn btn-xs";
    }
    return "btn";
}

void SelectorTitle(IOutputStream& os, const TString& text)
{
    if (!text.empty()) {
        os << text;
    }
}

template <ui64 flags>
void BtnHref(IOutputStream& os, const TString& text, const TString& href, bool push = false)
{
    if (flags & Button) {
        os << "<button type=\"button\" style=\"display: inline-block;margin:3px\" class=\""
           << BtnClass<flags>() << " "
           << (push? "btn-primary": "btn-default")
           << "\" onClick=\"window.location.href='" << href << "';\">"
           << text
           << "</button>";
    } else {
        os << "<a href=\"" << href << "\">"
           << text
           << "</a>";
    }
}

void DropdownBeginSublist(IOutputStream& os, const TString& text)
{
    os << "<li>" << text << "<ul class=\"dropdown-menu\">";
}

void DropdownEndSublist(IOutputStream& os)
{
    os << "</ul></li>";
}

void DropdownItem(IOutputStream& os, const TString& text, const TString& href, bool separated = false)
{
    if (separated) {
        os << "<li role=\"separator\" class=\"divider\"></li>";
    }
    os << "<li><a href=\"" << href << "\">" << text << "</a></li>";
}

TString SuggestSelection()
{
    return "--- ";
}

TString RemoveSelection()
{
    return "Remove";
}

TString GetDescription(const TString& value, const TVariants& variants)
{
    for (const auto& var : variants) {
        if (value == var.first) {
            return var.second;
        }
    }
    if (!value) {
        return SuggestSelection();
    }
    return value;
}

template <ui64 flags, bool sub = false>
void DropdownSelector(IOutputStream& os, const TCgiParameters& e, const TString& param, const TString& value,
                      const TString& text, const TVariants& variants, const TString& realValue = TString())
{
    HTML(os) {
        SelectorTitle(os, text);
        os << "<div class=\"dropdown\" style=\"display:inline-block;margin:3px\">";
            if (flags & Button) {
                os << "<button class=\"" << BtnClass<flags>() << " btn-primary dropdown-toggle\" type=\"button\" data-toggle=\"dropdown\">";
            } else {
                os << "<a href=\"#\" data-toggle=\"dropdown\">";
            }
            os << GetDescription(flags & CompositeValue? realValue: value, variants);
            if (flags & Caret) {
                os << "<span class=\"caret\"></span>";
            }
            if (flags & Button) {
                os <<"</button>";
            } else {
                os <<"</a>";
            }
            UL_CLASS ("dropdown-menu") {
                for (const auto& var : variants) {
                    DropdownItem(os, var.second, UrlReplace<sub>(e, param, value, var.first));
                }
                if (flags & Erasable) {
                    DropdownItem(os, RemoveSelection(), UrlErase<sub>(e, param, value), true);
                }
            }
        os << "</div>";
    }
}

void RequireSelection(TStringStream& ss, const TCgiParameters& e, const TString& param,
                   const TString& text, const TVariants& variants)
{
    const TString& value = e.Get(param);
    DropdownSelector<Link>(ss, e, param, value, text, variants);
    if (!value) {
        throw TPageGen<TSelectorsContainer>(ss.Str());
    }
}

void RequireMultipleSelection(TStringStream& ss, const TCgiParameters& e, const TString& param,
                              const TString& text, const TVariants& variants)
{
    SelectorTitle(ss, text);
    TSet<TString> selectedValues;
    for (const TString& subvalue : Subvalues(e, param)) {
        selectedValues.insert(subvalue);
    }
    for (const TString& subvalue : Subvalues(e, param)) {
        DropdownSelector<Erasable, true>(ss, e, param, subvalue, "", variants);
    }
    if (selectedValues.contains("")) {
        throw TPageGen<TSelectorsContainer>(ss.Str());
    } else {
        BtnHref<Button|ExtraSmall>(ss, "+", MakeUrlAddSub(e, param, ""));
        if (selectedValues.empty()) {
            throw TPageGen<TSelectorsContainer>(ss.Str());
        }
    }
}

void OptionalSelection(TStringStream& ss, const TCgiParameters& e, const TString& param,
                       const TString& text, const TVariants& variants)
{
    TSet<TString> selectedValues;
    for (const TString& subvalue : Subvalues(e, param)) {
        selectedValues.insert(subvalue);
    }
    if (!selectedValues.empty()) {
        SelectorTitle(ss, text);
    }
    for (const TString& subvalue : Subvalues(e, param)) {
        DropdownSelector<Erasable, true>(ss, e, param, subvalue, "", variants);
    }
    if (selectedValues.empty()) {
        BtnHref<Button|ExtraSmall>(ss, text, MakeUrlAddSub(e, param, ""));
    }
}

void OptionalMultipleSelection(TStringStream& ss, const TCgiParameters& e, const TString& param,
                              const TString& text, const TVariants& variants)
{
    TSet<TString> selectedValues;
    for (const TString& subvalue : Subvalues(e, param)) {
        selectedValues.insert(subvalue);
    }
    if (!selectedValues.empty()) {
        SelectorTitle(ss, text);
    }
    for (const TString& subvalue : Subvalues(e, param)) {
        DropdownSelector<Erasable, true>(ss, e, param, subvalue, "", variants);
    }
    if (selectedValues.contains("")) {
        throw TPageGen<TSelectorsContainer>(ss.Str());
    } else {
        BtnHref<Button|ExtraSmall>(ss, selectedValues.empty()? text: "+", MakeUrlAddSub(e, param, ""));
    }
}

TVariants ListColumns(const NAnalytics::TTable& table)
{
    TSet<TString> cols;
//    bool addSpecialCols = false;
//    if (addSpecialCols) {
//        cols.insert("_count");
//    }
    for (auto& row : table) {
        for (auto& kv : row) {
            cols.insert(kv.first);
        }
    }
    TVariants result;
    for (const auto& s : cols) {
        result.emplace_back(s, s);
    }
    return result;
}

TString TaggedValue(const TString& value, const TString& tag)
{
    if (!tag) {
        return value;
    }
    return value + "-" + tag;
}

TVariants ValueVars(const TVariants& values, const TString& tag)
{
    TVariants ret;
    for (auto& p : values) {
        ret.emplace_back(TaggedValue(p.first, tag), p.second);
    }
    return ret;
}

TVariants TagVars(const TString& value, const TVariants& tags)
{
    TVariants ret;
    for (auto& p : tags) {
        ret.emplace_back(TaggedValue(value, p.first), p.second);
    }
    return ret;
}

TVariants SeriesTags()
{
    TVariants ret; // MSVS2013 doesn't understand complex initializer lists
    ret.emplace_back("", "as is");
    ret.emplace_back("stack", "cumulative");
    return ret;
}

void SeriesSelectors(TStringStream& ss, const TCgiParameters& e,
                     const TString& xparam, const TString& yparam, const NAnalytics::TTable& data)
{
    TTags xtags;
    TString xn = ParseTagsOut(e.Get(xparam), xtags);
    DropdownSelector<Erasable, true>(ss, e, xparam, e.Get(xparam), "with Ox:",
                                 ValueVars(ListColumns(data), JoinTags(xtags)));
    if (xn) {
        DropdownSelector<Link, true>(ss, e, xparam, e.Get(xparam), "",
                                     TagVars(xn, SeriesTags()));
    }

    TString yns = e.Get(yparam);
    SelectorTitle(ss, "and Oy:");
    bool first = true;
    bool hasEmpty = false;
    for (auto& subvalue : Subvalues(e, yparam)) {
        TTags ytags;
        TString yn = ParseTagsOut(subvalue, ytags);
        DropdownSelector<Erasable, true>(ss, e, yparam, subvalue, first? "": ", ",
                                         ValueVars(ListColumns(data), JoinTags(ytags)));
        if (yn) {
            DropdownSelector<Link, true>(ss, e, yparam, subvalue, "",
                                         TagVars(yn, SeriesTags()));
        }
        first = false;
        if (yn.empty()) {
            hasEmpty = true;
        }
    }

    if (hasEmpty) {
        throw TPageGen<TSelectorsContainer>(ss.Str());
    } else {
        BtnHref<Button|ExtraSmall>(ss, "+", MakeUrlAddSub(e, yparam, ""));
    }

    if (!xn || !yns) {
        throw TPageGen<TSelectorsContainer>(ss.Str());
    }
}

class TProbesHtmlPrinter {
private:
    TVector<TVector<TString>> TableData;
    static constexpr int TimeoutSec = 15 * 60; // default timeout
public:
    void Push(const NLWTrace::TProbe* probe)
    {
        TableData.emplace_back();
        auto& row = TableData.back();

        row.emplace_back();
        TString& groups = row.back();
        bool first = true;
        for (const char* const* i = probe->Event.Groups; *i != nullptr; ++i, first = false) {
            groups.append(TString(first? "": ", ") + GroupHtml(*i));
        }

        row.push_back(ProbeHtml(probe->Event.GetProvider(), probe->Event.Name));

        row.emplace_back();
        TString& params = row.back();
        first = true;
        for (size_t i = 0; i < probe->Event.Signature.ParamCount; i++, first = false) {
            params.append(TString(first? "": ", ") + probe->Event.Signature.ParamTypes[i]
                          + " " + probe->Event.Signature.ParamNames[i]);
        }

        row.emplace_back(ToString(probe->GetExecutorsCount()));
    }

    void Output(IOutputStream& os)
    {
        HTML(os) {
            TABLE() {
                TABLEHEAD() {
                    TABLEH() { os << "Groups"; }
                    TABLEH() { os << "Name"; }
                    TABLEH() { os << "Params"; }
                    TABLEH() { os << "ExecCount"; }
                }
                TABLEBODY() {
                    for (auto& row : TableData) {
                        TABLER() {
                            for (TString& cell : row) {
                                TABLED() { os << cell; }
                            }
                        }
                    }
                }
            }
        }
    }
private:
    TString GroupHtml(const TString& group)
    {
        TStringStream ss;
        ss << "<div class=\"dropdown\" style=\"display:inline-block\">"
                 "<a href=\"#\" data-toggle=\"dropdown\">" << group << "</a>"
                 "<ul class=\"dropdown-menu\">"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group).Id() << "'});\">"
                     "Trace 1000 items</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group, 10000).Id() << "'});\">"
                     "Trace 10000 items</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group, 0, 1000000).Id() << "'});\">"
                     "Trace 1 second</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group, 0, 10000000).Id() << "'});\">"
                     "Trace 10 seconds</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group, 0, 0, true).Id() << "'});\">"
                     "Trace 1000 tracks</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(group, 10000, 0, true).Id() << "'});\">"
                     "Trace 10000 tracks</a></li>"
                 "</ul>"
           "</div>";
        return ss.Str();
    }

    TString ProbeHtml(const TString& provider, const TString& probe)
    {
        TStringStream ss;
        ss << "<div class=\"dropdown\">"
                 "<a href=\"#\" data-toggle=\"dropdown\">" << probe << "</a>"
                 "<ul class=\"dropdown-menu\">"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe).Id() << "'});\">"
                     "Trace 1000 items</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe, 10000).Id() << "'});\">"
                     "Trace 10000 items</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe, 0, 1000000).Id() << "'});\">"
                     "Trace 1 second</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe, 0, 10000000).Id() << "'});\">"
                     "Trace 10 seconds</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe, 0, 0, true).Id() << "'});\">"
                     "Trace 1000 tracks</a></li>"
                   "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=new&ui=y&timeout=" << TimeoutSec << "',"
                     "{id:'" << TAdHocTraceConfig(provider, probe, 10000, 0, true).Id() << "'});\">"
                     "Trace 10000 tracks</a></li>"
                 "</ul>"
           "</div>";
        return ss.Str();
    }
};

void TDashboardRegistry::Register(const NLWTrace::TDashboard& dashboard) {
    TGuard<TMutex> g(Mutex);
    Dashboards[dashboard.GetName()] = dashboard;
}

void TDashboardRegistry::Register(const TVector<NLWTrace::TDashboard>& dashboards) {
    for (const auto& dashboard : dashboards) {
        Register(dashboard);
    }
}

void TDashboardRegistry::Register(const TString& dashText) {
    NLWTrace::TDashboard dash;
    if (!google::protobuf::TextFormat::ParseFromString(dashText, &dash)) {
        ythrow yexception() << "Couldn't parse into dashboard";
    }
    Register(dash);
}

bool TDashboardRegistry::Get(const TString& name, NLWTrace::TDashboard& dash) {
    TGuard<TMutex> g(Mutex);
    if (!Dashboards.contains(name)) {
        return false;
    }
    dash = Dashboards[name];
    return true;
}

void TDashboardRegistry::Output(TStringStream& ss) {
    HTML(ss) {
        TABLE() {
            TABLEHEAD() {
                TABLEH() { ss << "Name"; }
            }
            TABLEBODY() {
                TGuard<TMutex> g(Mutex);
                for (auto& kv : Dashboards) {
                    const auto& dash = kv.second;
                    TABLER() {
                        TABLED() {
                            ss << "<a href='?mode=dashboard&name=" << dash.GetName() << "'>" << dash.GetName() << "</a>";
                        }
                    }
                }
            }
        }
    }
}

class ILogSource {
public:
    virtual ~ILogSource() {}
    virtual TString GetId() = 0;
    virtual TInstant GetStartTime() = 0;
    virtual TDuration GetTimeout(TInstant now) = 0;
    virtual ui64 GetEventsCount() = 0;
    virtual ui64 GetThreadsCount() = 0;
};

class TTraceLogSource : public ILogSource {
private:
    TString Id;
    const TTraceCleaner& Cleaner;
    const NLWTrace::TSession* Trace;
public:
    TTraceLogSource(const TString& id, const NLWTrace::TSession* trace, const TTraceCleaner& cleaner)
        : Id(id)
        , Cleaner(cleaner)
        , Trace(trace)
    {}

    TString GetId() override
    {
        return Id;
    }

    TInstant GetStartTime() override
    {
        return Trace->GetStartTime();
    }

    TDuration GetTimeout(TInstant now) override
    {
        TInstant deadline = Cleaner.GetDeadline(Id);
        if (deadline < now) {
            return TDuration::Zero();
        } else if (deadline == TInstant::Max()) {
            return TDuration::Max();
        } else {
            return deadline - now;
        }
    }

    ui64 GetEventsCount() override
    {
        return Trace->GetEventsCount();
    }

    ui64 GetThreadsCount() override
    {
        return Trace->GetThreadsCount();
    }
};

class TSnapshotLogSource : public ILogSource {
private:
    TString Sid;
    // Log should be used for read-only purpose, because it can be accessed from multiple threads
    // Atomic pointer is used to avoid thread-safety issues with snapshot deletion
    // (I hope protobuf const-implementation doesn't use any mutable non-thread-safe stuff inside)
    TAtomicSharedPtr<NLWTrace::TLogPb> Log;
public:
    // Constructor should be called under SnapshotsMtx lock
    TSnapshotLogSource(const TString& sid, const TAtomicSharedPtr<NLWTrace::TLogPb>& log)
        : Sid(sid)
        , Log(log)
    {}

    TString GetId() override
    {
        return Sid + "~";
    }

    TInstant GetStartTime() override
    {
        return TInstant::MicroSeconds(Log->GetCrtTime());
    }

    TDuration GetTimeout(TInstant now) override
    {
        Y_UNUSED(now);
        return TDuration::Max();
    }

    ui64 GetEventsCount() override
    {
        return Log->GetEventsCount();
    }

    ui64 GetThreadsCount() override
    {
        return Log->ThreadLogsSize();
    }
};

class TLogSources {
private:
    TTraceCleaner& Cleaner;
    TInstant Now;
    using TLogSourcePtr = std::unique_ptr<ILogSource>;
    TMap<TString, TLogSourcePtr> LogSources;
public:
    explicit TLogSources(TTraceCleaner& cleaner, TInstant now = TInstant::Now())
        : Cleaner(cleaner)
        , Now(now)
    {}

    void Push(const TString& sid, const TAtomicSharedPtr<NLWTrace::TLogPb>& log)
    {
        TLogSourcePtr ls(new TSnapshotLogSource(sid, log));
        LogSources.emplace(ls->GetId(), std::move(ls));
    }

    void Push(const TString& id, const NLWTrace::TSession* trace)
    {
        TLogSourcePtr ls(new TTraceLogSource(id, trace, Cleaner));
        LogSources.emplace(ls->GetId(), std::move(ls));
    }

    template <class TFunc>
    void ForEach(TFunc& func)
    {
        for (auto& kv : LogSources) {
            func.Push(kv.second.get());
        }
    }

    template <class TFunc>
    void ForEach(TFunc& func) const
    {
        for (const auto& kv : LogSources) {
            func.Push(kv.second.get());
        }
    }
};

class TTracesHtmlPrinter {
private:
    IOutputStream& Os;
    TInstant Now;
public:
    explicit TTracesHtmlPrinter(IOutputStream& os)
        : Os(os)
        , Now(TInstant::Now())
    {}

    void Push(ILogSource* src)
    {
        TString id = src->GetId();
        Os << "<tr>";
        Os << "<td>";
        try {
            Os << src->GetStartTime().ToStringUpToSeconds();
        } catch (...) {
            Os << "error: " << EncodeHtmlPcdata(CurrentExceptionMessage());
        }
        Os << "</td>"
           << "<td><div class=\"dropdown\">"
                "<a href=\"#\" data-toggle=\"dropdown\">" << TimeoutToString(src->GetTimeout(Now)) << "</a>"
                "<ul class=\"dropdown-menu\">"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y&timeout=60', {id:'" << id << "'});\">1 min</a></li>"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y&timeout=600', {id:'" << id << "'});\">10 min</a></li>"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y&timeout=3600', {id:'" << id << "'});\">1 hour</a></li>"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y&timeout=86400', {id:'" << id << "'});\">1 day</a></li>"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y&timeout=604800', {id:'" << id << "'});\">1 week</a></li>"
                  "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=settimeout&ui=y', {id:'" << id << "'});\">no timeout</a></li>"
                "</ul>"
              "</div></td>"
           << "<td>" << EncodeHtmlPcdata(id) << "</td>"
           << "<td>" << src->GetEventsCount() << "</td>"
           << "<td>" << src->GetThreadsCount() << "</td>"
           << "<td><a href=\"?mode=log&id=" << id << "\">Text</a></td>"
           << "<td><a href=\"?mode=log&format=json&id=" << id << "\">Json</a></td>"
           << "<td><a href=\"?mode=query&id=" << id << "\">Query</a></td>"
           << "<td><a href=\"?mode=analytics&id=" << id << "\">Analytics</a></td>"
           << "<td><div class=\"dropdown navbar-right\">" // navbar-right is hack to drop left
                    "<a href=\"#\" data-toggle=\"dropdown\">Modify</a>"
                    "<ul class=\"dropdown-menu\">"
                      "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=make_snapshot&ui=y', {id:'" << id << "'});\">Snapshot</a></li>"
                      "<li><a href=\"#\" onClick=\"$.redirectPost('?mode=delete&ui=y', {id:'" << id << "'});\">Delete</a></li>"
                    "</ul>"
              "</div></td>"
           << "</tr>\n";
    }
private:
    static TString TimeoutToString(TDuration d)
    {
        TStringStream ss;
        if (d == TDuration::Zero()) {
            ss << "0";
        } else if (d == TDuration::Max()) {
            ss << "-";
        } else {
            ui64 us = d.GetValue();
            ui64 ms = us / 1000;
            ui64 sec = ms / 1000;
            ui64 min = sec / 60;
            ui64 hours = min / 60;
            ui64 days = hours / 24;
            ui64 weeks = days / 7;
            us -= ms * 1000;
            ms -= sec * 1000;
            sec -= min * 60;
            min -= hours * 60;
            hours -= days * 24;
            days -= weeks * 7;
            int terms = 0;
            if ((terms > 0 && terms < 2) || ( terms == 0 && weeks)) { ss << (ss.Str()? " ": "") << weeks << "w"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && days))  { ss << (ss.Str()? " ": "") << days  << "d"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && hours)) { ss << (ss.Str()? " ": "") << hours << "h"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && min))   { ss << (ss.Str()? " ": "") << min   << "m"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && sec))   { ss << (ss.Str()? " ": "") << sec   << "s"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && ms))    { ss << (ss.Str()? " ": "") << ms   << "ms"; terms++; }
            if ((terms > 0 && terms < 2) || ( terms == 0 && us))    { ss << (ss.Str()? " ": "") << us   << "us"; terms++; }
        }
        return ss.Str();
    }
};

class TTracesLister {
private:
    TVariants& Variants;
public:
    TTracesLister(TVariants& variants)
        : Variants(variants)
    {}
    void Push(ILogSource* src)
    {
        Variants.emplace_back(src->GetId(), src->GetId());
    }
};

TVariants ListTraces(const TLogSources& srcs)
{
    TVariants variants;
    TTracesLister lister(variants);
    srcs.ForEach(lister);
    return variants;
}

class TTimestampCutter {
private:
    THashMap<TThread::TId, std::pair<ui64, TInstant>> CutTsForThread; // tid -> time of first item
    mutable ui64 CutTsMax = 0;
    mutable TInstant CutInstantMax;
    bool Enabled;
    ui64 NowTs;
public:
    explicit TTimestampCutter(bool enabled)
        : Enabled(enabled)
        , NowTs(GetCycleCount())
    {}

    void Push(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        auto it = CutTsForThread.find(tid);
        if (it != CutTsForThread.end()) {
            ui64& ts = it->second.first;
            TInstant& inst = it->second.second;
            ts = Min(ts, item.TimestampCycles);
            inst = Min(inst, item.Timestamp);
        } else {
            CutTsForThread[tid] = std::make_pair(item.TimestampCycles, item.Timestamp);
        }
    }

    // Timestamp from which we are ensured that cyclic log for every thread is not truncated
    // NOTE: should NOT be called from Push(tid, item) functions
    ui64 StartTimestamp() const
    {
        if (CutTsMax == 0) {
            FindStartTime();
        }
        return CutTsMax;
    }

    ui64 NowTimestamp() const
    {
        return NowTs;
    }

    TInstant StartInstant() const
    {
        if (CutInstantMax == TInstant::Zero()) {
            FindStartTime();
        }
        return CutInstantMax;
    }

    // Returns true iff item should be skipped to avoid surprizes
    bool Skip(const NLWTrace::TLogItem& item) const
    {
        return Enabled && item.TimestampCycles < StartTimestamp();
    }

private:
    void FindStartTime() const
    {
        for (auto& kv : CutTsForThread) {
            CutTsMax = Max(CutTsMax, kv.second.first);
            CutInstantMax = Max(CutInstantMax, kv.second.second);
        }
    }
};

class TLogFilter {
private:
    struct TFilter {
        TString ParamName;
        TString ParamValue;
        bool Parsed;

        TLogQuery Query;
        NLWTrace::TLiteral Value;

        explicit TFilter(const TString& text)
        {
            if (!text) { // Neither ParamName nor ParamValue is selected
                ParamName.clear();
                ParamValue.clear();
                Parsed = false;
                return;
            }
            size_t pos = text.find('=');
            if (pos == TString::npos) { // Only ParamName has been selected
                ParamName = text;
                ParamValue.clear();
                Parsed = false;
                return;
            }
            // Both ParamName and ParamValue have been selected
            ParamValue = text.substr(pos + 1);
            ParamName = text.substr(0, pos);
            Parsed = true;

            Query = TLogQuery(ParamName);
            Value = NLWTrace::TLiteral(ParamValue);
        }
    };
    TVector<TFilter> Filters;
    THashSet<const NLWTrace::TSignature*> Signatures; // Just to list param names
    TVariants ParamNames;
    THashMap<TString, THashSet<TString>> FilteredParamValues; // paramName -> { paramValue }
public:
    explicit TLogFilter(const TVector<TString>& filters)
    {
        for (const TString& subvalue : filters) {
            TFilter filter(subvalue);
            FilteredParamValues[filter.ParamName]; // just create empty set to gather values later
            if (filter.Parsed) {
                Filters.push_back(filter);
            }
        }
    }

    virtual ~TLogFilter() {}

    template <class TLog>
    bool Filter(const TLog& log)
    {
        Gather(log);
        for (const TFilter& filter : Filters) {
            if (filter.Query.ExecuteQuery(log) != filter.Value) {
                return false;
            }
        }
        return true;
    }

    void FilterSelectors(TStringStream& ss, const TCgiParameters& e, const TString& fparam)
    {
        bool first = true;
        bool allParsed = true;
        for (const TString& subvalue : Subvalues(e, fparam)) {
            TFilter filter(subvalue);
            allParsed = allParsed && filter.Parsed;
            if (first) {
                SelectorTitle(ss, "where");
            }
            DropdownSelector<Erasable | CompositeValue, true>(
                ss, e, fparam, subvalue, first? "": ", ", ListParamNames(),
                filter.ParamName
            );
            if (filter.ParamName) {
                DropdownSelector<Link | CompositeValue, true>(
                    ss, e, fparam, subvalue, "=", ListParamValues(filter.ParamName),
                    filter.ParamValue? (filter.ParamName + "=" + filter.ParamValue): ""
                );
            }
            first = false;
        }

        if (!allParsed) {
            throw TPageGen<TSelectorsContainer>(ss.Str());
        } else {
            BtnHref<Button|ExtraSmall>(ss, first? "where": "+", MakeUrlAddSub(e, fparam, ""));
        }
    }

    const TVariants& ListParamNames()
    {
        if (ParamNames.empty()) {
            THashSet<TString> paramNames;
            for (const NLWTrace::TSignature* sgn: Signatures) {
                for (size_t pi = 0; pi < sgn->ParamCount; pi++) {
                    paramNames.insert(sgn->ParamNames[pi]);
                }
            }
            for (auto& pn : paramNames) {
                ParamNames.emplace_back(pn, pn);
            }
        }
        return ParamNames;
    }

    bool IsFiltered(const TString& paramName) const
    {
        return FilteredParamValues.contains(paramName);
    }

private:
    // Gather param names and values for selectors
    void Gather(const NLWTrace::TLogItem& item)
    {
        Signatures.insert(&item.Probe->Event.Signature);
        if (!FilteredParamValues.empty() && item.SavedParamsCount > 0) {
            TString paramValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            for (size_t pi = 0; pi < item.SavedParamsCount; pi++) {
                auto iter = FilteredParamValues.find(item.Probe->Event.Signature.ParamNames[pi]);
                if (iter != FilteredParamValues.end()) {
                    iter->second.insert(paramValues[pi]);
                }
            }
        }
    }

    void Gather(const NLWTrace::TTrackLog& tl)
    {
        for (const NLWTrace::TLogItem& item : tl.Items) {
            Gather(item);
        }
    }

    TVariants ListParamValues(const TString& paramName) const
    {
        TVariants result;
        auto iter = FilteredParamValues.find(paramName);
        if (iter != FilteredParamValues.end()) {
            for (const TString& paramValue : iter->second) {
                result.emplace_back(paramName + "=" + paramValue, paramValue);
            }
        }
        Sort(result.begin(), result.end());
        return result;
    }
};

static void EscapeJSONString(IOutputStream& os, const TString& s)
{
    for (TString::const_iterator i = s.begin(), e = s.end(); i != e; ++i) {
        char c = *i;
        if (c < ' ') {
            os << Sprintf("\\u%04x", int(c));
        } else if (c == '"') {
            os << "\\\"";
        } else if (c == '\\') {
            os << "\\\\";
        } else {
            os << c;
        }
    }
}

static TString EscapeJSONString(const TString& s)
{
    TStringStream ss;
    EscapeJSONString(ss, s);
    return ss.Str();
}

class TLogJsonPrinter {
private:
    IOutputStream& Os;
    bool FirstThread;
    bool FirstItem;
public:
    explicit TLogJsonPrinter(IOutputStream& os)
        : Os(os)
        , FirstThread(true)
        , FirstItem(true)
    {}

    void OutputHeader()
    {
        Os << "{\n\t\"source\": \"" << HostName() << "\""
              "\n\t, \"items\": ["
              ;
    }

    void OutputFooter(const NLWTrace::TSession* trace)
    {
        Os << "\n\t\t]"
              "\n\t, \"threads\": ["
              ;
        trace->ReadThreads(*this);
        Os << "]"
              "\n\t, \"events_count\": " << trace->GetEventsCount() <<
              "\n\t, \"threads_count\": " << trace->GetThreadsCount() <<
              "\n\t, \"timestamp\": " << Now().GetValue() <<
              "\n}"
              ;
    }

    void PushThread(TThread::TId tid)
    {
        Os << (FirstThread? "": ", ") << tid;
        FirstThread = false;
    }

    void Push(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        Os << "\n\t\t" << (FirstItem? "": ", ");
        FirstItem = false;

        Os << "[" << tid <<
              ", " << item.Timestamp.GetValue() <<
              ", \"" << item.Probe->Event.GetProvider() << "\""
              ", \"" << item.Probe->Event.Name << "\""
              ", {"
              ;
        if (item.SavedParamsCount > 0) {
            TString ParamValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, ParamValues);
            bool first = true;
            for (size_t i = 0; i < item.SavedParamsCount; i++, first = false) {
                Os << (first? "": ", ") << "\"" << item.Probe->Event.Signature.ParamNames[i] << "\": \"";
                EscapeJSONString(Os, ParamValues[i]);
                Os << "\"";
            }
        }
        Os << "}]";
    }
};

class TLogTextPrinter : public TLogFilter {
private:
    TMultiMap<NLWTrace::TTypedParam, std::pair<TThread::TId, NLWTrace::TLogItem> > Items;
    TMultiMap<NLWTrace::TTypedParam, NLWTrace::TTrackLog> Depot;
    THashMap<NLWTrace::TProbe*, size_t> ProbeId;
    TVector<NLWTrace::TProbe*> Probes;
    TTimestampCutter CutTs;
    TLogQuery Order;
    bool ReverseOrder = false;
    ui64 Head = 0;
    ui64 Tail = 0;
    bool ShowTs = false;
public:
    TLogTextPrinter(const TVector<TString>& filters, ui64 head, ui64 tail, const TString& order, bool reverseOrder, bool cutTs, bool showTs)
        : TLogFilter(filters)
        , CutTs(cutTs)
        , Order(order)
        , ReverseOrder(reverseOrder)
        , Head(head)
        , Tail(tail)
        , ShowTs(showTs)
    {}

    TLogTextPrinter(const TCgiParameters& e)
        : TLogTextPrinter(
            Subvalues(e, "f"),
            e.Has("head")? FromString<ui64>(e.Get("head")): 0,
            e.Has("tail")? FromString<ui64>(e.Get("tail")): 0,
            e.Get("s"),
            e.Get("reverse") == "y",
            e.Get("cutts") == "y",
            e.Get("showts") == "y")
    {}

    enum EFormat {
        Text,
        Json
    };

    void Output(IOutputStream& os) const
    {
        OutputItems<Text>(os);
        OutputDepot<Text>(os);
    }

    void OutputJson(IOutputStream& os) const
    {
        os << "{\"depot\":[\n";
        OutputItems<Json>(os);
        OutputDepot<Json>(os);
        os << "],\"probes\":[";
        bool first = true;
        for (const NLWTrace::TProbe* probe : Probes) {
            os << (first? "": ",") << "{\"provider\":\"" << probe->Event.GetProvider()
                << "\",\"name\":\"" << probe->Event.Name << "\"}";
            first = false;
        }
        os << "]}";
    }

    NLWTrace::TTypedParam GetKey(const NLWTrace::TLogItem& item)
    {
        return Order? Order.ExecuteQuery(item): NLWTrace::TTypedParam(item.GetTimestampCycles());
    }

    NLWTrace::TTypedParam GetKey(const NLWTrace::TTrackLog& tl)
    {
        return Order? Order.ExecuteQuery(tl): NLWTrace::TTypedParam(tl.GetTimestampCycles());
    }

    void Push(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        CutTs.Push(tid, item);
        if (Filter(item)) {
            AddId(item);
            Items.emplace(GetKey(item), std::make_pair(tid, item));
        }
    }

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl)
    {
        Y_UNUSED(tid);
        if (Filter(tl)) {
            AddId(tl);
            Depot.emplace(GetKey(tl), tl);
        }
    }

private:
    void AddId(const NLWTrace::TLogItem& item)
    {
        if (ProbeId.find(item.Probe) == ProbeId.end()) {
            size_t id = Probes.size();
            ProbeId[item.Probe] = id;
            Probes.emplace_back(item.Probe);
        }
    }

    void AddId(const NLWTrace::TTrackLog& tl)
    {
        for (const auto& item : tl.Items) {
            AddId(item);
        }
    }

    bool HeadTailFilter(ui64 idx, ui64 size) const
    {
        bool headOk = idx < Head;
        bool tailOk = size < Tail + idx + 1ull;
        if (Head && Tail) {
            return headOk || tailOk;
        } else if (Head) {
            return headOk;
        } else if (Tail) {
            return tailOk;
        } else {
            return true;
        }
    }

    template <EFormat Format>
    void OutputItems(IOutputStream& os) const
    {
        ui64 idx = 0;
        ui64 size = Items.size();
        ui64 startTs = ShowTs? CutTs.StartTimestamp(): 0;
        ui64 prevTs = 0;
        bool first = true;
        if (!ReverseOrder) {
            for (auto i = Items.begin(), e = Items.end(); i != e; ++i, idx++) {
                if (HeadTailFilter(idx, size)) {
                    OutputItem<Format, true>(os, i->second.first, i->second.second, startTs, prevTs, first);
                    prevTs = startTs? i->second.second.GetTimestampCycles(): 0;
                }
            }
        } else {
            for (auto i = Items.rbegin(), e = Items.rend(); i != e; ++i, idx++) {
                if (HeadTailFilter(idx, size)) {
                    OutputItem<Format, true>(os, i->second.first, i->second.second, startTs, prevTs, first);
                    prevTs = startTs? i->second.second.GetTimestampCycles(): 0;
                }
            }
        }
    }

    template <EFormat Format>
    void OutputDepot(IOutputStream& os) const
    {
        ui64 idx = 0;
        ui64 size = Depot.size();
        bool first = true;
        if (!ReverseOrder) {
            for (auto i = Depot.begin(), e = Depot.end(); i != e; ++i, idx++) {
                if (HeadTailFilter(idx, size)) {
                    OutputTrackLog<Format>(os, i->second, first);
                }
            }
        } else {
            for (auto i = Depot.rbegin(), e = Depot.rend(); i != e; ++i, idx++) {
                if (HeadTailFilter(idx, size)) {
                    OutputTrackLog<Format>(os, i->second, first);
                }
            }
        }
    }

    template <EFormat Format, bool AsTrack = false>
    void OutputItem(IOutputStream& os, TThread::TId tid, const NLWTrace::TLogItem& item, ui64 startTs, ui64 prevTs, bool& first) const
    {
        if (CutTs.Skip(item)) {
            return;
        }
        if constexpr (Format == Text) {
            if (startTs) {
                if (!prevTs) {
                    prevTs = item.GetTimestampCycles();
                }
                os << Sprintf("%10.3lf %+10.3lf ms ",
                    NHPTimer::GetSeconds(item.GetTimestampCycles() - startTs) * 1000.0,
                    NHPTimer::GetSeconds(item.GetTimestampCycles() - prevTs) * 1000.0);
            }
            if (tid) {
                os << "<" << tid << "> ";
            }
            if (item.Timestamp != TInstant::Zero()) {
                os << "[" << item.Timestamp << "] ";
            } else {
                os << "[" << item.TimestampCycles << "] ";
            }
            os << GetProbeName(item.Probe) << "(";
            if (item.SavedParamsCount > 0) {
                TString ParamValues[LWTRACE_MAX_PARAMS];
                item.Probe->Event.Signature.SerializeParams(item.Params, ParamValues);
                bool first = true;
                for (size_t i = 0; i < item.SavedParamsCount; i++, first = false) {
                    os << (first? "": ", ") << item.Probe->Event.Signature.ParamNames[i] << "='" << EscapeC(ParamValues[i]) << "'";
                }
            }
            os << ")\n";
        } else if constexpr (Format == Json) {
            if (auto probeId = ProbeId.find(item.Probe); probeId != ProbeId.end()) {
                os << (first? "": ",") << (AsTrack? "[":"") << "[\"" << tid << "\",\"";
                if (item.Timestamp != TInstant::Zero()) {
                    os << item.Timestamp.MicroSeconds();
                } else {
                    os << Sprintf("%.3lf", NHPTimer::GetSeconds(item.TimestampCycles) * 1e9);
                }
                os << "\"," << probeId->second << ",{";
                if (item.SavedParamsCount > 0) {
                    TString ParamValues[LWTRACE_MAX_PARAMS];
                    item.Probe->Event.Signature.SerializeParams(item.Params, ParamValues);
                    bool first = true;
                    for (size_t i = 0; i < item.SavedParamsCount; i++, first = false) {
                        os << (first? "": ",") << "\"" << item.Probe->Event.Signature.ParamNames[i] << "\":\"";
                        EscapeJSONString(os, ParamValues[i]);
                        os << "\"";
                    }
                }
                os << "}]" << (AsTrack? "]":"");
            }
        }
        first = false;
    }

    template <EFormat Format>
    void OutputTrackLog(IOutputStream& os, const NLWTrace::TTrackLog& tl, bool& first) const
    {
        if constexpr (Format == Json) {
            os << (first? "": ",") << "[";
        }
        first = false;
        ui64 prevTs = tl.GetTimestampCycles();
        bool firstItem = true;
        for (const NLWTrace::TTrackLog::TItem& item: tl.Items) {
            OutputItem<Format>(os, item.ThreadId, item, tl.GetTimestampCycles(), prevTs, firstItem);
            prevTs = item.GetTimestampCycles();
        }
        if constexpr (Format == Json) {
            os << "]";
        }
        os << "\n";
    }
};

class TLogAnalyzer: public TLogFilter {
private:
    TMultiMap<ui64, std::pair<TThread::TId, NLWTrace::TLogItem>> Items;
    TVector<NLWTrace::TTrackLog> Depot;
    THashMap<TString, TTrackLogRefs> Groups;
    NAnalytics::TTable Table;
    bool TableCreated = false;
    TVector<TString> GroupBy;
    TTimestampCutter CutTs;
public:
    TLogAnalyzer(const TVector<TString>& filters, const TVector<TString>& groupBy, bool cutTs)
        : TLogFilter(filters)
        , CutTs(cutTs)
    {
        for (const TString& groupParam : groupBy) {
            GroupBy.push_back(groupParam);
        }
    }

    const NAnalytics::TTable& GetTable()
    {
        if (!TableCreated) {
            TableCreated = true;
            if (GroupBy.empty()) {
                for (auto i = Items.begin(), e = Items.end(); i != e; ++i) {
                    ParseItems(i->second.first, i->second.second);
                }
                ParseDepot();
            } else {
                for (auto i = Items.begin(), e = Items.end(); i != e; ++i) {
                    Map(i->second.first, i->second.second);
                }
                Reduce();
            }
        }
        return Table;
    }

    void Push(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        CutTs.Push(tid, item);
        if (Filter(item)) {
            Items.emplace(item.TimestampCycles, std::make_pair(tid, item));
        }
    }

    void Push(TThread::TId, const NLWTrace::TTrackLog& tl)
    {
        if (Filter(tl)) {
            Depot.emplace_back(tl);
        }
    }
private:
    void FillRow(NAnalytics::TRow& row, const NLWTrace::TLogItem& item)
    {
        if (item.SavedParamsCount > 0) {
            TString paramValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            for (size_t i = 0; i < item.SavedParamsCount; i++) {
                double value = FromString<double>(paramValues[i].data(), paramValues[i].size(), NAN);
                // If value cannot be cast to double or is inf/nan -- assume it's a string
                if (isfinite(value)) {
                    row[item.Probe->Event.Signature.ParamNames[i]] = value;
                } else {
                    row[item.Probe->Event.Signature.ParamNames[i]] = paramValues[i];
                }
            }
        }
    }

    TString GetParam(const NLWTrace::TLogItem& item, TString* paramValues, const TString& paramName)
    {
        for (size_t pi = 0; pi < item.SavedParamsCount; pi++) {
            if (paramName == item.Probe->Event.Signature.ParamNames[pi]) {
                return paramValues[pi];
            }
        }
        return TString();
    }

    TString GetGroup(const NLWTrace::TLogItem& item, TString* paramValues)
    {
        TStringStream ss;
        bool first = true;
        for (const TString& groupParam : GroupBy) {
            ss << (first? "": "|") << GetParam(item, paramValues, groupParam);
            first = false;
        }
        return ss.Str();
    }

    void ParseItems(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        if (CutTs.Skip(item)) {
            return;
        }
        Table.emplace_back();
        NAnalytics::TRow& row = Table.back();
        row["_thread"] = tid;
        if (item.Timestamp != TInstant::Zero()) {
            row["_wallTime"] = item.Timestamp.SecondsFloat();
            row["_wallRTime"] = item.Timestamp.SecondsFloat() - CutTs.StartInstant().SecondsFloat();
        }
        row["_cycles"] = item.TimestampCycles;
        row["_thrTime"] = CyclesToDuration((ui64)item.TimestampCycles).SecondsFloat();
        row["_thrRTime"] = double(i64(item.TimestampCycles) - i64(CutTs.StartTimestamp())) / NHPTimer::GetCyclesPerSecond();
        row["_thrNTime"] = double(i64(item.TimestampCycles) - i64(CutTs.NowTimestamp())) / NHPTimer::GetCyclesPerSecond();
        row.Name = GetProbeName(item.Probe);
        FillRow(row, item);
    }

    void Map(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        if (item.SavedParamsCount > 0 && !CutTs.Skip(item)) {
            TString paramValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            TTrackLogRefs& tl = Groups[GetGroup(item, paramValues)];
            tl.Items.emplace_back(tid, item);
        }
    }

    void Reduce()
    {
        for (auto& v : Groups) {
            const TString& group = v.first;
            const TTrackLogRefs& tl = v.second;
            Table.emplace_back();
            NAnalytics::TRow& row = Table.back();
            row.Name = group;
            for (const NLWTrace::TLogItem& item : tl.Items) {
               FillRow(row, item);
            }
        }
    }

    void ParseDepot()
    {
        for (NLWTrace::TTrackLog& tl : Depot) {
            Table.emplace_back();
            NAnalytics::TRow& row = Table.back();
            for (const NLWTrace::TLogItem& item : tl.Items) {
                FillRow(row, item);
            }
        }
    }
};

struct TSampleOpts {
    bool ShowProvider = false;
    size_t SizeLimit = 50;
};

enum ENodeType {
    NT_ROOT,
    NT_PROBE,
    NT_PARAM
};

class TPatternTree;
struct TPatternNode;

struct TTrack : public TTrackLogRefs {
    TString TrackId;
    TPatternNode* LastNode = nullptr;
};

using TTrackTr = TLogTraits<TTrackLogRefs>;
using TTrackIter = TTrackTr::const_iterator;

// Visitor for tree traversing
class IVisitor {
public:
    virtual ~IVisitor() {}
    virtual void Visit(TPatternNode* node) = 0;
};

// Per-node classifier
class TClassifier {
public:
    explicit TClassifier(TPatternNode* node, ENodeType childType, bool keepHead = false)
        : Node(node)
        , KeepHead(keepHead)
        , ChildType(childType)
    {}
    virtual ~TClassifier() {}
    virtual TPatternNode* Classify(TTrackIter cur, const TTrack& track) = 0;
    virtual void Accept(IVisitor* visitor) = 0;
    virtual bool IsLeaf() = 0;
    ENodeType GetChildType() const { return ChildType; }
public:
    TPatternNode* Node;
    const bool KeepHead;
    ENodeType ChildType;
};

// Track classification tree node
struct TPatternNode {
    TString Name;
    TPatternNode* Parent = nullptr;
    THolder<TClassifier> Classifier;
    struct TDesc {
        ENodeType Type = NT_ROOT;
        // NT_PROBE
        const NLWTrace::TProbe* Probe = nullptr;
        // NT_PARAM
        size_t Rollbacks = 0;
        TString ParamName;
        TString ParamValue;
    } Desc;

    ui64 TrackCount = 0;
    struct TTrackEntry {
        TTrack* Track;
        ui64 ResTotal;
        ui64 ResLast;

        TTrackEntry(TTrack* track, ui64 resTotal, ui64 resLast)
            : Track(track)
            , ResTotal(resTotal)
            , ResLast(resLast)
        {}
    };

    TVector<TTrackEntry> Tracks;

    ui64 ResTotalSum = 0;
    ui64 ResTotalMax = 0;
    TVector<ui64> ResTotalAll;

    ui64 ResLastSum = 0;
    ui64 ResLastMax = 0;
    TVector<ui64> ResLastAll;

    TVector<ui64> TimelineSum;
    NAnalytics::TTable Slices;

    TString GetPath() const
    {
        if (Parent) {
            return Parent->GetPath() + Name;
        }
        return "/";
    }

    NAnalytics::TTable GetTable() const
    {
        using namespace NAnalytics;
        NAnalytics::TTable ret;
        for (ui64 x : ResTotalAll) {
            ret.emplace_back();
            TRow& row = ret.back();
            row["resTotal"] = double(x) * 1000.0 / NHPTimer::GetClockRate();
        }
        for (ui64 x : ResLastAll) {
            ret.emplace_back();
            TRow& row = ret.back();
            row["resLast"] = double(x) * 1000.0 / NHPTimer::GetClockRate();
        }
        return ret;
    }

    template <typename TReader>
    void OutputSample(const TString& bn, double b1, double b2, const TSampleOpts& opts, TReader& reader) const
    {
        bool filterTotal = false;
        if (bn == "resTotal") {
            filterTotal = true;
        } else {
            WWW_CHECK(bn == "resLast", "wrong sample filter param: %s", bn.data());
        }

        size_t spaceLeft = opts.SizeLimit;
        for (const TTrackEntry& entry : Tracks) {
            const TTrack* track = entry.Track;
            // Filter out tracks that are not in sample
            if (filterTotal) {
                double resTotalMs = double(entry.ResTotal) * 1000.0 / NHPTimer::GetClockRate();
                if (resTotalMs < b1 || resTotalMs > b2) {
                    continue;
                }
            } else {
                double resLastMs = double(entry.ResLast) * 1000.0 / NHPTimer::GetClockRate();
                if (resLastMs < b1 || resLastMs > b2) {
                    continue;
                }
            }

            NLWTrace::TTrackLog tl;
            for (TTrackIter i = TTrackTr::begin(*track), e = TTrackTr::end(*track); i != e; ++i) {
                const NLWTrace::TLogItem& item = *i;
                const auto threadId = i->ThreadId;
                tl.Items.push_back(NLWTrace::TTrackLog::TItem(threadId, item));
            }
            reader.Push(0, tl);
            if (spaceLeft) {
                spaceLeft--;
                if (!spaceLeft) {
                    break;
                }
            }
        }
    }
};

// Track classification tree
class TPatternTree {
public:
    // Per-node classifier by probe name
    class TClassifyByProbe : public TClassifier {
    private:
        using TChildren = THashMap<NLWTrace::TProbe*, TPatternNode>;
        TChildren Children;
        TVector<TChildren::value_type*> SortedChildren;
    public:
        explicit TClassifyByProbe(TPatternNode* node)
            : TClassifier(node, NT_PROBE)
        {}

        TPatternNode* Classify(TTrackIter cur, const TTrack& track) override
        {
            Y_UNUSED(track);
            const NLWTrace::TLogItem& item = *cur;
            TPatternNode* node = &Children[item.Probe];
            node->Name = "/" + GetProbeName(item.Probe);
            node->Desc.Type = NT_PROBE;
            node->Desc.Probe = item.Probe;
            return node;
        }

        void Accept(IVisitor* visitor) override
        {
            if (SortedChildren.size() != Children.size()) {
                SortedChildren.clear();
                SortedChildren.reserve(Children.size());
                for (auto i = Children.begin(), e = Children.end(); i != e; ++i) {
                    SortedChildren.push_back(&*i);
                }
                Sort(SortedChildren, [] (TChildren::value_type* lhs, TChildren::value_type* rhs) {
                    NLWTrace::TProbe* lp = lhs->first;
                    NLWTrace::TProbe* rp = rhs->first;
                    if (int cmp = strcmp(lp->Event.GetProvider(), rp->Event.GetProvider())) {
                        return cmp < 0;
                    }
                    return strcmp(lp->Event.Name, rp->Event.Name) < 0;
                });
            }
            for (auto* kv : SortedChildren) {
                visitor->Visit(&kv->second);
            }
        }

        bool IsLeaf() override { return Children.empty(); }
    };

    // Per-node classifier by probe param value
    class TClassifyByParam : public TClassifier {
    private:
        size_t Rollbacks; // How many items should we look back in track to locate probe
        TString ParamName;
        using TChildren = THashMap<TString, TPatternNode>;
        TChildren Children;
        TVector<TChildren::value_type*> SortedChildren;
    public:
        TClassifyByParam(TPatternNode* node, size_t rollbacks, const TString& paramName)
            : TClassifier(node, NT_PARAM, true)
            , Rollbacks(rollbacks)
            , ParamName(paramName)
        {}

        TPatternNode* Classify(TTrackIter cur, const TTrack& track) override
        {
            WWW_CHECK((i64)Rollbacks >= 0 && std::distance(TTrackTr::begin(track), cur) >= (i64)Rollbacks, "wrong rollbacks in node '%s'",
                      Node->GetPath().data());
            const NLWTrace::TLogItem& item = *(cur - Rollbacks);
            WWW_CHECK(item.SavedParamsCount > 0, "classify by params on probe w/o param loggging in node '%s'",
                      Node->GetPath().data());
            TString paramValues[LWTRACE_MAX_PARAMS];
            TString* paramValue = nullptr;
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            for (size_t pi = 0; pi < item.SavedParamsCount; pi++) {
                if (item.Probe->Event.Signature.ParamNames[pi] == ParamName) {
                    paramValue = &paramValues[pi];
                }
            }
            WWW_CHECK(paramValue, "param '%s' not found in probe '%s' at path '%s'",
                      ParamName.data(), GetProbeName(item.Probe).data(), Node->GetPath().data());

            TPatternNode* node = &Children[*paramValue];
            // Path example: "//Provider1.Probe1/Provider2.Probe2@1.xxx=123@2.type=harakiri"
            node->Name = "@" + ToString(Rollbacks) + "." + ParamName + "=" + *paramValue;
            node->Desc.Type = NT_PARAM;
            node->Desc.Rollbacks = Rollbacks;
            node->Desc.ParamName = ParamName;
            node->Desc.ParamValue = *paramValue;
            return node;
        }

        void Accept(IVisitor* visitor) override
        {
            if (SortedChildren.size() != Children.size()) {
                SortedChildren.clear();
                SortedChildren.reserve(Children.size());
                for (auto i = Children.begin(), e = Children.end(); i != e; ++i) {
                    SortedChildren.push_back(&*i);
                }
                Sort(SortedChildren, [] (TChildren::value_type* lhs, TChildren::value_type* rhs) {
                    return lhs->first < rhs->first;
                });
            }
            for (auto* kv : SortedChildren) {
                visitor->Visit(&kv->second);
            }
        }

        bool IsLeaf() override { return Children.empty(); }
    };
private:
    TPatternNode Root;
    THashMap<TString, std::pair<size_t, TString>> ParamClassifiers; // path -> (rollbacks, param)
    TString SelectedPattern;
    TPatternNode* SelectedNode = nullptr;
    TVector<ui64> Timeline; // Just to avoid reallocations
public:
    TPatternTree(const TCgiParameters& e)
    {
        for (const TString& cl : Subvalues(e, "classify")) {
            size_t at = cl.find_last_of('@');
            if (at != TString::npos) {
                size_t dot = cl.find('.', at + 1);
                if (dot != TString::npos) {
                    size_t rollbacks = FromString<size_t>(cl.substr(at + 1, dot - at - 1));
                    ParamClassifiers[cl.substr(0, at)] = std::make_pair(rollbacks, cl.substr(dot + 1));
                }
            }
        }
        SelectedPattern = e.Get("pattern");
        InitNode(&Root, nullptr);
    }

    TPatternNode* GetSelectedNode()
    {
        return SelectedNode;
    }

    NAnalytics::TTable GetSelectedTable()
    {
        if (SelectedNode) {
            return SelectedNode->GetTable();
        } else {
            return NAnalytics::TTable();
        }
    }

    template <typename TReader>
    void OutputSelectedSample(const TString& bn, double b1, double b2, const TSampleOpts& opts, TReader& reader)
    {
        if (SelectedNode) {
            SelectedNode->OutputSample(bn, b1, b2, opts, reader);
        }
    }

    // Register track in given node
    void AddTrackToNode(TPatternNode* node, TTrack& track, ui64 resTotal, TVector<ui64>& timeline)
    {
        if (!SelectedNode) {
            if (node->GetPath() == SelectedPattern) {
                SelectedNode = node;
            }
        }

        // Counting
        node->TrackCount++;

        // Resource total
        node->ResTotalSum += resTotal;
        node->ResTotalMax = Max(node->ResTotalMax, resTotal);
        node->ResTotalAll.push_back(resTotal);

        // Resource last
        ui64 resLast = 0;
        resLast = resTotal - (timeline.size() < 2? 0: timeline[timeline.size() - 2]);
        node->ResLastSum += resLast;
        node->ResLastMax = Max(node->ResLastMax, resLast);
        node->ResLastAll.push_back(resLast);

        // Timeline
        if (node->TimelineSum.size() < timeline.size()) {
            node->TimelineSum.resize(timeline.size());
        }
        for (size_t i = 0; i < timeline.size(); i++) {
            node->TimelineSum[i] += timeline[i];
        }

        if (node == SelectedNode && !timeline.empty()) {
            node->Slices.emplace_back();
            NAnalytics::TRow& row = node->Slices.back();
            ui64 prev = 0;
            for (size_t i = 0; i < timeline.size(); i++) {
                // Note that col names should go in lexicographical order
                // in the same way as slices go in pattern timeline
                double sliceMs = double(timeline[i] - prev) * 1000.0 / NHPTimer::GetClockRate();
                row[Sprintf("%09lu", i)] = sliceMs;
                prev = timeline[i];
            }
        }

        // Interlink node and track
        node->Tracks.emplace_back(&track, resTotal, resLast);
        track.LastNode = node;
    }

    bool CheckPattern(const char*& pi, const char* pe, TStringBuf str)
    {
        auto si = str.begin(), se = str.end();
        for (;pi != pe && si != se; ++pi, ++si) {
            if (*pi != *si) {
                return false;
            }
        }
        return si == se;
    }

#define WWW_CHECK_PATTERN(str) if (!CheckPattern(pi, pe, (str))) { return false; }

    bool MatchTrack(const TTrack& track, const TString& patternStr)
    {
        const char* pi = patternStr.data();
        const char* pe = pi + patternStr.size();
        WWW_CHECK_PATTERN("/");
        for (TTrackIter i = TTrackTr::begin(track), e = TTrackTr::end(track); i != e; ++i) {
            if (pi == pe) {
                return true;
            }
            const NLWTrace::TLogItem& item = *i;
            WWW_CHECK_PATTERN("/");
            WWW_CHECK_PATTERN(item.Probe->Event.GetProvider());
            WWW_CHECK_PATTERN(".");
            WWW_CHECK_PATTERN(item.Probe->Event.Name);
            while (true) {
                if (pi == pe) {
                    return true;
                }
                char c = *pi;
                if (c == '/') {
                    break;
                } else if (c == '@') {
                    pi++;
                    // Parse rollbacks
                    TStringBuf p(pi, pe);
                    size_t dot = p.find('.');
                    if (dot == TStringBuf::npos) {
                        return false;
                    }
                    size_t rollbacks = 0;
                    try {
                        rollbacks = FromString<size_t>(p.substr(0, dot));
                    } catch (...) {
                        return false;
                    }

                    // Parse param name
                    size_t equals = p.find('=', dot + 1);
                    if (equals == TStringBuf::npos) {
                        return false;
                    }
                    TStringBuf paramName = p.substr(dot + 1, equals - dot - 1);

                    pi += equals + 1; // Advance to value

                    // Check param value
                    if ((i64)rollbacks < 0 || std::distance(TTrackTr::begin(track), i) < (i64)rollbacks) {
                        return false;
                    }
                    const NLWTrace::TLogItem& mitem = *(i - rollbacks);
                    if (mitem.SavedParamsCount == 0) {
                        return false;
                    }
                    TString paramValues[LWTRACE_MAX_PARAMS];
                    TString* paramValue = nullptr;
                    mitem.Probe->Event.Signature.SerializeParams(mitem.Params, paramValues);
                    for (size_t pi = 0; pi < mitem.SavedParamsCount; pi++) {
                        if (mitem.Probe->Event.Signature.ParamNames[pi] == paramName) {
                            paramValue = &paramValues[pi];
                        }
                    }
                    if (!paramValue) {
                        return false;
                    }
                    WWW_CHECK_PATTERN(*paramValue);
                } else {
                    return false;
                }
            }
        }
        return true;
    }

#undef WWW_CHECK_PATTERN

    // Push new track through pattern tree
    void AddTrack(TTrack& track)
    {
        // Truncate long tracks
        if (track.Items.size() > 50) {
            track.Items.resize(50);
        }

        if (SelectedPattern) {
            if (!MatchTrack(track, SelectedPattern)) {
                return;
            }
        }

        Timeline.clear();
        TPatternNode* node = &Root;
        AddTrackToNode(node, track, 0, Timeline);
        ui64 trackStart = TTrackTr::front(track).TimestampCycles;
        for (TTrackIter i = TTrackTr::begin(track), e = TTrackTr::end(track); i != e;) {
            // Get or create child by classification
            TPatternNode* parent = node;
            node = node->Classifier->Classify(i, track);
            if (!node->Classifier) {
                InitNode(node, parent);
            }

            const NLWTrace::TLogItem& item = *i;
            ui64 resTotal = item.TimestampCycles - trackStart;
            if (i != TTrackTr::begin(track)) {
                Timeline.push_back(resTotal);
            }
            AddTrackToNode(node, track, resTotal, Timeline);

            // Move through track
            if (!node->Classifier->KeepHead) {
                ++i;
            }
        }
    }

    // Traverse pattern tree (the only way to extract data from it)
    template <class TOnNode, class TOnDescend, class TOnAscend>
    void Traverse(TOnNode&& onNode, TOnDescend&& onDescend, TOnAscend&& onAscend)
    {
        struct TVisitor : public IVisitor {
            TOnNode OnNode;
            TOnDescend OnDescend;
            TOnAscend OnAscend;
            TVisitor(TOnNode&& onNode, TOnDescend&& onDescend, TOnAscend&& onAscend)
                : OnNode(onNode)
                , OnDescend(onDescend)
                , OnAscend(onAscend)
            {}
            virtual void Visit(TPatternNode* node) override
            {
                OnNode(node);
                if (!node->Classifier->IsLeaf()) {
                    OnDescend();
                    node->Classifier->Accept(this);
                    OnAscend();
                }
            }
        };
        TVisitor visitor(std::move(onNode), std::move(onDescend), std::move(onAscend));
        visitor.Visit(&Root);
    }

    TPatternNode* GetRoot()
    {
        return &Root;
    }

private:
    void InitNode(TPatternNode* node, TPatternNode* parent)
    {
        node->Parent = parent;
        auto iter = ParamClassifiers.find(node->GetPath());
        if (iter != ParamClassifiers.end()) {
            node->Classifier.Reset(new TClassifyByParam(node, iter->second.first, iter->second.second));
        } else {
            node->Classifier.Reset(new TClassifyByProbe(node));
        }
    }
};

class TLogTrackExtractor: public TLogFilter {
private:
    // Data storage
    TMultiMap<ui64, std::pair<TThread::TId, NLWTrace::TLogItem>> Items;
    TVector<NLWTrace::TTrackLog> Depot;

    // Data refs organized in tracks
    THashMap<TString, TTrack> Tracks;
    TVector<TTrack> TracksFromDepot;

    // Analysis
    TVector<TString> GroupBy;
    THashSet<TString> TrackIds; // The same content as in GroupBy
    TTimestampCutter CutTs;
    TPatternTree Tree;
public:
    TLogTrackExtractor(const TCgiParameters& e, const TVector<TString>& filters, const TVector<TString>& groupBy)
        : TLogFilter(filters)
        , CutTs(true) // Always cut input data for tracks
        , Tree(e)
    {
        for (const TString& groupParam : groupBy) {
            GroupBy.push_back(groupParam);
            TrackIds.insert(groupParam);
        }
    }

    // For reading lwtrace log (input point for all data)
    void Push(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        CutTs.Push(tid, item);
        if (Filter(item)) {
            Items.emplace(item.TimestampCycles, std::make_pair(tid, item));
        }
    }

    // For reading lwtrace depot (input point for all data)
    void Push(TThread::TId, const NLWTrace::TTrackLog& tl)
    {
        if (Filter(tl)) {
            Depot.emplace_back(tl);
        }
    }

    // Analyze logs that have been read
    void Run()
    {
        RunImplLog();
        RunImplDepot();
    }

    void RunImplLog()
    {
        // Create tracks by filling them with lwtrace items in order of occurance time
        for (auto& kv : Items) {
            AddItemToTrack(kv.second.first, kv.second.second);
        }
        // Push tracks throught pattern tree
        for (auto& kv : Tracks) {
            TTrack& track = kv.second;
            track.TrackId = kv.first;
            Tree.AddTrack(track);
        }
    }

    void RunImplDepot()
    {
        // Create tracks from depot
        // OPTIMIZE[serxa]: this convertion is not necessary, done just to keep things simple
        for (NLWTrace::TTrackLog& tl : Depot) {
            TTrack& track = TracksFromDepot.emplace_back();
            track.TrackId = ToString(tl.Id);
            for (const NLWTrace::TTrackLog::TItem& i : tl.Items) {
                track.Items.emplace_back(i.ThreadId, i);
            }
        }
        for (TTrack& t : TracksFromDepot) {
            Tree.AddTrack(t);
        }
    }

    // Selected node distribution
    NAnalytics::TTable Distribution(const TString& bn, const TString& b1Str, const TString& b2Str, const TString& widthStr)
    {
        using namespace NAnalytics;

        const NAnalytics::TTable& inputTable = Tree.GetSelectedTable();
        double b1 = b1Str? FromString<double>(b1Str): MinValue(bn, inputTable);
        double b2 = b2Str? FromString<double>(b2Str): MaxValue(bn, inputTable);
        if (isfinite(b1) && isfinite(b2)) {
            WWW_CHECK(b1 <= b2, "invalid xrange [%le; %le]", b1, b2);
            double width = widthStr? FromString<double>(widthStr): 99;
            double dx = (b2 - b1) / width;
            if (!(dx > 0)) {
                dx = 1.0;
            }
            return HistogramAll(inputTable, bn, b1, b2, dx);
        } else {
            // Empty table -- it's ok -- leave data table empty
            return NAnalytics::TTable();
        }
    }

    // Selected sample
    template <typename TReader>
    void OutputSample(const TString& bn, double b1, double b2, const TSampleOpts& opts, TReader& reader)
    {
        Tree.OutputSelectedSample(bn, b1, b2, opts, reader);
    }

    // Tabular representation of tracks data
    void OutputTable(IOutputStream& os, const TCgiParameters& e)
    {
        ui64 tracksTotal = Tree.GetRoot()->TrackCount;

        double maxAvgResTotal = 0;
        double maxMaxResTotal = 0;
        Tree.Traverse([&] (TPatternNode* node) {
            if (node->TrackCount > 0) {
                maxAvgResTotal = Max(maxAvgResTotal, double(node->ResTotalSum) / node->TrackCount);
                maxMaxResTotal = Max(maxMaxResTotal, double(node->ResTotalMax));
                Sort(node->ResTotalAll);
                Sort(node->ResLastAll);
            }
        }, [&] () { // On descend
        }, [&] () { // On ascend
        });
        double maxTime = Min(maxMaxResTotal, 1.25 * maxAvgResTotal);

        double percentile = e.Get("ile")? FromString<double>(e.Get("ile")): 90;
        WWW_CHECK(percentile >= 0.0 && percentile <= 100.0, "wrong percentile: %lf", percentile);

        ui64 row = 0;
        TVector<ui64> chain;
        HTML(os) {
            TABLE_CLASS("tracks-tree") {
                TABLEHEAD() {
                    os << "<tr>";
                    os << "<td rowspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">#</td>";
                    os << "<td rowspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">Pattern</td>";
                    os << "<td rowspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">";
                    DIV_CLASS("rotate") { os << "Track Count"; }
                    os << "</td>";
                    os << "<td colspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">Share</td>";
                    os << "<td colspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">Total, ms</td>";
                    os << "<td colspan=\"2\" style=\"vertical-align:bottom\" align=\"center\">Last, ms</td>";
                    os << "<td rowspan=\"2\" style=\"vertical-align:bottom\" class=\"timelinehead\" align=\"center\">Global Timeline</td>";
                    os << "</tr><tr>";
                    TABLEH() DIV_CLASS("rotate") { os << "Absolute"; }
                    TABLEH() DIV_CLASS("rotate") { os << "Relative"; }
                    TABLEH() DIV_CLASS("rotate") { os << "Average"; }
                    TABLEH() DIV_CLASS("rotate") { os << percentile << "%-ile"; }
                    TABLEH() DIV_CLASS("rotate") { os << "Average"; }
                    TABLEH() DIV_CLASS("rotate") { os << percentile << "%-ile"; }
                    os << "</tr>";
                }
                TABLEBODY() {
                    if (tracksTotal == 0) {
                        return;
                    }
                    Tree.Traverse([&] (TPatternNode* node) {
                        TString parentClass;
                        if (!chain.empty()) {
                            parentClass = " treegrid-parent-" + ToString(chain.back());
                        }
                        TString selectedClass;
                        if (e.Get("pattern") == node->GetPath()) {
                            selectedClass = " danger";
                        }
                        TABLER_CLASS("treegrid-" + ToString(++row) + parentClass + selectedClass) {
                            // Counting
                            ui64 tracksParent = node->Parent? node->Parent->TrackCount: tracksTotal;
                            double absShare = double(node->TrackCount) * 100 / tracksTotal;
                            double relShare = double(node->TrackCount) * 100 / tracksParent;

                            // Resource total
                            double avgResTotal = double(node->ResTotalSum) / node->TrackCount;
                            size_t ileResTotalIdx = node->ResTotalAll.size() * percentile / 100;
                            if (ileResTotalIdx > 0) {
                                ileResTotalIdx--;
                            }
                            double ileResTotal = double(ileResTotalIdx >= node->ResTotalAll.size()? 0: node->ResTotalAll[ileResTotalIdx]);
                            double avgResTotalMs = avgResTotal * 1000.0 / NHPTimer::GetClockRate();
                            double ileResTotalMs = ileResTotal * 1000.0 / NHPTimer::GetClockRate();

                            // Resource last
                            double avgResLast = double(node->ResLastSum) / node->TrackCount;
                            size_t ileResLastIdx = node->ResLastAll.size() * percentile / 100;
                            if (ileResLastIdx > 0) {
                                ileResLastIdx--;
                            }
                            double ileResLast = double(ileResLastIdx >= node->ResLastAll.size()? 0: node->ResLastAll[ileResLastIdx]);
                            double avgResLastMs = avgResLast * 1000.0 / NHPTimer::GetClockRate();
                            double ileResLastMs = ileResLast * 1000.0 / NHPTimer::GetClockRate();

                            // Output
                            TABLED() { os << row; }
                            TABLED_CLASS("treegrid-element") { OutputPattern(os, e, node); }
                            TABLED() { os << node->TrackCount; }
                            TABLED() { OutputShare(os, absShare); }
                            TABLED() { OutputShare(os, relShare); }
                            TABLED() { os << FormatFloat(avgResTotalMs); }
                            TABLED() { os << FormatFloat(ileResTotalMs); }
                            TABLED() { os << FormatFloat(avgResLastMs); }
                            TABLED() { os << FormatFloat(ileResLastMs); }
                            TABLED() { OutputTimeline(os, MakeTimeline(node), maxTime); }
                        }
                    }, [&] () { // On descend
                        chain.push_back(row);
                    }, [&] () { // On ascend
                        chain.pop_back();
                    });
                }
            }
        }
    }

    // Chromium-compatible trace representation of tracks data
    void OutputChromeTrace(IOutputStream& os, const TCgiParameters& e)
    {
        Y_UNUSED(e);
        TChromeTrace tr;
        for (TPatternNode::TTrackEntry& entry: Tree.GetRoot()->Tracks) {
            TTrack* track = entry.Track;
            auto first = TTrackTr::begin(*track);
            auto last = TTrackTr::rbegin(*track);

            TString name = track->LastNode->GetPath();

            const NLWTrace::TLogItem& firstItem = *first;
            TThread::TId firstTid = first->ThreadId;
            tr.Add(firstTid, firstItem.TimestampCycles, "b", "track", nullptr, name, track->TrackId);

            for (auto cur = TTrackTr::begin(*track), end = TTrackTr::end(*track); cur != end; ++cur) {
                const NLWTrace::TLogItem& item = *cur;

                tr.Add(cur->ThreadId, item.TimestampCycles, "i", "event", &item, GetProbeName(item.Probe));

                TString sliceName = GetProbeName(item.Probe);

                auto next = cur + 1;
                if (next != end) {
                    const NLWTrace::TLogItem& nextItem = *next;
                    tr.Add(cur->ThreadId, item.TimestampCycles, "b", "track", &item, sliceName, track->TrackId);
                    tr.Add(next->ThreadId, nextItem.TimestampCycles, "e", "track", &nextItem, sliceName, track->TrackId);
                } else {
                    tr.Add(cur->ThreadId, item.TimestampCycles, "n", "track", &item, sliceName, track->TrackId);
                }
            }

            const NLWTrace::TLogItem& lastItem = *last;
            tr.Add(last->ThreadId, lastItem.TimestampCycles, "e", "track", nullptr, name, track->TrackId);
        }
        tr.Output(os);
    }

    void OutputSliceCovarianceMatrix(IOutputStream& os, const TCgiParameters& e)
    {
        Y_UNUSED(e);
        TPatternNode* node = Tree.GetSelectedNode();
        if (!node) {
            return;
        }

        NAnalytics::TMatrix covMatrix = NAnalytics::CovarianceMatrix(node->Slices);
        double var = covMatrix.CellSum();

        double covMax = 0.0;
        for (double x : covMatrix) {
            if (covMax < x) {
                covMax = x;
            }
        }
        double dangerCov = covMax * 0.9 * 0.9;
        double warnCov = covMax * 0.5 * 0.5;

        HTML(os) {
            TABLE() {
                TTimeline timeline = MakeTimeline(node);
                TABLEHEAD() TABLER() {
                    TABLED();
                    for (auto& e : timeline) TABLED() {
                        TPatternNode* subnode = e.first;
                        os << subnode->Name;
                    }
                }

                auto tl = timeline.begin();
                TABLEBODY() for (size_t row = 0; row < covMatrix.Rows; row++) TABLER() {
                    TABLEH() {
                        if (tl != timeline.end()) {
                            TPatternNode* subnode = tl->first;
                            os << subnode->Name;
                            ++tl;
                        }
                    }

                    for (size_t col = 0; col < covMatrix.Cols; col++) {
                        double cov = covMatrix.Cell(row, col);
                        TString tdClass = (cov >= dangerCov? "danger": (cov >= warnCov? "warning": ""));
                        TABLED_CLASS(tdClass) {
                            double sigmaX = (covMatrix.Cell(row, row) > 0? sqrt(covMatrix.Cell(row, row)): 0);
                            double sigmaY = (covMatrix.Cell(col, col) > 0? sqrt(covMatrix.Cell(col, col)): 0);
                            os << Sprintf("cov=%.3lf&nbsp;ms<sup>2</sup>&nbsp;(%.3lf&nbsp;ms) corr=%.1lf%% var_share=%.1lf%%",
                                          cov, sqrt(abs(cov)), cov * 100.0 / sigmaX / sigmaY, cov * 100.0 / var);
                        }
                    }
                }
            }
        }
    }

private:
    TPatternNode* RollbackFind(TPatternNode* node)
    {
        for (;node != nullptr; node = node->Parent) {
            if (node->Desc.Type == NT_PROBE) {
                return node;
            }
        }
        return nullptr;
    }

    void OutputPattern(IOutputStream& os, const TCgiParameters& e, TPatternNode* node)
    {
        // Fill pattern name
        TString patternName;
        TString patternTitle;
        switch (node->Desc.Type) {
        case NT_ROOT:
            patternName = "All Tracks";
            break;
        case NT_PROBE:
            patternTitle = GetProbeName(node->Desc.Probe);
            patternName = node->Desc.Probe->Event.Name;
            break;
        case NT_PARAM:
            patternName.append(node->Desc.ParamName + " = " + node->Desc.ParamValue);
            break;
        }

        os << "<a href=\"" << MakeUrl(e, {
                {"pattern", node->GetPath()},
                {"ptrn_anlz", e.Get("ptrn_anlz") ? e.Get("ptrn_anlz") : "resTotal"},
                {"linesfill", "y"},
                {"linessteps", "y"},
                {"pointsshow", "n"},
                {"sel_x1", e.Get("sel_x1") ? e.Get("sel_x1") : "0"},
                {"sel_x2", e.Get("sel_x2") ? e.Get("sel_x2") : "inf"}}) << "\""
              " title=\"" + patternTitle + "\">" << patternName << "</a>";

        // Add/remove node menu
        if (node->Desc.Type != NT_ROOT) {
            os << "<div class=\"dropdown pull-right\" style=\"display:inline-block\">";
            if (node->Desc.Type == NT_PARAM) {
                os<< "<button class=\"btn btn-xs btn-default\" type=\"button\""
                  << "\" onClick=\"window.location.href='"
                  << MakeUrlEraseSub(e, "classify", node->Parent->GetPath() + "@"
                                     + ToString(node->Desc.Rollbacks) + "." + node->Desc.ParamName)
                  << "';\">"
                       "<span class=\"glyphicon glyphicon-minus\"></span>"
                     "</button>";
            }
            if (node->Classifier->GetChildType() != NT_PARAM) {
                os <<    "<button class=\"btn btn-xs btn-default dropdown-toggle\" type=\"button\""
                                " data-toggle=\"dropdown\" aria-haspopup=\"true\" aria-expanded=\"true\">"
                           "<span class=\"glyphicon glyphicon-plus\"></span>"
                         "</button>"
                         "<ul class=\"dropdown-menu\">"
                           "<li class=\"dropdown-header\">Classify by param:</li>";
                int rollbacks = 0;
                TPatternNode* probeNode = node;
                while (probeNode = RollbackFind(probeNode)) {
                    const NLWTrace::TProbe* probe = probeNode->Desc.Probe;
                    os << "<li class=\"dropdown-header\">" << GetProbeName(probe) << "</li>";
                    const NLWTrace::TSignature* sgn = &probe->Event.Signature;
                    for (size_t pi = 0; pi < sgn->ParamCount; pi++) {
                        TString param = sgn->ParamNames[pi];
                        if (TrackIds.contains(param) || IsFiltered(param)) {
                            continue;
                        }
                        os << "<li><a href=\""
                           << MakeUrlAddSub(e, "classify", node->GetPath() + "@" + ToString(rollbacks) + "." + param)
                           << "\">" << param << "</a></li>";
                    }
                    rollbacks++;
                    probeNode = probeNode->Parent;
                }
                os <<    "</ul>";
            }
            os << "</div>";
        }
    }

    void OutputShare(IOutputStream& os, double share)
    {
        double lshare = share;
        double rshare = 100 - lshare;
        os << "<div class=\"progress\" style=\"margin-bottom:0px;position:relative\">"
                "<div class=\"progress-bar progress-bar-success\" role=\"progressbar\""
                  " aria-valuenow=\"" << lshare << "\""
                  " aria-valuemin=\"0\""
                  " aria-valuemax=\"100\""
                  " style=\"width: " << lshare << "%;\">"
                "</div>"
                "<div class=\"progress-bar progress-bar-danger\" role=\"progressbar\""
                  " aria-valuenow=\"" << rshare << "\""
                  " aria-valuemin=\"0\""
                  " aria-valuemax=\"100\""
                  " style=\"width: " << rshare << "%;\">"
                "</div>"
                "<span style=\"position:absolute;left:0;width:100%;text-align:center;z-index:2;color:white\">"
                  << (share == 100? "100%": Sprintf("%2.1lf%%", share)) <<
                "</span>"
              "</div>";
    }

    using TTimeline = TVector<std::pair<TPatternNode*, double>>;

    TTimeline MakeTimeline(TPatternNode* node)
    {
        TTimeline ret;
        if (node->TrackCount == 0) {
            return ret;
        }
        ret.reserve(node->TimelineSum.size());
        for (double time : node->TimelineSum) {
            ret.emplace_back(nullptr, double(time) / node->TrackCount);
        }
        TPatternNode* n = node;
        for (auto i = ret.rbegin(), e = ret.rend(); i != e; ++i) {
            WWW_CHECK(n, "internal bug: wrong timeline length at pattern node '%s'", node->GetPath().data());
            i->first = n;
            n = n->Parent;
        }
        return ret;
    }

    void OutputTimeline(IOutputStream& os, const TTimeline& timeline, double maxTime)
    {
        static const char *barClass[] = {
            "progress-bar-info",
            "progress-bar-warning"
        };
        if (timeline.empty()) {
            return;
        }
        os << "<div class=\"progress\" style=\"margin-bottom:0px;color:black\">";
        double prevPos = 0.0;
        double prevTime = 0.0;
        size_t i = 0;
        for (auto& e : timeline) {
            TPatternNode* node = e.first;
            double time = e.second;
            double pos = time * 100 / maxTime;
            if (pos > 100) {
                pos = 100;
            }
            double width = pos - prevPos;
            os << "<div class=\"progress-bar " << barClass[i % 2] << "\" role=\"progressbar\""
                     " aria-valuenow=\"" << width << "\""
                     " aria-valuemin=\"0\""
                     " aria-valuemax=\"100\""
                     " style=\"width:" << width << "%;color:black\""
                     " title=\"" << FormatTimelineTooltip(time, prevTime, node) << "\">";
            if (width > 20) { // To ensure text will fit the bar
                os << FormatCycles(time - prevTime);
            }
            os << "</div>";
            prevPos = pos;
            prevTime = time;
            i++;
        }
        os << "</div>";
    }

    TString FormatTimelineTooltip(double time, double prevTime, TPatternNode* node)
    {
        return FormatCycles(time - prevTime) + ": "
                + FormatCycles(prevTime) + " -> " + FormatCycles(time)
                + "(" + node->Name + ")";
    }

    TString FormatFloat(double value)
    {
        if (value == 0.0) {
            return "0";
        }
        if (value > 1.0) {
            if (value > 100.0) {
                return Sprintf("%.0lf", value);
            }
            if (value > 10.0) {
                return Sprintf("%.1lf", value);
            }
            return Sprintf("%.2lf", value);
        } else if (value > 1e-3) {
            if (value > 1e-1) {
                return Sprintf("%.3lf", value);
            }
            if (value > 1e-2) {
                return Sprintf("%.4lf", value);
            }
            return Sprintf("%.5lf", value);
        } else if (value > 1e-6) {
            if (value > 1e-4) {
                return Sprintf("%.6lf", value);
            }
            if (value > 1e-5) {
                return Sprintf("%.7lf", value);
            }
            return Sprintf("%.8lfus", value);
        } else {
            if (value > 1e-7) {
                return Sprintf("%.9lfns", value);
            }
            if (value > 1e-8) {
                return Sprintf("%.10lfns", value);
            }
            return Sprintf("%.2le", value);
        }
    }

    TString FormatCycles(double timeCycles)
    {
        double timeSec = timeCycles / NHPTimer::GetClockRate();
        if (timeSec > 1.0) {
            if (timeSec > 100.0) {
                return Sprintf("%.0lfs", timeSec);
            }
            if (timeSec > 10.0) {
                return Sprintf("%.1lfs", timeSec);
            }
            return Sprintf("%.2lfs", timeSec);
        } else if (timeSec > 1e-3) {
            if (timeSec > 1e-1) {
                return Sprintf("%.0lfms", timeSec * 1e3);
            }
            if (timeSec > 1e-2) {
                return Sprintf("%.1lfms", timeSec * 1e3);
            }
            return Sprintf("%.2lfms", timeSec * 1e3);
        } else if (timeSec > 1e-6) {
            if (timeSec > 1e-4) {
                return Sprintf("%.0lfus", timeSec * 1e6);
            }
            if (timeSec > 1e-5) {
                return Sprintf("%.1lfus", timeSec * 1e6);
            }
            return Sprintf("%.2lfus", timeSec * 1e6);
        } else {
            if (timeSec > 1e-7) {
                return Sprintf("%.0lfns", timeSec * 1e9);
            }
            if (timeSec > 1e-8) {
                return Sprintf("%.1lfns", timeSec * 1e9);
            }
            return Sprintf("%.2lfns", timeSec * 1e9);
        }
    }

    TString GetParam(const NLWTrace::TLogItem& item, TString* paramValues, const TString& paramName)
    {
        for (size_t pi = 0; pi < item.SavedParamsCount; pi++) {
            if (paramName == item.Probe->Event.Signature.ParamNames[pi]) {
                return paramValues[pi];
            }
        }
        return TString();
    }

    TString GetGroup(const NLWTrace::TLogItem& item, TString* paramValues)
    {
        TStringStream ss;
        bool first = true;
        for (const TString& groupParam : GroupBy) {
            ss << (first? "": "|") << GetParam(item, paramValues, groupParam);
            first = false;
        }
        return ss.Str();
    }

    void AddItemToTrack(TThread::TId tid, const NLWTrace::TLogItem& item)
    {
        // Ensure cyclic per thread lwtrace logs wont drop *inner* items of a track
        // (note that some *starting* items can be dropped)
        if (item.SavedParamsCount > 0 && !CutTs.Skip(item)) {
            TString paramValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            Tracks[GetGroup(item, paramValues)].Items.emplace_back(tid, item);
        }
    }
};

NLWTrace::TProbeRegistry g_Probes;
TString g_sanitizerTest("TString g_sanitizerTest");
NLWTrace::TManager g_SafeManager(g_Probes, false);
NLWTrace::TManager g_UnsafeManager(g_Probes, true);
TDashboardRegistry g_DashboardRegistry;

class TLWTraceMonPage : public NMonitoring::IMonPage {
private:
    NLWTrace::TManager* TraceMngr;
    TString StartTime;
    TTraceCleaner Cleaner;
    TMutex SnapshotsMtx;
    THashMap<TString, TAtomicSharedPtr<NLWTrace::TLogPb>> Snapshots;
public:
    explicit TLWTraceMonPage(bool allowUnsafe = false)
        : NMonitoring::IMonPage("trace", "Tracing")
        , TraceMngr(&TraceManager(allowUnsafe))
        , Cleaner(TraceMngr)
    {
        time_t stime = TInstant::Now().TimeT();
        StartTime = CTimeR(&stime);
    }

    virtual void Output(NMonitoring::IMonHttpRequest& request) {
        TStringStream out;
        try {
            if (request.GetParams().Get("mode") == "") {
                OutputTracesAndSnapshots(request, out);
            } else if (request.GetParams().Get("mode") == "probes") {
                OutputProbes(request, out);
            } else if (request.GetParams().Get("mode") == "dashboards") {
                OutputDashboards(request, out);
            } else if (request.GetParams().Get("mode") == "dashboard") {
                OutputDashboard(request, out);
            } else if (request.GetParams().Get("mode") == "log") {
                OutputLog(request, out);
            } else if (request.GetParams().Get("mode") == "query") {
                OutputQuery(request, out);
            } else if (request.GetParams().Get("mode") == "builder") {
                OutputBuilder(request, out);
            } else if (request.GetParams().Get("mode") == "analytics") {
                OutputAnalytics(request, out);
            } else if (request.GetParams().Get("mode") == "new") {
                PostNew(request, out);
            } else if (request.GetParams().Get("mode") == "delete") {
                PostDelete(request, out);
            } else if (request.GetParams().Get("mode") == "make_snapshot") {
                PostSnapshot(request, out);
            } else if (request.GetParams().Get("mode") == "settimeout") {
                PostSetTimeout(request, out);
            } else {
                ythrow yexception() << "Bad request";
            }
        } catch (TPageGenBase& gen) {
            out.Clear();
            out << EncodeHtmlPcdata(gen.what());
        } catch (...) {
            out.Clear();
            if (request.GetParams().Get("error") == "text") {
                // Text error reply is helpful for ajax requests
                out << NMonitoring::HTTPOKTEXT;
                out << EncodeHtmlPcdata(CurrentExceptionMessage());
            } else {
                WWW_HTML(out) {
                    out << "<h2>Error</h2><pre>"
                        << EncodeHtmlPcdata(CurrentExceptionMessage())
                        << Endl;
                }
            }
        }
        request.Output() << out.Str();
    }

private:
    void OutputNavbar(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        TString active = " class=\"active\"";
        out <<
            "<nav class=\"navbar navbar-default\"><div class=\"container-fluid\">"
              << NavbarHeader() <<
              "<ul class=\"nav navbar-nav\">"
                "<li" << (request.GetParams().Get("mode") == ""? active: "") << "><a href=\"?mode=\">Traces</a></li>"
                "<li" << (request.GetParams().Get("mode") == "probes"? active: "") << "><a href=\"?mode=probes\">Probes</a></li>"
                "<li" << (request.GetParams().Get("mode") == "dashboards"? active: "") << "><a href=\"?mode=dashboards\">Dashboard</a></li>"
                "<li" << (request.GetParams().Get("mode") == "builder"? active: "") << "><a href=\"?mode=builder\">Builder</a></li>"
                "<li" << (request.GetParams().Get("mode") == "analytics"? active: "") << "><a href=\"?mode=analytics&id=\">Analytics</a></li>"
                "<li><a href=\"https://wiki.yandex-team.ru/development/poisk/arcadia/library/cpp/lwtrace/\" target=\"_blank\">Documentation</a></li>"
              "</ul>"
            "</div></nav>"
            ;
    }

    template <class TReader>
    void ReadSnapshots(TReader& reader) const
    {
        TGuard<TMutex> g(SnapshotsMtx);
        for (const auto& kv : Snapshots) {
            reader.Push(kv.first, kv.second);
        }
    }

    void OutputTracesAndSnapshots(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        TLogSources logSources(Cleaner);
        TraceMngr->ReadTraces(logSources);
        ReadSnapshots(logSources);

        TStringStream ss;
        TTracesHtmlPrinter printer(ss);
        logSources.ForEach(printer);
        WWW_HTML(out) {
            OutputNavbar(request, out);
            out <<
                "<table class=\"table table-striped\">"
                "<tr><th>Start Time</th><th>Timeout</th><th>Name</th><th>Events</th><th>Threads</th><th></th><th></th><th></th><th></th><th></th></tr>"
                << ss.Str() <<
                "</table>"
                ;
            out << "<hr/><p><strong>Start time:</strong> " << StartTime;
            out << "<br/><strong>Build date:</strong> ";
            out << __DATE__ << " " << __TIME__ << "</p>" << Endl;
        }
    }

    void OutputProbes(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        TStringStream ss;
        TProbesHtmlPrinter printer;
        TraceMngr->ReadProbes(printer);
        printer.Output(ss);
        WWW_HTML(out) {
            OutputNavbar(request, out);
            out << ss.Str();
        }
    }

    void OutputDashboards(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        TStringStream ss;
        g_DashboardRegistry.Output(ss);

        WWW_HTML(out) {
            OutputNavbar(request, out);
            out << ss.Str();
        }
    }

    void OutputDashboard(const NMonitoring::IMonHttpRequest& request, IOutputStream& out) {
        if (!request.GetParams().Has("name")) {
            ythrow yexception() << "Cgi-parameter 'name' is not specified";
        } else {
            auto name = request.GetParams().Get("name");
            NLWTrace::TDashboard dash;
            if (!g_DashboardRegistry.Get(name, dash)) {
                ythrow yexception() << "Dashboard doesn't exist";
            }
            WWW_HTML(out) {
                OutputNavbar(request, out);
                out << "<style type='text/css'>html, body { height: 100%; }</style>";
                out << "<h2>" << dash.GetName() << "</h2>";
                if (dash.GetDescription()) {
                    out << "<h3>" << dash.GetDescription() << "</h3>";
                }
                int height = 85; // %
                int minHeight = 100; // px
                out << "<table height='" << height << "%' width='100%' cellpadding='4'><tbody height='100%' width='100%'>";
                ui32 rows = 0;
                auto maxRowSpan = [](const auto& row) {
                    ui32 rowSpan = 1;
                    for (const auto& cell : row.GetCells()) {
                        rowSpan = Max(rowSpan, cell.GetRowSpan());
                    }
                    return rowSpan;
                };
                for (const auto& row : dash.GetRows()) {
                    rows += maxRowSpan(row);
                }
                for (const auto& row : dash.GetRows()) {
                    int rowSpan = maxRowSpan(row);
                    out << "<tr align='left' valign='top' style='height:" << (height * rowSpan / rows) << "%; min-height:" << (minHeight * rowSpan)<< "px'>";
                    for (const auto& cell : row.GetCells()) {
                        TString url = cell.GetUrl();
                        TString title = cell.GetTitle();
                        TString text = cell.GetText();
                        auto rowSpan = Max<ui64>(1, cell.GetRowSpan());
                        auto colSpan = Max<ui64>(1, cell.GetColSpan());
                        if (url) {
                            if (title) {
                                out << "<td rowspan='" << rowSpan << "' colSpan='1'><a href=" << url << ">" << title << "</a><br>";
                            }
                            out << "<iframe scrolling='no' width='" << 100 * colSpan << "%' height='" << height << "%' style='border: 0' src=" << url << "></iframe></td>";
                            // Add fake cells to fix html table
                            for (ui32 left = 1; left < colSpan; ++left) {
                                out << "<td height='100%' rowspan='" << rowSpan << "' colSpan='1'>"
                                    << "<iframe scrolling='no' width='100%' height='100%' style='border: 0' src=" << "" << "></iframe></td>";
                            }
                        } else {
                            out << "<td style='font-size: 25px' align='left' rowspan='" << rowSpan << "' colSpan='" << colSpan << "'>" << text << "</td>";
                        }
                    }
                }
                out << "</tbody></table>";
            }
        }
    }

    static double ParseDouble(const TString& s)
    {
        if (s == "inf") {
            return std::numeric_limits<double>::infinity();
        } else if (s == "-inf") {
            return -std::numeric_limits<double>::infinity();
        } else {
            return FromString<double>(s);
        }
    }

    void OutputLog(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        if (request.GetParams().NumOfValues("id") == 0) {
            ythrow yexception() << "Cgi-parameter 'id' is not specified";
        } else {
            const TCgiParameters& e = request.GetParams();
            TStringStream ss;
            if (e.Get("format") == "json") {
                TLogJsonPrinter printer(ss);
                printer.OutputHeader();
                TString id = e.Get("id");
                CheckAdHocTrace(id, TDuration::Minutes(1));
                TraceMngr->ReadLog(id, printer);
                printer.OutputFooter(TraceMngr->GetTrace(id));
                out << HTTPOKJSON;
                out << ss.Str();
            } if (e.Get("format") == "json2") {
                TLogTextPrinter printer(e);
                for (const TString& id : Subvalues(e, "id")) {
                    CheckAdHocTrace(id, TDuration::Minutes(1));
                    TraceMngr->ReadLog(id, printer);
                    TraceMngr->ReadDepot(id, printer);
                }
                printer.OutputJson(ss);
                out << HTTPOKJSON;
                out << ss.Str();
            } else if (e.Get("format") == "analytics" && e.Get("aggr") == "tracks") {
                TLogTrackExtractor logTrackExtractor(e,
                    Subvalues(e, "f"),
                    Subvalues(e, "g")
                );
                for (const TString& id : Subvalues(e, "id")) {
                    CheckAdHocTrace(id, TDuration::Minutes(1));
                    TraceMngr->ReadLog(id, logTrackExtractor);
                    TraceMngr->ReadDepot(id, logTrackExtractor);
                }
                TString patternAnalyzer;
                if (e.Get("pattern")) {
                    patternAnalyzer = e.Get("ptrn_anlz");
                }
                logTrackExtractor.Run();

                TLogTextPrinter printer(e);
                const TString& distBy = patternAnalyzer;
                double sel_x1 = e.Get("sel_x1")? ParseDouble(e.Get("sel_x1")): NAN;
                double sel_x2 = e.Get("sel_x2")? ParseDouble(e.Get("sel_x2")): NAN;
                TSampleOpts opts;
                opts.ShowProvider = (e.Get("show_provider") == "y");
                if (e.Get("size_limit")) {
                    opts.SizeLimit = FromString<size_t>(e.Get("size_limit"));
                }
                logTrackExtractor.OutputSample(distBy, sel_x1, sel_x2, opts, printer);
                printer.Output(ss);
                out << HTTPOKTEXT;
                out << ss.Str();
            } else {
                TLogTextPrinter printer(e);
                for (const TString& id : Subvalues(e, "id")) {
                    CheckAdHocTrace(id, TDuration::Minutes(1));
                    TraceMngr->ReadLog(id, printer);
                    TraceMngr->ReadDepot(id, printer);
                }
                printer.Output(ss);
                out << HTTPOKTEXT;
                out << ss.Str();
            }
        }
    }

    void OutputQuery(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        if (request.GetParams().NumOfValues("id") == 0) {
            ythrow yexception() << "Cgi-parameter 'id' is not specified";
        } else {
            TString id = request.GetParams().Get("id");
            const NLWTrace::TQuery& query = TraceMngr->GetTrace(id)->GetQuery();
            TString queryStr = query.DebugString();
            WWW_HTML(out) {
                out << "<h2>Trace Query: " << id << "</h2><pre>" << queryStr;
            }
        }
    }

    void OutputBuilder(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        Y_UNUSED(request);
        WWW_HTML(out) {
            OutputNavbar(request, out);
            out << "<form class=\"form-horizontal\" action=\"?mode=new&ui=y\" method=\"POST\">";
            DIV_CLASS("form-group") {
                LABEL_CLASS_FOR("col-sm-1 control-label", "inputId") { out << "Name"; }
                DIV_CLASS("col-sm-11") {
                    out << "<input class=\"form-control\" id=\"inputId\" name=\"id\" placeholder=\"mytrace\">";
                }
            }
            DIV_CLASS("form-group") {
                LABEL_CLASS_FOR("col-sm-1 control-label", "textareaQuery") { out << "Query"; }
                DIV_CLASS("col-sm-11") {
                    out << "<textarea class=\"form-control\" id=\"textareaQuery\" name=\"query\" rows=\"10\"></textarea>";
                }
            }
            DIV_CLASS("form-group") {
                DIV_CLASS("col-sm-offset-1 col-sm-11") {
                    out << "<button type=\"submit\" class=\"btn btn-default\">Trace</button>";
                }
            }
            out << "</form>";
        }
    }

    void OutputAnalytics(const NMonitoring::IMonHttpRequest& request, TStringStream& out)
    {
        using namespace NAnalytics;
        const TCgiParameters& e = request.GetParams();

        TLogSources logSources(Cleaner);
        TraceMngr->ReadTraces(logSources);
        ReadSnapshots(logSources);

        RequireMultipleSelection(out, e, "id", "Analyze ", ListTraces(logSources));

        THolder<TLogFilter> logFilter;
        TLogAnalyzer* logAnalyzer = nullptr;
        TLogTrackExtractor* logTracks = nullptr;
        if (request.GetParams().Get("aggr") == "tracks") {
            logFilter.Reset(logTracks = new TLogTrackExtractor(e,
                Subvalues(request.GetParams(), "f"),
                Subvalues(request.GetParams(), "g")
            ));
            for (const TString& id : Subvalues(request.GetParams(), "id")) {
                CheckAdHocTrace(id, TDuration::Minutes(1));
                TraceMngr->ReadLog(id, *logTracks);
                TraceMngr->ReadDepot(id, *logTracks);
            }
        } else {
            logFilter.Reset(logAnalyzer = new TLogAnalyzer(
                Subvalues(request.GetParams(), "f"),
                Subvalues(request.GetParams(), "g"),
                request.GetParams().Get("cutts") == "y"
            ));
            for (const TString& id : Subvalues(request.GetParams(), "id")) {
                CheckAdHocTrace(id, TDuration::Minutes(1));
                TraceMngr->ReadLog(id, *logAnalyzer);
                TraceMngr->ReadDepot(id, *logAnalyzer);
            }
        }

        logFilter->FilterSelectors(out, e, "f");

        OptionalMultipleSelection(out, e, "g", "group by", logFilter->ListParamNames());
        {
            auto paramNamesList = logFilter->ListParamNames();
            if (e.Get("aggr") == "tracks") {
                paramNamesList.emplace_back("_trackMs", "_trackMs");
            }
            OptionalSelection(out, e, "s", "order by", paramNamesList);
        }

        if (e.Get("s")) {
            TVariants variants;
            variants.emplace_back("", "asc");
            variants.emplace_back("y", "desc");
            DropdownSelector<Link>(out, e, "reverse", e.Get("reverse"), "", variants);
        }

        TString aggr = e.Get("aggr");
        TVariants variants1; // MSVS2013 doesn't understand complex initializer lists
        variants1.emplace_back("", "without aggregation");
        variants1.emplace_back("hist", "as histogram");
        variants1.emplace_back("tracks", "tracks");
        DropdownSelector<Link>(out, e, "aggr", e.Get("aggr"), "", variants1);

        unsigned refresh = e.Get("refresh")?
                    FromString<unsigned>(e.Get("refresh")):
                    1000;

        if (aggr == "tracks") {
            TVariants ileVars;
            ileVars.emplace_back("0", "0");
            ileVars.emplace_back("25", "25");
            ileVars.emplace_back("50", "50");
            ileVars.emplace_back("75", "75");
            ileVars.emplace_back("", "90");
            ileVars.emplace_back("95", "95");
            ileVars.emplace_back("99", "99");
            ileVars.emplace_back("99.9", "99.9");
            ileVars.emplace_back("100", "100");
            DropdownSelector<Link>(out, e, "ile", e.Get("ile"), "and show", ileVars);
            out << "%-ile. ";
            TString patternAnalyzer;
            TString distBy;
            TString distType;
            if (e.Get("pattern")) {
                TVariants analyzePatternVars;
                analyzePatternVars.emplace_back("resTotal", "distribution by total");
                analyzePatternVars.emplace_back("resLast", "distribution by last");
                analyzePatternVars.emplace_back("covMatrix", "covariance matrix");
                DropdownSelector<Link>(
                    out, e, "ptrn_anlz", e.Get("ptrn_anlz"),
                    "Pattern", analyzePatternVars
                );
                patternAnalyzer = e.Get("ptrn_anlz");

                TVariants distTypeVars;
                distTypeVars.emplace_back("", "as is");
                distTypeVars.emplace_back("-stack", "cumulative");
                DropdownSelector<Link>(out, e, "dist_type", e.Get("dist_type"), "", distTypeVars);
                distType = e.Get("dist_type");
            } else {
                out << "<i>Select pattern for more options</i>";
            }
            logTracks->Run();

            if (e.Get("download") == "y") {
                out.Clear();
                out <<
                    "HTTP/1.1 200 Ok\r\n"
                    "Content-Type: application/force-download\r\n"
                    "Content-Transfer-Encoding: binary\r\n"
                    "Content-Disposition: attachment; filename=\"trace_chrome.json\"\r\n"
                    "\r\n"
                    ;
                logTracks->OutputChromeTrace(out, e);
                return;
            }

            NAnalytics::TTable distData;
            bool showSample = false;
            TLogTextPrinter printer(e);

            if (patternAnalyzer == "resTotal" || patternAnalyzer == "resLast") {
                distBy = patternAnalyzer;
                distData = logTracks->Distribution(distBy, "", "", e.Get("width"));
                double sel_x1 = e.Get("sel_x1")? ParseDouble(e.Get("sel_x1")): NAN;
                double sel_x2 = e.Get("sel_x2")? ParseDouble(e.Get("sel_x2")): NAN;
                if (!isnan(sel_x1) && !isnan(sel_x2)) {
                    showSample = true;
                    TSampleOpts opts;
                    opts.ShowProvider = (e.Get("show_provider") == "y");
                    if (e.Get("size_limit")) {
                        opts.SizeLimit = FromString<size_t>(e.Get("size_limit"));
                    }
                    logTracks->OutputSample(distBy, sel_x1, sel_x2, opts, printer);
                }
            }

            TString selectors = out.Str();
            out.Clear();
            out << NMonitoring::HTTPOKHTML;
            out << "<!DOCTYPE html>" << Endl;
            HTML(out) {
                HTML_TAG() {
                    HEAD() {
                        out << NResource::Find("lwtrace/mon/static/analytics.header.html") << Endl;
                        if (distBy) {
                            out <<
                            "<script type=\"text/javascript\">\n"
                            "$(function() {\n"
                            "    var dataurl = null;\n"
                            "    var datajson = " << ToJsonFlot(distData, distBy, {"_count_sum" + distType}) << ";\n"
                            "    var refreshPeriod = 0;\n"
                            "    var xn = \"" << distBy << "\";\n"
                            "    var navigate = false;\n"
                              << NResource::Find("lwtrace/mon/static/analytics.js") <<
                            "    embededMode();"
                            "    enableSelection();"
                            "});\n"
                            "</script>\n";
                        }
                        // Show download button
                        out <<
                        "<script type=\"text/javascript\">"
                        "$(function() {"
                        "    $(\"#download-btn\").click(function(){window.location.href='"
                                                         << MakeUrlAdd(e, "download", "y") <<
                                                        "';});"
                        "    $(\"#download-btn\").removeClass(\"hidden\");"
                        "});"
                        "</script>\n";
                    }
                    BODY() {
                        // Wrap selectors with navbar
                        { TSelectorsContainer sc(out);
                            out << selectors;
                        }

                        logTracks->OutputTable(out, e);
                        if (distBy) {
                            out << NResource::Find("lwtrace/mon/static/analytics.flot.html") << Endl;
                            if (showSample) {
                                static const THashSet<TString> keepParams = {
                                    "f",
                                    "g",
                                    "head",
                                    "tail",
                                    "s",
                                    "reverse",
                                    "cutts",
                                    "showts",
                                    "show_provider",
                                    "size_limit",
                                    "aggr",
                                    "id",
                                    "pattern",
                                    "ptrn_anlz",
                                    "sel_x1",
                                    "sel_x2"
                                };
                                TCgiParameters cgiParams;
                                for (const auto& kv : request.GetParams()) {
                                    if (keepParams.count(kv.first)) {
                                        cgiParams.insert(kv);
                                    }
                                }
                                cgiParams.insert(std::pair<TString, TString>("mode", "log"));
                                BtnHref<Button|Medium>(out, "Open logs", MakeUrlAdd(cgiParams, "format", "analytics"));
                                out << "<pre>\n";
                                printer.Output(out);
                                out << "</pre>\n";
                            }
                        }

                        if (patternAnalyzer == "covMatrix") {
                            logTracks->OutputSliceCovarianceMatrix(out, e);
                        }
                    }
                }
            }
        } else {
            double width = e.Get("width")? FromString<double>(e.Get("width")): 99;

            NAnalytics::TTable data;
            if (aggr == "") {
                data = logAnalyzer->GetTable();
            } else if (aggr == "hist") {
                RequireSelection(out, e, "bn", "by", logFilter->ListParamNames());
                const NAnalytics::TTable& inputTable = logAnalyzer->GetTable();
                TString bn = e.Get("bn");
                double b1 = e.Get("b1")? FromString<double>(e.Get("b1")): MinValue(bn, inputTable);
                double b2 = e.Get("b2")? FromString<double>(e.Get("b2")): MaxValue(bn, inputTable);
                if (isfinite(b1) && isfinite(b2)) {
                    WWW_CHECK(b1 <= b2, "invalid xrange [%le; %le]", b1, b2);
                    double dx = e.Get("dx")? FromString<double>(e.Get("dx")): (b2-b1)/width;
                    data = HistogramAll(inputTable, e.Get("bn"), b1, b2, dx);
                } else {
                    // Empty table -- it's ok -- leave data table empty
                }
            }

            TString xn = e.Get("xn");

            TString outFormat = e.Get("out");
            TVariants variants2;
            variants2.emplace_back("html", "table");
            variants2.emplace_back("flot", "chart");
            variants2.emplace_back("gantt", "gantt");
            variants2.emplace_back("text", "text");
            variants2.emplace_back("csv", "CSV");
            variants2.emplace_back("json_flot", "JSON");

            RequireSelection(out, e, "out", "and show", variants2);
            if (outFormat == "csv") {
                TString sep = e.Get("sep")? e.Get("sep"): TString("\t");
                out.Clear();
                out << NMonitoring::HTTPOKTEXT;
                out << ToCsv(data, sep, e.Get("head") != "n");
            } else if (outFormat == "html") {
                TString selectors = out.Str();
                out.Clear();
                WWW_HTML(out) {
                    // Wrap selectors with navbar
                    { TSelectorsContainer sc(out);
                        out << selectors;
                    }
                    out << ToHtml(data);
                }
            } else if (outFormat == "json_flot") {
                SeriesSelectors(out, e, "xn", "yns", data);
                out.Clear();
                out << NMonitoring::HTTPOKJSON;
                out << ToJsonFlot(data, xn, SplitString(e.Get("yns"), ":"));
            } else if (outFormat == "flot") {
                SeriesSelectors(out, e, "xn", "yns", data);
                TString selectors = out.Str();

                TVector<TString> ynos = SplitString(e.Get("yns"), ":");
                out.Clear();
                out << NMonitoring::HTTPOKHTML;
                out << "<!DOCTYPE html>" << Endl;
                HTML(out) {
                    HTML_TAG() {
                        HEAD() {
                            out << NResource::Find("lwtrace/mon/static/analytics.header.html") << Endl;
                            out <<
                            "<script type=\"text/javascript\">\n"
                            "$(function() {\n"
                            "    var dataurl = \"" << EscapeJSONString(MakeUrl(e, "out", "json_flot")) << "\";\n"
                            "    var refreshPeriod = " << refresh << ";\n"
                            "    var xn = \"" << ParseName(xn) << "\";\n"
                            "    var navigate = true;\n"
                            << NResource::Find("lwtrace/mon/static/analytics.js") <<
                            "});\n"
                            "</script>\n"
                            ;
                        }
                        BODY() {
                            // Wrap selectors with navbar
                            { TSelectorsContainer sc(out);
                                out << selectors;
                            }
                            out << NResource::Find("lwtrace/mon/static/analytics.flot.html") << Endl;
                        }
                    }
                }
            } else if (outFormat == "gantt") {
                TString selectors = out.Str();
                out.Clear();
                out << NMonitoring::HTTPOKHTML;
                out << "<!DOCTYPE html>" << Endl;
                HTML(out) {
                    HTML_TAG() {
                        HEAD() {
                            out << NResource::Find("lwtrace/mon/static/analytics.header.html") << Endl;
                            out <<
                            "<script type=\"text/javascript\">\n"
                            "$(function() {\n"
                            "    var dataurl = \"" << EscapeJSONString(MakeUrl(e, {{"mode", "log"}, {"format", "json2"}, {"gantt",""}})) << "\";\n"
                            "    var refreshPeriod = " << refresh << ";\n"
                            "    var xn = \"" << ParseName(xn) << "\";\n"
                            "    var navigate = true;\n"
                            << NResource::Find("lwtrace/mon/static/analytics.js") <<
                            "});\n"
                            "</script>\n"
                            ;
                        }
                        BODY() {
                            // Wrap selectors with navbar
                            { TSelectorsContainer sc(out);
                                out << selectors;
                            }
                            out << NResource::Find("lwtrace/mon/static/analytics.gantt.html") << Endl;
                        }
                    }
                }
            } else if (outFormat = "text") {
                out << " <input type='text' id='logsLimit' size='2' placeholder='Limit'>" << Endl;
                out <<
                    R"END(<script>
                    {
                        var url = new URL(window.location.href);
                        if (url.searchParams.has('head')) {
                            document.getElementById('logsLimit').value = url.searchParams.get('head');
                        }
                    }

                    $('#logsLimit').on('keypress', function(ev) {
                        if (ev.keyCode == 13) {
                            var url = new URL(window.location.href);
                            var limit_value = document.getElementById('logsLimit').value;
                            if (limit_value && !isNaN(limit_value)) {
                                url.searchParams.set('head', limit_value);
                                window.location.href = url.toString();
                            } else if (!limit_value) {
                                url.searchParams.delete('head');
                                window.location.href = url.toString();
                            }
                        }
                    });
                    </script>)END";
                TString selectors = out.Str();
                TLogTextPrinter printer(e);
                TStringStream ss;
                for (const TString& id : Subvalues(e, "id")) {
                    CheckAdHocTrace(id, TDuration::Minutes(1));
                    TraceMngr->ReadLog(id, printer);
                    TraceMngr->ReadDepot(id, printer);
                }
                printer.Output(ss);

                out.Clear();
                out << NMonitoring::HTTPOKHTML;
                out << "<!DOCTYPE html>" << Endl;
                HTML(out) {
                    HTML_TAG() {
                        HEAD() {
                            out << NResource::Find("lwtrace/mon/static/analytics.header.html") << Endl;
                        }
                        BODY() {
                            // Wrap selectors with navbar
                            { TSelectorsContainer sc(out);
                                out << selectors;
                            }
                            static const THashSet<TString> keepParams = {
                                "s",
                                "head",
                                "reverse",
                                "cutts",
                                "showts",
                                "id",
                                "out"
                            };
                            TCgiParameters cgiParams;
                            for (const auto& kv : request.GetParams()) {
                                if (keepParams.count(kv.first)) {
                                    cgiParams.insert(kv);
                                }
                            }
                            cgiParams.insert(std::pair<TString, TString>("mode", "analytics"));

                            auto toggledButton = [&out, &e, &cgiParams] (const TString& label, const TString& cgiKey) {
                                if (e.Get(cgiKey) == "y") {
                                    BtnHref<Button|Medium>(out, label, MakeUrlErase(cgiParams, cgiKey, "y"), true);
                                } else {
                                    BtnHref<Button|Medium>(out, label, MakeUrlAdd(cgiParams, cgiKey, "y"));
                                }
                            };
                            toggledButton("Cut Tails", "cutts");
                            toggledButton("Relative Time", "showts");

                            cgiParams.erase("mode");
                            cgiParams.insert(std::pair<TString, TString>("mode", "log"));
                            BtnHref<Button|Medium>(out, "Fullscreen", MakeUrlAdd(cgiParams, "format", "text"));
                            out << "<pre>\n";
                            out << ss.Str() << Endl;
                            out << "</pre>\n";
                        }
                    }
                }
            }
        }
    }

    TDuration GetGetTimeout(const NMonitoring::IMonHttpRequest& request)
    {
        return (request.GetParams().Has("timeout")?
                TDuration::Seconds(FromString<double>(request.GetParams().Get("timeout"))):
                TDuration::Max());
    }

    void PostNew(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        WWW_CHECK(request.GetPostParams().Has("id"), "POST parameter 'id' is not specified");
        const TString& id = request.GetPostParams().Get("id");
        bool ui = (request.GetParams().Get("ui") == "y");
        TDuration timeout = GetGetTimeout(request);
        if (!CheckAdHocTrace(id, timeout)) {
            NLWTrace::TQuery query;
            TString queryStr = request.GetPostParams().Get("query");
            if (!ui) {
                queryStr = Base64Decode(queryStr); // Needed for trace.sh (historically)
            }
            WWW_CHECK(queryStr, "Empty trace query");
            bool parsed = NProtoBuf::TextFormat::ParseFromString(queryStr, &query);
            WWW_CHECK(parsed, "Trace query text protobuf parse failed"); // TODO[serxa]: report error line/col and message
            TraceMngr->New(id, query);
            Cleaner.Postpone(id, timeout, false);
        } else {
            WWW_CHECK(!request.GetPostParams().Has("query"), "trace id '%s' is reserved for ad-hoc traces", id.data());
        }
        if (ui) {
            WWW_HTML(out) {
                out <<
                "<div class=\"jumbotron alert-success\">"
                "<h2>Trace created successfully</h2>"
                "</div>"
                "<script type=\"text/javascript\">\n"
                "$(function() {\n"
                "    setTimeout(function() {"
                "        window.location.replace('?');"
                "    }, 1000);"
                "});\n"
                "</script>\n";
            }
        } else {
            out << HTTPOKTEXT;
            out << "OK\n";
        }
    }

    void PostDelete(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        WWW_CHECK(request.GetPostParams().Has("id"), "POST parameter 'id' is not specified");
        const TString& id = request.GetPostParams().Get("id");
        bool ui = (request.GetParams().Get("ui") == "y");
        TraceMngr->Delete(id);
        Cleaner.Forget(id);
        if (ui) {
            WWW_HTML(out) {
                out <<
                "<div class=\"jumbotron alert-success\">"
                "<h2>Trace deleted successfully</h2>"
                "</div>"
                "<script type=\"text/javascript\">\n"
                "$(function() {\n"
                "    setTimeout(function() {"
                "        window.location.replace('?');"
                "    }, 1000);"
                "});\n"
                "</script>\n";
            }
        } else {
            out << HTTPOKTEXT;
            out << "OK\n";
        }
    }

    void PostSnapshot(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        WWW_CHECK(request.GetPostParams().Has("id"), "POST parameter 'id' is not specified");
        const TString& id = request.GetPostParams().Get("id");
        bool ui = (request.GetParams().Get("ui") == "y");
        TInstant now = TInstant::Now();

        TGuard<TMutex> g(SnapshotsMtx);
        const NLWTrace::TSession* trace = TraceMngr->GetTrace(id);
        struct tm tm0;
        TString sid = id + Strftime("_%Y%m%d-%H%M%S", now.GmTime(&tm0));
        TAtomicSharedPtr<NLWTrace::TLogPb>& pbPtr = Snapshots[sid];
        pbPtr.Reset(new NLWTrace::TLogPb());
        trace->ToProtobuf(*pbPtr);
        pbPtr->SetName(sid);
        if (ui) {
            WWW_HTML(out) {
                out <<
                "<div class=\"jumbotron alert-success\">"
                "<h2>Snapshot created successfully</h2>"
                "</div>"
                "<script type=\"text/javascript\">\n"
                "$(function() {\n"
                "    setTimeout(function() {"
                "        window.location.replace('?');"
                "    }, 1000);"
                "});\n"
                "</script>\n";
            }
        } else {
            out << HTTPOKTEXT;
            out << "OK\n";
        }
    }

    void PostSetTimeout(const NMonitoring::IMonHttpRequest& request, IOutputStream& out)
    {
        WWW_CHECK(request.GetPostParams().Has("id"), "POST parameter 'id' is not specified");
        const TString& id = request.GetPostParams().Get("id");
        TDuration timeout = GetGetTimeout(request);
        bool ui = (request.GetParams().Get("ui") == "y");
        Cleaner.Postpone(id, timeout, true);
        if (ui) {
            WWW_HTML(out) {
                out <<
                "<div class=\"jumbotron alert-success\">"
                "<h2>Timeout changed successfully</h2>"
                "</div>"
                "<script type=\"text/javascript\">\n"
                "$(function() {\n"
                "    setTimeout(function() {"
                "        window.location.replace('?');"
                "    }, 1000);"
                "});\n"
                "</script>\n";
            }
        } else {
            out << HTTPOKTEXT;
            out << "OK\n";
        }
    }

    void RegisterDashboard(const TString& dashConfig) {
        g_DashboardRegistry.Register(dashConfig);
    }

private:
    // Returns true iff trace is ad-hoc and ensures trace is created
    bool CheckAdHocTrace(const TString& id, TDuration timeout)
    {
        TAdHocTraceConfig cfg;
        if (cfg.ParseId(id)) {
            if (!TraceMngr->HasTrace(id)) {
                TraceMngr->New(id, cfg.Query());
            }
            Cleaner.Postpone(id, timeout, false);
            return true;
        }
        return false;
    }
};

void RegisterPages(NMonitoring::TIndexMonPage* index, bool allowUnsafe) {
    THolder<NLwTraceMonPage::TLWTraceMonPage> p = MakeHolder<NLwTraceMonPage::TLWTraceMonPage>(allowUnsafe);
    index->Register(p.Release());

#define WWW_STATIC_FILE(file, type) \
        index->Register(new TResourceMonPage(file, file, NMonitoring::TResourceMonPage::type));
    WWW_STATIC_FILE("lwtrace/mon/static/common.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/common.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/css/bootstrap.min.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/css/d3-gantt.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/css/jquery.treegrid.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/analytics.css", CSS);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.eot", FONT_EOT);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.svg", SVG);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.ttf", FONT_TTF);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff2", FONT_WOFF2);
    WWW_STATIC_FILE("lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff", FONT_WOFF);
    WWW_STATIC_FILE("lwtrace/mon/static/img/collapse.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/expand.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/file.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/img/folder.png", PNG);
    WWW_STATIC_FILE("lwtrace/mon/static/js/bootstrap.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3.v4.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3-gantt.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/d3-tip-0.8.0-alpha.1.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/filesaver.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.extents.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.navigate.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.flot.selection.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.treegrid.bootstrap3.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.treegrid.min.js", JAVASCRIPT);
    WWW_STATIC_FILE("lwtrace/mon/static/js/jquery.url.min.js", JAVASCRIPT);
#undef WWW_STATIC_FILE
}

NLWTrace::TProbeRegistry& ProbeRegistry() {
    return g_Probes;
}

NLWTrace::TManager& TraceManager(bool allowUnsafe) {
    return allowUnsafe? g_UnsafeManager: g_SafeManager;
}

TDashboardRegistry& DashboardRegistry() {
    return g_DashboardRegistry;
}

} // namespace NLwTraceMonPage
