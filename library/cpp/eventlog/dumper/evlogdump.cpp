#include "common.h"
#include "evlogdump.h"

#include <library/cpp/eventlog/evdecoder.h>
#include <library/cpp/eventlog/iterator.h>
#include <library/cpp/eventlog/logparser.h>

#include <library/cpp/getopt/last_getopt.h>

#ifndef NO_SVN_DEPEND
#include <library/cpp/svnversion/svnversion.h>
#endif

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/type.h>

using namespace NEventLog;
namespace nlg = NLastGetopt;

IFrameFilterRef CreateDurationFrameFilter(const TString& params) {
    TStringBuf d(params);
    TStringBuf l, r;
    d.Split(':', l, r);
    TDuration min = l.empty() ? TDuration::Zero() : FromString<TDuration>(l);
    TDuration max = r.empty() ? TDuration::Max() : FromString<TDuration>(r);

    if (min > TDuration::Zero() || max < TDuration::Max()) {
        return new TDurationFrameFilter(min.GetValue(), max.GetValue());
    }

    return nullptr;
}

IFrameFilterRef CreateFrameIdFilter(const ui32 frameId) {
    return new TFrameIdFrameFilter(frameId);
}

IFrameFilterRef CreateContainsEventFrameFilter(const TString& args, const IEventFactory* factory) {
    return new TContainsEventFrameFilter(args, factory);
}

void ListAllEvents(IEventFactory* factory, IOutputStream* out) {
    Y_ENSURE(out);

    for (TEventClass eventClass = factory->EventClassBegin(); eventClass < factory->EventClassEnd(); eventClass++) {
        THolder<TEvent> event(factory->CreateLogEvent(eventClass));

        const TStringBuf& name = event->GetName();
        if (name) {
            (*out) << name << "\n";
        }
    }
}

void ListEventFieldsByEventId(const TEventClass eventClass, IEventFactory* factory, IOutputStream* out) {
    THolder<TEvent> event(factory->CreateLogEvent(eventClass));
    const NProtoBuf::Message* message = event->GetProto();
    const google::protobuf::Descriptor* descriptor = message->GetDescriptor();

    (*out) << descriptor->DebugString();
}

void ListEventFields(const TString& eventName, IEventFactory* factory, IOutputStream* out) {
    Y_ENSURE(out);

    TEventClass eventClass;

    try {
        eventClass = factory->ClassByName(eventName);
    } catch (yexception& e) {
        if (!TryFromString<TEventClass>(eventName, eventClass)) {
            (*out) << "Cannot dervie event class from event name: " << eventName << Endl;
            return;
        }
    }

     ListEventFieldsByEventId(eventClass, factory, out);
}

int PrintHelpEvents(const TString& helpEvents, IEventFactory* factory) {
    if (helpEvents == "all") {
        ListAllEvents(factory, &Cout);
    } else if (IsNumber(helpEvents)) {
        ListEventFieldsByEventId(IntFromString<ui32, 10>(helpEvents), factory, &Cout);
    } else {
        ListEventFields(helpEvents, factory, &Cout);
    }

    return 0;
}

int IterateEventLog(IEventFactory* fac, IEventProcessor* proc, int argc, const char** argv) {
    class TProxy: public ITunableEventProcessor {
    public:
        TProxy(IEventProcessor* proc)
            : Processor_(proc)
        {
        }

        void AddOptions(NLastGetopt::TOpts&) override {
        }

        void SetOptions(const TEvent::TOutputOptions& options) override {
            Processor_->SetOptions(options);
        }

        void ProcessEvent(const TEvent* ev) override {
            Processor_->ProcessEvent(ev);
        }

        bool CheckedProcessEvent(const TEvent* ev) override {
            return Processor_->CheckedProcessEvent(ev);
        }

    private:
        IEventProcessor* Processor_ = nullptr;
    };

    TProxy proxy(proc);
    return IterateEventLog(fac, &proxy, argc, argv);
}

int IterateEventLog(IEventFactory* fac, ITunableEventProcessor* proc, int argc, const char** argv) {
    nlg::TOpts opts;
    opts.AddHelpOption('?');
    opts.AddHelpOption('h');
    opts.SetTitle(
        "Search EventLog dumper\n\n"
        "Examples:\n"
        "evlogdump -s 1228484839332219 -e 1228484840332219 event_log\n"
        "evlogdump -s 2008-12-12T12:00:00+03 -e 2008-12-12T12:05:00+03 event_log\n"
        "evlogdump -s 12:40 -e 12:55 event_log\n"
        "evlogdump -i 301,302,303 event_log\n"
        "evlogdump -i SubSourceResponse,SubSourceOk,SubSourceError event_log\n"
        "evlogdump -d 40ms:60ms event_log\n"
        "evlogdump -c ReqId:ReqId:1563185655856304-30407004790538173600035-vla1-0671-p1\n"
        "\n"
        "Fine manual: https://nda.ya.ru/3Tpo3L\n"
        "\n"
        "If in trouble using this (or bugs encountered),\n"
        "please don't hesitate to ask middlesearch/basesearch dev team\n"
        "(vmordovin@, mvel@, etc)\n");

    TString start;
    opts.AddLongOption(
            's', "start-time",
            "Start time (Unix time in microseconds, ISO8601, or HH:MM[:SS] in the last 24 hours).\n"
            "Long frames can produce inaccurate cropping and event loss in dump (see SEARCH-5576)")
        .Optional()
        .StoreResult(&start);

    TString end;
    opts.AddLongOption(
            'e', "end-time",
            "End time (Unix time in microseconds, ISO8601, or HH:MM[:SS] in the last 24 hours)")
        .Optional()
        .StoreResult(&end);

    TOptions o;

    opts.AddLongOption(
            'm', "max-request-duration",
            "Max duration of a request in the log, in us")
        .Optional()
        .StoreResult(&o.MaxRequestDuration);

    bool oneFrame = false;
    opts.AddLongOption(
            'f', "one-frame",
            "Treat input file as single frame dump (e.g. from gdb)")
        .NoArgument()
        .Optional()
        .StoreValue(&oneFrame, true);

    opts.AddLongOption(
            'v', "version",
            "Print program version")
        .NoArgument()
        .Optional()
        .Handler(&nlg::PrintVersionAndExit);

    TString includeEventList;
    opts.AddLongOption(
            'i', "event-list",
            "Comma-separated list of included event class IDs or names "
            "(e.g. 265,287 or MainTaskError,ContextCreated)")
        .Optional()
        .StoreResult(&includeEventList)
        .StoreValue(&o.EnableEvents, true);

    TString durationFilterStr;
    opts.AddLongOption(
            'd', "duration",
            "DurationMin[:DurationMax] (values must contain a unit, valid examples are: 50us, 50ms, 50s)\n"
            "(show frames with duration greater or equal DurationMin [and less or equal than DurationMax])")
        .Optional()
        .StoreResult(&durationFilterStr);

    const ui32 INVALID_FRAME_ID = ui32(-1);
    ui32 frameIdFilter = INVALID_FRAME_ID;
    opts.AddLongOption(
            'n', "frame-id",
            "Filter frame with given id\n")
        .Optional()
        .StoreResult(&frameIdFilter);

    TString excludeEventList;
    opts.AddLongOption(
            'x', "exclude-list",
            "Comma-separated list of excluded event class IDs or names (see also --event-list option)")
        .Optional()
        .StoreResult(&excludeEventList)
        .StoreValue(&o.EnableEvents, false);

    opts.AddLongOption(
            'o', "frame-order",
            "Order events by time only inside a frame (faster)")
        .NoArgument()
        .Optional()
        .StoreValue(&o.ForceWeakOrdering, true);

    opts.AddLongOption(
            'O', "global-order",
            "Globally but lossy (may lose some frames) order events by time only (slower)")
        .NoArgument()
        .Optional()
        .StoreValue(&o.ForceStrongOrdering, true);

    opts.AddLongOption(
        "lossless-strong-order",
        "Globally order events by time only (super slow and memory heavy)")
        .NoArgument()
        .Optional()
        .StoreValue(&o.ForceLosslessStrongOrdering, true)
        .StoreValue(&o.ForceStrongOrdering, true);

    opts.AddLongOption(
            'S', "stream",
            "Force stream mode (for log files with invalid ending)")
        .NoArgument()
        .Optional()
        .StoreValue(&o.ForceStreamMode, true);

    opts.AddLongOption(
            't', "tail",
            "Open file like `tail -f` does")
        .NoArgument()
        .Optional()
        .StoreValue(&o.TailFMode, true);

    bool unescaped = false;
    opts.AddLongOption(
            'u', "unescaped",
            "Don't escape \\t, \\n chars in message fields")
        .NoArgument()
        .StoreValue(&unescaped, true);

    bool json = false;
    opts.AddLongOption(
            'j', "json",
            "Print messages as JSON values")
        .NoArgument()
        .StoreValue(&json, true);

    TEvent::TOutputOptions outputOptions;
    opts.AddLongOption(
            'r', "human-readable",
            "Print some fields (e.g. timestamp) in human-readable format, add time offsets")
        .NoArgument()
        .StoreValue(&outputOptions.HumanReadable, true);

    //Supports only fields of type string
    TString containsEvent;
    opts.AddLongOption(
            'c', "contains-event",
            "Only print frames that contain events whose particular field's value matches given value.\n"
            "Match group should be provided in format: EventName:FieldName:ValueToMatch\n"
            "If more than one match group is provided, they should be separated by / delimiter.\n"
            "Nested fields are supported through \'.\' in FieldName.\n"
            "Symbols \'/\' and \':\' in EventName, FieldName or ValueToMatch must be escaped.\n"
            "NOTE: bool values are 1 and 0.")
        .Optional()
        .StoreResult(&containsEvent);

    TString helpEvents;
    opts.AddLongOption("help-events")
        .RequiredArgument("EVENT, EVENT_ID or string \"all\"")
        .Optional()
        .StoreResult(&helpEvents);

    proc->AddOptions(opts);

    opts.SetFreeArgsMin(0);
    opts.SetFreeArgsMax(1);
    opts.SetFreeArgTitle(0, "<eventlog>", "Event log file");

    try {
        const nlg::TOptsParseResult optsRes(&opts, argc, argv);

        if (helpEvents) {
            return PrintHelpEvents(helpEvents, fac);
        }

        TVector<TString> freeArgs = optsRes.GetFreeArgs();
        if (freeArgs) {
            o.FileName = freeArgs[0];
        }

        ui32 conditionsHappened = 0;

        if (frameIdFilter != INVALID_FRAME_ID) {
            ++conditionsHappened;
        }
        if (containsEvent) {
            ++conditionsHappened;
        }
        if (durationFilterStr) {
            ++conditionsHappened;
        }

        if (conditionsHappened > 1) {
            throw nlg::TUsageException() << "You can only use no more than one of frame id, duration or contains event frame filters.";
        }

        if (frameIdFilter != INVALID_FRAME_ID) {
            o.FrameFilter = CreateFrameIdFilter(frameIdFilter);
        } else if (durationFilterStr) {
            o.FrameFilter = CreateDurationFrameFilter(durationFilterStr);
        } else if (containsEvent) {
            o.FrameFilter = CreateContainsEventFrameFilter(containsEvent, fac);
        }

        // handle some inconsistencies
        if (o.ForceWeakOrdering && o.ForceStrongOrdering) {
            throw nlg::TUsageException() << "You can use either strong (-O) or weak (-o) ordering. ";
        }
        if (includeEventList && excludeEventList) {
            throw nlg::TUsageException() << "You can use either include (-i) or exclude (-x) events list. ";
        }
        if (unescaped && json) {
            throw nlg::TUsageException() << "You can use either unescaped (-u) or json (-j) output format. ";
        }

        proc->CheckOptions();
    } catch (...) {
        Cerr << "Usage error: " << CurrentExceptionMessage() << Endl;
        return 1;
    }

    o.EvList = o.EnableEvents ? includeEventList : excludeEventList;
    if (json) {
        outputOptions.OutputFormat = TEvent::TOutputFormat::Json;
    } else if (unescaped) {
        outputOptions.OutputFormat = TEvent::TOutputFormat::TabSeparatedRaw;
    } else {
        outputOptions.OutputFormat = TEvent::TOutputFormat::TabSeparated;
    }

    IEventProcessor* eventProcessor = NEvClass::Processor();
    // FIXME(mvel): A little of hell here: `proc` and `eventProcessor` are `IEventProcessor`s
    // So we need to set options for BOTH!

    eventProcessor->SetOptions(outputOptions);
    proc->SetOptions(outputOptions);
    proc->SetEventProcessor(eventProcessor);

    if (oneFrame) {
        THolder<IInputStream> fileInput;

        // this is for coredumps analysis, usage:
        // gdb: dump binary memory framefile Data_ Data_ + Pos_
        // evlogdump -f framefile
        IInputStream* usedInput = nullptr;

        if (o.FileName.size()) {
            fileInput.Reset(new TUnbufferedFileInput(o.FileName));
            usedInput = fileInput.Get();
        } else {
            usedInput = &Cin;
        }

        try {
            for (;;) {
                proc->ProcessEvent(DecodeEvent(*usedInput, true, 0, nullptr, fac).Get());
            }
        } catch (...) {
        }

        return 0;
    }

    o.StartTime = ParseTime(start, MIN_START_TIME);
    o.EndTime = ParseTime(end, MAX_END_TIME);

    try {
        THolder<IIterator> it = CreateIterator(o, fac);

        while (const auto ev = it->Next()) {
            if (!proc->CheckedProcessEvent(ev.Get())) {
                break;
            }
        }

        return 0;
    } catch (...) {
        Cout.Flush();
        Cerr << "Error occured: " << CurrentExceptionMessage() << Endl;
    }

    return 1;
}
