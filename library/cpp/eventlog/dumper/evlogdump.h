#pragma once

#include "tunable_event_processor.h"

#include <library/cpp/eventlog/eventlog.h>

int IterateEventLog(IEventFactory* fac, IEventProcessor* proc, int argc, const char** argv);
int IterateEventLog(IEventFactory* fac, ITunableEventProcessor* proc, int argc, const char** argv);

// added for using in infra/libs/logger/log_printer.cpp
int PrintHelpEvents(const TString& helpEvents, IEventFactory* factory);
