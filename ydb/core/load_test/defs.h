#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/load_test/defs.h
#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/protos/testload.pb.h>

#include <ydb/core/load_test/events.h>
