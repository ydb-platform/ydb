#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/blobstorage/pdisk/defs.h
#include <ydb/core/base/defs.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <util/system/sanitizers.h>
#include <ydb/library/actors/core/monotonic_provider.h>
