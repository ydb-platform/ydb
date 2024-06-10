#pragma once

#ifdef GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fpqconfig_2eproto
#   define __PQCONFIG_WAS_INCLUDED__ 1
#else
#   define __PQCONFIG_WAS_INCLUDED__ 0
#endif

#include "defs.h"
#include "appdata_fwd.h"
#include "channel_profiles.h"
#include "domain.h"
#include "feature_flags.h"
#include "nameservice.h"
#include "tablet_types.h"
#include "resource_profile.h"
#include "event_filter.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/library/pdisk_io/aio.h>

#include <ydb/core/base/event_filter.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


namespace NKikimr {
} // NKikimr

#if defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fpqconfig_2eproto) && !__PQCONFIG_WAS_INCLUDED__
#   error NEVER EVER INCLUDE pqconfig.pb.h FROM appdata.h
#endif

#undef __PQCONFIG_WAS_INCLUDED__
