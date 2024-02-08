#pragma once

#include <ydb/core/blobstorage/defs.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/query/query_spacetracker.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/crypto/crypto.h>
#include <ydb/core/blobstorage/nodewarden/group_stat_aggregator.h>
#include <ydb/core/util/log_priority_mute_checker.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <ydb/core/base/interconnect_channels.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/overloaded.h>
#include <util/string/escape.h>
#include <library/cpp/monlib/service/pages/templates.h>
