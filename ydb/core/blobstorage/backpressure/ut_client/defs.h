#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_server.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <library/cpp/testing/unittest/registar.h>
