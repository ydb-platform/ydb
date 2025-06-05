#pragma once

#include <ydb/core/mind/bscontroller/defs.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/bscontroller/types.h>

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

#include <ydb/core/util/actorsys_test/testactorsys.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;
