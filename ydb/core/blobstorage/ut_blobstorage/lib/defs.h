#pragma once

#include <ydb/core/base/hive.h>
#include <ydb/core/blob_depot/blob_depot.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/mind/dynamic_nameserver.h>
#include <ydb/core/mind/labels_maintainer.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/util/testactorsys.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/rusage.h>
#include <util/random/fast.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;
