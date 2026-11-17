// Keep the real subscriber implementation in this regression. Only tablet-pipe
// routing is replaced; order/generation handling and owner delivery stay intact.
// Preload its headers before the local macros so their declarations are unchanged.
#include "console_configs_subscriber.h"
#include "console.h"
#include "util.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <util/system/hostname.h>
#include <util/generic/ptr.h>
#include <utility>
#include "ut_configs_subscriber_pipe.h"

#define TConfigsSubscriber TQueueTestConfigsSubscriber
#define CreateConfigsSubscriber CreateQueueTestConfigsSubscriber
#define NTabletPipe NQueueTestPipe
#include "console_configs_subscriber.cpp"
#undef NTabletPipe
#undef CreateConfigsSubscriber
#undef TConfigsSubscriber
