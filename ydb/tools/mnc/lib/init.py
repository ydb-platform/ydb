import logging
import os

from ydb.core.protos import msgbus_pb2 as msgbus
from ydb.tests.library.clients.kikimr_client import KiKiMRMessageBusClient
from ydb.tests.library.clients.kikimr_config_client import ConfigClient

from ydb.tools.mnc.lib import configs, progress


logger = logging.getLogger(__name__)


@progress.with_parent_task
async def act_static(config: dict, parent_task: progress.TaskNode = None):
    bootstrap_task = await parent_task.add_subtask("[bold green]Bootstrap", total=1)
    configs.init_ports(config)
    client = ConfigClient(config['hosts'][0], configs.first_static_grpc_port)
    client.bootstrap_cluster("multinode_cluster")
    await bootstrap_task.update(advance=1)
    await bootstrap_task.update(visible=False)
    return True


@progress.with_parent_task
async def act_dynamic(config: dict, parent_task: progress.TaskNode = None):
    configs.init_ports(config)
    client = KiKiMRMessageBusClient(config['hosts'][0], configs.first_static_grpc_port)
    domain = config['domain']
    database_task = await parent_task.add_subtask("[bold green]Init databases", total=len(domain['databases']))

    for db in domain['databases']:
        console_request = msgbus.TConsoleRequest()
        console_request.CreateTenantRequest.Request.path = os.path.join('/', domain['name'], db['name'])
        resources = console_request.CreateTenantRequest.Request.resources
        pool = resources.storage_units.add()
        pool.unit_kind = config['device_type'].lower()
        pool.count = db['storage_group_count']
        client.invoke(console_request, 'ConsoleRequest')
        await database_task.update(advance=1)
    await database_task.update(visible=False)

    return True
