import asyncio

import grpc
import requests

from ydb.public.api.grpc.ydb_topic_v1_pb2_grpc import TopicServiceStub
from ydb.public.api.protos import ydb_topic_pb2 as topic
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.functional.security.lib.security_test_helpers import get_nodelist_ids


ADMIN = 'root@builtin'


def request(method, url, database, token):
    return requests.request(
        method,
        url,
        params={'database': database} if database else {},
        headers={'Authorization': token} if token else {},
        verify=False,
        timeout=10,
    )


async def open_writer(channel, database, path):
    stream = TopicServiceStub(channel).StreamWrite(
        metadata=(('x-ydb-database', database), ('x-ydb-auth-ticket', ADMIN)),
        timeout=60,
        wait_for_ready=True,
    )
    init = topic.StreamWriteMessage.InitRequest(path=path, partition_id=0)
    await stream.write(topic.StreamWriteMessage.FromClient(init_request=init))
    reply = await stream.read()
    assert reply.status == StatusIds.SUCCESS and reply.HasField('init_response'), reply
    return stream


async def check_disconnect(endpoint, database, path, urls):
    async with grpc.aio.insecure_channel(endpoint) as channel:
        for url in urls:
            stream = await open_writer(channel, database, path)
            for method, db, token, status in (
                ('POST', database, None, 401),
                ('POST', database, 'monitoring@builtin', 403),
                ('GET', database, ADMIN, 405),
                ('POST', None, ADMIN, 400),
                ('POST', '/Root/other', ADMIN, 403),
                ('POST', database + '/', ADMIN, 409),
            ):
                response = request(method, url, db, token)
                assert response.status_code == status, (method, db, response.status_code, response.text)

            # A response on this same RPC proves the rejected requests kept it alive.
            refresh = topic.UpdateTokenRequest(token=ADMIN)
            await stream.write(topic.StreamWriteMessage.FromClient(update_token_request=refresh))
            reply = await stream.read()
            assert reply.status == StatusIds.SUCCESS and reply.HasField('update_token_response'), reply

            response = request('POST', url, database, ADMIN)
            assert response.status_code == 200, response.text
            assert await asyncio.wait_for(stream.code(), timeout=10) == grpc.StatusCode.UNAVAILABLE

        await open_writer(channel, database, path)


def test_disconnect_existing_grpc_clients(ydb_cluster_with_enforce_user_token_and_pers_queue_topic):
    cluster = ydb_cluster_with_enforce_user_token_and_pers_queue_topic
    database = cluster.pers_queue_database
    slot, = cluster.slots.values()
    node = cluster.nodes[1]
    local = f'https://{slot.host}:{slot.mon_port}'
    slot_id, = get_nodelist_ids(local, database=database)
    route = '/actors/grpc_disconnect'
    urls = (local + route, f'https://{node.host}:{node.mon_port}/node/{slot_id}{route}')
    asyncio.run(check_disconnect(f'{slot.host}:{slot.grpc_port}', database, cluster.pers_queue_topic_path, urls))
