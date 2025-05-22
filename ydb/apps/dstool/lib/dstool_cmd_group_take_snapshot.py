from ydb.apps.dstool.lib.bs_layout import BlobStorageLayout
import ydb.apps.dstool.lib.common as common
import uuid
from threading import Thread, Lock
import struct
from argparse import FileType

description = 'Take snapshot of groups metadata'
lock = Lock()
output_file = None


def fetch_blobs_from_vdisk(group_id, index, host, pdisk_id, vslot_id):
    session_id = uuid.uuid4()
    params = dict(pdiskId=pdisk_id, vdiskSlotId=vslot_id, sessionId=session_id)
    while True:
        data = common.fetch('vdisk_stream', params, explicit_host=host, fmt='raw')
        if not data or data == b'ERROR':
            break
        with lock:
            output_file.write(struct.pack('@III', group_id, index, len(data)))
            output_file.write(data)


def add_options(p):
    common.add_group_ids_option(p, required=True)
    p.add_argument('--output', type=FileType('wb'), required=True, help='Path to output binary file')
    common.add_basic_format_options(p)


def do(args):
    def get_endpoints():
        layout = BlobStorageLayout()
        layout.fetch_node_mon_endpoints({vslot_id.pdisk_id.node_id for vslot_id in layout.vslots})

        for group in layout.groups.values():
            if group.base.GroupId in args.group_ids:
                for index, vslot in enumerate(group.vslots_of_group):
                    id_ = vslot.base.VSlotId
                    yield group.base.GroupId, index, vslot.pdisk.node.node_mon_endpoint, id_.PDiskId, id_.VSlotId

    global output_file
    output_file = args.output

    with output_file:
        threads = []
        for p in get_endpoints():
            thread = Thread(target=fetch_blobs_from_vdisk, args=p, daemon=True)
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
