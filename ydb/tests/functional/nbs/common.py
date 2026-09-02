# -*- coding: utf-8 -*-
import json
import logging
import pytest
import random
import re
import string
import requests
import time
import urllib.parse

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.functional.nbs.helpers import execute_ydbd, execute_dstool_grpc

logger = logging.getLogger(__name__)

DEFAULT_DISK_BLOCKS_COUNT = 1048576
DBG_LINK_RE = re.compile(r'page=dbg&dbg=(\d+)')
PBUFFER_PB_RE = re.compile(r'persistent_buffer\?pb=([^"\'&\s]+)')


class NbsTestBase:
    """
    Base test utils for NBS 2
    """

    ddisk_pool_name = "ddp1"

    @pytest.fixture(autouse=True)
    def setup(self):
        nbs_database_name = "/Root/NBS"
        self.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.MIRROR_3_DC,
                enable_nbs=True,
                nbs_database_name=nbs_database_name,
                additional_log_configs={
                    'NBS_PARTITION': LogLevels.INFO,
                    'NBS2_LOAD_TEST': LogLevels.DEBUG,
                    'NBS_VOLUME': LogLevels.DEBUG,
                    'NBS_SS_PROXY': LogLevels.DEBUG,
                },
            )
        )
        self.cluster.start()
        self.start_nbs(nbs_database_name)

        yield

        self.cluster.stop()

    def start_nbs(self, nbs_database_name):
        logger.info("Creating NBS database: %s", nbs_database_name)
        self.cluster.create_database(nbs_database_name, storage_pool_units_count={'hdd': 9})

        logger.info("Registering and starting NBS dynamic slots...")
        slots = self.cluster.register_and_start_slots(nbs_database_name, count=1)

        logger.info("Waiting for NBS tenant to be up...")
        try:
            self.cluster.wait_tenant_up(nbs_database_name)
            logger.info("NBS tenant is ready")
        except Exception as e:
            logger.error("Failed to start NBS tenant: %s", e)
            raise

        return slots

    def create_ddisk_pool(self):
        define_ddisk_pool = '''
            Command {{
                DefineDDiskPool {{
                    BoxId: 1
                    Name: "{ddisk_pool_name}"
                    Geometry {{
                        NumFailRealms: 1
                        NumFailDomainsPerFailRealm: 5
                        NumVDisksPerFailDomain: 1
                        RealmLevelBegin: 10
                        RealmLevelEnd: 10
                        DomainLevelBegin: 10
                        DomainLevelEnd: 40
                    }}
                    PDiskFilter {{
                        Property {{
                            Type: ROT
                        }}
                    }}
                    NumDDiskGroups: 10
                }}
            }}
        '''.format(
            ddisk_pool_name=self.ddisk_pool_name
        )

        execute_ydbd(self.cluster, "token", ['admin', 'bs', 'config', 'invoke', '--proto', define_ddisk_pool])

    def create_partition(self, disk_id, blocks_count=DEFAULT_DISK_BLOCKS_COUNT):
        """
        Create a disk and return the parsed CreatePartition JSON result.
        """
        proc = execute_dstool_grpc(
            self.cluster,
            "token",
            [
                'nbs',
                'partition',
                'create',
                '--pool',
                self.ddisk_pool_name,
                '--block-size=4096',
                f'--blocks-count={blocks_count}',
                '--type=ssd',
                '--disk-id',
                disk_id,
            ],
            check_exit_code=False,
            return_process=True,
        )

        stdout = proc.std_out.decode('utf-8')
        stderr = proc.std_err.decode('utf-8')

        try:
            output = json.loads(stdout)
        except json.JSONDecodeError as e:
            assert False, (
                f"CreatePartition for disk {disk_id} did not return JSON: "
                f"{e}; stdout={stdout}, stderr={stderr}"
            )

        return output

    def create_disk(self, disk_id, blocks_count=DEFAULT_DISK_BLOCKS_COUNT):
        """
        Create a disk with specified number of blocks.
        Returns the partition tablet id.
        """
        output = self.create_partition(disk_id, blocks_count)
        assert output.get('status') == 'SUCCESS', (
            f"CreatePartition failed for disk {disk_id}: {output}"
        )
        tablet_id = output.get('tabletId', '')
        assert tablet_id, f"CreatePartition did not return tabletId: {output}"
        return tablet_id

    def mon_base_url(self):
        node = self.cluster.nodes[1]
        return f'http://{node.host}:{node.mon_port}'

    def fetch_mon(self, path):
        url = f'{self.mon_base_url()}{path}'
        response = requests.get(url, timeout=30)
        assert response.status_code == 200, (
            f"Mon request failed: {url} status={response.status_code} body={response.text[:500]}"
        )
        return response.text

    def fetch_partition_dbg_page(self, tablet_id, dbg_index=None, allow_missing=False):
        path = f'/tablets/app?TabletID={tablet_id}&page=dbg'
        if dbg_index is not None:
            path += f'&dbg={dbg_index}'
        if not allow_missing:
            return self.fetch_mon(path)

        url = f'{self.mon_base_url()}{path}'
        response = requests.get(url, timeout=30)
        # After SchemeShard drops the volume, Hive deletes the tablet; the mon
        # proxy may return a non-200 / "tablet not found" page.
        if response.status_code != 200:
            return ''
        return response.text

    @staticmethod
    def pbuffer_node_id(pb_service_id):
        # ActorId::ToString is "[nodeId:poolId:localId]".
        match = re.fullmatch(r'\[(\d+):\d+:\d+\]', pb_service_id)
        assert match, f"unexpected PBuffer service id format: {pb_service_id}"
        return int(match.group(1))

    def fetch_pbuffer_page(self, pb_service_ids):
        """
        Fetch Persistent Buffer mon pages for the given service ids (grouped by
        node — the mon actor only lists local PBuffers), with tablet LSN table.
        """
        by_node = {}
        for pb in pb_service_ids:
            by_node.setdefault(self.pbuffer_node_id(pb), []).append(pb)

        pages = []
        for node_id, pbs in sorted(by_node.items()):
            params = [
                ('formPresent', '1'),
                ('describeFreeSpace', '0'),
                ('showTablets', '1'),
                ('autoRefresh', '0'),
            ]
            for pb in pbs:
                params.append(('pb', pb))
            query = urllib.parse.urlencode(params, doseq=True)
            pages.append(
                self.fetch_mon(f'/node/{node_id}/actors/persistent_buffer?{query}')
            )
        return '\n'.join(pages)

    @staticmethod
    def parse_dbg_indexes(html):
        return [int(x) for x in DBG_LINK_RE.findall(html)]

    @staticmethod
    def parse_pbuffer_service_ids(html):
        ids = []
        for encoded in PBUFFER_PB_RE.findall(html):
            ids.append(urllib.parse.unquote(encoded))
        return ids

    def collect_pbuffer_service_ids(self, tablet_id, dbg_indexes):
        pb_ids = []
        seen = set()
        for dbg_index in dbg_indexes:
            html = self.fetch_partition_dbg_page(tablet_id, dbg_index)
            for pb_id in self.parse_pbuffer_service_ids(html):
                if pb_id not in seen:
                    seen.add(pb_id)
                    pb_ids.append(pb_id)
        return pb_ids

    def wait_until(self, predicate, timeout_seconds=60, sleep_seconds=1, description=''):
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                if predicate():
                    return
                last_error = None
            except AssertionError as e:
                last_error = e
            time.sleep(sleep_seconds)
        detail = f': {last_error}' if last_error else ''
        assert False, f'Timed out waiting for {description or "condition"}{detail}'

    def delete_disk(self, disk_id):
        """
        Delete a disk by disk id. Returns the deleted disk id.
        """
        output = json.loads(
            execute_dstool_grpc(
                self.cluster,
                "token",
                [
                    'nbs',
                    'partition',
                    'delete',
                    '--disk-id',
                    disk_id,
                ],
            )
        )

        assert output.get('status') == 'SUCCESS', (
            f"DeletePartition failed for disk {disk_id}: {output}"
        )
        deleted_disk_id = output.get('diskId', '')
        assert deleted_disk_id != "", f"DeletePartition did not return diskId: {output}"
        return deleted_disk_id

    def delete_disk_expect_failure(self, disk_id, expected_status='NOT_FOUND'):
        """
        Attempt to delete a disk and assert that the operation fails with the
        expected YDB status.
        """
        proc = execute_dstool_grpc(
            self.cluster,
            "token",
            [
                'nbs',
                'partition',
                'delete',
                '--disk-id',
                disk_id,
            ],
            check_exit_code=False,
            return_process=True,
        )

        stdout = proc.std_out.decode('utf-8')
        stderr = proc.std_err.decode('utf-8')

        try:
            output = json.loads(stdout)
        except json.JSONDecodeError as e:
            assert False, (
                f"DeletePartition for disk {disk_id} did not return JSON: "
                f"{e}; stdout={stdout}, stderr={stderr}"
            )

        status = output.get('status')
        assert status == expected_status, (
            f"DeletePartition for disk {disk_id} returned status {status}, "
            f"expected {expected_status}; output={output}, stderr={stderr}"
        )
        assert 'diskId' not in output, (
            f"DeletePartition unexpectedly returned diskId on failure: {output}"
        )

    def get_load_actor_adapter_actor_id(self, disk_id):
        get_load_actor_res = json.loads(
            execute_dstool_grpc(
                self.cluster,
                "token",
                ['nbs', 'partition', 'get-load-actor-adapter-actor-id', '--disk-id', disk_id],
            )
        )

        status = get_load_actor_res["status"]
        actor_id = get_load_actor_res["actorId"]
        assert status == "success"
        assert actor_id != ""

        return actor_id

    def write(self, actor_id, index, data):
        execute_dstool_grpc(
            self.cluster,
            "token",
            [
                'nbs',
                'partition',
                'io',
                '--id',
                actor_id,
                '--start_index',
                str(index),
                '--type=write',
                '--data',
                data,
            ],
        )

    def read(self, actor_id, index, blocks_count=1):
        read_result = json.loads(
            execute_dstool_grpc(
                self.cluster,
                "token",
                [
                    'nbs',
                    'partition',
                    'io',
                    '--id',
                    actor_id,
                    '--start_index',
                    str(index),
                    '--blocks_count',
                    str(blocks_count),
                    '--type=read',
                ],
            )
        )

        status = read_result["status"]
        data = read_result["data"]
        assert status == "success"

        return data

    def generate_random_data(self, size):
        """
        Generate random string data of specified size
        """
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    def generate_disk_id(self):
        return self.generate_random_data(10)

    def run_nbs_load_test(
        self,
        actor_id,
        write_rate,
        read_rate,
        duration=10,
        io_depth=32,
        load_type="LOAD_TYPE_RANDOM",
        range_start=0,
        range_end=DEFAULT_DISK_BLOCKS_COUNT - 1,
    ):
        """
        Helper function to run NBS load test with specified parameters

        Args:
            actor_id: Actor ID for the NBS partition
            write_rate: Percentage of write requests (0-100)
            read_rate: Percentage of read requests (0-100)
            duration: Test duration in seconds
            io_depth: Number of concurrent requests

        Returns:
            dict: Parsed test results
        """

        # Prepare load test config
        load_config = f'''NBS2Load {{
    DurationSeconds: {duration}
    DirectPartitionId: "{actor_id}"
    RangeTest {{
        Start: {range_start}
        End: {range_end}
        LoadType: {load_type}
        IoDepth: {io_depth}
        WriteRate: {write_rate}
        ReadRate: {read_rate}
    }}
}}'''

        # Start load test via HTTP API
        http_endpoint = f'http://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].mon_port}'
        url = f'{http_endpoint}/actors/load'

        response = requests.post(
            url,
            data={'mode': 'start', 'all_nodes': 'false', 'config': load_config},
            headers={'Content-Type': 'application/x-protobuf-text'},
        )

        assert response.status_code == 200, f"Failed to start load test: {response.text}"
        result = response.json()
        assert result.get('status') == 'ok', f"Load test start failed: {result}"
        uuid = result.get('uuid')

        logger.debug(f"Load actor start response: {result}")

        # Wait for test to complete (duration + buffer)
        time.sleep(duration + 2)

        # Get results
        results_response = requests.get(f'{http_endpoint}/actors/load?mode=results')
        assert results_response.status_code == 200

        logger.debug(f"Load actor results response: {results_response.text}")

        # Parse results from HTML - find the specific panel for this UUID
        results_html = results_response.text

        # Find the panel with this UUID
        # Format: <div class="panel-heading">UUID# {uuid} (node tag# {tag})</div>
        uuid_pattern = f'UUID# {uuid}.*?</div>'
        uuid_match = re.search(uuid_pattern, results_html, re.DOTALL)
        assert uuid_match, f"Could not find UUID {uuid} in response"

        # Extract the panel content starting from the UUID heading
        # The panel contains the table with TestResults
        panel_start = uuid_match.start()
        # Find the closing </div></div></div> for this panel (3 levels deep)
        panel_match = re.search(
            r'UUID# ' + re.escape(uuid) + r'.*?</div></div></div>', results_html[panel_start:], re.DOTALL
        )

        if not panel_match:
            # Fallback: try to find just the next TestResults after the UUID
            remaining_html = results_html[panel_start:]
            td_match = re.search(r'<td>\s*(\{[^<]*"TestResults"[^<]*\})\s*</td>', remaining_html, re.DOTALL)
            assert td_match, f"Could not find TestResults for UUID {uuid}"
            json_str = td_match.group(1).strip()
        else:
            panel_html = panel_match.group(0)
            # Find the <td> containing TestResults in this panel
            all_tds = re.findall(r'<td>(.*?)</td>', panel_html, re.DOTALL)
            json_str = None
            for td_content in all_tds:
                if 'TestResults' in td_content:
                    json_str = td_content.strip()
                    break

            assert json_str, f"Could not find TestResults JSON in panel for UUID {uuid}"

        try:
            # The JSON might have HTML entities
            json_str = json_str.replace('&quot;', '"').replace('&#39;', "'").replace('&lt;', '<').replace('&gt;', '>')
            full_results = json.loads(json_str)
            test_results = full_results.get('TestResults', full_results)
        except json.JSONDecodeError as e:
            # If JSON parsing fails, print the problematic text for debugging
            logger.error(f"Failed to parse JSON (first 500 chars): {json_str[:500]}")
            logger.error(f"Error: {e}")
            raise

        return test_results

    def verify_load_test_results(self, results, expected_blocks_read=None, expected_blocks_written=None):
        """
        Verify load test results have expected values

        Args:
            results: Test results dictionary
            expected_blocks_read: If True, verify blocks were read
            expected_blocks_written: If True, verify blocks were written
        """
        # Verify basic success (Result field may not be present, which means success)
        if 'Result' in results:
            assert results['Result'] == 0, "Load actor run finished with error"

        # Verify IOPS and throughput are non-zero
        assert 'Iops' in results, f"Missing Iops in results: {results}"
        iops = results['Iops']
        assert iops > 0, f"IOPS should be greater than 0, got {iops}"

        assert 'ThroughputMbs' in results, f"Missing ThroughputMbs in results: {results}"
        throughput = results['ThroughputMbs']
        assert throughput > 0, f"Throughput should be greater than 0, got {throughput}"

        # Verify requests completed
        assert 'RequestsCompleted' in results, f"Missing RequestsCompleted in results: {results}"
        requests_completed = results['RequestsCompleted']
        assert requests_completed > 0, f"RequestsCompleted should be greater than 0, got {requests_completed}"

        # Verify requests failed (if present)
        if 'RequestsFailed' in results:
            requests_failed = results['RequestsFailed']
            assert requests_failed == 0, f"RequestsFailed should be 0, got {requests_failed}"

        # Verify blocks read/written as expected
        if expected_blocks_read:
            assert 'BlocksRead' in results, f"Expected BlocksRead in results: {results}"
            blocks_read = results['BlocksRead']
            assert blocks_read > 0, f"BlocksRead should be greater than 0, got {blocks_read}"

        if expected_blocks_written:
            assert 'BlocksWritten' in results, f"Expected BlocksWritten in results: {results}"
            blocks_written = results['BlocksWritten']
            assert blocks_written > 0, f"BlocksWritten should be greater than 0, got {blocks_written}"

        logger.info(
            f"Test passed: IOPS={iops}, Throughput={throughput} MB/s, "
            f"Requests={requests_completed}, "
            f"BlocksRead={results.get('BlocksRead', 0)}, "
            f"BlocksWritten={results.get('BlocksWritten', 0)}"
        )
