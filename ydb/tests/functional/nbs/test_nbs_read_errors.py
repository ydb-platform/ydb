import json

from common import NbsTestBase, execute_dstool_grpc


class TestNbsReadErrors(NbsTestBase):
    def test_read_crossing_stripe_reports_error(self):
        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)
        self.write(actor_id, 127, 'stripe-end')
        assert self.read(actor_id, 127).startswith('stripe-end')
        # The default 512 KiB stripe contains 128 blocks of 4096 bytes.
        result = execute_dstool_grpc(
            self.cluster,
            'token',
            ['nbs', 'partition', 'io', '--id', actor_id, '--start_index', '127', '--blocks_count', '2', '--type=read'],
            check_exit_code=False,
            return_process=True,
        )
        output = json.loads(result.std_out.decode())
        assert output['status'] == 'failure', output
        assert output['data'] == '', output
        assert 'GENERIC_ERROR' in result.std_err.decode(), result.std_err
