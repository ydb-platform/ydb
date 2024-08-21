from runner import AbstractRunner

# not clean but working way to remove argument from parser
def remove_argunemt(parser, argument):
    for action in parser._actions:
        if argument in vars(action)['option_strings']:
            parser._handle_conflict_resolve(None, [(argument, action)])
            break


class Runner(AbstractRunner):
    def __init__(self, enable_spilling):
        self._enable_spilling = enable_spilling
        super().__init__()
        self.name = 'test'
    
    def init_args_parser(self):
        parser = super().init_args_parser()
        remove_argunemt(parser, "--enable-spilling")
        return parser
    
    @property
    def modes(self):
        return [
            'ds_1_scalar', 'ds_1_block', 'ds_1_scalar_pg',
            'ds_10_scalar', 'ds_10_block', 'ds_10_scalar_pg',
            'ds_100_scalar', 'ds_100_block', 'ds_100_scalar_pg',
            'h_1_scalar', 'h_1_block', 'h_1_scalar_pg', 'h_10_scalar', 'h_10_block', 'h_10_scalar_pg',
            'h_100_scalar', 'h_100_block', 'h_100_scalar_pg'
        ]

    @property
    def timeout_seconds(self):
        return 30000

    @property
    def udfs(self):
        return ['set', 'datetime2', 'url_base', 're2', 'math', 'unicode_base']

    @property
    def files(self):
        return super().files + [
            'contrib/ydb/library/yql/udfs/common/set/libset_udf.so',
            'contrib/ydb/library/yql/udfs/common/datetime2/libdatetime2_udf.so',
            'contrib/ydb/library/yql/udfs/common/url_base/liburl_udf.so',
            'contrib/ydb/library/yql/udfs/common/re2/libre2_udf.so',
            'contrib/ydb/library/yql/udfs/common/math/libmath_udf.so',
            'contrib/ydb/library/yql/udfs/common/unicode_base/libunicode_udf.so',
            f'yql/queries/tpc_benchmark/{self.download_file}',
            'yql/queries/tpc_benchmark/download_lib.sh',
            'yql/queries/tpc_benchmark/download_tables.sh',
            'yql/queries/tpc_benchmark/download_tpcds_tables.sh',
        ]

    @property
    def dirs(self):
        return []

    @property
    def download_file(self):
        if self.scale == '100':
            name = f"download_files_{self.test}_100.sh"
        elif self.scale == '10':
            name = f"download_files_{self.test}_10.sh"
        elif self.scale == '1':
            name = f"download_files_{self.test}_1.sh"
        else:
            raise ValueError(f'Unknown scale: {self.scale}')
        return name

    @property
    def arcadia_run_dir(self):
        return 'yql/queries/tpc_benchmark'
    
    @property
    def enable_spilling(self):
        return self._enable_spilling

    def get_nums(self):
        if self.test == 'h':
            return list(range(1, 23))
        elif self.test == 'ds':
            if self.name.endswith("_pg"):
                black_list = {
                    57,  # ERROR: invalid digit in external "numeric" value
                    58,  # Bad packed data. Invalid embedded size
                    72,  # Execution timeout
                    83,  # Bad packed data. Invalid embedded size
                }
            else:
                black_list = {
                    14,  # OOM
                    23,  # OOM
                    32,  # timeout
                    72,  # OOM
                }
            return list(set(range(1, 100)).difference(black_list))
        else:
            raise ValueError(f'Unknown test: {self.test}')

    def decode_mode(self, mode):
        self.syntax = "yql"
        self.test = "h"
        if mode.startswith('h_'):
            self.cases_dir = 'h/yql'
            self.bindings_file = 'bindings.json'
        elif mode.startswith('ds_'):
            self.cases_dir = 'ds/yql'
            if self.args.decimal:
                self.bindings_file = 'bindings_tpcds_decimal.json'
            else:
                self.bindings_file = 'bindings_tpcds.json'
            self.test = "ds"
        else:
            raise ValueError(f'Unknown mode: {mode}')

        if mode.startswith(('ds_1', 'h_1', 'h_10', 'h_100')):
            name, scale, mode = mode.split('_', 2)
            self.name = f'{name}_{scale}'
            self.scale = scale
            self.mode = mode
        else:
            raise ValueError('Unknown mode: {mode}')

        if mode == 'scalar':
            self.pragmas_path = "pragmas_scalar.yql"
            self.name += '_yql'
        elif mode == 'scalar_pg':
            self.syntax = "pg"
            self.pragmas_path = "pragmas_scalar_pg.yql"
            self.name += "_yql_pg"
            self.cases_dir = f"{self.test}/pg"
            if self.test == 'h':
                self.bindings_file = 'bindings_pg.json'
            else:
                self.bindings_file = 'bindings_tpcds_pg.json'
        elif mode == 'block':
            self.pragmas_path = "pragmas_block.yql"
            self.name += '_yql_blocks'
        
        if self.enable_spilling:
            self.name += "_spilling"

        self.data_path = f"{self.test}/{self.scale}"


def main():
    print("With spilling")
    Runner(True).run()
    print("No spilling")
    Runner(False).run()


    # run compare_results 
