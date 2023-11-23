import subprocess
from pathlib import Path
from typing import List


class YDBProtoFile:
    """
    YDBProtoFile is a proto file lying within YDB directory that
    we have to patch in order to generate valid GRPC for connector.
    """

    src_initial: str
    src_patched: str
    filepath: Path

    def __init__(self, filepath: Path, go_package: str):
        self.filepath = filepath

        # preserve original content
        with open(filepath, 'r') as f:
            self.src_initial = f.read()

        # prepare patched version
        lines_initial = self.src_initial.splitlines()

        import_line = f'option go_package = "{go_package}";'
        import_line_pos = 5

        lines_patched = lines_initial[:import_line_pos] + [import_line] + lines_initial[import_line_pos:]
        self.src_patched = '\n'.join(lines_patched)

    def patch(self):
        with open(self.filepath, 'w') as f:
            f.write(self.src_patched)

    def revert(self):
        with open(self.filepath, 'w') as f:
            f.write(self.src_initial)


def __call_subprocess(cmd: List[str]):
    formatted = "\n".join(map(str, cmd))
    print(f'Running command:\n{formatted}')

    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    exit_code = process.wait()

    if exit_code != 0:
        raise Exception(
            f'Subprocess failure: exit_code={exit_code} stdout={stdout.decode("utf-8")}, stderr={stderr.decode("utf-8")}'
        )

    if stdout:
        print(stdout.decode('utf-8'))
    if stderr:
        print(stderr.decode('utf-8'))
    return stdout


def get_arc_root() -> Path:
    out = __call_subprocess(['arc', 'root'])
    return Path(out.decode('utf-8').strip())


def build_protoc(arc_root: Path) -> Path:
    __call_subprocess([arc_root.joinpath('ya'), 'make', arc_root.joinpath('contrib/tools/protoc')])
    return arc_root.joinpath('contrib/tools/protoc/protoc')


def build_protoc_gen_go(arc_root: Path) -> Path:
    __call_subprocess(
        [arc_root.joinpath('ya'), 'make', arc_root.joinpath('vendor/google.golang.org/protobuf/cmd/protoc-gen-go')]
    )
    return arc_root.joinpath('vendor/google.golang.org/protobuf/cmd/protoc-gen-go/protoc-gen-go')


def build_protoc_gen_go_grpc(arc_root: Path) -> Path:
    __call_subprocess(
        [arc_root.joinpath('ya'), 'make', arc_root.joinpath('vendor/google.golang.org/grpc/cmd/protoc-gen-go-grpc')]
    )
    return arc_root.joinpath('vendor/google.golang.org/grpc/cmd/protoc-gen-go-grpc/protoc-gen-go-grpc')


def run_protoc(arc_root: Path):
    # compile protoc from Arcadia
    protoc_binary = build_protoc(arc_root)
    protoc_gen_go_binary = build_protoc_gen_go(arc_root)
    protoc_gen_go_grpc_binary = build_protoc_gen_go_grpc(arc_root)

    # look for project protofiles
    source_dir = arc_root.joinpath('ydb/library/yql/providers/generic/connector/api/service')
    target_dir = arc_root.joinpath('ydb/library/yql/providers/generic/connector/libgo/service')
    proto_files = source_dir.rglob('*.proto')

    # build protoc args
    cmd = [
        protoc_binary,
        f'--plugin=protoc-gen-go={protoc_gen_go_binary}',
        f'--plugin=protoc-gen-go-grpc={protoc_gen_go_grpc_binary}',
        f'--go_out={target_dir}',
        '--go_opt=module=a.yandex-team.ru/ydb/library/yql/providers/generic/connector/libgo/service',
        f'--go-grpc_out={target_dir}',
        '--go-grpc_opt=module=a.yandex-team.ru/ydb/library/yql/providers/generic/connector/libgo/service',
        f'-I{arc_root}',
        f'-I{arc_root.joinpath("contrib/libs/protobuf/src/")}',
    ]
    cmd.extend(proto_files)
    __call_subprocess(cmd)


def main():
    # derive Arcadia's root
    arc_root = get_arc_root()

    # YDB's protofiles this project depends on
    ydb_source_params = [
        ('ydb/public/api/protos/ydb_value.proto', 'github.com/ydb-platform/ydb-go-genproto/protos/Ydb'),
        ('ydb/public/api/protos/ydb_status_codes.proto', 'github.com/ydb-platform/ydb-go-genproto/protos/Ydb'),
        ('ydb/public/api/protos/ydb_issue_message.proto', 'github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue'),
    ]

    ydb_source_files = [YDBProtoFile(arc_root.joinpath(param[0]), param[1]) for param in ydb_source_params]

    # Patch YDB sources
    for f in ydb_source_files:
        f.patch()

    try:
        # Generate Connector GRPC API
        run_protoc(arc_root)
    finally:
        # pass
        # Revert changes in YDB sources
        for f in ydb_source_files:
            f.revert()
