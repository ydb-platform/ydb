import sys
import argparse
import base64
import json
import subprocess
from pathlib import Path


class _Go:
    PREFIX_CHARS = '+'

    def __init__(self):
        self._parser = argparse.ArgumentParser(prefix_chars=self.PREFIX_CHARS)
        command_parsers = self._parser.add_subparsers(title='command', required=True)
        self._add_command_tool(command_parsers)

    def main(self, argv=None) -> int:
        if argv is None:
            argv = sys.argv[1:]
        self.argv = argv
        self.args = self._parser.parse_args(self.argv)
        return self._on_command(self.argv[0])

    @staticmethod
    def cmd(cmd: list[str], cwd: str = None, env: dict[str, str] = None) -> int:
        try:
            r = subprocess.run(cmd, capture_output=True, cwd=cwd, env=env, check=True, timeout=1800, text=True)
            if r.stderr:
                sys.stderr.write(f'{r.stderr}\n')
            return r.returncode
        except subprocess.CalledProcessError as e:
            sys.stderr.write(f'\nFail command with returncode={e.returncode}:\n{' '.join(cmd)}\n{e.stderr}\n')
            return e.returncode
        except subprocess.TimeoutExpired as e:
            sys.stderr.write(f'\nFail command with timeout={e.timeout}:\n{' '.join(cmd)}\n{e.stderr}\n')
            return 1000  # timeout

    def _add_command_tool(self, command_parsers) -> None:
        command_tool = command_parsers.add_parser(
            _GoTool.COMMAND_NAME, prefix_chars=self.PREFIX_CHARS, help='Go cli tools'
        )
        tool_parsers = command_tool.add_subparsers(title='Go tools', required=True)
        _GoToolCover.add_to_tool_parsers(tool_parsers)
        _GoToolCovdata.add_to_tool_parsers(tool_parsers)

    def _on_command(self, command) -> int:
        match command:
            case _GoTool.COMMAND_NAME:
                return self._on_tool(self.argv[1])

    def _on_tool(self, tool) -> int:
        match tool:
            case _GoToolCover.TOOL_NAME:
                return _GoToolCover(self.args).execute()
            case _GoToolCovdata.TOOL_NAME:
                return _GoToolCovdata(self.args).execute()


class _GoTool:
    COMMAND_NAME = 'tool'

    def __init__(self, args):
        self.args = args
        self.source_root = Path(self.args.source_root).resolve()
        self.build_root = Path(self.args.build_root).resolve()
        self.curdir = (self.source_root / self.args.moddir).resolve()
        self.bindir = (self.build_root / self.args.moddir).resolve()

    @staticmethod
    def _add_arguments(parser) -> None:
        _GoTool._add_roots_arguments(parser)
        _GoTool._add_toolchain_arguments(parser)

    @staticmethod
    def _add_roots_arguments(parser) -> None:
        group = parser.add_argument_group('Root options')
        group.add_argument('++source-root', required=True, help='absolute path of source root')
        group.add_argument('++build-root', required=True, help='absolute path of build root')
        group.add_argument('++moddir', required=True, help='relative to source/build root path of module')

    @staticmethod
    def _add_toolchain_arguments(parser) -> None:
        group = parser.add_argument_group('Toolchain options')
        group.add_argument('++toolchain-root', required=True, help='absolute path of Go toolchain root')
        group.add_argument('++host-os', choices=['linux', 'darwin', 'windows'], required=True)
        group.add_argument('++host-arch', choices=['amd64', 'arm64'], required=True)

    def tool(self, tool: str) -> str:
        return str(
            Path(self.args.toolchain_root) / 'pkg' / 'tool' / f'{self.args.host_os}_{self.args.host_arch}' / tool
        )

    def execute(self) -> int:
        raise Exception("Must be overwrite")

    def go_package(self, go_file: Path) -> str:
        with go_file.open('r') as f:
            line = f.readline()
            while line:
                if line.startswith('package '):
                    return line.split()[1].strip()
                line = f.readline()
        return ''


class _GoToolCover(_GoTool):
    TOOL_NAME = 'cover'

    @staticmethod
    def add_to_tool_parsers(tool_parsers) -> int:
        cover = tool_parsers.add_parser(
            _GoToolCover.TOOL_NAME, prefix_chars=_Go.PREFIX_CHARS, help='tool for make coverage'
        )
        _GoTool._add_arguments(cover)
        group = cover.add_argument_group('Coverage options')
        group.add_argument(
            '++cover-module',
            help='relative to source root path of module for coverage',
        )
        group.add_argument(
            '++cover-mode',
            default='set',
            choices=['set', 'count', 'atomic'],
            help='mode of coverage, by default: %(default)',
        )
        group.add_argument(
            '++cover-outcfg',
            default='coveragecfg',
            help='output config of cover, required for apply coverage in compile *.go',
        )
        group.add_argument(
            '++cover-covervars',
            default='covervars.go',
            help='output go-file with cover vars generated by cover',
        )
        group.add_argument(
            '++cover-ext',
            default='.cover.go',
            help='extension of generated by cover from sources go-files',
        )
        group.add_argument('++cover-package', required=True, help='Go package name')
        group.add_argument('++cover-srcs', nargs='*', required=True, help='list of *.go sources')

    def __init__(self, args):
        super().__init__(args)
        self.cover_module = self.args.cover_module if self.args.cover_module else self.args.moddir
        self.cover_package = self.args.cover_package

    def execute(self) -> int:
        # For GO_TEST_FOR module in cover_module, else moddir is module for coverage
        go_files = []
        for go_file in self.args.cover_srcs:
            go_package = self.go_package(self.source_root / go_file)
            if go_package == 'main':
                self._make_empty_cover_go(go_file)  # {go_file}.cover.go declared as output, we must create it
                continue  # can't generate coverage for package main
            if go_package != self.cover_package:
                sys.stderr.write(
                    f"In file '{go_file}' package '{go_package}' != configured package '{self.cover_package}', file skipped by coverage\n"
                )
                self._make_empty_cover_go(go_file)  # {go_file}.cover.go declared as output, we must create it
                continue
            go_files.append(go_file)
        if not go_files:
            raise Exception(f"Not found files for coverage with package '{self.cover_package}'")
        return self._do_package_coverage(
            go_package,
            self.args.cover_covervars,
            f'{self.bindir}/coveroutfiles.txt',
            f'{self.bindir}/pkgcfg.json',
            go_files,
        )

    def _make_empty_cover_go(self, go_file: str) -> None:
        with open(self.bindir / Path(go_file).name.replace('.go', self.args.cover_ext), 'wt', encoding="utf-8") as f:
            f.write(f'package {self.cover_package}')

    def _do_package_coverage(
        self, go_package: str, covervars_file: str, outfileslist_file: str, pkgcfg_file: str, go_files: list[str]
    ) -> int:
        self.bindir.mkdir(parents=True, exist_ok=True)
        with open(pkgcfg_file, 'wt', encoding="utf-8") as f:
            # Original at https://github.com/golang/go/blob/fefb02adf45c4bcc879bd406a8d61f2a292c26a9/src/cmd/go/internal/work/exec.go#L1929
            json.dump(
                {
                    "PkgPath": f'a.yandex-team.ru/{self.cover_module}',
                    "PkgName": go_package,  # package name for generated covervars.go
                    "Granularity": "perblock",  # now always perblock, reserved as future extension point
                    "OutConfig": f'{self.args.cover_outcfg}',  # file with generated coverage config, which must be applied in -coveragecfg <HERE> in compile go-files
                    "Local": True,  # in coverage report use only basename of files
                    "ModulePath": f'a.yandex-team.ru/{Path(self.cover_module).parent}',
                },
                f,
            )
            f.write('\n')
        cover_outs = [covervars_file] + [  # covervars MUST BE first, required by cover
            Path(go_file).name.replace('.go', self.args.cover_ext) for go_file in go_files
        ]
        with open(outfileslist_file, 'wt', encoding="utf-8") as f:
            f.write('\n'.join(cover_outs + ['']))

        # Unique prefix for all coverage variables of this package
        cover_var = 'GoCover' + base64.b32encode((self.args.moddir + '|' + go_package).encode('utf-8')).decode(
            'utf-8'
        ).rstrip('=')

        # Orininal at https://github.com/golang/go/blob/fefb02adf45c4bcc879bd406a8d61f2a292c26a9/src/cmd/go/internal/work/exec.go#L1902
        cmd = [
            self.tool('cover'),
            '-pkgcfg',
            pkgcfg_file,
            '-mode',
            self.args.cover_mode,
            '-var',
            cover_var,
            '-outfilelist',
            str(outfileslist_file),
            *[str(Path(self.args.source_root) / go_file) for go_file in go_files],
        ]
        return _Go.cmd(cmd, self.bindir)


class _GoToolCovdata(_GoTool):
    TOOL_NAME = 'covdata'

    SUBTOOL_MERGE = 'merge'
    SUBTOOL_TEXTFMT = 'textfmt'

    @staticmethod
    def add_to_tool_parsers(tool_parsers) -> int:
        covdata = tool_parsers.add_parser(
            _GoToolCovdata.TOOL_NAME, prefix_chars=_Go.PREFIX_CHARS, help='tool for manage coverage data'
        )
        _GoTool._add_arguments(covdata)
        covdata_parsers = covdata.add_subparsers(title='Go tool covdata subtools', required=True)
        covdata_merge = covdata_parsers.add_parser(
            _GoToolCovdata.SUBTOOL_MERGE, prefix_chars=_Go.PREFIX_CHARS, help='Merge profiles together'
        )
        covdata_merge.add_argument(
            '+i',
            help='comma separated list of input directory with binary coverage data',
        )
        covdata_merge.add_argument(
            '+o',
            help='output directory for binary coverage data',
        )
        covdata_merge.add_argument(
            '++modpaths',
            help='comma separated list of module paths',
        )
        covdata_textfmt = covdata_parsers.add_parser(
            _GoToolCovdata.SUBTOOL_TEXTFMT,
            prefix_chars=_Go.PREFIX_CHARS,
            help='Convert coverage data to legacy textual format',
        )
        covdata_textfmt.add_argument(
            '+i',
            help='input directory with binary coverage data',
        )
        covdata_textfmt.add_argument(
            '+o',
            help='output coverage profile',
        )

    def __init__(self, args):
        super().__init__(args)

    def execute(self) -> int:
        self._on_subtool(self.args.subtool)

    def _on_subtool(self, tool: str) -> int:
        match tool:
            case _GoToolCovdata.TOOL_NAME:
                return self._merge()

    def _merge(self) -> int:
        cmd = [self.tool('covdata'), 'merge', '-i', self.args.i, '-o', self.args.o, '-modpaths', self.args.modpaths]
        return _Go.cmd(cmd, self.bindir)

    def _textfmt(self) -> int:
        cmd = [
            self.tool('covdata'),
            'textfmt',
            '-i',
            self.args.i,
            '-o',
            self.args.o,
        ]
        return _Go.cmd(cmd, self.bindir)


if __name__ == '__main__':
    sys.exit(_Go().main())
