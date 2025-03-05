import os
import sys
import shutil
import argparse

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf
import java_pack_to_file as jcov
import resolve_java_srcs as  resolve
from autotar_gendirs import unpack_dir


def writelines(f, rng):
    f.writelines(item + '\n' for item in rng)


class SourcesSorter:
    FILE_ARG = 1
    RESOURCES_DIR_ARG = 2
    SRCDIR_ARG = 3
    JSOURCES_DIR_ARG = 4

    def __init__(self, moddir):
        self.moddir = moddir
        self.next_arg = SourcesSorter.FILE_ARG

        self.cur_resources_list_file = None
        self.cur_jsources_list_file = None
        self.cur_srcdir = None
        self.cur_resources = []
        self.cur_jsources = []

    def sort_args(self, remaining_args):
        for src in remaining_args:
            if self.next_arg == SourcesSorter.RESOURCES_DIR_ARG:
                assert self.cur_resources_list_file is None
                self.cur_resources_list_file = src
                self.next_arg = SourcesSorter.FILE_ARG
                continue
            elif self.next_arg == SourcesSorter.JSOURCES_DIR_ARG:
                assert self.cur_jsources_list_file is None
                self.cur_jsources_list_file = src
                self.next_arg = SourcesSorter.FILE_ARG
                continue
            elif self.next_arg == SourcesSorter.SRCDIR_ARG:
                assert self.cur_srcdir is None
                self.cur_srcdir = src if os.path.isabs(src) else os.path.join(self.moddir, src)
                self.next_arg = SourcesSorter.FILE_ARG
                continue

            if src == '--resources':
                if self.cur_resources_list_file is not None:
                    with open(self.cur_resources_list_file, 'w') as f:
                        writelines(f, self.cur_resources)
                self.cur_resources_list_file = None
                self.cur_srcdir = None
                self.cur_resources = []
                self.next_arg = SourcesSorter.RESOURCES_DIR_ARG
                continue
            if src == '--jsources':
                if self.cur_jsources_list_file is not None:
                    with open(self.cur_jsources_list_file, 'w') as f:
                        writelines(f, self.cur_jsources)
                self.cur_jsources_list_file = None
                self.cur_jsources = []
                self.next_arg = SourcesSorter.JSOURCES_DIR_ARG
                continue
            elif src == '--srcdir':
                self.next_arg = SourcesSorter.SRCDIR_ARG
                continue

            yield src

        if self.cur_resources_list_file is not None:
            with open(self.cur_resources_list_file, 'w') as f:
                writelines(f, self.cur_resources)
        if self.cur_jsources_list_file is not None:
            with open(self.cur_jsources_list_file, 'w') as f:
                writelines(f, self.cur_jsources)


class SourcesConsumer:
    def __init__(self, source_root, with_kotlin, with_coverage):
        self.source_root = source_root
        self.with_kotlin = with_kotlin
        self.with_coverage = with_coverage

        self.java = []
        self.kotlin = []
        self.coverage = []

    def consume(self, src, sorter):
        if src.endswith(".java"):
            self.java.append(src)
            self.kotlin.append(src)
            self._add_rel_src_to_coverage(src)
        elif self.with_kotlin and src.endswith(".kt"):
            self.kotlin.append(src)
            self._add_rel_src_to_coverage(src)
        else:
            assert sorter.cur_srcdir is not None and sorter.cur_resources_list_file is not None
            sorter.cur_resources.append(os.path.relpath(src, sorter.cur_srcdir))

        if sorter.cur_jsources_list_file is not None:
            assert sorter.cur_srcdir is not None
            sorter.cur_jsources.append(os.path.relpath(src, sorter.cur_srcdir))

    def _add_rel_src_to_coverage(self, src):
        if not self.with_coverage or not self.source_root:
            return
        rel = os.path.relpath(src, self.source_root)
        if not rel.startswith('..' + os.path.sep):
            self.coverage.append(rel)


def prepare_build_dirs(bindir, with_sources_jar):
    dirs = ['cls', 'misc']
    if with_sources_jar:
        dirs.append('src')
    for dir in [os.path.join(bindir, dirname) for dirname in dirs]:
        if not os.path.exists(dir):
            os.makedirs(dir)


def split_cmd_by_delim(cmd, delim='DELIM'):
    result = [[]]
    for arg in cmd:
        if arg == delim:
            result.append([])
        else:
            result[-1].append(arg)
    return result[0], result[1:]


def main():
    args, resolve_args = split_cmd_by_delim(pcf.get_args(sys.argv[1:]))

    parser = argparse.ArgumentParser()
    parser.add_argument('--with-sources-jar', action='store_true')
    parser.add_argument('--moddir')
    parser.add_argument('--bindir')
    parser.add_argument('--java')
    parser.add_argument('--kotlin')
    parser.add_argument('--coverage')
    parser.add_argument('--source-root')
    args, remaining_args = parser.parse_known_args(args)

    prepare_build_dirs(args.bindir, args.with_sources_jar)

    src_sorter = SourcesSorter(args.moddir)
    src_consumer = SourcesConsumer(
        source_root=args.source_root,
        with_kotlin=True if args.kotlin else False,
        with_coverage=True if args.coverage else False)

    jsrcs_dir = None
    for src in src_sorter.sort_args(remaining_args):
        if src.endswith(".gentar"):
            unpack_dir(src, os.path.dirname(src))
            continue
        if src.endswith(".jsrc"):
            jsrcs_dir = os.path.join(args.bindir, 'jsrcs')
            unpack_dir(src, jsrcs_dir)
            continue

        src_consumer.consume(src, src_sorter)

    if args.java:
        with open(args.java, 'w') as f:
            writelines(f, src_consumer.java)
    if args.kotlin:
        with open(args.kotlin, 'w') as f:
            writelines(f, src_consumer.kotlin)
    if args.coverage:
        jcov.write_coverage_sources(args.coverage, args.source_root, src_consumer.coverage)

    for rargs in resolve_args:
        resolve.cli_main(rargs, force_skip_source_jars=not args.with_sources_jar)

    if jsrcs_dir is not None:
        resolve.resolve_sources_and_fill_filelists(
            directory=jsrcs_dir,
            sources_file=args.java,
            resources_file=os.path.join(args.bindir, 'default.res.txt'),
            kotlin_sources_file=args.kotlin if args.kotlin else None,
            include_patterns=['**/*'],
            exclude_patterns=[],
            resolve_kotlin=True if args.kotlin else False,
            append=True,
            all_resources=False,
        )
        if args.with_sources_jar:
            # TODO ugly hack here. Once jar directory preparation will be handled in a single script
            # sources copying should use common API here as well. Current "common API" is to populate
            # file with files to be copied by another script. It can't be uses here since there is no
            # way to send filelist to that external script from current point in code
            shutil.copytree(jsrcs_dir, os.path.join(args.bindir, 'src'), dirs_exist_ok=True)

    return 0


if __name__ == '__main__':
    sys.exit(main())
