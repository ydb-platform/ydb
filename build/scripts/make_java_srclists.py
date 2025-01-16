import os
import sys
import argparse

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf
import java_pack_to_file as jcov


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


def add_rel_src_to_coverage(coverage, src, source_root):
    rel = os.path.relpath(src, source_root)
    if not rel.startswith('..' + os.path.sep):
        coverage.append(rel)


def main():
    args = pcf.get_args(sys.argv[1:])
    parser = argparse.ArgumentParser()
    parser.add_argument('--moddir')
    parser.add_argument('--java')
    parser.add_argument('--kotlin')
    parser.add_argument('--coverage')
    parser.add_argument('--source-root')
    args, remaining_args = parser.parse_known_args(args)

    java = []
    kotlin = []
    coverage = []

    src_sorter = SourcesSorter(args.moddir)
    for src in src_sorter.sort_args(remaining_args):
        if src.endswith(".java"):
            java.append(src)
            kotlin.append(src)
            if args.coverage and args.source_root:
                add_rel_src_to_coverage(coverage, src, args.source_root)
        elif args.kotlin and src.endswith(".kt"):
            kotlin.append(src)
            if args.coverage and args.source_root:
                add_rel_src_to_coverage(coverage, src, args.source_root)
        else:
            assert src_sorter.cur_srcdir is not None and src_sorter.cur_resources_list_file is not None
            src_sorter.cur_resources.append(os.path.relpath(src, src_sorter.cur_srcdir))

        if src_sorter.cur_jsources_list_file is not None:
            assert src_sorter.cur_srcdir is not None
            src_sorter.cur_jsources.append(os.path.relpath(src, src_sorter.cur_srcdir))

    if args.java:
        with open(args.java, 'w') as f:
            writelines(f, java)
    if args.kotlin:
        with open(args.kotlin, 'w') as f:
            writelines(f, kotlin)
    if args.coverage:
        jcov.write_coverage_sources(args.coverage, args.source_root, coverage)

    return 0


if __name__ == '__main__':
    sys.exit(main())
