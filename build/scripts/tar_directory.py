import os
import argparse
import tarfile
import subprocess
import stat


def is_exe(fpath: str) -> bool:
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def _usage() -> str:
    return "\n".join(
        [
            "Usage:",
            f"`tar_directory.py [--tar-by-py] [--exclude fileOrDir0 ... --exclude fileOrDirN] archive.tar directory [skip prefix]`",
            "or",
            f"`tar_directory.py [--tar-by-py] --extract archive.tar output_directory`",
        ]
    )


def main():
    parser = argparse.ArgumentParser(description='Make or extract tar archive')
    parser.add_argument('--extract', action="store_true", default=False, help="Extract archive")
    parser.add_argument('--tar-by-py', action="store_true", default=False, help="Force use taring by python")
    parser.add_argument(
        '--exclude', type=str, action='append', default=[], help="Exclude for create archive (can use multiply times)"
    )
    parser.add_argument('archive', type=str, action='store', help="Archive name, for example, archive.tar")  # required
    parser.add_argument(
        'directory', type=str, action='store', help="Directory name for create from or extract to"
    )  # required
    parser.add_argument(
        'prefix', type=str, nargs='?', action='store', help="Path prefix for skip before create archive"
    )  # dont't required, because nargs=?

    args = parser.parse_args()

    if args.extract and (args.exclude or args.prefix):
        raise Exception(f"Illegal usage: {" ".join(args)}\n{_usage()}")

    tar = args.archive
    directory = args.directory
    prefix = args.prefix

    if args.extract:
        dest = os.path.abspath(directory)
        if not os.path.exists(dest):
            os.makedirs(dest)

    for tar_exe in ('/usr/bin/tar', '/bin/tar') if not args.tar_by_py else []:
        if not is_exe(tar_exe):
            continue
        if args.extract:
            command = [tar_exe, '-xf', tar, '-C', dest]
        else:
            source = os.path.relpath(directory, prefix) if prefix else directory
            command = (
                [tar_exe]
                + (['--exclude=' + exclude for exclude in args.exclude] if args.exclude else [])
                + ['-cf', tar]
                + (['-C', prefix] if prefix else [])
                + [source]
            )
        subprocess.run(command, check=True)
        break
    else:
        if args.extract:
            with tarfile.open(tar, 'r') as tar_file:
                tar_file.extractall(dest)
        else:
            source = directory
            with tarfile.open(tar, 'w') as out:

                def filter(tarinfo: tarfile.TarInfo):
                    if args.exclude:
                        for exclude in args.exclude:
                            if tarinfo.name.endswith(exclude):
                                return None
                    tarinfo.mode = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH if tarinfo.mode | stat.S_IXUSR else 0
                    tarinfo.mode = (
                        tarinfo.mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH
                    )
                    # Set mtime to 1970-01-01 00:00:01, else if mtime == 0
                    # it not used here https://a.yandex-team.ru/arcadia/contrib/python/python-libarchive/py3/libarchive/__init__.py#L355
                    # But mtime != 0 also ignored by libarchive too :((
                    tarinfo.mtime = 1
                    tarinfo.uid = 0
                    tarinfo.gid = 0
                    tarinfo.uname = 'dummy'
                    tarinfo.gname = 'dummy'
                    return tarinfo

                out.add(
                    os.path.abspath(source),
                    arcname=os.path.relpath(source, prefix) if prefix else source,
                    filter=filter,
                )


if __name__ == '__main__':
    main()
