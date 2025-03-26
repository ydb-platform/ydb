import os
import argparse
import tarfile


def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def _usage() -> str:
    return "\n".join(
        [
            "Usage:",
            f"`tar_directory.py archive.tar directory [skip prefix] [--exclude fileOrDir0 ... --exclude fileOrDirN]`",
            "or",
            f"`tar_directory.py archive.tar output_directory --extract`",
        ]
    )


def main():
    parser = argparse.ArgumentParser(description='Make or extract tar archive')
    parser.add_argument('--extract', action="store_true", default=False, help="Extract archive")
    parser.add_argument(
        '--exclude', type=str, action='append', default=[], help="Exclude for create archive (can use multiply times)"
    )
    parser.add_argument('archive', type=str, action='store', help="Archive name, for example, archive.tar")  # required
    parser.add_argument('directory', type=str, action='store', help="Directory name for create from or extract to")  # required
    parser.add_argument('prefix', type=str, nargs='?', action='store', help="Path prefix for skip before create archive")  # dont't required, because nargs=?

    args = parser.parse_args()

    if (args.extract and (args.exclude or args.prefix)):
        raise Exception(f"Illegal usage: {" ".join(args)}\n{_usage()}")

    tar = args.archive
    directory = args.directory
    prefix = args.prefix

    for tar_exe in ('/usr/bin/tar', '/bin/tar'):
        if not is_exe(tar_exe):
            continue
        if args.extract:
            dest = os.path.abspath(directory)
            if not os.path.exists(dest):
                os.makedirs(dest)
            os.execv(tar_exe, [tar_exe, '-xf', tar, '-C', dest])
        else:
            source = os.path.relpath(directory, prefix) if prefix else directory
            command = (
                [tar_exe]
                + (["--exclude='" + exclude + "'" for exclude in args.exclude] if args.exclude else [])
                + ['-cf', tar]
                + (['-C', prefix] if prefix else [])
                + [source]
            )
            os.execv(tar_exe, command)
        break
    else:
        if args.extract:
            dest = os.path.abspath(directory)
            if not os.path.exists(dest):
                os.makedirs(dest)
            with tarfile.open(tar, 'r') as tar_file:
                tar_file.extractall(dest)
        else:
            source = directory
            with tarfile.open(tar, 'w') as out:

                def filter(tarinfo):
                    for exclude in args.exclude:
                        if tarinfo.name.endswith(exclude):
                            return None
                    return tarinfo

                out.add(
                    os.path.abspath(source),
                    arcname=os.path.relpath(source, prefix) if prefix else source,
                    filter=filter if args.exclude else None,
                )


if __name__ == '__main__':
    main()
