from __future__ import annotations

import argparse
import gzip
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

try:
    import brotli

    brotli_installed = True
except ImportError:  # pragma: no cover
    brotli_installed = False


class Compressor:
    # Extensions that it's not worth trying to compress
    SKIP_COMPRESS_EXTENSIONS = (
        # Images
        "jpg",
        "jpeg",
        "png",
        "gif",
        "webp",
        # Compressed files
        "zip",
        "gz",
        "tgz",
        "bz2",
        "tbz",
        "xz",
        "br",
        # Flash
        "swf",
        "flv",
        # Fonts
        "woff",
        "woff2",
        # Video
        "3gp",
        "3gpp",
        "asf",
        "avi",
        "m4v",
        "mov",
        "mp4",
        "mpeg",
        "mpg",
        "webm",
        "wmv",
    )

    def __init__(
        self, extensions=None, use_gzip=True, use_brotli=True, log=print, quiet=False
    ):
        if extensions is None:
            extensions = self.SKIP_COMPRESS_EXTENSIONS
        self.extension_re = self.get_extension_re(extensions)
        self.use_gzip = use_gzip
        self.use_brotli = use_brotli and brotli_installed
        if not quiet:
            self.log = log

    @staticmethod
    def get_extension_re(extensions):
        if not extensions:
            return re.compile("^$")
        else:
            return re.compile(
                r"\.({})$".format("|".join(map(re.escape, extensions))), re.IGNORECASE
            )

    def should_compress(self, filename):
        return not self.extension_re.search(filename)

    def log(self, message):
        pass

    def compress(self, path):
        filenames = []
        with open(path, "rb") as f:
            stat_result = os.fstat(f.fileno())
            data = f.read()
        size = len(data)
        if self.use_brotli:
            compressed = self.compress_brotli(data)
            if self.is_compressed_effectively("Brotli", path, size, compressed):
                filenames.append(self.write_data(path, compressed, ".br", stat_result))
            else:
                # If Brotli compression wasn't effective gzip won't be either
                return filenames
        if self.use_gzip:
            compressed = self.compress_gzip(data)
            if self.is_compressed_effectively("Gzip", path, size, compressed):
                filenames.append(self.write_data(path, compressed, ".gz", stat_result))
        return filenames

    @staticmethod
    def compress_gzip(data):
        output = BytesIO()
        # Explicitly set mtime to 0 so gzip content is fully determined
        # by file content (0 = "no timestamp" according to gzip spec)
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=output, compresslevel=9, mtime=0
        ) as gz_file:
            gz_file.write(data)
        return output.getvalue()

    @staticmethod
    def compress_brotli(data):
        return brotli.compress(data)

    def is_compressed_effectively(self, encoding_name, path, orig_size, data):
        compressed_size = len(data)
        if orig_size == 0:
            is_effective = False
        else:
            ratio = compressed_size / orig_size
            is_effective = ratio <= 0.95
        if is_effective:
            self.log(
                f"{encoding_name} compressed {path} ({orig_size // 1024}K -> {compressed_size // 1024}K)"
            )
        else:
            self.log(f"Skipping {path} ({encoding_name} compression not effective)")
        return is_effective

    def write_data(self, path, data, suffix, stat_result):
        filename = path + suffix
        with open(filename, "wb") as f:
            f.write(data)
        os.utime(filename, (stat_result.st_atime, stat_result.st_mtime))
        return filename


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Search for all files inside <root> *not* matching "
        "<extensions> and produce compressed versions with "
        "'.gz' and '.br' suffixes (as long as this results in a "
        "smaller file)"
    )
    parser.add_argument(
        "-q", "--quiet", help="Don't produce log output", action="store_true"
    )
    parser.add_argument(
        "--no-gzip",
        help="Don't produce gzip '.gz' files",
        action="store_false",
        dest="use_gzip",
    )
    parser.add_argument(
        "--no-brotli",
        help="Don't produce brotli '.br' files",
        action="store_false",
        dest="use_brotli",
    )
    parser.add_argument("root", help="Path root from which to search for files")
    default_exclude = ", ".join(Compressor.SKIP_COMPRESS_EXTENSIONS)
    parser.add_argument(
        "extensions",
        nargs="*",
        help=(
            "File extensions to exclude from compression "
            + f"(default: {default_exclude})"
        ),
        default=Compressor.SKIP_COMPRESS_EXTENSIONS,
    )
    args = parser.parse_args(argv)

    compressor = Compressor(
        extensions=args.extensions,
        use_gzip=args.use_gzip,
        use_brotli=args.use_brotli,
        quiet=args.quiet,
    )

    with ThreadPoolExecutor() as executor:
        futures = []
        for dirpath, _dirs, files in os.walk(args.root):
            for filename in files:
                if compressor.should_compress(filename):
                    futures.append(
                        executor.submit(
                            compressor.compress, os.path.join(dirpath, filename)
                        )
                    )
        # Trigger any errors
        for future in as_completed(futures):
            future.result()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
