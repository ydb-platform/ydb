import os
import os.path

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Program, Switch, Recurse
from devtools.yamaker.project import CMakeNinjaNixProject

UNBUNDLE_FROM = {
    "abseil-cpp": "contrib/abseil-cpp",
    # base/
    # "_poco_crypto": "base/poco/Crypto",
    # "_poco_data": "base/poco/Data",
    # "_poco_foundation": "base/poco/Foundation",
    # "_poco_json": "base/poco/JSON",
    "_poco_mongodb": "base/poco/MongoDB",
    # "_poco_net_ssl": "base/poco/NetSSL_OpenSSL",
    # "_poco_net": "base/poco/Net",
    # "_poco_redis": "base/poco/Redis",
    # "_poco_util": "base/poco/Util",
    # "_poco_xml": "base/poco/XML",
    # contrib/
    "avrocpp": "contrib/avro",
    "aws-c-auth": "contrib/aws-c-auth",
    "aws-c-cal": "contrib/aws-c-cal",
    "aws-c-common": "contrib/aws-c-common",
    "aws-c-compression": "contrib/aws-c-compression",
    "aws-c-event-stream": "contrib/aws-c-event-stream",
    "aws-c-http": "contrib/aws-c-http",
    "aws-c-io": "contrib/aws-c-io",
    "aws-c-mqtt": "contrib/aws-c-mqtt",
    "aws-c-s3": "contrib/aws-c-s3",
    "aws-c-sdkutils": "contrib/aws-c-sdkutils",
    "aws-checksums": "contrib/aws-checksums",
    "aws-cpp-sdk-core": "contrib/aws/src/aws-cpp-sdk-core",
    "aws-cpp-sdk-s3": "contrib/aws/generated/src/aws-cpp-sdk-s3",
    "aws-crt-cpp": "contrib/aws-crt-cpp",
    "aws-s2n-tls": "contrib/aws-s2n-tls",
    "base64": "contrib/base64",
    "boost": "contrib/boost",
    "cares": "contrib/c-ares",
    "cctz": "contrib/cctz",
    "cityhash": "contrib/cityhash102",
    "consistent-hashing": "contrib/consistent-hashing",
    "cpuid": "contrib/libcpuid",
    "cxx": "contrib/llvm-project/libcxx",
    "cxxabi": "contrib/llvm-project/libcxxabi",
    "double-conversion": "contrib/double-conversion",
    "dragonbox_to_chars": "contrib/dragonbox",
    "farmhash": "contrib/libfarmhash",
    "fast_float": "contrib/fast_float",
    "fastops": "contrib/fastops",
    "fmt": "contrib/fmtlib",
    "h3_compat": "contrib/h3",  # TODO(dakovalkov): Если использовать просто h3, то матчится из глобального provide, несмотря на то что есть в проектном.
    "harmful": "base/harmful",
    "hashidsxx": "contrib/hashidsxx",
    "icu": "contrib/icu",
    "icudata": "contrib/icudata",
    "incbin_stub": "contrib/incbin",
    "libdivide": "contrib/libdivide",
    "lz4": "contrib/lz4",
    "lzma": "contrib/xz",
    "magic_enum": "contrib/magic_enum",
    "metrohash": "contrib/libmetrohash",
    "miniselect": "contrib/miniselect",
    "morton-nd": "contrib/morton-nd",
    "msgpack": "contrib/msgpack-c",
    "murmurhash": "contrib/murmurhash",
    "openssl": "contrib/openssl",
    # "pdqsort": "contrib/pdqsort", # CH имеет патченный pdqsort, поэтому нет возможности сделать unbundle. Возможно стоит унести куда-то из clickhouse/contrib
    "rapidjson": "contrib/rapidjson",
    "re2_st_stub": "contrib/re2-cmake",  # re2_st is generated in cmake folder during ClickHouse build.
    "re2": "contrib/re2",
    "readpassphrase": "base/readpassphrase",
    "replxx": "contrib/replxx",
    "roaring": "contrib/croaring",
    "simdjson": "contrib/simdjson",
    "snappy": "contrib/snappy",
    "sparsehash": "contrib/sparsehash-c11",
    "unwind": "contrib/libunwind",
    "wyhash": "contrib/wyhash",
    "xxhash": "contrib/xxHash",
    "zlib": "contrib/zlib-ng",
    "zstd": "contrib/zstd",
    "pocketfft": "contrib/pocketfft",
    # CHDB contribs
    "arrow": "contrib/arrow",
    "protoc": "contrib/google-protobuf",
    "brotli": "contrib/brotli",
    "flatbuffers": "contrib/flatbuffers",
    "hdfs3": "contrib/libhdfs3",
    "jemalloc": "contrib/jemalloc",
    "thrift": "contrib/thrift",
    "utf8proc": "contrib/utf8proc",
    # Сmake directories. Do not need to unbundle, just remove.
    "aws-cmake": "contrib/aws-cmake",
    "aws-s3-cmake": "contrib/aws-s3-cmake",
    "openssl-cmake": "contrib/openssl-cmake",
    "c-ares-cmake": "contrib/c-ares-cmake",
    "icu-cmake": "contrib/icu-cmake",
    "snappy-cmake": "contrib/snappy-cmake",
    "unwind-cmake": "contrib/libunwind-cmake",
    "zlib-ng-cmake": "contrib/zlib-ng-cmake",
    "brotli-cmake": "contrib/brotli-cmake",
    "thrift-cmake": "contrib/thrift-cmake",
    "utf8proc-cmake": "contrib/utf8proc-cmake",
    # "cctz-cmake": "contrib/cctz-cmake",  # Contains TimeZones.generated.cpp used in CH.
    # "libdivide-cmake": "contrib/libdivide-cmake",  # CH uses libdivide-config.h from this folder.
}


PUT = {
    # contrib-generated subtargets placement
    "tzdata": "contrib/cctz-cmake",
    "_orc": "contrib/orc",
    # base subtargets placement
    "common": "base/base",
    "widechar_width": "base/widechar_width",
    "dbms": "src",
    # src subtargets placements
    "clickhouse_common_io": "src/Common",
    "clickhouse_parsers": "src/Parsers",
    # the main executable clickhouse target
    "clickhouse-local-lib": "programs",
    # clickhouse has its own patched poco library
    "_poco_crypto": "base/poco/Crypto",
    "_poco_data": "base/poco/Data",
    "_poco_foundation": "base/poco/Foundation",
    "_poco_json": "base/poco/JSON",
    "_poco_net_ssl": "base/poco/NetSSL_OpenSSL",
    "_poco_net": "base/poco/Net",
    "_poco_redis": "base/poco/Redis",
    "_poco_util": "base/poco/Util",
    "_poco_xml": "base/poco/XML",
}

PUT_WITH = {
    "clickhouse_common_io": [
        "clickhouse_common_config",
        "clickhouse_common_zookeeper",
        "clickhouse_new_delete",
        # "string_utils", ???
        # "clickhouse_common_zookeeper_no_log",  # used only for tests/benchmarks.
        # "clickhouse_common_config_no_zookeeper_log", used only for tests/benchmarks.
    ],
    "dbms": [
        "clickhouse_aggregate_functions",
        "clickhouse_common_access",  # this name is missleading, sources are actually at src/Access/Common.
        "clickhouse_dictionaries_embedded",
        "clickhouse_dictionaries",
        "clickhouse_functions_array",
        "clickhouse_functions_gatherutils",
        "clickhouse_functions_jsonpath",
        "clickhouse_functions_obj",
        "clickhouse_functions_url",
        "clickhouse_storages_system",
        "clickhouse_table_functions",
        "daemon",
        "divide_impl_avx2",
        "divide_impl_sse2",
        "divide_impl",
        "loggers",
    ],
    "_poco_json": [
        "_poco_json_pdjson",
    ],
    "_poco_foundation": [
        "_poco_foundation_pcre",
    ],
    "_poco_xml": [
        "_poco_xml_expat",
    ],
}

# yamaker do not detect peerdirs between internal static libs.
# Add it manually.
INTERNAL_PEERDIRS = {
    "src": {
        "contrib/cctz-cmake",
        "contrib/orc",
        "src/Parsers",
        "src/Common",
        "base/poco/Crypto",
        "base/poco/Data",
        "base/poco/Foundation",
        "base/poco/JSON",
        "base/poco/Net",
        "base/poco/NetSSL_OpenSSL",
        "base/poco/Redis",
        "base/poco/Util",
        "base/poco/XML",
    },
    "src/Common": {
        "base/base",
        "base/widechar_width",
        "base/poco/Crypto",
        "base/poco/Foundation",
        "base/poco/Net",
        "base/poco/NetSSL_OpenSSL",
        "base/poco/Util",
        "base/poco/XML",
    },
    "src/Parsers": {
        "src/Common",
        "base/poco/Crypto",
        "base/poco/Foundation",
        "base/poco/Net",
        "base/poco/NetSSL_OpenSSL",
        "base/poco/Util",
    },
    "base/base": {
        "base/poco/Foundation",
        "base/poco/Net",
        "base/poco/Util",
    },
    # Repeat corresponding CMakeLists
    "base/poco/Crypto": {
        "base/poco/Foundation",
    },
    "base/poco/Data": {
        "base/poco/Foundation",
    },
    "base/poco/JSON": {
        "base/poco/Foundation",
    },
    "base/poco/Net": {
        "base/poco/Foundation",
    },
    "base/poco/NetSSL_OpenSSL": {
        "base/poco/Crypto",
        "base/poco/Net",
        "base/poco/Util",
    },
    "base/poco/Redis": {
        "base/poco/Net",
    },
    "base/poco/Util": {
        "base/poco/JSON",
        "base/poco/XML",
    },
    "base/poco/XML": {
        "base/poco/Foundation",
    },
    "programs": {
        "src",
    },
    "contrib/orc": {
        "contrib/orc/proto",
    },
}

CONTRIB_PEERDIRS = {
    "base/poco/Crypto": {
        "contrib/libs/openssl",
    },
    "programs": {
        "contrib/libs/apache/arrow_next",
        "contrib/libs/pybind11",
        "contrib/restricted/boost/iostreams",
    },
    "src": {
        "contrib/libs/apache/arrow_next",
        "contrib/libs/protoc",
    },
}


def add_peerdirs(self):
    for target, paths in INTERNAL_PEERDIRS.items():
        with self.yamakes[target] as lib:
            for path in paths:
                lib.PEERDIR.add(f"{self.arcdir}/{path}")

    for target, paths in CONTRIB_PEERDIRS.items():
        with self.yamakes[target] as lib:
            for path in paths:
                lib.PEERDIR.add(path)


def post_install(self):
    # Add missing internal peerdirs between static ch-libs because yamaker is blind.
    add_peerdirs(self)

    with self.yamakes["src"] as dbms:
        # compile divideImpl with different model flags
        # putting the code into different namespaces
        divide_src = "Functions/divide/divideImpl.cpp"
        dbms.SRCS.remove(divide_src)
        dbms.after("SRCS", f"SRC({divide_src} -DNAMESPACE=Generic)")
        for mode in ("SSE2", "AVX2"):
            dbms.CFLAGS.remove(f"-DNAMESPACE={mode}")
            dbms.after("SRCS", f"SRC_C_{mode}({divide_src} -DNAMESPACE={mode})")

        # Помечаем исходники из объектных библиотек как global.
        # См. eliminate-object-libraries.patch
        global_src_dirs = ["Functions", "Functions/array", "Functions/URL"]
        global_srcs = {src for src in dbms.SRCS if os.path.dirname(src) in global_src_dirs}
        dbms.SRCS -= global_srcs
        dbms.SRCS |= {GLOBAL(src) for src in global_srcs}

        # include <readpassphrase/readpassphrase.h> используется в Client/ConnectionParameters.cpp.
        # Т.к. в CH этот файл находится в base/readpassphrase/readpassphrase.h,
        # то в сборке нет специального ADDINCL на эту библиотеку и yamaker его не видит во время unbundle_from.
        dbms.PEERDIR.add("contrib/libs/libc_compat")
        dbms.ADDINCL.add("contrib/libs/libc_compat/include")

        # StorageFileLog is only supported on Linux.
        # https://github.com/ClickHouse/ClickHouse/blob/290ee6bbf17ce883302aecd9fcca7a57ee8e1031/src/CMakeLists.txt#L145-L149
        # See also: 26-darwin-filelog.patch
        storage_file_log_srcs = {
            src for src in dbms.SRCS if isinstance(src, str) and src.startswith("Storages/FileLog/")
        }
        dbms.SRCS -= storage_file_log_srcs
        dbms.after("SRCS", Switch(OS_LINUX=Linkable(SRCS=storage_file_log_srcs)))

        # Functions/FunctionsHashing.h uses XXH_INLINE_* symbols that are generated in xxhash lib
        # only when XXH_INLINE_ALL is ON. The flag should be global, so every .cpp which includes FunctionsHashing.h
        # works without any explicitly set flags.
        # dbms.CFLAGS.remove("-DXXH_INLINE_ALL")
        # dbms.CFLAGS.append(GLOBAL("-DXXH_INLINE_ALL"))
        # 32-pr-69201-allow-custom-database-engines.patch
        dbms.SRCS.add("Databases/registerDatabases.cpp")

        # For MAP_ANON/MAP_ANONYMOUS
        dbms.before(
            "CFLAGS",
            """
            IF (OS_DARWIN)
                CFLAGS(
                    -D_DARWIN_C_SOURCE
                )
            ENDIF()
            """,
        )

        # fastops are supported only on amd64
        dbms.PEERDIR.remove("contrib/libs/fastops/fastops")
        dbms.after("PEERDIR", Switch(ARCH_X86_64=Linkable(PEERDIR=["contrib/libs/fastops/fastops"])))

    os_cflags_switch_with_global = Switch(
        OS_LINUX=Linkable(CFLAGS=[GLOBAL("-DOS_LINUX")]),
        OS_DARWIN=Linkable(CFLAGS=[GLOBAL("-DOS_DARWIN")]),
    )
    os_cflags_switch_without_global = Switch(
        OS_LINUX=Linkable(CFLAGS=["-DOS_LINUX"]),
        OS_DARWIN=Linkable(CFLAGS=["-DOS_DARWIN"]),
    )

    self.yamakes["src"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["base/base"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["src/Common"].PEERDIR.add("library/cpp/sanitizer/include")

    for path, yamake in self.yamakes.items():
        if isinstance(yamake, Recurse):
            continue

        # some libraries might have received PEERDIR to itself. Handle such cases with brute force.
        yamake.PEERDIR.discard(f"{self.arcdir}/{path}")

        # Replace -DOS_LINUX with corresponding Switch statement
        if "-DOS_LINUX" in yamake.CFLAGS:
            if isinstance(yamake, Program):
                yamake.CFLAGS.remove("-DOS_LINUX")
                yamake.before("CFLAGS", os_cflags_switch_without_global)
            else:
                yamake.CFLAGS.remove("-DOS_LINUX")
                yamake.before("CFLAGS", os_cflags_switch_with_global)

        # POCO_HAVE_FD_EPOLL is checked for the <sys/epoll.h> import, which is missing on darwin
        if "-DPOCO_HAVE_FD_EPOLL" in yamake.CFLAGS:
            yamake.CFLAGS.remove("-DPOCO_HAVE_FD_EPOLL")
            yamake.before(
                "CFLAGS",
                """
                IF (OS_LINUX)
                    CFLAGS(
                         -DPOCO_HAVE_FD_EPOLL
                    )
                ENDIF()
                """,
            )

        # abseil-cpp is another library which could not be properly unbundled by includes only
        # ClickHouse uses only flat_hash_map/flat_hash_set from absl/container sublibrary.
        if f"{self.arcdir}/contrib/abseil-cpp" in yamake.ADDINCL:
            yamake.ADDINCL.remove(f"{self.arcdir}/contrib/abseil-cpp")

        # yamaker keep loosing sorted status of ADDINCL.
        # Ornung muss sein!
        yamake.ADDINCL = sorted(yamake.ADDINCL, key=ArcPath._as_cmp_tuple)

    # CHDB changes
    self.yamakes["programs"].before(
        "PEERDIR",
        """
        PY_REGISTER(
            _chdb
        )

        USE_PYTHON3()
        """,
    )

    self.yamakes["contrib/orc"].SRCS.remove("proto/orc_proto.proto")


def post_build(self):
    # '#include "config.h"' is too common and clashes with other contribs.
    fileutil.rename(f"{self.dstdir}/includes/configs/config.h", "clickhouse_config.h")
    for src_dir in ["base", "src", "programs"]:
        fileutil.re_sub_dir(f"{self.dstdir}/{src_dir}", '"config.h"', '"clickhouse_config.h"')
        fileutil.re_sub_dir(f"{self.dstdir}/{src_dir}", "<config.h>", "<clickhouse_config.h>")

    fileutil.copy(
        f"{self.srcdir}/contrib/arrow/cpp/src/arrow/ipc/feather.fbs", f"{self.dstdir}/contrib/arrow/cpp/src/generated"
    )

    # orc proto already exist
    fileutil.rename(f"{self.dstdir}/contrib/orc/proto/orc_proto.proto", "orc_chdb_proto.proto")


chdb = CMakeNinjaNixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/chdb/cpp",
    nixattr="chdb",
    use_provides=[
        "contrib/python/chdb/cpp/.yandex_meta",
    ],
    ignore_commands=[
        "bash",
        "sh",
        "StorageSystemLicenses.sh",
    ],
    addincl_global={
        "base/base": {
            "..",
            "contrib/libs/miniselect/include",  # miniselect headers are included from base/base/sort.h.
            "contrib/python/chdb/cpp/contrib/pdqsort",  # pdqsort headers are included from base/base/sort.h.
        },
        "src/Common": {
            "..",
            "../../base/pcg-random",
            "../../includes/configs",
        },
        "src": {
            ".",
            # metrohash.arcdir,  # metrohash.h is included from FunctionsHashing.h.
            # cityhash.arcdir,  # city.h is included from FunctionsHashing.h.
            # farmhash.arcdir,  # farmhash.h is included from FunctionsHashing.h.
            "contrib/libs/croaring/cpp",  # roaring.hh header is included from GinIndexStore.h.
            "contrib/libs/wyhash",  # wyhash.h header is included from FunctionsHashing.h.
            "contrib/libs/xxhash",  # xxhash.h header is included from FunctionsHashing.h.
            "contrib/restricted/murmurhash",  # MurmurHash{2,3}.h headers are included from FunctionsHashing.h.
        },
    },
    copy_sources=[
        ".editorconfig",
        # TODO: unbundle
        "base/pcg-random",
        # used in metrika
        "programs/library-bridge/ExternalDictionaryLibraryAPI.h",
        # Some .h are included only under disabled ifdef,
        # but we could not easily disable them because some .h from these dirs are included unconditionaly.
        # Just copy missing .h to avoid configure errors in ya make.
        "src/Coordination/*.h",
        "src/Core/PostgreSQL/*.h",
        "src/Databases/*.h",
        "src/Databases/MySQL/*.h",
        "src/Disks/IO/*.h",
        "src/Disks/ObjectStorages/*.h",
        "src/Formats/*.h",
        "src/Functions/*.h",
        "src/IO/*.h",
        "src/Storages/ObjectStorage/HDFS/*.h",
        "src/Disks/ObjectStorages/HDFS/*.h",
        "src/Storages/MergeTree/*.h",
        "src/Storages/System/*.h",
        "src/TableFunctions/*.h",
        "src/Storages/TimeSeries/*.h",
    ],
    keep_paths=[
        "contrib/orc/proto/ya.make",
        "ya.make",
    ],
    disable_includes=[
        # ClickHouse will be built without
        #
        # AzureBlob support:
        "azure/**.hpp",
        "Disks/ObjectStorages/AzureBlobStorage/*.h",
        # PostgreSQL support:
        "pqxx/*",
        "Databases/PostgreSQL/*.h",
        "Storages/PostgreSQL/*.h",
        # MongoDB support:
        "Poco/MongoDB/",
        # MySQL support:
        "mysql/*.h",
        "mysqlxx/*.h",
        "Formats/MySQLSource.h",
        "MySQLSource.h",
        "TableFunctions/TableFunctionMySQL.h",
        "Processors/Sources/MySQLSource.h",
        # HDFS support:
        "hdfs/*.h",
        "TableFunctions/TableFunctionHDFS.h",
        # SQLite support:
        "Databases/SQLite/*.h",
        "sqlite3.h",
        # RocksDB support:
        "rocksdb/*.h",
        "Storages/RocksDB/*.h",
        # Kafka support:
        "Storages/Kafka/*.h",
        # RabbitMQ support:
        "Storages/RabbitMQ/*.h",
        # NATS support:
        "Storages/NATS/*.h",
        # capnproto support:
        "capnp/*.h",
        # llvm support:
        "llvm/",
        # bzip support:
        "bzlib.h",
        # BoringSSL support:
        "openssl/digest.h",
        "openssl/hkdf.h",
        "openssl/aead.h",
        "openssl/fips.h",
        # Compact Language Detector 2 (cld2) support:
        "compact_lang_det.h",
        # QPL compression support:
        "libaccel_config.h",
        # Google S2 support:
        "s2_fwd.h",
        # stats support:
        "stats.hpp",
        # hyperscan support (TODO: re-enable these):
        "hs_compile.h",
        "hs.h",
        # Cassandra support:
        "cassandra.h",
        "CassandraSource.h",
        "Dictionaries/CassandraSource.h",
        # GRPC support:
        "clickhouse_grpc.grpc.pb.h",
        "grpc++/**.h",
        # Comprehensive authorization mechanisms support:
        "ldap.h",
        "gssapi/*.h",
        # Sentry support:
        "sentry.h",
        # nuraft / dynamic master selection support:
        "libnuraft/*.hxx",
        "Server/KeeperTCPHandlerFactory.h",
        "Coordination/Standalone/**.h",
        # minizip
        "mz.h",
        "unzip.h",
        "zip.h",
        # yaml-cpp support:
        "yaml-cpp/**.h",
        # liburing support:
        "liburing.h",
        # skim support:
        "skim.h",
        # PowerPC support:
        "vec_crc32.h",
        # s390x support:
        "crc32-s390x.h",
        "base/crc32c_s390x.h",
        # BMI2 support:
        "morton-nd/mortonND_BMI2.h",
        # libarchive support:
        "archive.h",
        "archive_entry.h",
        # FIU support:
        "fiu.h",
        "fiu-control.h",
        # ULID support:
        "ulid.h",
        # BLAKE3 support:
        "blake3.h",
        # GWP-Asan support:
        "gwp_asan/**.h",
        # PRQL support:
        "prql.h",
        # Annoy support:
        "annoylib.h",
        "kissrandom.h",
        # USearch support:
        "usearch/*.hpp",
        # libssh support:
        "libssh/libssh.h",
        # Intel libs support:
        "qpl/qpl.h",
        "qatseqprod.h",
        # Prometheus support:
        "prompb/**.h",
        # Unicode IDNA library support:
        "ada/idna/",
        # datasketches library support:
        "count_min.hpp",
        # aklomp base64 support
        "libbase64.h",
        # numa support:
        "numa.h",
        # INCBIN support:
        # "incbin.h",
        # I just do not know what these includes are intended for
        "Common/BitonicSort.h",
        "theta_a_not_b.hpp",
        "theta_intersection.hpp",
        "theta_sketch.hpp",
        "theta_union.hpp",
        "wnb/core/wordnet.hh",
        "RdrLemmatizer.h",
        "libstemmer.h",
        "kj/io.h",
        "sqids/sqids.hpp",
        # Missed dbpoco includes
        "winconfig.h",
        "expat.h",
        "Poco/UnWindows.h",
        "Poco/zlib.h",
        "Poco/NamedEvent_Android.h",
        "NamedEvent_Android.cpp",
        "Poco/NamedMutex_Android.h",
        "NamedMutex_Android.cpp",
        "Poco/FPEnvironment_DEC.h",
        "FPEnvironment_DEC.cpp",
        "Poco/FPEnvironment_DUMMY.h",
        "FPEnvironment_DUMMY.cpp",
        "Poco/PipeImpl_DUMMY.h",
        "PipeImpl_DUMMY.cpp",
        "Poco/SharedLibrary_HPUX.h",
        "SharedLibrary_HPUX.cpp",
        "SharedMemory_DUMMY.cpp",
        # arrow cmake
        "arrow/python/pyarrow.h",
        "BpackingAvx512.hh",
    ],
    put=PUT,
    put_with=PUT_WITH,
    install_targets=list(PUT.keys()),
    unbundle_from=UNBUNDLE_FROM,
    post_build=post_build,
    post_install=post_install,
    platform_dispatchers=[
        "includes/configs/clickhouse_config.h",
    ],
    # Это нужно что бы пометить исходники из clickhouse_functions_obj как global,
    # но это работает супер медленно, поэтому легче руками пометить все в post_install.
    # global_srcs=True,
)
