import sys
import os
import subprocess
import platform
import argparse
import re
import struct
import base64


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--ap-classpath', nargs='*', type=str, dest='classpath')
    parser.add_argument('--ap-options', nargs='*', type=str, dest='ap_options')
    cmd_start = args.index('--')
    return parser.parse_args(args[:cmd_start]), args[cmd_start + 1 :]


def get_ap_classpath(directory):
    jar_re = re.compile(r'.*(?<!-sources)\.jar')
    found_jars = [
        os.path.join(address, name)
        for address, dirs, files in os.walk(directory)
        for name in files
        if jar_re.match(name)
    ]
    if len(found_jars) != 1:
        raise Exception("found %d JAR files in directory %s" % (len(found_jars), directory))
    arg = 'plugin:org.jetbrains.kotlin.kapt3:apclasspath=' + found_jars[0]
    return '-P', arg


def get_ap_options(ap_options):
    if not ap_options:
        return []
    # Format of apoptions https://kotlinlang.org/docs/kapt.html#ap-javac-options-encoding
    # ObjectOutputStream https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/io/ObjectOutputStream.java

    kvs = bytes()  # all key-values
    for ap_option in ap_options:
        try:
            k, v = ap_option.split('=', 1)
        except ValueError:
            import logging

            logging.error("Expect key=value format at '" + ap_option + "' in KAPT_ANNOTATION_PROCESSOR_OPTIONS")
            continue
        k = bytes(k)  # UTF-8 supported
        kvs += struct.pack(">H", len(k))
        kvs += k
        v = bytes(v)  # UTF-8 supported
        kvs += struct.pack(">H", len(v))
        kvs += v

    s = struct.pack(">H", 0xACED)  # STREAM_MAGIC
    s += struct.pack(">H", 0x0005)  # STREAM_VERSION
    data_len = 4 + len(kvs)  # sizeof(ui32) options count + length of all key-values
    if data_len < 256:
        s += struct.pack("B", 0x77)  # TC_BLOCKDATA
        s += struct.pack("B", data_len)
    else:
        s += struct.pack("B", 0x7A)  # TC_BLOCKDATALONG
        s += struct.pack(">I", data_len)
    s += struct.pack(">I", len(ap_options))  # count of options
    s += kvs  # key-values of all options

    arg = 'plugin:org.jetbrains.kotlin.kapt3:apoptions=' + base64.b64encode(s).decode('utf-8')
    return '-P', arg


def create_extra_args(args):
    cp_opts = [arg for d in args.classpath for arg in get_ap_classpath(d)] + [
        arg for arg in get_ap_options(args.ap_options)
    ]
    return cp_opts


if __name__ == '__main__':
    args, cmd = parse_args(sys.argv[1:])
    res = cmd + create_extra_args(args)
    if platform.system() == 'Windows':
        sys.exit(subprocess.Popen(res).wait())
    else:
        os.execv(res[0], res)
