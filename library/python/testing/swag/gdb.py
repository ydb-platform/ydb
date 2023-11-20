#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import yatest.common

logger = logging.getLogger(__name__)

SHORT_DUMP_COMMAND = "{gdb} {executable} {core_file} --eval-command='backtrace full' --batch -q"
LONG_DUMP_COMMAND = "{gdb} {executable} {core_file} --eval-command='thread apply all bt full' --batch -q"


def get_gdb():
    return yatest.common.gdb_path()


def run_gdb_command(command, stdout_file, stderr_file):
    logger.debug("Running gdb command %s" % command)

    with open(stdout_file, "w") as out, open(stderr_file, "w") as err:
        yatest.common.process.execute(
            command,
            check_exit_code=True,
            wait=True,
            shell=True,
            stdout=out,
            stderr=err
        )


def dump_traceback(executable, core_file, output_file):
    """
    Dumps traceback if its possible

    :param executable: binary for gdb
    :param core_file: core file for gdb
    :param output_file: file to dump traceback to, also dump full traceback to <output_file + ".full">

    :return: string tuple (short_backtrace, full_backtrace)
    """
    try:
        gdb = get_gdb()
        short_dump_command = SHORT_DUMP_COMMAND.format(gdb=gdb, executable=executable, core_file=core_file)
        long_dump_command = LONG_DUMP_COMMAND.format(gdb=gdb, executable=executable, core_file=core_file)
        run_gdb_command(short_dump_command, output_file, output_file + '.err')
        output_file_full = output_file + ".full"
        output_file_full_err = output_file_full + '.err'
        run_gdb_command(long_dump_command, output_file_full, output_file_full_err)
    except Exception:
        logger.exception("Failed to print trace")
        return '', ''

    short_backtrace = ''
    full_backtrace = ''
    with open(output_file) as o, open(output_file_full) as e:
        short_backtrace = o.read()
        full_backtrace = e.read()
    return short_backtrace, full_backtrace
