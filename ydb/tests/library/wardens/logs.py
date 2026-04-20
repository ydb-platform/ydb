#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from ydb.tests.library.nemesis.safety_warden import (
    CommandExecutor,
    GrepLogFileForMarkers,
    GrepGzippedLogFilesForMarkersSafetyWarden,
    GrepDMesgForPatternsSafetyWarden,
    RemoteCommandExecutor,
)


def _ensure_executor(executor: CommandExecutor, list_of_hosts=None, ssh_username=None):
    """Return *executor* if given, otherwise build a ``RemoteCommandExecutor`` from legacy args."""
    if executor is not None:
        return executor
    if list_of_hosts is None:
        raise ValueError("Either executor or list_of_hosts must be provided")
    return RemoteCommandExecutor(list_of_hosts, username=ssh_username)


def kikimr_start_logs_safety_warden_factory(
        executor: CommandExecutor = None,
        deploy_path="/Berkanavt/kikimr/logs/",
        lines_after=5,
        cut=True,
        modification_days=1,
):
    start_markers = ['VERIFY', 'FAIL ', 'signal 11', 'signal 6', 'signal 15', 'uncaught exception', 'ERROR: AddressSanitizer', 'SIG']
    return [
        GrepLogFileForMarkers(
            executor,
            log_file_name=os.path.join(deploy_path, 'kikimr.start'),
            list_of_markers=start_markers,
            lines_after=lines_after,
            cut=cut,
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            executor,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.start.*gz'),
            list_of_markers=start_markers,
            modification_days=modification_days,
            lines_after=lines_after,
            cut=cut,
        ),
    ]


def kikimr_crit_and_alert_logs_safety_warden_factory(
        executor: CommandExecutor = None,
        deploy_path="/Berkanavt/kikimr/logs/",
):
    crit_markers = [':BS_HULLRECS CRIT:', ':BS_LOGCUTTER CRIT:', 'ALERT', ':BS_LOCALRECOVERY CRIT:']
    alert_markers = ['ALERT']
    return [
        GrepLogFileForMarkers(
            executor,
            log_file_name=os.path.join(deploy_path, 'kikimr.crit'),
            list_of_markers=crit_markers,
        ),
        GrepLogFileForMarkers(
            executor,
            log_file_name=os.path.join(deploy_path, 'kikimr.alert'),
            list_of_markers=alert_markers,
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            executor,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.crit.*gz'),
            list_of_markers=crit_markers,
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            executor,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.alert.*gz'),
            list_of_markers=alert_markers,
        ),
    ]


def kikimr_grep_dmesg_safety_warden_factory(
        executor: CommandExecutor = None,
        lines_after=5,
):
    markers = ['Out of memory: Kill process']

    return [
        GrepDMesgForPatternsSafetyWarden(
            executor,
            list_of_markers=markers,
            lines_after=lines_after,
        )
    ]
