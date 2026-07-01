#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from ydb.tests.library.nemesis.safety_warden import (
    GrepLogFileForMarkers,
    GrepGzippedLogFilesForMarkersSafetyWarden,
    GrepJournalctlKernelForPatternsSafetyWarden,
)


def kikimr_start_logs_safety_warden_factory(
        executor=None,
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
        executor=None,
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


def kikimr_grep_kernel_log_safety_warden_factory(
        executor=None,
        lines_after=5,
        hours_back=24,
):
    markers = ['Out of memory: Kill process']

    return [
        GrepJournalctlKernelForPatternsSafetyWarden(
            executor,
            list_of_markers=markers,
            lines_after=lines_after,
            hours_back=hours_back,
        )
    ]
