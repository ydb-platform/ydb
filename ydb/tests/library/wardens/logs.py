#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

# noinspection PyUnresolvedReferences
from ydb.tests.library.nemesis.safety_warden import GrepLogFileForMarkers, GrepDMesgForPatternsSafetyWarden
# noinspection PyUnresolvedReferences
from ydb.tests.library.nemesis.safety_warden import GrepGzippedLogFilesForMarkersSafetyWarden


def kikimr_start_logs_safety_warden_factory(
        list_of_host_names, ssh_username, deploy_path, lines_after=5, cut=True, modification_days=1
):
    start_markers = ['VERIFY', 'FAIL ', 'signal 11', 'signal 6', 'signal 15', 'uncaught exception', 'ERROR: AddressSanitizer', 'SIG']
    username = ssh_username
    return [
        GrepLogFileForMarkers(
            list_of_host_names,
            log_file_name=os.path.join(deploy_path, 'kikimr.start'),
            list_of_markers=start_markers,
            username=username,
            lines_after=lines_after,
            cut=cut
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            list_of_host_names,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.start.*gz'),
            list_of_markers=start_markers,
            modification_days=modification_days,
            username=username,
            lines_after=lines_after,
            cut=cut
        ),
    ]


def kikimr_crit_and_alert_logs_safety_warden_factory(
        list_of_host_names, ssh_username, deploy_path="/Berkanavt/kikimr/logs/"
):
    crit_markers = [':BS_HULLRECS CRIT:', ':BS_LOGCUTTER CRIT:', 'ALERT', ':BS_LOCALRECOVERY CRIT:']
    alert_markers = ['ALERT']
    username = ssh_username
    return [
        GrepLogFileForMarkers(
            list_of_host_names,
            log_file_name=os.path.join(deploy_path, 'kikimr.crit'),
            list_of_markers=crit_markers,
            username=username
        ),
        GrepLogFileForMarkers(
            list_of_host_names,
            log_file_name=os.path.join(deploy_path, 'kikimr.alert'),
            list_of_markers=alert_markers,
            username=username
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            list_of_host_names,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.crit.*gz'),
            list_of_markers=crit_markers,
            username=username
        ),
        GrepGzippedLogFilesForMarkersSafetyWarden(
            list_of_host_names,
            log_file_pattern=os.path.join(deploy_path, 'kikimr.alert.*gz'),
            list_of_markers=alert_markers,
            username=username
        ),
    ]


def kikimr_grep_dmesg_safety_warden_factory(list_of_host_names, ssh_username, lines_after=5):
    markers = ['Out of memory: Kill process']

    return [
        GrepDMesgForPatternsSafetyWarden(
            list_of_host_names,
            list_of_markers=markers,
            username=ssh_username,
            lines_after=lines_after
        )
    ]
