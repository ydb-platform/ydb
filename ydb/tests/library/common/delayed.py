# -*- coding: utf-8 -*-
from hamcrest import has_properties, contains_inanyorder, is_not
import logging
import time

from ydb.tests.library.common.wait_for import wait_for_and_assert, wait_for
from ydb.tests.library.matchers.response_matchers import DynamicFieldsProtobufMatcher
from ydb.tests.library.common.types import TabletStates
from ydb.tests.library.wardens import hive

logger = logging.getLogger(__name__)


def wait_tablets_state_by_id(
        client, state, tablet_ids=(), message='', timeout_seconds=120, skip_generations=None, generation_matcher=None):

    if skip_generations is None:
        skip_generations = {}
    if generation_matcher is None:
        generation_matcher = is_not

    def query_tablet_state():
        return client.tablet_state(tablet_ids=tablet_ids)

    wait_for_and_assert(
        query_tablet_state,
        DynamicFieldsProtobufMatcher().TabletStateInfo(
            contains_inanyorder(*(
                has_properties(
                    TabletId=tablet_id,
                    State=state,
                    Generation=generation_matcher(skip_generations.get(tablet_id)),
                )
                for tablet_id in tablet_ids
            ))
        ),
        message=message,
        timeout_seconds=timeout_seconds,
        log_progress=True
    )
    return None


def collect_tablets_state(client, tablet_ids=()):
    tablet_state = client.tablet_state(tablet_ids=tablet_ids)

    tablets_info = {}
    for tablet_id in tablet_ids:
        # unknown state
        tablets_info[tablet_id] = None

    for tablet_state_info in tablet_state.TabletStateInfo:
        tablet_id = tablet_state_info.TabletId
        if tablet_id not in tablets_info:
            continue

        if tablets_info[tablet_id] is not None:
            logger.error("duplicate found for tablet id %s" % tablet_id)

        tablets_info[tablet_id] = tablet_state_info.State
    return tablets_info


def collect_inactive_tablets(client, tablet_ids=()):
    tablets_info = collect_tablets_state(client, tablet_ids)
    non_active_tablets = list()

    for tablet_id, state in tablets_info.items():
        if state != TabletStates.Active:
            non_active_tablets.append(
                (
                    tablet_id,
                    state,
                )
            )
    return sorted(non_active_tablets)


def pretty_tablet_info(info):
    tablet_id, actual_state = info
    return "(%s: %s)" % (
        tablet_id, TabletStates.from_int(
            actual_state
        )
    )


def wait_tablets_are_active(client, tablet_ids=(), timeout_seconds=120, cluster=None, details=None):

    start_time = time.time()
    logger.info("Waiting tablets to become active")

    def predicate(raise_error=False):
        inactive_tablets = collect_inactive_tablets(client, tablet_ids)
        if inactive_tablets:
            inactive_tablets_count = len(inactive_tablets)
            if len(inactive_tablets) > 10:
                inactive_tablets = inactive_tablets[:10]
            extra_details = "is empty" if details is None else str(details)
            message = (
                "%d seconds passed, %d tablet(s) are not active. "
                "Inactive tablets are (first 10 entries): %s. Additional info %s" % (
                    int(time.time() - start_time),
                    inactive_tablets_count,
                    " ".join(map(pretty_tablet_info, inactive_tablets)),
                    extra_details,
                )
            )

            if cluster is not None:
                hive_violations = hive.BootQueueSizeWarden(cluster).list_of_liveness_violations
                hive_violations += hive.AllTabletsAliveLivenessWarden(cluster).list_of_liveness_violations
                if hive_violations:
                    message = "\n".join(
                        hive_violations + [
                            message
                        ]
                    )

            logger.error(message)
            if raise_error:
                raise AssertionError(
                    "\n".join(
                        ["", "#" * 30, message, "#" * 30]
                    )
                )

            return False
        return True

    wait_for(predicate, timeout_seconds)
    predicate(raise_error=True)
    logger.info(
        "%d tablet(s) are active now." % len(
            tablet_ids
        )
    )
