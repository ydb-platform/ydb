import datetime
import logging
import time
import random
from threading import Event
from collections import defaultdict

import grpc

from .ch import Clickhouse
from .cloud_config import create_userdata
from .gh import Github
from .jobs import get_jobs_summary
from .utils import generate_short_id, ExpireItemsSet
from .yc import YandexCloudProvider

logger = logging.getLogger(__name__)


class DynamicValue:
    def __init__(self, value):
        self.default = self.current = value
        self.expire_time = None

    def update_for(self, value, seconds):
        self.current = value
        self.expire_time = time.time() + seconds

    def get(self):
        if self.expire_time is not None:
            if time.time() > self.expire_time:
                self.current = self.default
                self.expire_time = None
        return self.current


class TimeoutFlag:
    def __init__(self, expire_time=300):
        self._value = False
        self.expire_time = expire_time
        self.expire_ts = None

    def set(self):
        self.expire_ts = time.time() + self.expire_time
        self._value = True

    def clear(self):
        self.expire_ts = None
        self._value = False

    @property
    def value(self):
        if self.expire_ts is None:
            return self._value

        if time.time() > self.expire_ts:
            self.clear()

        return self._value

    def __bool__(self):
        return self.value


class VMQuotaError(Exception):
    pass


class ScaleController:
    prefix = "ghrun"

    resource_exhausted_pause = 300

    # 10 minute limit for start VM and register github runner
    stale_runner_threshold = datetime.timedelta(seconds=600)

    def __init__(self, cfg, ch: Clickhouse, gh: Github, yc: YandexCloudProvider, exit_event: Event):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.ch = ch
        self.gh = gh
        self.yc = yc
        self.maximum_vms = DynamicValue(40)
        self.resource_exhausted = TimeoutFlag(self.resource_exhausted_pause)
        self.fresh_runners = defaultdict(lambda: ExpireItemsSet(60, "fresh_runners"))
        self.wait_for_runners = defaultdict(lambda: ExpireItemsSet(600, "wait_for_runners"))
        self.exit_event = exit_event

    def tick(self):
        queues = get_jobs_summary(self.ch)

        runner_vms, vm_count, vm_provisioning = self.yc.get_vm_list(self.prefix)
        self.logger.info("vms: %s/%s (total/provisioning)", vm_count, vm_provisioning)

        do_check_idle_runners = True

        for queue in queues:
            wait_for_runners = self.wait_for_runners[queue.preset]
            fresh_runners = self.fresh_runners[queue.preset]
            self.logger.info("preset %s: jobs %s/%s, wait_runners: %s, fresh_runners: %s",
                             queue.preset, queue.in_queue, queue.in_progress, len(wait_for_runners), len(fresh_runners))
            cnt = queue.in_queue - len(wait_for_runners) - len(fresh_runners)

            if queue.in_queue != 0:
                do_check_idle_runners = True

            if cnt > 0:
                if self.resource_exhausted:
                    self.logger.info("resource_exhausted: skip preset %s", queue.preset)
                    continue

                try:
                    runner_name = self.start_runner(queue.preset)
                except VMQuotaError:
                    self.logger.info("QUOTA_ERROR, update maximum_vm=%s for %s seconds",
                                     vm_count, self.resource_exhausted_pause)
                    self.resource_exhausted.set()
                    break
                else:
                    wait_for_runners.add(runner_name)

        # remove idle github runners one per tick
        if do_check_idle_runners:
            self.check_idle_runners(runner_vms)

    def check_idle_runners(self, runner_vms):
        # FIXME: too complex
        self.logger.info("check for idle and new runners")

        wait_list = {}
        fresh_list = {}

        for preset, wait_for_runners in self.wait_for_runners.items():
            wait_for_runners.evict_expired()
            for runner_id in wait_for_runners.items:
                wait_list[runner_id] = preset

        for preset, fresh_runners in self.fresh_runners.items():
            fresh_runners.evict_expired()
            for runner_id in fresh_runners.items:
                fresh_list[runner_id] = preset

        runner_list = self.gh.get_runners()

        runner_names = {r.name for r in runner_list}

        for runner in runner_list:
            if not runner.has_tag(self.prefix):
                continue

            runner_names.add(runner.name)

            if runner.name in wait_list:
                preset = wait_list[runner.name]
                self.logger.info("runner %s is ready", runner.full_name)
                self.wait_for_runners[preset].remove(runner.name)
                self.fresh_runners[preset].add(runner.name)
            elif runner.online and not runner.busy and runner.name not in fresh_list:
                self.logger.info("remove GH runner: #%s %s (%s)", runner.id, runner.name, ", ".join(runner.tags))
                self.delete_runner(runner)
                break
            elif runner.busy:
                if runner.name in fresh_list:
                    preset = fresh_list[runner.name]
                    self.fresh_runners[preset].remove(runner.name)
                    self.logger.info("remove runner %s from fresh runners", runner.full_name)
            elif runner.offline:
                self.logger.info(
                    "remove offline GH runner: #%s %s (%s)", runner.id, runner.name, ", ".join(runner.tags)
                )
                self.delete_runner(runner)

        stale_runners = set(runner_vms.keys()) - runner_names

        for runner_name in stale_runners:
            instance_id, created_at = runner_vms[runner_name]
            if datetime.datetime.now() - created_at > self.stale_runner_threshold:
                self.logger.warning("remove stale VM %s (%s), created_at=%s", runner_name, instance_id, created_at)
                self.delete_vm(instance_id)

    def start_runner(self, preset_name: str):
        runner_name = f"{self.prefix}-{generate_short_id()}"
        new_runner_token = self.gh.get_new_runner_token()
        # FIXME: auto-provisioned defined twice
        labels = [self.prefix, "auto-provisioned", f"build-preset-{preset_name}"]

        preset = self.cfg.vm_presets.get(preset_name, {})
        if 'additional_labels' in preset:
            labels.extend(preset['additional_labels'])

        vm_labels = {self.prefix: runner_name}
        user_data = create_userdata(self.gh.html_url, new_runner_token, runner_name, labels, self.cfg.ssh_keys)

        placement = random.choice(self.cfg.yc_zones)
        zone_id = placement['zone_id']
        subnet_id = placement['subnet_id']

        self.logger.info("start runner %s in %s (%s)", runner_name, zone_id, labels)

        try:
            instance = self.yc.start_vm(zone_id, subnet_id, runner_name, preset_name, user_data, vm_labels)
        except grpc.RpcError as rpc_error:
            # noinspection PyUnresolvedReferences
            if rpc_error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise VMQuotaError()
            else:
                raise

        self.logger.info("instance %s started", instance.id)
        return runner_name

    def delete_runner(self, runner):
        self.logger.info("remove runner %s from github", runner.full_name)
        self.gh.delete_runner(runner.id)
        self.logger.info("stop runner %s VM %s", runner.full_name, runner.instance_id)
        self.delete_vm(runner.instance_id)

    def delete_vm(self, instance_id):
        try:
            self.yc.delete_vm(instance_id, self.prefix)
        except grpc.RpcError as rpc_error:
            # noinspection PyUnresolvedReferences
            error_code = rpc_error.code()
            if error_code == grpc.StatusCode.NOT_FOUND:
                self.logger.info("vm %s has been already deleted (%s)", instance_id, rpc_error)
            elif error_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                self.logger.error("RESOURCE_EXHAUSTED while deleting VMs")

    def loop(self):
        self.logger.info(
            "start main loop, gh_repo=%s, ch_url=%s, folder_id=%s",
            self.gh.repo,
            self.ch.url,
            self.cfg.yc_folder_id,
        )
        sleep_seconds = 15
        while not self.exit_event.is_set():
            self.tick()
            time.sleep(sleep_seconds)
