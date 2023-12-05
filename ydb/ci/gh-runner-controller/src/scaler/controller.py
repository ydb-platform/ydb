import logging
import time
from threading import Event

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


class ScaleController:
    prefix = "ghrun"

    resource_exhausted_pause = 300

    def __init__(self, cfg, ch: Clickhouse, gh: Github, yc: YandexCloudProvider, exit_event: Event):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.ch = ch
        self.gh = gh
        self.yc = yc
        self.maximum_vms = DynamicValue(20)
        self.fresh_runners = ExpireItemsSet(60, lambda i: self.logger.info("remove %s from fresh_runners", i))
        self.wait_for_runners = ExpireItemsSet(600, lambda i: self.logger.info("remove %s from wait_for_runners", i))
        self.exit_event = exit_event

    def tick(self):
        jobs_queued, jobs_in_progress = get_jobs_summary(self.ch)
        maximum_vms = self.maximum_vms.get()
        vm_count, vm_provisioning = self.yc.get_vm_count(self.prefix)

        self.logger.info(
            "jobs: %s/%s (queued, in_progress), vms: %s/%s/%s (total/provisioning/max), wait_runners: %s, "
            "fresh_runners: %s",
            jobs_queued,
            jobs_in_progress,
            vm_count,
            vm_provisioning,
            maximum_vms,
            len(self.wait_for_runners),
            len(self.fresh_runners),
        )

        if vm_count >= maximum_vms:
            self.logger.info("maximum vms reached")
        else:
            cnt = min(maximum_vms - vm_count, jobs_queued - len(self.wait_for_runners) - len(self.fresh_runners))
            if cnt > 0:
                self.logger.info("try to start %s runners", cnt)
                for x in range(cnt):
                    try:
                        runner_name = self.start_runner()
                    except grpc.RpcError as rpc_error:
                        if rpc_error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                            self.logger.info(
                                "RESOURCE_EXHAUSTED, update maximum_vm=%s for %s seconds",
                                vm_count,
                                self.resource_exhausted_pause,
                            )
                            self.maximum_vms.update_for(vm_count, self.resource_exhausted_pause)
                            break
                        else:
                            raise
                    else:
                        self.wait_for_runners.add(runner_name)
                        # Incrementing vm_count without waiting success start of the vm
                        # it helps when we have a lot of tasks and maximum_vm can be fixed with too low value
                        vm_count += 1

        # remove idle github runners one per tick
        if jobs_queued == 0 or self.wait_for_runners:
            self.logger.info("check for idle and new runners")
            for runner in self.gh.get_runners():
                if not runner.has_tag(self.prefix):
                    continue

                if self.wait_for_runners.remove_if_contains(runner.name):
                    self.logger.info("runner %s is ready", runner.full_name)
                    self.fresh_runners.add(runner.name)
                elif runner.online and not runner.busy:
                    self.logger.info("remove GH runner: #%s %s (%s)", runner.id, runner.name, ", ".join(runner.tags))
                    self.delete_runner(runner)
                    break
                elif runner.busy:
                    if self.fresh_runners.remove_if_contains(runner.name):
                        self.logger.info("remove runner %s from fresh runners", runner.full_name)
                elif runner.offline:
                    self.logger.info(
                        "remove offline GH runner: #%s %s (%s)", runner.id, runner.name, ", ".join(runner.tags)
                    )
                    self.delete_runner(runner)

    def start_runner(self):
        runner_name = f"{self.prefix}-{generate_short_id()}"
        new_runner_token = self.gh.get_new_runner_token()
        # FIXME: auto-provisioned defined twice
        labels = [self.prefix, "auto-provisioned"]
        vm_labels = {self.prefix: runner_name}
        user_data = create_userdata(self.gh.html_url, new_runner_token, runner_name, labels, self.cfg.ssh_keys)
        self.logger.info("start runner %s (%s)", runner_name, labels)
        instance = self.yc.start_vm(runner_name, user_data, vm_labels)
        self.logger.info("instance %s started", instance.id)
        return runner_name

    def delete_runner(self, runner):
        self.logger.info("remove runner %s from github", runner.full_name)
        self.gh.delete_runner(runner.id)
        self.logger.info("stop runner %s VM %s", runner.full_name, runner.instance_id)

        try:
            self.yc.delete_vm(runner.instance_id, self.prefix)
        except grpc.RpcError as rpc_error:
            # noinspection PyUnresolvedReferences
            error_code = rpc_error.code()
            if error_code == grpc.StatusCode.NOT_FOUND:
                self.logger.info("vm %s has been already deleted (%s)", runner.instance_id, rpc_error)
            elif error_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                self.logger.error("RESOURCE_EXHAUSTED while deleting VMs")

    def loop(self):
        self.logger.info(
            "start main loop, gh_repo=%s, ch_url=%s, folder_id=%s, zone=%s, subnet=%s",
            self.gh.repo,
            self.ch.url,
            self.cfg.yc_folder_id,
            self.cfg.yc_zone_id,
            self.cfg.yc_subnet_id,
        )
        sleep_seconds = 15
        while not self.exit_event.is_set():
            self.tick()
            time.sleep(sleep_seconds)
