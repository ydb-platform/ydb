import dataclasses
import re
from collections import defaultdict
from typing import List, Dict, Tuple, Set


shard_partition_re = re.compile(r"_\d+$")


def get_common_shard_name(name):
    return shard_partition_re.sub("", name)


@dataclasses.dataclass
class CTestTestcaseLog:
    classname: str
    method: str
    fn: str
    url: str
    shard: "CTestLogShard"


class CTestLogShard:
    def __init__(self, name, status, log_url):
        self.name = name
        self.status = status
        self.log_url = log_url
        self.testcases: List[CTestTestcaseLog] = []
        self.idx: Dict[Tuple[str, str], CTestTestcaseLog] = {}

    def add_testcase(self, classname, method, fn, url):
        log = CTestTestcaseLog(classname, method, fn, url, self)
        self.testcases.append(log)
        self.idx[(classname, method)] = log

    def get_log(self, classname, method):
        return self.idx.get((classname, method))

    def get_extra_logs(self, found_testcases: Set[Tuple[str, str]]):
        extra_keys = set(self.idx.keys()) - found_testcases
        return [self.idx[k] for k in extra_keys]

    @property
    def logs(self):
        return self.idx.values()

    @property
    def filename(self):
        return f"{self.name}.xml"


class CTestLog:
    def __init__(self):
        self.name_shard = {}  # type: Dict[str, CTestLogShard]
        self.storage = defaultdict(dict)

    def add_shard(self, name, status, log_url):
        common_name = get_common_shard_name(name)
        shard = self.storage[common_name][name] = self.name_shard[name] = CTestLogShard(name, status, log_url)
        return shard

    def has_error_shard(self, name):
        return name in self.name_shard

    def get_shard(self, name):
        return self.name_shard[name]

    def get_log(self, common_name, cls, name):
        for shard in self.storage[common_name].values():
            log = shard.get_log(cls, name)

            if log:
                return log

        return None

    def get_extra_tests(self, common_name, names):
        result = []
        for shard in self.storage[common_name].values():
            extra = shard.get_extra_logs(set(names))
            if extra:
                result.extend(extra)
        return result

    @property
    def has_logs(self):
        return len(self.name_shard) > 0
