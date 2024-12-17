from .shard import Shard


class MultiShard(object):
    def __init__(self):
        self._all_shards = dict()

    def get_or_create(self, project, cluster, service) -> Shard:
        key = self._compose_key(project, cluster, service)
        s = self._all_shards.get(key)
        if s is None:
            s = Shard(project, cluster, service)
            self._all_shards[key] = s
        return s

    def find(self, project, cluster, service) -> Shard:
        key = self._compose_key(project, cluster, service)
        return self._all_shards.get(key)

    def delete(self, project, cluster, service):
        key = self._compose_key(project, cluster, service)
        self._all_shards.pop(key, None)

    def clear(self):
        self._all_shards.clear()

    @staticmethod
    def _compose_key(project, cluster, service) -> str:
        return "/".join([project, cluster, service])
