class Config(object):
    def __init__(self, auth, shards):
        self.auth = auth
        self.shards = set(shards)

    def contains(self, project, cluster, service):
        return not self.shards or (project, cluster, service) in self.shards

    @staticmethod
    def parse_shards(shard_strings):
        shards = list()
        for shard in shard_strings:
            p, c, s = shard.split("/")
            shards.append((p, c, s))
        return shards
