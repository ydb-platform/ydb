import json

# Variant 1
# {
#     "commonLabels": {
#       "host": "solomon-push"
#     },
#     "metrics": [
#       {
#         "labels": { "sensor": "Memory" },
#         "ts": "2014-11-10T13:14:15Z",
#         "value": 456
#       },
#       {
#         "labels": { "sensor": "Memory" },
#         "ts": 1415631708,
#         "value": 463
#       }
#     ]
#   }

# Variant 2 (cloud write)
# {
#     "commonLabels": {
#       "host": "solomon-push"
#     },
#     "metrics": [
#       {
#         "name" : "Memory",
#         "labels": { "cluster": "sas" },
#         "ts": "2014-11-10T13:14:15Z",
#         "value": 456
#       },
#       {
#         "name" : "Memory",
#         "labels": { "cluster": "sas" },
#         "ts": 1415631708,
#         "value": 463
#       }
#     ]
#   }


class Shard(object):
    def __init__(self, project, cluster, service):
        self._project = project
        self._cluster = cluster
        self._service = service
        self._metrics = dict()

    def add_metrics(self, metrics_json):
        count = 0
        common_labels = list(metrics_json.get("commonLabels", dict()).items())
        for m in metrics_json.get("metrics") or metrics_json.get("sensors"):
            labels = list(m.get("labels", dict()).items())
            name_label = m.get("name", None)
            if name_label:
                labels = labels + list({"name": name_label}.items())
            all_labels = sorted(labels + common_labels)
            if "timeseries" in m:
                for pt in m["timeseries"]:
                    self._metrics[(pt["ts"], tuple(all_labels))] = pt["value"]
                    count = count + 1
            else:
                self._metrics[(m["ts"], tuple(all_labels))] = m["value"]
                count = count + 1
        return count

    def as_text(self):
        m = list()
        for k, v in self._metrics.items():
            m.append({"ts": k[0], "labels": k[1], "value": v})
        return json.dumps(m, sort_keys=True, indent=2)
