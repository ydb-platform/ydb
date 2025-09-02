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
    DEPRECATED_TESTS_PROJECTS = ["my_project", "hist", "invalid"]

    class Metric(object):
        def __init__(self, labels, kind="DGAUGE", timestamps=[], values=[]):
            self.labels = labels
            self.kind = kind
            self.data = []
            for i in range(len(timestamps)):
                self.data.append((int(timestamps[i] * 1000), values[i]))

        def match(self, selectors):
            for key, value in selectors.items():
                if key == "project" or key == "cluster" or key == "service":
                    continue

                if value == "-":
                    if key in self.labels:
                        return False
                elif value == "*":
                    return key in self.labels
                else:
                    if key not in self.labels or self.labels[key] != value:
                        return False

            return True

        def add_point(self, ts, value):
            self.data.append((ts, value))

    def __init__(self, project, cluster, service):
        self._project = project
        self._cluster = cluster
        self._service = service
        self._metrics = {}

    def get_or_create(self, labels, type):
        key = (str(sorted(list(labels.items()))), type)

        if key not in self._metrics:
            new_metric = self.Metric(labels, type)
            self._metrics[key] = new_metric
            return new_metric

        return self._metrics[key]

    def get_matching_metrics(self, selectors):
        result = []

        for key, metric in self._metrics.items():
            if metric.match(selectors):
                result.append(metric)

        return result

    def add_metrics(self, metrics_json):
        count = 0
        common_labels = metrics_json.get("commonLabels", dict())
        for m in metrics_json.get("metrics") or metrics_json.get("sensors"):
            labels = m.get("labels", dict())
            type = m.get("type")
            name_label = m.get("name", None)
            if name_label:
                labels["name"] = name_label
            all_labels = labels | common_labels
            if "timeseries" in m:
                for pt in m["timeseries"]:
                    self.get_or_create(all_labels, type).add_point(pt["ts"], pt["value"])
                    count = count + 1
            else:
                self.get_or_create(all_labels, type).add_point(m["ts"], m["value"])
                count = count + 1
        return count

    def add_parsed_metrics(self, metrics_json):
        for metric in metrics_json["metrics"]:
            labels = metric["labels"]
            type = metric["type"]

            key = (str(sorted(list(labels.items()))), type)
            self._metrics[key] = self.Metric(labels, type, metric["timestamps"], metric["values"])

    def get_label_names(self, selectors):
        result = set()

        matching_metrics = self.get_matching_metrics(selectors)
        for metric in matching_metrics:
            for key, value in metric.labels.items():
                if (key not in selectors):
                    result.add(key)

        return list(result)

    def get_metrics(self, selectors):
        result = []

        matching_metrics = self.get_matching_metrics(selectors)
        for metric in matching_metrics:
            labels = metric.labels
            labels["project"] = self._project
            labels["cluster"] = self._cluster
            labels["service"] = self._service
            result.append({"labels": labels, "type": metric.kind})

        return sorted(result, key=lambda x: str(x))

    def get_data(self, selectors, time_from, time_to, downsampling):
        result = dict()

        nanoseconds_from = time_from.seconds * 1000 + int(time_from.nanos) // int(1e6)
        nanoseconds_to = time_to.seconds * 1000 + int(time_to.nanos) // int(1e6)

        if downsampling.HasField("disabled"):
            downsampling_disabled = True
            downsampling_grid_interval = 1
        else:
            downsampling_disabled = False
            downsampling_grid_interval = int(downsampling.grid_interval)

        matching_metrics = self.get_matching_metrics(selectors)
        if len(matching_metrics) != 1:
            return (result, "Invalid amount of metrics matching selectors, should be 1, have: {}".format(len(matching_metrics)))

        metric = matching_metrics[0]

        labels = metric.labels
        labels["project"] = self._project
        labels["cluster"] = self._cluster
        labels["service"] = self._service

        timestamps = []
        values = []

        for ts, value in metric.data:
            if isinstance(ts, str) or (ts >= nanoseconds_from and ts <= nanoseconds_to):
                if downsampling_disabled or ts % downsampling_grid_interval == 0:
                    timestamps.append(ts)
                    values.append(value)

        result["labels"] = labels
        result["type"] = metric.kind
        result["timestamps"] = timestamps
        result["values"] = values

        return (result, "")

    def as_text(self):
        m = list()
        for key, metric in self._metrics.items():
            for ts, value in metric.data:
                m.append({"ts": ts, "labels": sorted(list(metric.labels.items())), "value": value})
        return json.dumps(m, sort_keys=True, indent=2)
