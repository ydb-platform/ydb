import json
import requests
import six
import library.python.retry as retry


class Sensors:
    def __init__(self, data):
        self.data = data

    def find_sensor(self, labels):
        found = None
        for s in self.data:
            lbls = s["labels"]
            has_all_labels = all(lbls.get(k, None) == v for k, v in six.iteritems(labels))
            if not has_all_labels:
                continue
            new_found = s
            if found is not None:
                raise Exception("Multiple sensors matches for labels {}: {} and {}".format(labels, found, new_found))
            found = new_found

        return found["value"] if found is not None else None

    def find_sensors(self, labels, key_label):
        result = {}
        for s in self.data:
            lbls = s["labels"]
            has_all_labels = all(lbls.get(k, None) == v for k, v in six.iteritems(labels))
            if not has_all_labels:
                continue
            v = lbls.get(key_label, None)
            if v is not None:
                result[v] = s["value"]
        return result

    def collect_non_zeros(self):
        return [s for s in self.data if s.get("value", 0) != 0]

    def __str__(self):
        return str(self.data)


@retry.retry(retry.RetryConf().upto(10))
def load_metrics(url):
    response = requests.get(url)
    response.raise_for_status()
    data = json.loads(response.text)
    sensors = data.get("metrics")
    if sensors is None:
        sensors = data.get("sensors", [])
    return Sensors(sensors)
