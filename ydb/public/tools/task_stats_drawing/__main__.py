import argparse
import json
import matplotlib.pyplot as plt
import pandas as pd


def take_sort_key(t):
    return t.finish


def take_node_sort_key(n):
    return n.max_task_finished()


class PlanNode:
    plan_node_idx = 0

    def __init__(self, name):
        self.nodes = {}
        self.nodes_list = []
        self.name = name
        self.sub_nodes = []

    def build_sub_node(self, name):
        node = PlanNode(name)
        self.sub_nodes.append(node)
        return node

    def prepare(self):
        for i in self.sub_nodes:
            i.prepare()
        for n_id, n in self.nodes.items():
            n.tasks.sort(key=take_sort_key)
        self.nodes_list.sort(key=take_node_sort_key)

    def get_plans_with_tasks(self):
        result = []
        if (len(self.nodes) > 0):
            result.append(self)
        for i in self.sub_nodes:
            sub_result = i.get_plans_with_tasks()
            for t in sub_result:
                result.append(t)
        return result

    def min_task_start(self):
        start = 0
        first = True
        for id, n in self.nodes.items():
            for t in n.tasks:
                if (first or start > t.start):
                    start = t.start
                    first = False

        for i in self.sub_nodes:
            sub_start = i.min_task_start()
            if (sub_start == 0):
                continue
            if (first or sub_start < start):
                start = sub_start
                first = False
        return start


class Node:
    node_id = 0
    shards_count = 0

    def __init__(self, node_id, host):
        self.tasks = []
        self.node_id = node_id
        self.host = host
        self.finished = 0

    def max_task_finished(self):
        if (self.finished == 0):
            for t in self.tasks:
                if (self.finished < t.finish):
                    self.finished = t.finish
        return self.finished


class Task:
    task_id = ''
    node_id = 0
    full_executor_id = ''
    executor_id = ''
    executor = ''
    start = 0
    finish = 0
    pending = 0
    compute = 0
    waiting = 0
    stage = 0
    host = ''

    def parse(self, t, stage):
        self.task_id = t["TaskId"]
        self.node_id = t["NodeId"]
        self.host = t["Host"]
        self.executor_id = t["Host"].split('.')[0] + "::" + str(t["NodeId"])
        self.full_executor_id = str(stage) + "::" + t["Host"].split('.')[0] + \
            "::" + str(t["NodeId"])
        self.executor = self.executor_id + "::" + str(t["TaskId"])
        self.start = t["StartTimeMs"]
        self.finish = t["FinishTimeMs"]
        self.pending = t["PendingInputTimeUs"]/1000
        self.compute = t["ComputeTimeUs"]/1000
        if ("WaitTimeUs" in t):
            self.waiting = t["WaitTimeUs"]/1000
        else:
            self.waiting = 0
        self.stage = stage


def rgb_to_hex(rgb):
    return '#%02x%02x%02x' % rgb


def scan_json(plan_node, json, current_stack):
    current_stack.append(json["Node Type"])
    plan_node.name = "::".join(current_stack)
    if ("Plans" in json):
        for i in json["Plans"]:
            sub_stage = plan_node.build_sub_node("")
            scan_json(sub_stage, i, current_stack)
    if ("Stats" in json):
        if ("ComputeNodes" in json["Stats"]):
            for n in json["Stats"]["ComputeNodes"]:
                if ("Tasks" not in n):
                    continue
                json_tasks = n["Tasks"]
                for j_task in json_tasks:
                    task = Task()
                    task.parse(j_task, plan_node.plan_node_idx)
                    if (task.node_id not in plan_node.nodes):
                        node = Node(task.node_id, task.host)
                        plan_node.nodes[task.node_id] = node
                        plan_node.nodes_list.append(node)
                    plan_node.nodes[task.node_id].tasks.append(task)
        if ("NodesScanShards" in json["Stats"] and
                not json["Stats"]["NodesScanShards"] is None):
            for n in json["Stats"]["NodesScanShards"]:
                if (n["node_id"] in plan_node.nodes):
                    plan_node.nodes[n["node_id"]].shards_count = \
                        n["shards_count"]
    current_stack = current_stack.pop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='arguments for profiler')
    parser.add_argument('stats_path', type=str, help='path for profile')
    args = parser.parse_args()

    stats_path = args.stats_path
    json_started = False
    json_string = ""
    with open(stats_path, "r", encoding="utf-8") as f:
        json_string += f.readline()
    json_profile = json.loads(json_string)

    plan_node = PlanNode("")
    scan_json(plan_node, json_profile["Plans"][0], [])
    plan_node.prepare()
    start = plan_node.min_task_start()
    plans = plan_node.get_plans_with_tasks()

    tasks = {'executor_id': [], 'y': [], 'start': [], 'finish': [],
             'pending': [], 'compute': [], 'waiting': [], 'task_id': []}

    tasks_count = 0
    max_time = 0
    y_shift = 0
    legend_y = {'pos': [], 'label': []}
    tasks_count_by_node = {}
    s_pred_name = ""
    for s in plans:
        stage = s.name
        for n in s.nodes_list:
            legend_y['pos'].append(y_shift - 0.5 * len(n.tasks))
            label = ""
            if (s.name != s_pred_name):
                label = s.name + "\n"
            label = label + n.host + "\n" + str(n.node_id) + \
                "/t=" + str(len(n.tasks))
            if (n.shards_count):
                label = label + "/sh=" + str(n.shards_count)
            legend_y['label'].append(label)
            count_in_executor = 1
            for t in n.tasks:
                tasks['task_id'].append(t.task_id)
                executor_id = t.executor_id
                if (not (stage in tasks_count_by_node)):
                    tasks_count_by_node[stage] = {}
                tasks_count_by_stage = tasks_count_by_node[stage]
                if (executor_id not in tasks_count_by_stage):
                    tasks_count_by_node[stage][executor_id] = 0
                tasks_count_by_node[stage][executor_id] = \
                    tasks_count_by_node[stage][executor_id] + 1

                y_shift = y_shift - 1

                tasks['executor_id'].append(t.executor_id)
                tasks['start'].append(t.start - start)
                tasks['finish'].append(t.finish - start)
                tasks['pending'].append(t.pending)
                tasks['compute'].append(t.compute)
                tasks['waiting'].append(t.waiting)
                tasks['y'].append(y_shift)
                tasks_count = tasks_count + 1
                max_time = max(max_time, t.finish - start)
            y_shift -= 5
            s_pred_name = s.name
        y_shift -= 5

    df = pd.DataFrame(tasks)
    print(tasks_count_by_node)
    print(df)

    fig, ax = plt.subplots(1, figsize=(20, 6))
    ax.set_yticks(legend_y['pos'])
    ax.set_yticklabels(legend_y['label'])
    ax.barh(y=df.y, left=df.start, width=df.finish-df.start,
            color="#0e0c0c", alpha=1, label="other")
    ax.barh(y=df.y, left=df.start, width=df.waiting,
            color="#001eff", alpha=1, label="waiting")
    ax.barh(y=df.y, left=df.start+df.waiting, width=df.compute,
            color="#ff0000", alpha=1, label="compute")
    ax.legend()

    plt.savefig('image.png', bbox_inches='tight')
