from collections import defaultdict
import enum
from typing import List, Tuple, Dict, Optional, Type

import crowdkit.aggregation as aggregation
import pandas as pd
import toloka.client as toloka

from .. import base, mapping, objects, Worker, Human


class AggregationAlgorithm(enum.Enum):
    MAJORITY_VOTE = 'majority-vote'
    MAX_LIKELIHOOD = 'max-likelihood'
    DAWID_SKENE = 'dawid-skene'


WorkerLabel = Tuple[base.Label, Worker]
TaskLabels = List[WorkerLabel]
LabelProba = Tuple[base.Label, float]
TaskLabelsProbas = Dict[base.Label, float]
WorkerWeights = Dict[str, float]
Results = List[Tuple[Optional[TaskLabelsProbas], List[WorkerLabel]]]

aggregator_map = {
    AggregationAlgorithm.MAJORITY_VOTE: aggregation.MajorityVote(),
    AggregationAlgorithm.DAWID_SKENE: aggregation.DawidSkene(n_iter=10),
}


def split_generated_answer(combined_type: Type[objects.CombinedAnswer]) -> List[List[objects.CombinedAnswer]]:
    answer_groups = defaultdict(list)
    for instance in combined_type.possible_instances():
        answer_groups[type(instance.get_original_answer())].append(instance)

    return list(answer_groups.values())


def predict_labels_probas(
    labels: List[TaskLabels],
    aggregation_algorithm: AggregationAlgorithm,
    task_mapping: mapping.TaskMapping,
    worker_weights: Optional[WorkerWeights] = None,
) -> List[Optional[TaskLabelsProbas]]:
    label_cls = task_mapping.output_mapping[0].obj_meta.type
    assert issubclass(label_cls, base.Label)
    if not issubclass(label_cls, objects.CombinedAnswer):
        classes = label_cls.possible_instances()
        extra_data = None if worker_weights is None else (worker_weights, classes)
        return predict_labels_probas_for_class(labels, aggregation_algorithm, extra_data)

    results = [None for _ in labels]

    for classes in split_generated_answer(label_cls):
        indices, current_labels = [], []
        for i, task_labels in enumerate(labels):
            if not task_labels:
                continue
            matches = sum(label in classes for label, _ in task_labels)
            assert matches == 0 or matches == len(task_labels), 'Found mixed-class output labels'

            if matches:
                current_labels.append(task_labels)
                indices.append(i)

        extra_data = None if worker_weights is None else (worker_weights, classes)
        label_probas = predict_labels_probas_for_class(current_labels, aggregation_algorithm, extra_data)
        for i, probas in zip(indices, label_probas):
            results[i] = probas

    return results


def predict_labels_probas_for_class(
    labels: List[TaskLabels],
    aggregation_algorithm: AggregationAlgorithm,
    worker_weights_and_classes: Optional[Tuple[WorkerWeights, List[base.Label]]] = None,
) -> List[Optional[TaskLabelsProbas]]:
    if not labels:
        return []
    if aggregation_algorithm in aggregator_map:
        crowdkit_labels = []
        for task_idx, task_labels in enumerate(labels):
            for label, worker in task_labels:
                crowdkit_labels.append({'label': label, 'task': task_idx, 'worker': worker.id})
        aggregator = aggregator_map[aggregation_algorithm]
        results = [None for _ in labels]

        if not crowdkit_labels:
            return results
        crowdkit_label_probas = aggregator.fit_predict_proba(pd.DataFrame(crowdkit_labels))
        for task_idx, probas in zip(crowdkit_label_probas.index, crowdkit_label_probas.values):
            label_probas = {}
            for label_idx, proba in enumerate(probas):
                label = crowdkit_label_probas.columns[label_idx]
                label_probas[label] = proba
            results[task_idx] = label_probas
        return results

    assert aggregation_algorithm == AggregationAlgorithm.MAX_LIKELIHOOD
    assert worker_weights_and_classes is not None
    worker_weights, classes = worker_weights_and_classes
    num_classes = len(classes)

    def bayes_proba(answer: base.Label, answers: TaskLabels) -> float:
        result = 1.0
        for other_answer, w in answers:
            q = worker_weights[w.id]
            if other_answer == answer:
                result *= q
            else:
                result *= (1 - q) / (num_classes - 1)
        return result

    weighted_results = []
    for task_labels in labels:
        if not task_labels:
            weighted_results.append(None)
            continue
        calculated_z_proba = {label: bayes_proba(label, task_labels) * (1 / num_classes) for label in classes}
        normalization = sum(calculated_z_proba.values())
        weighted_results.append({label: value / normalization for label, value in calculated_z_proba.items()})

    return weighted_results


def get_most_probable_label(label_probas: Optional[TaskLabelsProbas]) -> Optional[LabelProba]:
    if label_probas is None:
        return None
    return max(label_probas.items(), key=lambda label_proba: label_proba[1])


def collect_labels_probas_from_assignments(
    assignments: List[toloka.Assignment],
    task_mapping: mapping.TaskMapping,
    pool_input_objects: List[mapping.Objects],
    aggregation_algorithm: AggregationAlgorithm,
    worker_weights: Optional[WorkerWeights] = None,
) -> List[Tuple[Optional[TaskLabelsProbas], List[WorkerLabel]]]:
    raw_labels = []
    for _, output_objects_list in mapping.get_solutions(assignments, task_mapping, pool_input_objects):
        task_labels = []
        for output_objects, assignment in output_objects_list:
            # for now label is always the first output object; composite labels not supported yet
            label = output_objects[0]
            assert isinstance(label, base.Label)
            worker = Human(assignment)  # TODO(DATAFORGE-75): can be model
            task_labels.append((label, worker))
        raw_labels.append(task_labels)

    return list(zip(predict_labels_probas(raw_labels, aggregation_algorithm, task_mapping, worker_weights), raw_labels))
