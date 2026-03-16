# -*- coding: utf-8 -*-
from __future__ import absolute_import
import re
import fractions
from collections import namedtuple

LabelScore = namedtuple('LabelScore', 'match model ref precision recall f1')


class TrainLogParser(object):

    def __init__(self):
        self.state = None
        self.featgen_percent = -2
        self.featgen_num_features = None
        self.featgen_seconds = None
        self.training_seconds = None
        self.storing_seconds = None

        self.iterations = []
        self.last_iteration = None
        self.log = []
        self.events = []

    def feed(self, line):
        # if line != '\n':
        self.log.append(line)
        if self.state is None:
            self.state = 'STARTING'
            self.handle_STARTING(line)
            self.events.append(('start', 0, len(self.log)))
            return 'start'
        event = getattr(self, "handle_" + self.state)(line)
        if event is not None:
            start, end = self.events[-1][2], len(self.log)
            if event in ('prepared', 'optimization_end'):
                end -= 1
            self.events.append((event, start, end))
        return event

    @property
    def last_log(self):
        event, start, end = self.events[-1]
        return ''.join(self.log[start:end])

    def handle_STARTING(self, line):
        if line.startswith('Feature generation'):
            self.state = 'FEATGEN'

    def handle_FEATGEN(self, line):
        if line in "0123456789.10":
            self.featgen_percent += 2
            return 'featgen_progress'

        m = re.match(r"Number of features: (\d+)", line)
        if m:
            self.featgen_num_features = int(m.group(1))
            return None

        if self._seconds(line) is not None:
            self.featgen_seconds = self._seconds(line)
            self.state = 'AFTER_FEATGEN'
            return 'featgen_end'

    def handle_AFTER_FEATGEN(self, line):
        if self._iteration_head(line) is not None:
            self.state = 'ITERATION'
            self.handle_ITERATION(line)
            return 'prepared'

        if 'terminated with error' in line:
            self.state = 'AFTER_ITERATION'
            return 'prepare_error'

    def handle_ITERATION(self, line):
        if self._iteration_head(line) is not None:
            self.last_iteration = {
                'num': self._iteration_head(line),
                'scores': {},
            }
            self.iterations.append(self.last_iteration)
        elif line == '\n':
            self.state = 'AFTER_ITERATION'
            return 'iteration'

        def add_re(key, pattern, typ):
            m = re.match(pattern, line)
            if m:
                self.last_iteration[key] = typ(m.group(1))

        add_re("loss", r"Loss: (\d+\.\d+)", float)
        add_re("feature_norm", r"Feature norm: (\d+\.\d+)", float)
        add_re("error_norm", r"Error norm: (\d+\.\d+)", float)
        add_re("active_features", r"Active features: (\d+)", int)
        add_re("linesearch_trials", r"Line search trials: (\d+)", int)
        add_re("linesearch_step", r"Line search step: (\d+\.\d+)", float)
        add_re("time", r"Seconds required for this iteration: (\d+\.\d+)", float)

        m = re.match(r"Macro-average precision, recall, F1: \((\d\.\d+), (\d\.\d+), (\d\.\d+)\)", line)
        if m:
            self.last_iteration['avg_precision'] = float(m.group(1))
            self.last_iteration['avg_recall'] = float(m.group(2))
            self.last_iteration['avg_f1'] = float(m.group(3))

        m = re.match(r"Item accuracy: (\d+) / (\d+)", line)
        if m:
            acc = fractions.Fraction(int(m.group(1)), int(m.group(2)))
            self.last_iteration['item_accuracy'] = acc
            self.last_iteration['item_accuracy_float'] = float(acc)

        m = re.match(r"Instance accuracy: (\d+) / (\d+)", line)
        if m:
            acc = fractions.Fraction(int(m.group(1)), int(m.group(2)))
            self.last_iteration['instance_accuracy'] = acc
            self.last_iteration['instance_accuracy_float'] = float(acc)

        m = re.match(r"\s{4}(.+): \((\d+), (\d+), (\d+)\) \((\d\.\d+), (\d\.\d+), (\d\.\d+)\)", line)
        if m:
            self.last_iteration['scores'][m.group(1)] = LabelScore(**{
                'match': int(m.group(2)),
                'model': int(m.group(3)),
                'ref': int(m.group(4)),
                'precision': float(m.group(5)),
                'recall': float(m.group(6)),
                'f1': float(m.group(7)),
            })

        m = re.match(r"\s{4}(.+): \(0, 0, 0\) \(\*{6}, \*{6}, \*{6}\)", line)
        if m:
            self.last_iteration['scores'][m.group(1)] = LabelScore(**{
                'match': 0,
                'model': 0,
                'ref': 0,
                'precision': None,
                'recall': None,
                'f1': None,
            })

    def handle_AFTER_ITERATION(self, line):
        if self._iteration_head(line) is not None:
            self.state = 'ITERATION'
            return self.handle_ITERATION(line)

        m = re.match(r"Total seconds required for training: (\d+\.\d+)", line)
        if m:
            self.training_seconds = float(m.group(1))

        if line.startswith('Storing the model'):
            self.state = 'STORING'
            return 'optimization_end'

    def handle_STORING(self, line):
        if line == '\n':
            return 'end'
        elif self._seconds(line):
            self.storing_seconds = self._seconds(line)

    def _iteration_head(self, line):
        m = re.match(r'\*{5} (?:Iteration|Epoch) #(\d+) \*{5}\n', line)
        if m:
            return int(m.group(1))

    def _seconds(self, line):
        m = re.match(r'Seconds required: (\d+\.\d+)', line)
        if m:
            return float(m.group(1))
