# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import warnings
import pytest

from pycrfsuite import Trainer


def test_trainer(tmpdir, xseq, yseq):
    trainer = Trainer('lbfgs')
    trainer.append(xseq, yseq)

    model_filename = str(tmpdir.join('model.crfsuite'))
    assert not os.path.isfile(model_filename)
    trainer.train(model_filename)
    assert os.path.isfile(model_filename)


def test_trainer_noselect(tmpdir, xseq, yseq):
    # This shouldn't segfault; see https://github.com/chokkan/crfsuite/pull/21
    trainer = Trainer()
    trainer.append(xseq, yseq)
    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)


def test_trainer_noappend(tmpdir):
    # This shouldn't segfault; see https://github.com/chokkan/crfsuite/pull/21
    trainer = Trainer()
    trainer.select('lbfgs')
    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)


def test_trainer_noselect_noappend(tmpdir):
    # This shouldn't segfault; see https://github.com/chokkan/crfsuite/pull/21
    trainer = Trainer()
    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)


def test_training_messages(tmpdir, xseq, yseq):

    class CapturingTrainer(Trainer):
        def __init__(self):
            self.messages = []

        def message(self, message):
            self.messages.append(message)

    trainer = CapturingTrainer()
    trainer.select('lbfgs')
    trainer.append(xseq, yseq)
    assert not trainer.messages

    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)
    assert trainer.messages
    assert 'type: CRF1d\n' in trainer.messages
    # print("".join(trainer.messages))


def test_training_messages_exception(tmpdir, xseq, yseq):

    class MyException(Exception):
        pass

    class BadTrainer(Trainer):
        def message(self, message):
            raise MyException("error")

    trainer = BadTrainer()
    trainer.select('lbfgs')
    trainer.append(xseq, yseq)

    model_filename = str(tmpdir.join('model.crfsuite'))

    with pytest.raises(MyException):
        trainer.train(model_filename)


def test_trainer_select_raises_error():
    trainer = Trainer()
    with pytest.raises(ValueError):
        trainer.select('foo')


@pytest.mark.parametrize("algo", [
    'lbfgs',
    'l2sgd',
    'ap',
    'averaged-perceptron',
    'pa',
    'passive-aggressive',
    'arow',
])
def test_algorithm_parameters(algo):
    trainer = Trainer(algo)
    params = trainer.get_params()
    assert params

    # set the same values
    trainer.set_params(params)
    params2 = trainer.get_params()
    assert params2 == params

    # change a value
    trainer.set('feature.possible_states', True)
    assert trainer.get_params()['feature.possible_states'] == True

    trainer.set('feature.possible_states', False)
    assert trainer.get_params()['feature.possible_states'] == False

    # invalid parameter
    params['foo'] = 5
    with pytest.raises(ValueError):
        trainer.set_params(params)


def test_params_and_help():
    trainer = Trainer()

    trainer.select('lbfgs')
    assert 'c1' in trainer.params()
    assert 'c2' in trainer.params()
    assert 'num_memories' in trainer.params()
    assert 'L1' in trainer.help('c1')

    trainer.select('l2sgd')
    assert 'c2' in trainer.params()
    assert 'c1' not in trainer.params()
    assert 'L2' in trainer.help('c2')


def test_help_invalid_parameter():
    trainer = Trainer()
    trainer.select('l2sgd')

    # This segfaults without a workaround;
    # see https://github.com/chokkan/crfsuite/pull/21
    with pytest.raises(ValueError):
        trainer.help('foo')

    with pytest.raises(ValueError):
        trainer.help('c1')


def test_get_parameter():
    trainer = Trainer()
    trainer.select('l2sgd')
    assert abs(trainer.get('c2') - 0.1) > 1e-6
    trainer.set('c2', 0.1)
    assert abs(trainer.get('c2') - 0.1) < 1e-6


def test_set_parameters_in_constructor():
    trainer = Trainer(params={'c2': 100})
    assert abs(trainer.get('c2') - 100) < 1e-6

