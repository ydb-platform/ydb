# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
r"""Checkpoint converter for Canned Estimators in TF 1.x.

This checkpoint converter tool is mainly for Canned Estimators, including DNN
Linear and DNNLinearCombined estimators. The allowed optimizers to be converted
include Adam, Adagrad, Ftrl, RMSProp, and SGD.

Note that, this converter is not suitable for the case where 'dnn_optimizer'
and 'linear_optimizer' in DNNLinearCombined model are the same.

If your current canned estimators and checkpoints are from TF 1.x, after you
migrate the canned estimator to v2 with `tf.keras.optimizers.*`, the converted
checkpoint allow you to restore and retrain the model in TF 2.0.

Usage:
  python checkpoint_convert.py '/path/to/checkpoint' '/path/to/graph.pbtxt' \
      '/path/to/new_checkpoint'

For example, if there is a V1 checkpoint to be converted and the files include:
  /tmp/my_checkpoint/model.ckpt-100.data-00000-of-00001
  /tmp/my_checkpoint/model.ckpt-100.index
  /tmp/my_checkpoint/model.ckpt-100.meta
  /tmp/my_checkpoint/graph.pbtxt

use the following command:
  mkdir /tmp/my_converted_checkpoint &&
  python checkpoint_convert.py \
      /tmp/my_checkpoint/model.ckpt-100 /tmp/my_checkpoint/graph.pbtxt \
      /tmp/my_converted_checkpoint/model.ckpt-100

This will generate three converted checkpoint files corresponding to the three
old checkpoint files in the new directory:
  /tmp/my_converted_checkpoint/model.ckpt-100.data-00000-of-00001
  /tmp/my_converted_checkpoint/model.ckpt-100.index
  /tmp/my_converted_checkpoint/model.ckpt-100.meta
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys
import tensorflow as tf
from google.protobuf import text_format
from tensorflow.core.framework import graph_pb2
from tensorflow.python.keras.optimizer_v2 import adagrad
from tensorflow.python.keras.optimizer_v2 import adam
from tensorflow.python.keras.optimizer_v2 import ftrl
from tensorflow.python.keras.optimizer_v2 import gradient_descent
from tensorflow.python.keras.optimizer_v2 import rmsprop

# Optimizer name mapping from v1 to v2.
OPT_NAME_V1_TO_V2 = {
    'Adagrad': 'Adagrad',
    'RMSProp': 'RMSprop',
    'Ftrl': 'Ftrl',
    'Adam': 'Adam',
    'SGD': 'SGD',
}

# Hyper-paratmeters of optimizer in checkpoint.
HP_IN_CKPT = {
    'Adam': {
        'beta1_power': 'training/Adam/beta_1',
        'beta2_power': 'training/Adam/beta_2',
    },
}

# Optimzier variable name mapping from v1 to v2.
OPT_VAR_NAME_V1_TO_V2 = {
    'Adam': {
        'Adam': 'm',
        'Adam_1': 'v',
    },
    'Ftrl': {
        'Ftrl': 'accumulator',
        'Ftrl_1': 'linear',
    },
    'RMSProp': {
        'RMSProp': 'rms',
        'RMSProp_1': None,
    },
    'Adagrad': {
        'Adagrad': 'accumulator',
    },
}

# Hyper-paratmeters of optimizer in graph.
HP_IN_GRAPH = {
    'Adam': ['decay', 'learning_rate'],
    'Ftrl': [
        'decay', 'l1_regularization_strength', 'l2_regularization_strength',
        'beta', 'learning_rate', 'learning_rate_power'
    ],
    'RMSProp': ['decay', 'learning_rate', 'momentum', 'rho'],
    'Adagrad': ['decay', 'learning_rate'],
    'SGD': ['decay', 'learning_rate', 'momentum'],
}

# optimizer v2 instance.
OPT_V2_INSTANCE = {
    'Adagrad': adagrad.Adagrad(),
    'Adam': adam.Adam(),
    'Ftrl': ftrl.Ftrl(),
    'RMSProp': rmsprop.RMSprop(),
    'SGD': gradient_descent.SGD(),
}


def _add_new_variable(initial_value, var_name_v2, var_name_v1, var_map,
                      var_names_map):
  """Creates a new variable and add it to the variable maps."""
  var = tf.Variable(initial_value, name=var_name_v2)
  var_map[var_name_v2] = var
  var_names_map[var_name_v2] = var_name_v1


def _add_opt_variable(opt_name_v2, var_name_v1, idx, suffix_v2, reader, var_map,
                      var_names_map):
  """Adds a new optimizer v2 variable."""
  var_name_v2 = 'training/' + opt_name_v2 + '/' + var_name_v1[:idx] + suffix_v2
  tensor = reader.get_tensor(var_name_v1)
  _add_new_variable(tensor, var_name_v2, var_name_v1, var_map, var_names_map)


def _convert_variables_in_ckpt(opt_name_v1, reader, variable_names, var_map,
                               var_names_map, est_type):
  """Converts all variables in checkpoint from v1 to v2."""
  global_step = None
  hp_ckpt = None
  # Global step is needed for Adam for hyper parameter conversion.
  if opt_name_v1 == 'Adam':
    global_step = reader.get_tensor('global_step')
  if opt_name_v1 in HP_IN_CKPT:
    hp_ckpt = HP_IN_CKPT[opt_name_v1]
  opt_name_v2 = OPT_NAME_V1_TO_V2[opt_name_v1]

  # For variables with equivalent mapping in checkpoint. There are three types:
  # 1) Hyper parameters. This is mainly for Adam optimizer.
  # 2) Optimizer variables.
  # 3) Model variables.
  for var_name in variable_names:
    # If a hyper parameter variable is in the checkpoint.
    if hp_ckpt and any(hp_name in var_name for hp_name in hp_ckpt):
      for hp_name in hp_ckpt:
        if hp_name in var_name:
          var_name_v2 = hp_ckpt[hp_name]
          tensor = reader.get_tensor(var_name)
          # For Adam optimizer, in the old checkpoint, the optimizer variables
          # are beta1_power and beta2_power. The corresponding variables in the
          # new checkpoint are beta_1 and beta_2, and
          # beta_1 = pow(beta1_power, 1/global_step)
          # beta_2 = pow(beta2_power, 1/global_step)
          tensor = tf.math.pow(tensor, 1.0 / global_step)
          _add_new_variable(tensor, var_name_v2, var_name, var_map,
                            var_names_map)
          break
    # If it's an optimizer variable.
    elif opt_name_v1 in var_name:
      suffix_mapping = OPT_VAR_NAME_V1_TO_V2[opt_name_v1]
      suffix_v1 = var_name.rsplit('/')[-1]
      suffix_v2 = suffix_mapping[suffix_v1]
      if suffix_v2:
        # For DNN model.
        if est_type == 'dnn':
          # The optimizer variable of DNN model in TF 1.x has 't_0' in its
          # name (b/131719899). This is amended in TF 2.0.
          idx = var_name.rfind('t_0')
          _add_opt_variable(opt_name_v2, var_name, idx, suffix_v2, reader,
                            var_map, var_names_map)
        # for Linear model.
        elif est_type == 'linear':
          # The optimizer variable of Linear model in TF 1.x has 'part_0' in its
          # name (b/131719899). This is amended in TF 2.0.
          idx = var_name.rfind('part_0')
          _add_opt_variable(opt_name_v2, var_name, idx, suffix_v2, reader,
                            var_map, var_names_map)
        # for DNNLinearCombined model.
        else:
          idx = var_name.rfind(suffix_v1)
          _add_opt_variable(opt_name_v2, var_name, idx, suffix_v2, reader,
                            var_map, var_names_map)
    # If it's a model variable which is already backward compatible.
    else:
      tensor = reader.get_tensor(var_name)
      _add_new_variable(tensor, var_name, var_name, var_map, var_names_map)


def _convert_hyper_params_in_graph(graph_from_path, opt_name_v1, var_map,
                                   var_names_map):
  """Generates hyper parameters for optimizer v2 from graph.pbtxt."""
  with tf.io.gfile.GFile(graph_from_path) as f:
    graph_def = text_format.Parse(f.read(), graph_pb2.GraphDef())

  # In keras optimizer, the hyper parameters are also stored in the checkpoint,
  # while v1 checkpoint doesn't contain any hyper parameters. For the
  # hyper parameter variables, there are two cases:
  # 1) The hyper parameter exist in the graph.
  #    If so, the hyper parameter value needs to be extracted from the graph
  #    node.
  # 2) The hyper parameter doesn't exist in the graph.
  #    The value of the hyper parameter is set as the default value from the
  #    config.
  nodes_full = HP_IN_GRAPH[opt_name_v1]
  nodes_in_graph = []
  opt_name_v2 = OPT_NAME_V1_TO_V2[opt_name_v1]
  tf.compat.v1.logging.info('For hyper parameter variables that are in Graph:')
  for node in graph_def.node:
    node_name = node.name.rsplit('/')[-1]
    # For case 1), if the hyper parameter of the keras optimizer can be found
    # in the graph, the graph node value is extracted as the hyper parameter
    # variable value, and added to the new variable list.
    if opt_name_v1 + '/' + node_name in nodes_full:
      hp_value = node.attr.get('value').tensor.float_val[0]
      hp_name_v2 = 'training/' + opt_name_v2 + '/' + node_name
      tf.compat.v1.logging.info(
          'Hyper parameter {} with value {} found in Graph.'.format(
              hp_name_v2, hp_value))
      _add_new_variable(hp_value, hp_name_v2, node_name, var_map, var_names_map)
      # Adds this node to nodes_in_graph
      nodes_in_graph.append(node_name)

  # For case 2), if the hyper parameter is not in graph, we need to add it
  # manually. The tensor value is its default value from optimizer v2 config.
  nodes_not_in_graph = sorted(list(set(nodes_full) - set(nodes_in_graph)))
  opt_v2_config = OPT_V2_INSTANCE[opt_name_v1].get_config()
  tf.compat.v1.logging.info(
      'For hyper parameter variables that are NOT in Graph:')
  for node_name in nodes_not_in_graph:
    hp_name_v2 = 'training/' + opt_name_v2 + '/' + node_name
    tf.compat.v1.logging.info(
        'Hyper parameter {} with default value {} is added.'.format(
            hp_name_v2, opt_v2_config[node_name]))
    _add_new_variable(opt_v2_config[node_name], hp_name_v2, node_name, var_map,
                      var_names_map)


def convert_checkpoint(estimator_type, source_checkpoint, source_graph,
                       target_checkpoint):
  """Converts checkpoint from TF 1.x to TF 2.0 for CannedEstimator.

  Args:
    estimator_type: The type of estimator to be converted. So far, the allowed
      args include 'dnn', 'linear', and 'combined'.
    source_checkpoint: Path to the source checkpoint file to be read in.
    source_graph: Path to the source graph file to be read in.
    target_checkpoint: Path to the target checkpoint to be written out.
  """
  with tf.Graph().as_default():
    # Get v1 optimizer names and it's corresponding variable name
    reader = tf.compat.v1.train.NewCheckpointReader(source_checkpoint)
    variable_names = sorted(reader.get_variable_to_shape_map())
    opt_names_v1 = {}
    for var_name in variable_names:
      for opt_name in OPT_NAME_V1_TO_V2:
        if opt_name in var_name:
          opt_names_v1[opt_name] = var_name

    # SGD doesn't appear in optimizer variables, so we need to add it manually
    # if no optimizer is found in checkpoint for DNN or Linear model.
    if not opt_names_v1:
      if estimator_type == 'dnn' or estimator_type == 'linear':
        opt_names_v1['SGD'] = ''
      # As the case is not handled in the converter if dnn_optimizer and
      # linear_optimizer in DNNLinearCombined model are the same, an error is
      # is raised if two SGD optimizers are used in DNNLinearCombined model.
      elif estimator_type == 'combined':
        raise ValueError('Two `SGD` optimizers are used in DNNLinearCombined '
                         'model, and this is not handled by the checkpoint '
                         'converter.')

    # A dict mapping from v2 variable name to the v2 variable.
    var_map = {}
    # A dict mapping from v2 variable name to v1 variable name.
    var_names_map = {}

    # Determine the names of dnn_optimizer and linear_optimizer in
    # DNNLinearCombined model.
    if estimator_type == 'combined':
      linear_opt_v1 = None
      if len(opt_names_v1) == 1:  # When one of the optimizer is 'SGD'.
        key = list(opt_names_v1.keys())[0]
        # Case 1: linear_optimizer is non-SGD, and dnn_optimizer is SGD.
        if opt_names_v1[key].startswith('linear/linear_model/'):
          linear_opt_v1 = key
        # Case 2: linear_optimizer is SGD, and dnn_optimizer is non-SGD.
        if not linear_opt_v1:
          linear_opt_v1 = 'SGD'
        opt_names_v1['SGD'] = ''
      else:  # two non-SGD optimizers
        for key in opt_names_v1:
          if opt_names_v1[key].startswith('linear/linear_model/'):
            linear_opt_v1 = key
      # Add the 'iter' hyper parameter to the new checkpoint for
      # linear_optimizer. Note dnn_optimizer uses global_step.
      tensor = reader.get_tensor('global_step')
      var_name_v2 = 'training/' + OPT_NAME_V1_TO_V2[linear_opt_v1] + '/iter'
      var_name_v1 = 'global_step'
      _add_new_variable(tensor, var_name_v2, var_name_v1, var_map,
                        var_names_map)

    for opt_name_v1 in opt_names_v1:
      # Convert all existing variables from checkpoint.
      _convert_variables_in_ckpt(opt_name_v1, reader, variable_names, var_map,
                                 var_names_map, estimator_type)
      # Convert hyper parameters for optimizer v2 from the graph.
      _convert_hyper_params_in_graph(source_graph, opt_name_v1, var_map,
                                     var_names_map)

    # Log the variable mapping from opt v1 to v2.
    tf.compat.v1.logging.info(
        '<----- Variable names converted (v1 --> v2): ----->')
    for name_v2 in var_names_map:
      tf.compat.v1.logging.info('%s --> %s' % (var_names_map[name_v2], name_v2))

    # Save to checkpoint v2.
    saver = tf.compat.v1.train.Saver(var_list=var_map)
    with tf.compat.v1.Session() as sess:
      sess.run(tf.compat.v1.initializers.global_variables())
      tf.compat.v1.logging.info('Writing checkpoint_to_path %s' %
                                target_checkpoint)
      saver.save(sess, target_checkpoint)


def main(_):
  convert_checkpoint(
      FLAGS.estimator_type,
      FLAGS.source_checkpoint,
      FLAGS.source_graph,
      FLAGS.target_checkpoint,
  )


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      'estimator_type',
      type=str,
      choices=['dnn', 'linear', 'combined'],
      help='The type of estimator to be converted. So far, the checkpoint '
      'converter only supports Canned Estimator. So the allowed types '
      'include linear, dnn and combined.')
  parser.add_argument(
      'source_checkpoint',
      type=str,
      help='Path to source checkpoint file to be read in.')
  parser.add_argument(
      'source_graph', type=str, help='Path to source graph file to be read in.')
  parser.add_argument(
      'target_checkpoint',
      type=str,
      help='Path to checkpoint file to be written out.')
  FLAGS, unparsed = parser.parse_known_args()
  tf.compat.v1.app.run(main=main, argv=[sys.argv[0]] + unparsed)
