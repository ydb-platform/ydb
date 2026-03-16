# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Class that creates an Estimator from a SavedModel."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import six
import tensorflow as tf
from tensorflow.python.saved_model import constants
from tensorflow.python.saved_model import loader_impl
from tensorflow.python.saved_model import signature_constants
from tensorflow.python.saved_model import utils_impl as saved_model_utils
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator as estimator_lib
from tensorflow_estimator.python.estimator import model_fn as model_fn_lib
from tensorflow_estimator.python.estimator.export import export_lib
from tensorflow_estimator.python.estimator.mode_keys import ModeKeys


@estimator_export('estimator.experimental.SavedModelEstimator')
class SavedModelEstimator(estimator_lib.EstimatorV2):
  """Create an Estimator from a SavedModel.

  Only SavedModels exported with
  `tf.estimator.Estimator.experimental_export_all_saved_models()` or
  `tf.estimator.Estimator.export_saved_model()` are supported for this class.

  Example with `tf.estimator.DNNClassifier`:

  **Step 1: Create and train DNNClassifier.**

  ```python
  feature1 = tf.feature_column.embedding_column(
      tf.feature_column.categorical_column_with_vocabulary_list(
          key='feature1', vocabulary_list=('green', 'yellow')), dimension=1)
  feature2 = tf.feature_column.numeric_column(key='feature2', default_value=0.0)

  classifier = tf.estimator.DNNClassifier(
      hidden_units=[4,2], feature_columns=[feature1, feature2])

  def input_fn():
    features = {'feature1': tf.constant(['green', 'green', 'yellow']),
                'feature2': tf.constant([3.5, 4.2, 6.1])}
    label = tf.constant([1., 0., 0.])
    return tf.data.Dataset.from_tensors((features, label)).repeat()

  classifier.train(input_fn=input_fn, steps=10)
  ```

  **Step 2: Export classifier.**
  First, build functions that specify the expected inputs.

  ```python
  # During train and evaluation, both the features and labels should be defined.
  supervised_input_receiver_fn = (
      tf.estimator.experimental.build_raw_supervised_input_receiver_fn(
          {'feature1': tf.placeholder(dtype=tf.string, shape=[None]),
           'feature2': tf.placeholder(dtype=tf.float32, shape=[None])},
          tf.placeholder(dtype=tf.float32, shape=[None])))

  # During predict mode, expect to receive a `tf.Example` proto, so a parsing
  # function is used.
  serving_input_receiver_fn = (
      tf.estimator.export.build_parsing_serving_input_receiver_fn(
          tf.feature_column.make_parse_example_spec([feature1, feature2])))
  ```

  Next, export the model as a SavedModel. A timestamped directory will be
  created (for example `/tmp/export_all/1234567890`).

  ```python
  # Option 1: Save all modes (train, eval, predict)
  export_dir = classifier.experimental_export_all_saved_models(
      '/tmp/export_all',
      {tf.estimator.ModeKeys.TRAIN: supervised_input_receiver_fn,
       tf.estimator.ModeKeys.EVAL: supervised_input_receiver_fn,
       tf.estimator.ModeKeys.PREDICT: serving_input_receiver_fn})

  # Option 2: Only export predict mode
  export_dir = classifier.export_saved_model(
      '/tmp/export_predict', serving_input_receiver_fn)
  ```

  **Step 3: Create a SavedModelEstimator from the exported SavedModel.**

  ```python
  est = tf.estimator.experimental.SavedModelEstimator(export_dir)

  # If all modes were exported, you can immediately evaluate and predict, or
  # continue training. Otherwise only predict is available.
  eval_results = est.evaluate(input_fn=input_fn, steps=1)
  print(eval_results)

  est.train(input_fn=input_fn, steps=20)

  def predict_input_fn():
    example = tf.train.Example()
    example.features.feature['feature1'].bytes_list.value.extend(['yellow'])
    example.features.feature['feature2'].float_list.value.extend([1.])
    return {'inputs':tf.constant([example.SerializeToString()])}

  predictions = est.predict(predict_input_fn)
  print(next(predictions))
  ```
  """

  def __init__(self, saved_model_dir, model_dir=None):
    """Initialize a SavedModelEstimator.

    The SavedModelEstimator loads its model function and variable values from
    the graphs defined in the SavedModel. There is no option to pass in
    `RunConfig` or `params` arguments, because the model function graph is
    defined statically in the SavedModel.

    Args:
      saved_model_dir: Directory containing SavedModel protobuf and subfolders.
      model_dir: Directory to save new checkpoints during training.

    Raises:
      NotImplementedError: If a DistributionStrategy is defined in the config.
        Unless the SavedModelEstimator is subclassed, this shouldn't happen.
    """

    super(SavedModelEstimator, self).__init__(
        model_fn=self._model_fn_from_saved_model, model_dir=model_dir)
    if self._train_distribution or self._eval_distribution:
      raise NotImplementedError(
          'SavedModelEstimator currently does not support '
          'DistributionStrategy.')
    self.saved_model_dir = saved_model_dir
    self.saved_model_loader = loader_impl.SavedModelLoader(saved_model_dir)
    self._available_modes = self._extract_available_modes()

  def _extract_available_modes(self):
    """Return list of modes found in SavedModel."""
    available_modes = []
    tf.compat.v1.logging.info(
        'Checking available modes for SavedModelEstimator.')
    for mode in [ModeKeys.TRAIN, ModeKeys.EVAL, ModeKeys.PREDICT]:
      try:
        self._get_meta_graph_def_for_mode(mode)
      except RuntimeError:
        tf.compat.v1.logging.warn('%s mode not found in SavedModel.' % mode)
        continue

      if self._get_signature_def_for_mode(mode) is not None:
        available_modes.append(mode)

    tf.compat.v1.logging.info('Available modes for Estimator: %s' %
                              available_modes)
    return available_modes

  def _validate_mode(self, mode):
    """Make sure that mode can be run using the SavedModel."""
    if mode not in self._available_modes:
      raise RuntimeError('%s mode is not available in the SavedModel. Use '
                         'saved_model_cli to check that the Metagraph for this '
                         'mode has been exported.' % mode)

  def _get_meta_graph_def_for_mode(self, mode):
    tags = export_lib.EXPORT_TAG_MAP[mode]
    return self.saved_model_loader.get_meta_graph_def_from_tags(tags)

  def _get_signature_def_for_mode(self, mode):
    meta_graph_def = self._get_meta_graph_def_for_mode(mode)
    if mode == ModeKeys.PREDICT:
      sig_def_key = tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY
    else:
      sig_def_key = mode
    if sig_def_key not in meta_graph_def.signature_def:
      tf.compat.v1.logging.warn(
          'Metagraph for mode %s was found, but SignatureDef with'
          ' key \"%s\" is missing.' % (mode, sig_def_key))
      return None
    return meta_graph_def.signature_def[sig_def_key]

  def _get_saver_def_from_mode(self, mode):
    meta_graph_def = self._get_meta_graph_def_for_mode(mode)
    return meta_graph_def.saver_def

  def _create_and_assert_global_step(self, graph):
    # Do nothing here. The global step variable will be created/loaded from the
    # SavedModel. If a global step variable were created here, the result
    # will be two duplicate global step variables, causing issues during
    # the warm-start phase.
    # Due to the global variable being created in the model function, this may
    # cause issues when running DistributionStrategy. Thus, DistributionStrategy
    # is not yet supported with SavedModelEstimator.
    return None

  def _model_fn_from_saved_model(self, features, labels, mode):
    """Load a SavedModel graph and return an EstimatorSpec."""
    # TODO(kathywu): Model function loads placeholders from the graph. Calling
    # export_all_saved_models creates another placeholder for the inputs, on top
    # of the original placeholders. There should be a way to avoid this.
    self._validate_mode(mode)

    g = tf.compat.v1.get_default_graph()
    if tf.compat.v1.train.get_global_step(g) is not None:
      raise RuntimeError(
          'Graph must not contain a global step tensor before the SavedModel is'
          ' loaded. Please make sure that the input function does not create a '
          'global step.')

    # Extract SignatureDef for information about the input and output tensors.
    signature_def = self._get_signature_def_for_mode(mode)

    # Generate input map for replacing the inputs in the SavedModel graph with
    # the provided features and labels.
    input_map = _generate_input_map(signature_def, features, labels)

    # Create a list of the names of output tensors. When the graph is loaded,
    # names of the output tensors may be remapped. This ensures that the correct
    # tensors are returned in the EstimatorSpec.
    output_tensor_names = [
        value.name for value in six.itervalues(signature_def.outputs)
    ]

    # Load the graph. `output_tensors` contains output `Tensors` in the same
    # same order as the `output_tensor_names` list.
    tags = export_lib.EXPORT_TAG_MAP[mode]
    _, output_tensors = self.saved_model_loader.load_graph(
        g, tags, input_map=input_map, return_elements=output_tensor_names)

    # Create saver object, and restore from the SavedModel `variables` directory
    # if no checkpoints have been saved in the `model_dir`.
    saver_obj = tf.compat.v1.train.Saver(
        saver_def=self._get_saver_def_from_mode(mode))
    init_fn = None
    if not super(SavedModelEstimator, self).latest_checkpoint():
      init_fn = self._restore_from_saver

    # Create a scaffold from the MetaGraphDef that contains ops to initialize
    # the graph. This should mirror the steps from _add_meta_graph_for_mode(),
    # which creates a MetaGraphDef from the EstimatorSpec's scaffold.
    # Get asset tensors, if any.
    meta_graph_def = self._get_meta_graph_def_for_mode(mode)
    asset_tensors_dictionary = loader_impl.get_asset_tensors(
        self.saved_model_loader.export_dir, meta_graph_def, import_scope=None)
    # TODO(kathywu): switch to loader_impl._get_main_op
    scaffold = tf.compat.v1.train.Scaffold(
        local_init_op=loader_impl._get_main_op_tensor(  # pylint: disable=protected-access
            meta_graph_def),
        local_init_feed_dict=asset_tensors_dictionary,
        saver=saver_obj,
        init_fn=init_fn)

    # Ensure that a global step tensor has been created.
    global_step_tensor = tf.compat.v1.train.get_global_step(g)
    tf.compat.v1.train.assert_global_step(global_step_tensor)

    # Extract values to return in the EstimatorSpec.
    output_map = dict(zip(output_tensor_names, output_tensors))
    outputs = {
        key: output_map[value.name]
        for key, value in six.iteritems(signature_def.outputs)
    }

    loss, predictions, metrics = _validate_and_extract_outputs(
        mode, outputs, signature_def.method_name)

    train_op = tf.compat.v1.get_collection(constants.TRAIN_OP_KEY)
    if len(train_op) > 1:
      raise RuntimeError('Multiple ops found in the train_op collection.')
    train_op = None if not train_op else train_op[0]

    _clear_saved_model_collections()
    return model_fn_lib.EstimatorSpec(
        scaffold=scaffold,
        mode=mode,
        loss=loss,
        train_op=train_op,
        predictions=predictions,
        eval_metric_ops=metrics)

  def _restore_from_saver(self, scaffold, session):
    return scaffold.saver.restore(session,
                                  _get_saved_model_ckpt(self.saved_model_dir))

  def latest_checkpoint(self):
    """Returns the filename of the latest saved checkpoint.

    Returns:
      Filename of latest checkpoint in `model_dir`. If no checkpoints are found
      in `model_dir`, then the path to the SavedModel checkpoint is returned.
    """
    return (super(SavedModelEstimator, self).latest_checkpoint() or
            _get_saved_model_ckpt(self.saved_model_dir))


def _get_saved_model_ckpt(saved_model_dir):
  """Return path to variables checkpoint in a `SavedModel` directory."""
  if not tf.compat.v1.gfile.Exists(
      os.path.join(
          saved_model_utils.get_variables_dir(saved_model_dir),
          tf.compat.as_text('variables.index'))):
    raise ValueError('Directory provided has an invalid SavedModel format: %s' %
                     saved_model_dir)
  return saved_model_utils.get_variables_path(saved_model_dir)


def _clear_saved_model_collections():
  """Clear collections that are expected empty when exporting a SavedModel.

  The SavedModel builder uses these collections to track ops necessary to
  restore the graph state. These collections are expected to be empty before
  MetaGraphs are added to the builder.
  """
  del tf.compat.v1.get_collection_ref(tf.saved_model.ASSETS_KEY)[:]
  del tf.compat.v1.get_collection_ref(
      tf.compat.v1.saved_model.LEGACY_INIT_OP_KEY)[:]
  del tf.compat.v1.get_collection_ref(tf.compat.v1.saved_model.MAIN_OP_KEY)[:]
  del tf.compat.v1.get_collection_ref(constants.TRAIN_OP_KEY)[:]


def _generate_input_map(signature_def, features, labels):
  """Return dict mapping an input tensor name to a feature or label tensor.

  Args:
    signature_def: SignatureDef loaded from SavedModel
    features: A `Tensor`, `SparseTensor`, or dict of string to `Tensor` or
      `SparseTensor`, specifying the features to be passed to the model.
    labels: A `Tensor`, `SparseTensor`, or dict of string to `Tensor` or
      `SparseTensor`, specifying the labels to be passed to the model. May be
      `None`.

  Returns:
    dict mapping string names of inputs to features or labels tensors

  Raises:
    ValueError: if SignatureDef inputs are not completely mapped by the input
      features and labels.
  """
  # Ensure that features and labels are dictionaries. If not, convert each to
  # a dictionary with a single item. The default keys are different for features
  # and labels.
  features = export_lib.wrap_and_check_input_tensors(features, 'feature')
  if labels is not None:
    # Unlike features, labels may be None (in prediction mode)
    labels = export_lib.wrap_and_check_input_tensors(labels, 'label')

  inputs = signature_def.inputs
  input_map = {}
  for key, tensor_info in six.iteritems(inputs):
    input_name = tensor_info.name
    if ':' in input_name:
      input_name = input_name[:input_name.find(':')]

    # When tensors are used as control inputs for operations, their names are
    # prepended with a '^' character in the GraphDef. To handle possible control
    # flow edge cases, control input names must be included in the input map.
    control_dependency_name = '^' + input_name

    if key in features:
      _check_same_dtype_and_shape(features[key], tensor_info, key)
      input_map[input_name] = input_map[control_dependency_name] = features[key]
    elif labels is not None and key in labels:
      _check_same_dtype_and_shape(labels[key], tensor_info, key)
      input_map[input_name] = input_map[control_dependency_name] = labels[key]
    else:
      raise ValueError(
          'Key \"%s\" not found in features or labels passed in to the model '
          'function. All required keys: %s' % (key, inputs.keys()))

  return input_map


def _check_same_dtype_and_shape(tensor, tensor_info, name):
  """Validate that tensor has the same properties as the TensorInfo proto.

  Args:
    tensor: a `Tensor` object.
    tensor_info: a `TensorInfo` proto.
    name: Name of the input (to identify Tensor if an error is raised).

  Raises:
    ValueError: If the tensor shape or dtype don't match the TensorInfo
  """
  dtype_error = (tensor.dtype != tf.dtypes.DType(tensor_info.dtype))
  shape_error = not tensor.shape.is_compatible_with(tensor_info.tensor_shape)

  if dtype_error or shape_error:
    msg = 'Tensor shape and/or dtype validation failed for input %s:' % name
    if dtype_error:
      msg += ('\n\tExpected dtype: %s, Got: %s' %
              (tf.dtypes.DType(tensor_info.dtype), tensor.dtype))
    if shape_error:
      msg += ('\n\tExpected shape: %s, Got: %s' %
              (tf.TensorShape(tensor_info.tensor_shape), tensor.shape))

    raise ValueError(msg)


def _extract_eval_metrics(output_dict):
  """Return a eval metric dict extracted from the output_dict.

  Eval metrics consist of a value tensor and an update op. Both must be in the
  passed-in tensor dictionary for an eval metric to be added to the returned
  dictionary.

  Args:
    output_dict: a dict that maps strings to tensors.

  Returns:
    dict mapping strings to (value, update_op) tuples.
  """
  # pylint: disable=protected-access
  metric_ops = {}
  separator_char = export_lib._SupervisedOutput._SEPARATOR_CHAR

  for key, tensor in six.iteritems(output_dict):
    split_key = key.split(separator_char)

    # The metric name may contain the separator character, so recreate its name.
    metric_name = separator_char.join(split_key[:-1])

    if split_key[0] == export_lib._SupervisedOutput.METRICS_NAME:
      # If the key ends with the value suffix, and there is a corresponding
      # key ending with the update_op suffix, then add tensors to metrics dict.
      if split_key[-1] == export_lib._SupervisedOutput.METRIC_VALUE_SUFFIX:
        update_op = ''.join([
            metric_name, separator_char,
            export_lib._SupervisedOutput.METRIC_UPDATE_SUFFIX
        ])
        if update_op in output_dict:
          update_op_tensor = output_dict[update_op]
          metric_ops[metric_name] = (tensor, update_op_tensor)

  # pylint: enable=protected-access
  return metric_ops


def _validate_and_extract_outputs(mode, output_dict, method_name):
  """Extract values from SignatureDef output dictionary.

  Args:
    mode: One of the modes enumerated in `tf.estimator.ModeKeys`.
    output_dict: dict of string SignatureDef keys to `Tensor`.
    method_name: Method name of the SignatureDef as a string.

  Returns:
    Tuple of (
      loss: `Tensor` object,
      predictions: dictionary mapping string keys to `Tensor` objects,
      metrics: dictionary mapping string keys to a tuple of two `Tensor` objects
    )

  Raises:
    RuntimeError: raised if SignatureDef has an invalid method name for the mode
  """
  # pylint: disable=protected-access
  loss, predictions, metrics = None, None, None

  if mode == ModeKeys.PREDICT:
    predictions = output_dict
  else:
    # Validate that the SignatureDef's method name matches the expected name for
    # the given mode.
    expected_method_name = signature_constants.SUPERVISED_TRAIN_METHOD_NAME
    if mode == ModeKeys.EVAL:
      expected_method_name = signature_constants.SUPERVISED_EVAL_METHOD_NAME
    if method_name != expected_method_name:
      raise RuntimeError(
          'Invalid SignatureDef method name for mode %s.\n\tExpected: %s\n\t'
          'Got: %s\nPlease ensure that the SavedModel was exported with '
          '`tf.estimator.experimental_export_all_saved_models()`.' %
          (mode, expected_method_name, method_name))

    # Extract loss, metrics and predictions from the output dict.
    loss = output_dict[export_lib._SupervisedOutput.LOSS_NAME]
    metrics = _extract_eval_metrics(output_dict)
    predictions = {
        key: value
        for key, value in six.iteritems(output_dict)
        if key.split(export_lib._SupervisedOutput._SEPARATOR_CHAR)[0] == (
            export_lib._SupervisedOutput.PREDICTIONS_NAME)
    }

  # pylint: enable=protected-access
  return loss, predictions, metrics
