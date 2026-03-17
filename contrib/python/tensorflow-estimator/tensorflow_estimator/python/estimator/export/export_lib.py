# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
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
"""All public utility methods for exporting Estimator to SavedModel.

This file includes functions and constants from core (model_utils) and export.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=unused-import,line-too-long, wildcard-import
from tensorflow.python.saved_model.model_utils import build_all_signature_defs
from tensorflow.python.saved_model.model_utils import export_outputs_for_mode
from tensorflow.python.saved_model.model_utils import EXPORT_TAG_MAP
from tensorflow.python.saved_model.model_utils import get_export_outputs
from tensorflow.python.saved_model.model_utils import get_temp_export_dir
from tensorflow.python.saved_model.model_utils import get_timestamped_export_dir
from tensorflow.python.saved_model.model_utils import SIGNATURE_KEY_MAP
from tensorflow.python.saved_model.model_utils.export_output import _SupervisedOutput
from tensorflow.python.saved_model.model_utils.export_output import ClassificationOutput
from tensorflow.python.saved_model.model_utils.export_output import EvalOutput
from tensorflow.python.saved_model.model_utils.export_output import ExportOutput
from tensorflow.python.saved_model.model_utils.export_output import PredictOutput
from tensorflow.python.saved_model.model_utils.export_output import RegressionOutput
from tensorflow.python.saved_model.model_utils.export_output import TrainOutput
from tensorflow_estimator.python.estimator.export.export import build_parsing_serving_input_receiver_fn
from tensorflow_estimator.python.estimator.export.export import build_raw_serving_input_receiver_fn
from tensorflow_estimator.python.estimator.export.export import build_raw_supervised_input_receiver_fn
from tensorflow_estimator.python.estimator.export.export import build_supervised_input_receiver_fn_from_input_fn
from tensorflow_estimator.python.estimator.export.export import ServingInputReceiver
from tensorflow_estimator.python.estimator.export.export import SupervisedInputReceiver
from tensorflow_estimator.python.estimator.export.export import TensorServingInputReceiver
from tensorflow_estimator.python.estimator.export.export import UnsupervisedInputReceiver
from tensorflow_estimator.python.estimator.export.export import wrap_and_check_input_tensors
# pylint: enable=unused-import,line-too-long, wildcard-import
