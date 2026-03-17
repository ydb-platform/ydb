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
"""Classes for different types of export output."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=unused-import
from tensorflow.python.saved_model.model_utils.export_output import _SupervisedOutput
from tensorflow.python.saved_model.model_utils.export_output import ClassificationOutput
from tensorflow.python.saved_model.model_utils.export_output import EvalOutput
from tensorflow.python.saved_model.model_utils.export_output import ExportOutput
from tensorflow.python.saved_model.model_utils.export_output import PredictOutput
from tensorflow.python.saved_model.model_utils.export_output import RegressionOutput
from tensorflow.python.saved_model.model_utils.export_output import TrainOutput
# pylint: enable=unused-import
from tensorflow.python.util.tf_export import estimator_export

estimator_export('estimator.export.ExportOutput')(ExportOutput)
estimator_export('estimator.export.ClassificationOutput')(ClassificationOutput)
estimator_export('estimator.export.RegressionOutput')(RegressionOutput)
estimator_export('estimator.export.PredictOutput')(PredictOutput)
estimator_export('estimator.export.EvalOutput')(EvalOutput)
