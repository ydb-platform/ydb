"""Utilities for real-time data augmentation on image data.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import warnings

import numpy as np

from .iterator import BatchFromFilesMixin, Iterator
from .utils import validate_filename


class DataFrameIterator(BatchFromFilesMixin, Iterator):
    """Iterator capable of reading images from a directory on disk
        through a dataframe.

    # Arguments
        dataframe: Pandas dataframe containing the filepaths relative to
            `directory` (or absolute paths if `directory` is None) of the
            images in a string column. It should include other column/s
            depending on the `class_mode`:
            - if `class_mode` is `"categorical"` (default value) it must
                include the `y_col` column with the class/es of each image.
                Values in column can be string/list/tuple if a single class
                or list/tuple if multiple classes.
            - if `class_mode` is `"binary"` or `"sparse"` it must include
                the given `y_col` column with class values as strings.
            - if `class_mode` is `"raw"` or `"multi_output"` it should contain
                the columns specified in `y_col`.
            - if `class_mode` is `"input"` or `None` no extra column is needed.
        directory: string, path to the directory to read images from. If `None`,
            data in `x_col` column should be absolute paths.
        image_data_generator: Instance of `ImageDataGenerator` to use for
            random transformations and normalization. If None, no transformations
            and normalizations are made.
        x_col: string, column in `dataframe` that contains the filenames (or
            absolute paths if `directory` is `None`).
        y_col: string or list, column/s in `dataframe` that has the target data.
        weight_col: string, column in `dataframe` that contains the sample
            weights. Default: `None`.
        target_size: tuple of integers, dimensions to resize input images to.
        color_mode: One of `"rgb"`, `"rgba"`, `"grayscale"`.
            Color mode to read images.
        classes: Optional list of strings, classes to use (e.g. `["dogs", "cats"]`).
            If None, all classes in `y_col` will be used.
        class_mode: one of "binary", "categorical", "input", "multi_output",
            "raw", "sparse" or None. Default: "categorical".
            Mode for yielding the targets:
            - `"binary"`: 1D numpy array of binary labels,
            - `"categorical"`: 2D numpy array of one-hot encoded labels.
                Supports multi-label output.
            - `"input"`: images identical to input images (mainly used to
                work with autoencoders),
            - `"multi_output"`: list with the values of the different columns,
            - `"raw"`: numpy array of values in `y_col` column(s),
            - `"sparse"`: 1D numpy array of integer labels,
            - `None`, no targets are returned (the generator will only yield
                batches of image data, which is useful to use in
                `model.predict_generator()`).
        batch_size: Integer, size of a batch.
        shuffle: Boolean, whether to shuffle the data between epochs.
        seed: Random seed for data shuffling.
        data_format: String, one of `channels_first`, `channels_last`.
        save_to_dir: Optional directory where to save the pictures
            being yielded, in a viewable format. This is useful
            for visualizing the random transformations being
            applied, for debugging purposes.
        save_prefix: String prefix to use for saving sample
            images (if `save_to_dir` is set).
        save_format: Format to use for saving sample images
            (if `save_to_dir` is set).
        subset: Subset of data (`"training"` or `"validation"`) if
            validation_split is set in ImageDataGenerator.
        interpolation: Interpolation method used to resample the image if the
            target size is different from that of the loaded image.
            Supported methods are "nearest", "bilinear", and "bicubic".
            If PIL version 1.1.3 or newer is installed, "lanczos" is also
            supported. If PIL version 3.4.0 or newer is installed, "box" and
            "hamming" are also supported. By default, "nearest" is used.
        dtype: Dtype to use for the generated arrays.
        validate_filenames: Boolean, whether to validate image filenames in
        `x_col`. If `True`, invalid images will be ignored. Disabling this option
        can lead to speed-up in the instantiation of this class. Default: `True`.
    """
    allowed_class_modes = {
        'binary', 'categorical', 'input', 'multi_output', 'raw', 'sparse', None
    }

    def __new__(cls, *args, **kwargs):
        try:
            from tensorflow.keras.utils import Sequence as TFSequence
            if TFSequence not in cls.__bases__:
                cls.__bases__ = cls.__bases__ + (TFSequence,)
        except ImportError:
            pass
        return super(DataFrameIterator, cls).__new__(cls)

    def __init__(self,
                 dataframe,
                 directory=None,
                 image_data_generator=None,
                 x_col="filename",
                 y_col="class",
                 weight_col=None,
                 target_size=(256, 256),
                 color_mode='rgb',
                 classes=None,
                 class_mode='categorical',
                 batch_size=32,
                 shuffle=True,
                 seed=None,
                 data_format='channels_last',
                 save_to_dir=None,
                 save_prefix='',
                 save_format='png',
                 subset=None,
                 interpolation='nearest',
                 dtype='float32',
                 validate_filenames=True):

        super(DataFrameIterator, self).set_processing_attrs(image_data_generator,
                                                            target_size,
                                                            color_mode,
                                                            data_format,
                                                            save_to_dir,
                                                            save_prefix,
                                                            save_format,
                                                            subset,
                                                            interpolation)
        df = dataframe.copy()
        self.directory = directory or ''
        self.class_mode = class_mode
        self.dtype = dtype
        # check that inputs match the required class_mode
        self._check_params(df, x_col, y_col, weight_col, classes)
        if validate_filenames:  # check which image files are valid and keep them
            df = self._filter_valid_filepaths(df, x_col)
        if class_mode not in ["input", "multi_output", "raw", None]:
            df, classes = self._filter_classes(df, y_col, classes)
            num_classes = len(classes)
            # build an index of all the unique classes
            self.class_indices = dict(zip(classes, range(len(classes))))
        # retrieve only training or validation set
        if self.split:
            num_files = len(df)
            start = int(self.split[0] * num_files)
            stop = int(self.split[1] * num_files)
            df = df.iloc[start: stop, :]
        # get labels for each observation
        if class_mode not in ["input", "multi_output", "raw", None]:
            self.classes = self.get_classes(df, y_col)
        self.filenames = df[x_col].tolist()
        self._sample_weight = df[weight_col].values if weight_col else None

        if class_mode == "multi_output":
            self._targets = [np.array(df[col].tolist()) for col in y_col]
        if class_mode == "raw":
            self._targets = df[y_col].values
        self.samples = len(self.filenames)
        validated_string = 'validated' if validate_filenames else 'non-validated'
        if class_mode in ["input", "multi_output", "raw", None]:
            print('Found {} {} image filenames.'
                  .format(self.samples, validated_string))
        else:
            print('Found {} {} image filenames belonging to {} classes.'
                  .format(self.samples, validated_string, num_classes))
        self._filepaths = [
            os.path.join(self.directory, fname) for fname in self.filenames
        ]
        super(DataFrameIterator, self).__init__(self.samples,
                                                batch_size,
                                                shuffle,
                                                seed)

    def _check_params(self, df, x_col, y_col, weight_col, classes):
        # check class mode is one of the currently supported
        if self.class_mode not in self.allowed_class_modes:
            raise ValueError('Invalid class_mode: {}; expected one of: {}'
                             .format(self.class_mode, self.allowed_class_modes))
        # check that y_col has several column names if class_mode is multi_output
        if (self.class_mode == 'multi_output') and not isinstance(y_col, list):
            raise TypeError(
                'If class_mode="{}", y_col must be a list. Received {}.'
                .format(self.class_mode, type(y_col).__name__)
            )
        # check that filenames/filepaths column values are all strings
        if not all(df[x_col].apply(lambda x: isinstance(x, str))):
            raise TypeError('All values in column x_col={} must be strings.'
                            .format(x_col))
        # check labels are string if class_mode is binary or sparse
        if self.class_mode in {'binary', 'sparse'}:
            if not all(df[y_col].apply(lambda x: isinstance(x, str))):
                raise TypeError('If class_mode="{}", y_col="{}" column '
                                'values must be strings.'
                                .format(self.class_mode, y_col))
        # check that if binary there are only 2 different classes
        if self.class_mode == 'binary':
            if classes:
                classes = set(classes)
                if len(classes) != 2:
                    raise ValueError('If class_mode="binary" there must be 2 '
                                     'classes. {} class/es were given.'
                                     .format(len(classes)))
            elif df[y_col].nunique() != 2:
                raise ValueError('If class_mode="binary" there must be 2 classes. '
                                 'Found {} classes.'.format(df[y_col].nunique()))
        # check values are string, list or tuple if class_mode is categorical
        if self.class_mode == 'categorical':
            types = (str, list, tuple)
            if not all(df[y_col].apply(lambda x: isinstance(x, types))):
                raise TypeError('If class_mode="{}", y_col="{}" column '
                                'values must be type string, list or tuple.'
                                .format(self.class_mode, y_col))
        # raise warning if classes are given but will be unused
        if classes and self.class_mode in {"input", "multi_output", "raw", None}:
            warnings.warn('`classes` will be ignored given the class_mode="{}"'
                          .format(self.class_mode))
        # check that if weight column that the values are numerical
        if weight_col and not issubclass(df[weight_col].dtype.type, np.number):
            raise TypeError('Column weight_col={} must be numeric.'
                            .format(weight_col))

    def get_classes(self, df, y_col):
        labels = []
        for label in df[y_col]:
            if isinstance(label, (list, tuple)):
                labels.append([self.class_indices[lbl] for lbl in label])
            else:
                labels.append(self.class_indices[label])
        return labels

    @staticmethod
    def _filter_classes(df, y_col, classes):
        df = df.copy()

        def remove_classes(labels, classes):
            if isinstance(labels, (list, tuple)):
                labels = [cls for cls in labels if cls in classes]
                return labels or None
            elif isinstance(labels, str):
                return labels if labels in classes else None
            else:
                raise TypeError(
                    "Expect string, list or tuple but found {} in {} column "
                    .format(type(labels), y_col)
                )

        if classes:
            classes = set(classes)  # sort and prepare for membership lookup
            df[y_col] = df[y_col].apply(lambda x: remove_classes(x, classes))
        else:
            classes = set()
            for v in df[y_col]:
                if isinstance(v, (list, tuple)):
                    classes.update(v)
                else:
                    classes.add(v)
        return df.dropna(subset=[y_col]), sorted(classes)

    def _filter_valid_filepaths(self, df, x_col):
        """Keep only dataframe rows with valid filenames

        # Arguments
            df: Pandas dataframe containing filenames in a column
            x_col: string, column in `df` that contains the filenames or filepaths

        # Returns
            absolute paths to image files
        """
        filepaths = df[x_col].map(
            lambda fname: os.path.join(self.directory, fname)
        )
        mask = filepaths.apply(validate_filename, args=(self.white_list_formats,))
        n_invalid = (~mask).sum()
        if n_invalid:
            warnings.warn(
                'Found {} invalid image filename(s) in x_col="{}". '
                'These filename(s) will be ignored.'
                .format(n_invalid, x_col)
            )
        return df[mask]

    @property
    def filepaths(self):
        return self._filepaths

    @property
    def labels(self):
        if self.class_mode in {"multi_output", "raw"}:
            return self._targets
        else:
            return self.classes

    @property
    def sample_weight(self):
        return self._sample_weight
