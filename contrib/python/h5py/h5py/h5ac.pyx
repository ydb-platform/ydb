# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


"""
    Low-level HDF5 "H5AC" cache configuration interface.
"""


cdef class CacheConfig:
    """Represents H5AC_cache_config_t objects

    """

    #cdef H5AC_cache_config_t cache_config
    #     /* general configuration fields: */
    def __cinit__(self):
        self.cache_config.version = H5AC__CURR_CACHE_CONFIG_VERSION

    @property
    def version(self):
        return self.cache_config.version

    @version.setter
    def version(self, int val):
        self.cache_config.version = val

    @property
    def rpt_fcn_enabled(self):
        return self.cache_config.rpt_fcn_enabled

    @rpt_fcn_enabled.setter
    def rpt_fcn_enabled(self, hbool_t val):
        self.cache_config.rpt_fcn_enabled = val

    @property
    def evictions_enabled(self):
        return self.cache_config.evictions_enabled

    @evictions_enabled.setter
    def evictions_enabled(self, hbool_t val):
        self.cache_config.evictions_enabled = val

    @property
    def set_initial_size(self):
        return self.cache_config.set_initial_size

    @set_initial_size.setter
    def set_initial_size(self, hbool_t val):
        self.cache_config.set_initial_size = val

    @property
    def initial_size(self):
        return self.cache_config.initial_size

    @initial_size.setter
    def initial_size(self, size_t val):
        self.cache_config.initial_size = val

    @property
    def min_clean_fraction(self):
        return self.cache_config.min_clean_fraction

    @min_clean_fraction.setter
    def min_clean_fraction(self, double val):
        self.cache_config.min_clean_fraction = val

    @property
    def max_size(self):
        return self.cache_config.max_size

    @max_size.setter
    def max_size(self, size_t val):
        self.cache_config.max_size = val

    @property
    def min_size(self):
        return self.cache_config.min_size

    @min_size.setter
    def min_size(self, size_t val):
        self.cache_config.min_size = val

    @property
    def epoch_length(self):
        return self.cache_config.epoch_length

    @epoch_length.setter
    def epoch_length(self, long int val):
        self.cache_config.epoch_length = val

    #    /* size increase control fields: */
    @property
    def incr_mode(self):
        return self.cache_config.incr_mode

    @incr_mode.setter
    def incr_mode(self, H5C_cache_incr_mode val):
        self.cache_config.incr_mode = val

    @property
    def lower_hr_threshold(self):
        return self.cache_config.lower_hr_threshold

    @lower_hr_threshold.setter
    def lower_hr_threshold(self, double val):
        self.cache_config.lower_hr_threshold = val

    @property
    def increment(self):
        return self.cache_config.increment

    @increment.setter
    def increment(self, double val):
        self.cache_config.increment = val

    @property
    def apply_max_increment(self):
        return self.cache_config.apply_max_increment

    @apply_max_increment.setter
    def apply_max_increment(self, hbool_t val):
        self.cache_config.apply_max_increment = val

    @property
    def max_increment(self):
        return self.cache_config.max_increment

    @max_increment.setter
    def max_increment(self, size_t val):
        self.cache_config.max_increment = val

    @property
    def flash_incr_mode(self):
        return self.cache_config.flash_incr_mode

    @flash_incr_mode.setter
    def flash_incr_mode(self, H5C_cache_flash_incr_mode val):
        self.cache_config.flash_incr_mode = val

    @property
    def flash_multiple(self):
        return self.cache_config.flash_multiple

    @flash_multiple.setter
    def flash_multiple(self, double val):
        self.cache_config.flash_multiple = val

    @property
    def flash_threshold(self):
        return self.cache_config.flash_threshold

    @flash_threshold.setter
    def flash_threshold(self, double val):
        self.cache_config.flash_threshold = val

    # /* size decrease control fields: */
    @property
    def decr_mode(self):
        return self.cache_config.decr_mode

    @decr_mode.setter
    def decr_mode(self, H5C_cache_decr_mode val):
        self.cache_config.decr_mode = val

    @property
    def upper_hr_threshold(self):
        return self.cache_config.upper_hr_threshold

    @upper_hr_threshold.setter
    def upper_hr_threshold(self, double val):
        self.cache_config.upper_hr_threshold = val

    @property
    def decrements(self):
        return self.cache_config.decrement

    @decrements.setter
    def decrements(self, double val):
        self.cache_config.decrement = val

    @property
    def apply_max_decrement(self):
        return self.cache_config.apply_max_decrement

    @apply_max_decrement.setter
    def apply_max_decrement(self, hbool_t val):
        self.cache_config.apply_max_decrement = val

    @property
    def max_decrement(self):
        return self.cache_config.max_decrement

    @max_decrement.setter
    def max_decrement(self, size_t val):
        self.cache_config.max_decrement = val

    @property
    def epochs_before_eviction(self):
        return self.cache_config.epochs_before_eviction

    @epochs_before_eviction.setter
    def epochs_before_eviction(self, int val):
        self.cache_config.epochs_before_eviction = val

    @property
    def apply_empty_reserve(self):
        return self.cache_config.apply_empty_reserve

    @apply_empty_reserve.setter
    def apply_empty_reserve(self, hbool_t val):
        self.cache_config.apply_empty_reserve = val

    @property
    def empty_reserve(self):
        return self.cache_config.empty_reserve

    @empty_reserve.setter
    def empty_reserve(self, double val):
        self.cache_config.empty_reserve = val

    # /* parallel configuration fields: */
    @property
    def dirty_bytes_threshold(self):
        return self.cache_config.dirty_bytes_threshold

    @dirty_bytes_threshold.setter
    def dirty_bytes_threshold(self, int val):
        self.cache_config.dirty_bytes_threshold = val
