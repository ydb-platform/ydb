#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc


class LivenessWarden(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def list_of_liveness_violations(self):
        pass


class AggregateLivenessWarden(LivenessWarden):

    def __init__(self, list_of_liveness_wardens):
        super(AggregateLivenessWarden, self).__init__()
        self.__list_of_liveness_wardens = list(list_of_liveness_wardens)

    @property
    def list_of_liveness_violations(self):
        list_of_liveness_violations = []
        for warden in self.__list_of_liveness_wardens:
            if hasattr(warden, 'list_of_liveness_violations') and warden.list_of_liveness_violations:
                list_of_liveness_violations.extend(warden.list_of_liveness_violations)
        return list_of_liveness_violations
