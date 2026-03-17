# Copyright 2011-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tornado support for Motor, an asynchronous driver for MongoDB."""

from . import core, motor_gridfs
from .frameworks import tornado as tornado_framework
from .metaprogramming import create_class_with_framework

__all__ = ['MotorClient', 'MotorClientEncryption']


def create_motor_class(cls):
    return create_class_with_framework(cls, tornado_framework,
                                       'motor.motor_tornado')


MotorClient = create_motor_class(core.AgnosticClient)


MotorClientSession = create_motor_class(core.AgnosticClientSession)


MotorDatabase = create_motor_class(core.AgnosticDatabase)


MotorCollection = create_motor_class(core.AgnosticCollection)


MotorCursor = create_motor_class(core.AgnosticCursor)


MotorCommandCursor = create_motor_class(core.AgnosticCommandCursor)


MotorLatentCommandCursor = create_motor_class(core.AgnosticLatentCommandCursor)


MotorChangeStream = create_motor_class(core.AgnosticChangeStream)


MotorGridFSBucket = create_motor_class(motor_gridfs.AgnosticGridFSBucket)


MotorGridIn = create_motor_class(motor_gridfs.AgnosticGridIn)


MotorGridOut = create_motor_class(motor_gridfs.AgnosticGridOut)


MotorGridOutCursor = create_motor_class(motor_gridfs.AgnosticGridOutCursor)


MotorClientEncryption = create_motor_class(core.AgnosticClientEncryption)
