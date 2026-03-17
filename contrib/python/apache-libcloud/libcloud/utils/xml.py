# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    "fixxpath",
    "findtext",
    "findattr",
    "findall",
    "findall_ignore_namespace",
    "findtext_ignore_namespace",
]


def fixxpath(xpath, namespace=None):
    # ElementTree wants namespaces in its xpaths, so here we add them.
    if not namespace:
        return xpath
    return "/".join(["{{{}}}{}".format(namespace, e) for e in xpath.split("/")])


def findtext(element, xpath, namespace=None, no_text_value=""):
    """
    :param no_text_value: Value to return if the provided element has no text
                          value.
    :type no_text_value: ``object``
    """
    value = element.findtext(fixxpath(xpath=xpath, namespace=namespace))

    if value == "":
        return no_text_value
    return value


def findtext_ignore_namespace(element, xpath, namespace=None, no_text_value=""):
    """
    Special version of findtext() which first tries to find the provided value using the provided
    namespace and in case no results are found we fallback to the xpath lookup without namespace.

    This is needed because some providers return some responses with namespace and some without.
    """

    result = findtext(
        element=element, xpath=xpath, namespace=namespace, no_text_value=no_text_value
    )

    if not result and namespace:
        result = findtext(element=element, xpath=xpath, namespace=None, no_text_value=no_text_value)

    return result


def findattr(element, xpath, namespace=None):
    return element.findtext(fixxpath(xpath=xpath, namespace=namespace))


def findall(element, xpath, namespace=None):
    return element.findall(fixxpath(xpath=xpath, namespace=namespace))


def findall_ignore_namespace(element, xpath, namespace=None):
    """
    Special version of findall() which first tries to find the provided value using the provided
    namespace and in case no results are found we fallback to the xpath lookup without namespace.

    This is needed because some providers return some responses with namespace and some without.
    """
    result = findall(element=element, xpath=xpath, namespace=namespace)

    if not result and namespace:
        result = findall(element=element, xpath=xpath, namespace=None)

    return result
