# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ncclient.xml_ import *

from ncclient.operations.rpc import RPC

from ncclient.operations import util

from .errors import OperationError

import logging

logger = logging.getLogger("ncclient.operations.edit")

"Operations related to changing device configuration"

class EditConfig(RPC):
    "`edit-config` RPC"

    def request(self, config, format='xml', target='candidate', default_operation=None,
            test_option=None, error_option=None):
        """Loads all or part of the specified *config* to the *target* configuration datastore.

        *target* is the name of the configuration datastore being edited

        *config* is the configuration, which must be rooted in the `config` element. It can be specified either as a string or an :class:`~xml.etree.ElementTree.Element`.

        *default_operation* if specified must be one of { `"merge"`, `"replace"`, or `"none"` }

        *test_option* if specified must be one of { `"test-then-set"`, `"set"`, `"test-only"` }

        *error_option* if specified must be one of { `"stop-on-error"`, `"continue-on-error"`, `"rollback-on-error"` }

        The `"rollback-on-error"` *error_option* depends on the `:rollback-on-error` capability.
        """
        node = new_ele("edit-config")
        node.append(util.datastore_or_url("target", target, self._assert))
        if (default_operation is not None
                and util.validate_args('default_operation', default_operation, ["merge", "replace", "none"]) is True):
            sub_ele(node, "default-operation").text = default_operation
        if (test_option is not None
                and util.validate_args('test_option', test_option, ["test-then-set", "set", "test-only"]) is True):
            self._assert(':validate')
            if test_option == 'test-only':
                self._assert(':validate:1.1')
            sub_ele(node, "test-option").text = test_option
        if (error_option is not None
                and util.validate_args('error_option', error_option, ["stop-on-error", "continue-on-error", "rollback-on-error"]) is True):
            if error_option == "rollback-on-error":
                self._assert(":rollback-on-error")
            sub_ele(node, "error-option").text = error_option
        if format == 'xml':
            node.append(validated_element(config, ("config", qualify("config"))))
        elif format == 'text':
            config_text = sub_ele(node, "config-text")
            sub_ele(config_text, "configuration-text").text = config
        elif format == 'url':
            if util.url_validator(config):
                self._assert(':url')
                sub_ele(node, "url").text = config
            else:
                raise OperationError("Invalid URL.")
        node = self._device_handler.transform_edit_config(node)
        return self._request(node)


class DeleteConfig(RPC):
    "`delete-config` RPC"

    def request(self, target):
        """Delete a configuration datastore.

        *target* specifies the  name or URL of configuration datastore to delete

        :seealso: :ref:`srctarget_params`"""
        node = new_ele("delete-config")
        node.append(util.datastore_or_url("target", target, self._assert))
        return self._request(node)


class CopyConfig(RPC):
    "`copy-config` RPC"

    def request(self, source, target):
        """Create or replace an entire configuration datastore with the contents of another complete
        configuration datastore.

        *source* is the name of the configuration datastore to use as the source of the copy operation or `config` element containing the configuration subtree to copy

        *target* is the name of the configuration datastore to use as the destination of the copy operation

        :seealso: :ref:`srctarget_params`"""
        node = new_ele("copy-config")
        node.append(util.datastore_or_url("target", target, self._assert))

        try:
            # datastore name or URL
            node.append(util.datastore_or_url("source", source, self._assert))
        except Exception:
            # `source` with `config` element containing the configuration subtree to copy
            node.append(validated_element(source, ("source", qualify("source"))))

        return self._request(node)


class Validate(RPC):
    "`validate` RPC. Depends on the `:validate` capability."

    DEPENDS = [':validate']

    def request(self, source="candidate"):
        """Validate the contents of the specified configuration.

        *source* is the name of the configuration datastore being validated or `config` element containing the configuration subtree to be validated

        :seealso: :ref:`srctarget_params`"""
        node = new_ele("validate")
        if type(source) is str:
            src = util.datastore_or_url("source", source, self._assert)
        else:
            validated_element(source, ("config", qualify("config")))
            src = new_ele("source")
            src.append(source)
        node.append(src)
        return self._request(node)


class Commit(RPC):
    "`commit` RPC. Depends on the `:candidate` capability, and the `:confirmed-commit`."

    DEPENDS = [':candidate']

    def request(self, confirmed=False, timeout=None, persist=None, persist_id=None):
        """Commit the candidate configuration as the device's new current configuration. Depends on the `:candidate` capability.

        A confirmed commit (i.e. if *confirmed* is `True`) is reverted if there is no followup commit within the *timeout* interval. If no timeout is specified the confirm timeout defaults to 600 seconds (10 minutes). A confirming commit may have the *confirmed* parameter but this is not required. Depends on the `:confirmed-commit` capability.

        *confirmed* whether this is a confirmed commit

        *timeout* specifies the confirm timeout in seconds

        *persist* make the confirmed commit survive a session termination, and set a token on the ongoing confirmed commit

        *persist_id* value must be equal to the value given in the <persist> parameter to the original <commit> operation.
        """
        node = new_ele("commit")
        if persist and persist_id:
            raise OperationError("Invalid operation as persist cannot be present with persist-id")
        if confirmed:
            self._assert(":confirmed-commit")
            sub_ele(node, "confirmed")
            if timeout is not None:
                sub_ele(node, "confirm-timeout").text = timeout
            if persist is not None:
                sub_ele(node, "persist").text = persist
        if persist_id:
            sub_ele(node, "persist-id").text = persist_id

        return self._request(node)


class CancelCommit(RPC):
    "`cancel-commit` RPC. Depends on the `:candidate` and `:confirmed-commit` capabilities."

    DEPENDS = [':candidate', ':confirmed-commit']

    def request(self, persist_id=None):
        """Cancel an ongoing confirmed commit. Depends on the `:candidate` and `:confirmed-commit` capabilities.

        *persist-id* value must be equal to the value given in the <persist> parameter to the previous <commit> operation.
        """
        node = new_ele("cancel-commit")
        if persist_id is not None:
            sub_ele(node, "persist-id").text = persist_id

        return self._request(node)


class DiscardChanges(RPC):
    "`discard-changes` RPC. Depends on the `:candidate` capability."

    DEPENDS = [":candidate"]

    def request(self):
        """Revert the candidate configuration to the currently running configuration. Any uncommitted changes are discarded."""

        return self._request(new_ele("discard-changes"))
