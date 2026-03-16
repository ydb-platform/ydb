# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.exceptions import (
    FacebookBadObjectError,
)
from facebook_business.api import (
    FacebookAdsApi,
    Cursor,
    FacebookRequest,
)

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.objectparser import ObjectParser

import logging

class AbstractCrudObject(AbstractObject):
    """
    Extends AbstractObject and implements methods to create, read, update,
    and delete.
    Attributes:
        parent_id: The object's parent's id. (default None)
        api: The api instance associated with this object. (default None)
    """

    def __init__(self, fbid=None, parent_id=None, api=None):
        """Initializes a CRUD object.
        Args:
            fbid (optional): The id of the object ont the Graph.
            parent_id (optional): The id of the object's parent.
            api (optional): An api object which all calls will go through. If
                an api object is not specified, api calls will revert to going
                through the default api.
        """
        super(AbstractCrudObject, self).__init__()

        self._api = api or FacebookAdsApi.get_default_api()
        self._changes = {}
        if (parent_id is not None):
            warning_message = "parent_id as a parameter of constructor is " \
                  "being deprecated."
            logging.warning(warning_message)
        self._parent_id = parent_id
        self._data['id'] = fbid
        self._include_summary = True

    def __setitem__(self, key, value):
        """Sets an item in this CRUD object while maintaining a changelog."""

        if key not in self._data or self._data[key] != value:
            self._changes[key] = value
        super(AbstractCrudObject, self).__setitem__(key, value)
        if '_setitem_trigger' in dir(self):
            self._setitem_trigger(key, value)

        return self

    def __delitem__(self, key):
        del self._data[key]
        self._changes.pop(key, None)

    def __eq__(self, other):
        """Two objects are the same if they have the same fbid."""
        return (
            # Same class
            isinstance(other, self.__class__) and

            # Both have id's
            self.get_id() and other.get_id() and

            # Both have same id
            self.get_id() == other.get_id()
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def get_by_ids(cls, ids, params=None, fields=None, api=None):
        api = api or FacebookAdsApi.get_default_api()
        params = dict(params or {})
        cls._assign_fields_to_params(fields, params)
        params['ids'] = ','.join(map(str, ids))
        response = api.call(
            'GET',
            ['/'],
            params=params,
        )
        result = []
        for fbid, data in response.json().items():
            obj = cls(fbid, api=api)
            obj._set_data(data)
            result.append(obj)
        return result

    # Getters

    def get_id(self):
        """Returns the object's fbid if set. Else, it returns None."""
        return self[self.Field.id] if hasattr(self, 'Field') and hasattr(self.Field, 'Field') else self['id']

    # @deprecated deprecate parent_id in AbstractCrudObject
    def get_parent_id(self):
        warning_message = "parent_id is being deprecated."
        logging.warning(warning_message)
        """Returns the object's parent's id."""
        return self._parent_id or FacebookAdsApi.get_default_account_id()

    def get_api(self):
        """
        Returns the api associated with the object.
        """
        return self._api

    def get_id_assured(self):
        """Returns the fbid of the object.
        Raises:
            FacebookBadObjectError if the object does not have an id.
        """
        if not self.get(self.Field.id):
            raise FacebookBadObjectError(
                "%s object needs an id for this operation."
                % self.__class__.__name__,
            )

        return self.get_id()

    # @deprecated deprecate parent_id in AbstractCrudObject
    def get_parent_id_assured(self):
        """Returns the object's parent's fbid.
        Raises:
            FacebookBadObjectError if the object does not have a parent id.
        """
        warning_message = "parent_id is being deprecated."
        logging.warning(warning_message)
        if not self.get_parent_id():
            raise FacebookBadObjectError(
                "%s object needs a parent_id for this operation."
                % self.__class__.__name__,
            )

        return self.get_parent_id()

    def get_api_assured(self):
        """Returns the fbid of the object.
        Raises:
            FacebookBadObjectError if get_api returns None.
        """
        api = self.get_api()
        if not api:
            raise FacebookBadObjectError(
                "%s does not yet have an associated api object.\n"
                "Did you forget to instantiate an API session with: "
                "FacebookAdsApi.init(app_id, app_secret, access_token)"
                % self.__class__.__name__,
            )

        return api

    # Data management

    def _clear_history(self):
        self._changes = {}
        if 'filename' in self._data:
            del self._data['filename']
        return self

    def _set_data(self, data):
        """
        Sets object's data as if it were read from the server.
        Warning: Does not log changes.
        """
        for key in map(str, data):
            self[key] = data[key]

            # clear history due to the update
            self._changes.pop(key, None)
        self._json = data
        return self

    def export_changed_data(self):
        """
        Returns a dictionary of property names mapped to their values for
        properties modified from their original values.
        """
        return self.export_value(self._changes)

    def export_data(self):
        """
        Deprecated. Use export_all_data() or export_changed_data() instead.
        """
        return self.export_changed_data()

    # CRUD Helpers

    def clear_id(self):
        """Clears the object's fbid."""
        del self[self.Field.id]
        return self

    def get_node_path(self):
        """Returns the node's relative path as a tuple of tokens."""
        return (self.get_id_assured(),)

    def get_node_path_string(self):
        """Returns the node's path as a tuple."""
        return '/'.join(self.get_node_path())

    # CRUD
    # @deprecated
    # use Object(parent_id).create_xxx() instead
    def remote_create(
        self,
        batch=None,
        failure=None,
        files=None,
        params=None,
        success=None,
        api_version=None,
    ):
        """Creates the object by calling the API.
        Args:
            batch (optional): A FacebookAdsApiBatch object. If specified,
                the call will be added to the batch.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
        Returns:
            self if not a batch call.
            the return value of batch.add if a batch call.
        """
        warning_message = "`remote_create` is being deprecated, please update your code with new function."
        logging.warning(warning_message)
        if self.get_id():
            raise FacebookBadObjectError(
                "This %s object was already created."
                % self.__class__.__name__,
            )
        if not 'get_endpoint' in dir(self):
            raise TypeError('Cannot create object of type %s.'
                            % self.__class__.__name__)

        params = {} if not params else params.copy()
        params.update(self.export_all_data())
        request = None
        if hasattr(self, 'api_create'):
            request = self.api_create(self.get_parent_id_assured(), pending=True)
        else:
            request = FacebookRequest(
                node_id=self.get_parent_id_assured(),
                method='POST',
                endpoint=self.get_endpoint(),
                api=self._api,
                target_class=self.__class__,
                response_parser=ObjectParser(
                    reuse_object=self
                ),
            )
        request.add_params(params)
        request.add_files(files)

        if batch is not None:

            def callback_success(response):
                self._set_data(response.json())
                self._clear_history()

                if success:
                    success(response)

            def callback_failure(response):
                if failure:
                    failure(response)

            return batch.add_request(
                request=request,
                success=callback_success,
                failure=callback_failure,
            )
        else:
            response = request.execute()
            self._set_data(response._json)
            self._clear_history()

            return self

    # @deprecated
    # use Object(id).api_get() instead
    def remote_read(
        self,
        batch=None,
        failure=None,
        fields=None,
        params=None,
        success=None,
        api_version=None,
    ):
        """Reads the object by calling the API.
        Args:
            batch (optional): A FacebookAdsApiBatch object. If specified,
                the call will be added to the batch.
            fields (optional): A list of fields to read.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
        Returns:
            self if not a batch call.
            the return value of batch.add if a batch call.
        """
        warning_message = "`remote_read` is being deprecated, please update your code with new function."
        logging.warning(warning_message)
        params = dict(params or {})
        if hasattr(self, 'api_get'):
            request = self.api_get(pending=True)
        else:
            request = FacebookRequest(
                node_id=self.get_id_assured(),
                method='GET',
                endpoint='/',
                api=self._api,
                target_class=self.__class__,
                response_parser=ObjectParser(
                    reuse_object=self
                ),
            )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            def callback_success(response):
                self._set_data(response.json())

                if success:
                    success(response)

            def callback_failure(response):
                if failure:
                    failure(response)

            batch_call = batch.add_request(
                request=request,
                success=callback_success,
                failure=callback_failure,
            )
            return batch_call
        else:
            self = request.execute()
            return self

    # @deprecated
    # use Object(id).api_update() instead
    def remote_update(
        self,
        batch=None,
        failure=None,
        files=None,
        params=None,
        success=None,
        api_version=None,
    ):
        """Updates the object by calling the API with only the changes recorded.
        Args:
            batch (optional): A FacebookAdsApiBatch object. If specified,
                the call will be added to the batch.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
        Returns:
            self if not a batch call.
            the return value of batch.add if a batch call.
        """
        warning_message = "`remote_update` is being deprecated, please update your code with new function."
        logging.warning(warning_message)
        params = {} if not params else params.copy()
        params.update(self.export_changed_data())
        self._set_data(params)
        if hasattr(self, 'api_update'):
            request = self.api_update(pending=True)
        else:
            request = FacebookRequest(
                node_id=self.get_id_assured(),
                method='POST',
                endpoint='/',
                api=self._api,
                target_class=self.__class__,
                response_parser=ObjectParser(
                    reuse_object=self
                ),
            )
        request.add_params(params)
        request.add_files(files)

        if batch is not None:
            def callback_success(response):
                self._clear_history()

                if success:
                    success(response)

            def callback_failure(response):
                if failure:
                    failure(response)

            batch_call = batch.add_request(
                request=request,
                success=callback_success,
                failure=callback_failure,
            )
            return batch_call
        else:
            request.execute()
            self._clear_history()

            return self

    # @deprecated
    # use Object(id).api_delete() instead
    def remote_delete(
        self,
        batch=None,
        failure=None,
        params=None,
        success=None,
        api_version=None,
    ):
        """Deletes the object by calling the API with the DELETE http method.
        Args:
            batch (optional): A FacebookAdsApiBatch object. If specified,
                the call will be added to the batch.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
        Returns:
            self if not a batch call.
            the return value of batch.add if a batch call.
        """
        warning_message = "`remote_delete` is being deprecated, please update your code with new function."
        logging.warning(warning_message)
        if hasattr(self, 'api_delete'):
            request = self.api_delete(pending=True)
        else:
            request = FacebookRequest(
                node_id=self.get_id_assured(),
                method='DELETE',
                endpoint='/',
                api=self._api,
            )
        request.add_params(params)
        if batch is not None:
            def callback_success(response):
                self.clear_id()

                if success:
                    success(response)

            def callback_failure(response):
                if failure:
                    failure(response)

            batch_call = batch.add_request(
                request=request,
                success=callback_success,
                failure=callback_failure,
            )
            return batch_call
        else:
            request.execute()
            self.clear_id()

            return self

    # Helpers

    # @deprecated
    def remote_save(self, *args, **kwargs):
        """
        Calls remote_create method if object has not been created. Else, calls
        the remote_update method.
        """
        warning_message = "`remote_save` is being deprecated, please update your code with new function."
        logging.warning(warning_message)
        if self.get_id():
            return self.remote_update(*args, **kwargs)
        else:
            return self.remote_create(*args, **kwargs)

    def remote_archive(
        self,
        batch=None,
        failure=None,
        success=None
    ):
        if 'Status' not in dir(self) or 'archived' not in dir(self.Status):
            raise TypeError('Cannot archive object of type %s.'
                            % self.__class__.__name__)
        return self.api_create(
            params={
                'status': self.Status.archived,
            },
            batch=batch,
            failure=failure,
            success=success,
        )

    # @deprecated
    save = remote_save

    def iterate_edge(
        self,
        target_objects_class,
        fields=None,
        params=None,
        fetch_first_page=True,
        include_summary=True,
        endpoint=None
    ):
        """
        Returns Cursor with argument self as source_object and
        the rest as given __init__ arguments.
        Note: list(iterate_edge(...)) can prefetch all the objects.
        """
        source_object = self
        cursor = Cursor(
            source_object,
            target_objects_class,
            fields=fields,
            params=params,
            include_summary=include_summary,
            endpoint=endpoint,
        )
        if fetch_first_page:
            cursor.load_next_page()
        return cursor

    def iterate_edge_async(self, target_objects_class, fields=None,
                           params=None, is_async=False, include_summary=True,
                           endpoint=None):
        from facebook_business.adobjects.adreportrun import AdReportRun
        """
        Behaves as iterate_edge(...) if parameter is_async if False
        (Default value)
        If is_async is True:
        Returns an AsyncJob which can be checked using remote_read()
        to verify when the job is completed and the result ready to query
        or download using get_result()
        Example:
        >>> job = object.iterate_edge_async(
                TargetClass, fields, params, is_async=True)
        >>> time.sleep(10)
        >>> job.remote_read()
        >>> if job:
                result = job.read_result()
                print result
        """
        synchronous = not is_async
        synchronous_iterator = self.iterate_edge(
            target_objects_class,
            fields,
            params,
            fetch_first_page=synchronous,
            include_summary=include_summary,
        )
        if synchronous:
            return synchronous_iterator

        if not params:
            params = {}
        else:
            params = dict(params)
        self.__class__._assign_fields_to_params(fields, params)

        # To force an async response from an edge, do a POST instead of GET.
        # The response comes in the format of an AsyncJob which
        # indicates the progress of the async request.
        if endpoint is None:
            endpoint = target_objects_class.get_endpoint()
        response = self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), endpoint),
            params=params,
        ).json()

        # AsyncJob stores the real iterator
        # for when the result is ready to be queried
        result = AdReportRun()

        if 'report_run_id' in response:
            response['id'] = response['report_run_id']
        result._set_data(response)
        return result

    def edge_object(self, target_objects_class, fields=None, params=None, endpoint=None):
        """
        Returns first object when iterating over Cursor with argument
        self as source_object and the rest as given __init__ arguments.
        """
        params = {} if not params else params.copy()
        params['limit'] = '1'
        for obj in self.iterate_edge(
            target_objects_class,
            fields=fields,
            params=params,
            endpoint=endpoint,
        ):
            return obj

        # if nothing found, return None
        return None

    def assure_call(self):
        if not self._api:
            raise FacebookBadObjectError(
                'Api call cannot be made if api is not set')
