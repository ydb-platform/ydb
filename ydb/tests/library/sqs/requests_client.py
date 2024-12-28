#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
import logging
import urllib
import requests
import xmltodict
import six

from ydb.tests.library.common.helpers import wrap_in_list


logger = logging.getLogger(__name__)

DEFAULT_DATE = '20180101'
REQUEST_TIMEOUT = 180  # teamcity is very slow


def to_bytes(v):
    if v is None:
        return None

    if isinstance(v, bytes):
        return v

    return v.encode('utf-8')


def auth_string(user):
    return "AWS4-HMAC-SHA256 Credential={user}/{date}/yandex/sqs/aws4_request".format(
        user=user, date=DEFAULT_DATE
    )


def auth_headers(user, security_token=None, iam_token=None):
    headers = {
        'X-Amz-Date': '{}T104419Z'.format(DEFAULT_DATE),
        'Authorization': auth_string(user)
    }
    if security_token is not None:
        headers['X-Amz-Security-Token'] = security_token
    if iam_token is not None:
        headers['X-YaCloud-SubjectToken'] = iam_token

    return headers


SQS_ATTRIBUTE_TYPES = {'String': 'StringValue', 'Number': 'StringValue', 'Binary': 'BinaryValue'}


class SqsMessageAttribute(object):
    def __init__(self, name, attr_type, value):
        self.name = name
        self.value = value
        self.type = attr_type


class SqsSendMessageParams(object):
    def __init__(
            self, message_body,
            delay_seconds=None, attributes=None, deduplication_id=None, group_id=None
    ):
        self.message_body = message_body
        self.delay_seconds = delay_seconds
        self.attributes = attributes
        self.deduplication_id = deduplication_id
        self.group_id = group_id


class SqsChangeMessageVisibilityParams(object):
    def __init__(
            self,
            receipt_handle,
            visibility_timeout
    ):
        self.receipt_handle = receipt_handle
        self.visibility_timeout = visibility_timeout


class SqsHttpApi(object):
    def __init__(self, server, port, user, raise_on_error=False, timeout=REQUEST_TIMEOUT, security_token=None, force_private=False, iam_token=None, folder_id=None):
        self.__auth_headers = auth_headers(user, security_token, iam_token)
        if not server.startswith('http'):
            server = 'http://' + server
        self.__request = requests.Request(
            'POST',
            "{}:{}".format(server, port),
            headers=auth_headers(user, security_token, iam_token)
        )
        self.__private_request = requests.Request(
            'POST',
            "{}:{}/private".format(server, port),
            headers=auth_headers(user, security_token, iam_token)
        )
        self.__session = requests.Session()
        self.__raise_on_error = raise_on_error
        self.__user = user
        self.__timeout = timeout
        self.__security_token = security_token
        assert isinstance(force_private, bool)
        self.__force_private = force_private
        self.__folder_id = folder_id

    def execute_request(self, action, extract_result_method, default=None, private=False, **params):
        if params is None:
            params = {}
        params['Action'] = action

        if self.__folder_id is not None:
            params['folderId'] = self.__folder_id

        if self.__force_private or private:
            request = self.__private_request
        else:
            request = self.__request
        request.data = urllib.parse.urlencode(params)
        logger.debug("Execute request {} {} from user {} with params: {}".format(
            request.method, request.url, self.__user, request.data)
        )
        prep = request.prepare()
        try:
            response = self.__session.send(prep, timeout=self.__timeout)
        except (requests.ConnectionError, requests.exceptions.Timeout) as ex:
            logging.debug("Request failed with connection exception {}: {}".format(type(ex), ex))
            if self.__raise_on_error:
                raise
            else:
                response = None
        return self._process_response(
            response,
            extract_result_method, default
        )

    def _process_response(self, response, extract_method, default=None):
        if response is None:
            logging.debug('Returning {} by default'.format(default))
            return default
        if response.status_code != 200:
            logger.warn("Last request failed with code {}, reason '{}' and text '{}'".format(
                response.status_code, response.reason, response.text
            ))
            # Assert that no internal info will be given to user
            assert response.text.find('.cpp:') == -1, 'No internal info should be given to user'
            if self.__raise_on_error:
                raise RuntimeError(
                    "Request {} failed with status {} and text {}".format(
                        self.__request.data, response.status_code, response.text
                    )
                )
            return default

        logging.debug('Parsing response: {}'.format(response.text))
        result = xmltodict.parse(response.text)
        try:
            return extract_method(result)
        except (KeyError, TypeError) as ex:
            logger.error("Could not process response from SQS: {}. {}: {}".format(result, type(ex), ex))
            return default

    def create_user(self, username):
        return self.execute_request(
            action='CreateUser',
            extract_result_method=lambda x: x['CreateUserResponse']['ResponseMetadata'],
            UserName=username
        )

    def delete_user(self, username):
        return self.execute_request(
            action='DeleteUser',
            extract_result_method=lambda x: x['DeleteUserResponse']['ResponseMetadata'],
            UserName=username
        )

    def list_users(self):
        return self.execute_request(
            action='ListUsers',
            extract_result_method=lambda x: wrap_in_list(x['ListUsersResponse']['ListUsersResult']['UserName'])
        )

    def list_queues(self, name_prefix=None):
        if name_prefix is not None:
            return self.execute_request(
                action='ListQueues',
                extract_result_method=lambda x: wrap_in_list(x['ListQueuesResponse']['ListQueuesResult']['QueueUrl']), default=(),
                QueueNamePrefix=name_prefix
            )
        else:
            return self.execute_request(
                action='ListQueues',
                extract_result_method=lambda x: wrap_in_list(x['ListQueuesResponse']['ListQueuesResult']['QueueUrl']), default=(),
            )

    def private_count_queues(self):
        return self.execute_request(
            action='CountQueues',
            private=True,
            extract_result_method=lambda x: x['CountQueuesResponse']['CountQueuesResult']['Count'],
        )

    def create_queue(self, queue_name, is_fifo=False, attributes=None, private_api=False, created_timestamp_sec=None, custom_name=None, tags=None):
        # if is_fifo and not queue_name.endswith('.fifo'):
        #     return None
        if attributes is None:
            attributes = {}
        if tags is None:
            tags = {}
        if is_fifo:
            attributes = dict(attributes)  # copy
            attributes['FifoQueue'] = 'true'
        params = {}
        if created_timestamp_sec is not None:
            params['CreatedTimestamp'] = created_timestamp_sec
        if custom_name is not None:
            params['CustomQueueName'] = custom_name

        for i, (k, v) in enumerate(attributes.items()):
            params['Attribute.{id}.Name'.format(id=i+1)] = k
            params['Attribute.{id}.Value'.format(id=i + 1)] = v

        for i, (k, v) in enumerate(tags.items()):
            params['Tag.{id}.Key'.format(id=i+1)] = k
            params['Tag.{id}.Value'.format(id=i+1)] = v

        return self.execute_request(
            action='CreateQueue',
            private=private_api,
            extract_result_method=lambda x: x['CreateQueueResponse']['CreateQueueResult']['QueueUrl'],
            QueueName=queue_name,
            **params
        )

    def delete_queue(self, queue_url):
        return self.execute_request(
            action='DeleteQueue',
            extract_result_method=lambda x: x['DeleteQueueResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url
        )

    def private_delete_queue_batch(self, queue_urls):
        args = {}
        for i, url in enumerate(queue_urls):
            args["DeleteQueueBatchRequestEntry.{}.Id".format(i+1)] = i
            args["DeleteQueueBatchRequestEntry.{}.QueueUrl".format(i+1)] = url

        return self.execute_request(
            action='DeleteQueueBatch',
            private=True,
            extract_result_method=lambda x: x['DeleteQueueBatchResponse']['DeleteQueueBatchResult'],
            **args
        )

    def purge_queue(self, queue_url):
        return self.execute_request(
            action='PurgeQueue',
            extract_result_method=lambda x: x['PurgeQueueResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url
        )

    def private_purge_queue_batch(self, queue_urls):
        args = {}
        for i, url in enumerate(queue_urls):
            args["PurgeQueueBatchRequestEntry.{}.Id".format(i+1)] = i
            args["PurgeQueueBatchRequestEntry.{}.QueueUrl".format(i+1)] = url

        return self.execute_request(
            action='PurgeQueueBatch',
            private=True,
            extract_result_method=lambda x: x['PurgeQueueBatchResponse']['PurgeQueueBatchResult'],
            **args
        )

    def get_queue_url(self, queue_name):
        return self.execute_request(
            action='GetQueueUrl',
            extract_result_method=lambda x: x['GetQueueUrlResponse']['GetQueueUrlResult']['QueueUrl'],
            QueueName=queue_name
        )

    def list_dead_letter_source_queues(self, queue_url):
        return self.execute_request(
            action='ListDeadLetterSourceQueues',
            extract_result_method=lambda x: wrap_in_list(x['ListDeadLetterSourceQueuesResponse']['ListDeadLetterSourceQueuesResult']['QueueUrl']),
            QueueUrl=queue_url
        )

    def get_queue_attributes(self, queue_url, attributes=['All']):
        params = {}
        for i in six.moves.range(len(attributes)):
            params['AttributeName.{}'.format(i + 1)] = attributes[i]
        attrib_list = self.execute_request(
            action='GetQueueAttributes',
            extract_result_method=lambda x: x['GetQueueAttributesResponse']['GetQueueAttributesResult']['Attribute'],
            QueueUrl=queue_url,
            **params
        )
        result = {}
        if attrib_list is None:
            return result

        for attr in wrap_in_list(attrib_list):
            result[attr['Name']] = attr['Value']
        return result

    def private_get_queue_attributes_batch(self, queue_urls):
        args = {
            'AttributeName.1': 'All',
        }
        for i, url in enumerate(queue_urls):
            args["GetQueueAttributesBatchRequestEntry.{}.Id".format(i+1)] = i
            args["GetQueueAttributesBatchRequestEntry.{}.QueueUrl".format(i+1)] = url
        result_list = self.execute_request(
            action='GetQueueAttributesBatch',
            private=True,
            extract_result_method=lambda x: x['GetQueueAttributesBatchResponse']['GetQueueAttributesBatchResult'],
            **args
        )
        if 'GetQueueAttributesBatchResultEntry' in result_list:
            entries = result_list['GetQueueAttributesBatchResultEntry']
            for entry in wrap_in_list(entries):
                result = {}
                for attr in wrap_in_list(entry['Attribute']):
                    result[attr['Name']] = attr['Value']
                entry['__AttributesDict'] = result

        return result_list

    def modify_permissions(self, action, subject, path, permissions):
        args = {
            'Path': path,
            'Subject': subject
        }
        for i, permission in enumerate(permissions):
            args['Permission.' + str(i)] = permission

        return self.execute_request(
            action=action + 'Permissions',
            extract_result_method=lambda x: x['ModifyPermissionsResponse']['ResponseMetadata']['RequestId'],
            **args
        )

    def list_permissions(self, path):
        args = {
            'Path': path,
        }
        return self.execute_request(
            action='ListPermissions',
            extract_result_method=lambda x: x['ListPermissionsResponse']['YaListPermissionsResult'],
            **args
        )

    def set_queue_attributes(self, queue_url, attributes):
        params = {}
        i = 1
        for attr in attributes:
            params['Attribute.{}.Name'.format(i)] = attr
            params['Attribute.{}.Value'.format(i)] = attributes[attr]
            i += 1
        return self.execute_request(
            action='SetQueueAttributes',
            extract_result_method=lambda x: x['SetQueueAttributesResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url,
            **params
        )

    def send_message(
            self, queue_url, message_body,
            delay_seconds=None, attributes=None, deduplication_id=None, group_id=None
    ):
        if not to_bytes(queue_url).endswith(to_bytes('.fifo')):
            if deduplication_id is not None or group_id is not None:
                raise ValueError("Deduplication id and Group id parameters may be set for FIFO queues only")
        params = {
            'QueueUrl': queue_url,
            'MessageBody': message_body,
        }
        if delay_seconds is not None:
            params['DelaySeconds'] = str(delay_seconds)
        if deduplication_id is not None:
            params['MessageDeduplicationId'] = str(deduplication_id)
        if group_id is not None:
            params['MessageGroupId'] = str(group_id)

        if attributes is not None:
            attr_id_counter = itertools.count()
            for attr in attributes:
                if attr.type not in SQS_ATTRIBUTE_TYPES:
                    raise ValueError("Unknown attribute type: {}".format(attr.type))

                _id = next(attr_id_counter)
                params['MessageAttribute.{id}.Name'.format(id=_id)] = attr.name
                params['MessageAttribute.{id}.Value.DataType'.format(id=_id)] = attr.type
                params['MessageAttribute.{id}.Value.{value_type}'.format(
                    id=_id, value_type=SQS_ATTRIBUTE_TYPES[attr.type]
                )] = attr.value

        return self.execute_request(
            action='SendMessage',
            extract_result_method=lambda x: x['SendMessageResponse']['SendMessageResult']['MessageId'],
            **params
        )

    def send_message_batch(self, queue_url, send_message_params_list):
        params = {
            'QueueUrl': queue_url,
        }
        for i, entry in enumerate(send_message_params_list):
            params['SendMessageBatchRequestEntry.{i}.Id'.format(i=i+1)] = str(i)
            params['SendMessageBatchRequestEntry.{i}.MessageBody'.format(i=i+1)] = entry.message_body
            if entry.delay_seconds is not None:
                params['SendMessageBatchRequestEntry.{i}.DelaySeconds'.format(i=i+1)] = str(entry.delay_seconds)
            if entry.deduplication_id is not None:
                params['SendMessageBatchRequestEntry.{i}.MessageDeduplicationId'.format(i=i+1)] = str(entry.deduplication_id)
            if entry.group_id is not None:
                params['SendMessageBatchRequestEntry.{i}.MessageGroupId'.format(i=i+1)] = str(entry.group_id)

            if entry.attributes is not None:
                attr_id_counter = itertools.count()
                for attr in entry.attributes:
                    if attr.type not in SQS_ATTRIBUTE_TYPES:
                        raise ValueError("Unknown attribute type: {}".format(attr.type))

                    _id = next(attr_id_counter)
                    params['SendMessageBatchRequestEntry.{i}.MessageAttribute.{id}.Name'.format(i=i+1, id=_id)] = attr.name
                    params['SendMessageBatchRequestEntry.{i}.MessageAttribute.{id}.Value.DataType'.format(i=i+1, id=_id)] = attr.type
                    params['SendMessageBatchRequestEntry.{i}.MessageAttribute.{id}.Value.{value_type}'.format(
                        i=i+1, id=_id, value_type=SQS_ATTRIBUTE_TYPES[attr.type]
                    )] = attr.value

        resp = self.execute_request(
            action='SendMessageBatch',
            extract_result_method=lambda x: x['SendMessageBatchResponse'],
            **params
        )
        result = [dict() for i in six.moves.range(len(send_message_params_list))]
        results = resp.get('SendMessageBatchResult')
        logging.debug('results: {}'.format(results))
        if results:
            entries = results.get('SendMessageBatchResultEntry')
            logging.debug('entries: {}'.format(entries))
            if entries:
                for res in wrap_in_list(entries):
                    i = int(res['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['SendMessageBatchResultEntry'] = res

            errors = results.get('BatchResultErrorEntry')
            logging.debug('errors: {}'.format(errors))
            if errors:
                for err in wrap_in_list(errors):
                    i = int(err['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['BatchResultErrorEntry'] = err
        return result

    def receive_message(
            self, queue_url, max_number_of_messages=None, wait_timeout=None,
            visibility_timeout=None, meta_attributes=None, message_attributes=None,
            receive_request_attempt_id=None
    ):
        params = {'QueueUrl': queue_url}
        if max_number_of_messages is not None:
            params['MaxNumberOfMessages'] = max_number_of_messages
        if wait_timeout is not None:
            params['WaitTimeSeconds'] = wait_timeout
        if visibility_timeout is not None:
            params['VisibilityTimeout'] = visibility_timeout

        if meta_attributes is not None:
            attr_id_counter = itertools.count()
            for attr in meta_attributes:
                _id = next(attr_id_counter)
                params['AttributeName.{}'.format(_id)] = attr

        if message_attributes is not None:
            attr_id_counter = itertools.count()
            for attr in message_attributes:
                _id = next(attr_id_counter)
                params['MessageAttributeName.{}'.format(_id)] = attr

        if receive_request_attempt_id is not None:
            params['ReceiveRequestAttemptId'] = receive_request_attempt_id

        return self.execute_request(
            action='ReceiveMessage',
            extract_result_method=lambda x: wrap_in_list(
                x['ReceiveMessageResponse']['ReceiveMessageResult']['Message']
            ),
            **params
        )

    def delete_message(self, queue_url, handle):
        return self.execute_request(
            action='DeleteMessage',
            extract_result_method=lambda x: x['DeleteMessageResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url, ReceiptHandle=handle
        )

    def delete_message_batch(self, queue_url, message_handles):
        args = {}
        for i, handle in enumerate(message_handles):
            args["DeleteMessageBatchRequestEntry.{}.Id".format(i+1)] = str(i)
            args["DeleteMessageBatchRequestEntry.{}.ReceiptHandle".format(i+1)] = handle

        resp = self.execute_request(
            action='DeleteMessageBatch',
            extract_result_method=lambda x: x['DeleteMessageBatchResponse'],
            QueueUrl=queue_url, **args
        )
        result = [dict() for i in six.moves.range(len(message_handles))]
        results = resp.get('DeleteMessageBatchResult')
        logging.debug('results: {}'.format(results))
        if results:
            entries = results.get('DeleteMessageBatchResultEntry')
            logging.debug('entries: {}'.format(entries))
            if entries:
                for res in wrap_in_list(entries):
                    i = int(res['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['DeleteMessageBatchResultEntry'] = res

            errors = results.get('BatchResultErrorEntry')
            logging.debug('errors: {}'.format(errors))
            if errors:
                for err in wrap_in_list(errors):
                    i = int(err['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['BatchResultErrorEntry'] = err
        return result

    def change_message_visibility(self, queue_url, handle, visibility_timeout):
        return self.execute_request(
            action='ChangeMessageVisibility',
            extract_result_method=lambda x: x['ChangeMessageVisibilityResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url, ReceiptHandle=handle, VisibilityTimeout=visibility_timeout
        )

    def change_message_visibility_batch(self, queue_url, change_message_visibility_params_list):
        args = {}
        for i, params in enumerate(change_message_visibility_params_list):
            args["ChangeMessageVisibilityBatchRequestEntry.{}.Id".format(i+1)] = str(i)
            args["ChangeMessageVisibilityBatchRequestEntry.{}.ReceiptHandle".format(i+1)] = params.receipt_handle
            args["ChangeMessageVisibilityBatchRequestEntry.{}.VisibilityTimeout".format(i+1)] = params.visibility_timeout

        resp = self.execute_request(
            action='ChangeMessageVisibilityBatch',
            extract_result_method=lambda x: x['ChangeMessageVisibilityBatchResponse'],
            QueueUrl=queue_url, **args
        )
        result = [dict() for i in six.moves.range(len(change_message_visibility_params_list))]
        results = resp.get('ChangeMessageVisibilityBatchResult')
        logging.debug('results: {}'.format(results))
        if results:
            entries = results.get('ChangeMessageVisibilityBatchResultEntry')
            logging.debug('entries: {}'.format(entries))
            if entries:
                for res in wrap_in_list(entries):
                    i = int(res['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['ChangeMessageVisibilityBatchResultEntry'] = res

            errors = results.get('BatchResultErrorEntry')
            logging.debug('errors: {}'.format(errors))
            if errors:
                for err in wrap_in_list(errors):
                    i = int(err['Id'])
                    assert i < len(result) and i >= 0
                    result[i]['BatchResultErrorEntry'] = err
        return result

    def list_queue_tags(self, queue_url):
        tags = self.execute_request(
            action='ListQueueTags',
            extract_result_method=lambda x: x['ListQueueTagsResponse']['ListQueueTagsResult']['Tag'],
            QueueUrl=queue_url
        )

        return {} if tags is None else {
            tag['Key']: tag['Value']
            for tag in wrap_in_list(tags)
        }

    def tag_queue(self, queue_url, tags):
        params = {}
        for i, (k, v) in enumerate(tags.items(), 1):
            params['Tag.{}.Key'.format(i)] = k
            params['Tag.{}.Value'.format(i)] = v
        return self.execute_request(
            action='TagQueue',
            extract_result_method=lambda x: x['TagQueueResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url,
            **params
        )

    def untag_queue(self, queue_url, tag_keys):
        params = {}
        for i, k in enumerate(tag_keys, 1):
            params['TagKey.{}'.format(i)] = k
        return self.execute_request(
            action='UntagQueue',
            extract_result_method=lambda x: x['UntagQueueResponse']['ResponseMetadata']['RequestId'],
            QueueUrl=queue_url,
            **params
        )
