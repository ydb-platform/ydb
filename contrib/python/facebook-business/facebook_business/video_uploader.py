# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

"""
video uploader that is used to upload video to adaccount
"""

from facebook_business.exceptions import FacebookError
from facebook_business.exceptions import FacebookRequestError
from abc import ABCMeta, abstractmethod

import os
import ntpath
import time


class VideoUploader(object):
    """
    Video Uploader that can upload videos to adaccount
    """

    def __init__(self):
        self._session = None

    def upload(self, video, wait_for_encoding=False):
        """
        Upload the given video file.
        Args:
            video(required): The AdVideo object that will be uploaded
            wait_for_encoding: Whether to wait until encoding is finished.
        """
        # Check there is no existing session
        if self._session:
            raise FacebookError(
                "There is already an upload session for this video uploader",
            )

        # Initiate an upload session
        self._session = VideoUploadSession(video, wait_for_encoding)
        result = self._session.start()
        self._session = None
        return result


class VideoUploadSession(object):

    def __init__(self, video, wait_for_encoding=False, interval=3, timeout=180):
        self._video = video
        self._api = video.get_api_assured()
        if (video.Field.filepath in video):
            self._file_path = video[video.Field.filepath]
            self._slideshow_spec = None
        elif (video.Field.slideshow_spec in video):
            self._slideshow_spec = video[video.Field.slideshow_spec]
            self._file_path = None
        self._account_id = video.get_parent_id_assured()
        self._wait_for_encoding = wait_for_encoding
        # Setup start request manager
        self._start_request_manager = VideoUploadStartRequestManager(
            self._api,
        )

        # Setup transfer request manager
        self._transfer_request_manager = VideoUploadTransferRequestManager(
            self._api,
        )

        # Setup finish request manager
        self._finish_request_manager = VideoUploadFinishRequestManager(
            self._api,
        )
        self._timeout = timeout
        self._interval = interval

    def start(self):
        # Run start request manager
        start_response = self._start_request_manager.send_request(
            self.getStartRequestContext(),
        ).json()
        self._start_offset = int(start_response['start_offset'])
        self._end_offset = int(start_response['end_offset'])
        self._session_id = start_response['upload_session_id']
        video_id = start_response['video_id']

        # Run transfer request manager
        self._transfer_request_manager.send_request(
            self.getTransferRequestContext(),
        )

        # Run finish request manager
        response = self._finish_request_manager.send_request(
            self.getFinishRequestContext(),
        )

        if self._wait_for_encoding:
            VideoEncodingStatusChecker.waitUntilReady(
                self._api, video_id, interval=self._interval, timeout=self._timeout
            )

        # Populate the video info
        body = response.json().copy()
        body['id'] = video_id
        del body['success']

        return body

    def getStartRequestContext(self):
        context = VideoUploadRequestContext()
        if (self._file_path):
            context.file_size = os.path.getsize(self._file_path)
        context.account_id = self._account_id
        return context

    def getTransferRequestContext(self):
        context = VideoUploadRequestContext()
        context.session_id = self._session_id
        context.start_offset = self._start_offset
        context.end_offset = self._end_offset
        if (self._file_path):
            context.file_path = self._file_path
        if (self._slideshow_spec):
            context.slideshow_spec = self._slideshow_spec
        context.account_id = self._account_id
        return context

    def getFinishRequestContext(self):
        context = VideoUploadRequestContext()
        context.session_id = self._session_id
        context.account_id = self._account_id
        if (self._file_path):
            context.file_name = ntpath.basename(self._file_path)
        return context


class VideoUploadRequestManager(object):
    """
    Abstract class for request managers
    """
    __metaclass__ = ABCMeta

    def __init__(self, api):
        self._api = api

    @abstractmethod
    def send_request(self, context):
        """
        send upload request
        """
        pass

    @abstractmethod
    def getParamsFromContext(self, context):
        """
        get upload params from context
        """
        pass


class VideoUploadStartRequestManager(VideoUploadRequestManager):

    def send_request(self, context):
        """
        send start request with the given context
        """
        # Init a VideoUploadRequest and send the request
        request = VideoUploadRequest(self._api)
        request.setParams(self.getParamsFromContext(context))
        return request.send((context.account_id, 'advideos'))

    def getParamsFromContext(self, context):
        return {
            'file_size': context.file_size,
            'upload_phase': 'start',
        }


class VideoUploadTransferRequestManager(VideoUploadRequestManager):

    def send_request(self, context):
        """
        send transfer request with the given context
        """
        # Init a VideoUploadRequest
        request = VideoUploadRequest(self._api)
        self._start_offset = context.start_offset
        self._end_offset = context.end_offset
        filepath = context.file_path
        file_size = os.path.getsize(filepath)
        # Give a chance to retry every 10M, or at least twice
        retry = max(file_size / (1024 * 1024 * 10), 2)
        f = open(filepath, 'rb')
        # While the there are still more chunks to send
        while self._start_offset != self._end_offset:
            # Read a chunk of file
            f.seek(self._start_offset)
            chunk = f.read(self._end_offset - self._start_offset)
            context.start_offset = self._start_offset
            context.end_offset = self._end_offset
            # Parse the context
            request.setParams(
                self.getParamsFromContext(context),
                {'video_file_chunk': (
                    os.path.basename(context.file_path),
                    chunk,
                    'multipart/form-data',
                )},
            )
            # send the request
            try:
                response = request.send(
                    (context.account_id, 'advideos'),
                ).json()
                self._start_offset = int(response['start_offset'])
                self._end_offset = int(response['end_offset'])
            except FacebookRequestError as e:
                subcode = e.api_error_subcode()
                body = e.body()
                if subcode == 1363037:
                    # existing issue, try again immediately
                    if (body and 'error' in body and
                            'error_data' in body['error'] and
                            'start_offset' in body['error']['error_data'] and
                            retry > 0):
                        self._start_offset = int(
                            body['error']['error_data']['start_offset'],
                        )
                        self._end_offset = int(
                            body['error']['error_data']['end_offset'],
                        )
                        retry = max(retry - 1, 0)
                        continue
                elif ('error' in body and
                        'is_transient' in body['error']):
                    if body['error']['is_transient']:
                        time.sleep(1)
                        continue
                f.close()
                raise e

        f.close()
        return response

    def getParamsFromContext(self, context):
        return {
            'upload_phase': 'transfer',
            'start_offset': context.start_offset,
            'upload_session_id': context.session_id,
        }


class VideoUploadFinishRequestManager(VideoUploadRequestManager):

    def send_request(self, context):
        """
        send transfer request with the given context
        """
        # Init a VideoUploadRequest
        request = VideoUploadRequest(self._api)

        # Parse the context
        request.setParams(self.getParamsFromContext(context))

        # send the request
        return request.send((context.account_id, 'advideos'))

    def getParamsFromContext(self, context):
        return {
            'upload_phase': 'finish',
            'upload_session_id': context.session_id,
            'title': context.file_name,
        }


class VideoUploadRequestContext(object):
    """
    Upload request context that contains the param data
    """

    @property
    def account_id(self):
        return self._account_id

    @account_id.setter
    def account_id(self, account_id):
        self._account_id = account_id

    @property
    def file_name(self):
        return self._name

    @file_name.setter
    def file_name(self, name):
        self._name = name

    @property
    def file_size(self):
        return self._size

    @file_size.setter
    def file_size(self, size):
        self._size = size

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, session_id):
        self._session_id = session_id

    @property
    def start_offset(self):
        return self._start_offset

    @start_offset.setter
    def start_offset(self, start_offset):
        self._start_offset = start_offset

    @property
    def end_offset(self):
        return self._end_offset

    @end_offset.setter
    def end_offset(self, end_offset):
        self._end_offset = end_offset

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, file):
        self._file = file

    @property
    def file_path(self):
        return self._filepath

    @file_path.setter
    def file_path(self, filepath):
        self._filepath = filepath


class VideoUploadRequest(object):

    def __init__(self, api):
        self._params = None
        self._files = None
        self._api = api

    def send(self, path):
        """
        send the current request
        """
        return self._api.call(
            'POST',
            path,
            params=self._params,
            files=self._files,
            url_override='https://graph-video.facebook.com',
        )

    def setParams(self, params, files=None):
        self._params = params
        self._files = files


class VideoEncodingStatusChecker(object):

    @staticmethod
    def waitUntilReady(api, video_id, interval, timeout):
        start_time = time.time()
        while True:
            status = VideoEncodingStatusChecker.getStatus(api, video_id)
            status = status['video_status']
            if status != 'processing':
                break
            if start_time + timeout <= time.time():
                raise FacebookError('video encoding timeout: ' + str(timeout))
            time.sleep(interval)
        if status != 'ready':
            raise FacebookError(
                'video encoding status: ' + status,
            )
        return

    @staticmethod
    def getStatus(api, video_id):
        result = api.call(
            'GET',
            [int(video_id)],
            params={'fields': 'status'},
        ).json()
        return result['status']
