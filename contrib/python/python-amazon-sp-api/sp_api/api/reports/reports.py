import zlib
from collections import abc
from datetime import datetime
from io import BytesIO, StringIO
from typing import Optional

import requests

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Marketplaces


class Reports(Client):
    """
    Reports SP-API Client
    :link:

    The Selling Partner API for Reports lets you retrieve and manage a variety of reports that can help selling partners manage their businesses.
    """

    @sp_endpoint('/reports/2021-06-30/reports', method='GET')
    def get_reports(self, **kwargs) -> ApiResponse:
        """
        get_reports(self, **kwargs) -> ApiResponse

        Returns report details for the reports that match the filters that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                report_types = ["FEE_DISCOUNTS_REPORT", "GET_AFN_INVENTORY_DATA"]
                processing_status = ["IN_QUEUE", "IN_PROGRESS"]
                res = Reports().get_reports(reportTypes=report_types, processingStatuses=processing_status)

        Args:
            key reportTypes: str[] or ReportType[] | optional  A list of report types used to filter reports. When reportTypes is provided, the other filter parameters (processingStatuses, marketplaceIds, createdSince, createdUntil) and pageSize may also be provided. Either reportTypes or nextToken is required.
            key processingStatuses: str[] or ProcessingStatus[] optional	A list of processing statuses used to filter reports.
            key marketplaceIds: str[] or Marketplaces[] optional	A list of marketplace identifiers used to filter reports. The reports returned will match at least one of the marketplaces that you specify.
            key pageSize: int optional	The maximum number of reports to return in a single call.
            key createdSince: str or datetime optional	The earliest report creation date and time for reports to include in the response, in ISO 8601 date time format. The default is 90 days ago. Reports are retained for a maximum of 90 days.	string (date-time)	-
            key	createdUntil: str or datetime optional	The latest report creation date and time for reports to include in the response, in ISO 8601 date time format. The default is now.	string (date-time)	-
            key nextToken: str optional	A stringget_report_document token returned in the response to your previous request. nextToken is returned when the number of results exceeds the specified pageSize value. To get the next page of results, call the getReports operation and include this token as the only parameter. Specifying nextToken with any other parameters will cause the request to fail.	string	-


        Returns:
            ApiResponse
        """
        if kwargs.get('reportTypes', None) and isinstance(kwargs.get('reportTypes'), abc.Iterable):
            kwargs.update({'reportTypes': ','.join(kwargs.get('reportTypes'))})
        if kwargs.get('processingStatuses', None) and isinstance(kwargs.get('processingStatuses'), abc.Iterable):
            kwargs.update({'processingStatuses': ','.join(kwargs.get('processingStatuses'))})
        if kwargs.get('marketplaceIds', None) and isinstance(kwargs.get('marketplaceIds'), abc.Iterable):
            marketplaces = kwargs.get('marketplaceIds')
            kwargs.update({'marketplaceIds': ','.join(
                [m.marketplace_id if isinstance(m, Marketplaces) else m for m in marketplaces])})
        for k in ['createdSince', 'createdUntil']:
            if kwargs.get(k, None) and isinstance(kwargs.get(k), datetime):
                kwargs.update({k: kwargs.get(k).isoformat()})
        if not kwargs.get('nextToken'):
            return self._request(kwargs.pop('path'), params=kwargs)
        return self._request(kwargs.pop('path'), params=kwargs, add_marketplace=False)

    @sp_endpoint('/reports/2021-06-30/reports', method='POST')
    def create_report(self, **kwargs) -> ApiResponse:
        """
        create_report(self, **kwargs) -> ApiResponse

        See report types at
        :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/reports-api/reporttype-values.md

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0167                                  15
        ======================================  ==============

        Examples:
            literal blocks::

                res = Reports().create_report(
                    reportType=ReportType.GET_MERCHANT_LISTINGS_ALL_DATA,
                    dataStartTime='2019-12-10T20:11:24.000Z',
                    marketplaceIds=[
                        "A1PA6795UKMFR9",
                        "ATVPDKIKX0DER"
                    ])

        Args:
            key reportOptions: optional	Additional information passed to reports. This varies by report type.	ReportOptions
            key reportType: required	The report type. :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/reports-api/reporttype-values.md
            key dataStartTime: optional	The start of a date and time range, in ISO 8601 date time format, used for selecting the data to report. The default is now. The value must be prior to or equal to the current date and time. Not all report types make use of this.	string (date-time)
            key dataEndTime: optional	The end of a date and time range, in ISO 8601 date time format, used for selecting the data to report. The default is now. The value must be prior to or equal to the current date and time. Not all report types make use of this.	string (date-time)
            key marketplaceIds: optional, defaults to the client's marketplace  A list of marketplace identifiers. The report document's contents will contain data for all of the specified marketplaces, unless the report type indicates otherwise.	< string > array

        Returns:
            ApiResponse
        """
        if isinstance(kwargs.get('dataStartTime', None), datetime):
            kwargs.update({'dataStartTime': kwargs.get('dataStartTime').isoformat()})
        if isinstance(kwargs.get('dataEndTime', None), datetime):
            kwargs.update({'dataEndTime': kwargs.get('dataEndTime').isoformat()})
        return self._request(kwargs.pop('path'), data=kwargs)

    @sp_endpoint('/reports/2021-06-30/reports/{}', method='DELETE')
    def cancel_report(self, reportId, **kwargs) -> ApiResponse:
        """
        cancel_report(self, reportId, **kwargs) -> ApiResponse

        Cancels the report that you specify. Only reports with processingStatus=IN_QUEUE can be cancelled. Cancelled reports are returned in subsequent calls to the getReport and getReports operations.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0022                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            reportId:string | * REQUIRED The identifier for the report. This identifier is unique only in combination with a seller ID.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), reportId), data=kwargs)

    @sp_endpoint('/reports/2021-06-30/reports/{}', method='GET')
    def get_report(self, reportId, **kwargs) -> ApiResponse:
        """
        get_report(self, report_id, **kwargs)
        Returns report details (including the reportDocumentId, if available) for the report that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       15
        ======================================  ==============


        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().get_report('ID323')

        Args:
            reportId: str

        Returns:
            ApiResponse

        """

        return self._request(fill_query_params(kwargs.pop('path'), reportId), params=kwargs)

    @sp_endpoint('/reports/2021-06-30/schedules', method='GET')
    def get_report_schedules(self, **kwargs) -> ApiResponse:
        """
        Returns report schedule details that match the filters that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key reportTypes: str[] or ReportType[] | required  A list of report types used to filter report schedules. Min count : 1. Max count : 10.

        Returns:
            ApiResponse
        """
        if kwargs.get('reportTypes', None) and isinstance(kwargs.get('reportTypes'), abc.Iterable):
            kwargs.update({'reportTypes': ','.join(kwargs.get('reportTypes'))})

        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/reports/2021-06-30/schedules', method='POST')
    def create_report_schedule(self, **kwargs) -> ApiResponse:
        """
        create_report_schedule(self, **kwargs) -> ApiResponse

        Creates a report schedule. If a report schedule with the same report type and marketplace IDs already exists, it will be cancelled and replaced with this one.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        Examples:
            literal blocks::

                Reports().create_report_schedule(reportType='FEE_DISCOUNTS_REPORT',
                                           period=Schedules.MINUTES_5.value,
                                           nextReportCreationTime="2019-12-10T20:11:24.000Z",
                                           marketplaceIds=["A1PA6795UKMFR9", "ATVPDKIKX0DER"])

        Args:
            key reportType: str
            key marketplaceIds: str
            key reportOptions: dict
            key period: Schedules
            key nextReportCreationTime: str datetime isoformat

        Returns:
            ApiResponse:
        """

        return self._request(kwargs.pop('path'), data=kwargs)

    @sp_endpoint('/reports/2021-06-30/schedules/{}', method='DELETE')
    def cancel_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse:
        """
        cancel_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse

        Cancels the report schedule that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().cancel_report_schedule('ID')

        Args:
            reportScheduleId: str
            kwargs:

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportScheduleId), data=kwargs)

    def delete_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse:
        """
        cancel_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse

        Cancels the report schedule that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().cancel_report_schedule('ID')

        Args:
            reportScheduleId: str
            kwargs:

        Returns:
            ApiResponse
        """
        return self.cancel_report_schedule(reportScheduleId)

    @sp_endpoint('/reports/2021-06-30/schedules/{}', method='GET')
    def get_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse:
        """
        get_report_schedule(self, reportScheduleId, **kwargs) -> ApiResponse

        Returns report schedule details for the report schedule that you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0222                                  10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().get_report_schedule('ID323')

        Args:
            reportScheduleId: str | required The identifier for the report schedule. This identifier is unique only in combination with a seller ID.
            kwargs:

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportScheduleId), params=kwargs)

    @sp_endpoint('/reports/2021-06-30/documents/{}', method='GET')
    def get_report_document(self, reportDocumentId, download: bool = False, file=None,
                            character_code: Optional[str] = None, **kwargs) -> ApiResponse:
        """
        get_report_document(self, document_id, decrypt: bool = False, file=None, character_code: Optional[str] = None, **kwargs) -> ApiResponse
        Returns the information required for retrieving a report document's contents. This includes a presigned URL for the report document as well as the information required to decrypt the document's contents.

        If decrypt = True the report will automatically be loaded and decrypted/unpacked
        If file is set to a file (or file like object), the report's contents are written to the file


        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0167                                  15
        ======================================  ==============



        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().get_report_document('0356cf79-b8b0-4226-b4b9-0ee058ea5760', download=True, file=file)

        Args:
            reportDocumentId: str | the document to load
            download: bool | flag to automatically download a report
            file: If passed, will save the document to the file specified.
                  Only valid if decrypt=True
            character_code: If passed, will be a file with the specified character code.
                            The default is the Content-Encoding in the response while
                            obtaining the document from the document URL.
                            It fallbacks to 'iso-8859-1' if no encoding was found.
                            Only valid if decrypt=True.

        Returns:
             ApiResponse
        """  # noqa: E501
        res = self._request(fill_query_params(kwargs.pop(
            'path'), reportDocumentId), add_marketplace=False)
        if download or file or ('decrypt' in kwargs and kwargs['decrypt']):
            document_response = requests.get(
                res.payload.get('url'),
                proxies=self.proxies,
                verify=self.verify,
            )
            document = document_response.content
            if not character_code:
                character_code = (
                    document_response.encoding
                    if document_response and document_response.encoding
                    else 'iso-8859-1'
                )
                if character_code.lower() == 'windows-31j':
                    character_code = 'cp932'
            if 'compressionAlgorithm' in res.payload:
                try:
                    document = zlib.decompress(bytearray(document), 15 + 32)
                except Exception as e:
                    pass

            if character_code:
                try:
                    decoded_document = document.decode(character_code)
                except Exception as e:
                    decoded_document = document

            if download:
                res.payload.update({
                    'document': decoded_document,
                })
            if file:
                self._handle_file(file, decoded_document, character_code)
        return res

    @staticmethod
    def _handle_file(file, document, encoding):
        if isinstance(file, str):
            with open(file, "w+", encoding=encoding) as text_file:
                text_file.write(document)
        elif isinstance(file, BytesIO):
            file.write(document.encode(encoding))
            file.seek(0)
        elif isinstance(file, StringIO):
            file.write(document)
            file.seek(0)
        else:
            file.write(document)
