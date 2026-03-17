import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Services(Client):
    """
    Services SP-API Client
    :link: 

    With the Services API, you can build applications that help service providers get and modify their service orders.
    """


    @sp_endpoint('/service/v1/serviceJobs/{}', method='GET')
    def get_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse:
        """
        get_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse

        Gets service job details for the service job indicated by the service job identifier you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        20                                      40
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            serviceJobId:string | * REQUIRED A service job identifier.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), serviceJobId), params=kwargs)
    

    @sp_endpoint('/service/v1/serviceJobs/{}/cancellations', method='PUT')
    def cancel_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse:
        """
        cancel_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse

        Cancels the service job indicated by the service job identifier you specify.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       20
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            serviceJobId:string | * REQUIRED An Amazon defined service job identifier.
            key cancellationReasonCode:string | * REQUIRED A cancel reason code that specifies the reason for cancelling a service job.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), serviceJobId), data=kwargs)
    

    @sp_endpoint('/service/v1/serviceJobs/{}/completions', method='PUT')
    def complete_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse:
        """
        complete_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse

        Completes the service job indicated by the service job identifier you specify.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       20
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            serviceJobId:string | * REQUIRED An Amazon defined service job identifier.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), serviceJobId), data=kwargs)
    

    @sp_endpoint('/service/v1/serviceJobs', method='GET')
    def get_service_jobs(self, **kwargs) -> ApiResponse:
        """
        get_service_jobs(self, **kwargs) -> ApiResponse

        Gets service job details for the specified filter query.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      40
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key serviceOrderIds: array |  List of service order ids for the query you want to perform.Max values supported 20.
            key serviceJobStatus: array |  A list of one or more job status by which to filter the list of jobs.
            key pageToken: string |  String returned in the response of your previous request.
            key pageSize: integer |  A non-negative integer that indicates the maximum number of jobs to return in the list, Value must be 1 - 20. Default 20.
            key sortField: string |  Sort fields on which you want to sort the output.
            key sortOrder: string |  Sort order for the query you want to perform.
            key createdAfter: string |  A date used for selecting jobs created after (or at) a specified time must be in ISO 8601 format. Required if LastUpdatedAfter is not specified.Specifying both CreatedAfter and LastUpdatedAfter returns an error.
            key createdBefore: string |  A date used for selecting jobs created before (or at) a specified time must be in ISO 8601 format.
            key lastUpdatedAfter: string |  A date used for selecting jobs updated after (or at) a specified time must be in ISO 8601 format. Required if createdAfter is not specified.Specifying both CreatedAfter and LastUpdatedAfter returns an error.
            key lastUpdatedBefore: string |  A date used for selecting jobs updated before (or at) a specified time must be in ISO 8601 format.
            key scheduleStartDate: string |  A date used for filtering jobs schedule after (or at) a specified time must be in ISO 8601 format. schedule end date should not be earlier than schedule start date.
            key scheduleEndDate: string |  A date used for filtering jobs schedule before (or at) a specified time must be in ISO 8601 format. schedule end date should not be earlier than schedule start date.
            key marketplaceIds: array | * REQUIRED Used to select jobs that were placed in the specified marketplaces.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/service/v1/serviceJobs/{}/appointments', method='POST')
    def add_appointment_for_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse:
        """
        add_appointment_for_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse

        Adds an appointment to the service job indicated by the service job identifier you specify.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       20
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            serviceJobId:string | * REQUIRED An Amazon defined service job identifier.
            body: {
              "appointmentTime": {
                "startTime": "2019-08-24T14:15:22Z",
                "durationInMinutes": 0
              }
            }

         Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), serviceJobId), data=kwargs)
    

    @sp_endpoint('/service/v1/serviceJobs/{}', method='POST')
    def reschedule_appointment_for_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse:
        """
        reschedule_appointment_for_service_job_by_service_job_id(self, serviceJobId, **kwargs) -> ApiResponse

        Reschedules an appointment for the service job indicated by the service job identifier you specify.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       20
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            serviceJobId:string | * REQUIRED An Amazon defined service job identifier.
            appointmentId:string | * REQUIRED An existing appointment identifier for the Service Job.
            kwargs: Example | >>>
                {
                    "appointmentTime": {
                        "startTime": "2019-08-24T14:15:22Z",
                        "durationInMinutes": 0
                    },
                    "rescheduleReasonCode": "string"
                }

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), serviceJobId), data=kwargs)
    
