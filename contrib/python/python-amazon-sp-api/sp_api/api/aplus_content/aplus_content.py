import urllib.parse
from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class AplusContent(Client):
    """
    AplusContent SP-API Client
    :link: 

    With the A+ Content API, you can build applications that help selling partners add rich marketing content to their Amazon product detail pages. A+ content helps selling partners share their brand and product story, which helps buyers make informed purchasing decisions. Selling partners assemble content by choosing from content modules and adding images and text.
    """


    @sp_endpoint('/aplus/2020-11-01/contentDocuments', method='GET')
    def search_content_documents(self, **kwargs) -> ApiResponse:
        """
        search_content_documents(self, **kwargs) -> ApiResponse

        Returns a list of all A+ Content documents assigned to a selling partner. This operation returns only the metadata of the A+ Content documents. Call the getContentDocument operation to get the actual contents of the A+ Content documents.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            key pageToken:string |  A page token from the nextPageToken response element returned by your previous call to this operation. nextPageToken is returned when the results of a call exceed the page size. To get the next page of results, call the operation and include pageToken as the only parameter. Specifying pageToken with any other parameter will cause the request to fail. When no nextPageToken value is returned there are no more pages to return. A pageToken value is not usable across different operations.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments', method='POST')
    def create_content_document(self, **kwargs) -> ApiResponse:
        """
        create_content_document(self, **kwargs) -> ApiResponse

        Creates a new A+ Content document.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            postContentDocumentRequest: | * REQUIRED {'properties': {'contentDocument': {'$ref': '#/definitions/ContentDocument'}}, 'required': ['contentDocument'], 'type': 'object'}
        

        Returns:
            ApiResponse:
        """

        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, add_marketplace=False)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}', method='GET')
    def get_content_document(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        get_content_document(self, contentReferenceKey, **kwargs) -> ApiResponse

        Returns an A+ Content document, if available.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ Content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            key includedDataSet:array | * REQUIRED The set of A+ Content data types to include in the response.
        

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), params=kwargs)

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}', method='POST')
    def update_content_document(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        update_content_document(self, contentReferenceKey, **kwargs) -> ApiResponse

        Updates an existing A+ Content document.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ Content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            postContentDocumentRequest: | * REQUIRED {'properties': {'contentDocument': {'$ref': '#/definitions/ContentDocument'}}, 'required': ['contentDocument'], 'type': 'object'}

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), data=kwargs.pop("body"), params=kwargs, add_marketplace=False)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}/asins', method='GET')
    def list_content_document_asin_relations(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        list_content_document_asin_relations(self, contentReferenceKey, **kwargs) -> ApiResponse

        Returns a list of ASINs related to the specified A+ Content document, if available. If you do not include the asinSet parameter, the operation returns all ASINs related to the content document.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ Content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            key includedDataSet:array |  The set of A+ Content data types to include in the response. If you do not include this parameter, the operation returns the related ASINs without metadata.
            key asinSet:array |  The set of ASINs.
            key pageToken:string |  A page token from the nextPageToken response element returned by your previous call to this operation. nextPageToken is returned when the results of a call exceed the page size. To get the next page of results, call the operation and include pageToken as the only parameter. Specifying pageToken with any other parameter will cause the request to fail. When no nextPageToken value is returned there are no more pages to return. A pageToken value is not usable across different operations.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), params=kwargs)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}/asins', method='POST')
    def post_content_document_asin_relations(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        post_content_document_asin_relations(self, contentReferenceKey, **kwargs) -> ApiResponse

        Replaces all ASINs related to the specified A+ Content document, if available. This may add or remove ASINs, depending on the current set of related ASINs. Removing an ASIN has the side effect of suspending the content document from that ASIN.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            postContentDocumentAsinRelationsRequest: | * REQUIRED {'properties': {'asinSet': {'$ref': '#/definitions/AsinSet'}}, 'required': ['asinSet'], 'type': 'object'}

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), data=kwargs.pop("body"), params=kwargs, add_marketplace=False)
    

    @sp_endpoint('/aplus/2020-11-01/contentAsinValidations', method='POST')
    def validate_content_document_asin_relations(self, **kwargs) -> ApiResponse:
        """
        validate_content_document_asin_relations(self, **kwargs) -> ApiResponse

        Checks if the A+ Content document is valid for use on a set of ASINs.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            key asinSet:array |  The set of ASINs.
            postContentDocumentRequest: | * REQUIRED {'properties': {'contentDocument': {'$ref': '#/definitions/ContentDocument'}}, 'required': ['contentDocument'], 'type': 'object'}
        

        Returns:
            ApiResponse:
        """

        return self._request(kwargs.pop('path'), data=kwargs.pop("body"), params=kwargs, add_marketplace=False)
    

    @sp_endpoint('/aplus/2020-11-01/contentPublishRecords', method='GET')
    def search_content_publish_records(self, **kwargs) -> ApiResponse:
        """
        search_content_publish_records(self, **kwargs) -> ApiResponse

        Searches for A+ Content publishing records, if available.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.
            key asin:string | * REQUIRED The Amazon Standard Identification Number (ASIN).
            key pageToken:string |  A page token from the nextPageToken response element returned by your previous call to this operation. nextPageToken is returned when the results of a call exceed the page size. To get the next page of results, call the operation and include pageToken as the only parameter. Specifying pageToken with any other parameter will cause the request to fail. When no nextPageToken value is returned there are no more pages to return. A pageToken value is not usable across different operations.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'), params=kwargs)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}/approvalSubmissions', method='POST')
    def post_content_document_approval_submission(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        post_content_document_approval_submission(self, contentReferenceKey, **kwargs) -> ApiResponse

        Submits an A+ Content document for review, approval, and publishing.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), params=kwargs)
    

    @sp_endpoint('/aplus/2020-11-01/contentDocuments/{}/suspendSubmissions', method='POST')
    def post_content_document_suspend_submission(self, contentReferenceKey, **kwargs) -> ApiResponse:
        """
        post_content_document_suspend_submission(self, contentReferenceKey, **kwargs) -> ApiResponse

        Submits a request to suspend visible A+ Content. This neither deletes the content document nor the ASIN relations.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            contentReferenceKey:string | * REQUIRED The unique reference key for the A+ Content document. A content reference key cannot form a permalink and may change in the future. A content reference key is not guaranteed to match any A+ content identifier.
            key marketplaceId:string | * REQUIRED The identifier for the marketplace where the A+ Content is published.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), contentReferenceKey), data=kwargs)
