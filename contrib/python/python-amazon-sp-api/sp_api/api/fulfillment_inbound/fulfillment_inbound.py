from sp_api.base import Client, Marketplaces, ApiResponse
from sp_api.base import sp_endpoint, fill_query_params


class FulfillmentInbound(Client):
    @sp_endpoint("/fba/inbound/v0/itemsGuidance")
    def item_guidance(self, **kwargs):
        """
        item_guidance(self, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().item_guidance(**{"MarkeplaceId": "US", "ASINList": ["ASIN1"]})

        Args:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop("path"), params=kwargs)

    @sp_endpoint("/fba/inbound/v0/plans", method="POST")
    def plans(self, data, **kwargs):
        """
        plans(self, data, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                res = FulfillmentInbound().plans({
                        "ShipFromAddress": {
                            "Name": "Name",
                            "AddressLine1": "123 any st",
                            "AddressLine2": "AddressLine2",
                            "DistrictOrCounty": "Washtenaw",
                            "City": "Ann Arbor",
                            "StateOrProvinceCode": "MI",
                            "CountryCode": "US",
                            "PostalCode": "48188"
                        },
                        "LabelPrepPreference": "SELLER_LABEL",
                        "ShipToCountryCode": "ShipToCountryCode",
                        "ShipToCountrySubdivisionCode": "ShipToCountrySubdivisionCode",
                        "InboundShipmentPlanRequestItems": [
                            {
                                "SellerSKU": "SellerSKU",
                                "ASIN": "ASIN",
                                "Condition": "NewItem",
                                "Quantity": 1,
                                "QuantityInCase": 1,
                                "PrepDetailsList": [
                                    {
                                        "PrepInstruction": "Polybagging",
                                        "PrepOwner": "AMAZON"
                                    }
                                ]
                            }
                        ]
                    })

        Args:
            data:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop("path"), data={**data, **kwargs})

    @sp_endpoint("/fba/inbound/v0/shipments/{}", method="POST")
    def create_shipment(self, shipment_id, data, **kwargs):
        """
        create_shipment(self, shipment_id, data, **kwargs)

        Examples:
            literal blocks::

                FulfillmentInbound().create_shipment('123', {
                        "InboundShipmentHeader": {
                            "ShipmentName": "43545345",
                            "ShipFromAddress": {
                                "Name": "35435345",
                                "AddressLine1": "123 any st",
                                "DistrictOrCounty": "Washtenaw",
                                "City": "Ann Arbor",
                                "StateOrProvinceCode": "Test",
                                "CountryCode": "US",
                                "PostalCode": "48103"
                            },
                            "DestinationFulfillmentCenterId": "AEB2",
                            "AreCasesRequired": True,
                            "ShipmentStatus": "WORKING",
                            "LabelPrepPreference": "SELLER_LABEL",
                            "IntendedBoxContentsSource": "NONE"
                        },
                        "InboundShipmentItems": [
                            {
                                "ShipmentId": "345453",
                                "SellerSKU": "34534545",
                                "FulfillmentNetworkSKU": "435435435",
                                "QuantityShipped": 0,
                                "QuantityReceived": 0,
                                "QuantityInCase": 0,
                                "ReleaseDate": "2020-04-23",
                                "PrepDetailsList": [
                                    {
                                        "PrepInstruction": "Polybagging",
                                        "PrepOwner": "AMAZON"
                                    }
                                ]
                            }
                        ],
                        "MarketplaceId": "MarketplaceId"
                    })

        Args:
            shipment_id:
            data:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), data={**data, **kwargs}
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}", method="PUT")
    def update_shipment(self, shipment_id, data, **kwargs):
        """
        update_shipment(self, shipment_id, data, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().update_shipment('123', {
                        "MarketplaceId": "ATVPDKIKX0DER",
                        "InboundShipmentHeader": {
                            "ShipmentName": "Shipment for FBA15DJCQ1ZF",
                            "ShipFromAddress": {
                                "Name": "Uma Test",
                                "AddressLine1": "123 any st",
                                "AddressLine2": "",
                                "DistrictOrCounty": "Washtenaw",
                                "City": "Ann Arbor",
                                "StateOrProvinceCode": "CO",
                                "CountryCode": "US",
                                "PostalCode": "48104"
                            },
                            "DestinationFulfillmentCenterId": "ABE2",
                            "ShipmentStatus": "WORKING",
                            "LabelPrepPreference": "SELLER_LABEL"
                        },
                        "InboundShipmentItems": [
                            {
                                "SellerSKU": "PSMM-TEST-SKU-Apr-03_21_17_20-0379",
                                "QuantityShipped": 1
                            }
                        ]
                    })

        Args:
            shipment_id:
            data:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), data={**data, **kwargs}
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/preorder")
    def preorder(self, shipment_id, **kwargs):
        """
        preorder(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().preorder('shipmentId1', MarketplaceId='MarketplaceId1')

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), params=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/preorder/confirm", method="PUT")
    def confirm_preorder(self, shipment_id, **kwargs):
        """
        confirm_preorder(self, shipment_id, **kwargs)

        Args:
            shipment_id:
            **kwargs:

        Returns:

        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), params=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/prepInstructions")
    def prep_instruction(self, data, **kwargs):
        """
        prep_instruction(self, data, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().prep_instruction({"ShipToCountryCode": "US", "ASINList": ["ASIN1"]})

        Args:
            data:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop("path"), params={**data, **kwargs})

    @sp_endpoint("/fba/inbound/v0/shipments/{}/transport")
    def get_transport_information(self, shipment_id, **kwargs):
        """
        get_transport_information(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().get_transport_information('shipmentId1')

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), params=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/transport", method="PUT")
    def update_transport_information(self, shipment_id, **kwargs):
        """
        update_transport_information(self, shipment_id, **kwargs) -> ApiResponse

        putTransportDetails

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), data=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/transport/void", method="POST")
    def void_transport(self, shipment_id, **kwargs):
        """
        void_transport(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().void_transport('shipmentId1')

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id),
            data=kwargs,
            add_marketplace=False,
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/transport/estimate", method="POST")
    def estimate_transport(self, shipment_id, **kwargs):
        """
        estimate_transport(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().estimate_transport('shipmentId1')

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id),
            data=kwargs,
            add_marketplace=False,
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/transport/confirm", method="POST")
    def confirm_transport(self, shipment_id, **kwargs):
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id),
            data=kwargs,
            add_marketplace=False,
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/labels")
    def get_labels(self, shipment_id, **kwargs):
        """
        get_labels(self, shipment_id, **kwargs)

        Args:
            shipment_id:
            **kwargs:

        Returns:

        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id),
            params=kwargs,
            add_marketplace=False,
        )

    @sp_endpoint("/fba/inbound/v0/shipments/{}/billOfLading")
    def bill_of_lading(self, shipment_id, **kwargs):
        """
        bill_of_lading(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().bill_of_lading('shipmentId')

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), params=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/shipments")
    def get_shipments(self, **kwargs):
        """
        get_shipments(self, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().get_shipments(QueryType='SHIPMENT', MarketplaceId="ATVPDKIKX0DER")

        Args:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop("path"), params=kwargs)

    @sp_endpoint("/fba/inbound/v0/shipments/{}/items")
    def shipment_items_by_shipment(self, shipment_id, **kwargs):
        """
        shipment_items_by_shipment(self, shipment_id, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().shipment_items_by_shipment('FBA15DJ9SVVD', MarketplaceId="ATVPDKIKX0DER")

        Args:
            shipment_id:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(
            fill_query_params(kwargs.pop("path"), shipment_id), params=kwargs
        )

    @sp_endpoint("/fba/inbound/v0/shipmentItems")
    def shipment_items(self, **kwargs):
        """
        shipment_items(self, **kwargs) -> ApiResponse

        Examples:
            literal blocks::

                FulfillmentInbound().shipment_items(QueryType='SHIPMENT', MarketplaceId="ATVPDKIKX0DER", NextToken='NextToken')

        Args:
            **kwargs:

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop("path"), params=kwargs)

    def get_shipments_by_id(self, shipment_id_list, **kwargs) -> ApiResponse:
        """
        get_shipments_by_id(self, shipment_id_list, **kwargs) -> ApiResponse

            FulfillmentInbound().get_shipments_by_id('FBA16TBYQ6J6')
        Args:
            shipment_id_list: str or [str]
            **kwargs:

        Returns:
            ApiResponse
        """
        if not isinstance(shipment_id_list, str):
            shipment_id_list = ','.join(shipment_id_list)
        return self.get_shipments(QueryType='SHIPMENT', ShipmentIdList=shipment_id_list, **kwargs)
