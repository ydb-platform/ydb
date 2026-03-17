import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorShipments(Client):
    """
    VendorShipments SP-API Client
    :link: 

    The Selling Partner API for Retail Procurement Shipments provides programmatic access to retail shipping data for vendors.
    """


    @sp_endpoint('/vendor/shipping/v1/shipmentConfirmations', method='POST')
    def submit_shipment_confirmations(self, **kwargs) -> ApiResponse:
        """
        submit_shipment_confirmations(self, **kwargs) -> ApiResponse

        Submits one or more shipment confirmations for vendor orders.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "shipmentConfirmations": [
                {
                  "shipmentIdentifier": "string",
                  "shipmentConfirmationType": "Original",
                  "shipmentType": "TruckLoad",
                  "shipmentStructure": "PalletizedAssortmentCase",
                  "transportationDetails": {
                    "carrierScac": "string",
                    "carrierShipmentReferenceNumber": "string",
                    "transportationMode": "Road",
                    "billOfLadingNumber": "string"
                  },
                  "amazonReferenceNumber": "string",
                  "shipmentConfirmationDate": "2019-08-24T14:15:22Z",
                  "shippedDate": "2019-08-24T14:15:22Z",
                  "estimatedDeliveryDate": "2019-08-24T14:15:22Z",
                  "sellingParty": {
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "partyId": "string",
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
                      }
                    ]
                  },
                  "shipFromParty": {
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "partyId": "string",
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
                      }
                    ]
                  },
                  "shipToParty": {
                    "address": {
                      "name": "string",
                      "addressLine1": "string",
                      "addressLine2": "string",
                      "addressLine3": "string",
                      "city": "string",
                      "county": "string",
                      "district": "string",
                      "stateOrRegion": "string",
                      "postalCode": "string",
                      "countryCode": "string",
                      "phone": "string"
                    },
                    "partyId": "string",
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
                      }
                    ]
                  },
                  "shipmentMeasurements": {
                    "grossShipmentWeight": {
                      "unitOfMeasure": "G",
                      "value": "string"
                    },
                    "shipmentVolume": {
                      "unitOfMeasure": "CuFt",
                      "value": "string"
                    },
                    "cartonCount": 0,
                    "palletCount": 0
                  },
                  "importDetails": {
                    "methodOfPayment": "PaidByBuyer",
                    "sealNumber": "string",
                    "route": {
                      "stops": [
                        {
                          "functionCode": "PortOfDischarge",
                          "locationIdentification": {
                            "type": "string",
                            "locationCode": "string",
                            "countryCode": "string"
                          },
                          "arrivalTime": "2019-08-24T14:15:22Z",
                          "departureTime": "2019-08-24T14:15:22Z"
                        }
                      ]
                    },
                    "importContainers": "string",
                    "billableWeight": {
                      "unitOfMeasure": "G",
                      "value": "string"
                    },
                    "estimatedShipByDate": "2019-08-24T14:15:22Z"
                  },
                  "shippedItems": [
                    {
                      "itemSequenceNumber": "string",
                      "amazonProductIdentifier": "string",
                      "vendorProductIdentifier": "string",
                      "shippedQuantity": {
                        "amount": 0,
                        "unitOfMeasure": "Cases",
                        "unitSize": 0
                      },
                      "itemDetails": {
                        "purchaseOrderNumber": "string",
                        "lotNumber": "string",
                        "expiry": {
                          "manufacturerDate": "2019-08-24T14:15:22Z",
                          "expiryDate": "2019-08-24T14:15:22Z",
                          "expiryAfterDuration": {
                            "durationUnit": "Days",
                            "durationValue": 0
                          }
                        },
                        "maximumRetailPrice": {
                          "currencyCode": "string",
                          "amount": "string"
                        },
                        "handlingCode": "Oversized"
                      }
                    }
                  ],
                  "cartons": [
                    {
                      "cartonIdentifiers": [
                        {
                          "containerIdentificationType": "SSCC",
                          "containerIdentificationNumber": "string"
                        }
                      ],
                      "cartonSequenceNumber": "string",
                      "dimensions": {
                        "length": "string",
                        "width": "string",
                        "height": "string",
                        "unitOfMeasure": "In"
                      },
                      "weight": {
                        "unitOfMeasure": "G",
                        "value": "string"
                      },
                      "trackingNumber": "string",
                      "items": [
                        {
                          "itemReference": "string",
                          "shippedQuantity": {
                            "amount": 0,
                            "unitOfMeasure": "Cases",
                            "unitSize": 0
                          },
                          "itemDetails": {
                            "purchaseOrderNumber": "string",
                            "lotNumber": "string",
                            "expiry": {
                              "manufacturerDate": "2019-08-24T14:15:22Z",
                              "expiryDate": "2019-08-24T14:15:22Z",
                              "expiryAfterDuration": {}
                            },
                            "maximumRetailPrice": {
                              "currencyCode": "string",
                              "amount": "string"
                            },
                            "handlingCode": "Oversized"
                          }
                        }
                      ]
                    }
                  ],
                  "pallets": [
                    {
                      "palletIdentifiers": [
                        {
                          "containerIdentificationType": "SSCC",
                          "containerIdentificationNumber": "string"
                        }
                      ],
                      "tier": 0,
                      "block": 0,
                      "dimensions": {
                        "length": "string",
                        "width": "string",
                        "height": "string",
                        "unitOfMeasure": "In"
                      },
                      "weight": {
                        "unitOfMeasure": "G",
                        "value": "string"
                      },
                      "cartonReferenceDetails": {
                        "cartonCount": 0,
                        "cartonReferenceNumbers": [
                          "string"
                        ]
                      },
                      "items": [
                        {
                          "itemReference": "string",
                          "shippedQuantity": {
                            "amount": 0,
                            "unitOfMeasure": "Cases",
                            "unitSize": 0
                          },
                          "itemDetails": {
                            "purchaseOrderNumber": "string",
                            "lotNumber": "string",
                            "expiry": {
                              "manufacturerDate": "2019-08-24T14:15:22Z",
                              "expiryDate": "2019-08-24T14:15:22Z",
                              "expiryAfterDuration": {}
                            },
                            "maximumRetailPrice": {
                              "currencyCode": "string",
                              "amount": "string"
                            },
                            "handlingCode": "Oversized"
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            }

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  data=kwargs)
    
