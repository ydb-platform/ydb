import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorDirectFulfillmentPayments(Client):
    """
    VendorDirectFulfillmentPayments SP-API Client
    :link: 

    The Selling Partner API for Direct Fulfillment Payments provides programmatic access to a direct fulfillment vendor's invoice data.
    """


    @sp_endpoint('/vendor/directFulfillment/payments/v1/invoices', method='POST')
    def submit_invoice(self, **kwargs) -> ApiResponse:
        """
        submit_invoice(self, **kwargs) -> ApiResponse

        Submits one or more invoices for a vendor's direct fulfillment orders.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body: {
              "invoices": [
                {
                  "invoiceNumber": "string",
                  "invoiceDate": "2019-08-24T14:15:22Z",
                  "referenceNumber": "string",
                  "remitToParty": {
                    "partyId": "string",
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
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
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
                        "taxRegistrationMessage": "string"
                      }
                    ]
                  },
                  "shipFromParty": {
                    "partyId": "string",
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
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
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
                        "taxRegistrationMessage": "string"
                      }
                    ]
                  },
                  "billToParty": {
                    "partyId": "string",
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
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string",
                        "taxRegistrationAddress": {
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
                        "taxRegistrationMessage": "string"
                      }
                    ]
                  },
                  "shipToCountryCode": "string",
                  "paymentTermsCode": "string",
                  "invoiceTotal": {
                    "currencyCode": "string",
                    "amount": "string"
                  },
                  "taxTotals": [
                    {
                      "taxType": "CGST",
                      "taxRate": "string",
                      "taxAmount": {
                        "currencyCode": "string",
                        "amount": "string"
                      },
                      "taxableAmount": {
                        "currencyCode": "string",
                        "amount": "string"
                      }
                    }
                  ],
                  "additionalDetails": [
                    {
                      "type": "SUR",
                      "detail": "string",
                      "languageCode": "string"
                    }
                  ],
                  "chargeDetails": [
                    {
                      "type": "GIFTWRAP",
                      "chargeAmount": {
                        "currencyCode": "string",
                        "amount": "string"
                      },
                      "taxDetails": [
                        {
                          "taxType": "CGST",
                          "taxRate": "string",
                          "taxAmount": {
                            "currencyCode": "string",
                            "amount": "string"
                          },
                          "taxableAmount": {
                            "currencyCode": "string",
                            "amount": "string"
                          }
                        }
                      ]
                    }
                  ],
                  "items": [
                    {
                      "itemSequenceNumber": "string",
                      "buyerProductIdentifier": "string",
                      "vendorProductIdentifier": "string",
                      "invoicedQuantity": {
                        "amount": 0,
                        "unitOfMeasure": "string"
                      },
                      "netCost": {
                        "currencyCode": "string",
                        "amount": "string"
                      },
                      "purchaseOrderNumber": "string",
                      "vendorOrderNumber": "string",
                      "hsnCode": "string",
                      "taxDetails": [
                        {
                          "taxType": "CGST",
                          "taxRate": "string",
                          "taxAmount": {
                            "currencyCode": "string",
                            "amount": "string"
                          },
                          "taxableAmount": {
                            "currencyCode": "string",
                            "amount": "string"
                          }
                        }
                      ],
                      "chargeDetails": [
                        {
                          "type": "GIFTWRAP",
                          "chargeAmount": {
                            "currencyCode": "string",
                            "amount": "string"
                          },
                          "taxDetails": [
                            {
                              "taxType": "CGST",
                              "taxRate": "string",
                              "taxAmount": {},
                              "taxableAmount": {}
                            }
                          ]
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
    
        return self._request(kwargs.pop('path'),  data=kwargs, add_marketplace=False)
    
