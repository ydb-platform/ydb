import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class VendorInvoices(Client):
    """
    VendorInvoices SP-API Client
    :link: 

    The Selling Partner API for Retail Procurement Payments provides programmatic access to vendors payments data.
    """

    @sp_endpoint('/vendor/payments/v1/invoices', method='POST')
    def submit_invoices(self, data, **kwargs) -> ApiResponse:
        """
        submit_invoices(self, data, **kwargs) -> ApiResponse

        Submit new invoices to Amazon.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        10                                      10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            data:
            {
              "invoices": [
                {
                  "invoiceType": "Invoice",
                  "id": "string",
                  "referenceNumber": "string",
                  "date": "2019-08-24T14:15:22Z",
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
                      "postalOrZipCode": "string",
                      "countryCode": "st",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
                      }
                    ]
                  },
                  "shipToParty": {
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
                      "postalOrZipCode": "string",
                      "countryCode": "st",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
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
                      "postalOrZipCode": "string",
                      "countryCode": "st",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
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
                      "postalOrZipCode": "string",
                      "countryCode": "st",
                      "phone": "string"
                    },
                    "taxRegistrationDetails": [
                      {
                        "taxRegistrationType": "VAT",
                        "taxRegistrationNumber": "string"
                      }
                    ]
                  },
                  "paymentTerms": {
                    "type": "Basic",
                    "discountPercent": "string",
                    "discountDueDays": 0,
                    "netDueDays": 0
                  },
                  "invoiceTotal": {
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
                      "type": "Freight",
                      "description": "string",
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
                  "allowanceDetails": [
                    {
                      "type": "Discount",
                      "description": "string",
                      "allowanceAmount": {
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
                      "itemSequenceNumber": 0,
                      "amazonProductIdentifier": "string",
                      "vendorProductIdentifier": "string",
                      "invoicedQuantity": {
                        "amount": 0,
                        "unitOfMeasure": "Cases",
                        "unitSize": 0
                      },
                      "netCost": {
                        "currencyCode": "string",
                        "amount": "string"
                      },
                      "purchaseOrderNumber": "string",
                      "hsnCode": "string",
                      "creditNoteDetails": {
                        "referenceInvoiceNumber": "string",
                        "debitNoteNumber": "string",
                        "returnsReferenceNumber": "string",
                        "goodsReturnDate": "2019-08-24T14:15:22Z",
                        "rmaId": "string",
                        "coopReferenceNumber": "string",
                        "consignorsReferenceNumber": "string"
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
                      ],
                      "chargeDetails": [
                        {
                          "type": "Freight",
                          "description": "string",
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
                      ],
                      "allowanceDetails": [
                        {
                          "type": "Discount",
                          "description": "string",
                          "allowanceAmount": {
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
    
        return self._request(kwargs.pop('path'),  data={**data, **kwargs}, add_marketplace=False)
