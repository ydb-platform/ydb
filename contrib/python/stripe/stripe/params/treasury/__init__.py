# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.treasury._credit_reversal_create_params import (
        CreditReversalCreateParams as CreditReversalCreateParams,
    )
    from stripe.params.treasury._credit_reversal_list_params import (
        CreditReversalListParams as CreditReversalListParams,
    )
    from stripe.params.treasury._credit_reversal_retrieve_params import (
        CreditReversalRetrieveParams as CreditReversalRetrieveParams,
    )
    from stripe.params.treasury._debit_reversal_create_params import (
        DebitReversalCreateParams as DebitReversalCreateParams,
    )
    from stripe.params.treasury._debit_reversal_list_params import (
        DebitReversalListParams as DebitReversalListParams,
    )
    from stripe.params.treasury._debit_reversal_retrieve_params import (
        DebitReversalRetrieveParams as DebitReversalRetrieveParams,
    )
    from stripe.params.treasury._financial_account_close_params import (
        FinancialAccountCloseParams as FinancialAccountCloseParams,
        FinancialAccountCloseParamsForwardingSettings as FinancialAccountCloseParamsForwardingSettings,
    )
    from stripe.params.treasury._financial_account_create_params import (
        FinancialAccountCreateParams as FinancialAccountCreateParams,
        FinancialAccountCreateParamsFeatures as FinancialAccountCreateParamsFeatures,
        FinancialAccountCreateParamsFeaturesCardIssuing as FinancialAccountCreateParamsFeaturesCardIssuing,
        FinancialAccountCreateParamsFeaturesDepositInsurance as FinancialAccountCreateParamsFeaturesDepositInsurance,
        FinancialAccountCreateParamsFeaturesFinancialAddresses as FinancialAccountCreateParamsFeaturesFinancialAddresses,
        FinancialAccountCreateParamsFeaturesFinancialAddressesAba as FinancialAccountCreateParamsFeaturesFinancialAddressesAba,
        FinancialAccountCreateParamsFeaturesInboundTransfers as FinancialAccountCreateParamsFeaturesInboundTransfers,
        FinancialAccountCreateParamsFeaturesInboundTransfersAch as FinancialAccountCreateParamsFeaturesInboundTransfersAch,
        FinancialAccountCreateParamsFeaturesIntraStripeFlows as FinancialAccountCreateParamsFeaturesIntraStripeFlows,
        FinancialAccountCreateParamsFeaturesOutboundPayments as FinancialAccountCreateParamsFeaturesOutboundPayments,
        FinancialAccountCreateParamsFeaturesOutboundPaymentsAch as FinancialAccountCreateParamsFeaturesOutboundPaymentsAch,
        FinancialAccountCreateParamsFeaturesOutboundPaymentsUsDomesticWire as FinancialAccountCreateParamsFeaturesOutboundPaymentsUsDomesticWire,
        FinancialAccountCreateParamsFeaturesOutboundTransfers as FinancialAccountCreateParamsFeaturesOutboundTransfers,
        FinancialAccountCreateParamsFeaturesOutboundTransfersAch as FinancialAccountCreateParamsFeaturesOutboundTransfersAch,
        FinancialAccountCreateParamsFeaturesOutboundTransfersUsDomesticWire as FinancialAccountCreateParamsFeaturesOutboundTransfersUsDomesticWire,
        FinancialAccountCreateParamsPlatformRestrictions as FinancialAccountCreateParamsPlatformRestrictions,
    )
    from stripe.params.treasury._financial_account_features_retrieve_params import (
        FinancialAccountFeaturesRetrieveParams as FinancialAccountFeaturesRetrieveParams,
    )
    from stripe.params.treasury._financial_account_features_update_params import (
        FinancialAccountFeaturesUpdateParams as FinancialAccountFeaturesUpdateParams,
        FinancialAccountFeaturesUpdateParamsCardIssuing as FinancialAccountFeaturesUpdateParamsCardIssuing,
        FinancialAccountFeaturesUpdateParamsDepositInsurance as FinancialAccountFeaturesUpdateParamsDepositInsurance,
        FinancialAccountFeaturesUpdateParamsFinancialAddresses as FinancialAccountFeaturesUpdateParamsFinancialAddresses,
        FinancialAccountFeaturesUpdateParamsFinancialAddressesAba as FinancialAccountFeaturesUpdateParamsFinancialAddressesAba,
        FinancialAccountFeaturesUpdateParamsInboundTransfers as FinancialAccountFeaturesUpdateParamsInboundTransfers,
        FinancialAccountFeaturesUpdateParamsInboundTransfersAch as FinancialAccountFeaturesUpdateParamsInboundTransfersAch,
        FinancialAccountFeaturesUpdateParamsIntraStripeFlows as FinancialAccountFeaturesUpdateParamsIntraStripeFlows,
        FinancialAccountFeaturesUpdateParamsOutboundPayments as FinancialAccountFeaturesUpdateParamsOutboundPayments,
        FinancialAccountFeaturesUpdateParamsOutboundPaymentsAch as FinancialAccountFeaturesUpdateParamsOutboundPaymentsAch,
        FinancialAccountFeaturesUpdateParamsOutboundPaymentsUsDomesticWire as FinancialAccountFeaturesUpdateParamsOutboundPaymentsUsDomesticWire,
        FinancialAccountFeaturesUpdateParamsOutboundTransfers as FinancialAccountFeaturesUpdateParamsOutboundTransfers,
        FinancialAccountFeaturesUpdateParamsOutboundTransfersAch as FinancialAccountFeaturesUpdateParamsOutboundTransfersAch,
        FinancialAccountFeaturesUpdateParamsOutboundTransfersUsDomesticWire as FinancialAccountFeaturesUpdateParamsOutboundTransfersUsDomesticWire,
    )
    from stripe.params.treasury._financial_account_list_params import (
        FinancialAccountListParams as FinancialAccountListParams,
        FinancialAccountListParamsCreated as FinancialAccountListParamsCreated,
    )
    from stripe.params.treasury._financial_account_modify_params import (
        FinancialAccountModifyParams as FinancialAccountModifyParams,
        FinancialAccountModifyParamsFeatures as FinancialAccountModifyParamsFeatures,
        FinancialAccountModifyParamsFeaturesCardIssuing as FinancialAccountModifyParamsFeaturesCardIssuing,
        FinancialAccountModifyParamsFeaturesDepositInsurance as FinancialAccountModifyParamsFeaturesDepositInsurance,
        FinancialAccountModifyParamsFeaturesFinancialAddresses as FinancialAccountModifyParamsFeaturesFinancialAddresses,
        FinancialAccountModifyParamsFeaturesFinancialAddressesAba as FinancialAccountModifyParamsFeaturesFinancialAddressesAba,
        FinancialAccountModifyParamsFeaturesInboundTransfers as FinancialAccountModifyParamsFeaturesInboundTransfers,
        FinancialAccountModifyParamsFeaturesInboundTransfersAch as FinancialAccountModifyParamsFeaturesInboundTransfersAch,
        FinancialAccountModifyParamsFeaturesIntraStripeFlows as FinancialAccountModifyParamsFeaturesIntraStripeFlows,
        FinancialAccountModifyParamsFeaturesOutboundPayments as FinancialAccountModifyParamsFeaturesOutboundPayments,
        FinancialAccountModifyParamsFeaturesOutboundPaymentsAch as FinancialAccountModifyParamsFeaturesOutboundPaymentsAch,
        FinancialAccountModifyParamsFeaturesOutboundPaymentsUsDomesticWire as FinancialAccountModifyParamsFeaturesOutboundPaymentsUsDomesticWire,
        FinancialAccountModifyParamsFeaturesOutboundTransfers as FinancialAccountModifyParamsFeaturesOutboundTransfers,
        FinancialAccountModifyParamsFeaturesOutboundTransfersAch as FinancialAccountModifyParamsFeaturesOutboundTransfersAch,
        FinancialAccountModifyParamsFeaturesOutboundTransfersUsDomesticWire as FinancialAccountModifyParamsFeaturesOutboundTransfersUsDomesticWire,
        FinancialAccountModifyParamsForwardingSettings as FinancialAccountModifyParamsForwardingSettings,
        FinancialAccountModifyParamsPlatformRestrictions as FinancialAccountModifyParamsPlatformRestrictions,
    )
    from stripe.params.treasury._financial_account_retrieve_features_params import (
        FinancialAccountRetrieveFeaturesParams as FinancialAccountRetrieveFeaturesParams,
    )
    from stripe.params.treasury._financial_account_retrieve_params import (
        FinancialAccountRetrieveParams as FinancialAccountRetrieveParams,
    )
    from stripe.params.treasury._financial_account_update_features_params import (
        FinancialAccountUpdateFeaturesParams as FinancialAccountUpdateFeaturesParams,
        FinancialAccountUpdateFeaturesParamsCardIssuing as FinancialAccountUpdateFeaturesParamsCardIssuing,
        FinancialAccountUpdateFeaturesParamsDepositInsurance as FinancialAccountUpdateFeaturesParamsDepositInsurance,
        FinancialAccountUpdateFeaturesParamsFinancialAddresses as FinancialAccountUpdateFeaturesParamsFinancialAddresses,
        FinancialAccountUpdateFeaturesParamsFinancialAddressesAba as FinancialAccountUpdateFeaturesParamsFinancialAddressesAba,
        FinancialAccountUpdateFeaturesParamsInboundTransfers as FinancialAccountUpdateFeaturesParamsInboundTransfers,
        FinancialAccountUpdateFeaturesParamsInboundTransfersAch as FinancialAccountUpdateFeaturesParamsInboundTransfersAch,
        FinancialAccountUpdateFeaturesParamsIntraStripeFlows as FinancialAccountUpdateFeaturesParamsIntraStripeFlows,
        FinancialAccountUpdateFeaturesParamsOutboundPayments as FinancialAccountUpdateFeaturesParamsOutboundPayments,
        FinancialAccountUpdateFeaturesParamsOutboundPaymentsAch as FinancialAccountUpdateFeaturesParamsOutboundPaymentsAch,
        FinancialAccountUpdateFeaturesParamsOutboundPaymentsUsDomesticWire as FinancialAccountUpdateFeaturesParamsOutboundPaymentsUsDomesticWire,
        FinancialAccountUpdateFeaturesParamsOutboundTransfers as FinancialAccountUpdateFeaturesParamsOutboundTransfers,
        FinancialAccountUpdateFeaturesParamsOutboundTransfersAch as FinancialAccountUpdateFeaturesParamsOutboundTransfersAch,
        FinancialAccountUpdateFeaturesParamsOutboundTransfersUsDomesticWire as FinancialAccountUpdateFeaturesParamsOutboundTransfersUsDomesticWire,
    )
    from stripe.params.treasury._financial_account_update_params import (
        FinancialAccountUpdateParams as FinancialAccountUpdateParams,
        FinancialAccountUpdateParamsFeatures as FinancialAccountUpdateParamsFeatures,
        FinancialAccountUpdateParamsFeaturesCardIssuing as FinancialAccountUpdateParamsFeaturesCardIssuing,
        FinancialAccountUpdateParamsFeaturesDepositInsurance as FinancialAccountUpdateParamsFeaturesDepositInsurance,
        FinancialAccountUpdateParamsFeaturesFinancialAddresses as FinancialAccountUpdateParamsFeaturesFinancialAddresses,
        FinancialAccountUpdateParamsFeaturesFinancialAddressesAba as FinancialAccountUpdateParamsFeaturesFinancialAddressesAba,
        FinancialAccountUpdateParamsFeaturesInboundTransfers as FinancialAccountUpdateParamsFeaturesInboundTransfers,
        FinancialAccountUpdateParamsFeaturesInboundTransfersAch as FinancialAccountUpdateParamsFeaturesInboundTransfersAch,
        FinancialAccountUpdateParamsFeaturesIntraStripeFlows as FinancialAccountUpdateParamsFeaturesIntraStripeFlows,
        FinancialAccountUpdateParamsFeaturesOutboundPayments as FinancialAccountUpdateParamsFeaturesOutboundPayments,
        FinancialAccountUpdateParamsFeaturesOutboundPaymentsAch as FinancialAccountUpdateParamsFeaturesOutboundPaymentsAch,
        FinancialAccountUpdateParamsFeaturesOutboundPaymentsUsDomesticWire as FinancialAccountUpdateParamsFeaturesOutboundPaymentsUsDomesticWire,
        FinancialAccountUpdateParamsFeaturesOutboundTransfers as FinancialAccountUpdateParamsFeaturesOutboundTransfers,
        FinancialAccountUpdateParamsFeaturesOutboundTransfersAch as FinancialAccountUpdateParamsFeaturesOutboundTransfersAch,
        FinancialAccountUpdateParamsFeaturesOutboundTransfersUsDomesticWire as FinancialAccountUpdateParamsFeaturesOutboundTransfersUsDomesticWire,
        FinancialAccountUpdateParamsForwardingSettings as FinancialAccountUpdateParamsForwardingSettings,
        FinancialAccountUpdateParamsPlatformRestrictions as FinancialAccountUpdateParamsPlatformRestrictions,
    )
    from stripe.params.treasury._inbound_transfer_cancel_params import (
        InboundTransferCancelParams as InboundTransferCancelParams,
    )
    from stripe.params.treasury._inbound_transfer_create_params import (
        InboundTransferCreateParams as InboundTransferCreateParams,
    )
    from stripe.params.treasury._inbound_transfer_fail_params import (
        InboundTransferFailParams as InboundTransferFailParams,
        InboundTransferFailParamsFailureDetails as InboundTransferFailParamsFailureDetails,
    )
    from stripe.params.treasury._inbound_transfer_list_params import (
        InboundTransferListParams as InboundTransferListParams,
    )
    from stripe.params.treasury._inbound_transfer_retrieve_params import (
        InboundTransferRetrieveParams as InboundTransferRetrieveParams,
    )
    from stripe.params.treasury._inbound_transfer_return_inbound_transfer_params import (
        InboundTransferReturnInboundTransferParams as InboundTransferReturnInboundTransferParams,
    )
    from stripe.params.treasury._inbound_transfer_succeed_params import (
        InboundTransferSucceedParams as InboundTransferSucceedParams,
    )
    from stripe.params.treasury._outbound_payment_cancel_params import (
        OutboundPaymentCancelParams as OutboundPaymentCancelParams,
    )
    from stripe.params.treasury._outbound_payment_create_params import (
        OutboundPaymentCreateParams as OutboundPaymentCreateParams,
        OutboundPaymentCreateParamsDestinationPaymentMethodData as OutboundPaymentCreateParamsDestinationPaymentMethodData,
        OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetails as OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetails,
        OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetailsAddress as OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetailsAddress,
        OutboundPaymentCreateParamsDestinationPaymentMethodDataUsBankAccount as OutboundPaymentCreateParamsDestinationPaymentMethodDataUsBankAccount,
        OutboundPaymentCreateParamsDestinationPaymentMethodOptions as OutboundPaymentCreateParamsDestinationPaymentMethodOptions,
        OutboundPaymentCreateParamsDestinationPaymentMethodOptionsUsBankAccount as OutboundPaymentCreateParamsDestinationPaymentMethodOptionsUsBankAccount,
        OutboundPaymentCreateParamsEndUserDetails as OutboundPaymentCreateParamsEndUserDetails,
    )
    from stripe.params.treasury._outbound_payment_fail_params import (
        OutboundPaymentFailParams as OutboundPaymentFailParams,
    )
    from stripe.params.treasury._outbound_payment_list_params import (
        OutboundPaymentListParams as OutboundPaymentListParams,
        OutboundPaymentListParamsCreated as OutboundPaymentListParamsCreated,
    )
    from stripe.params.treasury._outbound_payment_post_params import (
        OutboundPaymentPostParams as OutboundPaymentPostParams,
    )
    from stripe.params.treasury._outbound_payment_retrieve_params import (
        OutboundPaymentRetrieveParams as OutboundPaymentRetrieveParams,
    )
    from stripe.params.treasury._outbound_payment_return_outbound_payment_params import (
        OutboundPaymentReturnOutboundPaymentParams as OutboundPaymentReturnOutboundPaymentParams,
        OutboundPaymentReturnOutboundPaymentParamsReturnedDetails as OutboundPaymentReturnOutboundPaymentParamsReturnedDetails,
    )
    from stripe.params.treasury._outbound_payment_update_params import (
        OutboundPaymentUpdateParams as OutboundPaymentUpdateParams,
        OutboundPaymentUpdateParamsTrackingDetails as OutboundPaymentUpdateParamsTrackingDetails,
        OutboundPaymentUpdateParamsTrackingDetailsAch as OutboundPaymentUpdateParamsTrackingDetailsAch,
        OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire as OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire,
    )
    from stripe.params.treasury._outbound_transfer_cancel_params import (
        OutboundTransferCancelParams as OutboundTransferCancelParams,
    )
    from stripe.params.treasury._outbound_transfer_create_params import (
        OutboundTransferCreateParams as OutboundTransferCreateParams,
        OutboundTransferCreateParamsDestinationPaymentMethodData as OutboundTransferCreateParamsDestinationPaymentMethodData,
        OutboundTransferCreateParamsDestinationPaymentMethodOptions as OutboundTransferCreateParamsDestinationPaymentMethodOptions,
        OutboundTransferCreateParamsDestinationPaymentMethodOptionsUsBankAccount as OutboundTransferCreateParamsDestinationPaymentMethodOptionsUsBankAccount,
    )
    from stripe.params.treasury._outbound_transfer_fail_params import (
        OutboundTransferFailParams as OutboundTransferFailParams,
    )
    from stripe.params.treasury._outbound_transfer_list_params import (
        OutboundTransferListParams as OutboundTransferListParams,
    )
    from stripe.params.treasury._outbound_transfer_post_params import (
        OutboundTransferPostParams as OutboundTransferPostParams,
    )
    from stripe.params.treasury._outbound_transfer_retrieve_params import (
        OutboundTransferRetrieveParams as OutboundTransferRetrieveParams,
    )
    from stripe.params.treasury._outbound_transfer_return_outbound_transfer_params import (
        OutboundTransferReturnOutboundTransferParams as OutboundTransferReturnOutboundTransferParams,
        OutboundTransferReturnOutboundTransferParamsReturnedDetails as OutboundTransferReturnOutboundTransferParamsReturnedDetails,
    )
    from stripe.params.treasury._outbound_transfer_update_params import (
        OutboundTransferUpdateParams as OutboundTransferUpdateParams,
        OutboundTransferUpdateParamsTrackingDetails as OutboundTransferUpdateParamsTrackingDetails,
        OutboundTransferUpdateParamsTrackingDetailsAch as OutboundTransferUpdateParamsTrackingDetailsAch,
        OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire as OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire,
    )
    from stripe.params.treasury._received_credit_create_params import (
        ReceivedCreditCreateParams as ReceivedCreditCreateParams,
        ReceivedCreditCreateParamsInitiatingPaymentMethodDetails as ReceivedCreditCreateParamsInitiatingPaymentMethodDetails,
        ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount as ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount,
    )
    from stripe.params.treasury._received_credit_list_params import (
        ReceivedCreditListParams as ReceivedCreditListParams,
        ReceivedCreditListParamsLinkedFlows as ReceivedCreditListParamsLinkedFlows,
    )
    from stripe.params.treasury._received_credit_retrieve_params import (
        ReceivedCreditRetrieveParams as ReceivedCreditRetrieveParams,
    )
    from stripe.params.treasury._received_debit_create_params import (
        ReceivedDebitCreateParams as ReceivedDebitCreateParams,
        ReceivedDebitCreateParamsInitiatingPaymentMethodDetails as ReceivedDebitCreateParamsInitiatingPaymentMethodDetails,
        ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount as ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount,
    )
    from stripe.params.treasury._received_debit_list_params import (
        ReceivedDebitListParams as ReceivedDebitListParams,
    )
    from stripe.params.treasury._received_debit_retrieve_params import (
        ReceivedDebitRetrieveParams as ReceivedDebitRetrieveParams,
    )
    from stripe.params.treasury._transaction_entry_list_params import (
        TransactionEntryListParams as TransactionEntryListParams,
        TransactionEntryListParamsCreated as TransactionEntryListParamsCreated,
        TransactionEntryListParamsEffectiveAt as TransactionEntryListParamsEffectiveAt,
    )
    from stripe.params.treasury._transaction_entry_retrieve_params import (
        TransactionEntryRetrieveParams as TransactionEntryRetrieveParams,
    )
    from stripe.params.treasury._transaction_list_params import (
        TransactionListParams as TransactionListParams,
        TransactionListParamsCreated as TransactionListParamsCreated,
        TransactionListParamsStatusTransitions as TransactionListParamsStatusTransitions,
        TransactionListParamsStatusTransitionsPostedAt as TransactionListParamsStatusTransitionsPostedAt,
    )
    from stripe.params.treasury._transaction_retrieve_params import (
        TransactionRetrieveParams as TransactionRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "CreditReversalCreateParams": (
        "stripe.params.treasury._credit_reversal_create_params",
        False,
    ),
    "CreditReversalListParams": (
        "stripe.params.treasury._credit_reversal_list_params",
        False,
    ),
    "CreditReversalRetrieveParams": (
        "stripe.params.treasury._credit_reversal_retrieve_params",
        False,
    ),
    "DebitReversalCreateParams": (
        "stripe.params.treasury._debit_reversal_create_params",
        False,
    ),
    "DebitReversalListParams": (
        "stripe.params.treasury._debit_reversal_list_params",
        False,
    ),
    "DebitReversalRetrieveParams": (
        "stripe.params.treasury._debit_reversal_retrieve_params",
        False,
    ),
    "FinancialAccountCloseParams": (
        "stripe.params.treasury._financial_account_close_params",
        False,
    ),
    "FinancialAccountCloseParamsForwardingSettings": (
        "stripe.params.treasury._financial_account_close_params",
        False,
    ),
    "FinancialAccountCreateParams": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeatures": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesCardIssuing": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesDepositInsurance": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesFinancialAddresses": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesFinancialAddressesAba": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesInboundTransfers": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesInboundTransfersAch": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesIntraStripeFlows": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundPayments": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundPaymentsAch": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundPaymentsUsDomesticWire": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundTransfers": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundTransfersAch": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsFeaturesOutboundTransfersUsDomesticWire": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountCreateParamsPlatformRestrictions": (
        "stripe.params.treasury._financial_account_create_params",
        False,
    ),
    "FinancialAccountFeaturesRetrieveParams": (
        "stripe.params.treasury._financial_account_features_retrieve_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParams": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsCardIssuing": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsDepositInsurance": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsFinancialAddresses": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsFinancialAddressesAba": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsInboundTransfers": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsInboundTransfersAch": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsIntraStripeFlows": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundPayments": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundPaymentsAch": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundPaymentsUsDomesticWire": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundTransfers": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundTransfersAch": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountFeaturesUpdateParamsOutboundTransfersUsDomesticWire": (
        "stripe.params.treasury._financial_account_features_update_params",
        False,
    ),
    "FinancialAccountListParams": (
        "stripe.params.treasury._financial_account_list_params",
        False,
    ),
    "FinancialAccountListParamsCreated": (
        "stripe.params.treasury._financial_account_list_params",
        False,
    ),
    "FinancialAccountModifyParams": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeatures": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesCardIssuing": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesDepositInsurance": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesFinancialAddresses": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesFinancialAddressesAba": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesInboundTransfers": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesInboundTransfersAch": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesIntraStripeFlows": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundPayments": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundPaymentsAch": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundPaymentsUsDomesticWire": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundTransfers": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundTransfersAch": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsFeaturesOutboundTransfersUsDomesticWire": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsForwardingSettings": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountModifyParamsPlatformRestrictions": (
        "stripe.params.treasury._financial_account_modify_params",
        False,
    ),
    "FinancialAccountRetrieveFeaturesParams": (
        "stripe.params.treasury._financial_account_retrieve_features_params",
        False,
    ),
    "FinancialAccountRetrieveParams": (
        "stripe.params.treasury._financial_account_retrieve_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParams": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsCardIssuing": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsDepositInsurance": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsFinancialAddresses": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsFinancialAddressesAba": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsInboundTransfers": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsInboundTransfersAch": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsIntraStripeFlows": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundPayments": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundPaymentsAch": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundPaymentsUsDomesticWire": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundTransfers": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundTransfersAch": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateFeaturesParamsOutboundTransfersUsDomesticWire": (
        "stripe.params.treasury._financial_account_update_features_params",
        False,
    ),
    "FinancialAccountUpdateParams": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeatures": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesCardIssuing": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesDepositInsurance": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesFinancialAddresses": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesFinancialAddressesAba": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesInboundTransfers": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesInboundTransfersAch": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesIntraStripeFlows": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundPayments": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundPaymentsAch": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundPaymentsUsDomesticWire": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundTransfers": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundTransfersAch": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsFeaturesOutboundTransfersUsDomesticWire": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsForwardingSettings": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "FinancialAccountUpdateParamsPlatformRestrictions": (
        "stripe.params.treasury._financial_account_update_params",
        False,
    ),
    "InboundTransferCancelParams": (
        "stripe.params.treasury._inbound_transfer_cancel_params",
        False,
    ),
    "InboundTransferCreateParams": (
        "stripe.params.treasury._inbound_transfer_create_params",
        False,
    ),
    "InboundTransferFailParams": (
        "stripe.params.treasury._inbound_transfer_fail_params",
        False,
    ),
    "InboundTransferFailParamsFailureDetails": (
        "stripe.params.treasury._inbound_transfer_fail_params",
        False,
    ),
    "InboundTransferListParams": (
        "stripe.params.treasury._inbound_transfer_list_params",
        False,
    ),
    "InboundTransferRetrieveParams": (
        "stripe.params.treasury._inbound_transfer_retrieve_params",
        False,
    ),
    "InboundTransferReturnInboundTransferParams": (
        "stripe.params.treasury._inbound_transfer_return_inbound_transfer_params",
        False,
    ),
    "InboundTransferSucceedParams": (
        "stripe.params.treasury._inbound_transfer_succeed_params",
        False,
    ),
    "OutboundPaymentCancelParams": (
        "stripe.params.treasury._outbound_payment_cancel_params",
        False,
    ),
    "OutboundPaymentCreateParams": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodData": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetails": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetailsAddress": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodDataUsBankAccount": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodOptions": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsDestinationPaymentMethodOptionsUsBankAccount": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentCreateParamsEndUserDetails": (
        "stripe.params.treasury._outbound_payment_create_params",
        False,
    ),
    "OutboundPaymentFailParams": (
        "stripe.params.treasury._outbound_payment_fail_params",
        False,
    ),
    "OutboundPaymentListParams": (
        "stripe.params.treasury._outbound_payment_list_params",
        False,
    ),
    "OutboundPaymentListParamsCreated": (
        "stripe.params.treasury._outbound_payment_list_params",
        False,
    ),
    "OutboundPaymentPostParams": (
        "stripe.params.treasury._outbound_payment_post_params",
        False,
    ),
    "OutboundPaymentRetrieveParams": (
        "stripe.params.treasury._outbound_payment_retrieve_params",
        False,
    ),
    "OutboundPaymentReturnOutboundPaymentParams": (
        "stripe.params.treasury._outbound_payment_return_outbound_payment_params",
        False,
    ),
    "OutboundPaymentReturnOutboundPaymentParamsReturnedDetails": (
        "stripe.params.treasury._outbound_payment_return_outbound_payment_params",
        False,
    ),
    "OutboundPaymentUpdateParams": (
        "stripe.params.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetails": (
        "stripe.params.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetailsAch": (
        "stripe.params.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire": (
        "stripe.params.treasury._outbound_payment_update_params",
        False,
    ),
    "OutboundTransferCancelParams": (
        "stripe.params.treasury._outbound_transfer_cancel_params",
        False,
    ),
    "OutboundTransferCreateParams": (
        "stripe.params.treasury._outbound_transfer_create_params",
        False,
    ),
    "OutboundTransferCreateParamsDestinationPaymentMethodData": (
        "stripe.params.treasury._outbound_transfer_create_params",
        False,
    ),
    "OutboundTransferCreateParamsDestinationPaymentMethodOptions": (
        "stripe.params.treasury._outbound_transfer_create_params",
        False,
    ),
    "OutboundTransferCreateParamsDestinationPaymentMethodOptionsUsBankAccount": (
        "stripe.params.treasury._outbound_transfer_create_params",
        False,
    ),
    "OutboundTransferFailParams": (
        "stripe.params.treasury._outbound_transfer_fail_params",
        False,
    ),
    "OutboundTransferListParams": (
        "stripe.params.treasury._outbound_transfer_list_params",
        False,
    ),
    "OutboundTransferPostParams": (
        "stripe.params.treasury._outbound_transfer_post_params",
        False,
    ),
    "OutboundTransferRetrieveParams": (
        "stripe.params.treasury._outbound_transfer_retrieve_params",
        False,
    ),
    "OutboundTransferReturnOutboundTransferParams": (
        "stripe.params.treasury._outbound_transfer_return_outbound_transfer_params",
        False,
    ),
    "OutboundTransferReturnOutboundTransferParamsReturnedDetails": (
        "stripe.params.treasury._outbound_transfer_return_outbound_transfer_params",
        False,
    ),
    "OutboundTransferUpdateParams": (
        "stripe.params.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetails": (
        "stripe.params.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetailsAch": (
        "stripe.params.treasury._outbound_transfer_update_params",
        False,
    ),
    "OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire": (
        "stripe.params.treasury._outbound_transfer_update_params",
        False,
    ),
    "ReceivedCreditCreateParams": (
        "stripe.params.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedCreditCreateParamsInitiatingPaymentMethodDetails": (
        "stripe.params.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedCreditCreateParamsInitiatingPaymentMethodDetailsUsBankAccount": (
        "stripe.params.treasury._received_credit_create_params",
        False,
    ),
    "ReceivedCreditListParams": (
        "stripe.params.treasury._received_credit_list_params",
        False,
    ),
    "ReceivedCreditListParamsLinkedFlows": (
        "stripe.params.treasury._received_credit_list_params",
        False,
    ),
    "ReceivedCreditRetrieveParams": (
        "stripe.params.treasury._received_credit_retrieve_params",
        False,
    ),
    "ReceivedDebitCreateParams": (
        "stripe.params.treasury._received_debit_create_params",
        False,
    ),
    "ReceivedDebitCreateParamsInitiatingPaymentMethodDetails": (
        "stripe.params.treasury._received_debit_create_params",
        False,
    ),
    "ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount": (
        "stripe.params.treasury._received_debit_create_params",
        False,
    ),
    "ReceivedDebitListParams": (
        "stripe.params.treasury._received_debit_list_params",
        False,
    ),
    "ReceivedDebitRetrieveParams": (
        "stripe.params.treasury._received_debit_retrieve_params",
        False,
    ),
    "TransactionEntryListParams": (
        "stripe.params.treasury._transaction_entry_list_params",
        False,
    ),
    "TransactionEntryListParamsCreated": (
        "stripe.params.treasury._transaction_entry_list_params",
        False,
    ),
    "TransactionEntryListParamsEffectiveAt": (
        "stripe.params.treasury._transaction_entry_list_params",
        False,
    ),
    "TransactionEntryRetrieveParams": (
        "stripe.params.treasury._transaction_entry_retrieve_params",
        False,
    ),
    "TransactionListParams": (
        "stripe.params.treasury._transaction_list_params",
        False,
    ),
    "TransactionListParamsCreated": (
        "stripe.params.treasury._transaction_list_params",
        False,
    ),
    "TransactionListParamsStatusTransitions": (
        "stripe.params.treasury._transaction_list_params",
        False,
    ),
    "TransactionListParamsStatusTransitionsPostedAt": (
        "stripe.params.treasury._transaction_list_params",
        False,
    ),
    "TransactionRetrieveParams": (
        "stripe.params.treasury._transaction_retrieve_params",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
