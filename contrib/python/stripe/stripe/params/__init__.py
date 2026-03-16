# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params import (
        apps as apps,
        billing as billing,
        billing_portal as billing_portal,
        checkout as checkout,
        climate as climate,
        entitlements as entitlements,
        financial_connections as financial_connections,
        forwarding as forwarding,
        identity as identity,
        issuing as issuing,
        radar as radar,
        reporting as reporting,
        sigma as sigma,
        tax as tax,
        terminal as terminal,
        test_helpers as test_helpers,
        treasury as treasury,
    )
    from stripe.params._account_capability_list_params import (
        AccountCapabilityListParams as AccountCapabilityListParams,
    )
    from stripe.params._account_capability_retrieve_params import (
        AccountCapabilityRetrieveParams as AccountCapabilityRetrieveParams,
    )
    from stripe.params._account_capability_update_params import (
        AccountCapabilityUpdateParams as AccountCapabilityUpdateParams,
    )
    from stripe.params._account_create_external_account_params import (
        AccountCreateExternalAccountParams as AccountCreateExternalAccountParams,
        AccountCreateExternalAccountParamsBankAccount as AccountCreateExternalAccountParamsBankAccount,
        AccountCreateExternalAccountParamsCard as AccountCreateExternalAccountParamsCard,
        AccountCreateExternalAccountParamsCardToken as AccountCreateExternalAccountParamsCardToken,
    )
    from stripe.params._account_create_login_link_params import (
        AccountCreateLoginLinkParams as AccountCreateLoginLinkParams,
    )
    from stripe.params._account_create_params import (
        AccountCreateParams as AccountCreateParams,
        AccountCreateParamsBankAccount as AccountCreateParamsBankAccount,
        AccountCreateParamsBusinessProfile as AccountCreateParamsBusinessProfile,
        AccountCreateParamsBusinessProfileAnnualRevenue as AccountCreateParamsBusinessProfileAnnualRevenue,
        AccountCreateParamsBusinessProfileMonthlyEstimatedRevenue as AccountCreateParamsBusinessProfileMonthlyEstimatedRevenue,
        AccountCreateParamsBusinessProfileSupportAddress as AccountCreateParamsBusinessProfileSupportAddress,
        AccountCreateParamsCapabilities as AccountCreateParamsCapabilities,
        AccountCreateParamsCapabilitiesAcssDebitPayments as AccountCreateParamsCapabilitiesAcssDebitPayments,
        AccountCreateParamsCapabilitiesAffirmPayments as AccountCreateParamsCapabilitiesAffirmPayments,
        AccountCreateParamsCapabilitiesAfterpayClearpayPayments as AccountCreateParamsCapabilitiesAfterpayClearpayPayments,
        AccountCreateParamsCapabilitiesAlmaPayments as AccountCreateParamsCapabilitiesAlmaPayments,
        AccountCreateParamsCapabilitiesAmazonPayPayments as AccountCreateParamsCapabilitiesAmazonPayPayments,
        AccountCreateParamsCapabilitiesAuBecsDebitPayments as AccountCreateParamsCapabilitiesAuBecsDebitPayments,
        AccountCreateParamsCapabilitiesBacsDebitPayments as AccountCreateParamsCapabilitiesBacsDebitPayments,
        AccountCreateParamsCapabilitiesBancontactPayments as AccountCreateParamsCapabilitiesBancontactPayments,
        AccountCreateParamsCapabilitiesBankTransferPayments as AccountCreateParamsCapabilitiesBankTransferPayments,
        AccountCreateParamsCapabilitiesBilliePayments as AccountCreateParamsCapabilitiesBilliePayments,
        AccountCreateParamsCapabilitiesBlikPayments as AccountCreateParamsCapabilitiesBlikPayments,
        AccountCreateParamsCapabilitiesBoletoPayments as AccountCreateParamsCapabilitiesBoletoPayments,
        AccountCreateParamsCapabilitiesCardIssuing as AccountCreateParamsCapabilitiesCardIssuing,
        AccountCreateParamsCapabilitiesCardPayments as AccountCreateParamsCapabilitiesCardPayments,
        AccountCreateParamsCapabilitiesCartesBancairesPayments as AccountCreateParamsCapabilitiesCartesBancairesPayments,
        AccountCreateParamsCapabilitiesCashappPayments as AccountCreateParamsCapabilitiesCashappPayments,
        AccountCreateParamsCapabilitiesCryptoPayments as AccountCreateParamsCapabilitiesCryptoPayments,
        AccountCreateParamsCapabilitiesEpsPayments as AccountCreateParamsCapabilitiesEpsPayments,
        AccountCreateParamsCapabilitiesFpxPayments as AccountCreateParamsCapabilitiesFpxPayments,
        AccountCreateParamsCapabilitiesGbBankTransferPayments as AccountCreateParamsCapabilitiesGbBankTransferPayments,
        AccountCreateParamsCapabilitiesGiropayPayments as AccountCreateParamsCapabilitiesGiropayPayments,
        AccountCreateParamsCapabilitiesGrabpayPayments as AccountCreateParamsCapabilitiesGrabpayPayments,
        AccountCreateParamsCapabilitiesIdealPayments as AccountCreateParamsCapabilitiesIdealPayments,
        AccountCreateParamsCapabilitiesIndiaInternationalPayments as AccountCreateParamsCapabilitiesIndiaInternationalPayments,
        AccountCreateParamsCapabilitiesJcbPayments as AccountCreateParamsCapabilitiesJcbPayments,
        AccountCreateParamsCapabilitiesJpBankTransferPayments as AccountCreateParamsCapabilitiesJpBankTransferPayments,
        AccountCreateParamsCapabilitiesKakaoPayPayments as AccountCreateParamsCapabilitiesKakaoPayPayments,
        AccountCreateParamsCapabilitiesKlarnaPayments as AccountCreateParamsCapabilitiesKlarnaPayments,
        AccountCreateParamsCapabilitiesKonbiniPayments as AccountCreateParamsCapabilitiesKonbiniPayments,
        AccountCreateParamsCapabilitiesKrCardPayments as AccountCreateParamsCapabilitiesKrCardPayments,
        AccountCreateParamsCapabilitiesLegacyPayments as AccountCreateParamsCapabilitiesLegacyPayments,
        AccountCreateParamsCapabilitiesLinkPayments as AccountCreateParamsCapabilitiesLinkPayments,
        AccountCreateParamsCapabilitiesMbWayPayments as AccountCreateParamsCapabilitiesMbWayPayments,
        AccountCreateParamsCapabilitiesMobilepayPayments as AccountCreateParamsCapabilitiesMobilepayPayments,
        AccountCreateParamsCapabilitiesMultibancoPayments as AccountCreateParamsCapabilitiesMultibancoPayments,
        AccountCreateParamsCapabilitiesMxBankTransferPayments as AccountCreateParamsCapabilitiesMxBankTransferPayments,
        AccountCreateParamsCapabilitiesNaverPayPayments as AccountCreateParamsCapabilitiesNaverPayPayments,
        AccountCreateParamsCapabilitiesNzBankAccountBecsDebitPayments as AccountCreateParamsCapabilitiesNzBankAccountBecsDebitPayments,
        AccountCreateParamsCapabilitiesOxxoPayments as AccountCreateParamsCapabilitiesOxxoPayments,
        AccountCreateParamsCapabilitiesP24Payments as AccountCreateParamsCapabilitiesP24Payments,
        AccountCreateParamsCapabilitiesPayByBankPayments as AccountCreateParamsCapabilitiesPayByBankPayments,
        AccountCreateParamsCapabilitiesPaycoPayments as AccountCreateParamsCapabilitiesPaycoPayments,
        AccountCreateParamsCapabilitiesPaynowPayments as AccountCreateParamsCapabilitiesPaynowPayments,
        AccountCreateParamsCapabilitiesPaytoPayments as AccountCreateParamsCapabilitiesPaytoPayments,
        AccountCreateParamsCapabilitiesPixPayments as AccountCreateParamsCapabilitiesPixPayments,
        AccountCreateParamsCapabilitiesPromptpayPayments as AccountCreateParamsCapabilitiesPromptpayPayments,
        AccountCreateParamsCapabilitiesRevolutPayPayments as AccountCreateParamsCapabilitiesRevolutPayPayments,
        AccountCreateParamsCapabilitiesSamsungPayPayments as AccountCreateParamsCapabilitiesSamsungPayPayments,
        AccountCreateParamsCapabilitiesSatispayPayments as AccountCreateParamsCapabilitiesSatispayPayments,
        AccountCreateParamsCapabilitiesSepaBankTransferPayments as AccountCreateParamsCapabilitiesSepaBankTransferPayments,
        AccountCreateParamsCapabilitiesSepaDebitPayments as AccountCreateParamsCapabilitiesSepaDebitPayments,
        AccountCreateParamsCapabilitiesSofortPayments as AccountCreateParamsCapabilitiesSofortPayments,
        AccountCreateParamsCapabilitiesSwishPayments as AccountCreateParamsCapabilitiesSwishPayments,
        AccountCreateParamsCapabilitiesTaxReportingUs1099K as AccountCreateParamsCapabilitiesTaxReportingUs1099K,
        AccountCreateParamsCapabilitiesTaxReportingUs1099Misc as AccountCreateParamsCapabilitiesTaxReportingUs1099Misc,
        AccountCreateParamsCapabilitiesTransfers as AccountCreateParamsCapabilitiesTransfers,
        AccountCreateParamsCapabilitiesTreasury as AccountCreateParamsCapabilitiesTreasury,
        AccountCreateParamsCapabilitiesTwintPayments as AccountCreateParamsCapabilitiesTwintPayments,
        AccountCreateParamsCapabilitiesUsBankAccountAchPayments as AccountCreateParamsCapabilitiesUsBankAccountAchPayments,
        AccountCreateParamsCapabilitiesUsBankTransferPayments as AccountCreateParamsCapabilitiesUsBankTransferPayments,
        AccountCreateParamsCapabilitiesZipPayments as AccountCreateParamsCapabilitiesZipPayments,
        AccountCreateParamsCard as AccountCreateParamsCard,
        AccountCreateParamsCardToken as AccountCreateParamsCardToken,
        AccountCreateParamsCompany as AccountCreateParamsCompany,
        AccountCreateParamsCompanyAddress as AccountCreateParamsCompanyAddress,
        AccountCreateParamsCompanyAddressKana as AccountCreateParamsCompanyAddressKana,
        AccountCreateParamsCompanyAddressKanji as AccountCreateParamsCompanyAddressKanji,
        AccountCreateParamsCompanyDirectorshipDeclaration as AccountCreateParamsCompanyDirectorshipDeclaration,
        AccountCreateParamsCompanyOwnershipDeclaration as AccountCreateParamsCompanyOwnershipDeclaration,
        AccountCreateParamsCompanyRegistrationDate as AccountCreateParamsCompanyRegistrationDate,
        AccountCreateParamsCompanyRepresentativeDeclaration as AccountCreateParamsCompanyRepresentativeDeclaration,
        AccountCreateParamsCompanyVerification as AccountCreateParamsCompanyVerification,
        AccountCreateParamsCompanyVerificationDocument as AccountCreateParamsCompanyVerificationDocument,
        AccountCreateParamsController as AccountCreateParamsController,
        AccountCreateParamsControllerFees as AccountCreateParamsControllerFees,
        AccountCreateParamsControllerLosses as AccountCreateParamsControllerLosses,
        AccountCreateParamsControllerStripeDashboard as AccountCreateParamsControllerStripeDashboard,
        AccountCreateParamsDocuments as AccountCreateParamsDocuments,
        AccountCreateParamsDocumentsBankAccountOwnershipVerification as AccountCreateParamsDocumentsBankAccountOwnershipVerification,
        AccountCreateParamsDocumentsCompanyLicense as AccountCreateParamsDocumentsCompanyLicense,
        AccountCreateParamsDocumentsCompanyMemorandumOfAssociation as AccountCreateParamsDocumentsCompanyMemorandumOfAssociation,
        AccountCreateParamsDocumentsCompanyMinisterialDecree as AccountCreateParamsDocumentsCompanyMinisterialDecree,
        AccountCreateParamsDocumentsCompanyRegistrationVerification as AccountCreateParamsDocumentsCompanyRegistrationVerification,
        AccountCreateParamsDocumentsCompanyTaxIdVerification as AccountCreateParamsDocumentsCompanyTaxIdVerification,
        AccountCreateParamsDocumentsProofOfAddress as AccountCreateParamsDocumentsProofOfAddress,
        AccountCreateParamsDocumentsProofOfRegistration as AccountCreateParamsDocumentsProofOfRegistration,
        AccountCreateParamsDocumentsProofOfRegistrationSigner as AccountCreateParamsDocumentsProofOfRegistrationSigner,
        AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnership as AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnership,
        AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner as AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner,
        AccountCreateParamsGroups as AccountCreateParamsGroups,
        AccountCreateParamsIndividual as AccountCreateParamsIndividual,
        AccountCreateParamsIndividualAddress as AccountCreateParamsIndividualAddress,
        AccountCreateParamsIndividualAddressKana as AccountCreateParamsIndividualAddressKana,
        AccountCreateParamsIndividualAddressKanji as AccountCreateParamsIndividualAddressKanji,
        AccountCreateParamsIndividualDob as AccountCreateParamsIndividualDob,
        AccountCreateParamsIndividualRegisteredAddress as AccountCreateParamsIndividualRegisteredAddress,
        AccountCreateParamsIndividualRelationship as AccountCreateParamsIndividualRelationship,
        AccountCreateParamsIndividualVerification as AccountCreateParamsIndividualVerification,
        AccountCreateParamsIndividualVerificationAdditionalDocument as AccountCreateParamsIndividualVerificationAdditionalDocument,
        AccountCreateParamsIndividualVerificationDocument as AccountCreateParamsIndividualVerificationDocument,
        AccountCreateParamsSettings as AccountCreateParamsSettings,
        AccountCreateParamsSettingsBacsDebitPayments as AccountCreateParamsSettingsBacsDebitPayments,
        AccountCreateParamsSettingsBranding as AccountCreateParamsSettingsBranding,
        AccountCreateParamsSettingsCardIssuing as AccountCreateParamsSettingsCardIssuing,
        AccountCreateParamsSettingsCardIssuingTosAcceptance as AccountCreateParamsSettingsCardIssuingTosAcceptance,
        AccountCreateParamsSettingsCardPayments as AccountCreateParamsSettingsCardPayments,
        AccountCreateParamsSettingsCardPaymentsDeclineOn as AccountCreateParamsSettingsCardPaymentsDeclineOn,
        AccountCreateParamsSettingsInvoices as AccountCreateParamsSettingsInvoices,
        AccountCreateParamsSettingsPayments as AccountCreateParamsSettingsPayments,
        AccountCreateParamsSettingsPayouts as AccountCreateParamsSettingsPayouts,
        AccountCreateParamsSettingsPayoutsSchedule as AccountCreateParamsSettingsPayoutsSchedule,
        AccountCreateParamsSettingsTreasury as AccountCreateParamsSettingsTreasury,
        AccountCreateParamsSettingsTreasuryTosAcceptance as AccountCreateParamsSettingsTreasuryTosAcceptance,
        AccountCreateParamsTosAcceptance as AccountCreateParamsTosAcceptance,
    )
    from stripe.params._account_create_person_params import (
        AccountCreatePersonParams as AccountCreatePersonParams,
        AccountCreatePersonParamsAdditionalTosAcceptances as AccountCreatePersonParamsAdditionalTosAcceptances,
        AccountCreatePersonParamsAdditionalTosAcceptancesAccount as AccountCreatePersonParamsAdditionalTosAcceptancesAccount,
        AccountCreatePersonParamsAddress as AccountCreatePersonParamsAddress,
        AccountCreatePersonParamsAddressKana as AccountCreatePersonParamsAddressKana,
        AccountCreatePersonParamsAddressKanji as AccountCreatePersonParamsAddressKanji,
        AccountCreatePersonParamsDob as AccountCreatePersonParamsDob,
        AccountCreatePersonParamsDocuments as AccountCreatePersonParamsDocuments,
        AccountCreatePersonParamsDocumentsCompanyAuthorization as AccountCreatePersonParamsDocumentsCompanyAuthorization,
        AccountCreatePersonParamsDocumentsPassport as AccountCreatePersonParamsDocumentsPassport,
        AccountCreatePersonParamsDocumentsVisa as AccountCreatePersonParamsDocumentsVisa,
        AccountCreatePersonParamsRegisteredAddress as AccountCreatePersonParamsRegisteredAddress,
        AccountCreatePersonParamsRelationship as AccountCreatePersonParamsRelationship,
        AccountCreatePersonParamsUsCfpbData as AccountCreatePersonParamsUsCfpbData,
        AccountCreatePersonParamsUsCfpbDataEthnicityDetails as AccountCreatePersonParamsUsCfpbDataEthnicityDetails,
        AccountCreatePersonParamsUsCfpbDataRaceDetails as AccountCreatePersonParamsUsCfpbDataRaceDetails,
        AccountCreatePersonParamsVerification as AccountCreatePersonParamsVerification,
        AccountCreatePersonParamsVerificationAdditionalDocument as AccountCreatePersonParamsVerificationAdditionalDocument,
        AccountCreatePersonParamsVerificationDocument as AccountCreatePersonParamsVerificationDocument,
    )
    from stripe.params._account_delete_external_account_params import (
        AccountDeleteExternalAccountParams as AccountDeleteExternalAccountParams,
    )
    from stripe.params._account_delete_params import (
        AccountDeleteParams as AccountDeleteParams,
    )
    from stripe.params._account_delete_person_params import (
        AccountDeletePersonParams as AccountDeletePersonParams,
    )
    from stripe.params._account_external_account_create_params import (
        AccountExternalAccountCreateParams as AccountExternalAccountCreateParams,
        AccountExternalAccountCreateParamsBankAccount as AccountExternalAccountCreateParamsBankAccount,
        AccountExternalAccountCreateParamsCard as AccountExternalAccountCreateParamsCard,
        AccountExternalAccountCreateParamsCardToken as AccountExternalAccountCreateParamsCardToken,
    )
    from stripe.params._account_external_account_delete_params import (
        AccountExternalAccountDeleteParams as AccountExternalAccountDeleteParams,
    )
    from stripe.params._account_external_account_list_params import (
        AccountExternalAccountListParams as AccountExternalAccountListParams,
    )
    from stripe.params._account_external_account_retrieve_params import (
        AccountExternalAccountRetrieveParams as AccountExternalAccountRetrieveParams,
    )
    from stripe.params._account_external_account_update_params import (
        AccountExternalAccountUpdateParams as AccountExternalAccountUpdateParams,
        AccountExternalAccountUpdateParamsDocuments as AccountExternalAccountUpdateParamsDocuments,
        AccountExternalAccountUpdateParamsDocumentsBankAccountOwnershipVerification as AccountExternalAccountUpdateParamsDocumentsBankAccountOwnershipVerification,
    )
    from stripe.params._account_link_create_params import (
        AccountLinkCreateParams as AccountLinkCreateParams,
        AccountLinkCreateParamsCollectionOptions as AccountLinkCreateParamsCollectionOptions,
    )
    from stripe.params._account_list_capabilities_params import (
        AccountListCapabilitiesParams as AccountListCapabilitiesParams,
    )
    from stripe.params._account_list_external_accounts_params import (
        AccountListExternalAccountsParams as AccountListExternalAccountsParams,
    )
    from stripe.params._account_list_params import (
        AccountListParams as AccountListParams,
        AccountListParamsCreated as AccountListParamsCreated,
    )
    from stripe.params._account_list_persons_params import (
        AccountListPersonsParams as AccountListPersonsParams,
        AccountListPersonsParamsRelationship as AccountListPersonsParamsRelationship,
    )
    from stripe.params._account_login_link_create_params import (
        AccountLoginLinkCreateParams as AccountLoginLinkCreateParams,
    )
    from stripe.params._account_modify_capability_params import (
        AccountModifyCapabilityParams as AccountModifyCapabilityParams,
    )
    from stripe.params._account_modify_external_account_params import (
        AccountModifyExternalAccountParams as AccountModifyExternalAccountParams,
        AccountModifyExternalAccountParamsDocuments as AccountModifyExternalAccountParamsDocuments,
        AccountModifyExternalAccountParamsDocumentsBankAccountOwnershipVerification as AccountModifyExternalAccountParamsDocumentsBankAccountOwnershipVerification,
    )
    from stripe.params._account_modify_person_params import (
        AccountModifyPersonParams as AccountModifyPersonParams,
        AccountModifyPersonParamsAdditionalTosAcceptances as AccountModifyPersonParamsAdditionalTosAcceptances,
        AccountModifyPersonParamsAdditionalTosAcceptancesAccount as AccountModifyPersonParamsAdditionalTosAcceptancesAccount,
        AccountModifyPersonParamsAddress as AccountModifyPersonParamsAddress,
        AccountModifyPersonParamsAddressKana as AccountModifyPersonParamsAddressKana,
        AccountModifyPersonParamsAddressKanji as AccountModifyPersonParamsAddressKanji,
        AccountModifyPersonParamsDob as AccountModifyPersonParamsDob,
        AccountModifyPersonParamsDocuments as AccountModifyPersonParamsDocuments,
        AccountModifyPersonParamsDocumentsCompanyAuthorization as AccountModifyPersonParamsDocumentsCompanyAuthorization,
        AccountModifyPersonParamsDocumentsPassport as AccountModifyPersonParamsDocumentsPassport,
        AccountModifyPersonParamsDocumentsVisa as AccountModifyPersonParamsDocumentsVisa,
        AccountModifyPersonParamsRegisteredAddress as AccountModifyPersonParamsRegisteredAddress,
        AccountModifyPersonParamsRelationship as AccountModifyPersonParamsRelationship,
        AccountModifyPersonParamsUsCfpbData as AccountModifyPersonParamsUsCfpbData,
        AccountModifyPersonParamsUsCfpbDataEthnicityDetails as AccountModifyPersonParamsUsCfpbDataEthnicityDetails,
        AccountModifyPersonParamsUsCfpbDataRaceDetails as AccountModifyPersonParamsUsCfpbDataRaceDetails,
        AccountModifyPersonParamsVerification as AccountModifyPersonParamsVerification,
        AccountModifyPersonParamsVerificationAdditionalDocument as AccountModifyPersonParamsVerificationAdditionalDocument,
        AccountModifyPersonParamsVerificationDocument as AccountModifyPersonParamsVerificationDocument,
    )
    from stripe.params._account_person_create_params import (
        AccountPersonCreateParams as AccountPersonCreateParams,
        AccountPersonCreateParamsAdditionalTosAcceptances as AccountPersonCreateParamsAdditionalTosAcceptances,
        AccountPersonCreateParamsAdditionalTosAcceptancesAccount as AccountPersonCreateParamsAdditionalTosAcceptancesAccount,
        AccountPersonCreateParamsAddress as AccountPersonCreateParamsAddress,
        AccountPersonCreateParamsAddressKana as AccountPersonCreateParamsAddressKana,
        AccountPersonCreateParamsAddressKanji as AccountPersonCreateParamsAddressKanji,
        AccountPersonCreateParamsDob as AccountPersonCreateParamsDob,
        AccountPersonCreateParamsDocuments as AccountPersonCreateParamsDocuments,
        AccountPersonCreateParamsDocumentsCompanyAuthorization as AccountPersonCreateParamsDocumentsCompanyAuthorization,
        AccountPersonCreateParamsDocumentsPassport as AccountPersonCreateParamsDocumentsPassport,
        AccountPersonCreateParamsDocumentsVisa as AccountPersonCreateParamsDocumentsVisa,
        AccountPersonCreateParamsRegisteredAddress as AccountPersonCreateParamsRegisteredAddress,
        AccountPersonCreateParamsRelationship as AccountPersonCreateParamsRelationship,
        AccountPersonCreateParamsUsCfpbData as AccountPersonCreateParamsUsCfpbData,
        AccountPersonCreateParamsUsCfpbDataEthnicityDetails as AccountPersonCreateParamsUsCfpbDataEthnicityDetails,
        AccountPersonCreateParamsUsCfpbDataRaceDetails as AccountPersonCreateParamsUsCfpbDataRaceDetails,
        AccountPersonCreateParamsVerification as AccountPersonCreateParamsVerification,
        AccountPersonCreateParamsVerificationAdditionalDocument as AccountPersonCreateParamsVerificationAdditionalDocument,
        AccountPersonCreateParamsVerificationDocument as AccountPersonCreateParamsVerificationDocument,
    )
    from stripe.params._account_person_delete_params import (
        AccountPersonDeleteParams as AccountPersonDeleteParams,
    )
    from stripe.params._account_person_list_params import (
        AccountPersonListParams as AccountPersonListParams,
        AccountPersonListParamsRelationship as AccountPersonListParamsRelationship,
    )
    from stripe.params._account_person_retrieve_params import (
        AccountPersonRetrieveParams as AccountPersonRetrieveParams,
    )
    from stripe.params._account_person_update_params import (
        AccountPersonUpdateParams as AccountPersonUpdateParams,
        AccountPersonUpdateParamsAdditionalTosAcceptances as AccountPersonUpdateParamsAdditionalTosAcceptances,
        AccountPersonUpdateParamsAdditionalTosAcceptancesAccount as AccountPersonUpdateParamsAdditionalTosAcceptancesAccount,
        AccountPersonUpdateParamsAddress as AccountPersonUpdateParamsAddress,
        AccountPersonUpdateParamsAddressKana as AccountPersonUpdateParamsAddressKana,
        AccountPersonUpdateParamsAddressKanji as AccountPersonUpdateParamsAddressKanji,
        AccountPersonUpdateParamsDob as AccountPersonUpdateParamsDob,
        AccountPersonUpdateParamsDocuments as AccountPersonUpdateParamsDocuments,
        AccountPersonUpdateParamsDocumentsCompanyAuthorization as AccountPersonUpdateParamsDocumentsCompanyAuthorization,
        AccountPersonUpdateParamsDocumentsPassport as AccountPersonUpdateParamsDocumentsPassport,
        AccountPersonUpdateParamsDocumentsVisa as AccountPersonUpdateParamsDocumentsVisa,
        AccountPersonUpdateParamsRegisteredAddress as AccountPersonUpdateParamsRegisteredAddress,
        AccountPersonUpdateParamsRelationship as AccountPersonUpdateParamsRelationship,
        AccountPersonUpdateParamsUsCfpbData as AccountPersonUpdateParamsUsCfpbData,
        AccountPersonUpdateParamsUsCfpbDataEthnicityDetails as AccountPersonUpdateParamsUsCfpbDataEthnicityDetails,
        AccountPersonUpdateParamsUsCfpbDataRaceDetails as AccountPersonUpdateParamsUsCfpbDataRaceDetails,
        AccountPersonUpdateParamsVerification as AccountPersonUpdateParamsVerification,
        AccountPersonUpdateParamsVerificationAdditionalDocument as AccountPersonUpdateParamsVerificationAdditionalDocument,
        AccountPersonUpdateParamsVerificationDocument as AccountPersonUpdateParamsVerificationDocument,
    )
    from stripe.params._account_persons_params import (
        AccountPersonsParams as AccountPersonsParams,
        AccountPersonsParamsRelationship as AccountPersonsParamsRelationship,
    )
    from stripe.params._account_reject_params import (
        AccountRejectParams as AccountRejectParams,
    )
    from stripe.params._account_retrieve_capability_params import (
        AccountRetrieveCapabilityParams as AccountRetrieveCapabilityParams,
    )
    from stripe.params._account_retrieve_current_params import (
        AccountRetrieveCurrentParams as AccountRetrieveCurrentParams,
    )
    from stripe.params._account_retrieve_external_account_params import (
        AccountRetrieveExternalAccountParams as AccountRetrieveExternalAccountParams,
    )
    from stripe.params._account_retrieve_params import (
        AccountRetrieveParams as AccountRetrieveParams,
    )
    from stripe.params._account_retrieve_person_params import (
        AccountRetrievePersonParams as AccountRetrievePersonParams,
    )
    from stripe.params._account_session_create_params import (
        AccountSessionCreateParams as AccountSessionCreateParams,
        AccountSessionCreateParamsComponents as AccountSessionCreateParamsComponents,
        AccountSessionCreateParamsComponentsAccountManagement as AccountSessionCreateParamsComponentsAccountManagement,
        AccountSessionCreateParamsComponentsAccountManagementFeatures as AccountSessionCreateParamsComponentsAccountManagementFeatures,
        AccountSessionCreateParamsComponentsAccountOnboarding as AccountSessionCreateParamsComponentsAccountOnboarding,
        AccountSessionCreateParamsComponentsAccountOnboardingFeatures as AccountSessionCreateParamsComponentsAccountOnboardingFeatures,
        AccountSessionCreateParamsComponentsBalances as AccountSessionCreateParamsComponentsBalances,
        AccountSessionCreateParamsComponentsBalancesFeatures as AccountSessionCreateParamsComponentsBalancesFeatures,
        AccountSessionCreateParamsComponentsDisputesList as AccountSessionCreateParamsComponentsDisputesList,
        AccountSessionCreateParamsComponentsDisputesListFeatures as AccountSessionCreateParamsComponentsDisputesListFeatures,
        AccountSessionCreateParamsComponentsDocuments as AccountSessionCreateParamsComponentsDocuments,
        AccountSessionCreateParamsComponentsDocumentsFeatures as AccountSessionCreateParamsComponentsDocumentsFeatures,
        AccountSessionCreateParamsComponentsFinancialAccount as AccountSessionCreateParamsComponentsFinancialAccount,
        AccountSessionCreateParamsComponentsFinancialAccountFeatures as AccountSessionCreateParamsComponentsFinancialAccountFeatures,
        AccountSessionCreateParamsComponentsFinancialAccountTransactions as AccountSessionCreateParamsComponentsFinancialAccountTransactions,
        AccountSessionCreateParamsComponentsFinancialAccountTransactionsFeatures as AccountSessionCreateParamsComponentsFinancialAccountTransactionsFeatures,
        AccountSessionCreateParamsComponentsInstantPayoutsPromotion as AccountSessionCreateParamsComponentsInstantPayoutsPromotion,
        AccountSessionCreateParamsComponentsInstantPayoutsPromotionFeatures as AccountSessionCreateParamsComponentsInstantPayoutsPromotionFeatures,
        AccountSessionCreateParamsComponentsIssuingCard as AccountSessionCreateParamsComponentsIssuingCard,
        AccountSessionCreateParamsComponentsIssuingCardFeatures as AccountSessionCreateParamsComponentsIssuingCardFeatures,
        AccountSessionCreateParamsComponentsIssuingCardsList as AccountSessionCreateParamsComponentsIssuingCardsList,
        AccountSessionCreateParamsComponentsIssuingCardsListFeatures as AccountSessionCreateParamsComponentsIssuingCardsListFeatures,
        AccountSessionCreateParamsComponentsNotificationBanner as AccountSessionCreateParamsComponentsNotificationBanner,
        AccountSessionCreateParamsComponentsNotificationBannerFeatures as AccountSessionCreateParamsComponentsNotificationBannerFeatures,
        AccountSessionCreateParamsComponentsPaymentDetails as AccountSessionCreateParamsComponentsPaymentDetails,
        AccountSessionCreateParamsComponentsPaymentDetailsFeatures as AccountSessionCreateParamsComponentsPaymentDetailsFeatures,
        AccountSessionCreateParamsComponentsPaymentDisputes as AccountSessionCreateParamsComponentsPaymentDisputes,
        AccountSessionCreateParamsComponentsPaymentDisputesFeatures as AccountSessionCreateParamsComponentsPaymentDisputesFeatures,
        AccountSessionCreateParamsComponentsPayments as AccountSessionCreateParamsComponentsPayments,
        AccountSessionCreateParamsComponentsPaymentsFeatures as AccountSessionCreateParamsComponentsPaymentsFeatures,
        AccountSessionCreateParamsComponentsPayoutDetails as AccountSessionCreateParamsComponentsPayoutDetails,
        AccountSessionCreateParamsComponentsPayoutDetailsFeatures as AccountSessionCreateParamsComponentsPayoutDetailsFeatures,
        AccountSessionCreateParamsComponentsPayouts as AccountSessionCreateParamsComponentsPayouts,
        AccountSessionCreateParamsComponentsPayoutsFeatures as AccountSessionCreateParamsComponentsPayoutsFeatures,
        AccountSessionCreateParamsComponentsPayoutsList as AccountSessionCreateParamsComponentsPayoutsList,
        AccountSessionCreateParamsComponentsPayoutsListFeatures as AccountSessionCreateParamsComponentsPayoutsListFeatures,
        AccountSessionCreateParamsComponentsTaxRegistrations as AccountSessionCreateParamsComponentsTaxRegistrations,
        AccountSessionCreateParamsComponentsTaxRegistrationsFeatures as AccountSessionCreateParamsComponentsTaxRegistrationsFeatures,
        AccountSessionCreateParamsComponentsTaxSettings as AccountSessionCreateParamsComponentsTaxSettings,
        AccountSessionCreateParamsComponentsTaxSettingsFeatures as AccountSessionCreateParamsComponentsTaxSettingsFeatures,
    )
    from stripe.params._account_update_params import (
        AccountUpdateParams as AccountUpdateParams,
        AccountUpdateParamsBankAccount as AccountUpdateParamsBankAccount,
        AccountUpdateParamsBusinessProfile as AccountUpdateParamsBusinessProfile,
        AccountUpdateParamsBusinessProfileAnnualRevenue as AccountUpdateParamsBusinessProfileAnnualRevenue,
        AccountUpdateParamsBusinessProfileMonthlyEstimatedRevenue as AccountUpdateParamsBusinessProfileMonthlyEstimatedRevenue,
        AccountUpdateParamsBusinessProfileSupportAddress as AccountUpdateParamsBusinessProfileSupportAddress,
        AccountUpdateParamsCapabilities as AccountUpdateParamsCapabilities,
        AccountUpdateParamsCapabilitiesAcssDebitPayments as AccountUpdateParamsCapabilitiesAcssDebitPayments,
        AccountUpdateParamsCapabilitiesAffirmPayments as AccountUpdateParamsCapabilitiesAffirmPayments,
        AccountUpdateParamsCapabilitiesAfterpayClearpayPayments as AccountUpdateParamsCapabilitiesAfterpayClearpayPayments,
        AccountUpdateParamsCapabilitiesAlmaPayments as AccountUpdateParamsCapabilitiesAlmaPayments,
        AccountUpdateParamsCapabilitiesAmazonPayPayments as AccountUpdateParamsCapabilitiesAmazonPayPayments,
        AccountUpdateParamsCapabilitiesAuBecsDebitPayments as AccountUpdateParamsCapabilitiesAuBecsDebitPayments,
        AccountUpdateParamsCapabilitiesBacsDebitPayments as AccountUpdateParamsCapabilitiesBacsDebitPayments,
        AccountUpdateParamsCapabilitiesBancontactPayments as AccountUpdateParamsCapabilitiesBancontactPayments,
        AccountUpdateParamsCapabilitiesBankTransferPayments as AccountUpdateParamsCapabilitiesBankTransferPayments,
        AccountUpdateParamsCapabilitiesBilliePayments as AccountUpdateParamsCapabilitiesBilliePayments,
        AccountUpdateParamsCapabilitiesBlikPayments as AccountUpdateParamsCapabilitiesBlikPayments,
        AccountUpdateParamsCapabilitiesBoletoPayments as AccountUpdateParamsCapabilitiesBoletoPayments,
        AccountUpdateParamsCapabilitiesCardIssuing as AccountUpdateParamsCapabilitiesCardIssuing,
        AccountUpdateParamsCapabilitiesCardPayments as AccountUpdateParamsCapabilitiesCardPayments,
        AccountUpdateParamsCapabilitiesCartesBancairesPayments as AccountUpdateParamsCapabilitiesCartesBancairesPayments,
        AccountUpdateParamsCapabilitiesCashappPayments as AccountUpdateParamsCapabilitiesCashappPayments,
        AccountUpdateParamsCapabilitiesCryptoPayments as AccountUpdateParamsCapabilitiesCryptoPayments,
        AccountUpdateParamsCapabilitiesEpsPayments as AccountUpdateParamsCapabilitiesEpsPayments,
        AccountUpdateParamsCapabilitiesFpxPayments as AccountUpdateParamsCapabilitiesFpxPayments,
        AccountUpdateParamsCapabilitiesGbBankTransferPayments as AccountUpdateParamsCapabilitiesGbBankTransferPayments,
        AccountUpdateParamsCapabilitiesGiropayPayments as AccountUpdateParamsCapabilitiesGiropayPayments,
        AccountUpdateParamsCapabilitiesGrabpayPayments as AccountUpdateParamsCapabilitiesGrabpayPayments,
        AccountUpdateParamsCapabilitiesIdealPayments as AccountUpdateParamsCapabilitiesIdealPayments,
        AccountUpdateParamsCapabilitiesIndiaInternationalPayments as AccountUpdateParamsCapabilitiesIndiaInternationalPayments,
        AccountUpdateParamsCapabilitiesJcbPayments as AccountUpdateParamsCapabilitiesJcbPayments,
        AccountUpdateParamsCapabilitiesJpBankTransferPayments as AccountUpdateParamsCapabilitiesJpBankTransferPayments,
        AccountUpdateParamsCapabilitiesKakaoPayPayments as AccountUpdateParamsCapabilitiesKakaoPayPayments,
        AccountUpdateParamsCapabilitiesKlarnaPayments as AccountUpdateParamsCapabilitiesKlarnaPayments,
        AccountUpdateParamsCapabilitiesKonbiniPayments as AccountUpdateParamsCapabilitiesKonbiniPayments,
        AccountUpdateParamsCapabilitiesKrCardPayments as AccountUpdateParamsCapabilitiesKrCardPayments,
        AccountUpdateParamsCapabilitiesLegacyPayments as AccountUpdateParamsCapabilitiesLegacyPayments,
        AccountUpdateParamsCapabilitiesLinkPayments as AccountUpdateParamsCapabilitiesLinkPayments,
        AccountUpdateParamsCapabilitiesMbWayPayments as AccountUpdateParamsCapabilitiesMbWayPayments,
        AccountUpdateParamsCapabilitiesMobilepayPayments as AccountUpdateParamsCapabilitiesMobilepayPayments,
        AccountUpdateParamsCapabilitiesMultibancoPayments as AccountUpdateParamsCapabilitiesMultibancoPayments,
        AccountUpdateParamsCapabilitiesMxBankTransferPayments as AccountUpdateParamsCapabilitiesMxBankTransferPayments,
        AccountUpdateParamsCapabilitiesNaverPayPayments as AccountUpdateParamsCapabilitiesNaverPayPayments,
        AccountUpdateParamsCapabilitiesNzBankAccountBecsDebitPayments as AccountUpdateParamsCapabilitiesNzBankAccountBecsDebitPayments,
        AccountUpdateParamsCapabilitiesOxxoPayments as AccountUpdateParamsCapabilitiesOxxoPayments,
        AccountUpdateParamsCapabilitiesP24Payments as AccountUpdateParamsCapabilitiesP24Payments,
        AccountUpdateParamsCapabilitiesPayByBankPayments as AccountUpdateParamsCapabilitiesPayByBankPayments,
        AccountUpdateParamsCapabilitiesPaycoPayments as AccountUpdateParamsCapabilitiesPaycoPayments,
        AccountUpdateParamsCapabilitiesPaynowPayments as AccountUpdateParamsCapabilitiesPaynowPayments,
        AccountUpdateParamsCapabilitiesPaytoPayments as AccountUpdateParamsCapabilitiesPaytoPayments,
        AccountUpdateParamsCapabilitiesPixPayments as AccountUpdateParamsCapabilitiesPixPayments,
        AccountUpdateParamsCapabilitiesPromptpayPayments as AccountUpdateParamsCapabilitiesPromptpayPayments,
        AccountUpdateParamsCapabilitiesRevolutPayPayments as AccountUpdateParamsCapabilitiesRevolutPayPayments,
        AccountUpdateParamsCapabilitiesSamsungPayPayments as AccountUpdateParamsCapabilitiesSamsungPayPayments,
        AccountUpdateParamsCapabilitiesSatispayPayments as AccountUpdateParamsCapabilitiesSatispayPayments,
        AccountUpdateParamsCapabilitiesSepaBankTransferPayments as AccountUpdateParamsCapabilitiesSepaBankTransferPayments,
        AccountUpdateParamsCapabilitiesSepaDebitPayments as AccountUpdateParamsCapabilitiesSepaDebitPayments,
        AccountUpdateParamsCapabilitiesSofortPayments as AccountUpdateParamsCapabilitiesSofortPayments,
        AccountUpdateParamsCapabilitiesSwishPayments as AccountUpdateParamsCapabilitiesSwishPayments,
        AccountUpdateParamsCapabilitiesTaxReportingUs1099K as AccountUpdateParamsCapabilitiesTaxReportingUs1099K,
        AccountUpdateParamsCapabilitiesTaxReportingUs1099Misc as AccountUpdateParamsCapabilitiesTaxReportingUs1099Misc,
        AccountUpdateParamsCapabilitiesTransfers as AccountUpdateParamsCapabilitiesTransfers,
        AccountUpdateParamsCapabilitiesTreasury as AccountUpdateParamsCapabilitiesTreasury,
        AccountUpdateParamsCapabilitiesTwintPayments as AccountUpdateParamsCapabilitiesTwintPayments,
        AccountUpdateParamsCapabilitiesUsBankAccountAchPayments as AccountUpdateParamsCapabilitiesUsBankAccountAchPayments,
        AccountUpdateParamsCapabilitiesUsBankTransferPayments as AccountUpdateParamsCapabilitiesUsBankTransferPayments,
        AccountUpdateParamsCapabilitiesZipPayments as AccountUpdateParamsCapabilitiesZipPayments,
        AccountUpdateParamsCard as AccountUpdateParamsCard,
        AccountUpdateParamsCardToken as AccountUpdateParamsCardToken,
        AccountUpdateParamsCompany as AccountUpdateParamsCompany,
        AccountUpdateParamsCompanyAddress as AccountUpdateParamsCompanyAddress,
        AccountUpdateParamsCompanyAddressKana as AccountUpdateParamsCompanyAddressKana,
        AccountUpdateParamsCompanyAddressKanji as AccountUpdateParamsCompanyAddressKanji,
        AccountUpdateParamsCompanyDirectorshipDeclaration as AccountUpdateParamsCompanyDirectorshipDeclaration,
        AccountUpdateParamsCompanyOwnershipDeclaration as AccountUpdateParamsCompanyOwnershipDeclaration,
        AccountUpdateParamsCompanyRegistrationDate as AccountUpdateParamsCompanyRegistrationDate,
        AccountUpdateParamsCompanyRepresentativeDeclaration as AccountUpdateParamsCompanyRepresentativeDeclaration,
        AccountUpdateParamsCompanyVerification as AccountUpdateParamsCompanyVerification,
        AccountUpdateParamsCompanyVerificationDocument as AccountUpdateParamsCompanyVerificationDocument,
        AccountUpdateParamsDocuments as AccountUpdateParamsDocuments,
        AccountUpdateParamsDocumentsBankAccountOwnershipVerification as AccountUpdateParamsDocumentsBankAccountOwnershipVerification,
        AccountUpdateParamsDocumentsCompanyLicense as AccountUpdateParamsDocumentsCompanyLicense,
        AccountUpdateParamsDocumentsCompanyMemorandumOfAssociation as AccountUpdateParamsDocumentsCompanyMemorandumOfAssociation,
        AccountUpdateParamsDocumentsCompanyMinisterialDecree as AccountUpdateParamsDocumentsCompanyMinisterialDecree,
        AccountUpdateParamsDocumentsCompanyRegistrationVerification as AccountUpdateParamsDocumentsCompanyRegistrationVerification,
        AccountUpdateParamsDocumentsCompanyTaxIdVerification as AccountUpdateParamsDocumentsCompanyTaxIdVerification,
        AccountUpdateParamsDocumentsProofOfAddress as AccountUpdateParamsDocumentsProofOfAddress,
        AccountUpdateParamsDocumentsProofOfRegistration as AccountUpdateParamsDocumentsProofOfRegistration,
        AccountUpdateParamsDocumentsProofOfRegistrationSigner as AccountUpdateParamsDocumentsProofOfRegistrationSigner,
        AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnership as AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnership,
        AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner as AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner,
        AccountUpdateParamsGroups as AccountUpdateParamsGroups,
        AccountUpdateParamsIndividual as AccountUpdateParamsIndividual,
        AccountUpdateParamsIndividualAddress as AccountUpdateParamsIndividualAddress,
        AccountUpdateParamsIndividualAddressKana as AccountUpdateParamsIndividualAddressKana,
        AccountUpdateParamsIndividualAddressKanji as AccountUpdateParamsIndividualAddressKanji,
        AccountUpdateParamsIndividualDob as AccountUpdateParamsIndividualDob,
        AccountUpdateParamsIndividualRegisteredAddress as AccountUpdateParamsIndividualRegisteredAddress,
        AccountUpdateParamsIndividualRelationship as AccountUpdateParamsIndividualRelationship,
        AccountUpdateParamsIndividualVerification as AccountUpdateParamsIndividualVerification,
        AccountUpdateParamsIndividualVerificationAdditionalDocument as AccountUpdateParamsIndividualVerificationAdditionalDocument,
        AccountUpdateParamsIndividualVerificationDocument as AccountUpdateParamsIndividualVerificationDocument,
        AccountUpdateParamsSettings as AccountUpdateParamsSettings,
        AccountUpdateParamsSettingsBacsDebitPayments as AccountUpdateParamsSettingsBacsDebitPayments,
        AccountUpdateParamsSettingsBranding as AccountUpdateParamsSettingsBranding,
        AccountUpdateParamsSettingsCardIssuing as AccountUpdateParamsSettingsCardIssuing,
        AccountUpdateParamsSettingsCardIssuingTosAcceptance as AccountUpdateParamsSettingsCardIssuingTosAcceptance,
        AccountUpdateParamsSettingsCardPayments as AccountUpdateParamsSettingsCardPayments,
        AccountUpdateParamsSettingsCardPaymentsDeclineOn as AccountUpdateParamsSettingsCardPaymentsDeclineOn,
        AccountUpdateParamsSettingsInvoices as AccountUpdateParamsSettingsInvoices,
        AccountUpdateParamsSettingsPayments as AccountUpdateParamsSettingsPayments,
        AccountUpdateParamsSettingsPayouts as AccountUpdateParamsSettingsPayouts,
        AccountUpdateParamsSettingsPayoutsSchedule as AccountUpdateParamsSettingsPayoutsSchedule,
        AccountUpdateParamsSettingsTreasury as AccountUpdateParamsSettingsTreasury,
        AccountUpdateParamsSettingsTreasuryTosAcceptance as AccountUpdateParamsSettingsTreasuryTosAcceptance,
        AccountUpdateParamsTosAcceptance as AccountUpdateParamsTosAcceptance,
    )
    from stripe.params._apple_pay_domain_create_params import (
        ApplePayDomainCreateParams as ApplePayDomainCreateParams,
    )
    from stripe.params._apple_pay_domain_delete_params import (
        ApplePayDomainDeleteParams as ApplePayDomainDeleteParams,
    )
    from stripe.params._apple_pay_domain_list_params import (
        ApplePayDomainListParams as ApplePayDomainListParams,
    )
    from stripe.params._apple_pay_domain_retrieve_params import (
        ApplePayDomainRetrieveParams as ApplePayDomainRetrieveParams,
    )
    from stripe.params._application_fee_create_refund_params import (
        ApplicationFeeCreateRefundParams as ApplicationFeeCreateRefundParams,
    )
    from stripe.params._application_fee_list_params import (
        ApplicationFeeListParams as ApplicationFeeListParams,
        ApplicationFeeListParamsCreated as ApplicationFeeListParamsCreated,
    )
    from stripe.params._application_fee_list_refunds_params import (
        ApplicationFeeListRefundsParams as ApplicationFeeListRefundsParams,
    )
    from stripe.params._application_fee_modify_refund_params import (
        ApplicationFeeModifyRefundParams as ApplicationFeeModifyRefundParams,
    )
    from stripe.params._application_fee_refund_create_params import (
        ApplicationFeeRefundCreateParams as ApplicationFeeRefundCreateParams,
    )
    from stripe.params._application_fee_refund_list_params import (
        ApplicationFeeRefundListParams as ApplicationFeeRefundListParams,
    )
    from stripe.params._application_fee_refund_params import (
        ApplicationFeeRefundParams as ApplicationFeeRefundParams,
    )
    from stripe.params._application_fee_refund_retrieve_params import (
        ApplicationFeeRefundRetrieveParams as ApplicationFeeRefundRetrieveParams,
    )
    from stripe.params._application_fee_refund_update_params import (
        ApplicationFeeRefundUpdateParams as ApplicationFeeRefundUpdateParams,
    )
    from stripe.params._application_fee_retrieve_params import (
        ApplicationFeeRetrieveParams as ApplicationFeeRetrieveParams,
    )
    from stripe.params._application_fee_retrieve_refund_params import (
        ApplicationFeeRetrieveRefundParams as ApplicationFeeRetrieveRefundParams,
    )
    from stripe.params._balance_retrieve_params import (
        BalanceRetrieveParams as BalanceRetrieveParams,
    )
    from stripe.params._balance_settings_modify_params import (
        BalanceSettingsModifyParams as BalanceSettingsModifyParams,
        BalanceSettingsModifyParamsPayments as BalanceSettingsModifyParamsPayments,
        BalanceSettingsModifyParamsPaymentsPayouts as BalanceSettingsModifyParamsPaymentsPayouts,
        BalanceSettingsModifyParamsPaymentsPayoutsSchedule as BalanceSettingsModifyParamsPaymentsPayoutsSchedule,
        BalanceSettingsModifyParamsPaymentsSettlementTiming as BalanceSettingsModifyParamsPaymentsSettlementTiming,
    )
    from stripe.params._balance_settings_retrieve_params import (
        BalanceSettingsRetrieveParams as BalanceSettingsRetrieveParams,
    )
    from stripe.params._balance_settings_update_params import (
        BalanceSettingsUpdateParams as BalanceSettingsUpdateParams,
        BalanceSettingsUpdateParamsPayments as BalanceSettingsUpdateParamsPayments,
        BalanceSettingsUpdateParamsPaymentsPayouts as BalanceSettingsUpdateParamsPaymentsPayouts,
        BalanceSettingsUpdateParamsPaymentsPayoutsSchedule as BalanceSettingsUpdateParamsPaymentsPayoutsSchedule,
        BalanceSettingsUpdateParamsPaymentsSettlementTiming as BalanceSettingsUpdateParamsPaymentsSettlementTiming,
    )
    from stripe.params._balance_transaction_list_params import (
        BalanceTransactionListParams as BalanceTransactionListParams,
        BalanceTransactionListParamsCreated as BalanceTransactionListParamsCreated,
    )
    from stripe.params._balance_transaction_retrieve_params import (
        BalanceTransactionRetrieveParams as BalanceTransactionRetrieveParams,
    )
    from stripe.params._bank_account_delete_params import (
        BankAccountDeleteParams as BankAccountDeleteParams,
    )
    from stripe.params._card_delete_params import (
        CardDeleteParams as CardDeleteParams,
    )
    from stripe.params._charge_capture_params import (
        ChargeCaptureParams as ChargeCaptureParams,
        ChargeCaptureParamsTransferData as ChargeCaptureParamsTransferData,
    )
    from stripe.params._charge_create_params import (
        ChargeCreateParams as ChargeCreateParams,
        ChargeCreateParamsDestination as ChargeCreateParamsDestination,
        ChargeCreateParamsRadarOptions as ChargeCreateParamsRadarOptions,
        ChargeCreateParamsShipping as ChargeCreateParamsShipping,
        ChargeCreateParamsShippingAddress as ChargeCreateParamsShippingAddress,
        ChargeCreateParamsTransferData as ChargeCreateParamsTransferData,
    )
    from stripe.params._charge_list_params import (
        ChargeListParams as ChargeListParams,
        ChargeListParamsCreated as ChargeListParamsCreated,
    )
    from stripe.params._charge_list_refunds_params import (
        ChargeListRefundsParams as ChargeListRefundsParams,
    )
    from stripe.params._charge_modify_params import (
        ChargeModifyParams as ChargeModifyParams,
        ChargeModifyParamsFraudDetails as ChargeModifyParamsFraudDetails,
        ChargeModifyParamsShipping as ChargeModifyParamsShipping,
        ChargeModifyParamsShippingAddress as ChargeModifyParamsShippingAddress,
    )
    from stripe.params._charge_retrieve_params import (
        ChargeRetrieveParams as ChargeRetrieveParams,
    )
    from stripe.params._charge_retrieve_refund_params import (
        ChargeRetrieveRefundParams as ChargeRetrieveRefundParams,
    )
    from stripe.params._charge_search_params import (
        ChargeSearchParams as ChargeSearchParams,
    )
    from stripe.params._charge_update_params import (
        ChargeUpdateParams as ChargeUpdateParams,
        ChargeUpdateParamsFraudDetails as ChargeUpdateParamsFraudDetails,
        ChargeUpdateParamsShipping as ChargeUpdateParamsShipping,
        ChargeUpdateParamsShippingAddress as ChargeUpdateParamsShippingAddress,
    )
    from stripe.params._confirmation_token_create_params import (
        ConfirmationTokenCreateParams as ConfirmationTokenCreateParams,
        ConfirmationTokenCreateParamsPaymentMethodData as ConfirmationTokenCreateParamsPaymentMethodData,
        ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit as ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataAffirm as ConfirmationTokenCreateParamsPaymentMethodDataAffirm,
        ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay as ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay,
        ConfirmationTokenCreateParamsPaymentMethodDataAlipay as ConfirmationTokenCreateParamsPaymentMethodDataAlipay,
        ConfirmationTokenCreateParamsPaymentMethodDataAlma as ConfirmationTokenCreateParamsPaymentMethodDataAlma,
        ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay as ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay,
        ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit as ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit as ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataBancontact as ConfirmationTokenCreateParamsPaymentMethodDataBancontact,
        ConfirmationTokenCreateParamsPaymentMethodDataBillie as ConfirmationTokenCreateParamsPaymentMethodDataBillie,
        ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails as ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails,
        ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress as ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress,
        ConfirmationTokenCreateParamsPaymentMethodDataBlik as ConfirmationTokenCreateParamsPaymentMethodDataBlik,
        ConfirmationTokenCreateParamsPaymentMethodDataBoleto as ConfirmationTokenCreateParamsPaymentMethodDataBoleto,
        ConfirmationTokenCreateParamsPaymentMethodDataCashapp as ConfirmationTokenCreateParamsPaymentMethodDataCashapp,
        ConfirmationTokenCreateParamsPaymentMethodDataCrypto as ConfirmationTokenCreateParamsPaymentMethodDataCrypto,
        ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance as ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance,
        ConfirmationTokenCreateParamsPaymentMethodDataEps as ConfirmationTokenCreateParamsPaymentMethodDataEps,
        ConfirmationTokenCreateParamsPaymentMethodDataFpx as ConfirmationTokenCreateParamsPaymentMethodDataFpx,
        ConfirmationTokenCreateParamsPaymentMethodDataGiropay as ConfirmationTokenCreateParamsPaymentMethodDataGiropay,
        ConfirmationTokenCreateParamsPaymentMethodDataGrabpay as ConfirmationTokenCreateParamsPaymentMethodDataGrabpay,
        ConfirmationTokenCreateParamsPaymentMethodDataIdeal as ConfirmationTokenCreateParamsPaymentMethodDataIdeal,
        ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent as ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent,
        ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay as ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay,
        ConfirmationTokenCreateParamsPaymentMethodDataKlarna as ConfirmationTokenCreateParamsPaymentMethodDataKlarna,
        ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob as ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob,
        ConfirmationTokenCreateParamsPaymentMethodDataKonbini as ConfirmationTokenCreateParamsPaymentMethodDataKonbini,
        ConfirmationTokenCreateParamsPaymentMethodDataKrCard as ConfirmationTokenCreateParamsPaymentMethodDataKrCard,
        ConfirmationTokenCreateParamsPaymentMethodDataLink as ConfirmationTokenCreateParamsPaymentMethodDataLink,
        ConfirmationTokenCreateParamsPaymentMethodDataMbWay as ConfirmationTokenCreateParamsPaymentMethodDataMbWay,
        ConfirmationTokenCreateParamsPaymentMethodDataMobilepay as ConfirmationTokenCreateParamsPaymentMethodDataMobilepay,
        ConfirmationTokenCreateParamsPaymentMethodDataMultibanco as ConfirmationTokenCreateParamsPaymentMethodDataMultibanco,
        ConfirmationTokenCreateParamsPaymentMethodDataNaverPay as ConfirmationTokenCreateParamsPaymentMethodDataNaverPay,
        ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount as ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount,
        ConfirmationTokenCreateParamsPaymentMethodDataOxxo as ConfirmationTokenCreateParamsPaymentMethodDataOxxo,
        ConfirmationTokenCreateParamsPaymentMethodDataP24 as ConfirmationTokenCreateParamsPaymentMethodDataP24,
        ConfirmationTokenCreateParamsPaymentMethodDataPayByBank as ConfirmationTokenCreateParamsPaymentMethodDataPayByBank,
        ConfirmationTokenCreateParamsPaymentMethodDataPayco as ConfirmationTokenCreateParamsPaymentMethodDataPayco,
        ConfirmationTokenCreateParamsPaymentMethodDataPaynow as ConfirmationTokenCreateParamsPaymentMethodDataPaynow,
        ConfirmationTokenCreateParamsPaymentMethodDataPaypal as ConfirmationTokenCreateParamsPaymentMethodDataPaypal,
        ConfirmationTokenCreateParamsPaymentMethodDataPayto as ConfirmationTokenCreateParamsPaymentMethodDataPayto,
        ConfirmationTokenCreateParamsPaymentMethodDataPix as ConfirmationTokenCreateParamsPaymentMethodDataPix,
        ConfirmationTokenCreateParamsPaymentMethodDataPromptpay as ConfirmationTokenCreateParamsPaymentMethodDataPromptpay,
        ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions as ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions,
        ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay as ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay,
        ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay as ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay,
        ConfirmationTokenCreateParamsPaymentMethodDataSatispay as ConfirmationTokenCreateParamsPaymentMethodDataSatispay,
        ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit as ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit,
        ConfirmationTokenCreateParamsPaymentMethodDataSofort as ConfirmationTokenCreateParamsPaymentMethodDataSofort,
        ConfirmationTokenCreateParamsPaymentMethodDataSwish as ConfirmationTokenCreateParamsPaymentMethodDataSwish,
        ConfirmationTokenCreateParamsPaymentMethodDataTwint as ConfirmationTokenCreateParamsPaymentMethodDataTwint,
        ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount as ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount,
        ConfirmationTokenCreateParamsPaymentMethodDataWechatPay as ConfirmationTokenCreateParamsPaymentMethodDataWechatPay,
        ConfirmationTokenCreateParamsPaymentMethodDataZip as ConfirmationTokenCreateParamsPaymentMethodDataZip,
        ConfirmationTokenCreateParamsPaymentMethodOptions as ConfirmationTokenCreateParamsPaymentMethodOptions,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCard as ConfirmationTokenCreateParamsPaymentMethodOptionsCard,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments as ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments,
        ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan as ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan,
        ConfirmationTokenCreateParamsShipping as ConfirmationTokenCreateParamsShipping,
        ConfirmationTokenCreateParamsShippingAddress as ConfirmationTokenCreateParamsShippingAddress,
    )
    from stripe.params._confirmation_token_retrieve_params import (
        ConfirmationTokenRetrieveParams as ConfirmationTokenRetrieveParams,
    )
    from stripe.params._country_spec_list_params import (
        CountrySpecListParams as CountrySpecListParams,
    )
    from stripe.params._country_spec_retrieve_params import (
        CountrySpecRetrieveParams as CountrySpecRetrieveParams,
    )
    from stripe.params._coupon_create_params import (
        CouponCreateParams as CouponCreateParams,
        CouponCreateParamsAppliesTo as CouponCreateParamsAppliesTo,
        CouponCreateParamsCurrencyOptions as CouponCreateParamsCurrencyOptions,
    )
    from stripe.params._coupon_delete_params import (
        CouponDeleteParams as CouponDeleteParams,
    )
    from stripe.params._coupon_list_params import (
        CouponListParams as CouponListParams,
        CouponListParamsCreated as CouponListParamsCreated,
    )
    from stripe.params._coupon_modify_params import (
        CouponModifyParams as CouponModifyParams,
        CouponModifyParamsCurrencyOptions as CouponModifyParamsCurrencyOptions,
    )
    from stripe.params._coupon_retrieve_params import (
        CouponRetrieveParams as CouponRetrieveParams,
    )
    from stripe.params._coupon_update_params import (
        CouponUpdateParams as CouponUpdateParams,
        CouponUpdateParamsCurrencyOptions as CouponUpdateParamsCurrencyOptions,
    )
    from stripe.params._credit_note_create_params import (
        CreditNoteCreateParams as CreditNoteCreateParams,
        CreditNoteCreateParamsLine as CreditNoteCreateParamsLine,
        CreditNoteCreateParamsLineTaxAmount as CreditNoteCreateParamsLineTaxAmount,
        CreditNoteCreateParamsRefund as CreditNoteCreateParamsRefund,
        CreditNoteCreateParamsRefundPaymentRecordRefund as CreditNoteCreateParamsRefundPaymentRecordRefund,
        CreditNoteCreateParamsShippingCost as CreditNoteCreateParamsShippingCost,
    )
    from stripe.params._credit_note_line_item_list_params import (
        CreditNoteLineItemListParams as CreditNoteLineItemListParams,
    )
    from stripe.params._credit_note_list_lines_params import (
        CreditNoteListLinesParams as CreditNoteListLinesParams,
    )
    from stripe.params._credit_note_list_params import (
        CreditNoteListParams as CreditNoteListParams,
        CreditNoteListParamsCreated as CreditNoteListParamsCreated,
    )
    from stripe.params._credit_note_modify_params import (
        CreditNoteModifyParams as CreditNoteModifyParams,
    )
    from stripe.params._credit_note_preview_lines_list_params import (
        CreditNotePreviewLinesListParams as CreditNotePreviewLinesListParams,
        CreditNotePreviewLinesListParamsLine as CreditNotePreviewLinesListParamsLine,
        CreditNotePreviewLinesListParamsLineTaxAmount as CreditNotePreviewLinesListParamsLineTaxAmount,
        CreditNotePreviewLinesListParamsRefund as CreditNotePreviewLinesListParamsRefund,
        CreditNotePreviewLinesListParamsRefundPaymentRecordRefund as CreditNotePreviewLinesListParamsRefundPaymentRecordRefund,
        CreditNotePreviewLinesListParamsShippingCost as CreditNotePreviewLinesListParamsShippingCost,
    )
    from stripe.params._credit_note_preview_lines_params import (
        CreditNotePreviewLinesParams as CreditNotePreviewLinesParams,
        CreditNotePreviewLinesParamsLine as CreditNotePreviewLinesParamsLine,
        CreditNotePreviewLinesParamsLineTaxAmount as CreditNotePreviewLinesParamsLineTaxAmount,
        CreditNotePreviewLinesParamsRefund as CreditNotePreviewLinesParamsRefund,
        CreditNotePreviewLinesParamsRefundPaymentRecordRefund as CreditNotePreviewLinesParamsRefundPaymentRecordRefund,
        CreditNotePreviewLinesParamsShippingCost as CreditNotePreviewLinesParamsShippingCost,
    )
    from stripe.params._credit_note_preview_params import (
        CreditNotePreviewParams as CreditNotePreviewParams,
        CreditNotePreviewParamsLine as CreditNotePreviewParamsLine,
        CreditNotePreviewParamsLineTaxAmount as CreditNotePreviewParamsLineTaxAmount,
        CreditNotePreviewParamsRefund as CreditNotePreviewParamsRefund,
        CreditNotePreviewParamsRefundPaymentRecordRefund as CreditNotePreviewParamsRefundPaymentRecordRefund,
        CreditNotePreviewParamsShippingCost as CreditNotePreviewParamsShippingCost,
    )
    from stripe.params._credit_note_retrieve_params import (
        CreditNoteRetrieveParams as CreditNoteRetrieveParams,
    )
    from stripe.params._credit_note_update_params import (
        CreditNoteUpdateParams as CreditNoteUpdateParams,
    )
    from stripe.params._credit_note_void_credit_note_params import (
        CreditNoteVoidCreditNoteParams as CreditNoteVoidCreditNoteParams,
    )
    from stripe.params._customer_balance_transaction_create_params import (
        CustomerBalanceTransactionCreateParams as CustomerBalanceTransactionCreateParams,
    )
    from stripe.params._customer_balance_transaction_list_params import (
        CustomerBalanceTransactionListParams as CustomerBalanceTransactionListParams,
        CustomerBalanceTransactionListParamsCreated as CustomerBalanceTransactionListParamsCreated,
    )
    from stripe.params._customer_balance_transaction_retrieve_params import (
        CustomerBalanceTransactionRetrieveParams as CustomerBalanceTransactionRetrieveParams,
    )
    from stripe.params._customer_balance_transaction_update_params import (
        CustomerBalanceTransactionUpdateParams as CustomerBalanceTransactionUpdateParams,
    )
    from stripe.params._customer_cash_balance_retrieve_params import (
        CustomerCashBalanceRetrieveParams as CustomerCashBalanceRetrieveParams,
    )
    from stripe.params._customer_cash_balance_transaction_list_params import (
        CustomerCashBalanceTransactionListParams as CustomerCashBalanceTransactionListParams,
    )
    from stripe.params._customer_cash_balance_transaction_retrieve_params import (
        CustomerCashBalanceTransactionRetrieveParams as CustomerCashBalanceTransactionRetrieveParams,
    )
    from stripe.params._customer_cash_balance_update_params import (
        CustomerCashBalanceUpdateParams as CustomerCashBalanceUpdateParams,
        CustomerCashBalanceUpdateParamsSettings as CustomerCashBalanceUpdateParamsSettings,
    )
    from stripe.params._customer_create_balance_transaction_params import (
        CustomerCreateBalanceTransactionParams as CustomerCreateBalanceTransactionParams,
    )
    from stripe.params._customer_create_funding_instructions_params import (
        CustomerCreateFundingInstructionsParams as CustomerCreateFundingInstructionsParams,
        CustomerCreateFundingInstructionsParamsBankTransfer as CustomerCreateFundingInstructionsParamsBankTransfer,
        CustomerCreateFundingInstructionsParamsBankTransferEuBankTransfer as CustomerCreateFundingInstructionsParamsBankTransferEuBankTransfer,
    )
    from stripe.params._customer_create_params import (
        CustomerCreateParams as CustomerCreateParams,
        CustomerCreateParamsAddress as CustomerCreateParamsAddress,
        CustomerCreateParamsCashBalance as CustomerCreateParamsCashBalance,
        CustomerCreateParamsCashBalanceSettings as CustomerCreateParamsCashBalanceSettings,
        CustomerCreateParamsInvoiceSettings as CustomerCreateParamsInvoiceSettings,
        CustomerCreateParamsInvoiceSettingsCustomField as CustomerCreateParamsInvoiceSettingsCustomField,
        CustomerCreateParamsInvoiceSettingsRenderingOptions as CustomerCreateParamsInvoiceSettingsRenderingOptions,
        CustomerCreateParamsShipping as CustomerCreateParamsShipping,
        CustomerCreateParamsShippingAddress as CustomerCreateParamsShippingAddress,
        CustomerCreateParamsTax as CustomerCreateParamsTax,
        CustomerCreateParamsTaxIdDatum as CustomerCreateParamsTaxIdDatum,
    )
    from stripe.params._customer_create_source_params import (
        CustomerCreateSourceParams as CustomerCreateSourceParams,
    )
    from stripe.params._customer_create_tax_id_params import (
        CustomerCreateTaxIdParams as CustomerCreateTaxIdParams,
    )
    from stripe.params._customer_delete_discount_params import (
        CustomerDeleteDiscountParams as CustomerDeleteDiscountParams,
    )
    from stripe.params._customer_delete_params import (
        CustomerDeleteParams as CustomerDeleteParams,
    )
    from stripe.params._customer_delete_source_params import (
        CustomerDeleteSourceParams as CustomerDeleteSourceParams,
    )
    from stripe.params._customer_delete_tax_id_params import (
        CustomerDeleteTaxIdParams as CustomerDeleteTaxIdParams,
    )
    from stripe.params._customer_fund_cash_balance_params import (
        CustomerFundCashBalanceParams as CustomerFundCashBalanceParams,
    )
    from stripe.params._customer_funding_instructions_create_params import (
        CustomerFundingInstructionsCreateParams as CustomerFundingInstructionsCreateParams,
        CustomerFundingInstructionsCreateParamsBankTransfer as CustomerFundingInstructionsCreateParamsBankTransfer,
        CustomerFundingInstructionsCreateParamsBankTransferEuBankTransfer as CustomerFundingInstructionsCreateParamsBankTransferEuBankTransfer,
    )
    from stripe.params._customer_list_balance_transactions_params import (
        CustomerListBalanceTransactionsParams as CustomerListBalanceTransactionsParams,
        CustomerListBalanceTransactionsParamsCreated as CustomerListBalanceTransactionsParamsCreated,
    )
    from stripe.params._customer_list_cash_balance_transactions_params import (
        CustomerListCashBalanceTransactionsParams as CustomerListCashBalanceTransactionsParams,
    )
    from stripe.params._customer_list_params import (
        CustomerListParams as CustomerListParams,
        CustomerListParamsCreated as CustomerListParamsCreated,
    )
    from stripe.params._customer_list_payment_methods_params import (
        CustomerListPaymentMethodsParams as CustomerListPaymentMethodsParams,
    )
    from stripe.params._customer_list_sources_params import (
        CustomerListSourcesParams as CustomerListSourcesParams,
    )
    from stripe.params._customer_list_tax_ids_params import (
        CustomerListTaxIdsParams as CustomerListTaxIdsParams,
    )
    from stripe.params._customer_modify_balance_transaction_params import (
        CustomerModifyBalanceTransactionParams as CustomerModifyBalanceTransactionParams,
    )
    from stripe.params._customer_modify_cash_balance_params import (
        CustomerModifyCashBalanceParams as CustomerModifyCashBalanceParams,
        CustomerModifyCashBalanceParamsSettings as CustomerModifyCashBalanceParamsSettings,
    )
    from stripe.params._customer_modify_params import (
        CustomerModifyParams as CustomerModifyParams,
        CustomerModifyParamsAddress as CustomerModifyParamsAddress,
        CustomerModifyParamsCashBalance as CustomerModifyParamsCashBalance,
        CustomerModifyParamsCashBalanceSettings as CustomerModifyParamsCashBalanceSettings,
        CustomerModifyParamsInvoiceSettings as CustomerModifyParamsInvoiceSettings,
        CustomerModifyParamsInvoiceSettingsCustomField as CustomerModifyParamsInvoiceSettingsCustomField,
        CustomerModifyParamsInvoiceSettingsRenderingOptions as CustomerModifyParamsInvoiceSettingsRenderingOptions,
        CustomerModifyParamsShipping as CustomerModifyParamsShipping,
        CustomerModifyParamsShippingAddress as CustomerModifyParamsShippingAddress,
        CustomerModifyParamsTax as CustomerModifyParamsTax,
    )
    from stripe.params._customer_modify_source_params import (
        CustomerModifySourceParams as CustomerModifySourceParams,
        CustomerModifySourceParamsOwner as CustomerModifySourceParamsOwner,
        CustomerModifySourceParamsOwnerAddress as CustomerModifySourceParamsOwnerAddress,
    )
    from stripe.params._customer_payment_method_list_params import (
        CustomerPaymentMethodListParams as CustomerPaymentMethodListParams,
    )
    from stripe.params._customer_payment_method_retrieve_params import (
        CustomerPaymentMethodRetrieveParams as CustomerPaymentMethodRetrieveParams,
    )
    from stripe.params._customer_payment_source_create_params import (
        CustomerPaymentSourceCreateParams as CustomerPaymentSourceCreateParams,
    )
    from stripe.params._customer_payment_source_delete_params import (
        CustomerPaymentSourceDeleteParams as CustomerPaymentSourceDeleteParams,
    )
    from stripe.params._customer_payment_source_list_params import (
        CustomerPaymentSourceListParams as CustomerPaymentSourceListParams,
    )
    from stripe.params._customer_payment_source_retrieve_params import (
        CustomerPaymentSourceRetrieveParams as CustomerPaymentSourceRetrieveParams,
    )
    from stripe.params._customer_payment_source_update_params import (
        CustomerPaymentSourceUpdateParams as CustomerPaymentSourceUpdateParams,
        CustomerPaymentSourceUpdateParamsOwner as CustomerPaymentSourceUpdateParamsOwner,
        CustomerPaymentSourceUpdateParamsOwnerAddress as CustomerPaymentSourceUpdateParamsOwnerAddress,
    )
    from stripe.params._customer_payment_source_verify_params import (
        CustomerPaymentSourceVerifyParams as CustomerPaymentSourceVerifyParams,
    )
    from stripe.params._customer_retrieve_balance_transaction_params import (
        CustomerRetrieveBalanceTransactionParams as CustomerRetrieveBalanceTransactionParams,
    )
    from stripe.params._customer_retrieve_cash_balance_params import (
        CustomerRetrieveCashBalanceParams as CustomerRetrieveCashBalanceParams,
    )
    from stripe.params._customer_retrieve_cash_balance_transaction_params import (
        CustomerRetrieveCashBalanceTransactionParams as CustomerRetrieveCashBalanceTransactionParams,
    )
    from stripe.params._customer_retrieve_params import (
        CustomerRetrieveParams as CustomerRetrieveParams,
    )
    from stripe.params._customer_retrieve_payment_method_params import (
        CustomerRetrievePaymentMethodParams as CustomerRetrievePaymentMethodParams,
    )
    from stripe.params._customer_retrieve_source_params import (
        CustomerRetrieveSourceParams as CustomerRetrieveSourceParams,
    )
    from stripe.params._customer_retrieve_tax_id_params import (
        CustomerRetrieveTaxIdParams as CustomerRetrieveTaxIdParams,
    )
    from stripe.params._customer_search_params import (
        CustomerSearchParams as CustomerSearchParams,
    )
    from stripe.params._customer_session_create_params import (
        CustomerSessionCreateParams as CustomerSessionCreateParams,
        CustomerSessionCreateParamsComponents as CustomerSessionCreateParamsComponents,
        CustomerSessionCreateParamsComponentsBuyButton as CustomerSessionCreateParamsComponentsBuyButton,
        CustomerSessionCreateParamsComponentsCustomerSheet as CustomerSessionCreateParamsComponentsCustomerSheet,
        CustomerSessionCreateParamsComponentsCustomerSheetFeatures as CustomerSessionCreateParamsComponentsCustomerSheetFeatures,
        CustomerSessionCreateParamsComponentsMobilePaymentElement as CustomerSessionCreateParamsComponentsMobilePaymentElement,
        CustomerSessionCreateParamsComponentsMobilePaymentElementFeatures as CustomerSessionCreateParamsComponentsMobilePaymentElementFeatures,
        CustomerSessionCreateParamsComponentsPaymentElement as CustomerSessionCreateParamsComponentsPaymentElement,
        CustomerSessionCreateParamsComponentsPaymentElementFeatures as CustomerSessionCreateParamsComponentsPaymentElementFeatures,
        CustomerSessionCreateParamsComponentsPricingTable as CustomerSessionCreateParamsComponentsPricingTable,
    )
    from stripe.params._customer_tax_id_create_params import (
        CustomerTaxIdCreateParams as CustomerTaxIdCreateParams,
    )
    from stripe.params._customer_tax_id_delete_params import (
        CustomerTaxIdDeleteParams as CustomerTaxIdDeleteParams,
    )
    from stripe.params._customer_tax_id_list_params import (
        CustomerTaxIdListParams as CustomerTaxIdListParams,
    )
    from stripe.params._customer_tax_id_retrieve_params import (
        CustomerTaxIdRetrieveParams as CustomerTaxIdRetrieveParams,
    )
    from stripe.params._customer_update_params import (
        CustomerUpdateParams as CustomerUpdateParams,
        CustomerUpdateParamsAddress as CustomerUpdateParamsAddress,
        CustomerUpdateParamsCashBalance as CustomerUpdateParamsCashBalance,
        CustomerUpdateParamsCashBalanceSettings as CustomerUpdateParamsCashBalanceSettings,
        CustomerUpdateParamsInvoiceSettings as CustomerUpdateParamsInvoiceSettings,
        CustomerUpdateParamsInvoiceSettingsCustomField as CustomerUpdateParamsInvoiceSettingsCustomField,
        CustomerUpdateParamsInvoiceSettingsRenderingOptions as CustomerUpdateParamsInvoiceSettingsRenderingOptions,
        CustomerUpdateParamsShipping as CustomerUpdateParamsShipping,
        CustomerUpdateParamsShippingAddress as CustomerUpdateParamsShippingAddress,
        CustomerUpdateParamsTax as CustomerUpdateParamsTax,
    )
    from stripe.params._dispute_close_params import (
        DisputeCloseParams as DisputeCloseParams,
    )
    from stripe.params._dispute_list_params import (
        DisputeListParams as DisputeListParams,
        DisputeListParamsCreated as DisputeListParamsCreated,
    )
    from stripe.params._dispute_modify_params import (
        DisputeModifyParams as DisputeModifyParams,
        DisputeModifyParamsEvidence as DisputeModifyParamsEvidence,
        DisputeModifyParamsEvidenceEnhancedEvidence as DisputeModifyParamsEvidenceEnhancedEvidence,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3 as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress,
        DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompliance as DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompliance,
    )
    from stripe.params._dispute_retrieve_params import (
        DisputeRetrieveParams as DisputeRetrieveParams,
    )
    from stripe.params._dispute_update_params import (
        DisputeUpdateParams as DisputeUpdateParams,
        DisputeUpdateParamsEvidence as DisputeUpdateParamsEvidence,
        DisputeUpdateParamsEvidenceEnhancedEvidence as DisputeUpdateParamsEvidenceEnhancedEvidence,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3 as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress,
        DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompliance as DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompliance,
    )
    from stripe.params._ephemeral_key_create_params import (
        EphemeralKeyCreateParams as EphemeralKeyCreateParams,
    )
    from stripe.params._ephemeral_key_delete_params import (
        EphemeralKeyDeleteParams as EphemeralKeyDeleteParams,
    )
    from stripe.params._event_list_params import (
        EventListParams as EventListParams,
        EventListParamsCreated as EventListParamsCreated,
    )
    from stripe.params._event_retrieve_params import (
        EventRetrieveParams as EventRetrieveParams,
    )
    from stripe.params._exchange_rate_list_params import (
        ExchangeRateListParams as ExchangeRateListParams,
    )
    from stripe.params._exchange_rate_retrieve_params import (
        ExchangeRateRetrieveParams as ExchangeRateRetrieveParams,
    )
    from stripe.params._file_create_params import (
        FileCreateParams as FileCreateParams,
        FileCreateParamsFileLinkData as FileCreateParamsFileLinkData,
    )
    from stripe.params._file_link_create_params import (
        FileLinkCreateParams as FileLinkCreateParams,
    )
    from stripe.params._file_link_list_params import (
        FileLinkListParams as FileLinkListParams,
        FileLinkListParamsCreated as FileLinkListParamsCreated,
    )
    from stripe.params._file_link_modify_params import (
        FileLinkModifyParams as FileLinkModifyParams,
    )
    from stripe.params._file_link_retrieve_params import (
        FileLinkRetrieveParams as FileLinkRetrieveParams,
    )
    from stripe.params._file_link_update_params import (
        FileLinkUpdateParams as FileLinkUpdateParams,
    )
    from stripe.params._file_list_params import (
        FileListParams as FileListParams,
        FileListParamsCreated as FileListParamsCreated,
    )
    from stripe.params._file_retrieve_params import (
        FileRetrieveParams as FileRetrieveParams,
    )
    from stripe.params._invoice_add_lines_params import (
        InvoiceAddLinesParams as InvoiceAddLinesParams,
        InvoiceAddLinesParamsLine as InvoiceAddLinesParamsLine,
        InvoiceAddLinesParamsLineDiscount as InvoiceAddLinesParamsLineDiscount,
        InvoiceAddLinesParamsLinePeriod as InvoiceAddLinesParamsLinePeriod,
        InvoiceAddLinesParamsLinePriceData as InvoiceAddLinesParamsLinePriceData,
        InvoiceAddLinesParamsLinePriceDataProductData as InvoiceAddLinesParamsLinePriceDataProductData,
        InvoiceAddLinesParamsLinePricing as InvoiceAddLinesParamsLinePricing,
        InvoiceAddLinesParamsLineTaxAmount as InvoiceAddLinesParamsLineTaxAmount,
        InvoiceAddLinesParamsLineTaxAmountTaxRateData as InvoiceAddLinesParamsLineTaxAmountTaxRateData,
    )
    from stripe.params._invoice_attach_payment_params import (
        InvoiceAttachPaymentParams as InvoiceAttachPaymentParams,
    )
    from stripe.params._invoice_create_params import (
        InvoiceCreateParams as InvoiceCreateParams,
        InvoiceCreateParamsAutomaticTax as InvoiceCreateParamsAutomaticTax,
        InvoiceCreateParamsAutomaticTaxLiability as InvoiceCreateParamsAutomaticTaxLiability,
        InvoiceCreateParamsCustomField as InvoiceCreateParamsCustomField,
        InvoiceCreateParamsDiscount as InvoiceCreateParamsDiscount,
        InvoiceCreateParamsFromInvoice as InvoiceCreateParamsFromInvoice,
        InvoiceCreateParamsIssuer as InvoiceCreateParamsIssuer,
        InvoiceCreateParamsPaymentSettings as InvoiceCreateParamsPaymentSettings,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptions as InvoiceCreateParamsPaymentSettingsPaymentMethodOptions,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsBancontact as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsBancontact,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCard as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCard,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallments as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallments,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsKonbini as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsKonbini,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPayto as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPayto,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        InvoiceCreateParamsRendering as InvoiceCreateParamsRendering,
        InvoiceCreateParamsRenderingPdf as InvoiceCreateParamsRenderingPdf,
        InvoiceCreateParamsShippingCost as InvoiceCreateParamsShippingCost,
        InvoiceCreateParamsShippingCostShippingRateData as InvoiceCreateParamsShippingCostShippingRateData,
        InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimate as InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimate,
        InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMaximum as InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMaximum,
        InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMinimum as InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMinimum,
        InvoiceCreateParamsShippingCostShippingRateDataFixedAmount as InvoiceCreateParamsShippingCostShippingRateDataFixedAmount,
        InvoiceCreateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions as InvoiceCreateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions,
        InvoiceCreateParamsShippingDetails as InvoiceCreateParamsShippingDetails,
        InvoiceCreateParamsShippingDetailsAddress as InvoiceCreateParamsShippingDetailsAddress,
        InvoiceCreateParamsTransferData as InvoiceCreateParamsTransferData,
    )
    from stripe.params._invoice_create_preview_params import (
        InvoiceCreatePreviewParams as InvoiceCreatePreviewParams,
        InvoiceCreatePreviewParamsAutomaticTax as InvoiceCreatePreviewParamsAutomaticTax,
        InvoiceCreatePreviewParamsAutomaticTaxLiability as InvoiceCreatePreviewParamsAutomaticTaxLiability,
        InvoiceCreatePreviewParamsCustomerDetails as InvoiceCreatePreviewParamsCustomerDetails,
        InvoiceCreatePreviewParamsCustomerDetailsAddress as InvoiceCreatePreviewParamsCustomerDetailsAddress,
        InvoiceCreatePreviewParamsCustomerDetailsShipping as InvoiceCreatePreviewParamsCustomerDetailsShipping,
        InvoiceCreatePreviewParamsCustomerDetailsShippingAddress as InvoiceCreatePreviewParamsCustomerDetailsShippingAddress,
        InvoiceCreatePreviewParamsCustomerDetailsTax as InvoiceCreatePreviewParamsCustomerDetailsTax,
        InvoiceCreatePreviewParamsCustomerDetailsTaxId as InvoiceCreatePreviewParamsCustomerDetailsTaxId,
        InvoiceCreatePreviewParamsDiscount as InvoiceCreatePreviewParamsDiscount,
        InvoiceCreatePreviewParamsInvoiceItem as InvoiceCreatePreviewParamsInvoiceItem,
        InvoiceCreatePreviewParamsInvoiceItemDiscount as InvoiceCreatePreviewParamsInvoiceItemDiscount,
        InvoiceCreatePreviewParamsInvoiceItemPeriod as InvoiceCreatePreviewParamsInvoiceItemPeriod,
        InvoiceCreatePreviewParamsInvoiceItemPriceData as InvoiceCreatePreviewParamsInvoiceItemPriceData,
        InvoiceCreatePreviewParamsIssuer as InvoiceCreatePreviewParamsIssuer,
        InvoiceCreatePreviewParamsScheduleDetails as InvoiceCreatePreviewParamsScheduleDetails,
        InvoiceCreatePreviewParamsScheduleDetailsBillingMode as InvoiceCreatePreviewParamsScheduleDetailsBillingMode,
        InvoiceCreatePreviewParamsScheduleDetailsBillingModeFlexible as InvoiceCreatePreviewParamsScheduleDetailsBillingModeFlexible,
        InvoiceCreatePreviewParamsScheduleDetailsPhase as InvoiceCreatePreviewParamsScheduleDetailsPhase,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItem as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItem,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemDiscount as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemDiscount,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriod as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriod,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodEnd as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodEnd,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodStart as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodStart,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPriceData as InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPriceData,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTax as InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTax,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTaxLiability as InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTaxLiability,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseBillingThresholds as InvoiceCreatePreviewParamsScheduleDetailsPhaseBillingThresholds,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseDiscount as InvoiceCreatePreviewParamsScheduleDetailsPhaseDiscount,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseDuration as InvoiceCreatePreviewParamsScheduleDetailsPhaseDuration,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettings as InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettings,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettingsIssuer as InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettingsIssuer,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseItem as InvoiceCreatePreviewParamsScheduleDetailsPhaseItem,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseItemBillingThresholds as InvoiceCreatePreviewParamsScheduleDetailsPhaseItemBillingThresholds,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseItemDiscount as InvoiceCreatePreviewParamsScheduleDetailsPhaseItemDiscount,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceData as InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceData,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceDataRecurring as InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceDataRecurring,
        InvoiceCreatePreviewParamsScheduleDetailsPhaseTransferData as InvoiceCreatePreviewParamsScheduleDetailsPhaseTransferData,
        InvoiceCreatePreviewParamsSubscriptionDetails as InvoiceCreatePreviewParamsSubscriptionDetails,
        InvoiceCreatePreviewParamsSubscriptionDetailsBillingMode as InvoiceCreatePreviewParamsSubscriptionDetailsBillingMode,
        InvoiceCreatePreviewParamsSubscriptionDetailsBillingModeFlexible as InvoiceCreatePreviewParamsSubscriptionDetailsBillingModeFlexible,
        InvoiceCreatePreviewParamsSubscriptionDetailsItem as InvoiceCreatePreviewParamsSubscriptionDetailsItem,
        InvoiceCreatePreviewParamsSubscriptionDetailsItemBillingThresholds as InvoiceCreatePreviewParamsSubscriptionDetailsItemBillingThresholds,
        InvoiceCreatePreviewParamsSubscriptionDetailsItemDiscount as InvoiceCreatePreviewParamsSubscriptionDetailsItemDiscount,
        InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceData as InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceData,
        InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceDataRecurring as InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceDataRecurring,
    )
    from stripe.params._invoice_delete_params import (
        InvoiceDeleteParams as InvoiceDeleteParams,
    )
    from stripe.params._invoice_finalize_invoice_params import (
        InvoiceFinalizeInvoiceParams as InvoiceFinalizeInvoiceParams,
    )
    from stripe.params._invoice_item_create_params import (
        InvoiceItemCreateParams as InvoiceItemCreateParams,
        InvoiceItemCreateParamsDiscount as InvoiceItemCreateParamsDiscount,
        InvoiceItemCreateParamsPeriod as InvoiceItemCreateParamsPeriod,
        InvoiceItemCreateParamsPriceData as InvoiceItemCreateParamsPriceData,
        InvoiceItemCreateParamsPricing as InvoiceItemCreateParamsPricing,
    )
    from stripe.params._invoice_item_delete_params import (
        InvoiceItemDeleteParams as InvoiceItemDeleteParams,
    )
    from stripe.params._invoice_item_list_params import (
        InvoiceItemListParams as InvoiceItemListParams,
        InvoiceItemListParamsCreated as InvoiceItemListParamsCreated,
    )
    from stripe.params._invoice_item_modify_params import (
        InvoiceItemModifyParams as InvoiceItemModifyParams,
        InvoiceItemModifyParamsDiscount as InvoiceItemModifyParamsDiscount,
        InvoiceItemModifyParamsPeriod as InvoiceItemModifyParamsPeriod,
        InvoiceItemModifyParamsPriceData as InvoiceItemModifyParamsPriceData,
        InvoiceItemModifyParamsPricing as InvoiceItemModifyParamsPricing,
    )
    from stripe.params._invoice_item_retrieve_params import (
        InvoiceItemRetrieveParams as InvoiceItemRetrieveParams,
    )
    from stripe.params._invoice_item_update_params import (
        InvoiceItemUpdateParams as InvoiceItemUpdateParams,
        InvoiceItemUpdateParamsDiscount as InvoiceItemUpdateParamsDiscount,
        InvoiceItemUpdateParamsPeriod as InvoiceItemUpdateParamsPeriod,
        InvoiceItemUpdateParamsPriceData as InvoiceItemUpdateParamsPriceData,
        InvoiceItemUpdateParamsPricing as InvoiceItemUpdateParamsPricing,
    )
    from stripe.params._invoice_line_item_list_params import (
        InvoiceLineItemListParams as InvoiceLineItemListParams,
    )
    from stripe.params._invoice_line_item_update_params import (
        InvoiceLineItemUpdateParams as InvoiceLineItemUpdateParams,
        InvoiceLineItemUpdateParamsDiscount as InvoiceLineItemUpdateParamsDiscount,
        InvoiceLineItemUpdateParamsPeriod as InvoiceLineItemUpdateParamsPeriod,
        InvoiceLineItemUpdateParamsPriceData as InvoiceLineItemUpdateParamsPriceData,
        InvoiceLineItemUpdateParamsPriceDataProductData as InvoiceLineItemUpdateParamsPriceDataProductData,
        InvoiceLineItemUpdateParamsPricing as InvoiceLineItemUpdateParamsPricing,
        InvoiceLineItemUpdateParamsTaxAmount as InvoiceLineItemUpdateParamsTaxAmount,
        InvoiceLineItemUpdateParamsTaxAmountTaxRateData as InvoiceLineItemUpdateParamsTaxAmountTaxRateData,
    )
    from stripe.params._invoice_list_lines_params import (
        InvoiceListLinesParams as InvoiceListLinesParams,
    )
    from stripe.params._invoice_list_params import (
        InvoiceListParams as InvoiceListParams,
        InvoiceListParamsCreated as InvoiceListParamsCreated,
        InvoiceListParamsDueDate as InvoiceListParamsDueDate,
    )
    from stripe.params._invoice_mark_uncollectible_params import (
        InvoiceMarkUncollectibleParams as InvoiceMarkUncollectibleParams,
    )
    from stripe.params._invoice_modify_params import (
        InvoiceModifyParams as InvoiceModifyParams,
        InvoiceModifyParamsAutomaticTax as InvoiceModifyParamsAutomaticTax,
        InvoiceModifyParamsAutomaticTaxLiability as InvoiceModifyParamsAutomaticTaxLiability,
        InvoiceModifyParamsCustomField as InvoiceModifyParamsCustomField,
        InvoiceModifyParamsDiscount as InvoiceModifyParamsDiscount,
        InvoiceModifyParamsIssuer as InvoiceModifyParamsIssuer,
        InvoiceModifyParamsPaymentSettings as InvoiceModifyParamsPaymentSettings,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptions as InvoiceModifyParamsPaymentSettingsPaymentMethodOptions,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsBancontact as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsBancontact,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCard as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCard,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallments as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallments,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsKonbini as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsKonbini,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPayto as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPayto,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        InvoiceModifyParamsRendering as InvoiceModifyParamsRendering,
        InvoiceModifyParamsRenderingPdf as InvoiceModifyParamsRenderingPdf,
        InvoiceModifyParamsShippingCost as InvoiceModifyParamsShippingCost,
        InvoiceModifyParamsShippingCostShippingRateData as InvoiceModifyParamsShippingCostShippingRateData,
        InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimate as InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimate,
        InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMaximum as InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMaximum,
        InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMinimum as InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMinimum,
        InvoiceModifyParamsShippingCostShippingRateDataFixedAmount as InvoiceModifyParamsShippingCostShippingRateDataFixedAmount,
        InvoiceModifyParamsShippingCostShippingRateDataFixedAmountCurrencyOptions as InvoiceModifyParamsShippingCostShippingRateDataFixedAmountCurrencyOptions,
        InvoiceModifyParamsShippingDetails as InvoiceModifyParamsShippingDetails,
        InvoiceModifyParamsShippingDetailsAddress as InvoiceModifyParamsShippingDetailsAddress,
        InvoiceModifyParamsTransferData as InvoiceModifyParamsTransferData,
    )
    from stripe.params._invoice_pay_params import (
        InvoicePayParams as InvoicePayParams,
    )
    from stripe.params._invoice_payment_list_params import (
        InvoicePaymentListParams as InvoicePaymentListParams,
        InvoicePaymentListParamsCreated as InvoicePaymentListParamsCreated,
        InvoicePaymentListParamsPayment as InvoicePaymentListParamsPayment,
    )
    from stripe.params._invoice_payment_retrieve_params import (
        InvoicePaymentRetrieveParams as InvoicePaymentRetrieveParams,
    )
    from stripe.params._invoice_remove_lines_params import (
        InvoiceRemoveLinesParams as InvoiceRemoveLinesParams,
        InvoiceRemoveLinesParamsLine as InvoiceRemoveLinesParamsLine,
    )
    from stripe.params._invoice_rendering_template_archive_params import (
        InvoiceRenderingTemplateArchiveParams as InvoiceRenderingTemplateArchiveParams,
    )
    from stripe.params._invoice_rendering_template_list_params import (
        InvoiceRenderingTemplateListParams as InvoiceRenderingTemplateListParams,
    )
    from stripe.params._invoice_rendering_template_retrieve_params import (
        InvoiceRenderingTemplateRetrieveParams as InvoiceRenderingTemplateRetrieveParams,
    )
    from stripe.params._invoice_rendering_template_unarchive_params import (
        InvoiceRenderingTemplateUnarchiveParams as InvoiceRenderingTemplateUnarchiveParams,
    )
    from stripe.params._invoice_retrieve_params import (
        InvoiceRetrieveParams as InvoiceRetrieveParams,
    )
    from stripe.params._invoice_search_params import (
        InvoiceSearchParams as InvoiceSearchParams,
    )
    from stripe.params._invoice_send_invoice_params import (
        InvoiceSendInvoiceParams as InvoiceSendInvoiceParams,
    )
    from stripe.params._invoice_update_lines_params import (
        InvoiceUpdateLinesParams as InvoiceUpdateLinesParams,
        InvoiceUpdateLinesParamsLine as InvoiceUpdateLinesParamsLine,
        InvoiceUpdateLinesParamsLineDiscount as InvoiceUpdateLinesParamsLineDiscount,
        InvoiceUpdateLinesParamsLinePeriod as InvoiceUpdateLinesParamsLinePeriod,
        InvoiceUpdateLinesParamsLinePriceData as InvoiceUpdateLinesParamsLinePriceData,
        InvoiceUpdateLinesParamsLinePriceDataProductData as InvoiceUpdateLinesParamsLinePriceDataProductData,
        InvoiceUpdateLinesParamsLinePricing as InvoiceUpdateLinesParamsLinePricing,
        InvoiceUpdateLinesParamsLineTaxAmount as InvoiceUpdateLinesParamsLineTaxAmount,
        InvoiceUpdateLinesParamsLineTaxAmountTaxRateData as InvoiceUpdateLinesParamsLineTaxAmountTaxRateData,
    )
    from stripe.params._invoice_update_params import (
        InvoiceUpdateParams as InvoiceUpdateParams,
        InvoiceUpdateParamsAutomaticTax as InvoiceUpdateParamsAutomaticTax,
        InvoiceUpdateParamsAutomaticTaxLiability as InvoiceUpdateParamsAutomaticTaxLiability,
        InvoiceUpdateParamsCustomField as InvoiceUpdateParamsCustomField,
        InvoiceUpdateParamsDiscount as InvoiceUpdateParamsDiscount,
        InvoiceUpdateParamsIssuer as InvoiceUpdateParamsIssuer,
        InvoiceUpdateParamsPaymentSettings as InvoiceUpdateParamsPaymentSettings,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptions as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptions,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCard as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCard,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallments as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallments,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPayto as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPayto,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        InvoiceUpdateParamsRendering as InvoiceUpdateParamsRendering,
        InvoiceUpdateParamsRenderingPdf as InvoiceUpdateParamsRenderingPdf,
        InvoiceUpdateParamsShippingCost as InvoiceUpdateParamsShippingCost,
        InvoiceUpdateParamsShippingCostShippingRateData as InvoiceUpdateParamsShippingCostShippingRateData,
        InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimate as InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimate,
        InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMaximum as InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMaximum,
        InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMinimum as InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMinimum,
        InvoiceUpdateParamsShippingCostShippingRateDataFixedAmount as InvoiceUpdateParamsShippingCostShippingRateDataFixedAmount,
        InvoiceUpdateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions as InvoiceUpdateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions,
        InvoiceUpdateParamsShippingDetails as InvoiceUpdateParamsShippingDetails,
        InvoiceUpdateParamsShippingDetailsAddress as InvoiceUpdateParamsShippingDetailsAddress,
        InvoiceUpdateParamsTransferData as InvoiceUpdateParamsTransferData,
    )
    from stripe.params._invoice_void_invoice_params import (
        InvoiceVoidInvoiceParams as InvoiceVoidInvoiceParams,
    )
    from stripe.params._mandate_retrieve_params import (
        MandateRetrieveParams as MandateRetrieveParams,
    )
    from stripe.params._payment_attempt_record_list_params import (
        PaymentAttemptRecordListParams as PaymentAttemptRecordListParams,
    )
    from stripe.params._payment_attempt_record_retrieve_params import (
        PaymentAttemptRecordRetrieveParams as PaymentAttemptRecordRetrieveParams,
    )
    from stripe.params._payment_intent_amount_details_line_item_list_params import (
        PaymentIntentAmountDetailsLineItemListParams as PaymentIntentAmountDetailsLineItemListParams,
    )
    from stripe.params._payment_intent_apply_customer_balance_params import (
        PaymentIntentApplyCustomerBalanceParams as PaymentIntentApplyCustomerBalanceParams,
    )
    from stripe.params._payment_intent_cancel_params import (
        PaymentIntentCancelParams as PaymentIntentCancelParams,
    )
    from stripe.params._payment_intent_capture_params import (
        PaymentIntentCaptureParams as PaymentIntentCaptureParams,
        PaymentIntentCaptureParamsAmountDetails as PaymentIntentCaptureParamsAmountDetails,
        PaymentIntentCaptureParamsAmountDetailsLineItem as PaymentIntentCaptureParamsAmountDetailsLineItem,
        PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentCaptureParamsAmountDetailsLineItemTax as PaymentIntentCaptureParamsAmountDetailsLineItemTax,
        PaymentIntentCaptureParamsAmountDetailsShipping as PaymentIntentCaptureParamsAmountDetailsShipping,
        PaymentIntentCaptureParamsAmountDetailsTax as PaymentIntentCaptureParamsAmountDetailsTax,
        PaymentIntentCaptureParamsHooks as PaymentIntentCaptureParamsHooks,
        PaymentIntentCaptureParamsHooksInputs as PaymentIntentCaptureParamsHooksInputs,
        PaymentIntentCaptureParamsHooksInputsTax as PaymentIntentCaptureParamsHooksInputsTax,
        PaymentIntentCaptureParamsPaymentDetails as PaymentIntentCaptureParamsPaymentDetails,
        PaymentIntentCaptureParamsTransferData as PaymentIntentCaptureParamsTransferData,
    )
    from stripe.params._payment_intent_confirm_params import (
        PaymentIntentConfirmParams as PaymentIntentConfirmParams,
        PaymentIntentConfirmParamsAmountDetails as PaymentIntentConfirmParamsAmountDetails,
        PaymentIntentConfirmParamsAmountDetailsLineItem as PaymentIntentConfirmParamsAmountDetailsLineItem,
        PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentConfirmParamsAmountDetailsLineItemTax as PaymentIntentConfirmParamsAmountDetailsLineItemTax,
        PaymentIntentConfirmParamsAmountDetailsShipping as PaymentIntentConfirmParamsAmountDetailsShipping,
        PaymentIntentConfirmParamsAmountDetailsTax as PaymentIntentConfirmParamsAmountDetailsTax,
        PaymentIntentConfirmParamsHooks as PaymentIntentConfirmParamsHooks,
        PaymentIntentConfirmParamsHooksInputs as PaymentIntentConfirmParamsHooksInputs,
        PaymentIntentConfirmParamsHooksInputsTax as PaymentIntentConfirmParamsHooksInputsTax,
        PaymentIntentConfirmParamsMandateData as PaymentIntentConfirmParamsMandateData,
        PaymentIntentConfirmParamsMandateDataCustomerAcceptance as PaymentIntentConfirmParamsMandateDataCustomerAcceptance,
        PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOffline as PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOffline,
        PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOnline as PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOnline,
        PaymentIntentConfirmParamsPaymentDetails as PaymentIntentConfirmParamsPaymentDetails,
        PaymentIntentConfirmParamsPaymentMethodData as PaymentIntentConfirmParamsPaymentMethodData,
        PaymentIntentConfirmParamsPaymentMethodDataAcssDebit as PaymentIntentConfirmParamsPaymentMethodDataAcssDebit,
        PaymentIntentConfirmParamsPaymentMethodDataAffirm as PaymentIntentConfirmParamsPaymentMethodDataAffirm,
        PaymentIntentConfirmParamsPaymentMethodDataAfterpayClearpay as PaymentIntentConfirmParamsPaymentMethodDataAfterpayClearpay,
        PaymentIntentConfirmParamsPaymentMethodDataAlipay as PaymentIntentConfirmParamsPaymentMethodDataAlipay,
        PaymentIntentConfirmParamsPaymentMethodDataAlma as PaymentIntentConfirmParamsPaymentMethodDataAlma,
        PaymentIntentConfirmParamsPaymentMethodDataAmazonPay as PaymentIntentConfirmParamsPaymentMethodDataAmazonPay,
        PaymentIntentConfirmParamsPaymentMethodDataAuBecsDebit as PaymentIntentConfirmParamsPaymentMethodDataAuBecsDebit,
        PaymentIntentConfirmParamsPaymentMethodDataBacsDebit as PaymentIntentConfirmParamsPaymentMethodDataBacsDebit,
        PaymentIntentConfirmParamsPaymentMethodDataBancontact as PaymentIntentConfirmParamsPaymentMethodDataBancontact,
        PaymentIntentConfirmParamsPaymentMethodDataBillie as PaymentIntentConfirmParamsPaymentMethodDataBillie,
        PaymentIntentConfirmParamsPaymentMethodDataBillingDetails as PaymentIntentConfirmParamsPaymentMethodDataBillingDetails,
        PaymentIntentConfirmParamsPaymentMethodDataBillingDetailsAddress as PaymentIntentConfirmParamsPaymentMethodDataBillingDetailsAddress,
        PaymentIntentConfirmParamsPaymentMethodDataBlik as PaymentIntentConfirmParamsPaymentMethodDataBlik,
        PaymentIntentConfirmParamsPaymentMethodDataBoleto as PaymentIntentConfirmParamsPaymentMethodDataBoleto,
        PaymentIntentConfirmParamsPaymentMethodDataCashapp as PaymentIntentConfirmParamsPaymentMethodDataCashapp,
        PaymentIntentConfirmParamsPaymentMethodDataCrypto as PaymentIntentConfirmParamsPaymentMethodDataCrypto,
        PaymentIntentConfirmParamsPaymentMethodDataCustomerBalance as PaymentIntentConfirmParamsPaymentMethodDataCustomerBalance,
        PaymentIntentConfirmParamsPaymentMethodDataEps as PaymentIntentConfirmParamsPaymentMethodDataEps,
        PaymentIntentConfirmParamsPaymentMethodDataFpx as PaymentIntentConfirmParamsPaymentMethodDataFpx,
        PaymentIntentConfirmParamsPaymentMethodDataGiropay as PaymentIntentConfirmParamsPaymentMethodDataGiropay,
        PaymentIntentConfirmParamsPaymentMethodDataGrabpay as PaymentIntentConfirmParamsPaymentMethodDataGrabpay,
        PaymentIntentConfirmParamsPaymentMethodDataIdeal as PaymentIntentConfirmParamsPaymentMethodDataIdeal,
        PaymentIntentConfirmParamsPaymentMethodDataInteracPresent as PaymentIntentConfirmParamsPaymentMethodDataInteracPresent,
        PaymentIntentConfirmParamsPaymentMethodDataKakaoPay as PaymentIntentConfirmParamsPaymentMethodDataKakaoPay,
        PaymentIntentConfirmParamsPaymentMethodDataKlarna as PaymentIntentConfirmParamsPaymentMethodDataKlarna,
        PaymentIntentConfirmParamsPaymentMethodDataKlarnaDob as PaymentIntentConfirmParamsPaymentMethodDataKlarnaDob,
        PaymentIntentConfirmParamsPaymentMethodDataKonbini as PaymentIntentConfirmParamsPaymentMethodDataKonbini,
        PaymentIntentConfirmParamsPaymentMethodDataKrCard as PaymentIntentConfirmParamsPaymentMethodDataKrCard,
        PaymentIntentConfirmParamsPaymentMethodDataLink as PaymentIntentConfirmParamsPaymentMethodDataLink,
        PaymentIntentConfirmParamsPaymentMethodDataMbWay as PaymentIntentConfirmParamsPaymentMethodDataMbWay,
        PaymentIntentConfirmParamsPaymentMethodDataMobilepay as PaymentIntentConfirmParamsPaymentMethodDataMobilepay,
        PaymentIntentConfirmParamsPaymentMethodDataMultibanco as PaymentIntentConfirmParamsPaymentMethodDataMultibanco,
        PaymentIntentConfirmParamsPaymentMethodDataNaverPay as PaymentIntentConfirmParamsPaymentMethodDataNaverPay,
        PaymentIntentConfirmParamsPaymentMethodDataNzBankAccount as PaymentIntentConfirmParamsPaymentMethodDataNzBankAccount,
        PaymentIntentConfirmParamsPaymentMethodDataOxxo as PaymentIntentConfirmParamsPaymentMethodDataOxxo,
        PaymentIntentConfirmParamsPaymentMethodDataP24 as PaymentIntentConfirmParamsPaymentMethodDataP24,
        PaymentIntentConfirmParamsPaymentMethodDataPayByBank as PaymentIntentConfirmParamsPaymentMethodDataPayByBank,
        PaymentIntentConfirmParamsPaymentMethodDataPayco as PaymentIntentConfirmParamsPaymentMethodDataPayco,
        PaymentIntentConfirmParamsPaymentMethodDataPaynow as PaymentIntentConfirmParamsPaymentMethodDataPaynow,
        PaymentIntentConfirmParamsPaymentMethodDataPaypal as PaymentIntentConfirmParamsPaymentMethodDataPaypal,
        PaymentIntentConfirmParamsPaymentMethodDataPayto as PaymentIntentConfirmParamsPaymentMethodDataPayto,
        PaymentIntentConfirmParamsPaymentMethodDataPix as PaymentIntentConfirmParamsPaymentMethodDataPix,
        PaymentIntentConfirmParamsPaymentMethodDataPromptpay as PaymentIntentConfirmParamsPaymentMethodDataPromptpay,
        PaymentIntentConfirmParamsPaymentMethodDataRadarOptions as PaymentIntentConfirmParamsPaymentMethodDataRadarOptions,
        PaymentIntentConfirmParamsPaymentMethodDataRevolutPay as PaymentIntentConfirmParamsPaymentMethodDataRevolutPay,
        PaymentIntentConfirmParamsPaymentMethodDataSamsungPay as PaymentIntentConfirmParamsPaymentMethodDataSamsungPay,
        PaymentIntentConfirmParamsPaymentMethodDataSatispay as PaymentIntentConfirmParamsPaymentMethodDataSatispay,
        PaymentIntentConfirmParamsPaymentMethodDataSepaDebit as PaymentIntentConfirmParamsPaymentMethodDataSepaDebit,
        PaymentIntentConfirmParamsPaymentMethodDataSofort as PaymentIntentConfirmParamsPaymentMethodDataSofort,
        PaymentIntentConfirmParamsPaymentMethodDataSwish as PaymentIntentConfirmParamsPaymentMethodDataSwish,
        PaymentIntentConfirmParamsPaymentMethodDataTwint as PaymentIntentConfirmParamsPaymentMethodDataTwint,
        PaymentIntentConfirmParamsPaymentMethodDataUsBankAccount as PaymentIntentConfirmParamsPaymentMethodDataUsBankAccount,
        PaymentIntentConfirmParamsPaymentMethodDataWechatPay as PaymentIntentConfirmParamsPaymentMethodDataWechatPay,
        PaymentIntentConfirmParamsPaymentMethodDataZip as PaymentIntentConfirmParamsPaymentMethodDataZip,
        PaymentIntentConfirmParamsPaymentMethodOptions as PaymentIntentConfirmParamsPaymentMethodOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebit as PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebit,
        PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsAffirm as PaymentIntentConfirmParamsPaymentMethodOptionsAffirm,
        PaymentIntentConfirmParamsPaymentMethodOptionsAfterpayClearpay as PaymentIntentConfirmParamsPaymentMethodOptionsAfterpayClearpay,
        PaymentIntentConfirmParamsPaymentMethodOptionsAlipay as PaymentIntentConfirmParamsPaymentMethodOptionsAlipay,
        PaymentIntentConfirmParamsPaymentMethodOptionsAlma as PaymentIntentConfirmParamsPaymentMethodOptionsAlma,
        PaymentIntentConfirmParamsPaymentMethodOptionsAmazonPay as PaymentIntentConfirmParamsPaymentMethodOptionsAmazonPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsAuBecsDebit as PaymentIntentConfirmParamsPaymentMethodOptionsAuBecsDebit,
        PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebit as PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebit,
        PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsBancontact as PaymentIntentConfirmParamsPaymentMethodOptionsBancontact,
        PaymentIntentConfirmParamsPaymentMethodOptionsBillie as PaymentIntentConfirmParamsPaymentMethodOptionsBillie,
        PaymentIntentConfirmParamsPaymentMethodOptionsBlik as PaymentIntentConfirmParamsPaymentMethodOptionsBlik,
        PaymentIntentConfirmParamsPaymentMethodOptionsBoleto as PaymentIntentConfirmParamsPaymentMethodOptionsBoleto,
        PaymentIntentConfirmParamsPaymentMethodOptionsCard as PaymentIntentConfirmParamsPaymentMethodOptionsCard,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallments as PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallments,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallmentsPlan as PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallmentsPlan,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsCardMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardPresent as PaymentIntentConfirmParamsPaymentMethodOptionsCardPresent,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardPresentRouting as PaymentIntentConfirmParamsPaymentMethodOptionsCardPresentRouting,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure as PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        PaymentIntentConfirmParamsPaymentMethodOptionsCashapp as PaymentIntentConfirmParamsPaymentMethodOptionsCashapp,
        PaymentIntentConfirmParamsPaymentMethodOptionsCrypto as PaymentIntentConfirmParamsPaymentMethodOptionsCrypto,
        PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalance as PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalance,
        PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransfer as PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransfer,
        PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        PaymentIntentConfirmParamsPaymentMethodOptionsEps as PaymentIntentConfirmParamsPaymentMethodOptionsEps,
        PaymentIntentConfirmParamsPaymentMethodOptionsFpx as PaymentIntentConfirmParamsPaymentMethodOptionsFpx,
        PaymentIntentConfirmParamsPaymentMethodOptionsGiropay as PaymentIntentConfirmParamsPaymentMethodOptionsGiropay,
        PaymentIntentConfirmParamsPaymentMethodOptionsGrabpay as PaymentIntentConfirmParamsPaymentMethodOptionsGrabpay,
        PaymentIntentConfirmParamsPaymentMethodOptionsIdeal as PaymentIntentConfirmParamsPaymentMethodOptionsIdeal,
        PaymentIntentConfirmParamsPaymentMethodOptionsInteracPresent as PaymentIntentConfirmParamsPaymentMethodOptionsInteracPresent,
        PaymentIntentConfirmParamsPaymentMethodOptionsKakaoPay as PaymentIntentConfirmParamsPaymentMethodOptionsKakaoPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsKlarna as PaymentIntentConfirmParamsPaymentMethodOptionsKlarna,
        PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand as PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand,
        PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription as PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription,
        PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        PaymentIntentConfirmParamsPaymentMethodOptionsKonbini as PaymentIntentConfirmParamsPaymentMethodOptionsKonbini,
        PaymentIntentConfirmParamsPaymentMethodOptionsKrCard as PaymentIntentConfirmParamsPaymentMethodOptionsKrCard,
        PaymentIntentConfirmParamsPaymentMethodOptionsLink as PaymentIntentConfirmParamsPaymentMethodOptionsLink,
        PaymentIntentConfirmParamsPaymentMethodOptionsMbWay as PaymentIntentConfirmParamsPaymentMethodOptionsMbWay,
        PaymentIntentConfirmParamsPaymentMethodOptionsMobilepay as PaymentIntentConfirmParamsPaymentMethodOptionsMobilepay,
        PaymentIntentConfirmParamsPaymentMethodOptionsMultibanco as PaymentIntentConfirmParamsPaymentMethodOptionsMultibanco,
        PaymentIntentConfirmParamsPaymentMethodOptionsNaverPay as PaymentIntentConfirmParamsPaymentMethodOptionsNaverPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsNzBankAccount as PaymentIntentConfirmParamsPaymentMethodOptionsNzBankAccount,
        PaymentIntentConfirmParamsPaymentMethodOptionsOxxo as PaymentIntentConfirmParamsPaymentMethodOptionsOxxo,
        PaymentIntentConfirmParamsPaymentMethodOptionsP24 as PaymentIntentConfirmParamsPaymentMethodOptionsP24,
        PaymentIntentConfirmParamsPaymentMethodOptionsPayByBank as PaymentIntentConfirmParamsPaymentMethodOptionsPayByBank,
        PaymentIntentConfirmParamsPaymentMethodOptionsPayco as PaymentIntentConfirmParamsPaymentMethodOptionsPayco,
        PaymentIntentConfirmParamsPaymentMethodOptionsPaynow as PaymentIntentConfirmParamsPaymentMethodOptionsPaynow,
        PaymentIntentConfirmParamsPaymentMethodOptionsPaypal as PaymentIntentConfirmParamsPaymentMethodOptionsPaypal,
        PaymentIntentConfirmParamsPaymentMethodOptionsPayto as PaymentIntentConfirmParamsPaymentMethodOptionsPayto,
        PaymentIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsPix as PaymentIntentConfirmParamsPaymentMethodOptionsPix,
        PaymentIntentConfirmParamsPaymentMethodOptionsPromptpay as PaymentIntentConfirmParamsPaymentMethodOptionsPromptpay,
        PaymentIntentConfirmParamsPaymentMethodOptionsRevolutPay as PaymentIntentConfirmParamsPaymentMethodOptionsRevolutPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsSamsungPay as PaymentIntentConfirmParamsPaymentMethodOptionsSamsungPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsSatispay as PaymentIntentConfirmParamsPaymentMethodOptionsSatispay,
        PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebit as PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebit,
        PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsSofort as PaymentIntentConfirmParamsPaymentMethodOptionsSofort,
        PaymentIntentConfirmParamsPaymentMethodOptionsSwish as PaymentIntentConfirmParamsPaymentMethodOptionsSwish,
        PaymentIntentConfirmParamsPaymentMethodOptionsTwint as PaymentIntentConfirmParamsPaymentMethodOptionsTwint,
        PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccount as PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccount,
        PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections as PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions as PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks as PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks,
        PaymentIntentConfirmParamsPaymentMethodOptionsWechatPay as PaymentIntentConfirmParamsPaymentMethodOptionsWechatPay,
        PaymentIntentConfirmParamsPaymentMethodOptionsZip as PaymentIntentConfirmParamsPaymentMethodOptionsZip,
        PaymentIntentConfirmParamsRadarOptions as PaymentIntentConfirmParamsRadarOptions,
        PaymentIntentConfirmParamsShipping as PaymentIntentConfirmParamsShipping,
        PaymentIntentConfirmParamsShippingAddress as PaymentIntentConfirmParamsShippingAddress,
    )
    from stripe.params._payment_intent_create_params import (
        PaymentIntentCreateParams as PaymentIntentCreateParams,
        PaymentIntentCreateParamsAmountDetails as PaymentIntentCreateParamsAmountDetails,
        PaymentIntentCreateParamsAmountDetailsLineItem as PaymentIntentCreateParamsAmountDetailsLineItem,
        PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentCreateParamsAmountDetailsLineItemTax as PaymentIntentCreateParamsAmountDetailsLineItemTax,
        PaymentIntentCreateParamsAmountDetailsShipping as PaymentIntentCreateParamsAmountDetailsShipping,
        PaymentIntentCreateParamsAmountDetailsTax as PaymentIntentCreateParamsAmountDetailsTax,
        PaymentIntentCreateParamsAutomaticPaymentMethods as PaymentIntentCreateParamsAutomaticPaymentMethods,
        PaymentIntentCreateParamsHooks as PaymentIntentCreateParamsHooks,
        PaymentIntentCreateParamsHooksInputs as PaymentIntentCreateParamsHooksInputs,
        PaymentIntentCreateParamsHooksInputsTax as PaymentIntentCreateParamsHooksInputsTax,
        PaymentIntentCreateParamsMandateData as PaymentIntentCreateParamsMandateData,
        PaymentIntentCreateParamsMandateDataCustomerAcceptance as PaymentIntentCreateParamsMandateDataCustomerAcceptance,
        PaymentIntentCreateParamsMandateDataCustomerAcceptanceOffline as PaymentIntentCreateParamsMandateDataCustomerAcceptanceOffline,
        PaymentIntentCreateParamsMandateDataCustomerAcceptanceOnline as PaymentIntentCreateParamsMandateDataCustomerAcceptanceOnline,
        PaymentIntentCreateParamsPaymentDetails as PaymentIntentCreateParamsPaymentDetails,
        PaymentIntentCreateParamsPaymentMethodData as PaymentIntentCreateParamsPaymentMethodData,
        PaymentIntentCreateParamsPaymentMethodDataAcssDebit as PaymentIntentCreateParamsPaymentMethodDataAcssDebit,
        PaymentIntentCreateParamsPaymentMethodDataAffirm as PaymentIntentCreateParamsPaymentMethodDataAffirm,
        PaymentIntentCreateParamsPaymentMethodDataAfterpayClearpay as PaymentIntentCreateParamsPaymentMethodDataAfterpayClearpay,
        PaymentIntentCreateParamsPaymentMethodDataAlipay as PaymentIntentCreateParamsPaymentMethodDataAlipay,
        PaymentIntentCreateParamsPaymentMethodDataAlma as PaymentIntentCreateParamsPaymentMethodDataAlma,
        PaymentIntentCreateParamsPaymentMethodDataAmazonPay as PaymentIntentCreateParamsPaymentMethodDataAmazonPay,
        PaymentIntentCreateParamsPaymentMethodDataAuBecsDebit as PaymentIntentCreateParamsPaymentMethodDataAuBecsDebit,
        PaymentIntentCreateParamsPaymentMethodDataBacsDebit as PaymentIntentCreateParamsPaymentMethodDataBacsDebit,
        PaymentIntentCreateParamsPaymentMethodDataBancontact as PaymentIntentCreateParamsPaymentMethodDataBancontact,
        PaymentIntentCreateParamsPaymentMethodDataBillie as PaymentIntentCreateParamsPaymentMethodDataBillie,
        PaymentIntentCreateParamsPaymentMethodDataBillingDetails as PaymentIntentCreateParamsPaymentMethodDataBillingDetails,
        PaymentIntentCreateParamsPaymentMethodDataBillingDetailsAddress as PaymentIntentCreateParamsPaymentMethodDataBillingDetailsAddress,
        PaymentIntentCreateParamsPaymentMethodDataBlik as PaymentIntentCreateParamsPaymentMethodDataBlik,
        PaymentIntentCreateParamsPaymentMethodDataBoleto as PaymentIntentCreateParamsPaymentMethodDataBoleto,
        PaymentIntentCreateParamsPaymentMethodDataCashapp as PaymentIntentCreateParamsPaymentMethodDataCashapp,
        PaymentIntentCreateParamsPaymentMethodDataCrypto as PaymentIntentCreateParamsPaymentMethodDataCrypto,
        PaymentIntentCreateParamsPaymentMethodDataCustomerBalance as PaymentIntentCreateParamsPaymentMethodDataCustomerBalance,
        PaymentIntentCreateParamsPaymentMethodDataEps as PaymentIntentCreateParamsPaymentMethodDataEps,
        PaymentIntentCreateParamsPaymentMethodDataFpx as PaymentIntentCreateParamsPaymentMethodDataFpx,
        PaymentIntentCreateParamsPaymentMethodDataGiropay as PaymentIntentCreateParamsPaymentMethodDataGiropay,
        PaymentIntentCreateParamsPaymentMethodDataGrabpay as PaymentIntentCreateParamsPaymentMethodDataGrabpay,
        PaymentIntentCreateParamsPaymentMethodDataIdeal as PaymentIntentCreateParamsPaymentMethodDataIdeal,
        PaymentIntentCreateParamsPaymentMethodDataInteracPresent as PaymentIntentCreateParamsPaymentMethodDataInteracPresent,
        PaymentIntentCreateParamsPaymentMethodDataKakaoPay as PaymentIntentCreateParamsPaymentMethodDataKakaoPay,
        PaymentIntentCreateParamsPaymentMethodDataKlarna as PaymentIntentCreateParamsPaymentMethodDataKlarna,
        PaymentIntentCreateParamsPaymentMethodDataKlarnaDob as PaymentIntentCreateParamsPaymentMethodDataKlarnaDob,
        PaymentIntentCreateParamsPaymentMethodDataKonbini as PaymentIntentCreateParamsPaymentMethodDataKonbini,
        PaymentIntentCreateParamsPaymentMethodDataKrCard as PaymentIntentCreateParamsPaymentMethodDataKrCard,
        PaymentIntentCreateParamsPaymentMethodDataLink as PaymentIntentCreateParamsPaymentMethodDataLink,
        PaymentIntentCreateParamsPaymentMethodDataMbWay as PaymentIntentCreateParamsPaymentMethodDataMbWay,
        PaymentIntentCreateParamsPaymentMethodDataMobilepay as PaymentIntentCreateParamsPaymentMethodDataMobilepay,
        PaymentIntentCreateParamsPaymentMethodDataMultibanco as PaymentIntentCreateParamsPaymentMethodDataMultibanco,
        PaymentIntentCreateParamsPaymentMethodDataNaverPay as PaymentIntentCreateParamsPaymentMethodDataNaverPay,
        PaymentIntentCreateParamsPaymentMethodDataNzBankAccount as PaymentIntentCreateParamsPaymentMethodDataNzBankAccount,
        PaymentIntentCreateParamsPaymentMethodDataOxxo as PaymentIntentCreateParamsPaymentMethodDataOxxo,
        PaymentIntentCreateParamsPaymentMethodDataP24 as PaymentIntentCreateParamsPaymentMethodDataP24,
        PaymentIntentCreateParamsPaymentMethodDataPayByBank as PaymentIntentCreateParamsPaymentMethodDataPayByBank,
        PaymentIntentCreateParamsPaymentMethodDataPayco as PaymentIntentCreateParamsPaymentMethodDataPayco,
        PaymentIntentCreateParamsPaymentMethodDataPaynow as PaymentIntentCreateParamsPaymentMethodDataPaynow,
        PaymentIntentCreateParamsPaymentMethodDataPaypal as PaymentIntentCreateParamsPaymentMethodDataPaypal,
        PaymentIntentCreateParamsPaymentMethodDataPayto as PaymentIntentCreateParamsPaymentMethodDataPayto,
        PaymentIntentCreateParamsPaymentMethodDataPix as PaymentIntentCreateParamsPaymentMethodDataPix,
        PaymentIntentCreateParamsPaymentMethodDataPromptpay as PaymentIntentCreateParamsPaymentMethodDataPromptpay,
        PaymentIntentCreateParamsPaymentMethodDataRadarOptions as PaymentIntentCreateParamsPaymentMethodDataRadarOptions,
        PaymentIntentCreateParamsPaymentMethodDataRevolutPay as PaymentIntentCreateParamsPaymentMethodDataRevolutPay,
        PaymentIntentCreateParamsPaymentMethodDataSamsungPay as PaymentIntentCreateParamsPaymentMethodDataSamsungPay,
        PaymentIntentCreateParamsPaymentMethodDataSatispay as PaymentIntentCreateParamsPaymentMethodDataSatispay,
        PaymentIntentCreateParamsPaymentMethodDataSepaDebit as PaymentIntentCreateParamsPaymentMethodDataSepaDebit,
        PaymentIntentCreateParamsPaymentMethodDataSofort as PaymentIntentCreateParamsPaymentMethodDataSofort,
        PaymentIntentCreateParamsPaymentMethodDataSwish as PaymentIntentCreateParamsPaymentMethodDataSwish,
        PaymentIntentCreateParamsPaymentMethodDataTwint as PaymentIntentCreateParamsPaymentMethodDataTwint,
        PaymentIntentCreateParamsPaymentMethodDataUsBankAccount as PaymentIntentCreateParamsPaymentMethodDataUsBankAccount,
        PaymentIntentCreateParamsPaymentMethodDataWechatPay as PaymentIntentCreateParamsPaymentMethodDataWechatPay,
        PaymentIntentCreateParamsPaymentMethodDataZip as PaymentIntentCreateParamsPaymentMethodDataZip,
        PaymentIntentCreateParamsPaymentMethodOptions as PaymentIntentCreateParamsPaymentMethodOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsAcssDebit as PaymentIntentCreateParamsPaymentMethodOptionsAcssDebit,
        PaymentIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsAffirm as PaymentIntentCreateParamsPaymentMethodOptionsAffirm,
        PaymentIntentCreateParamsPaymentMethodOptionsAfterpayClearpay as PaymentIntentCreateParamsPaymentMethodOptionsAfterpayClearpay,
        PaymentIntentCreateParamsPaymentMethodOptionsAlipay as PaymentIntentCreateParamsPaymentMethodOptionsAlipay,
        PaymentIntentCreateParamsPaymentMethodOptionsAlma as PaymentIntentCreateParamsPaymentMethodOptionsAlma,
        PaymentIntentCreateParamsPaymentMethodOptionsAmazonPay as PaymentIntentCreateParamsPaymentMethodOptionsAmazonPay,
        PaymentIntentCreateParamsPaymentMethodOptionsAuBecsDebit as PaymentIntentCreateParamsPaymentMethodOptionsAuBecsDebit,
        PaymentIntentCreateParamsPaymentMethodOptionsBacsDebit as PaymentIntentCreateParamsPaymentMethodOptionsBacsDebit,
        PaymentIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsBancontact as PaymentIntentCreateParamsPaymentMethodOptionsBancontact,
        PaymentIntentCreateParamsPaymentMethodOptionsBillie as PaymentIntentCreateParamsPaymentMethodOptionsBillie,
        PaymentIntentCreateParamsPaymentMethodOptionsBlik as PaymentIntentCreateParamsPaymentMethodOptionsBlik,
        PaymentIntentCreateParamsPaymentMethodOptionsBoleto as PaymentIntentCreateParamsPaymentMethodOptionsBoleto,
        PaymentIntentCreateParamsPaymentMethodOptionsCard as PaymentIntentCreateParamsPaymentMethodOptionsCard,
        PaymentIntentCreateParamsPaymentMethodOptionsCardInstallments as PaymentIntentCreateParamsPaymentMethodOptionsCardInstallments,
        PaymentIntentCreateParamsPaymentMethodOptionsCardInstallmentsPlan as PaymentIntentCreateParamsPaymentMethodOptionsCardInstallmentsPlan,
        PaymentIntentCreateParamsPaymentMethodOptionsCardMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsCardMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsCardPresent as PaymentIntentCreateParamsPaymentMethodOptionsCardPresent,
        PaymentIntentCreateParamsPaymentMethodOptionsCardPresentRouting as PaymentIntentCreateParamsPaymentMethodOptionsCardPresentRouting,
        PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecure as PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecure,
        PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        PaymentIntentCreateParamsPaymentMethodOptionsCashapp as PaymentIntentCreateParamsPaymentMethodOptionsCashapp,
        PaymentIntentCreateParamsPaymentMethodOptionsCrypto as PaymentIntentCreateParamsPaymentMethodOptionsCrypto,
        PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalance as PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalance,
        PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransfer as PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransfer,
        PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        PaymentIntentCreateParamsPaymentMethodOptionsEps as PaymentIntentCreateParamsPaymentMethodOptionsEps,
        PaymentIntentCreateParamsPaymentMethodOptionsFpx as PaymentIntentCreateParamsPaymentMethodOptionsFpx,
        PaymentIntentCreateParamsPaymentMethodOptionsGiropay as PaymentIntentCreateParamsPaymentMethodOptionsGiropay,
        PaymentIntentCreateParamsPaymentMethodOptionsGrabpay as PaymentIntentCreateParamsPaymentMethodOptionsGrabpay,
        PaymentIntentCreateParamsPaymentMethodOptionsIdeal as PaymentIntentCreateParamsPaymentMethodOptionsIdeal,
        PaymentIntentCreateParamsPaymentMethodOptionsInteracPresent as PaymentIntentCreateParamsPaymentMethodOptionsInteracPresent,
        PaymentIntentCreateParamsPaymentMethodOptionsKakaoPay as PaymentIntentCreateParamsPaymentMethodOptionsKakaoPay,
        PaymentIntentCreateParamsPaymentMethodOptionsKlarna as PaymentIntentCreateParamsPaymentMethodOptionsKlarna,
        PaymentIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand as PaymentIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand,
        PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscription as PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscription,
        PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        PaymentIntentCreateParamsPaymentMethodOptionsKonbini as PaymentIntentCreateParamsPaymentMethodOptionsKonbini,
        PaymentIntentCreateParamsPaymentMethodOptionsKrCard as PaymentIntentCreateParamsPaymentMethodOptionsKrCard,
        PaymentIntentCreateParamsPaymentMethodOptionsLink as PaymentIntentCreateParamsPaymentMethodOptionsLink,
        PaymentIntentCreateParamsPaymentMethodOptionsMbWay as PaymentIntentCreateParamsPaymentMethodOptionsMbWay,
        PaymentIntentCreateParamsPaymentMethodOptionsMobilepay as PaymentIntentCreateParamsPaymentMethodOptionsMobilepay,
        PaymentIntentCreateParamsPaymentMethodOptionsMultibanco as PaymentIntentCreateParamsPaymentMethodOptionsMultibanco,
        PaymentIntentCreateParamsPaymentMethodOptionsNaverPay as PaymentIntentCreateParamsPaymentMethodOptionsNaverPay,
        PaymentIntentCreateParamsPaymentMethodOptionsNzBankAccount as PaymentIntentCreateParamsPaymentMethodOptionsNzBankAccount,
        PaymentIntentCreateParamsPaymentMethodOptionsOxxo as PaymentIntentCreateParamsPaymentMethodOptionsOxxo,
        PaymentIntentCreateParamsPaymentMethodOptionsP24 as PaymentIntentCreateParamsPaymentMethodOptionsP24,
        PaymentIntentCreateParamsPaymentMethodOptionsPayByBank as PaymentIntentCreateParamsPaymentMethodOptionsPayByBank,
        PaymentIntentCreateParamsPaymentMethodOptionsPayco as PaymentIntentCreateParamsPaymentMethodOptionsPayco,
        PaymentIntentCreateParamsPaymentMethodOptionsPaynow as PaymentIntentCreateParamsPaymentMethodOptionsPaynow,
        PaymentIntentCreateParamsPaymentMethodOptionsPaypal as PaymentIntentCreateParamsPaymentMethodOptionsPaypal,
        PaymentIntentCreateParamsPaymentMethodOptionsPayto as PaymentIntentCreateParamsPaymentMethodOptionsPayto,
        PaymentIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsPix as PaymentIntentCreateParamsPaymentMethodOptionsPix,
        PaymentIntentCreateParamsPaymentMethodOptionsPromptpay as PaymentIntentCreateParamsPaymentMethodOptionsPromptpay,
        PaymentIntentCreateParamsPaymentMethodOptionsRevolutPay as PaymentIntentCreateParamsPaymentMethodOptionsRevolutPay,
        PaymentIntentCreateParamsPaymentMethodOptionsSamsungPay as PaymentIntentCreateParamsPaymentMethodOptionsSamsungPay,
        PaymentIntentCreateParamsPaymentMethodOptionsSatispay as PaymentIntentCreateParamsPaymentMethodOptionsSatispay,
        PaymentIntentCreateParamsPaymentMethodOptionsSepaDebit as PaymentIntentCreateParamsPaymentMethodOptionsSepaDebit,
        PaymentIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsSofort as PaymentIntentCreateParamsPaymentMethodOptionsSofort,
        PaymentIntentCreateParamsPaymentMethodOptionsSwish as PaymentIntentCreateParamsPaymentMethodOptionsSwish,
        PaymentIntentCreateParamsPaymentMethodOptionsTwint as PaymentIntentCreateParamsPaymentMethodOptionsTwint,
        PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccount as PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccount,
        PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections as PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions as PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks as PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks,
        PaymentIntentCreateParamsPaymentMethodOptionsWechatPay as PaymentIntentCreateParamsPaymentMethodOptionsWechatPay,
        PaymentIntentCreateParamsPaymentMethodOptionsZip as PaymentIntentCreateParamsPaymentMethodOptionsZip,
        PaymentIntentCreateParamsRadarOptions as PaymentIntentCreateParamsRadarOptions,
        PaymentIntentCreateParamsShipping as PaymentIntentCreateParamsShipping,
        PaymentIntentCreateParamsShippingAddress as PaymentIntentCreateParamsShippingAddress,
        PaymentIntentCreateParamsTransferData as PaymentIntentCreateParamsTransferData,
    )
    from stripe.params._payment_intent_increment_authorization_params import (
        PaymentIntentIncrementAuthorizationParams as PaymentIntentIncrementAuthorizationParams,
        PaymentIntentIncrementAuthorizationParamsAmountDetails as PaymentIntentIncrementAuthorizationParamsAmountDetails,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItem as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItem,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemTax as PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemTax,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsShipping as PaymentIntentIncrementAuthorizationParamsAmountDetailsShipping,
        PaymentIntentIncrementAuthorizationParamsAmountDetailsTax as PaymentIntentIncrementAuthorizationParamsAmountDetailsTax,
        PaymentIntentIncrementAuthorizationParamsHooks as PaymentIntentIncrementAuthorizationParamsHooks,
        PaymentIntentIncrementAuthorizationParamsHooksInputs as PaymentIntentIncrementAuthorizationParamsHooksInputs,
        PaymentIntentIncrementAuthorizationParamsHooksInputsTax as PaymentIntentIncrementAuthorizationParamsHooksInputsTax,
        PaymentIntentIncrementAuthorizationParamsPaymentDetails as PaymentIntentIncrementAuthorizationParamsPaymentDetails,
        PaymentIntentIncrementAuthorizationParamsTransferData as PaymentIntentIncrementAuthorizationParamsTransferData,
    )
    from stripe.params._payment_intent_list_amount_details_line_items_params import (
        PaymentIntentListAmountDetailsLineItemsParams as PaymentIntentListAmountDetailsLineItemsParams,
    )
    from stripe.params._payment_intent_list_params import (
        PaymentIntentListParams as PaymentIntentListParams,
        PaymentIntentListParamsCreated as PaymentIntentListParamsCreated,
    )
    from stripe.params._payment_intent_modify_params import (
        PaymentIntentModifyParams as PaymentIntentModifyParams,
        PaymentIntentModifyParamsAmountDetails as PaymentIntentModifyParamsAmountDetails,
        PaymentIntentModifyParamsAmountDetailsLineItem as PaymentIntentModifyParamsAmountDetailsLineItem,
        PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentModifyParamsAmountDetailsLineItemTax as PaymentIntentModifyParamsAmountDetailsLineItemTax,
        PaymentIntentModifyParamsAmountDetailsShipping as PaymentIntentModifyParamsAmountDetailsShipping,
        PaymentIntentModifyParamsAmountDetailsTax as PaymentIntentModifyParamsAmountDetailsTax,
        PaymentIntentModifyParamsHooks as PaymentIntentModifyParamsHooks,
        PaymentIntentModifyParamsHooksInputs as PaymentIntentModifyParamsHooksInputs,
        PaymentIntentModifyParamsHooksInputsTax as PaymentIntentModifyParamsHooksInputsTax,
        PaymentIntentModifyParamsPaymentDetails as PaymentIntentModifyParamsPaymentDetails,
        PaymentIntentModifyParamsPaymentMethodData as PaymentIntentModifyParamsPaymentMethodData,
        PaymentIntentModifyParamsPaymentMethodDataAcssDebit as PaymentIntentModifyParamsPaymentMethodDataAcssDebit,
        PaymentIntentModifyParamsPaymentMethodDataAffirm as PaymentIntentModifyParamsPaymentMethodDataAffirm,
        PaymentIntentModifyParamsPaymentMethodDataAfterpayClearpay as PaymentIntentModifyParamsPaymentMethodDataAfterpayClearpay,
        PaymentIntentModifyParamsPaymentMethodDataAlipay as PaymentIntentModifyParamsPaymentMethodDataAlipay,
        PaymentIntentModifyParamsPaymentMethodDataAlma as PaymentIntentModifyParamsPaymentMethodDataAlma,
        PaymentIntentModifyParamsPaymentMethodDataAmazonPay as PaymentIntentModifyParamsPaymentMethodDataAmazonPay,
        PaymentIntentModifyParamsPaymentMethodDataAuBecsDebit as PaymentIntentModifyParamsPaymentMethodDataAuBecsDebit,
        PaymentIntentModifyParamsPaymentMethodDataBacsDebit as PaymentIntentModifyParamsPaymentMethodDataBacsDebit,
        PaymentIntentModifyParamsPaymentMethodDataBancontact as PaymentIntentModifyParamsPaymentMethodDataBancontact,
        PaymentIntentModifyParamsPaymentMethodDataBillie as PaymentIntentModifyParamsPaymentMethodDataBillie,
        PaymentIntentModifyParamsPaymentMethodDataBillingDetails as PaymentIntentModifyParamsPaymentMethodDataBillingDetails,
        PaymentIntentModifyParamsPaymentMethodDataBillingDetailsAddress as PaymentIntentModifyParamsPaymentMethodDataBillingDetailsAddress,
        PaymentIntentModifyParamsPaymentMethodDataBlik as PaymentIntentModifyParamsPaymentMethodDataBlik,
        PaymentIntentModifyParamsPaymentMethodDataBoleto as PaymentIntentModifyParamsPaymentMethodDataBoleto,
        PaymentIntentModifyParamsPaymentMethodDataCashapp as PaymentIntentModifyParamsPaymentMethodDataCashapp,
        PaymentIntentModifyParamsPaymentMethodDataCrypto as PaymentIntentModifyParamsPaymentMethodDataCrypto,
        PaymentIntentModifyParamsPaymentMethodDataCustomerBalance as PaymentIntentModifyParamsPaymentMethodDataCustomerBalance,
        PaymentIntentModifyParamsPaymentMethodDataEps as PaymentIntentModifyParamsPaymentMethodDataEps,
        PaymentIntentModifyParamsPaymentMethodDataFpx as PaymentIntentModifyParamsPaymentMethodDataFpx,
        PaymentIntentModifyParamsPaymentMethodDataGiropay as PaymentIntentModifyParamsPaymentMethodDataGiropay,
        PaymentIntentModifyParamsPaymentMethodDataGrabpay as PaymentIntentModifyParamsPaymentMethodDataGrabpay,
        PaymentIntentModifyParamsPaymentMethodDataIdeal as PaymentIntentModifyParamsPaymentMethodDataIdeal,
        PaymentIntentModifyParamsPaymentMethodDataInteracPresent as PaymentIntentModifyParamsPaymentMethodDataInteracPresent,
        PaymentIntentModifyParamsPaymentMethodDataKakaoPay as PaymentIntentModifyParamsPaymentMethodDataKakaoPay,
        PaymentIntentModifyParamsPaymentMethodDataKlarna as PaymentIntentModifyParamsPaymentMethodDataKlarna,
        PaymentIntentModifyParamsPaymentMethodDataKlarnaDob as PaymentIntentModifyParamsPaymentMethodDataKlarnaDob,
        PaymentIntentModifyParamsPaymentMethodDataKonbini as PaymentIntentModifyParamsPaymentMethodDataKonbini,
        PaymentIntentModifyParamsPaymentMethodDataKrCard as PaymentIntentModifyParamsPaymentMethodDataKrCard,
        PaymentIntentModifyParamsPaymentMethodDataLink as PaymentIntentModifyParamsPaymentMethodDataLink,
        PaymentIntentModifyParamsPaymentMethodDataMbWay as PaymentIntentModifyParamsPaymentMethodDataMbWay,
        PaymentIntentModifyParamsPaymentMethodDataMobilepay as PaymentIntentModifyParamsPaymentMethodDataMobilepay,
        PaymentIntentModifyParamsPaymentMethodDataMultibanco as PaymentIntentModifyParamsPaymentMethodDataMultibanco,
        PaymentIntentModifyParamsPaymentMethodDataNaverPay as PaymentIntentModifyParamsPaymentMethodDataNaverPay,
        PaymentIntentModifyParamsPaymentMethodDataNzBankAccount as PaymentIntentModifyParamsPaymentMethodDataNzBankAccount,
        PaymentIntentModifyParamsPaymentMethodDataOxxo as PaymentIntentModifyParamsPaymentMethodDataOxxo,
        PaymentIntentModifyParamsPaymentMethodDataP24 as PaymentIntentModifyParamsPaymentMethodDataP24,
        PaymentIntentModifyParamsPaymentMethodDataPayByBank as PaymentIntentModifyParamsPaymentMethodDataPayByBank,
        PaymentIntentModifyParamsPaymentMethodDataPayco as PaymentIntentModifyParamsPaymentMethodDataPayco,
        PaymentIntentModifyParamsPaymentMethodDataPaynow as PaymentIntentModifyParamsPaymentMethodDataPaynow,
        PaymentIntentModifyParamsPaymentMethodDataPaypal as PaymentIntentModifyParamsPaymentMethodDataPaypal,
        PaymentIntentModifyParamsPaymentMethodDataPayto as PaymentIntentModifyParamsPaymentMethodDataPayto,
        PaymentIntentModifyParamsPaymentMethodDataPix as PaymentIntentModifyParamsPaymentMethodDataPix,
        PaymentIntentModifyParamsPaymentMethodDataPromptpay as PaymentIntentModifyParamsPaymentMethodDataPromptpay,
        PaymentIntentModifyParamsPaymentMethodDataRadarOptions as PaymentIntentModifyParamsPaymentMethodDataRadarOptions,
        PaymentIntentModifyParamsPaymentMethodDataRevolutPay as PaymentIntentModifyParamsPaymentMethodDataRevolutPay,
        PaymentIntentModifyParamsPaymentMethodDataSamsungPay as PaymentIntentModifyParamsPaymentMethodDataSamsungPay,
        PaymentIntentModifyParamsPaymentMethodDataSatispay as PaymentIntentModifyParamsPaymentMethodDataSatispay,
        PaymentIntentModifyParamsPaymentMethodDataSepaDebit as PaymentIntentModifyParamsPaymentMethodDataSepaDebit,
        PaymentIntentModifyParamsPaymentMethodDataSofort as PaymentIntentModifyParamsPaymentMethodDataSofort,
        PaymentIntentModifyParamsPaymentMethodDataSwish as PaymentIntentModifyParamsPaymentMethodDataSwish,
        PaymentIntentModifyParamsPaymentMethodDataTwint as PaymentIntentModifyParamsPaymentMethodDataTwint,
        PaymentIntentModifyParamsPaymentMethodDataUsBankAccount as PaymentIntentModifyParamsPaymentMethodDataUsBankAccount,
        PaymentIntentModifyParamsPaymentMethodDataWechatPay as PaymentIntentModifyParamsPaymentMethodDataWechatPay,
        PaymentIntentModifyParamsPaymentMethodDataZip as PaymentIntentModifyParamsPaymentMethodDataZip,
        PaymentIntentModifyParamsPaymentMethodOptions as PaymentIntentModifyParamsPaymentMethodOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsAcssDebit as PaymentIntentModifyParamsPaymentMethodOptionsAcssDebit,
        PaymentIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsAffirm as PaymentIntentModifyParamsPaymentMethodOptionsAffirm,
        PaymentIntentModifyParamsPaymentMethodOptionsAfterpayClearpay as PaymentIntentModifyParamsPaymentMethodOptionsAfterpayClearpay,
        PaymentIntentModifyParamsPaymentMethodOptionsAlipay as PaymentIntentModifyParamsPaymentMethodOptionsAlipay,
        PaymentIntentModifyParamsPaymentMethodOptionsAlma as PaymentIntentModifyParamsPaymentMethodOptionsAlma,
        PaymentIntentModifyParamsPaymentMethodOptionsAmazonPay as PaymentIntentModifyParamsPaymentMethodOptionsAmazonPay,
        PaymentIntentModifyParamsPaymentMethodOptionsAuBecsDebit as PaymentIntentModifyParamsPaymentMethodOptionsAuBecsDebit,
        PaymentIntentModifyParamsPaymentMethodOptionsBacsDebit as PaymentIntentModifyParamsPaymentMethodOptionsBacsDebit,
        PaymentIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsBancontact as PaymentIntentModifyParamsPaymentMethodOptionsBancontact,
        PaymentIntentModifyParamsPaymentMethodOptionsBillie as PaymentIntentModifyParamsPaymentMethodOptionsBillie,
        PaymentIntentModifyParamsPaymentMethodOptionsBlik as PaymentIntentModifyParamsPaymentMethodOptionsBlik,
        PaymentIntentModifyParamsPaymentMethodOptionsBoleto as PaymentIntentModifyParamsPaymentMethodOptionsBoleto,
        PaymentIntentModifyParamsPaymentMethodOptionsCard as PaymentIntentModifyParamsPaymentMethodOptionsCard,
        PaymentIntentModifyParamsPaymentMethodOptionsCardInstallments as PaymentIntentModifyParamsPaymentMethodOptionsCardInstallments,
        PaymentIntentModifyParamsPaymentMethodOptionsCardInstallmentsPlan as PaymentIntentModifyParamsPaymentMethodOptionsCardInstallmentsPlan,
        PaymentIntentModifyParamsPaymentMethodOptionsCardMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsCardMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsCardPresent as PaymentIntentModifyParamsPaymentMethodOptionsCardPresent,
        PaymentIntentModifyParamsPaymentMethodOptionsCardPresentRouting as PaymentIntentModifyParamsPaymentMethodOptionsCardPresentRouting,
        PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecure as PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecure,
        PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        PaymentIntentModifyParamsPaymentMethodOptionsCashapp as PaymentIntentModifyParamsPaymentMethodOptionsCashapp,
        PaymentIntentModifyParamsPaymentMethodOptionsCrypto as PaymentIntentModifyParamsPaymentMethodOptionsCrypto,
        PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalance as PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalance,
        PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransfer as PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransfer,
        PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        PaymentIntentModifyParamsPaymentMethodOptionsEps as PaymentIntentModifyParamsPaymentMethodOptionsEps,
        PaymentIntentModifyParamsPaymentMethodOptionsFpx as PaymentIntentModifyParamsPaymentMethodOptionsFpx,
        PaymentIntentModifyParamsPaymentMethodOptionsGiropay as PaymentIntentModifyParamsPaymentMethodOptionsGiropay,
        PaymentIntentModifyParamsPaymentMethodOptionsGrabpay as PaymentIntentModifyParamsPaymentMethodOptionsGrabpay,
        PaymentIntentModifyParamsPaymentMethodOptionsIdeal as PaymentIntentModifyParamsPaymentMethodOptionsIdeal,
        PaymentIntentModifyParamsPaymentMethodOptionsInteracPresent as PaymentIntentModifyParamsPaymentMethodOptionsInteracPresent,
        PaymentIntentModifyParamsPaymentMethodOptionsKakaoPay as PaymentIntentModifyParamsPaymentMethodOptionsKakaoPay,
        PaymentIntentModifyParamsPaymentMethodOptionsKlarna as PaymentIntentModifyParamsPaymentMethodOptionsKlarna,
        PaymentIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand as PaymentIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand,
        PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscription as PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscription,
        PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        PaymentIntentModifyParamsPaymentMethodOptionsKonbini as PaymentIntentModifyParamsPaymentMethodOptionsKonbini,
        PaymentIntentModifyParamsPaymentMethodOptionsKrCard as PaymentIntentModifyParamsPaymentMethodOptionsKrCard,
        PaymentIntentModifyParamsPaymentMethodOptionsLink as PaymentIntentModifyParamsPaymentMethodOptionsLink,
        PaymentIntentModifyParamsPaymentMethodOptionsMbWay as PaymentIntentModifyParamsPaymentMethodOptionsMbWay,
        PaymentIntentModifyParamsPaymentMethodOptionsMobilepay as PaymentIntentModifyParamsPaymentMethodOptionsMobilepay,
        PaymentIntentModifyParamsPaymentMethodOptionsMultibanco as PaymentIntentModifyParamsPaymentMethodOptionsMultibanco,
        PaymentIntentModifyParamsPaymentMethodOptionsNaverPay as PaymentIntentModifyParamsPaymentMethodOptionsNaverPay,
        PaymentIntentModifyParamsPaymentMethodOptionsNzBankAccount as PaymentIntentModifyParamsPaymentMethodOptionsNzBankAccount,
        PaymentIntentModifyParamsPaymentMethodOptionsOxxo as PaymentIntentModifyParamsPaymentMethodOptionsOxxo,
        PaymentIntentModifyParamsPaymentMethodOptionsP24 as PaymentIntentModifyParamsPaymentMethodOptionsP24,
        PaymentIntentModifyParamsPaymentMethodOptionsPayByBank as PaymentIntentModifyParamsPaymentMethodOptionsPayByBank,
        PaymentIntentModifyParamsPaymentMethodOptionsPayco as PaymentIntentModifyParamsPaymentMethodOptionsPayco,
        PaymentIntentModifyParamsPaymentMethodOptionsPaynow as PaymentIntentModifyParamsPaymentMethodOptionsPaynow,
        PaymentIntentModifyParamsPaymentMethodOptionsPaypal as PaymentIntentModifyParamsPaymentMethodOptionsPaypal,
        PaymentIntentModifyParamsPaymentMethodOptionsPayto as PaymentIntentModifyParamsPaymentMethodOptionsPayto,
        PaymentIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsPix as PaymentIntentModifyParamsPaymentMethodOptionsPix,
        PaymentIntentModifyParamsPaymentMethodOptionsPromptpay as PaymentIntentModifyParamsPaymentMethodOptionsPromptpay,
        PaymentIntentModifyParamsPaymentMethodOptionsRevolutPay as PaymentIntentModifyParamsPaymentMethodOptionsRevolutPay,
        PaymentIntentModifyParamsPaymentMethodOptionsSamsungPay as PaymentIntentModifyParamsPaymentMethodOptionsSamsungPay,
        PaymentIntentModifyParamsPaymentMethodOptionsSatispay as PaymentIntentModifyParamsPaymentMethodOptionsSatispay,
        PaymentIntentModifyParamsPaymentMethodOptionsSepaDebit as PaymentIntentModifyParamsPaymentMethodOptionsSepaDebit,
        PaymentIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsSofort as PaymentIntentModifyParamsPaymentMethodOptionsSofort,
        PaymentIntentModifyParamsPaymentMethodOptionsSwish as PaymentIntentModifyParamsPaymentMethodOptionsSwish,
        PaymentIntentModifyParamsPaymentMethodOptionsTwint as PaymentIntentModifyParamsPaymentMethodOptionsTwint,
        PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccount as PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccount,
        PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections as PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions as PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks as PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks,
        PaymentIntentModifyParamsPaymentMethodOptionsWechatPay as PaymentIntentModifyParamsPaymentMethodOptionsWechatPay,
        PaymentIntentModifyParamsPaymentMethodOptionsZip as PaymentIntentModifyParamsPaymentMethodOptionsZip,
        PaymentIntentModifyParamsShipping as PaymentIntentModifyParamsShipping,
        PaymentIntentModifyParamsShippingAddress as PaymentIntentModifyParamsShippingAddress,
        PaymentIntentModifyParamsTransferData as PaymentIntentModifyParamsTransferData,
    )
    from stripe.params._payment_intent_retrieve_params import (
        PaymentIntentRetrieveParams as PaymentIntentRetrieveParams,
    )
    from stripe.params._payment_intent_search_params import (
        PaymentIntentSearchParams as PaymentIntentSearchParams,
    )
    from stripe.params._payment_intent_update_params import (
        PaymentIntentUpdateParams as PaymentIntentUpdateParams,
        PaymentIntentUpdateParamsAmountDetails as PaymentIntentUpdateParamsAmountDetails,
        PaymentIntentUpdateParamsAmountDetailsLineItem as PaymentIntentUpdateParamsAmountDetailsLineItem,
        PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptions as PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptions,
        PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCard as PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCard,
        PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent as PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent,
        PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna as PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna,
        PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal as PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal,
        PaymentIntentUpdateParamsAmountDetailsLineItemTax as PaymentIntentUpdateParamsAmountDetailsLineItemTax,
        PaymentIntentUpdateParamsAmountDetailsShipping as PaymentIntentUpdateParamsAmountDetailsShipping,
        PaymentIntentUpdateParamsAmountDetailsTax as PaymentIntentUpdateParamsAmountDetailsTax,
        PaymentIntentUpdateParamsHooks as PaymentIntentUpdateParamsHooks,
        PaymentIntentUpdateParamsHooksInputs as PaymentIntentUpdateParamsHooksInputs,
        PaymentIntentUpdateParamsHooksInputsTax as PaymentIntentUpdateParamsHooksInputsTax,
        PaymentIntentUpdateParamsPaymentDetails as PaymentIntentUpdateParamsPaymentDetails,
        PaymentIntentUpdateParamsPaymentMethodData as PaymentIntentUpdateParamsPaymentMethodData,
        PaymentIntentUpdateParamsPaymentMethodDataAcssDebit as PaymentIntentUpdateParamsPaymentMethodDataAcssDebit,
        PaymentIntentUpdateParamsPaymentMethodDataAffirm as PaymentIntentUpdateParamsPaymentMethodDataAffirm,
        PaymentIntentUpdateParamsPaymentMethodDataAfterpayClearpay as PaymentIntentUpdateParamsPaymentMethodDataAfterpayClearpay,
        PaymentIntentUpdateParamsPaymentMethodDataAlipay as PaymentIntentUpdateParamsPaymentMethodDataAlipay,
        PaymentIntentUpdateParamsPaymentMethodDataAlma as PaymentIntentUpdateParamsPaymentMethodDataAlma,
        PaymentIntentUpdateParamsPaymentMethodDataAmazonPay as PaymentIntentUpdateParamsPaymentMethodDataAmazonPay,
        PaymentIntentUpdateParamsPaymentMethodDataAuBecsDebit as PaymentIntentUpdateParamsPaymentMethodDataAuBecsDebit,
        PaymentIntentUpdateParamsPaymentMethodDataBacsDebit as PaymentIntentUpdateParamsPaymentMethodDataBacsDebit,
        PaymentIntentUpdateParamsPaymentMethodDataBancontact as PaymentIntentUpdateParamsPaymentMethodDataBancontact,
        PaymentIntentUpdateParamsPaymentMethodDataBillie as PaymentIntentUpdateParamsPaymentMethodDataBillie,
        PaymentIntentUpdateParamsPaymentMethodDataBillingDetails as PaymentIntentUpdateParamsPaymentMethodDataBillingDetails,
        PaymentIntentUpdateParamsPaymentMethodDataBillingDetailsAddress as PaymentIntentUpdateParamsPaymentMethodDataBillingDetailsAddress,
        PaymentIntentUpdateParamsPaymentMethodDataBlik as PaymentIntentUpdateParamsPaymentMethodDataBlik,
        PaymentIntentUpdateParamsPaymentMethodDataBoleto as PaymentIntentUpdateParamsPaymentMethodDataBoleto,
        PaymentIntentUpdateParamsPaymentMethodDataCashapp as PaymentIntentUpdateParamsPaymentMethodDataCashapp,
        PaymentIntentUpdateParamsPaymentMethodDataCrypto as PaymentIntentUpdateParamsPaymentMethodDataCrypto,
        PaymentIntentUpdateParamsPaymentMethodDataCustomerBalance as PaymentIntentUpdateParamsPaymentMethodDataCustomerBalance,
        PaymentIntentUpdateParamsPaymentMethodDataEps as PaymentIntentUpdateParamsPaymentMethodDataEps,
        PaymentIntentUpdateParamsPaymentMethodDataFpx as PaymentIntentUpdateParamsPaymentMethodDataFpx,
        PaymentIntentUpdateParamsPaymentMethodDataGiropay as PaymentIntentUpdateParamsPaymentMethodDataGiropay,
        PaymentIntentUpdateParamsPaymentMethodDataGrabpay as PaymentIntentUpdateParamsPaymentMethodDataGrabpay,
        PaymentIntentUpdateParamsPaymentMethodDataIdeal as PaymentIntentUpdateParamsPaymentMethodDataIdeal,
        PaymentIntentUpdateParamsPaymentMethodDataInteracPresent as PaymentIntentUpdateParamsPaymentMethodDataInteracPresent,
        PaymentIntentUpdateParamsPaymentMethodDataKakaoPay as PaymentIntentUpdateParamsPaymentMethodDataKakaoPay,
        PaymentIntentUpdateParamsPaymentMethodDataKlarna as PaymentIntentUpdateParamsPaymentMethodDataKlarna,
        PaymentIntentUpdateParamsPaymentMethodDataKlarnaDob as PaymentIntentUpdateParamsPaymentMethodDataKlarnaDob,
        PaymentIntentUpdateParamsPaymentMethodDataKonbini as PaymentIntentUpdateParamsPaymentMethodDataKonbini,
        PaymentIntentUpdateParamsPaymentMethodDataKrCard as PaymentIntentUpdateParamsPaymentMethodDataKrCard,
        PaymentIntentUpdateParamsPaymentMethodDataLink as PaymentIntentUpdateParamsPaymentMethodDataLink,
        PaymentIntentUpdateParamsPaymentMethodDataMbWay as PaymentIntentUpdateParamsPaymentMethodDataMbWay,
        PaymentIntentUpdateParamsPaymentMethodDataMobilepay as PaymentIntentUpdateParamsPaymentMethodDataMobilepay,
        PaymentIntentUpdateParamsPaymentMethodDataMultibanco as PaymentIntentUpdateParamsPaymentMethodDataMultibanco,
        PaymentIntentUpdateParamsPaymentMethodDataNaverPay as PaymentIntentUpdateParamsPaymentMethodDataNaverPay,
        PaymentIntentUpdateParamsPaymentMethodDataNzBankAccount as PaymentIntentUpdateParamsPaymentMethodDataNzBankAccount,
        PaymentIntentUpdateParamsPaymentMethodDataOxxo as PaymentIntentUpdateParamsPaymentMethodDataOxxo,
        PaymentIntentUpdateParamsPaymentMethodDataP24 as PaymentIntentUpdateParamsPaymentMethodDataP24,
        PaymentIntentUpdateParamsPaymentMethodDataPayByBank as PaymentIntentUpdateParamsPaymentMethodDataPayByBank,
        PaymentIntentUpdateParamsPaymentMethodDataPayco as PaymentIntentUpdateParamsPaymentMethodDataPayco,
        PaymentIntentUpdateParamsPaymentMethodDataPaynow as PaymentIntentUpdateParamsPaymentMethodDataPaynow,
        PaymentIntentUpdateParamsPaymentMethodDataPaypal as PaymentIntentUpdateParamsPaymentMethodDataPaypal,
        PaymentIntentUpdateParamsPaymentMethodDataPayto as PaymentIntentUpdateParamsPaymentMethodDataPayto,
        PaymentIntentUpdateParamsPaymentMethodDataPix as PaymentIntentUpdateParamsPaymentMethodDataPix,
        PaymentIntentUpdateParamsPaymentMethodDataPromptpay as PaymentIntentUpdateParamsPaymentMethodDataPromptpay,
        PaymentIntentUpdateParamsPaymentMethodDataRadarOptions as PaymentIntentUpdateParamsPaymentMethodDataRadarOptions,
        PaymentIntentUpdateParamsPaymentMethodDataRevolutPay as PaymentIntentUpdateParamsPaymentMethodDataRevolutPay,
        PaymentIntentUpdateParamsPaymentMethodDataSamsungPay as PaymentIntentUpdateParamsPaymentMethodDataSamsungPay,
        PaymentIntentUpdateParamsPaymentMethodDataSatispay as PaymentIntentUpdateParamsPaymentMethodDataSatispay,
        PaymentIntentUpdateParamsPaymentMethodDataSepaDebit as PaymentIntentUpdateParamsPaymentMethodDataSepaDebit,
        PaymentIntentUpdateParamsPaymentMethodDataSofort as PaymentIntentUpdateParamsPaymentMethodDataSofort,
        PaymentIntentUpdateParamsPaymentMethodDataSwish as PaymentIntentUpdateParamsPaymentMethodDataSwish,
        PaymentIntentUpdateParamsPaymentMethodDataTwint as PaymentIntentUpdateParamsPaymentMethodDataTwint,
        PaymentIntentUpdateParamsPaymentMethodDataUsBankAccount as PaymentIntentUpdateParamsPaymentMethodDataUsBankAccount,
        PaymentIntentUpdateParamsPaymentMethodDataWechatPay as PaymentIntentUpdateParamsPaymentMethodDataWechatPay,
        PaymentIntentUpdateParamsPaymentMethodDataZip as PaymentIntentUpdateParamsPaymentMethodDataZip,
        PaymentIntentUpdateParamsPaymentMethodOptions as PaymentIntentUpdateParamsPaymentMethodOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebit as PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebit,
        PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsAffirm as PaymentIntentUpdateParamsPaymentMethodOptionsAffirm,
        PaymentIntentUpdateParamsPaymentMethodOptionsAfterpayClearpay as PaymentIntentUpdateParamsPaymentMethodOptionsAfterpayClearpay,
        PaymentIntentUpdateParamsPaymentMethodOptionsAlipay as PaymentIntentUpdateParamsPaymentMethodOptionsAlipay,
        PaymentIntentUpdateParamsPaymentMethodOptionsAlma as PaymentIntentUpdateParamsPaymentMethodOptionsAlma,
        PaymentIntentUpdateParamsPaymentMethodOptionsAmazonPay as PaymentIntentUpdateParamsPaymentMethodOptionsAmazonPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsAuBecsDebit as PaymentIntentUpdateParamsPaymentMethodOptionsAuBecsDebit,
        PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebit as PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebit,
        PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsBancontact as PaymentIntentUpdateParamsPaymentMethodOptionsBancontact,
        PaymentIntentUpdateParamsPaymentMethodOptionsBillie as PaymentIntentUpdateParamsPaymentMethodOptionsBillie,
        PaymentIntentUpdateParamsPaymentMethodOptionsBlik as PaymentIntentUpdateParamsPaymentMethodOptionsBlik,
        PaymentIntentUpdateParamsPaymentMethodOptionsBoleto as PaymentIntentUpdateParamsPaymentMethodOptionsBoleto,
        PaymentIntentUpdateParamsPaymentMethodOptionsCard as PaymentIntentUpdateParamsPaymentMethodOptionsCard,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallments as PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallments,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallmentsPlan as PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallmentsPlan,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsCardMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardPresent as PaymentIntentUpdateParamsPaymentMethodOptionsCardPresent,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardPresentRouting as PaymentIntentUpdateParamsPaymentMethodOptionsCardPresentRouting,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure as PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        PaymentIntentUpdateParamsPaymentMethodOptionsCashapp as PaymentIntentUpdateParamsPaymentMethodOptionsCashapp,
        PaymentIntentUpdateParamsPaymentMethodOptionsCrypto as PaymentIntentUpdateParamsPaymentMethodOptionsCrypto,
        PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalance as PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalance,
        PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransfer as PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransfer,
        PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        PaymentIntentUpdateParamsPaymentMethodOptionsEps as PaymentIntentUpdateParamsPaymentMethodOptionsEps,
        PaymentIntentUpdateParamsPaymentMethodOptionsFpx as PaymentIntentUpdateParamsPaymentMethodOptionsFpx,
        PaymentIntentUpdateParamsPaymentMethodOptionsGiropay as PaymentIntentUpdateParamsPaymentMethodOptionsGiropay,
        PaymentIntentUpdateParamsPaymentMethodOptionsGrabpay as PaymentIntentUpdateParamsPaymentMethodOptionsGrabpay,
        PaymentIntentUpdateParamsPaymentMethodOptionsIdeal as PaymentIntentUpdateParamsPaymentMethodOptionsIdeal,
        PaymentIntentUpdateParamsPaymentMethodOptionsInteracPresent as PaymentIntentUpdateParamsPaymentMethodOptionsInteracPresent,
        PaymentIntentUpdateParamsPaymentMethodOptionsKakaoPay as PaymentIntentUpdateParamsPaymentMethodOptionsKakaoPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsKlarna as PaymentIntentUpdateParamsPaymentMethodOptionsKlarna,
        PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand as PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand,
        PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription as PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription,
        PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        PaymentIntentUpdateParamsPaymentMethodOptionsKonbini as PaymentIntentUpdateParamsPaymentMethodOptionsKonbini,
        PaymentIntentUpdateParamsPaymentMethodOptionsKrCard as PaymentIntentUpdateParamsPaymentMethodOptionsKrCard,
        PaymentIntentUpdateParamsPaymentMethodOptionsLink as PaymentIntentUpdateParamsPaymentMethodOptionsLink,
        PaymentIntentUpdateParamsPaymentMethodOptionsMbWay as PaymentIntentUpdateParamsPaymentMethodOptionsMbWay,
        PaymentIntentUpdateParamsPaymentMethodOptionsMobilepay as PaymentIntentUpdateParamsPaymentMethodOptionsMobilepay,
        PaymentIntentUpdateParamsPaymentMethodOptionsMultibanco as PaymentIntentUpdateParamsPaymentMethodOptionsMultibanco,
        PaymentIntentUpdateParamsPaymentMethodOptionsNaverPay as PaymentIntentUpdateParamsPaymentMethodOptionsNaverPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsNzBankAccount as PaymentIntentUpdateParamsPaymentMethodOptionsNzBankAccount,
        PaymentIntentUpdateParamsPaymentMethodOptionsOxxo as PaymentIntentUpdateParamsPaymentMethodOptionsOxxo,
        PaymentIntentUpdateParamsPaymentMethodOptionsP24 as PaymentIntentUpdateParamsPaymentMethodOptionsP24,
        PaymentIntentUpdateParamsPaymentMethodOptionsPayByBank as PaymentIntentUpdateParamsPaymentMethodOptionsPayByBank,
        PaymentIntentUpdateParamsPaymentMethodOptionsPayco as PaymentIntentUpdateParamsPaymentMethodOptionsPayco,
        PaymentIntentUpdateParamsPaymentMethodOptionsPaynow as PaymentIntentUpdateParamsPaymentMethodOptionsPaynow,
        PaymentIntentUpdateParamsPaymentMethodOptionsPaypal as PaymentIntentUpdateParamsPaymentMethodOptionsPaypal,
        PaymentIntentUpdateParamsPaymentMethodOptionsPayto as PaymentIntentUpdateParamsPaymentMethodOptionsPayto,
        PaymentIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsPix as PaymentIntentUpdateParamsPaymentMethodOptionsPix,
        PaymentIntentUpdateParamsPaymentMethodOptionsPromptpay as PaymentIntentUpdateParamsPaymentMethodOptionsPromptpay,
        PaymentIntentUpdateParamsPaymentMethodOptionsRevolutPay as PaymentIntentUpdateParamsPaymentMethodOptionsRevolutPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsSamsungPay as PaymentIntentUpdateParamsPaymentMethodOptionsSamsungPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsSatispay as PaymentIntentUpdateParamsPaymentMethodOptionsSatispay,
        PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebit as PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebit,
        PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsSofort as PaymentIntentUpdateParamsPaymentMethodOptionsSofort,
        PaymentIntentUpdateParamsPaymentMethodOptionsSwish as PaymentIntentUpdateParamsPaymentMethodOptionsSwish,
        PaymentIntentUpdateParamsPaymentMethodOptionsTwint as PaymentIntentUpdateParamsPaymentMethodOptionsTwint,
        PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccount as PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccount,
        PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections as PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions as PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks as PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks,
        PaymentIntentUpdateParamsPaymentMethodOptionsWechatPay as PaymentIntentUpdateParamsPaymentMethodOptionsWechatPay,
        PaymentIntentUpdateParamsPaymentMethodOptionsZip as PaymentIntentUpdateParamsPaymentMethodOptionsZip,
        PaymentIntentUpdateParamsShipping as PaymentIntentUpdateParamsShipping,
        PaymentIntentUpdateParamsShippingAddress as PaymentIntentUpdateParamsShippingAddress,
        PaymentIntentUpdateParamsTransferData as PaymentIntentUpdateParamsTransferData,
    )
    from stripe.params._payment_intent_verify_microdeposits_params import (
        PaymentIntentVerifyMicrodepositsParams as PaymentIntentVerifyMicrodepositsParams,
    )
    from stripe.params._payment_link_create_params import (
        PaymentLinkCreateParams as PaymentLinkCreateParams,
        PaymentLinkCreateParamsAfterCompletion as PaymentLinkCreateParamsAfterCompletion,
        PaymentLinkCreateParamsAfterCompletionHostedConfirmation as PaymentLinkCreateParamsAfterCompletionHostedConfirmation,
        PaymentLinkCreateParamsAfterCompletionRedirect as PaymentLinkCreateParamsAfterCompletionRedirect,
        PaymentLinkCreateParamsAutomaticTax as PaymentLinkCreateParamsAutomaticTax,
        PaymentLinkCreateParamsAutomaticTaxLiability as PaymentLinkCreateParamsAutomaticTaxLiability,
        PaymentLinkCreateParamsConsentCollection as PaymentLinkCreateParamsConsentCollection,
        PaymentLinkCreateParamsConsentCollectionPaymentMethodReuseAgreement as PaymentLinkCreateParamsConsentCollectionPaymentMethodReuseAgreement,
        PaymentLinkCreateParamsCustomField as PaymentLinkCreateParamsCustomField,
        PaymentLinkCreateParamsCustomFieldDropdown as PaymentLinkCreateParamsCustomFieldDropdown,
        PaymentLinkCreateParamsCustomFieldDropdownOption as PaymentLinkCreateParamsCustomFieldDropdownOption,
        PaymentLinkCreateParamsCustomFieldLabel as PaymentLinkCreateParamsCustomFieldLabel,
        PaymentLinkCreateParamsCustomFieldNumeric as PaymentLinkCreateParamsCustomFieldNumeric,
        PaymentLinkCreateParamsCustomFieldText as PaymentLinkCreateParamsCustomFieldText,
        PaymentLinkCreateParamsCustomText as PaymentLinkCreateParamsCustomText,
        PaymentLinkCreateParamsCustomTextAfterSubmit as PaymentLinkCreateParamsCustomTextAfterSubmit,
        PaymentLinkCreateParamsCustomTextShippingAddress as PaymentLinkCreateParamsCustomTextShippingAddress,
        PaymentLinkCreateParamsCustomTextSubmit as PaymentLinkCreateParamsCustomTextSubmit,
        PaymentLinkCreateParamsCustomTextTermsOfServiceAcceptance as PaymentLinkCreateParamsCustomTextTermsOfServiceAcceptance,
        PaymentLinkCreateParamsInvoiceCreation as PaymentLinkCreateParamsInvoiceCreation,
        PaymentLinkCreateParamsInvoiceCreationInvoiceData as PaymentLinkCreateParamsInvoiceCreationInvoiceData,
        PaymentLinkCreateParamsInvoiceCreationInvoiceDataCustomField as PaymentLinkCreateParamsInvoiceCreationInvoiceDataCustomField,
        PaymentLinkCreateParamsInvoiceCreationInvoiceDataIssuer as PaymentLinkCreateParamsInvoiceCreationInvoiceDataIssuer,
        PaymentLinkCreateParamsInvoiceCreationInvoiceDataRenderingOptions as PaymentLinkCreateParamsInvoiceCreationInvoiceDataRenderingOptions,
        PaymentLinkCreateParamsLineItem as PaymentLinkCreateParamsLineItem,
        PaymentLinkCreateParamsLineItemAdjustableQuantity as PaymentLinkCreateParamsLineItemAdjustableQuantity,
        PaymentLinkCreateParamsLineItemPriceData as PaymentLinkCreateParamsLineItemPriceData,
        PaymentLinkCreateParamsLineItemPriceDataProductData as PaymentLinkCreateParamsLineItemPriceDataProductData,
        PaymentLinkCreateParamsLineItemPriceDataRecurring as PaymentLinkCreateParamsLineItemPriceDataRecurring,
        PaymentLinkCreateParamsNameCollection as PaymentLinkCreateParamsNameCollection,
        PaymentLinkCreateParamsNameCollectionBusiness as PaymentLinkCreateParamsNameCollectionBusiness,
        PaymentLinkCreateParamsNameCollectionIndividual as PaymentLinkCreateParamsNameCollectionIndividual,
        PaymentLinkCreateParamsOptionalItem as PaymentLinkCreateParamsOptionalItem,
        PaymentLinkCreateParamsOptionalItemAdjustableQuantity as PaymentLinkCreateParamsOptionalItemAdjustableQuantity,
        PaymentLinkCreateParamsPaymentIntentData as PaymentLinkCreateParamsPaymentIntentData,
        PaymentLinkCreateParamsPhoneNumberCollection as PaymentLinkCreateParamsPhoneNumberCollection,
        PaymentLinkCreateParamsRestrictions as PaymentLinkCreateParamsRestrictions,
        PaymentLinkCreateParamsRestrictionsCompletedSessions as PaymentLinkCreateParamsRestrictionsCompletedSessions,
        PaymentLinkCreateParamsShippingAddressCollection as PaymentLinkCreateParamsShippingAddressCollection,
        PaymentLinkCreateParamsShippingOption as PaymentLinkCreateParamsShippingOption,
        PaymentLinkCreateParamsSubscriptionData as PaymentLinkCreateParamsSubscriptionData,
        PaymentLinkCreateParamsSubscriptionDataInvoiceSettings as PaymentLinkCreateParamsSubscriptionDataInvoiceSettings,
        PaymentLinkCreateParamsSubscriptionDataInvoiceSettingsIssuer as PaymentLinkCreateParamsSubscriptionDataInvoiceSettingsIssuer,
        PaymentLinkCreateParamsSubscriptionDataTrialSettings as PaymentLinkCreateParamsSubscriptionDataTrialSettings,
        PaymentLinkCreateParamsSubscriptionDataTrialSettingsEndBehavior as PaymentLinkCreateParamsSubscriptionDataTrialSettingsEndBehavior,
        PaymentLinkCreateParamsTaxIdCollection as PaymentLinkCreateParamsTaxIdCollection,
        PaymentLinkCreateParamsTransferData as PaymentLinkCreateParamsTransferData,
    )
    from stripe.params._payment_link_line_item_list_params import (
        PaymentLinkLineItemListParams as PaymentLinkLineItemListParams,
    )
    from stripe.params._payment_link_list_line_items_params import (
        PaymentLinkListLineItemsParams as PaymentLinkListLineItemsParams,
    )
    from stripe.params._payment_link_list_params import (
        PaymentLinkListParams as PaymentLinkListParams,
    )
    from stripe.params._payment_link_modify_params import (
        PaymentLinkModifyParams as PaymentLinkModifyParams,
        PaymentLinkModifyParamsAfterCompletion as PaymentLinkModifyParamsAfterCompletion,
        PaymentLinkModifyParamsAfterCompletionHostedConfirmation as PaymentLinkModifyParamsAfterCompletionHostedConfirmation,
        PaymentLinkModifyParamsAfterCompletionRedirect as PaymentLinkModifyParamsAfterCompletionRedirect,
        PaymentLinkModifyParamsAutomaticTax as PaymentLinkModifyParamsAutomaticTax,
        PaymentLinkModifyParamsAutomaticTaxLiability as PaymentLinkModifyParamsAutomaticTaxLiability,
        PaymentLinkModifyParamsCustomField as PaymentLinkModifyParamsCustomField,
        PaymentLinkModifyParamsCustomFieldDropdown as PaymentLinkModifyParamsCustomFieldDropdown,
        PaymentLinkModifyParamsCustomFieldDropdownOption as PaymentLinkModifyParamsCustomFieldDropdownOption,
        PaymentLinkModifyParamsCustomFieldLabel as PaymentLinkModifyParamsCustomFieldLabel,
        PaymentLinkModifyParamsCustomFieldNumeric as PaymentLinkModifyParamsCustomFieldNumeric,
        PaymentLinkModifyParamsCustomFieldText as PaymentLinkModifyParamsCustomFieldText,
        PaymentLinkModifyParamsCustomText as PaymentLinkModifyParamsCustomText,
        PaymentLinkModifyParamsCustomTextAfterSubmit as PaymentLinkModifyParamsCustomTextAfterSubmit,
        PaymentLinkModifyParamsCustomTextShippingAddress as PaymentLinkModifyParamsCustomTextShippingAddress,
        PaymentLinkModifyParamsCustomTextSubmit as PaymentLinkModifyParamsCustomTextSubmit,
        PaymentLinkModifyParamsCustomTextTermsOfServiceAcceptance as PaymentLinkModifyParamsCustomTextTermsOfServiceAcceptance,
        PaymentLinkModifyParamsInvoiceCreation as PaymentLinkModifyParamsInvoiceCreation,
        PaymentLinkModifyParamsInvoiceCreationInvoiceData as PaymentLinkModifyParamsInvoiceCreationInvoiceData,
        PaymentLinkModifyParamsInvoiceCreationInvoiceDataCustomField as PaymentLinkModifyParamsInvoiceCreationInvoiceDataCustomField,
        PaymentLinkModifyParamsInvoiceCreationInvoiceDataIssuer as PaymentLinkModifyParamsInvoiceCreationInvoiceDataIssuer,
        PaymentLinkModifyParamsInvoiceCreationInvoiceDataRenderingOptions as PaymentLinkModifyParamsInvoiceCreationInvoiceDataRenderingOptions,
        PaymentLinkModifyParamsLineItem as PaymentLinkModifyParamsLineItem,
        PaymentLinkModifyParamsLineItemAdjustableQuantity as PaymentLinkModifyParamsLineItemAdjustableQuantity,
        PaymentLinkModifyParamsNameCollection as PaymentLinkModifyParamsNameCollection,
        PaymentLinkModifyParamsNameCollectionBusiness as PaymentLinkModifyParamsNameCollectionBusiness,
        PaymentLinkModifyParamsNameCollectionIndividual as PaymentLinkModifyParamsNameCollectionIndividual,
        PaymentLinkModifyParamsOptionalItem as PaymentLinkModifyParamsOptionalItem,
        PaymentLinkModifyParamsOptionalItemAdjustableQuantity as PaymentLinkModifyParamsOptionalItemAdjustableQuantity,
        PaymentLinkModifyParamsPaymentIntentData as PaymentLinkModifyParamsPaymentIntentData,
        PaymentLinkModifyParamsPhoneNumberCollection as PaymentLinkModifyParamsPhoneNumberCollection,
        PaymentLinkModifyParamsRestrictions as PaymentLinkModifyParamsRestrictions,
        PaymentLinkModifyParamsRestrictionsCompletedSessions as PaymentLinkModifyParamsRestrictionsCompletedSessions,
        PaymentLinkModifyParamsShippingAddressCollection as PaymentLinkModifyParamsShippingAddressCollection,
        PaymentLinkModifyParamsSubscriptionData as PaymentLinkModifyParamsSubscriptionData,
        PaymentLinkModifyParamsSubscriptionDataInvoiceSettings as PaymentLinkModifyParamsSubscriptionDataInvoiceSettings,
        PaymentLinkModifyParamsSubscriptionDataInvoiceSettingsIssuer as PaymentLinkModifyParamsSubscriptionDataInvoiceSettingsIssuer,
        PaymentLinkModifyParamsSubscriptionDataTrialSettings as PaymentLinkModifyParamsSubscriptionDataTrialSettings,
        PaymentLinkModifyParamsSubscriptionDataTrialSettingsEndBehavior as PaymentLinkModifyParamsSubscriptionDataTrialSettingsEndBehavior,
        PaymentLinkModifyParamsTaxIdCollection as PaymentLinkModifyParamsTaxIdCollection,
    )
    from stripe.params._payment_link_retrieve_params import (
        PaymentLinkRetrieveParams as PaymentLinkRetrieveParams,
    )
    from stripe.params._payment_link_update_params import (
        PaymentLinkUpdateParams as PaymentLinkUpdateParams,
        PaymentLinkUpdateParamsAfterCompletion as PaymentLinkUpdateParamsAfterCompletion,
        PaymentLinkUpdateParamsAfterCompletionHostedConfirmation as PaymentLinkUpdateParamsAfterCompletionHostedConfirmation,
        PaymentLinkUpdateParamsAfterCompletionRedirect as PaymentLinkUpdateParamsAfterCompletionRedirect,
        PaymentLinkUpdateParamsAutomaticTax as PaymentLinkUpdateParamsAutomaticTax,
        PaymentLinkUpdateParamsAutomaticTaxLiability as PaymentLinkUpdateParamsAutomaticTaxLiability,
        PaymentLinkUpdateParamsCustomField as PaymentLinkUpdateParamsCustomField,
        PaymentLinkUpdateParamsCustomFieldDropdown as PaymentLinkUpdateParamsCustomFieldDropdown,
        PaymentLinkUpdateParamsCustomFieldDropdownOption as PaymentLinkUpdateParamsCustomFieldDropdownOption,
        PaymentLinkUpdateParamsCustomFieldLabel as PaymentLinkUpdateParamsCustomFieldLabel,
        PaymentLinkUpdateParamsCustomFieldNumeric as PaymentLinkUpdateParamsCustomFieldNumeric,
        PaymentLinkUpdateParamsCustomFieldText as PaymentLinkUpdateParamsCustomFieldText,
        PaymentLinkUpdateParamsCustomText as PaymentLinkUpdateParamsCustomText,
        PaymentLinkUpdateParamsCustomTextAfterSubmit as PaymentLinkUpdateParamsCustomTextAfterSubmit,
        PaymentLinkUpdateParamsCustomTextShippingAddress as PaymentLinkUpdateParamsCustomTextShippingAddress,
        PaymentLinkUpdateParamsCustomTextSubmit as PaymentLinkUpdateParamsCustomTextSubmit,
        PaymentLinkUpdateParamsCustomTextTermsOfServiceAcceptance as PaymentLinkUpdateParamsCustomTextTermsOfServiceAcceptance,
        PaymentLinkUpdateParamsInvoiceCreation as PaymentLinkUpdateParamsInvoiceCreation,
        PaymentLinkUpdateParamsInvoiceCreationInvoiceData as PaymentLinkUpdateParamsInvoiceCreationInvoiceData,
        PaymentLinkUpdateParamsInvoiceCreationInvoiceDataCustomField as PaymentLinkUpdateParamsInvoiceCreationInvoiceDataCustomField,
        PaymentLinkUpdateParamsInvoiceCreationInvoiceDataIssuer as PaymentLinkUpdateParamsInvoiceCreationInvoiceDataIssuer,
        PaymentLinkUpdateParamsInvoiceCreationInvoiceDataRenderingOptions as PaymentLinkUpdateParamsInvoiceCreationInvoiceDataRenderingOptions,
        PaymentLinkUpdateParamsLineItem as PaymentLinkUpdateParamsLineItem,
        PaymentLinkUpdateParamsLineItemAdjustableQuantity as PaymentLinkUpdateParamsLineItemAdjustableQuantity,
        PaymentLinkUpdateParamsNameCollection as PaymentLinkUpdateParamsNameCollection,
        PaymentLinkUpdateParamsNameCollectionBusiness as PaymentLinkUpdateParamsNameCollectionBusiness,
        PaymentLinkUpdateParamsNameCollectionIndividual as PaymentLinkUpdateParamsNameCollectionIndividual,
        PaymentLinkUpdateParamsOptionalItem as PaymentLinkUpdateParamsOptionalItem,
        PaymentLinkUpdateParamsOptionalItemAdjustableQuantity as PaymentLinkUpdateParamsOptionalItemAdjustableQuantity,
        PaymentLinkUpdateParamsPaymentIntentData as PaymentLinkUpdateParamsPaymentIntentData,
        PaymentLinkUpdateParamsPhoneNumberCollection as PaymentLinkUpdateParamsPhoneNumberCollection,
        PaymentLinkUpdateParamsRestrictions as PaymentLinkUpdateParamsRestrictions,
        PaymentLinkUpdateParamsRestrictionsCompletedSessions as PaymentLinkUpdateParamsRestrictionsCompletedSessions,
        PaymentLinkUpdateParamsShippingAddressCollection as PaymentLinkUpdateParamsShippingAddressCollection,
        PaymentLinkUpdateParamsSubscriptionData as PaymentLinkUpdateParamsSubscriptionData,
        PaymentLinkUpdateParamsSubscriptionDataInvoiceSettings as PaymentLinkUpdateParamsSubscriptionDataInvoiceSettings,
        PaymentLinkUpdateParamsSubscriptionDataInvoiceSettingsIssuer as PaymentLinkUpdateParamsSubscriptionDataInvoiceSettingsIssuer,
        PaymentLinkUpdateParamsSubscriptionDataTrialSettings as PaymentLinkUpdateParamsSubscriptionDataTrialSettings,
        PaymentLinkUpdateParamsSubscriptionDataTrialSettingsEndBehavior as PaymentLinkUpdateParamsSubscriptionDataTrialSettingsEndBehavior,
        PaymentLinkUpdateParamsTaxIdCollection as PaymentLinkUpdateParamsTaxIdCollection,
    )
    from stripe.params._payment_method_attach_params import (
        PaymentMethodAttachParams as PaymentMethodAttachParams,
    )
    from stripe.params._payment_method_configuration_create_params import (
        PaymentMethodConfigurationCreateParams as PaymentMethodConfigurationCreateParams,
        PaymentMethodConfigurationCreateParamsAcssDebit as PaymentMethodConfigurationCreateParamsAcssDebit,
        PaymentMethodConfigurationCreateParamsAcssDebitDisplayPreference as PaymentMethodConfigurationCreateParamsAcssDebitDisplayPreference,
        PaymentMethodConfigurationCreateParamsAffirm as PaymentMethodConfigurationCreateParamsAffirm,
        PaymentMethodConfigurationCreateParamsAffirmDisplayPreference as PaymentMethodConfigurationCreateParamsAffirmDisplayPreference,
        PaymentMethodConfigurationCreateParamsAfterpayClearpay as PaymentMethodConfigurationCreateParamsAfterpayClearpay,
        PaymentMethodConfigurationCreateParamsAfterpayClearpayDisplayPreference as PaymentMethodConfigurationCreateParamsAfterpayClearpayDisplayPreference,
        PaymentMethodConfigurationCreateParamsAlipay as PaymentMethodConfigurationCreateParamsAlipay,
        PaymentMethodConfigurationCreateParamsAlipayDisplayPreference as PaymentMethodConfigurationCreateParamsAlipayDisplayPreference,
        PaymentMethodConfigurationCreateParamsAlma as PaymentMethodConfigurationCreateParamsAlma,
        PaymentMethodConfigurationCreateParamsAlmaDisplayPreference as PaymentMethodConfigurationCreateParamsAlmaDisplayPreference,
        PaymentMethodConfigurationCreateParamsAmazonPay as PaymentMethodConfigurationCreateParamsAmazonPay,
        PaymentMethodConfigurationCreateParamsAmazonPayDisplayPreference as PaymentMethodConfigurationCreateParamsAmazonPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsApplePay as PaymentMethodConfigurationCreateParamsApplePay,
        PaymentMethodConfigurationCreateParamsApplePayDisplayPreference as PaymentMethodConfigurationCreateParamsApplePayDisplayPreference,
        PaymentMethodConfigurationCreateParamsApplePayLater as PaymentMethodConfigurationCreateParamsApplePayLater,
        PaymentMethodConfigurationCreateParamsApplePayLaterDisplayPreference as PaymentMethodConfigurationCreateParamsApplePayLaterDisplayPreference,
        PaymentMethodConfigurationCreateParamsAuBecsDebit as PaymentMethodConfigurationCreateParamsAuBecsDebit,
        PaymentMethodConfigurationCreateParamsAuBecsDebitDisplayPreference as PaymentMethodConfigurationCreateParamsAuBecsDebitDisplayPreference,
        PaymentMethodConfigurationCreateParamsBacsDebit as PaymentMethodConfigurationCreateParamsBacsDebit,
        PaymentMethodConfigurationCreateParamsBacsDebitDisplayPreference as PaymentMethodConfigurationCreateParamsBacsDebitDisplayPreference,
        PaymentMethodConfigurationCreateParamsBancontact as PaymentMethodConfigurationCreateParamsBancontact,
        PaymentMethodConfigurationCreateParamsBancontactDisplayPreference as PaymentMethodConfigurationCreateParamsBancontactDisplayPreference,
        PaymentMethodConfigurationCreateParamsBillie as PaymentMethodConfigurationCreateParamsBillie,
        PaymentMethodConfigurationCreateParamsBillieDisplayPreference as PaymentMethodConfigurationCreateParamsBillieDisplayPreference,
        PaymentMethodConfigurationCreateParamsBlik as PaymentMethodConfigurationCreateParamsBlik,
        PaymentMethodConfigurationCreateParamsBlikDisplayPreference as PaymentMethodConfigurationCreateParamsBlikDisplayPreference,
        PaymentMethodConfigurationCreateParamsBoleto as PaymentMethodConfigurationCreateParamsBoleto,
        PaymentMethodConfigurationCreateParamsBoletoDisplayPreference as PaymentMethodConfigurationCreateParamsBoletoDisplayPreference,
        PaymentMethodConfigurationCreateParamsCard as PaymentMethodConfigurationCreateParamsCard,
        PaymentMethodConfigurationCreateParamsCardDisplayPreference as PaymentMethodConfigurationCreateParamsCardDisplayPreference,
        PaymentMethodConfigurationCreateParamsCartesBancaires as PaymentMethodConfigurationCreateParamsCartesBancaires,
        PaymentMethodConfigurationCreateParamsCartesBancairesDisplayPreference as PaymentMethodConfigurationCreateParamsCartesBancairesDisplayPreference,
        PaymentMethodConfigurationCreateParamsCashapp as PaymentMethodConfigurationCreateParamsCashapp,
        PaymentMethodConfigurationCreateParamsCashappDisplayPreference as PaymentMethodConfigurationCreateParamsCashappDisplayPreference,
        PaymentMethodConfigurationCreateParamsCrypto as PaymentMethodConfigurationCreateParamsCrypto,
        PaymentMethodConfigurationCreateParamsCryptoDisplayPreference as PaymentMethodConfigurationCreateParamsCryptoDisplayPreference,
        PaymentMethodConfigurationCreateParamsCustomerBalance as PaymentMethodConfigurationCreateParamsCustomerBalance,
        PaymentMethodConfigurationCreateParamsCustomerBalanceDisplayPreference as PaymentMethodConfigurationCreateParamsCustomerBalanceDisplayPreference,
        PaymentMethodConfigurationCreateParamsEps as PaymentMethodConfigurationCreateParamsEps,
        PaymentMethodConfigurationCreateParamsEpsDisplayPreference as PaymentMethodConfigurationCreateParamsEpsDisplayPreference,
        PaymentMethodConfigurationCreateParamsFpx as PaymentMethodConfigurationCreateParamsFpx,
        PaymentMethodConfigurationCreateParamsFpxDisplayPreference as PaymentMethodConfigurationCreateParamsFpxDisplayPreference,
        PaymentMethodConfigurationCreateParamsFrMealVoucherConecs as PaymentMethodConfigurationCreateParamsFrMealVoucherConecs,
        PaymentMethodConfigurationCreateParamsFrMealVoucherConecsDisplayPreference as PaymentMethodConfigurationCreateParamsFrMealVoucherConecsDisplayPreference,
        PaymentMethodConfigurationCreateParamsGiropay as PaymentMethodConfigurationCreateParamsGiropay,
        PaymentMethodConfigurationCreateParamsGiropayDisplayPreference as PaymentMethodConfigurationCreateParamsGiropayDisplayPreference,
        PaymentMethodConfigurationCreateParamsGooglePay as PaymentMethodConfigurationCreateParamsGooglePay,
        PaymentMethodConfigurationCreateParamsGooglePayDisplayPreference as PaymentMethodConfigurationCreateParamsGooglePayDisplayPreference,
        PaymentMethodConfigurationCreateParamsGrabpay as PaymentMethodConfigurationCreateParamsGrabpay,
        PaymentMethodConfigurationCreateParamsGrabpayDisplayPreference as PaymentMethodConfigurationCreateParamsGrabpayDisplayPreference,
        PaymentMethodConfigurationCreateParamsIdeal as PaymentMethodConfigurationCreateParamsIdeal,
        PaymentMethodConfigurationCreateParamsIdealDisplayPreference as PaymentMethodConfigurationCreateParamsIdealDisplayPreference,
        PaymentMethodConfigurationCreateParamsJcb as PaymentMethodConfigurationCreateParamsJcb,
        PaymentMethodConfigurationCreateParamsJcbDisplayPreference as PaymentMethodConfigurationCreateParamsJcbDisplayPreference,
        PaymentMethodConfigurationCreateParamsKakaoPay as PaymentMethodConfigurationCreateParamsKakaoPay,
        PaymentMethodConfigurationCreateParamsKakaoPayDisplayPreference as PaymentMethodConfigurationCreateParamsKakaoPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsKlarna as PaymentMethodConfigurationCreateParamsKlarna,
        PaymentMethodConfigurationCreateParamsKlarnaDisplayPreference as PaymentMethodConfigurationCreateParamsKlarnaDisplayPreference,
        PaymentMethodConfigurationCreateParamsKonbini as PaymentMethodConfigurationCreateParamsKonbini,
        PaymentMethodConfigurationCreateParamsKonbiniDisplayPreference as PaymentMethodConfigurationCreateParamsKonbiniDisplayPreference,
        PaymentMethodConfigurationCreateParamsKrCard as PaymentMethodConfigurationCreateParamsKrCard,
        PaymentMethodConfigurationCreateParamsKrCardDisplayPreference as PaymentMethodConfigurationCreateParamsKrCardDisplayPreference,
        PaymentMethodConfigurationCreateParamsLink as PaymentMethodConfigurationCreateParamsLink,
        PaymentMethodConfigurationCreateParamsLinkDisplayPreference as PaymentMethodConfigurationCreateParamsLinkDisplayPreference,
        PaymentMethodConfigurationCreateParamsMbWay as PaymentMethodConfigurationCreateParamsMbWay,
        PaymentMethodConfigurationCreateParamsMbWayDisplayPreference as PaymentMethodConfigurationCreateParamsMbWayDisplayPreference,
        PaymentMethodConfigurationCreateParamsMobilepay as PaymentMethodConfigurationCreateParamsMobilepay,
        PaymentMethodConfigurationCreateParamsMobilepayDisplayPreference as PaymentMethodConfigurationCreateParamsMobilepayDisplayPreference,
        PaymentMethodConfigurationCreateParamsMultibanco as PaymentMethodConfigurationCreateParamsMultibanco,
        PaymentMethodConfigurationCreateParamsMultibancoDisplayPreference as PaymentMethodConfigurationCreateParamsMultibancoDisplayPreference,
        PaymentMethodConfigurationCreateParamsNaverPay as PaymentMethodConfigurationCreateParamsNaverPay,
        PaymentMethodConfigurationCreateParamsNaverPayDisplayPreference as PaymentMethodConfigurationCreateParamsNaverPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsNzBankAccount as PaymentMethodConfigurationCreateParamsNzBankAccount,
        PaymentMethodConfigurationCreateParamsNzBankAccountDisplayPreference as PaymentMethodConfigurationCreateParamsNzBankAccountDisplayPreference,
        PaymentMethodConfigurationCreateParamsOxxo as PaymentMethodConfigurationCreateParamsOxxo,
        PaymentMethodConfigurationCreateParamsOxxoDisplayPreference as PaymentMethodConfigurationCreateParamsOxxoDisplayPreference,
        PaymentMethodConfigurationCreateParamsP24 as PaymentMethodConfigurationCreateParamsP24,
        PaymentMethodConfigurationCreateParamsP24DisplayPreference as PaymentMethodConfigurationCreateParamsP24DisplayPreference,
        PaymentMethodConfigurationCreateParamsPayByBank as PaymentMethodConfigurationCreateParamsPayByBank,
        PaymentMethodConfigurationCreateParamsPayByBankDisplayPreference as PaymentMethodConfigurationCreateParamsPayByBankDisplayPreference,
        PaymentMethodConfigurationCreateParamsPayco as PaymentMethodConfigurationCreateParamsPayco,
        PaymentMethodConfigurationCreateParamsPaycoDisplayPreference as PaymentMethodConfigurationCreateParamsPaycoDisplayPreference,
        PaymentMethodConfigurationCreateParamsPaynow as PaymentMethodConfigurationCreateParamsPaynow,
        PaymentMethodConfigurationCreateParamsPaynowDisplayPreference as PaymentMethodConfigurationCreateParamsPaynowDisplayPreference,
        PaymentMethodConfigurationCreateParamsPaypal as PaymentMethodConfigurationCreateParamsPaypal,
        PaymentMethodConfigurationCreateParamsPaypalDisplayPreference as PaymentMethodConfigurationCreateParamsPaypalDisplayPreference,
        PaymentMethodConfigurationCreateParamsPayto as PaymentMethodConfigurationCreateParamsPayto,
        PaymentMethodConfigurationCreateParamsPaytoDisplayPreference as PaymentMethodConfigurationCreateParamsPaytoDisplayPreference,
        PaymentMethodConfigurationCreateParamsPix as PaymentMethodConfigurationCreateParamsPix,
        PaymentMethodConfigurationCreateParamsPixDisplayPreference as PaymentMethodConfigurationCreateParamsPixDisplayPreference,
        PaymentMethodConfigurationCreateParamsPromptpay as PaymentMethodConfigurationCreateParamsPromptpay,
        PaymentMethodConfigurationCreateParamsPromptpayDisplayPreference as PaymentMethodConfigurationCreateParamsPromptpayDisplayPreference,
        PaymentMethodConfigurationCreateParamsRevolutPay as PaymentMethodConfigurationCreateParamsRevolutPay,
        PaymentMethodConfigurationCreateParamsRevolutPayDisplayPreference as PaymentMethodConfigurationCreateParamsRevolutPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsSamsungPay as PaymentMethodConfigurationCreateParamsSamsungPay,
        PaymentMethodConfigurationCreateParamsSamsungPayDisplayPreference as PaymentMethodConfigurationCreateParamsSamsungPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsSatispay as PaymentMethodConfigurationCreateParamsSatispay,
        PaymentMethodConfigurationCreateParamsSatispayDisplayPreference as PaymentMethodConfigurationCreateParamsSatispayDisplayPreference,
        PaymentMethodConfigurationCreateParamsSepaDebit as PaymentMethodConfigurationCreateParamsSepaDebit,
        PaymentMethodConfigurationCreateParamsSepaDebitDisplayPreference as PaymentMethodConfigurationCreateParamsSepaDebitDisplayPreference,
        PaymentMethodConfigurationCreateParamsSofort as PaymentMethodConfigurationCreateParamsSofort,
        PaymentMethodConfigurationCreateParamsSofortDisplayPreference as PaymentMethodConfigurationCreateParamsSofortDisplayPreference,
        PaymentMethodConfigurationCreateParamsSwish as PaymentMethodConfigurationCreateParamsSwish,
        PaymentMethodConfigurationCreateParamsSwishDisplayPreference as PaymentMethodConfigurationCreateParamsSwishDisplayPreference,
        PaymentMethodConfigurationCreateParamsTwint as PaymentMethodConfigurationCreateParamsTwint,
        PaymentMethodConfigurationCreateParamsTwintDisplayPreference as PaymentMethodConfigurationCreateParamsTwintDisplayPreference,
        PaymentMethodConfigurationCreateParamsUsBankAccount as PaymentMethodConfigurationCreateParamsUsBankAccount,
        PaymentMethodConfigurationCreateParamsUsBankAccountDisplayPreference as PaymentMethodConfigurationCreateParamsUsBankAccountDisplayPreference,
        PaymentMethodConfigurationCreateParamsWechatPay as PaymentMethodConfigurationCreateParamsWechatPay,
        PaymentMethodConfigurationCreateParamsWechatPayDisplayPreference as PaymentMethodConfigurationCreateParamsWechatPayDisplayPreference,
        PaymentMethodConfigurationCreateParamsZip as PaymentMethodConfigurationCreateParamsZip,
        PaymentMethodConfigurationCreateParamsZipDisplayPreference as PaymentMethodConfigurationCreateParamsZipDisplayPreference,
    )
    from stripe.params._payment_method_configuration_list_params import (
        PaymentMethodConfigurationListParams as PaymentMethodConfigurationListParams,
    )
    from stripe.params._payment_method_configuration_modify_params import (
        PaymentMethodConfigurationModifyParams as PaymentMethodConfigurationModifyParams,
        PaymentMethodConfigurationModifyParamsAcssDebit as PaymentMethodConfigurationModifyParamsAcssDebit,
        PaymentMethodConfigurationModifyParamsAcssDebitDisplayPreference as PaymentMethodConfigurationModifyParamsAcssDebitDisplayPreference,
        PaymentMethodConfigurationModifyParamsAffirm as PaymentMethodConfigurationModifyParamsAffirm,
        PaymentMethodConfigurationModifyParamsAffirmDisplayPreference as PaymentMethodConfigurationModifyParamsAffirmDisplayPreference,
        PaymentMethodConfigurationModifyParamsAfterpayClearpay as PaymentMethodConfigurationModifyParamsAfterpayClearpay,
        PaymentMethodConfigurationModifyParamsAfterpayClearpayDisplayPreference as PaymentMethodConfigurationModifyParamsAfterpayClearpayDisplayPreference,
        PaymentMethodConfigurationModifyParamsAlipay as PaymentMethodConfigurationModifyParamsAlipay,
        PaymentMethodConfigurationModifyParamsAlipayDisplayPreference as PaymentMethodConfigurationModifyParamsAlipayDisplayPreference,
        PaymentMethodConfigurationModifyParamsAlma as PaymentMethodConfigurationModifyParamsAlma,
        PaymentMethodConfigurationModifyParamsAlmaDisplayPreference as PaymentMethodConfigurationModifyParamsAlmaDisplayPreference,
        PaymentMethodConfigurationModifyParamsAmazonPay as PaymentMethodConfigurationModifyParamsAmazonPay,
        PaymentMethodConfigurationModifyParamsAmazonPayDisplayPreference as PaymentMethodConfigurationModifyParamsAmazonPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsApplePay as PaymentMethodConfigurationModifyParamsApplePay,
        PaymentMethodConfigurationModifyParamsApplePayDisplayPreference as PaymentMethodConfigurationModifyParamsApplePayDisplayPreference,
        PaymentMethodConfigurationModifyParamsApplePayLater as PaymentMethodConfigurationModifyParamsApplePayLater,
        PaymentMethodConfigurationModifyParamsApplePayLaterDisplayPreference as PaymentMethodConfigurationModifyParamsApplePayLaterDisplayPreference,
        PaymentMethodConfigurationModifyParamsAuBecsDebit as PaymentMethodConfigurationModifyParamsAuBecsDebit,
        PaymentMethodConfigurationModifyParamsAuBecsDebitDisplayPreference as PaymentMethodConfigurationModifyParamsAuBecsDebitDisplayPreference,
        PaymentMethodConfigurationModifyParamsBacsDebit as PaymentMethodConfigurationModifyParamsBacsDebit,
        PaymentMethodConfigurationModifyParamsBacsDebitDisplayPreference as PaymentMethodConfigurationModifyParamsBacsDebitDisplayPreference,
        PaymentMethodConfigurationModifyParamsBancontact as PaymentMethodConfigurationModifyParamsBancontact,
        PaymentMethodConfigurationModifyParamsBancontactDisplayPreference as PaymentMethodConfigurationModifyParamsBancontactDisplayPreference,
        PaymentMethodConfigurationModifyParamsBillie as PaymentMethodConfigurationModifyParamsBillie,
        PaymentMethodConfigurationModifyParamsBillieDisplayPreference as PaymentMethodConfigurationModifyParamsBillieDisplayPreference,
        PaymentMethodConfigurationModifyParamsBlik as PaymentMethodConfigurationModifyParamsBlik,
        PaymentMethodConfigurationModifyParamsBlikDisplayPreference as PaymentMethodConfigurationModifyParamsBlikDisplayPreference,
        PaymentMethodConfigurationModifyParamsBoleto as PaymentMethodConfigurationModifyParamsBoleto,
        PaymentMethodConfigurationModifyParamsBoletoDisplayPreference as PaymentMethodConfigurationModifyParamsBoletoDisplayPreference,
        PaymentMethodConfigurationModifyParamsCard as PaymentMethodConfigurationModifyParamsCard,
        PaymentMethodConfigurationModifyParamsCardDisplayPreference as PaymentMethodConfigurationModifyParamsCardDisplayPreference,
        PaymentMethodConfigurationModifyParamsCartesBancaires as PaymentMethodConfigurationModifyParamsCartesBancaires,
        PaymentMethodConfigurationModifyParamsCartesBancairesDisplayPreference as PaymentMethodConfigurationModifyParamsCartesBancairesDisplayPreference,
        PaymentMethodConfigurationModifyParamsCashapp as PaymentMethodConfigurationModifyParamsCashapp,
        PaymentMethodConfigurationModifyParamsCashappDisplayPreference as PaymentMethodConfigurationModifyParamsCashappDisplayPreference,
        PaymentMethodConfigurationModifyParamsCrypto as PaymentMethodConfigurationModifyParamsCrypto,
        PaymentMethodConfigurationModifyParamsCryptoDisplayPreference as PaymentMethodConfigurationModifyParamsCryptoDisplayPreference,
        PaymentMethodConfigurationModifyParamsCustomerBalance as PaymentMethodConfigurationModifyParamsCustomerBalance,
        PaymentMethodConfigurationModifyParamsCustomerBalanceDisplayPreference as PaymentMethodConfigurationModifyParamsCustomerBalanceDisplayPreference,
        PaymentMethodConfigurationModifyParamsEps as PaymentMethodConfigurationModifyParamsEps,
        PaymentMethodConfigurationModifyParamsEpsDisplayPreference as PaymentMethodConfigurationModifyParamsEpsDisplayPreference,
        PaymentMethodConfigurationModifyParamsFpx as PaymentMethodConfigurationModifyParamsFpx,
        PaymentMethodConfigurationModifyParamsFpxDisplayPreference as PaymentMethodConfigurationModifyParamsFpxDisplayPreference,
        PaymentMethodConfigurationModifyParamsFrMealVoucherConecs as PaymentMethodConfigurationModifyParamsFrMealVoucherConecs,
        PaymentMethodConfigurationModifyParamsFrMealVoucherConecsDisplayPreference as PaymentMethodConfigurationModifyParamsFrMealVoucherConecsDisplayPreference,
        PaymentMethodConfigurationModifyParamsGiropay as PaymentMethodConfigurationModifyParamsGiropay,
        PaymentMethodConfigurationModifyParamsGiropayDisplayPreference as PaymentMethodConfigurationModifyParamsGiropayDisplayPreference,
        PaymentMethodConfigurationModifyParamsGooglePay as PaymentMethodConfigurationModifyParamsGooglePay,
        PaymentMethodConfigurationModifyParamsGooglePayDisplayPreference as PaymentMethodConfigurationModifyParamsGooglePayDisplayPreference,
        PaymentMethodConfigurationModifyParamsGrabpay as PaymentMethodConfigurationModifyParamsGrabpay,
        PaymentMethodConfigurationModifyParamsGrabpayDisplayPreference as PaymentMethodConfigurationModifyParamsGrabpayDisplayPreference,
        PaymentMethodConfigurationModifyParamsIdeal as PaymentMethodConfigurationModifyParamsIdeal,
        PaymentMethodConfigurationModifyParamsIdealDisplayPreference as PaymentMethodConfigurationModifyParamsIdealDisplayPreference,
        PaymentMethodConfigurationModifyParamsJcb as PaymentMethodConfigurationModifyParamsJcb,
        PaymentMethodConfigurationModifyParamsJcbDisplayPreference as PaymentMethodConfigurationModifyParamsJcbDisplayPreference,
        PaymentMethodConfigurationModifyParamsKakaoPay as PaymentMethodConfigurationModifyParamsKakaoPay,
        PaymentMethodConfigurationModifyParamsKakaoPayDisplayPreference as PaymentMethodConfigurationModifyParamsKakaoPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsKlarna as PaymentMethodConfigurationModifyParamsKlarna,
        PaymentMethodConfigurationModifyParamsKlarnaDisplayPreference as PaymentMethodConfigurationModifyParamsKlarnaDisplayPreference,
        PaymentMethodConfigurationModifyParamsKonbini as PaymentMethodConfigurationModifyParamsKonbini,
        PaymentMethodConfigurationModifyParamsKonbiniDisplayPreference as PaymentMethodConfigurationModifyParamsKonbiniDisplayPreference,
        PaymentMethodConfigurationModifyParamsKrCard as PaymentMethodConfigurationModifyParamsKrCard,
        PaymentMethodConfigurationModifyParamsKrCardDisplayPreference as PaymentMethodConfigurationModifyParamsKrCardDisplayPreference,
        PaymentMethodConfigurationModifyParamsLink as PaymentMethodConfigurationModifyParamsLink,
        PaymentMethodConfigurationModifyParamsLinkDisplayPreference as PaymentMethodConfigurationModifyParamsLinkDisplayPreference,
        PaymentMethodConfigurationModifyParamsMbWay as PaymentMethodConfigurationModifyParamsMbWay,
        PaymentMethodConfigurationModifyParamsMbWayDisplayPreference as PaymentMethodConfigurationModifyParamsMbWayDisplayPreference,
        PaymentMethodConfigurationModifyParamsMobilepay as PaymentMethodConfigurationModifyParamsMobilepay,
        PaymentMethodConfigurationModifyParamsMobilepayDisplayPreference as PaymentMethodConfigurationModifyParamsMobilepayDisplayPreference,
        PaymentMethodConfigurationModifyParamsMultibanco as PaymentMethodConfigurationModifyParamsMultibanco,
        PaymentMethodConfigurationModifyParamsMultibancoDisplayPreference as PaymentMethodConfigurationModifyParamsMultibancoDisplayPreference,
        PaymentMethodConfigurationModifyParamsNaverPay as PaymentMethodConfigurationModifyParamsNaverPay,
        PaymentMethodConfigurationModifyParamsNaverPayDisplayPreference as PaymentMethodConfigurationModifyParamsNaverPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsNzBankAccount as PaymentMethodConfigurationModifyParamsNzBankAccount,
        PaymentMethodConfigurationModifyParamsNzBankAccountDisplayPreference as PaymentMethodConfigurationModifyParamsNzBankAccountDisplayPreference,
        PaymentMethodConfigurationModifyParamsOxxo as PaymentMethodConfigurationModifyParamsOxxo,
        PaymentMethodConfigurationModifyParamsOxxoDisplayPreference as PaymentMethodConfigurationModifyParamsOxxoDisplayPreference,
        PaymentMethodConfigurationModifyParamsP24 as PaymentMethodConfigurationModifyParamsP24,
        PaymentMethodConfigurationModifyParamsP24DisplayPreference as PaymentMethodConfigurationModifyParamsP24DisplayPreference,
        PaymentMethodConfigurationModifyParamsPayByBank as PaymentMethodConfigurationModifyParamsPayByBank,
        PaymentMethodConfigurationModifyParamsPayByBankDisplayPreference as PaymentMethodConfigurationModifyParamsPayByBankDisplayPreference,
        PaymentMethodConfigurationModifyParamsPayco as PaymentMethodConfigurationModifyParamsPayco,
        PaymentMethodConfigurationModifyParamsPaycoDisplayPreference as PaymentMethodConfigurationModifyParamsPaycoDisplayPreference,
        PaymentMethodConfigurationModifyParamsPaynow as PaymentMethodConfigurationModifyParamsPaynow,
        PaymentMethodConfigurationModifyParamsPaynowDisplayPreference as PaymentMethodConfigurationModifyParamsPaynowDisplayPreference,
        PaymentMethodConfigurationModifyParamsPaypal as PaymentMethodConfigurationModifyParamsPaypal,
        PaymentMethodConfigurationModifyParamsPaypalDisplayPreference as PaymentMethodConfigurationModifyParamsPaypalDisplayPreference,
        PaymentMethodConfigurationModifyParamsPayto as PaymentMethodConfigurationModifyParamsPayto,
        PaymentMethodConfigurationModifyParamsPaytoDisplayPreference as PaymentMethodConfigurationModifyParamsPaytoDisplayPreference,
        PaymentMethodConfigurationModifyParamsPix as PaymentMethodConfigurationModifyParamsPix,
        PaymentMethodConfigurationModifyParamsPixDisplayPreference as PaymentMethodConfigurationModifyParamsPixDisplayPreference,
        PaymentMethodConfigurationModifyParamsPromptpay as PaymentMethodConfigurationModifyParamsPromptpay,
        PaymentMethodConfigurationModifyParamsPromptpayDisplayPreference as PaymentMethodConfigurationModifyParamsPromptpayDisplayPreference,
        PaymentMethodConfigurationModifyParamsRevolutPay as PaymentMethodConfigurationModifyParamsRevolutPay,
        PaymentMethodConfigurationModifyParamsRevolutPayDisplayPreference as PaymentMethodConfigurationModifyParamsRevolutPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsSamsungPay as PaymentMethodConfigurationModifyParamsSamsungPay,
        PaymentMethodConfigurationModifyParamsSamsungPayDisplayPreference as PaymentMethodConfigurationModifyParamsSamsungPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsSatispay as PaymentMethodConfigurationModifyParamsSatispay,
        PaymentMethodConfigurationModifyParamsSatispayDisplayPreference as PaymentMethodConfigurationModifyParamsSatispayDisplayPreference,
        PaymentMethodConfigurationModifyParamsSepaDebit as PaymentMethodConfigurationModifyParamsSepaDebit,
        PaymentMethodConfigurationModifyParamsSepaDebitDisplayPreference as PaymentMethodConfigurationModifyParamsSepaDebitDisplayPreference,
        PaymentMethodConfigurationModifyParamsSofort as PaymentMethodConfigurationModifyParamsSofort,
        PaymentMethodConfigurationModifyParamsSofortDisplayPreference as PaymentMethodConfigurationModifyParamsSofortDisplayPreference,
        PaymentMethodConfigurationModifyParamsSwish as PaymentMethodConfigurationModifyParamsSwish,
        PaymentMethodConfigurationModifyParamsSwishDisplayPreference as PaymentMethodConfigurationModifyParamsSwishDisplayPreference,
        PaymentMethodConfigurationModifyParamsTwint as PaymentMethodConfigurationModifyParamsTwint,
        PaymentMethodConfigurationModifyParamsTwintDisplayPreference as PaymentMethodConfigurationModifyParamsTwintDisplayPreference,
        PaymentMethodConfigurationModifyParamsUsBankAccount as PaymentMethodConfigurationModifyParamsUsBankAccount,
        PaymentMethodConfigurationModifyParamsUsBankAccountDisplayPreference as PaymentMethodConfigurationModifyParamsUsBankAccountDisplayPreference,
        PaymentMethodConfigurationModifyParamsWechatPay as PaymentMethodConfigurationModifyParamsWechatPay,
        PaymentMethodConfigurationModifyParamsWechatPayDisplayPreference as PaymentMethodConfigurationModifyParamsWechatPayDisplayPreference,
        PaymentMethodConfigurationModifyParamsZip as PaymentMethodConfigurationModifyParamsZip,
        PaymentMethodConfigurationModifyParamsZipDisplayPreference as PaymentMethodConfigurationModifyParamsZipDisplayPreference,
    )
    from stripe.params._payment_method_configuration_retrieve_params import (
        PaymentMethodConfigurationRetrieveParams as PaymentMethodConfigurationRetrieveParams,
    )
    from stripe.params._payment_method_configuration_update_params import (
        PaymentMethodConfigurationUpdateParams as PaymentMethodConfigurationUpdateParams,
        PaymentMethodConfigurationUpdateParamsAcssDebit as PaymentMethodConfigurationUpdateParamsAcssDebit,
        PaymentMethodConfigurationUpdateParamsAcssDebitDisplayPreference as PaymentMethodConfigurationUpdateParamsAcssDebitDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAffirm as PaymentMethodConfigurationUpdateParamsAffirm,
        PaymentMethodConfigurationUpdateParamsAffirmDisplayPreference as PaymentMethodConfigurationUpdateParamsAffirmDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAfterpayClearpay as PaymentMethodConfigurationUpdateParamsAfterpayClearpay,
        PaymentMethodConfigurationUpdateParamsAfterpayClearpayDisplayPreference as PaymentMethodConfigurationUpdateParamsAfterpayClearpayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAlipay as PaymentMethodConfigurationUpdateParamsAlipay,
        PaymentMethodConfigurationUpdateParamsAlipayDisplayPreference as PaymentMethodConfigurationUpdateParamsAlipayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAlma as PaymentMethodConfigurationUpdateParamsAlma,
        PaymentMethodConfigurationUpdateParamsAlmaDisplayPreference as PaymentMethodConfigurationUpdateParamsAlmaDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAmazonPay as PaymentMethodConfigurationUpdateParamsAmazonPay,
        PaymentMethodConfigurationUpdateParamsAmazonPayDisplayPreference as PaymentMethodConfigurationUpdateParamsAmazonPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsApplePay as PaymentMethodConfigurationUpdateParamsApplePay,
        PaymentMethodConfigurationUpdateParamsApplePayDisplayPreference as PaymentMethodConfigurationUpdateParamsApplePayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsApplePayLater as PaymentMethodConfigurationUpdateParamsApplePayLater,
        PaymentMethodConfigurationUpdateParamsApplePayLaterDisplayPreference as PaymentMethodConfigurationUpdateParamsApplePayLaterDisplayPreference,
        PaymentMethodConfigurationUpdateParamsAuBecsDebit as PaymentMethodConfigurationUpdateParamsAuBecsDebit,
        PaymentMethodConfigurationUpdateParamsAuBecsDebitDisplayPreference as PaymentMethodConfigurationUpdateParamsAuBecsDebitDisplayPreference,
        PaymentMethodConfigurationUpdateParamsBacsDebit as PaymentMethodConfigurationUpdateParamsBacsDebit,
        PaymentMethodConfigurationUpdateParamsBacsDebitDisplayPreference as PaymentMethodConfigurationUpdateParamsBacsDebitDisplayPreference,
        PaymentMethodConfigurationUpdateParamsBancontact as PaymentMethodConfigurationUpdateParamsBancontact,
        PaymentMethodConfigurationUpdateParamsBancontactDisplayPreference as PaymentMethodConfigurationUpdateParamsBancontactDisplayPreference,
        PaymentMethodConfigurationUpdateParamsBillie as PaymentMethodConfigurationUpdateParamsBillie,
        PaymentMethodConfigurationUpdateParamsBillieDisplayPreference as PaymentMethodConfigurationUpdateParamsBillieDisplayPreference,
        PaymentMethodConfigurationUpdateParamsBlik as PaymentMethodConfigurationUpdateParamsBlik,
        PaymentMethodConfigurationUpdateParamsBlikDisplayPreference as PaymentMethodConfigurationUpdateParamsBlikDisplayPreference,
        PaymentMethodConfigurationUpdateParamsBoleto as PaymentMethodConfigurationUpdateParamsBoleto,
        PaymentMethodConfigurationUpdateParamsBoletoDisplayPreference as PaymentMethodConfigurationUpdateParamsBoletoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsCard as PaymentMethodConfigurationUpdateParamsCard,
        PaymentMethodConfigurationUpdateParamsCardDisplayPreference as PaymentMethodConfigurationUpdateParamsCardDisplayPreference,
        PaymentMethodConfigurationUpdateParamsCartesBancaires as PaymentMethodConfigurationUpdateParamsCartesBancaires,
        PaymentMethodConfigurationUpdateParamsCartesBancairesDisplayPreference as PaymentMethodConfigurationUpdateParamsCartesBancairesDisplayPreference,
        PaymentMethodConfigurationUpdateParamsCashapp as PaymentMethodConfigurationUpdateParamsCashapp,
        PaymentMethodConfigurationUpdateParamsCashappDisplayPreference as PaymentMethodConfigurationUpdateParamsCashappDisplayPreference,
        PaymentMethodConfigurationUpdateParamsCrypto as PaymentMethodConfigurationUpdateParamsCrypto,
        PaymentMethodConfigurationUpdateParamsCryptoDisplayPreference as PaymentMethodConfigurationUpdateParamsCryptoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsCustomerBalance as PaymentMethodConfigurationUpdateParamsCustomerBalance,
        PaymentMethodConfigurationUpdateParamsCustomerBalanceDisplayPreference as PaymentMethodConfigurationUpdateParamsCustomerBalanceDisplayPreference,
        PaymentMethodConfigurationUpdateParamsEps as PaymentMethodConfigurationUpdateParamsEps,
        PaymentMethodConfigurationUpdateParamsEpsDisplayPreference as PaymentMethodConfigurationUpdateParamsEpsDisplayPreference,
        PaymentMethodConfigurationUpdateParamsFpx as PaymentMethodConfigurationUpdateParamsFpx,
        PaymentMethodConfigurationUpdateParamsFpxDisplayPreference as PaymentMethodConfigurationUpdateParamsFpxDisplayPreference,
        PaymentMethodConfigurationUpdateParamsFrMealVoucherConecs as PaymentMethodConfigurationUpdateParamsFrMealVoucherConecs,
        PaymentMethodConfigurationUpdateParamsFrMealVoucherConecsDisplayPreference as PaymentMethodConfigurationUpdateParamsFrMealVoucherConecsDisplayPreference,
        PaymentMethodConfigurationUpdateParamsGiropay as PaymentMethodConfigurationUpdateParamsGiropay,
        PaymentMethodConfigurationUpdateParamsGiropayDisplayPreference as PaymentMethodConfigurationUpdateParamsGiropayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsGooglePay as PaymentMethodConfigurationUpdateParamsGooglePay,
        PaymentMethodConfigurationUpdateParamsGooglePayDisplayPreference as PaymentMethodConfigurationUpdateParamsGooglePayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsGrabpay as PaymentMethodConfigurationUpdateParamsGrabpay,
        PaymentMethodConfigurationUpdateParamsGrabpayDisplayPreference as PaymentMethodConfigurationUpdateParamsGrabpayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsIdeal as PaymentMethodConfigurationUpdateParamsIdeal,
        PaymentMethodConfigurationUpdateParamsIdealDisplayPreference as PaymentMethodConfigurationUpdateParamsIdealDisplayPreference,
        PaymentMethodConfigurationUpdateParamsJcb as PaymentMethodConfigurationUpdateParamsJcb,
        PaymentMethodConfigurationUpdateParamsJcbDisplayPreference as PaymentMethodConfigurationUpdateParamsJcbDisplayPreference,
        PaymentMethodConfigurationUpdateParamsKakaoPay as PaymentMethodConfigurationUpdateParamsKakaoPay,
        PaymentMethodConfigurationUpdateParamsKakaoPayDisplayPreference as PaymentMethodConfigurationUpdateParamsKakaoPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsKlarna as PaymentMethodConfigurationUpdateParamsKlarna,
        PaymentMethodConfigurationUpdateParamsKlarnaDisplayPreference as PaymentMethodConfigurationUpdateParamsKlarnaDisplayPreference,
        PaymentMethodConfigurationUpdateParamsKonbini as PaymentMethodConfigurationUpdateParamsKonbini,
        PaymentMethodConfigurationUpdateParamsKonbiniDisplayPreference as PaymentMethodConfigurationUpdateParamsKonbiniDisplayPreference,
        PaymentMethodConfigurationUpdateParamsKrCard as PaymentMethodConfigurationUpdateParamsKrCard,
        PaymentMethodConfigurationUpdateParamsKrCardDisplayPreference as PaymentMethodConfigurationUpdateParamsKrCardDisplayPreference,
        PaymentMethodConfigurationUpdateParamsLink as PaymentMethodConfigurationUpdateParamsLink,
        PaymentMethodConfigurationUpdateParamsLinkDisplayPreference as PaymentMethodConfigurationUpdateParamsLinkDisplayPreference,
        PaymentMethodConfigurationUpdateParamsMbWay as PaymentMethodConfigurationUpdateParamsMbWay,
        PaymentMethodConfigurationUpdateParamsMbWayDisplayPreference as PaymentMethodConfigurationUpdateParamsMbWayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsMobilepay as PaymentMethodConfigurationUpdateParamsMobilepay,
        PaymentMethodConfigurationUpdateParamsMobilepayDisplayPreference as PaymentMethodConfigurationUpdateParamsMobilepayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsMultibanco as PaymentMethodConfigurationUpdateParamsMultibanco,
        PaymentMethodConfigurationUpdateParamsMultibancoDisplayPreference as PaymentMethodConfigurationUpdateParamsMultibancoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsNaverPay as PaymentMethodConfigurationUpdateParamsNaverPay,
        PaymentMethodConfigurationUpdateParamsNaverPayDisplayPreference as PaymentMethodConfigurationUpdateParamsNaverPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsNzBankAccount as PaymentMethodConfigurationUpdateParamsNzBankAccount,
        PaymentMethodConfigurationUpdateParamsNzBankAccountDisplayPreference as PaymentMethodConfigurationUpdateParamsNzBankAccountDisplayPreference,
        PaymentMethodConfigurationUpdateParamsOxxo as PaymentMethodConfigurationUpdateParamsOxxo,
        PaymentMethodConfigurationUpdateParamsOxxoDisplayPreference as PaymentMethodConfigurationUpdateParamsOxxoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsP24 as PaymentMethodConfigurationUpdateParamsP24,
        PaymentMethodConfigurationUpdateParamsP24DisplayPreference as PaymentMethodConfigurationUpdateParamsP24DisplayPreference,
        PaymentMethodConfigurationUpdateParamsPayByBank as PaymentMethodConfigurationUpdateParamsPayByBank,
        PaymentMethodConfigurationUpdateParamsPayByBankDisplayPreference as PaymentMethodConfigurationUpdateParamsPayByBankDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPayco as PaymentMethodConfigurationUpdateParamsPayco,
        PaymentMethodConfigurationUpdateParamsPaycoDisplayPreference as PaymentMethodConfigurationUpdateParamsPaycoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPaynow as PaymentMethodConfigurationUpdateParamsPaynow,
        PaymentMethodConfigurationUpdateParamsPaynowDisplayPreference as PaymentMethodConfigurationUpdateParamsPaynowDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPaypal as PaymentMethodConfigurationUpdateParamsPaypal,
        PaymentMethodConfigurationUpdateParamsPaypalDisplayPreference as PaymentMethodConfigurationUpdateParamsPaypalDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPayto as PaymentMethodConfigurationUpdateParamsPayto,
        PaymentMethodConfigurationUpdateParamsPaytoDisplayPreference as PaymentMethodConfigurationUpdateParamsPaytoDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPix as PaymentMethodConfigurationUpdateParamsPix,
        PaymentMethodConfigurationUpdateParamsPixDisplayPreference as PaymentMethodConfigurationUpdateParamsPixDisplayPreference,
        PaymentMethodConfigurationUpdateParamsPromptpay as PaymentMethodConfigurationUpdateParamsPromptpay,
        PaymentMethodConfigurationUpdateParamsPromptpayDisplayPreference as PaymentMethodConfigurationUpdateParamsPromptpayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsRevolutPay as PaymentMethodConfigurationUpdateParamsRevolutPay,
        PaymentMethodConfigurationUpdateParamsRevolutPayDisplayPreference as PaymentMethodConfigurationUpdateParamsRevolutPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsSamsungPay as PaymentMethodConfigurationUpdateParamsSamsungPay,
        PaymentMethodConfigurationUpdateParamsSamsungPayDisplayPreference as PaymentMethodConfigurationUpdateParamsSamsungPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsSatispay as PaymentMethodConfigurationUpdateParamsSatispay,
        PaymentMethodConfigurationUpdateParamsSatispayDisplayPreference as PaymentMethodConfigurationUpdateParamsSatispayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsSepaDebit as PaymentMethodConfigurationUpdateParamsSepaDebit,
        PaymentMethodConfigurationUpdateParamsSepaDebitDisplayPreference as PaymentMethodConfigurationUpdateParamsSepaDebitDisplayPreference,
        PaymentMethodConfigurationUpdateParamsSofort as PaymentMethodConfigurationUpdateParamsSofort,
        PaymentMethodConfigurationUpdateParamsSofortDisplayPreference as PaymentMethodConfigurationUpdateParamsSofortDisplayPreference,
        PaymentMethodConfigurationUpdateParamsSwish as PaymentMethodConfigurationUpdateParamsSwish,
        PaymentMethodConfigurationUpdateParamsSwishDisplayPreference as PaymentMethodConfigurationUpdateParamsSwishDisplayPreference,
        PaymentMethodConfigurationUpdateParamsTwint as PaymentMethodConfigurationUpdateParamsTwint,
        PaymentMethodConfigurationUpdateParamsTwintDisplayPreference as PaymentMethodConfigurationUpdateParamsTwintDisplayPreference,
        PaymentMethodConfigurationUpdateParamsUsBankAccount as PaymentMethodConfigurationUpdateParamsUsBankAccount,
        PaymentMethodConfigurationUpdateParamsUsBankAccountDisplayPreference as PaymentMethodConfigurationUpdateParamsUsBankAccountDisplayPreference,
        PaymentMethodConfigurationUpdateParamsWechatPay as PaymentMethodConfigurationUpdateParamsWechatPay,
        PaymentMethodConfigurationUpdateParamsWechatPayDisplayPreference as PaymentMethodConfigurationUpdateParamsWechatPayDisplayPreference,
        PaymentMethodConfigurationUpdateParamsZip as PaymentMethodConfigurationUpdateParamsZip,
        PaymentMethodConfigurationUpdateParamsZipDisplayPreference as PaymentMethodConfigurationUpdateParamsZipDisplayPreference,
    )
    from stripe.params._payment_method_create_params import (
        PaymentMethodCreateParams as PaymentMethodCreateParams,
        PaymentMethodCreateParamsAcssDebit as PaymentMethodCreateParamsAcssDebit,
        PaymentMethodCreateParamsAffirm as PaymentMethodCreateParamsAffirm,
        PaymentMethodCreateParamsAfterpayClearpay as PaymentMethodCreateParamsAfterpayClearpay,
        PaymentMethodCreateParamsAlipay as PaymentMethodCreateParamsAlipay,
        PaymentMethodCreateParamsAlma as PaymentMethodCreateParamsAlma,
        PaymentMethodCreateParamsAmazonPay as PaymentMethodCreateParamsAmazonPay,
        PaymentMethodCreateParamsAuBecsDebit as PaymentMethodCreateParamsAuBecsDebit,
        PaymentMethodCreateParamsBacsDebit as PaymentMethodCreateParamsBacsDebit,
        PaymentMethodCreateParamsBancontact as PaymentMethodCreateParamsBancontact,
        PaymentMethodCreateParamsBillie as PaymentMethodCreateParamsBillie,
        PaymentMethodCreateParamsBillingDetails as PaymentMethodCreateParamsBillingDetails,
        PaymentMethodCreateParamsBillingDetailsAddress as PaymentMethodCreateParamsBillingDetailsAddress,
        PaymentMethodCreateParamsBlik as PaymentMethodCreateParamsBlik,
        PaymentMethodCreateParamsBoleto as PaymentMethodCreateParamsBoleto,
        PaymentMethodCreateParamsCard as PaymentMethodCreateParamsCard,
        PaymentMethodCreateParamsCardNetworks as PaymentMethodCreateParamsCardNetworks,
        PaymentMethodCreateParamsCashapp as PaymentMethodCreateParamsCashapp,
        PaymentMethodCreateParamsCrypto as PaymentMethodCreateParamsCrypto,
        PaymentMethodCreateParamsCustom as PaymentMethodCreateParamsCustom,
        PaymentMethodCreateParamsCustomerBalance as PaymentMethodCreateParamsCustomerBalance,
        PaymentMethodCreateParamsEps as PaymentMethodCreateParamsEps,
        PaymentMethodCreateParamsFpx as PaymentMethodCreateParamsFpx,
        PaymentMethodCreateParamsGiropay as PaymentMethodCreateParamsGiropay,
        PaymentMethodCreateParamsGrabpay as PaymentMethodCreateParamsGrabpay,
        PaymentMethodCreateParamsIdeal as PaymentMethodCreateParamsIdeal,
        PaymentMethodCreateParamsInteracPresent as PaymentMethodCreateParamsInteracPresent,
        PaymentMethodCreateParamsKakaoPay as PaymentMethodCreateParamsKakaoPay,
        PaymentMethodCreateParamsKlarna as PaymentMethodCreateParamsKlarna,
        PaymentMethodCreateParamsKlarnaDob as PaymentMethodCreateParamsKlarnaDob,
        PaymentMethodCreateParamsKonbini as PaymentMethodCreateParamsKonbini,
        PaymentMethodCreateParamsKrCard as PaymentMethodCreateParamsKrCard,
        PaymentMethodCreateParamsLink as PaymentMethodCreateParamsLink,
        PaymentMethodCreateParamsMbWay as PaymentMethodCreateParamsMbWay,
        PaymentMethodCreateParamsMobilepay as PaymentMethodCreateParamsMobilepay,
        PaymentMethodCreateParamsMultibanco as PaymentMethodCreateParamsMultibanco,
        PaymentMethodCreateParamsNaverPay as PaymentMethodCreateParamsNaverPay,
        PaymentMethodCreateParamsNzBankAccount as PaymentMethodCreateParamsNzBankAccount,
        PaymentMethodCreateParamsOxxo as PaymentMethodCreateParamsOxxo,
        PaymentMethodCreateParamsP24 as PaymentMethodCreateParamsP24,
        PaymentMethodCreateParamsPayByBank as PaymentMethodCreateParamsPayByBank,
        PaymentMethodCreateParamsPayco as PaymentMethodCreateParamsPayco,
        PaymentMethodCreateParamsPaynow as PaymentMethodCreateParamsPaynow,
        PaymentMethodCreateParamsPaypal as PaymentMethodCreateParamsPaypal,
        PaymentMethodCreateParamsPayto as PaymentMethodCreateParamsPayto,
        PaymentMethodCreateParamsPix as PaymentMethodCreateParamsPix,
        PaymentMethodCreateParamsPromptpay as PaymentMethodCreateParamsPromptpay,
        PaymentMethodCreateParamsRadarOptions as PaymentMethodCreateParamsRadarOptions,
        PaymentMethodCreateParamsRevolutPay as PaymentMethodCreateParamsRevolutPay,
        PaymentMethodCreateParamsSamsungPay as PaymentMethodCreateParamsSamsungPay,
        PaymentMethodCreateParamsSatispay as PaymentMethodCreateParamsSatispay,
        PaymentMethodCreateParamsSepaDebit as PaymentMethodCreateParamsSepaDebit,
        PaymentMethodCreateParamsSofort as PaymentMethodCreateParamsSofort,
        PaymentMethodCreateParamsSwish as PaymentMethodCreateParamsSwish,
        PaymentMethodCreateParamsTwint as PaymentMethodCreateParamsTwint,
        PaymentMethodCreateParamsUsBankAccount as PaymentMethodCreateParamsUsBankAccount,
        PaymentMethodCreateParamsWechatPay as PaymentMethodCreateParamsWechatPay,
        PaymentMethodCreateParamsZip as PaymentMethodCreateParamsZip,
    )
    from stripe.params._payment_method_detach_params import (
        PaymentMethodDetachParams as PaymentMethodDetachParams,
    )
    from stripe.params._payment_method_domain_create_params import (
        PaymentMethodDomainCreateParams as PaymentMethodDomainCreateParams,
    )
    from stripe.params._payment_method_domain_list_params import (
        PaymentMethodDomainListParams as PaymentMethodDomainListParams,
    )
    from stripe.params._payment_method_domain_modify_params import (
        PaymentMethodDomainModifyParams as PaymentMethodDomainModifyParams,
    )
    from stripe.params._payment_method_domain_retrieve_params import (
        PaymentMethodDomainRetrieveParams as PaymentMethodDomainRetrieveParams,
    )
    from stripe.params._payment_method_domain_update_params import (
        PaymentMethodDomainUpdateParams as PaymentMethodDomainUpdateParams,
    )
    from stripe.params._payment_method_domain_validate_params import (
        PaymentMethodDomainValidateParams as PaymentMethodDomainValidateParams,
    )
    from stripe.params._payment_method_list_params import (
        PaymentMethodListParams as PaymentMethodListParams,
    )
    from stripe.params._payment_method_modify_params import (
        PaymentMethodModifyParams as PaymentMethodModifyParams,
        PaymentMethodModifyParamsBillingDetails as PaymentMethodModifyParamsBillingDetails,
        PaymentMethodModifyParamsBillingDetailsAddress as PaymentMethodModifyParamsBillingDetailsAddress,
        PaymentMethodModifyParamsCard as PaymentMethodModifyParamsCard,
        PaymentMethodModifyParamsCardNetworks as PaymentMethodModifyParamsCardNetworks,
        PaymentMethodModifyParamsPayto as PaymentMethodModifyParamsPayto,
        PaymentMethodModifyParamsUsBankAccount as PaymentMethodModifyParamsUsBankAccount,
    )
    from stripe.params._payment_method_retrieve_params import (
        PaymentMethodRetrieveParams as PaymentMethodRetrieveParams,
    )
    from stripe.params._payment_method_update_params import (
        PaymentMethodUpdateParams as PaymentMethodUpdateParams,
        PaymentMethodUpdateParamsBillingDetails as PaymentMethodUpdateParamsBillingDetails,
        PaymentMethodUpdateParamsBillingDetailsAddress as PaymentMethodUpdateParamsBillingDetailsAddress,
        PaymentMethodUpdateParamsCard as PaymentMethodUpdateParamsCard,
        PaymentMethodUpdateParamsCardNetworks as PaymentMethodUpdateParamsCardNetworks,
        PaymentMethodUpdateParamsPayto as PaymentMethodUpdateParamsPayto,
        PaymentMethodUpdateParamsUsBankAccount as PaymentMethodUpdateParamsUsBankAccount,
    )
    from stripe.params._payment_record_report_payment_attempt_canceled_params import (
        PaymentRecordReportPaymentAttemptCanceledParams as PaymentRecordReportPaymentAttemptCanceledParams,
    )
    from stripe.params._payment_record_report_payment_attempt_failed_params import (
        PaymentRecordReportPaymentAttemptFailedParams as PaymentRecordReportPaymentAttemptFailedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_guaranteed_params import (
        PaymentRecordReportPaymentAttemptGuaranteedParams as PaymentRecordReportPaymentAttemptGuaranteedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_informational_params import (
        PaymentRecordReportPaymentAttemptInformationalParams as PaymentRecordReportPaymentAttemptInformationalParams,
        PaymentRecordReportPaymentAttemptInformationalParamsCustomerDetails as PaymentRecordReportPaymentAttemptInformationalParamsCustomerDetails,
        PaymentRecordReportPaymentAttemptInformationalParamsShippingDetails as PaymentRecordReportPaymentAttemptInformationalParamsShippingDetails,
        PaymentRecordReportPaymentAttemptInformationalParamsShippingDetailsAddress as PaymentRecordReportPaymentAttemptInformationalParamsShippingDetailsAddress,
    )
    from stripe.params._payment_record_report_payment_attempt_params import (
        PaymentRecordReportPaymentAttemptParams as PaymentRecordReportPaymentAttemptParams,
        PaymentRecordReportPaymentAttemptParamsFailed as PaymentRecordReportPaymentAttemptParamsFailed,
        PaymentRecordReportPaymentAttemptParamsGuaranteed as PaymentRecordReportPaymentAttemptParamsGuaranteed,
        PaymentRecordReportPaymentAttemptParamsPaymentMethodDetails as PaymentRecordReportPaymentAttemptParamsPaymentMethodDetails,
        PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetails as PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetails,
        PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetailsAddress as PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetailsAddress,
        PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsCustom as PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsCustom,
        PaymentRecordReportPaymentAttemptParamsShippingDetails as PaymentRecordReportPaymentAttemptParamsShippingDetails,
        PaymentRecordReportPaymentAttemptParamsShippingDetailsAddress as PaymentRecordReportPaymentAttemptParamsShippingDetailsAddress,
    )
    from stripe.params._payment_record_report_payment_params import (
        PaymentRecordReportPaymentParams as PaymentRecordReportPaymentParams,
        PaymentRecordReportPaymentParamsAmountRequested as PaymentRecordReportPaymentParamsAmountRequested,
        PaymentRecordReportPaymentParamsCustomerDetails as PaymentRecordReportPaymentParamsCustomerDetails,
        PaymentRecordReportPaymentParamsFailed as PaymentRecordReportPaymentParamsFailed,
        PaymentRecordReportPaymentParamsGuaranteed as PaymentRecordReportPaymentParamsGuaranteed,
        PaymentRecordReportPaymentParamsPaymentMethodDetails as PaymentRecordReportPaymentParamsPaymentMethodDetails,
        PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetails as PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetails,
        PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetailsAddress as PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetailsAddress,
        PaymentRecordReportPaymentParamsPaymentMethodDetailsCustom as PaymentRecordReportPaymentParamsPaymentMethodDetailsCustom,
        PaymentRecordReportPaymentParamsProcessorDetails as PaymentRecordReportPaymentParamsProcessorDetails,
        PaymentRecordReportPaymentParamsProcessorDetailsCustom as PaymentRecordReportPaymentParamsProcessorDetailsCustom,
        PaymentRecordReportPaymentParamsShippingDetails as PaymentRecordReportPaymentParamsShippingDetails,
        PaymentRecordReportPaymentParamsShippingDetailsAddress as PaymentRecordReportPaymentParamsShippingDetailsAddress,
    )
    from stripe.params._payment_record_report_refund_params import (
        PaymentRecordReportRefundParams as PaymentRecordReportRefundParams,
        PaymentRecordReportRefundParamsAmount as PaymentRecordReportRefundParamsAmount,
        PaymentRecordReportRefundParamsProcessorDetails as PaymentRecordReportRefundParamsProcessorDetails,
        PaymentRecordReportRefundParamsProcessorDetailsCustom as PaymentRecordReportRefundParamsProcessorDetailsCustom,
        PaymentRecordReportRefundParamsRefunded as PaymentRecordReportRefundParamsRefunded,
    )
    from stripe.params._payment_record_retrieve_params import (
        PaymentRecordRetrieveParams as PaymentRecordRetrieveParams,
    )
    from stripe.params._payout_cancel_params import (
        PayoutCancelParams as PayoutCancelParams,
    )
    from stripe.params._payout_create_params import (
        PayoutCreateParams as PayoutCreateParams,
    )
    from stripe.params._payout_list_params import (
        PayoutListParams as PayoutListParams,
        PayoutListParamsArrivalDate as PayoutListParamsArrivalDate,
        PayoutListParamsCreated as PayoutListParamsCreated,
    )
    from stripe.params._payout_modify_params import (
        PayoutModifyParams as PayoutModifyParams,
    )
    from stripe.params._payout_retrieve_params import (
        PayoutRetrieveParams as PayoutRetrieveParams,
    )
    from stripe.params._payout_reverse_params import (
        PayoutReverseParams as PayoutReverseParams,
    )
    from stripe.params._payout_update_params import (
        PayoutUpdateParams as PayoutUpdateParams,
    )
    from stripe.params._plan_create_params import (
        PlanCreateParams as PlanCreateParams,
        PlanCreateParamsProduct as PlanCreateParamsProduct,
        PlanCreateParamsTier as PlanCreateParamsTier,
        PlanCreateParamsTransformUsage as PlanCreateParamsTransformUsage,
    )
    from stripe.params._plan_delete_params import (
        PlanDeleteParams as PlanDeleteParams,
    )
    from stripe.params._plan_list_params import (
        PlanListParams as PlanListParams,
        PlanListParamsCreated as PlanListParamsCreated,
    )
    from stripe.params._plan_modify_params import (
        PlanModifyParams as PlanModifyParams,
    )
    from stripe.params._plan_retrieve_params import (
        PlanRetrieveParams as PlanRetrieveParams,
    )
    from stripe.params._plan_update_params import (
        PlanUpdateParams as PlanUpdateParams,
    )
    from stripe.params._price_create_params import (
        PriceCreateParams as PriceCreateParams,
        PriceCreateParamsCurrencyOptions as PriceCreateParamsCurrencyOptions,
        PriceCreateParamsCurrencyOptionsCustomUnitAmount as PriceCreateParamsCurrencyOptionsCustomUnitAmount,
        PriceCreateParamsCurrencyOptionsTier as PriceCreateParamsCurrencyOptionsTier,
        PriceCreateParamsCustomUnitAmount as PriceCreateParamsCustomUnitAmount,
        PriceCreateParamsProductData as PriceCreateParamsProductData,
        PriceCreateParamsRecurring as PriceCreateParamsRecurring,
        PriceCreateParamsTier as PriceCreateParamsTier,
        PriceCreateParamsTransformQuantity as PriceCreateParamsTransformQuantity,
    )
    from stripe.params._price_list_params import (
        PriceListParams as PriceListParams,
        PriceListParamsCreated as PriceListParamsCreated,
        PriceListParamsRecurring as PriceListParamsRecurring,
    )
    from stripe.params._price_modify_params import (
        PriceModifyParams as PriceModifyParams,
        PriceModifyParamsCurrencyOptions as PriceModifyParamsCurrencyOptions,
        PriceModifyParamsCurrencyOptionsCustomUnitAmount as PriceModifyParamsCurrencyOptionsCustomUnitAmount,
        PriceModifyParamsCurrencyOptionsTier as PriceModifyParamsCurrencyOptionsTier,
    )
    from stripe.params._price_retrieve_params import (
        PriceRetrieveParams as PriceRetrieveParams,
    )
    from stripe.params._price_search_params import (
        PriceSearchParams as PriceSearchParams,
    )
    from stripe.params._price_update_params import (
        PriceUpdateParams as PriceUpdateParams,
        PriceUpdateParamsCurrencyOptions as PriceUpdateParamsCurrencyOptions,
        PriceUpdateParamsCurrencyOptionsCustomUnitAmount as PriceUpdateParamsCurrencyOptionsCustomUnitAmount,
        PriceUpdateParamsCurrencyOptionsTier as PriceUpdateParamsCurrencyOptionsTier,
    )
    from stripe.params._product_create_feature_params import (
        ProductCreateFeatureParams as ProductCreateFeatureParams,
    )
    from stripe.params._product_create_params import (
        ProductCreateParams as ProductCreateParams,
        ProductCreateParamsDefaultPriceData as ProductCreateParamsDefaultPriceData,
        ProductCreateParamsDefaultPriceDataCurrencyOptions as ProductCreateParamsDefaultPriceDataCurrencyOptions,
        ProductCreateParamsDefaultPriceDataCurrencyOptionsCustomUnitAmount as ProductCreateParamsDefaultPriceDataCurrencyOptionsCustomUnitAmount,
        ProductCreateParamsDefaultPriceDataCurrencyOptionsTier as ProductCreateParamsDefaultPriceDataCurrencyOptionsTier,
        ProductCreateParamsDefaultPriceDataCustomUnitAmount as ProductCreateParamsDefaultPriceDataCustomUnitAmount,
        ProductCreateParamsDefaultPriceDataRecurring as ProductCreateParamsDefaultPriceDataRecurring,
        ProductCreateParamsMarketingFeature as ProductCreateParamsMarketingFeature,
        ProductCreateParamsPackageDimensions as ProductCreateParamsPackageDimensions,
    )
    from stripe.params._product_delete_feature_params import (
        ProductDeleteFeatureParams as ProductDeleteFeatureParams,
    )
    from stripe.params._product_delete_params import (
        ProductDeleteParams as ProductDeleteParams,
    )
    from stripe.params._product_feature_create_params import (
        ProductFeatureCreateParams as ProductFeatureCreateParams,
    )
    from stripe.params._product_feature_delete_params import (
        ProductFeatureDeleteParams as ProductFeatureDeleteParams,
    )
    from stripe.params._product_feature_list_params import (
        ProductFeatureListParams as ProductFeatureListParams,
    )
    from stripe.params._product_feature_retrieve_params import (
        ProductFeatureRetrieveParams as ProductFeatureRetrieveParams,
    )
    from stripe.params._product_list_features_params import (
        ProductListFeaturesParams as ProductListFeaturesParams,
    )
    from stripe.params._product_list_params import (
        ProductListParams as ProductListParams,
        ProductListParamsCreated as ProductListParamsCreated,
    )
    from stripe.params._product_modify_params import (
        ProductModifyParams as ProductModifyParams,
        ProductModifyParamsMarketingFeature as ProductModifyParamsMarketingFeature,
        ProductModifyParamsPackageDimensions as ProductModifyParamsPackageDimensions,
    )
    from stripe.params._product_retrieve_feature_params import (
        ProductRetrieveFeatureParams as ProductRetrieveFeatureParams,
    )
    from stripe.params._product_retrieve_params import (
        ProductRetrieveParams as ProductRetrieveParams,
    )
    from stripe.params._product_search_params import (
        ProductSearchParams as ProductSearchParams,
    )
    from stripe.params._product_update_params import (
        ProductUpdateParams as ProductUpdateParams,
        ProductUpdateParamsMarketingFeature as ProductUpdateParamsMarketingFeature,
        ProductUpdateParamsPackageDimensions as ProductUpdateParamsPackageDimensions,
    )
    from stripe.params._promotion_code_create_params import (
        PromotionCodeCreateParams as PromotionCodeCreateParams,
        PromotionCodeCreateParamsPromotion as PromotionCodeCreateParamsPromotion,
        PromotionCodeCreateParamsRestrictions as PromotionCodeCreateParamsRestrictions,
        PromotionCodeCreateParamsRestrictionsCurrencyOptions as PromotionCodeCreateParamsRestrictionsCurrencyOptions,
    )
    from stripe.params._promotion_code_list_params import (
        PromotionCodeListParams as PromotionCodeListParams,
        PromotionCodeListParamsCreated as PromotionCodeListParamsCreated,
    )
    from stripe.params._promotion_code_modify_params import (
        PromotionCodeModifyParams as PromotionCodeModifyParams,
        PromotionCodeModifyParamsRestrictions as PromotionCodeModifyParamsRestrictions,
        PromotionCodeModifyParamsRestrictionsCurrencyOptions as PromotionCodeModifyParamsRestrictionsCurrencyOptions,
    )
    from stripe.params._promotion_code_retrieve_params import (
        PromotionCodeRetrieveParams as PromotionCodeRetrieveParams,
    )
    from stripe.params._promotion_code_update_params import (
        PromotionCodeUpdateParams as PromotionCodeUpdateParams,
        PromotionCodeUpdateParamsRestrictions as PromotionCodeUpdateParamsRestrictions,
        PromotionCodeUpdateParamsRestrictionsCurrencyOptions as PromotionCodeUpdateParamsRestrictionsCurrencyOptions,
    )
    from stripe.params._quote_accept_params import (
        QuoteAcceptParams as QuoteAcceptParams,
    )
    from stripe.params._quote_cancel_params import (
        QuoteCancelParams as QuoteCancelParams,
    )
    from stripe.params._quote_computed_upfront_line_items_list_params import (
        QuoteComputedUpfrontLineItemsListParams as QuoteComputedUpfrontLineItemsListParams,
    )
    from stripe.params._quote_create_params import (
        QuoteCreateParams as QuoteCreateParams,
        QuoteCreateParamsAutomaticTax as QuoteCreateParamsAutomaticTax,
        QuoteCreateParamsAutomaticTaxLiability as QuoteCreateParamsAutomaticTaxLiability,
        QuoteCreateParamsDiscount as QuoteCreateParamsDiscount,
        QuoteCreateParamsFromQuote as QuoteCreateParamsFromQuote,
        QuoteCreateParamsInvoiceSettings as QuoteCreateParamsInvoiceSettings,
        QuoteCreateParamsInvoiceSettingsIssuer as QuoteCreateParamsInvoiceSettingsIssuer,
        QuoteCreateParamsLineItem as QuoteCreateParamsLineItem,
        QuoteCreateParamsLineItemDiscount as QuoteCreateParamsLineItemDiscount,
        QuoteCreateParamsLineItemPriceData as QuoteCreateParamsLineItemPriceData,
        QuoteCreateParamsLineItemPriceDataRecurring as QuoteCreateParamsLineItemPriceDataRecurring,
        QuoteCreateParamsSubscriptionData as QuoteCreateParamsSubscriptionData,
        QuoteCreateParamsSubscriptionDataBillingMode as QuoteCreateParamsSubscriptionDataBillingMode,
        QuoteCreateParamsSubscriptionDataBillingModeFlexible as QuoteCreateParamsSubscriptionDataBillingModeFlexible,
        QuoteCreateParamsTransferData as QuoteCreateParamsTransferData,
    )
    from stripe.params._quote_finalize_quote_params import (
        QuoteFinalizeQuoteParams as QuoteFinalizeQuoteParams,
    )
    from stripe.params._quote_line_item_list_params import (
        QuoteLineItemListParams as QuoteLineItemListParams,
    )
    from stripe.params._quote_list_computed_upfront_line_items_params import (
        QuoteListComputedUpfrontLineItemsParams as QuoteListComputedUpfrontLineItemsParams,
    )
    from stripe.params._quote_list_line_items_params import (
        QuoteListLineItemsParams as QuoteListLineItemsParams,
    )
    from stripe.params._quote_list_params import (
        QuoteListParams as QuoteListParams,
    )
    from stripe.params._quote_modify_params import (
        QuoteModifyParams as QuoteModifyParams,
        QuoteModifyParamsAutomaticTax as QuoteModifyParamsAutomaticTax,
        QuoteModifyParamsAutomaticTaxLiability as QuoteModifyParamsAutomaticTaxLiability,
        QuoteModifyParamsDiscount as QuoteModifyParamsDiscount,
        QuoteModifyParamsInvoiceSettings as QuoteModifyParamsInvoiceSettings,
        QuoteModifyParamsInvoiceSettingsIssuer as QuoteModifyParamsInvoiceSettingsIssuer,
        QuoteModifyParamsLineItem as QuoteModifyParamsLineItem,
        QuoteModifyParamsLineItemDiscount as QuoteModifyParamsLineItemDiscount,
        QuoteModifyParamsLineItemPriceData as QuoteModifyParamsLineItemPriceData,
        QuoteModifyParamsLineItemPriceDataRecurring as QuoteModifyParamsLineItemPriceDataRecurring,
        QuoteModifyParamsSubscriptionData as QuoteModifyParamsSubscriptionData,
        QuoteModifyParamsTransferData as QuoteModifyParamsTransferData,
    )
    from stripe.params._quote_pdf_params import (
        QuotePdfParams as QuotePdfParams,
    )
    from stripe.params._quote_retrieve_params import (
        QuoteRetrieveParams as QuoteRetrieveParams,
    )
    from stripe.params._quote_update_params import (
        QuoteUpdateParams as QuoteUpdateParams,
        QuoteUpdateParamsAutomaticTax as QuoteUpdateParamsAutomaticTax,
        QuoteUpdateParamsAutomaticTaxLiability as QuoteUpdateParamsAutomaticTaxLiability,
        QuoteUpdateParamsDiscount as QuoteUpdateParamsDiscount,
        QuoteUpdateParamsInvoiceSettings as QuoteUpdateParamsInvoiceSettings,
        QuoteUpdateParamsInvoiceSettingsIssuer as QuoteUpdateParamsInvoiceSettingsIssuer,
        QuoteUpdateParamsLineItem as QuoteUpdateParamsLineItem,
        QuoteUpdateParamsLineItemDiscount as QuoteUpdateParamsLineItemDiscount,
        QuoteUpdateParamsLineItemPriceData as QuoteUpdateParamsLineItemPriceData,
        QuoteUpdateParamsLineItemPriceDataRecurring as QuoteUpdateParamsLineItemPriceDataRecurring,
        QuoteUpdateParamsSubscriptionData as QuoteUpdateParamsSubscriptionData,
        QuoteUpdateParamsTransferData as QuoteUpdateParamsTransferData,
    )
    from stripe.params._refund_cancel_params import (
        RefundCancelParams as RefundCancelParams,
    )
    from stripe.params._refund_create_params import (
        RefundCreateParams as RefundCreateParams,
    )
    from stripe.params._refund_expire_params import (
        RefundExpireParams as RefundExpireParams,
    )
    from stripe.params._refund_list_params import (
        RefundListParams as RefundListParams,
        RefundListParamsCreated as RefundListParamsCreated,
    )
    from stripe.params._refund_modify_params import (
        RefundModifyParams as RefundModifyParams,
    )
    from stripe.params._refund_retrieve_params import (
        RefundRetrieveParams as RefundRetrieveParams,
    )
    from stripe.params._refund_update_params import (
        RefundUpdateParams as RefundUpdateParams,
    )
    from stripe.params._review_approve_params import (
        ReviewApproveParams as ReviewApproveParams,
    )
    from stripe.params._review_list_params import (
        ReviewListParams as ReviewListParams,
        ReviewListParamsCreated as ReviewListParamsCreated,
    )
    from stripe.params._review_retrieve_params import (
        ReviewRetrieveParams as ReviewRetrieveParams,
    )
    from stripe.params._setup_attempt_list_params import (
        SetupAttemptListParams as SetupAttemptListParams,
        SetupAttemptListParamsCreated as SetupAttemptListParamsCreated,
    )
    from stripe.params._setup_intent_cancel_params import (
        SetupIntentCancelParams as SetupIntentCancelParams,
    )
    from stripe.params._setup_intent_confirm_params import (
        SetupIntentConfirmParams as SetupIntentConfirmParams,
        SetupIntentConfirmParamsMandateData as SetupIntentConfirmParamsMandateData,
        SetupIntentConfirmParamsMandateDataCustomerAcceptance as SetupIntentConfirmParamsMandateDataCustomerAcceptance,
        SetupIntentConfirmParamsMandateDataCustomerAcceptanceOffline as SetupIntentConfirmParamsMandateDataCustomerAcceptanceOffline,
        SetupIntentConfirmParamsMandateDataCustomerAcceptanceOnline as SetupIntentConfirmParamsMandateDataCustomerAcceptanceOnline,
        SetupIntentConfirmParamsPaymentMethodData as SetupIntentConfirmParamsPaymentMethodData,
        SetupIntentConfirmParamsPaymentMethodDataAcssDebit as SetupIntentConfirmParamsPaymentMethodDataAcssDebit,
        SetupIntentConfirmParamsPaymentMethodDataAffirm as SetupIntentConfirmParamsPaymentMethodDataAffirm,
        SetupIntentConfirmParamsPaymentMethodDataAfterpayClearpay as SetupIntentConfirmParamsPaymentMethodDataAfterpayClearpay,
        SetupIntentConfirmParamsPaymentMethodDataAlipay as SetupIntentConfirmParamsPaymentMethodDataAlipay,
        SetupIntentConfirmParamsPaymentMethodDataAlma as SetupIntentConfirmParamsPaymentMethodDataAlma,
        SetupIntentConfirmParamsPaymentMethodDataAmazonPay as SetupIntentConfirmParamsPaymentMethodDataAmazonPay,
        SetupIntentConfirmParamsPaymentMethodDataAuBecsDebit as SetupIntentConfirmParamsPaymentMethodDataAuBecsDebit,
        SetupIntentConfirmParamsPaymentMethodDataBacsDebit as SetupIntentConfirmParamsPaymentMethodDataBacsDebit,
        SetupIntentConfirmParamsPaymentMethodDataBancontact as SetupIntentConfirmParamsPaymentMethodDataBancontact,
        SetupIntentConfirmParamsPaymentMethodDataBillie as SetupIntentConfirmParamsPaymentMethodDataBillie,
        SetupIntentConfirmParamsPaymentMethodDataBillingDetails as SetupIntentConfirmParamsPaymentMethodDataBillingDetails,
        SetupIntentConfirmParamsPaymentMethodDataBillingDetailsAddress as SetupIntentConfirmParamsPaymentMethodDataBillingDetailsAddress,
        SetupIntentConfirmParamsPaymentMethodDataBlik as SetupIntentConfirmParamsPaymentMethodDataBlik,
        SetupIntentConfirmParamsPaymentMethodDataBoleto as SetupIntentConfirmParamsPaymentMethodDataBoleto,
        SetupIntentConfirmParamsPaymentMethodDataCashapp as SetupIntentConfirmParamsPaymentMethodDataCashapp,
        SetupIntentConfirmParamsPaymentMethodDataCrypto as SetupIntentConfirmParamsPaymentMethodDataCrypto,
        SetupIntentConfirmParamsPaymentMethodDataCustomerBalance as SetupIntentConfirmParamsPaymentMethodDataCustomerBalance,
        SetupIntentConfirmParamsPaymentMethodDataEps as SetupIntentConfirmParamsPaymentMethodDataEps,
        SetupIntentConfirmParamsPaymentMethodDataFpx as SetupIntentConfirmParamsPaymentMethodDataFpx,
        SetupIntentConfirmParamsPaymentMethodDataGiropay as SetupIntentConfirmParamsPaymentMethodDataGiropay,
        SetupIntentConfirmParamsPaymentMethodDataGrabpay as SetupIntentConfirmParamsPaymentMethodDataGrabpay,
        SetupIntentConfirmParamsPaymentMethodDataIdeal as SetupIntentConfirmParamsPaymentMethodDataIdeal,
        SetupIntentConfirmParamsPaymentMethodDataInteracPresent as SetupIntentConfirmParamsPaymentMethodDataInteracPresent,
        SetupIntentConfirmParamsPaymentMethodDataKakaoPay as SetupIntentConfirmParamsPaymentMethodDataKakaoPay,
        SetupIntentConfirmParamsPaymentMethodDataKlarna as SetupIntentConfirmParamsPaymentMethodDataKlarna,
        SetupIntentConfirmParamsPaymentMethodDataKlarnaDob as SetupIntentConfirmParamsPaymentMethodDataKlarnaDob,
        SetupIntentConfirmParamsPaymentMethodDataKonbini as SetupIntentConfirmParamsPaymentMethodDataKonbini,
        SetupIntentConfirmParamsPaymentMethodDataKrCard as SetupIntentConfirmParamsPaymentMethodDataKrCard,
        SetupIntentConfirmParamsPaymentMethodDataLink as SetupIntentConfirmParamsPaymentMethodDataLink,
        SetupIntentConfirmParamsPaymentMethodDataMbWay as SetupIntentConfirmParamsPaymentMethodDataMbWay,
        SetupIntentConfirmParamsPaymentMethodDataMobilepay as SetupIntentConfirmParamsPaymentMethodDataMobilepay,
        SetupIntentConfirmParamsPaymentMethodDataMultibanco as SetupIntentConfirmParamsPaymentMethodDataMultibanco,
        SetupIntentConfirmParamsPaymentMethodDataNaverPay as SetupIntentConfirmParamsPaymentMethodDataNaverPay,
        SetupIntentConfirmParamsPaymentMethodDataNzBankAccount as SetupIntentConfirmParamsPaymentMethodDataNzBankAccount,
        SetupIntentConfirmParamsPaymentMethodDataOxxo as SetupIntentConfirmParamsPaymentMethodDataOxxo,
        SetupIntentConfirmParamsPaymentMethodDataP24 as SetupIntentConfirmParamsPaymentMethodDataP24,
        SetupIntentConfirmParamsPaymentMethodDataPayByBank as SetupIntentConfirmParamsPaymentMethodDataPayByBank,
        SetupIntentConfirmParamsPaymentMethodDataPayco as SetupIntentConfirmParamsPaymentMethodDataPayco,
        SetupIntentConfirmParamsPaymentMethodDataPaynow as SetupIntentConfirmParamsPaymentMethodDataPaynow,
        SetupIntentConfirmParamsPaymentMethodDataPaypal as SetupIntentConfirmParamsPaymentMethodDataPaypal,
        SetupIntentConfirmParamsPaymentMethodDataPayto as SetupIntentConfirmParamsPaymentMethodDataPayto,
        SetupIntentConfirmParamsPaymentMethodDataPix as SetupIntentConfirmParamsPaymentMethodDataPix,
        SetupIntentConfirmParamsPaymentMethodDataPromptpay as SetupIntentConfirmParamsPaymentMethodDataPromptpay,
        SetupIntentConfirmParamsPaymentMethodDataRadarOptions as SetupIntentConfirmParamsPaymentMethodDataRadarOptions,
        SetupIntentConfirmParamsPaymentMethodDataRevolutPay as SetupIntentConfirmParamsPaymentMethodDataRevolutPay,
        SetupIntentConfirmParamsPaymentMethodDataSamsungPay as SetupIntentConfirmParamsPaymentMethodDataSamsungPay,
        SetupIntentConfirmParamsPaymentMethodDataSatispay as SetupIntentConfirmParamsPaymentMethodDataSatispay,
        SetupIntentConfirmParamsPaymentMethodDataSepaDebit as SetupIntentConfirmParamsPaymentMethodDataSepaDebit,
        SetupIntentConfirmParamsPaymentMethodDataSofort as SetupIntentConfirmParamsPaymentMethodDataSofort,
        SetupIntentConfirmParamsPaymentMethodDataSwish as SetupIntentConfirmParamsPaymentMethodDataSwish,
        SetupIntentConfirmParamsPaymentMethodDataTwint as SetupIntentConfirmParamsPaymentMethodDataTwint,
        SetupIntentConfirmParamsPaymentMethodDataUsBankAccount as SetupIntentConfirmParamsPaymentMethodDataUsBankAccount,
        SetupIntentConfirmParamsPaymentMethodDataWechatPay as SetupIntentConfirmParamsPaymentMethodDataWechatPay,
        SetupIntentConfirmParamsPaymentMethodDataZip as SetupIntentConfirmParamsPaymentMethodDataZip,
        SetupIntentConfirmParamsPaymentMethodOptions as SetupIntentConfirmParamsPaymentMethodOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsAcssDebit as SetupIntentConfirmParamsPaymentMethodOptionsAcssDebit,
        SetupIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsAmazonPay as SetupIntentConfirmParamsPaymentMethodOptionsAmazonPay,
        SetupIntentConfirmParamsPaymentMethodOptionsBacsDebit as SetupIntentConfirmParamsPaymentMethodOptionsBacsDebit,
        SetupIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsCard as SetupIntentConfirmParamsPaymentMethodOptionsCard,
        SetupIntentConfirmParamsPaymentMethodOptionsCardMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsCardMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsCardPresent as SetupIntentConfirmParamsPaymentMethodOptionsCardPresent,
        SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure as SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure,
        SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        SetupIntentConfirmParamsPaymentMethodOptionsKlarna as SetupIntentConfirmParamsPaymentMethodOptionsKlarna,
        SetupIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand as SetupIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand,
        SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription as SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription,
        SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        SetupIntentConfirmParamsPaymentMethodOptionsLink as SetupIntentConfirmParamsPaymentMethodOptionsLink,
        SetupIntentConfirmParamsPaymentMethodOptionsPaypal as SetupIntentConfirmParamsPaymentMethodOptionsPaypal,
        SetupIntentConfirmParamsPaymentMethodOptionsPayto as SetupIntentConfirmParamsPaymentMethodOptionsPayto,
        SetupIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsSepaDebit as SetupIntentConfirmParamsPaymentMethodOptionsSepaDebit,
        SetupIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccount as SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccount,
        SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections as SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions as SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks as SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks,
    )
    from stripe.params._setup_intent_create_params import (
        SetupIntentCreateParams as SetupIntentCreateParams,
        SetupIntentCreateParamsAutomaticPaymentMethods as SetupIntentCreateParamsAutomaticPaymentMethods,
        SetupIntentCreateParamsMandateData as SetupIntentCreateParamsMandateData,
        SetupIntentCreateParamsMandateDataCustomerAcceptance as SetupIntentCreateParamsMandateDataCustomerAcceptance,
        SetupIntentCreateParamsMandateDataCustomerAcceptanceOffline as SetupIntentCreateParamsMandateDataCustomerAcceptanceOffline,
        SetupIntentCreateParamsMandateDataCustomerAcceptanceOnline as SetupIntentCreateParamsMandateDataCustomerAcceptanceOnline,
        SetupIntentCreateParamsPaymentMethodData as SetupIntentCreateParamsPaymentMethodData,
        SetupIntentCreateParamsPaymentMethodDataAcssDebit as SetupIntentCreateParamsPaymentMethodDataAcssDebit,
        SetupIntentCreateParamsPaymentMethodDataAffirm as SetupIntentCreateParamsPaymentMethodDataAffirm,
        SetupIntentCreateParamsPaymentMethodDataAfterpayClearpay as SetupIntentCreateParamsPaymentMethodDataAfterpayClearpay,
        SetupIntentCreateParamsPaymentMethodDataAlipay as SetupIntentCreateParamsPaymentMethodDataAlipay,
        SetupIntentCreateParamsPaymentMethodDataAlma as SetupIntentCreateParamsPaymentMethodDataAlma,
        SetupIntentCreateParamsPaymentMethodDataAmazonPay as SetupIntentCreateParamsPaymentMethodDataAmazonPay,
        SetupIntentCreateParamsPaymentMethodDataAuBecsDebit as SetupIntentCreateParamsPaymentMethodDataAuBecsDebit,
        SetupIntentCreateParamsPaymentMethodDataBacsDebit as SetupIntentCreateParamsPaymentMethodDataBacsDebit,
        SetupIntentCreateParamsPaymentMethodDataBancontact as SetupIntentCreateParamsPaymentMethodDataBancontact,
        SetupIntentCreateParamsPaymentMethodDataBillie as SetupIntentCreateParamsPaymentMethodDataBillie,
        SetupIntentCreateParamsPaymentMethodDataBillingDetails as SetupIntentCreateParamsPaymentMethodDataBillingDetails,
        SetupIntentCreateParamsPaymentMethodDataBillingDetailsAddress as SetupIntentCreateParamsPaymentMethodDataBillingDetailsAddress,
        SetupIntentCreateParamsPaymentMethodDataBlik as SetupIntentCreateParamsPaymentMethodDataBlik,
        SetupIntentCreateParamsPaymentMethodDataBoleto as SetupIntentCreateParamsPaymentMethodDataBoleto,
        SetupIntentCreateParamsPaymentMethodDataCashapp as SetupIntentCreateParamsPaymentMethodDataCashapp,
        SetupIntentCreateParamsPaymentMethodDataCrypto as SetupIntentCreateParamsPaymentMethodDataCrypto,
        SetupIntentCreateParamsPaymentMethodDataCustomerBalance as SetupIntentCreateParamsPaymentMethodDataCustomerBalance,
        SetupIntentCreateParamsPaymentMethodDataEps as SetupIntentCreateParamsPaymentMethodDataEps,
        SetupIntentCreateParamsPaymentMethodDataFpx as SetupIntentCreateParamsPaymentMethodDataFpx,
        SetupIntentCreateParamsPaymentMethodDataGiropay as SetupIntentCreateParamsPaymentMethodDataGiropay,
        SetupIntentCreateParamsPaymentMethodDataGrabpay as SetupIntentCreateParamsPaymentMethodDataGrabpay,
        SetupIntentCreateParamsPaymentMethodDataIdeal as SetupIntentCreateParamsPaymentMethodDataIdeal,
        SetupIntentCreateParamsPaymentMethodDataInteracPresent as SetupIntentCreateParamsPaymentMethodDataInteracPresent,
        SetupIntentCreateParamsPaymentMethodDataKakaoPay as SetupIntentCreateParamsPaymentMethodDataKakaoPay,
        SetupIntentCreateParamsPaymentMethodDataKlarna as SetupIntentCreateParamsPaymentMethodDataKlarna,
        SetupIntentCreateParamsPaymentMethodDataKlarnaDob as SetupIntentCreateParamsPaymentMethodDataKlarnaDob,
        SetupIntentCreateParamsPaymentMethodDataKonbini as SetupIntentCreateParamsPaymentMethodDataKonbini,
        SetupIntentCreateParamsPaymentMethodDataKrCard as SetupIntentCreateParamsPaymentMethodDataKrCard,
        SetupIntentCreateParamsPaymentMethodDataLink as SetupIntentCreateParamsPaymentMethodDataLink,
        SetupIntentCreateParamsPaymentMethodDataMbWay as SetupIntentCreateParamsPaymentMethodDataMbWay,
        SetupIntentCreateParamsPaymentMethodDataMobilepay as SetupIntentCreateParamsPaymentMethodDataMobilepay,
        SetupIntentCreateParamsPaymentMethodDataMultibanco as SetupIntentCreateParamsPaymentMethodDataMultibanco,
        SetupIntentCreateParamsPaymentMethodDataNaverPay as SetupIntentCreateParamsPaymentMethodDataNaverPay,
        SetupIntentCreateParamsPaymentMethodDataNzBankAccount as SetupIntentCreateParamsPaymentMethodDataNzBankAccount,
        SetupIntentCreateParamsPaymentMethodDataOxxo as SetupIntentCreateParamsPaymentMethodDataOxxo,
        SetupIntentCreateParamsPaymentMethodDataP24 as SetupIntentCreateParamsPaymentMethodDataP24,
        SetupIntentCreateParamsPaymentMethodDataPayByBank as SetupIntentCreateParamsPaymentMethodDataPayByBank,
        SetupIntentCreateParamsPaymentMethodDataPayco as SetupIntentCreateParamsPaymentMethodDataPayco,
        SetupIntentCreateParamsPaymentMethodDataPaynow as SetupIntentCreateParamsPaymentMethodDataPaynow,
        SetupIntentCreateParamsPaymentMethodDataPaypal as SetupIntentCreateParamsPaymentMethodDataPaypal,
        SetupIntentCreateParamsPaymentMethodDataPayto as SetupIntentCreateParamsPaymentMethodDataPayto,
        SetupIntentCreateParamsPaymentMethodDataPix as SetupIntentCreateParamsPaymentMethodDataPix,
        SetupIntentCreateParamsPaymentMethodDataPromptpay as SetupIntentCreateParamsPaymentMethodDataPromptpay,
        SetupIntentCreateParamsPaymentMethodDataRadarOptions as SetupIntentCreateParamsPaymentMethodDataRadarOptions,
        SetupIntentCreateParamsPaymentMethodDataRevolutPay as SetupIntentCreateParamsPaymentMethodDataRevolutPay,
        SetupIntentCreateParamsPaymentMethodDataSamsungPay as SetupIntentCreateParamsPaymentMethodDataSamsungPay,
        SetupIntentCreateParamsPaymentMethodDataSatispay as SetupIntentCreateParamsPaymentMethodDataSatispay,
        SetupIntentCreateParamsPaymentMethodDataSepaDebit as SetupIntentCreateParamsPaymentMethodDataSepaDebit,
        SetupIntentCreateParamsPaymentMethodDataSofort as SetupIntentCreateParamsPaymentMethodDataSofort,
        SetupIntentCreateParamsPaymentMethodDataSwish as SetupIntentCreateParamsPaymentMethodDataSwish,
        SetupIntentCreateParamsPaymentMethodDataTwint as SetupIntentCreateParamsPaymentMethodDataTwint,
        SetupIntentCreateParamsPaymentMethodDataUsBankAccount as SetupIntentCreateParamsPaymentMethodDataUsBankAccount,
        SetupIntentCreateParamsPaymentMethodDataWechatPay as SetupIntentCreateParamsPaymentMethodDataWechatPay,
        SetupIntentCreateParamsPaymentMethodDataZip as SetupIntentCreateParamsPaymentMethodDataZip,
        SetupIntentCreateParamsPaymentMethodOptions as SetupIntentCreateParamsPaymentMethodOptions,
        SetupIntentCreateParamsPaymentMethodOptionsAcssDebit as SetupIntentCreateParamsPaymentMethodOptionsAcssDebit,
        SetupIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsAmazonPay as SetupIntentCreateParamsPaymentMethodOptionsAmazonPay,
        SetupIntentCreateParamsPaymentMethodOptionsBacsDebit as SetupIntentCreateParamsPaymentMethodOptionsBacsDebit,
        SetupIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsCard as SetupIntentCreateParamsPaymentMethodOptionsCard,
        SetupIntentCreateParamsPaymentMethodOptionsCardMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsCardMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsCardPresent as SetupIntentCreateParamsPaymentMethodOptionsCardPresent,
        SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecure as SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecure,
        SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        SetupIntentCreateParamsPaymentMethodOptionsKlarna as SetupIntentCreateParamsPaymentMethodOptionsKlarna,
        SetupIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand as SetupIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand,
        SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscription as SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscription,
        SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        SetupIntentCreateParamsPaymentMethodOptionsLink as SetupIntentCreateParamsPaymentMethodOptionsLink,
        SetupIntentCreateParamsPaymentMethodOptionsPaypal as SetupIntentCreateParamsPaymentMethodOptionsPaypal,
        SetupIntentCreateParamsPaymentMethodOptionsPayto as SetupIntentCreateParamsPaymentMethodOptionsPayto,
        SetupIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsSepaDebit as SetupIntentCreateParamsPaymentMethodOptionsSepaDebit,
        SetupIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsUsBankAccount as SetupIntentCreateParamsPaymentMethodOptionsUsBankAccount,
        SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections as SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions as SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks as SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks,
        SetupIntentCreateParamsSingleUse as SetupIntentCreateParamsSingleUse,
    )
    from stripe.params._setup_intent_list_params import (
        SetupIntentListParams as SetupIntentListParams,
        SetupIntentListParamsCreated as SetupIntentListParamsCreated,
    )
    from stripe.params._setup_intent_modify_params import (
        SetupIntentModifyParams as SetupIntentModifyParams,
        SetupIntentModifyParamsPaymentMethodData as SetupIntentModifyParamsPaymentMethodData,
        SetupIntentModifyParamsPaymentMethodDataAcssDebit as SetupIntentModifyParamsPaymentMethodDataAcssDebit,
        SetupIntentModifyParamsPaymentMethodDataAffirm as SetupIntentModifyParamsPaymentMethodDataAffirm,
        SetupIntentModifyParamsPaymentMethodDataAfterpayClearpay as SetupIntentModifyParamsPaymentMethodDataAfterpayClearpay,
        SetupIntentModifyParamsPaymentMethodDataAlipay as SetupIntentModifyParamsPaymentMethodDataAlipay,
        SetupIntentModifyParamsPaymentMethodDataAlma as SetupIntentModifyParamsPaymentMethodDataAlma,
        SetupIntentModifyParamsPaymentMethodDataAmazonPay as SetupIntentModifyParamsPaymentMethodDataAmazonPay,
        SetupIntentModifyParamsPaymentMethodDataAuBecsDebit as SetupIntentModifyParamsPaymentMethodDataAuBecsDebit,
        SetupIntentModifyParamsPaymentMethodDataBacsDebit as SetupIntentModifyParamsPaymentMethodDataBacsDebit,
        SetupIntentModifyParamsPaymentMethodDataBancontact as SetupIntentModifyParamsPaymentMethodDataBancontact,
        SetupIntentModifyParamsPaymentMethodDataBillie as SetupIntentModifyParamsPaymentMethodDataBillie,
        SetupIntentModifyParamsPaymentMethodDataBillingDetails as SetupIntentModifyParamsPaymentMethodDataBillingDetails,
        SetupIntentModifyParamsPaymentMethodDataBillingDetailsAddress as SetupIntentModifyParamsPaymentMethodDataBillingDetailsAddress,
        SetupIntentModifyParamsPaymentMethodDataBlik as SetupIntentModifyParamsPaymentMethodDataBlik,
        SetupIntentModifyParamsPaymentMethodDataBoleto as SetupIntentModifyParamsPaymentMethodDataBoleto,
        SetupIntentModifyParamsPaymentMethodDataCashapp as SetupIntentModifyParamsPaymentMethodDataCashapp,
        SetupIntentModifyParamsPaymentMethodDataCrypto as SetupIntentModifyParamsPaymentMethodDataCrypto,
        SetupIntentModifyParamsPaymentMethodDataCustomerBalance as SetupIntentModifyParamsPaymentMethodDataCustomerBalance,
        SetupIntentModifyParamsPaymentMethodDataEps as SetupIntentModifyParamsPaymentMethodDataEps,
        SetupIntentModifyParamsPaymentMethodDataFpx as SetupIntentModifyParamsPaymentMethodDataFpx,
        SetupIntentModifyParamsPaymentMethodDataGiropay as SetupIntentModifyParamsPaymentMethodDataGiropay,
        SetupIntentModifyParamsPaymentMethodDataGrabpay as SetupIntentModifyParamsPaymentMethodDataGrabpay,
        SetupIntentModifyParamsPaymentMethodDataIdeal as SetupIntentModifyParamsPaymentMethodDataIdeal,
        SetupIntentModifyParamsPaymentMethodDataInteracPresent as SetupIntentModifyParamsPaymentMethodDataInteracPresent,
        SetupIntentModifyParamsPaymentMethodDataKakaoPay as SetupIntentModifyParamsPaymentMethodDataKakaoPay,
        SetupIntentModifyParamsPaymentMethodDataKlarna as SetupIntentModifyParamsPaymentMethodDataKlarna,
        SetupIntentModifyParamsPaymentMethodDataKlarnaDob as SetupIntentModifyParamsPaymentMethodDataKlarnaDob,
        SetupIntentModifyParamsPaymentMethodDataKonbini as SetupIntentModifyParamsPaymentMethodDataKonbini,
        SetupIntentModifyParamsPaymentMethodDataKrCard as SetupIntentModifyParamsPaymentMethodDataKrCard,
        SetupIntentModifyParamsPaymentMethodDataLink as SetupIntentModifyParamsPaymentMethodDataLink,
        SetupIntentModifyParamsPaymentMethodDataMbWay as SetupIntentModifyParamsPaymentMethodDataMbWay,
        SetupIntentModifyParamsPaymentMethodDataMobilepay as SetupIntentModifyParamsPaymentMethodDataMobilepay,
        SetupIntentModifyParamsPaymentMethodDataMultibanco as SetupIntentModifyParamsPaymentMethodDataMultibanco,
        SetupIntentModifyParamsPaymentMethodDataNaverPay as SetupIntentModifyParamsPaymentMethodDataNaverPay,
        SetupIntentModifyParamsPaymentMethodDataNzBankAccount as SetupIntentModifyParamsPaymentMethodDataNzBankAccount,
        SetupIntentModifyParamsPaymentMethodDataOxxo as SetupIntentModifyParamsPaymentMethodDataOxxo,
        SetupIntentModifyParamsPaymentMethodDataP24 as SetupIntentModifyParamsPaymentMethodDataP24,
        SetupIntentModifyParamsPaymentMethodDataPayByBank as SetupIntentModifyParamsPaymentMethodDataPayByBank,
        SetupIntentModifyParamsPaymentMethodDataPayco as SetupIntentModifyParamsPaymentMethodDataPayco,
        SetupIntentModifyParamsPaymentMethodDataPaynow as SetupIntentModifyParamsPaymentMethodDataPaynow,
        SetupIntentModifyParamsPaymentMethodDataPaypal as SetupIntentModifyParamsPaymentMethodDataPaypal,
        SetupIntentModifyParamsPaymentMethodDataPayto as SetupIntentModifyParamsPaymentMethodDataPayto,
        SetupIntentModifyParamsPaymentMethodDataPix as SetupIntentModifyParamsPaymentMethodDataPix,
        SetupIntentModifyParamsPaymentMethodDataPromptpay as SetupIntentModifyParamsPaymentMethodDataPromptpay,
        SetupIntentModifyParamsPaymentMethodDataRadarOptions as SetupIntentModifyParamsPaymentMethodDataRadarOptions,
        SetupIntentModifyParamsPaymentMethodDataRevolutPay as SetupIntentModifyParamsPaymentMethodDataRevolutPay,
        SetupIntentModifyParamsPaymentMethodDataSamsungPay as SetupIntentModifyParamsPaymentMethodDataSamsungPay,
        SetupIntentModifyParamsPaymentMethodDataSatispay as SetupIntentModifyParamsPaymentMethodDataSatispay,
        SetupIntentModifyParamsPaymentMethodDataSepaDebit as SetupIntentModifyParamsPaymentMethodDataSepaDebit,
        SetupIntentModifyParamsPaymentMethodDataSofort as SetupIntentModifyParamsPaymentMethodDataSofort,
        SetupIntentModifyParamsPaymentMethodDataSwish as SetupIntentModifyParamsPaymentMethodDataSwish,
        SetupIntentModifyParamsPaymentMethodDataTwint as SetupIntentModifyParamsPaymentMethodDataTwint,
        SetupIntentModifyParamsPaymentMethodDataUsBankAccount as SetupIntentModifyParamsPaymentMethodDataUsBankAccount,
        SetupIntentModifyParamsPaymentMethodDataWechatPay as SetupIntentModifyParamsPaymentMethodDataWechatPay,
        SetupIntentModifyParamsPaymentMethodDataZip as SetupIntentModifyParamsPaymentMethodDataZip,
        SetupIntentModifyParamsPaymentMethodOptions as SetupIntentModifyParamsPaymentMethodOptions,
        SetupIntentModifyParamsPaymentMethodOptionsAcssDebit as SetupIntentModifyParamsPaymentMethodOptionsAcssDebit,
        SetupIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsAmazonPay as SetupIntentModifyParamsPaymentMethodOptionsAmazonPay,
        SetupIntentModifyParamsPaymentMethodOptionsBacsDebit as SetupIntentModifyParamsPaymentMethodOptionsBacsDebit,
        SetupIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsCard as SetupIntentModifyParamsPaymentMethodOptionsCard,
        SetupIntentModifyParamsPaymentMethodOptionsCardMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsCardMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsCardPresent as SetupIntentModifyParamsPaymentMethodOptionsCardPresent,
        SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecure as SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecure,
        SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        SetupIntentModifyParamsPaymentMethodOptionsKlarna as SetupIntentModifyParamsPaymentMethodOptionsKlarna,
        SetupIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand as SetupIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand,
        SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscription as SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscription,
        SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        SetupIntentModifyParamsPaymentMethodOptionsLink as SetupIntentModifyParamsPaymentMethodOptionsLink,
        SetupIntentModifyParamsPaymentMethodOptionsPaypal as SetupIntentModifyParamsPaymentMethodOptionsPaypal,
        SetupIntentModifyParamsPaymentMethodOptionsPayto as SetupIntentModifyParamsPaymentMethodOptionsPayto,
        SetupIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsSepaDebit as SetupIntentModifyParamsPaymentMethodOptionsSepaDebit,
        SetupIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsUsBankAccount as SetupIntentModifyParamsPaymentMethodOptionsUsBankAccount,
        SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections as SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions as SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks as SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks,
    )
    from stripe.params._setup_intent_retrieve_params import (
        SetupIntentRetrieveParams as SetupIntentRetrieveParams,
    )
    from stripe.params._setup_intent_update_params import (
        SetupIntentUpdateParams as SetupIntentUpdateParams,
        SetupIntentUpdateParamsPaymentMethodData as SetupIntentUpdateParamsPaymentMethodData,
        SetupIntentUpdateParamsPaymentMethodDataAcssDebit as SetupIntentUpdateParamsPaymentMethodDataAcssDebit,
        SetupIntentUpdateParamsPaymentMethodDataAffirm as SetupIntentUpdateParamsPaymentMethodDataAffirm,
        SetupIntentUpdateParamsPaymentMethodDataAfterpayClearpay as SetupIntentUpdateParamsPaymentMethodDataAfterpayClearpay,
        SetupIntentUpdateParamsPaymentMethodDataAlipay as SetupIntentUpdateParamsPaymentMethodDataAlipay,
        SetupIntentUpdateParamsPaymentMethodDataAlma as SetupIntentUpdateParamsPaymentMethodDataAlma,
        SetupIntentUpdateParamsPaymentMethodDataAmazonPay as SetupIntentUpdateParamsPaymentMethodDataAmazonPay,
        SetupIntentUpdateParamsPaymentMethodDataAuBecsDebit as SetupIntentUpdateParamsPaymentMethodDataAuBecsDebit,
        SetupIntentUpdateParamsPaymentMethodDataBacsDebit as SetupIntentUpdateParamsPaymentMethodDataBacsDebit,
        SetupIntentUpdateParamsPaymentMethodDataBancontact as SetupIntentUpdateParamsPaymentMethodDataBancontact,
        SetupIntentUpdateParamsPaymentMethodDataBillie as SetupIntentUpdateParamsPaymentMethodDataBillie,
        SetupIntentUpdateParamsPaymentMethodDataBillingDetails as SetupIntentUpdateParamsPaymentMethodDataBillingDetails,
        SetupIntentUpdateParamsPaymentMethodDataBillingDetailsAddress as SetupIntentUpdateParamsPaymentMethodDataBillingDetailsAddress,
        SetupIntentUpdateParamsPaymentMethodDataBlik as SetupIntentUpdateParamsPaymentMethodDataBlik,
        SetupIntentUpdateParamsPaymentMethodDataBoleto as SetupIntentUpdateParamsPaymentMethodDataBoleto,
        SetupIntentUpdateParamsPaymentMethodDataCashapp as SetupIntentUpdateParamsPaymentMethodDataCashapp,
        SetupIntentUpdateParamsPaymentMethodDataCrypto as SetupIntentUpdateParamsPaymentMethodDataCrypto,
        SetupIntentUpdateParamsPaymentMethodDataCustomerBalance as SetupIntentUpdateParamsPaymentMethodDataCustomerBalance,
        SetupIntentUpdateParamsPaymentMethodDataEps as SetupIntentUpdateParamsPaymentMethodDataEps,
        SetupIntentUpdateParamsPaymentMethodDataFpx as SetupIntentUpdateParamsPaymentMethodDataFpx,
        SetupIntentUpdateParamsPaymentMethodDataGiropay as SetupIntentUpdateParamsPaymentMethodDataGiropay,
        SetupIntentUpdateParamsPaymentMethodDataGrabpay as SetupIntentUpdateParamsPaymentMethodDataGrabpay,
        SetupIntentUpdateParamsPaymentMethodDataIdeal as SetupIntentUpdateParamsPaymentMethodDataIdeal,
        SetupIntentUpdateParamsPaymentMethodDataInteracPresent as SetupIntentUpdateParamsPaymentMethodDataInteracPresent,
        SetupIntentUpdateParamsPaymentMethodDataKakaoPay as SetupIntentUpdateParamsPaymentMethodDataKakaoPay,
        SetupIntentUpdateParamsPaymentMethodDataKlarna as SetupIntentUpdateParamsPaymentMethodDataKlarna,
        SetupIntentUpdateParamsPaymentMethodDataKlarnaDob as SetupIntentUpdateParamsPaymentMethodDataKlarnaDob,
        SetupIntentUpdateParamsPaymentMethodDataKonbini as SetupIntentUpdateParamsPaymentMethodDataKonbini,
        SetupIntentUpdateParamsPaymentMethodDataKrCard as SetupIntentUpdateParamsPaymentMethodDataKrCard,
        SetupIntentUpdateParamsPaymentMethodDataLink as SetupIntentUpdateParamsPaymentMethodDataLink,
        SetupIntentUpdateParamsPaymentMethodDataMbWay as SetupIntentUpdateParamsPaymentMethodDataMbWay,
        SetupIntentUpdateParamsPaymentMethodDataMobilepay as SetupIntentUpdateParamsPaymentMethodDataMobilepay,
        SetupIntentUpdateParamsPaymentMethodDataMultibanco as SetupIntentUpdateParamsPaymentMethodDataMultibanco,
        SetupIntentUpdateParamsPaymentMethodDataNaverPay as SetupIntentUpdateParamsPaymentMethodDataNaverPay,
        SetupIntentUpdateParamsPaymentMethodDataNzBankAccount as SetupIntentUpdateParamsPaymentMethodDataNzBankAccount,
        SetupIntentUpdateParamsPaymentMethodDataOxxo as SetupIntentUpdateParamsPaymentMethodDataOxxo,
        SetupIntentUpdateParamsPaymentMethodDataP24 as SetupIntentUpdateParamsPaymentMethodDataP24,
        SetupIntentUpdateParamsPaymentMethodDataPayByBank as SetupIntentUpdateParamsPaymentMethodDataPayByBank,
        SetupIntentUpdateParamsPaymentMethodDataPayco as SetupIntentUpdateParamsPaymentMethodDataPayco,
        SetupIntentUpdateParamsPaymentMethodDataPaynow as SetupIntentUpdateParamsPaymentMethodDataPaynow,
        SetupIntentUpdateParamsPaymentMethodDataPaypal as SetupIntentUpdateParamsPaymentMethodDataPaypal,
        SetupIntentUpdateParamsPaymentMethodDataPayto as SetupIntentUpdateParamsPaymentMethodDataPayto,
        SetupIntentUpdateParamsPaymentMethodDataPix as SetupIntentUpdateParamsPaymentMethodDataPix,
        SetupIntentUpdateParamsPaymentMethodDataPromptpay as SetupIntentUpdateParamsPaymentMethodDataPromptpay,
        SetupIntentUpdateParamsPaymentMethodDataRadarOptions as SetupIntentUpdateParamsPaymentMethodDataRadarOptions,
        SetupIntentUpdateParamsPaymentMethodDataRevolutPay as SetupIntentUpdateParamsPaymentMethodDataRevolutPay,
        SetupIntentUpdateParamsPaymentMethodDataSamsungPay as SetupIntentUpdateParamsPaymentMethodDataSamsungPay,
        SetupIntentUpdateParamsPaymentMethodDataSatispay as SetupIntentUpdateParamsPaymentMethodDataSatispay,
        SetupIntentUpdateParamsPaymentMethodDataSepaDebit as SetupIntentUpdateParamsPaymentMethodDataSepaDebit,
        SetupIntentUpdateParamsPaymentMethodDataSofort as SetupIntentUpdateParamsPaymentMethodDataSofort,
        SetupIntentUpdateParamsPaymentMethodDataSwish as SetupIntentUpdateParamsPaymentMethodDataSwish,
        SetupIntentUpdateParamsPaymentMethodDataTwint as SetupIntentUpdateParamsPaymentMethodDataTwint,
        SetupIntentUpdateParamsPaymentMethodDataUsBankAccount as SetupIntentUpdateParamsPaymentMethodDataUsBankAccount,
        SetupIntentUpdateParamsPaymentMethodDataWechatPay as SetupIntentUpdateParamsPaymentMethodDataWechatPay,
        SetupIntentUpdateParamsPaymentMethodDataZip as SetupIntentUpdateParamsPaymentMethodDataZip,
        SetupIntentUpdateParamsPaymentMethodOptions as SetupIntentUpdateParamsPaymentMethodOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsAcssDebit as SetupIntentUpdateParamsPaymentMethodOptionsAcssDebit,
        SetupIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsAmazonPay as SetupIntentUpdateParamsPaymentMethodOptionsAmazonPay,
        SetupIntentUpdateParamsPaymentMethodOptionsBacsDebit as SetupIntentUpdateParamsPaymentMethodOptionsBacsDebit,
        SetupIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsCard as SetupIntentUpdateParamsPaymentMethodOptionsCard,
        SetupIntentUpdateParamsPaymentMethodOptionsCardMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsCardMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsCardPresent as SetupIntentUpdateParamsPaymentMethodOptionsCardPresent,
        SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure as SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure,
        SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions as SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires as SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires,
        SetupIntentUpdateParamsPaymentMethodOptionsKlarna as SetupIntentUpdateParamsPaymentMethodOptionsKlarna,
        SetupIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand as SetupIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand,
        SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription as SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription,
        SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling as SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling,
        SetupIntentUpdateParamsPaymentMethodOptionsLink as SetupIntentUpdateParamsPaymentMethodOptionsLink,
        SetupIntentUpdateParamsPaymentMethodOptionsPaypal as SetupIntentUpdateParamsPaymentMethodOptionsPaypal,
        SetupIntentUpdateParamsPaymentMethodOptionsPayto as SetupIntentUpdateParamsPaymentMethodOptionsPayto,
        SetupIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsSepaDebit as SetupIntentUpdateParamsPaymentMethodOptionsSepaDebit,
        SetupIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccount as SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccount,
        SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections as SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions as SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions,
        SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks as SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks,
    )
    from stripe.params._setup_intent_verify_microdeposits_params import (
        SetupIntentVerifyMicrodepositsParams as SetupIntentVerifyMicrodepositsParams,
    )
    from stripe.params._shipping_rate_create_params import (
        ShippingRateCreateParams as ShippingRateCreateParams,
        ShippingRateCreateParamsDeliveryEstimate as ShippingRateCreateParamsDeliveryEstimate,
        ShippingRateCreateParamsDeliveryEstimateMaximum as ShippingRateCreateParamsDeliveryEstimateMaximum,
        ShippingRateCreateParamsDeliveryEstimateMinimum as ShippingRateCreateParamsDeliveryEstimateMinimum,
        ShippingRateCreateParamsFixedAmount as ShippingRateCreateParamsFixedAmount,
        ShippingRateCreateParamsFixedAmountCurrencyOptions as ShippingRateCreateParamsFixedAmountCurrencyOptions,
    )
    from stripe.params._shipping_rate_list_params import (
        ShippingRateListParams as ShippingRateListParams,
        ShippingRateListParamsCreated as ShippingRateListParamsCreated,
    )
    from stripe.params._shipping_rate_modify_params import (
        ShippingRateModifyParams as ShippingRateModifyParams,
        ShippingRateModifyParamsFixedAmount as ShippingRateModifyParamsFixedAmount,
        ShippingRateModifyParamsFixedAmountCurrencyOptions as ShippingRateModifyParamsFixedAmountCurrencyOptions,
    )
    from stripe.params._shipping_rate_retrieve_params import (
        ShippingRateRetrieveParams as ShippingRateRetrieveParams,
    )
    from stripe.params._shipping_rate_update_params import (
        ShippingRateUpdateParams as ShippingRateUpdateParams,
        ShippingRateUpdateParamsFixedAmount as ShippingRateUpdateParamsFixedAmount,
        ShippingRateUpdateParamsFixedAmountCurrencyOptions as ShippingRateUpdateParamsFixedAmountCurrencyOptions,
    )
    from stripe.params._source_create_params import (
        SourceCreateParams as SourceCreateParams,
        SourceCreateParamsMandate as SourceCreateParamsMandate,
        SourceCreateParamsMandateAcceptance as SourceCreateParamsMandateAcceptance,
        SourceCreateParamsMandateAcceptanceOffline as SourceCreateParamsMandateAcceptanceOffline,
        SourceCreateParamsMandateAcceptanceOnline as SourceCreateParamsMandateAcceptanceOnline,
        SourceCreateParamsOwner as SourceCreateParamsOwner,
        SourceCreateParamsOwnerAddress as SourceCreateParamsOwnerAddress,
        SourceCreateParamsReceiver as SourceCreateParamsReceiver,
        SourceCreateParamsRedirect as SourceCreateParamsRedirect,
        SourceCreateParamsSourceOrder as SourceCreateParamsSourceOrder,
        SourceCreateParamsSourceOrderItem as SourceCreateParamsSourceOrderItem,
        SourceCreateParamsSourceOrderShipping as SourceCreateParamsSourceOrderShipping,
        SourceCreateParamsSourceOrderShippingAddress as SourceCreateParamsSourceOrderShippingAddress,
    )
    from stripe.params._source_detach_params import (
        SourceDetachParams as SourceDetachParams,
    )
    from stripe.params._source_list_source_transactions_params import (
        SourceListSourceTransactionsParams as SourceListSourceTransactionsParams,
    )
    from stripe.params._source_modify_params import (
        SourceModifyParams as SourceModifyParams,
        SourceModifyParamsMandate as SourceModifyParamsMandate,
        SourceModifyParamsMandateAcceptance as SourceModifyParamsMandateAcceptance,
        SourceModifyParamsMandateAcceptanceOffline as SourceModifyParamsMandateAcceptanceOffline,
        SourceModifyParamsMandateAcceptanceOnline as SourceModifyParamsMandateAcceptanceOnline,
        SourceModifyParamsOwner as SourceModifyParamsOwner,
        SourceModifyParamsOwnerAddress as SourceModifyParamsOwnerAddress,
        SourceModifyParamsSourceOrder as SourceModifyParamsSourceOrder,
        SourceModifyParamsSourceOrderItem as SourceModifyParamsSourceOrderItem,
        SourceModifyParamsSourceOrderShipping as SourceModifyParamsSourceOrderShipping,
        SourceModifyParamsSourceOrderShippingAddress as SourceModifyParamsSourceOrderShippingAddress,
    )
    from stripe.params._source_retrieve_params import (
        SourceRetrieveParams as SourceRetrieveParams,
    )
    from stripe.params._source_transaction_list_params import (
        SourceTransactionListParams as SourceTransactionListParams,
    )
    from stripe.params._source_update_params import (
        SourceUpdateParams as SourceUpdateParams,
        SourceUpdateParamsMandate as SourceUpdateParamsMandate,
        SourceUpdateParamsMandateAcceptance as SourceUpdateParamsMandateAcceptance,
        SourceUpdateParamsMandateAcceptanceOffline as SourceUpdateParamsMandateAcceptanceOffline,
        SourceUpdateParamsMandateAcceptanceOnline as SourceUpdateParamsMandateAcceptanceOnline,
        SourceUpdateParamsOwner as SourceUpdateParamsOwner,
        SourceUpdateParamsOwnerAddress as SourceUpdateParamsOwnerAddress,
        SourceUpdateParamsSourceOrder as SourceUpdateParamsSourceOrder,
        SourceUpdateParamsSourceOrderItem as SourceUpdateParamsSourceOrderItem,
        SourceUpdateParamsSourceOrderShipping as SourceUpdateParamsSourceOrderShipping,
        SourceUpdateParamsSourceOrderShippingAddress as SourceUpdateParamsSourceOrderShippingAddress,
    )
    from stripe.params._source_verify_params import (
        SourceVerifyParams as SourceVerifyParams,
    )
    from stripe.params._subscription_cancel_params import (
        SubscriptionCancelParams as SubscriptionCancelParams,
        SubscriptionCancelParamsCancellationDetails as SubscriptionCancelParamsCancellationDetails,
    )
    from stripe.params._subscription_create_params import (
        SubscriptionCreateParams as SubscriptionCreateParams,
        SubscriptionCreateParamsAddInvoiceItem as SubscriptionCreateParamsAddInvoiceItem,
        SubscriptionCreateParamsAddInvoiceItemDiscount as SubscriptionCreateParamsAddInvoiceItemDiscount,
        SubscriptionCreateParamsAddInvoiceItemPeriod as SubscriptionCreateParamsAddInvoiceItemPeriod,
        SubscriptionCreateParamsAddInvoiceItemPeriodEnd as SubscriptionCreateParamsAddInvoiceItemPeriodEnd,
        SubscriptionCreateParamsAddInvoiceItemPeriodStart as SubscriptionCreateParamsAddInvoiceItemPeriodStart,
        SubscriptionCreateParamsAddInvoiceItemPriceData as SubscriptionCreateParamsAddInvoiceItemPriceData,
        SubscriptionCreateParamsAutomaticTax as SubscriptionCreateParamsAutomaticTax,
        SubscriptionCreateParamsAutomaticTaxLiability as SubscriptionCreateParamsAutomaticTaxLiability,
        SubscriptionCreateParamsBillingCycleAnchorConfig as SubscriptionCreateParamsBillingCycleAnchorConfig,
        SubscriptionCreateParamsBillingMode as SubscriptionCreateParamsBillingMode,
        SubscriptionCreateParamsBillingModeFlexible as SubscriptionCreateParamsBillingModeFlexible,
        SubscriptionCreateParamsBillingThresholds as SubscriptionCreateParamsBillingThresholds,
        SubscriptionCreateParamsDiscount as SubscriptionCreateParamsDiscount,
        SubscriptionCreateParamsInvoiceSettings as SubscriptionCreateParamsInvoiceSettings,
        SubscriptionCreateParamsInvoiceSettingsIssuer as SubscriptionCreateParamsInvoiceSettingsIssuer,
        SubscriptionCreateParamsItem as SubscriptionCreateParamsItem,
        SubscriptionCreateParamsItemBillingThresholds as SubscriptionCreateParamsItemBillingThresholds,
        SubscriptionCreateParamsItemDiscount as SubscriptionCreateParamsItemDiscount,
        SubscriptionCreateParamsItemPriceData as SubscriptionCreateParamsItemPriceData,
        SubscriptionCreateParamsItemPriceDataRecurring as SubscriptionCreateParamsItemPriceDataRecurring,
        SubscriptionCreateParamsPaymentSettings as SubscriptionCreateParamsPaymentSettings,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptions as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptions,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsBancontact as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsBancontact,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCard as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCard,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsKonbini as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsKonbini,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPayto as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPayto,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SubscriptionCreateParamsPendingInvoiceItemInterval as SubscriptionCreateParamsPendingInvoiceItemInterval,
        SubscriptionCreateParamsTransferData as SubscriptionCreateParamsTransferData,
        SubscriptionCreateParamsTrialSettings as SubscriptionCreateParamsTrialSettings,
        SubscriptionCreateParamsTrialSettingsEndBehavior as SubscriptionCreateParamsTrialSettingsEndBehavior,
    )
    from stripe.params._subscription_delete_discount_params import (
        SubscriptionDeleteDiscountParams as SubscriptionDeleteDiscountParams,
    )
    from stripe.params._subscription_item_create_params import (
        SubscriptionItemCreateParams as SubscriptionItemCreateParams,
        SubscriptionItemCreateParamsBillingThresholds as SubscriptionItemCreateParamsBillingThresholds,
        SubscriptionItemCreateParamsDiscount as SubscriptionItemCreateParamsDiscount,
        SubscriptionItemCreateParamsPriceData as SubscriptionItemCreateParamsPriceData,
        SubscriptionItemCreateParamsPriceDataRecurring as SubscriptionItemCreateParamsPriceDataRecurring,
    )
    from stripe.params._subscription_item_delete_params import (
        SubscriptionItemDeleteParams as SubscriptionItemDeleteParams,
    )
    from stripe.params._subscription_item_list_params import (
        SubscriptionItemListParams as SubscriptionItemListParams,
    )
    from stripe.params._subscription_item_modify_params import (
        SubscriptionItemModifyParams as SubscriptionItemModifyParams,
        SubscriptionItemModifyParamsBillingThresholds as SubscriptionItemModifyParamsBillingThresholds,
        SubscriptionItemModifyParamsDiscount as SubscriptionItemModifyParamsDiscount,
        SubscriptionItemModifyParamsPriceData as SubscriptionItemModifyParamsPriceData,
        SubscriptionItemModifyParamsPriceDataRecurring as SubscriptionItemModifyParamsPriceDataRecurring,
    )
    from stripe.params._subscription_item_retrieve_params import (
        SubscriptionItemRetrieveParams as SubscriptionItemRetrieveParams,
    )
    from stripe.params._subscription_item_update_params import (
        SubscriptionItemUpdateParams as SubscriptionItemUpdateParams,
        SubscriptionItemUpdateParamsBillingThresholds as SubscriptionItemUpdateParamsBillingThresholds,
        SubscriptionItemUpdateParamsDiscount as SubscriptionItemUpdateParamsDiscount,
        SubscriptionItemUpdateParamsPriceData as SubscriptionItemUpdateParamsPriceData,
        SubscriptionItemUpdateParamsPriceDataRecurring as SubscriptionItemUpdateParamsPriceDataRecurring,
    )
    from stripe.params._subscription_list_params import (
        SubscriptionListParams as SubscriptionListParams,
        SubscriptionListParamsAutomaticTax as SubscriptionListParamsAutomaticTax,
        SubscriptionListParamsCreated as SubscriptionListParamsCreated,
        SubscriptionListParamsCurrentPeriodEnd as SubscriptionListParamsCurrentPeriodEnd,
        SubscriptionListParamsCurrentPeriodStart as SubscriptionListParamsCurrentPeriodStart,
    )
    from stripe.params._subscription_migrate_params import (
        SubscriptionMigrateParams as SubscriptionMigrateParams,
        SubscriptionMigrateParamsBillingMode as SubscriptionMigrateParamsBillingMode,
        SubscriptionMigrateParamsBillingModeFlexible as SubscriptionMigrateParamsBillingModeFlexible,
    )
    from stripe.params._subscription_modify_params import (
        SubscriptionModifyParams as SubscriptionModifyParams,
        SubscriptionModifyParamsAddInvoiceItem as SubscriptionModifyParamsAddInvoiceItem,
        SubscriptionModifyParamsAddInvoiceItemDiscount as SubscriptionModifyParamsAddInvoiceItemDiscount,
        SubscriptionModifyParamsAddInvoiceItemPeriod as SubscriptionModifyParamsAddInvoiceItemPeriod,
        SubscriptionModifyParamsAddInvoiceItemPeriodEnd as SubscriptionModifyParamsAddInvoiceItemPeriodEnd,
        SubscriptionModifyParamsAddInvoiceItemPeriodStart as SubscriptionModifyParamsAddInvoiceItemPeriodStart,
        SubscriptionModifyParamsAddInvoiceItemPriceData as SubscriptionModifyParamsAddInvoiceItemPriceData,
        SubscriptionModifyParamsAutomaticTax as SubscriptionModifyParamsAutomaticTax,
        SubscriptionModifyParamsAutomaticTaxLiability as SubscriptionModifyParamsAutomaticTaxLiability,
        SubscriptionModifyParamsBillingThresholds as SubscriptionModifyParamsBillingThresholds,
        SubscriptionModifyParamsCancellationDetails as SubscriptionModifyParamsCancellationDetails,
        SubscriptionModifyParamsDiscount as SubscriptionModifyParamsDiscount,
        SubscriptionModifyParamsInvoiceSettings as SubscriptionModifyParamsInvoiceSettings,
        SubscriptionModifyParamsInvoiceSettingsIssuer as SubscriptionModifyParamsInvoiceSettingsIssuer,
        SubscriptionModifyParamsItem as SubscriptionModifyParamsItem,
        SubscriptionModifyParamsItemBillingThresholds as SubscriptionModifyParamsItemBillingThresholds,
        SubscriptionModifyParamsItemDiscount as SubscriptionModifyParamsItemDiscount,
        SubscriptionModifyParamsItemPriceData as SubscriptionModifyParamsItemPriceData,
        SubscriptionModifyParamsItemPriceDataRecurring as SubscriptionModifyParamsItemPriceDataRecurring,
        SubscriptionModifyParamsPauseCollection as SubscriptionModifyParamsPauseCollection,
        SubscriptionModifyParamsPaymentSettings as SubscriptionModifyParamsPaymentSettings,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptions as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptions,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsBancontact as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsBancontact,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCard as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCard,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsKonbini as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsKonbini,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPayto as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPayto,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SubscriptionModifyParamsPendingInvoiceItemInterval as SubscriptionModifyParamsPendingInvoiceItemInterval,
        SubscriptionModifyParamsTransferData as SubscriptionModifyParamsTransferData,
        SubscriptionModifyParamsTrialSettings as SubscriptionModifyParamsTrialSettings,
        SubscriptionModifyParamsTrialSettingsEndBehavior as SubscriptionModifyParamsTrialSettingsEndBehavior,
    )
    from stripe.params._subscription_resume_params import (
        SubscriptionResumeParams as SubscriptionResumeParams,
    )
    from stripe.params._subscription_retrieve_params import (
        SubscriptionRetrieveParams as SubscriptionRetrieveParams,
    )
    from stripe.params._subscription_schedule_cancel_params import (
        SubscriptionScheduleCancelParams as SubscriptionScheduleCancelParams,
    )
    from stripe.params._subscription_schedule_create_params import (
        SubscriptionScheduleCreateParams as SubscriptionScheduleCreateParams,
        SubscriptionScheduleCreateParamsBillingMode as SubscriptionScheduleCreateParamsBillingMode,
        SubscriptionScheduleCreateParamsBillingModeFlexible as SubscriptionScheduleCreateParamsBillingModeFlexible,
        SubscriptionScheduleCreateParamsDefaultSettings as SubscriptionScheduleCreateParamsDefaultSettings,
        SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTax as SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTax,
        SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTaxLiability as SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTaxLiability,
        SubscriptionScheduleCreateParamsDefaultSettingsBillingThresholds as SubscriptionScheduleCreateParamsDefaultSettingsBillingThresholds,
        SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettings as SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettings,
        SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettingsIssuer as SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettingsIssuer,
        SubscriptionScheduleCreateParamsDefaultSettingsTransferData as SubscriptionScheduleCreateParamsDefaultSettingsTransferData,
        SubscriptionScheduleCreateParamsPhase as SubscriptionScheduleCreateParamsPhase,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItem as SubscriptionScheduleCreateParamsPhaseAddInvoiceItem,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItemDiscount as SubscriptionScheduleCreateParamsPhaseAddInvoiceItemDiscount,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriod as SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriod,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodEnd as SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodEnd,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodStart as SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodStart,
        SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPriceData as SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPriceData,
        SubscriptionScheduleCreateParamsPhaseAutomaticTax as SubscriptionScheduleCreateParamsPhaseAutomaticTax,
        SubscriptionScheduleCreateParamsPhaseAutomaticTaxLiability as SubscriptionScheduleCreateParamsPhaseAutomaticTaxLiability,
        SubscriptionScheduleCreateParamsPhaseBillingThresholds as SubscriptionScheduleCreateParamsPhaseBillingThresholds,
        SubscriptionScheduleCreateParamsPhaseDiscount as SubscriptionScheduleCreateParamsPhaseDiscount,
        SubscriptionScheduleCreateParamsPhaseDuration as SubscriptionScheduleCreateParamsPhaseDuration,
        SubscriptionScheduleCreateParamsPhaseInvoiceSettings as SubscriptionScheduleCreateParamsPhaseInvoiceSettings,
        SubscriptionScheduleCreateParamsPhaseInvoiceSettingsIssuer as SubscriptionScheduleCreateParamsPhaseInvoiceSettingsIssuer,
        SubscriptionScheduleCreateParamsPhaseItem as SubscriptionScheduleCreateParamsPhaseItem,
        SubscriptionScheduleCreateParamsPhaseItemBillingThresholds as SubscriptionScheduleCreateParamsPhaseItemBillingThresholds,
        SubscriptionScheduleCreateParamsPhaseItemDiscount as SubscriptionScheduleCreateParamsPhaseItemDiscount,
        SubscriptionScheduleCreateParamsPhaseItemPriceData as SubscriptionScheduleCreateParamsPhaseItemPriceData,
        SubscriptionScheduleCreateParamsPhaseItemPriceDataRecurring as SubscriptionScheduleCreateParamsPhaseItemPriceDataRecurring,
        SubscriptionScheduleCreateParamsPhaseTransferData as SubscriptionScheduleCreateParamsPhaseTransferData,
    )
    from stripe.params._subscription_schedule_list_params import (
        SubscriptionScheduleListParams as SubscriptionScheduleListParams,
        SubscriptionScheduleListParamsCanceledAt as SubscriptionScheduleListParamsCanceledAt,
        SubscriptionScheduleListParamsCompletedAt as SubscriptionScheduleListParamsCompletedAt,
        SubscriptionScheduleListParamsCreated as SubscriptionScheduleListParamsCreated,
        SubscriptionScheduleListParamsReleasedAt as SubscriptionScheduleListParamsReleasedAt,
    )
    from stripe.params._subscription_schedule_modify_params import (
        SubscriptionScheduleModifyParams as SubscriptionScheduleModifyParams,
        SubscriptionScheduleModifyParamsDefaultSettings as SubscriptionScheduleModifyParamsDefaultSettings,
        SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTax as SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTax,
        SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTaxLiability as SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTaxLiability,
        SubscriptionScheduleModifyParamsDefaultSettingsBillingThresholds as SubscriptionScheduleModifyParamsDefaultSettingsBillingThresholds,
        SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettings as SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettings,
        SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettingsIssuer as SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettingsIssuer,
        SubscriptionScheduleModifyParamsDefaultSettingsTransferData as SubscriptionScheduleModifyParamsDefaultSettingsTransferData,
        SubscriptionScheduleModifyParamsPhase as SubscriptionScheduleModifyParamsPhase,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItem as SubscriptionScheduleModifyParamsPhaseAddInvoiceItem,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItemDiscount as SubscriptionScheduleModifyParamsPhaseAddInvoiceItemDiscount,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriod as SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriod,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodEnd as SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodEnd,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodStart as SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodStart,
        SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPriceData as SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPriceData,
        SubscriptionScheduleModifyParamsPhaseAutomaticTax as SubscriptionScheduleModifyParamsPhaseAutomaticTax,
        SubscriptionScheduleModifyParamsPhaseAutomaticTaxLiability as SubscriptionScheduleModifyParamsPhaseAutomaticTaxLiability,
        SubscriptionScheduleModifyParamsPhaseBillingThresholds as SubscriptionScheduleModifyParamsPhaseBillingThresholds,
        SubscriptionScheduleModifyParamsPhaseDiscount as SubscriptionScheduleModifyParamsPhaseDiscount,
        SubscriptionScheduleModifyParamsPhaseDuration as SubscriptionScheduleModifyParamsPhaseDuration,
        SubscriptionScheduleModifyParamsPhaseInvoiceSettings as SubscriptionScheduleModifyParamsPhaseInvoiceSettings,
        SubscriptionScheduleModifyParamsPhaseInvoiceSettingsIssuer as SubscriptionScheduleModifyParamsPhaseInvoiceSettingsIssuer,
        SubscriptionScheduleModifyParamsPhaseItem as SubscriptionScheduleModifyParamsPhaseItem,
        SubscriptionScheduleModifyParamsPhaseItemBillingThresholds as SubscriptionScheduleModifyParamsPhaseItemBillingThresholds,
        SubscriptionScheduleModifyParamsPhaseItemDiscount as SubscriptionScheduleModifyParamsPhaseItemDiscount,
        SubscriptionScheduleModifyParamsPhaseItemPriceData as SubscriptionScheduleModifyParamsPhaseItemPriceData,
        SubscriptionScheduleModifyParamsPhaseItemPriceDataRecurring as SubscriptionScheduleModifyParamsPhaseItemPriceDataRecurring,
        SubscriptionScheduleModifyParamsPhaseTransferData as SubscriptionScheduleModifyParamsPhaseTransferData,
    )
    from stripe.params._subscription_schedule_release_params import (
        SubscriptionScheduleReleaseParams as SubscriptionScheduleReleaseParams,
    )
    from stripe.params._subscription_schedule_retrieve_params import (
        SubscriptionScheduleRetrieveParams as SubscriptionScheduleRetrieveParams,
    )
    from stripe.params._subscription_schedule_update_params import (
        SubscriptionScheduleUpdateParams as SubscriptionScheduleUpdateParams,
        SubscriptionScheduleUpdateParamsDefaultSettings as SubscriptionScheduleUpdateParamsDefaultSettings,
        SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTax as SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTax,
        SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTaxLiability as SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTaxLiability,
        SubscriptionScheduleUpdateParamsDefaultSettingsBillingThresholds as SubscriptionScheduleUpdateParamsDefaultSettingsBillingThresholds,
        SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettings as SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettings,
        SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettingsIssuer as SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettingsIssuer,
        SubscriptionScheduleUpdateParamsDefaultSettingsTransferData as SubscriptionScheduleUpdateParamsDefaultSettingsTransferData,
        SubscriptionScheduleUpdateParamsPhase as SubscriptionScheduleUpdateParamsPhase,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItem as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItem,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemDiscount as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemDiscount,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriod as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriod,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodEnd as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodEnd,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodStart as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodStart,
        SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPriceData as SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPriceData,
        SubscriptionScheduleUpdateParamsPhaseAutomaticTax as SubscriptionScheduleUpdateParamsPhaseAutomaticTax,
        SubscriptionScheduleUpdateParamsPhaseAutomaticTaxLiability as SubscriptionScheduleUpdateParamsPhaseAutomaticTaxLiability,
        SubscriptionScheduleUpdateParamsPhaseBillingThresholds as SubscriptionScheduleUpdateParamsPhaseBillingThresholds,
        SubscriptionScheduleUpdateParamsPhaseDiscount as SubscriptionScheduleUpdateParamsPhaseDiscount,
        SubscriptionScheduleUpdateParamsPhaseDuration as SubscriptionScheduleUpdateParamsPhaseDuration,
        SubscriptionScheduleUpdateParamsPhaseInvoiceSettings as SubscriptionScheduleUpdateParamsPhaseInvoiceSettings,
        SubscriptionScheduleUpdateParamsPhaseInvoiceSettingsIssuer as SubscriptionScheduleUpdateParamsPhaseInvoiceSettingsIssuer,
        SubscriptionScheduleUpdateParamsPhaseItem as SubscriptionScheduleUpdateParamsPhaseItem,
        SubscriptionScheduleUpdateParamsPhaseItemBillingThresholds as SubscriptionScheduleUpdateParamsPhaseItemBillingThresholds,
        SubscriptionScheduleUpdateParamsPhaseItemDiscount as SubscriptionScheduleUpdateParamsPhaseItemDiscount,
        SubscriptionScheduleUpdateParamsPhaseItemPriceData as SubscriptionScheduleUpdateParamsPhaseItemPriceData,
        SubscriptionScheduleUpdateParamsPhaseItemPriceDataRecurring as SubscriptionScheduleUpdateParamsPhaseItemPriceDataRecurring,
        SubscriptionScheduleUpdateParamsPhaseTransferData as SubscriptionScheduleUpdateParamsPhaseTransferData,
    )
    from stripe.params._subscription_search_params import (
        SubscriptionSearchParams as SubscriptionSearchParams,
    )
    from stripe.params._subscription_update_params import (
        SubscriptionUpdateParams as SubscriptionUpdateParams,
        SubscriptionUpdateParamsAddInvoiceItem as SubscriptionUpdateParamsAddInvoiceItem,
        SubscriptionUpdateParamsAddInvoiceItemDiscount as SubscriptionUpdateParamsAddInvoiceItemDiscount,
        SubscriptionUpdateParamsAddInvoiceItemPeriod as SubscriptionUpdateParamsAddInvoiceItemPeriod,
        SubscriptionUpdateParamsAddInvoiceItemPeriodEnd as SubscriptionUpdateParamsAddInvoiceItemPeriodEnd,
        SubscriptionUpdateParamsAddInvoiceItemPeriodStart as SubscriptionUpdateParamsAddInvoiceItemPeriodStart,
        SubscriptionUpdateParamsAddInvoiceItemPriceData as SubscriptionUpdateParamsAddInvoiceItemPriceData,
        SubscriptionUpdateParamsAutomaticTax as SubscriptionUpdateParamsAutomaticTax,
        SubscriptionUpdateParamsAutomaticTaxLiability as SubscriptionUpdateParamsAutomaticTaxLiability,
        SubscriptionUpdateParamsBillingThresholds as SubscriptionUpdateParamsBillingThresholds,
        SubscriptionUpdateParamsCancellationDetails as SubscriptionUpdateParamsCancellationDetails,
        SubscriptionUpdateParamsDiscount as SubscriptionUpdateParamsDiscount,
        SubscriptionUpdateParamsInvoiceSettings as SubscriptionUpdateParamsInvoiceSettings,
        SubscriptionUpdateParamsInvoiceSettingsIssuer as SubscriptionUpdateParamsInvoiceSettingsIssuer,
        SubscriptionUpdateParamsItem as SubscriptionUpdateParamsItem,
        SubscriptionUpdateParamsItemBillingThresholds as SubscriptionUpdateParamsItemBillingThresholds,
        SubscriptionUpdateParamsItemDiscount as SubscriptionUpdateParamsItemDiscount,
        SubscriptionUpdateParamsItemPriceData as SubscriptionUpdateParamsItemPriceData,
        SubscriptionUpdateParamsItemPriceDataRecurring as SubscriptionUpdateParamsItemPriceDataRecurring,
        SubscriptionUpdateParamsPauseCollection as SubscriptionUpdateParamsPauseCollection,
        SubscriptionUpdateParamsPaymentSettings as SubscriptionUpdateParamsPaymentSettings,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptions as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptions,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCard as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCard,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPayto as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPayto,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections,
        SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters as SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters,
        SubscriptionUpdateParamsPendingInvoiceItemInterval as SubscriptionUpdateParamsPendingInvoiceItemInterval,
        SubscriptionUpdateParamsTransferData as SubscriptionUpdateParamsTransferData,
        SubscriptionUpdateParamsTrialSettings as SubscriptionUpdateParamsTrialSettings,
        SubscriptionUpdateParamsTrialSettingsEndBehavior as SubscriptionUpdateParamsTrialSettingsEndBehavior,
    )
    from stripe.params._tax_code_list_params import (
        TaxCodeListParams as TaxCodeListParams,
    )
    from stripe.params._tax_code_retrieve_params import (
        TaxCodeRetrieveParams as TaxCodeRetrieveParams,
    )
    from stripe.params._tax_id_create_params import (
        TaxIdCreateParams as TaxIdCreateParams,
        TaxIdCreateParamsOwner as TaxIdCreateParamsOwner,
    )
    from stripe.params._tax_id_delete_params import (
        TaxIdDeleteParams as TaxIdDeleteParams,
    )
    from stripe.params._tax_id_list_params import (
        TaxIdListParams as TaxIdListParams,
        TaxIdListParamsOwner as TaxIdListParamsOwner,
    )
    from stripe.params._tax_id_retrieve_params import (
        TaxIdRetrieveParams as TaxIdRetrieveParams,
    )
    from stripe.params._tax_rate_create_params import (
        TaxRateCreateParams as TaxRateCreateParams,
    )
    from stripe.params._tax_rate_list_params import (
        TaxRateListParams as TaxRateListParams,
        TaxRateListParamsCreated as TaxRateListParamsCreated,
    )
    from stripe.params._tax_rate_modify_params import (
        TaxRateModifyParams as TaxRateModifyParams,
    )
    from stripe.params._tax_rate_retrieve_params import (
        TaxRateRetrieveParams as TaxRateRetrieveParams,
    )
    from stripe.params._tax_rate_update_params import (
        TaxRateUpdateParams as TaxRateUpdateParams,
    )
    from stripe.params._token_create_params import (
        TokenCreateParams as TokenCreateParams,
        TokenCreateParamsAccount as TokenCreateParamsAccount,
        TokenCreateParamsAccountCompany as TokenCreateParamsAccountCompany,
        TokenCreateParamsAccountCompanyAddress as TokenCreateParamsAccountCompanyAddress,
        TokenCreateParamsAccountCompanyAddressKana as TokenCreateParamsAccountCompanyAddressKana,
        TokenCreateParamsAccountCompanyAddressKanji as TokenCreateParamsAccountCompanyAddressKanji,
        TokenCreateParamsAccountCompanyDirectorshipDeclaration as TokenCreateParamsAccountCompanyDirectorshipDeclaration,
        TokenCreateParamsAccountCompanyOwnershipDeclaration as TokenCreateParamsAccountCompanyOwnershipDeclaration,
        TokenCreateParamsAccountCompanyRegistrationDate as TokenCreateParamsAccountCompanyRegistrationDate,
        TokenCreateParamsAccountCompanyRepresentativeDeclaration as TokenCreateParamsAccountCompanyRepresentativeDeclaration,
        TokenCreateParamsAccountCompanyVerification as TokenCreateParamsAccountCompanyVerification,
        TokenCreateParamsAccountCompanyVerificationDocument as TokenCreateParamsAccountCompanyVerificationDocument,
        TokenCreateParamsAccountIndividual as TokenCreateParamsAccountIndividual,
        TokenCreateParamsAccountIndividualAddress as TokenCreateParamsAccountIndividualAddress,
        TokenCreateParamsAccountIndividualAddressKana as TokenCreateParamsAccountIndividualAddressKana,
        TokenCreateParamsAccountIndividualAddressKanji as TokenCreateParamsAccountIndividualAddressKanji,
        TokenCreateParamsAccountIndividualDob as TokenCreateParamsAccountIndividualDob,
        TokenCreateParamsAccountIndividualRegisteredAddress as TokenCreateParamsAccountIndividualRegisteredAddress,
        TokenCreateParamsAccountIndividualRelationship as TokenCreateParamsAccountIndividualRelationship,
        TokenCreateParamsAccountIndividualVerification as TokenCreateParamsAccountIndividualVerification,
        TokenCreateParamsAccountIndividualVerificationAdditionalDocument as TokenCreateParamsAccountIndividualVerificationAdditionalDocument,
        TokenCreateParamsAccountIndividualVerificationDocument as TokenCreateParamsAccountIndividualVerificationDocument,
        TokenCreateParamsBankAccount as TokenCreateParamsBankAccount,
        TokenCreateParamsCard as TokenCreateParamsCard,
        TokenCreateParamsCardNetworks as TokenCreateParamsCardNetworks,
        TokenCreateParamsCvcUpdate as TokenCreateParamsCvcUpdate,
        TokenCreateParamsPerson as TokenCreateParamsPerson,
        TokenCreateParamsPersonAdditionalTosAcceptances as TokenCreateParamsPersonAdditionalTosAcceptances,
        TokenCreateParamsPersonAdditionalTosAcceptancesAccount as TokenCreateParamsPersonAdditionalTosAcceptancesAccount,
        TokenCreateParamsPersonAddress as TokenCreateParamsPersonAddress,
        TokenCreateParamsPersonAddressKana as TokenCreateParamsPersonAddressKana,
        TokenCreateParamsPersonAddressKanji as TokenCreateParamsPersonAddressKanji,
        TokenCreateParamsPersonDob as TokenCreateParamsPersonDob,
        TokenCreateParamsPersonDocuments as TokenCreateParamsPersonDocuments,
        TokenCreateParamsPersonDocumentsCompanyAuthorization as TokenCreateParamsPersonDocumentsCompanyAuthorization,
        TokenCreateParamsPersonDocumentsPassport as TokenCreateParamsPersonDocumentsPassport,
        TokenCreateParamsPersonDocumentsVisa as TokenCreateParamsPersonDocumentsVisa,
        TokenCreateParamsPersonRegisteredAddress as TokenCreateParamsPersonRegisteredAddress,
        TokenCreateParamsPersonRelationship as TokenCreateParamsPersonRelationship,
        TokenCreateParamsPersonUsCfpbData as TokenCreateParamsPersonUsCfpbData,
        TokenCreateParamsPersonUsCfpbDataEthnicityDetails as TokenCreateParamsPersonUsCfpbDataEthnicityDetails,
        TokenCreateParamsPersonUsCfpbDataRaceDetails as TokenCreateParamsPersonUsCfpbDataRaceDetails,
        TokenCreateParamsPersonVerification as TokenCreateParamsPersonVerification,
        TokenCreateParamsPersonVerificationAdditionalDocument as TokenCreateParamsPersonVerificationAdditionalDocument,
        TokenCreateParamsPersonVerificationDocument as TokenCreateParamsPersonVerificationDocument,
        TokenCreateParamsPii as TokenCreateParamsPii,
    )
    from stripe.params._token_retrieve_params import (
        TokenRetrieveParams as TokenRetrieveParams,
    )
    from stripe.params._topup_cancel_params import (
        TopupCancelParams as TopupCancelParams,
    )
    from stripe.params._topup_create_params import (
        TopupCreateParams as TopupCreateParams,
    )
    from stripe.params._topup_list_params import (
        TopupListParams as TopupListParams,
        TopupListParamsAmount as TopupListParamsAmount,
        TopupListParamsCreated as TopupListParamsCreated,
    )
    from stripe.params._topup_modify_params import (
        TopupModifyParams as TopupModifyParams,
    )
    from stripe.params._topup_retrieve_params import (
        TopupRetrieveParams as TopupRetrieveParams,
    )
    from stripe.params._topup_update_params import (
        TopupUpdateParams as TopupUpdateParams,
    )
    from stripe.params._transfer_create_params import (
        TransferCreateParams as TransferCreateParams,
    )
    from stripe.params._transfer_create_reversal_params import (
        TransferCreateReversalParams as TransferCreateReversalParams,
    )
    from stripe.params._transfer_list_params import (
        TransferListParams as TransferListParams,
        TransferListParamsCreated as TransferListParamsCreated,
    )
    from stripe.params._transfer_list_reversals_params import (
        TransferListReversalsParams as TransferListReversalsParams,
    )
    from stripe.params._transfer_modify_params import (
        TransferModifyParams as TransferModifyParams,
    )
    from stripe.params._transfer_modify_reversal_params import (
        TransferModifyReversalParams as TransferModifyReversalParams,
    )
    from stripe.params._transfer_retrieve_params import (
        TransferRetrieveParams as TransferRetrieveParams,
    )
    from stripe.params._transfer_retrieve_reversal_params import (
        TransferRetrieveReversalParams as TransferRetrieveReversalParams,
    )
    from stripe.params._transfer_reversal_create_params import (
        TransferReversalCreateParams as TransferReversalCreateParams,
    )
    from stripe.params._transfer_reversal_list_params import (
        TransferReversalListParams as TransferReversalListParams,
    )
    from stripe.params._transfer_reversal_retrieve_params import (
        TransferReversalRetrieveParams as TransferReversalRetrieveParams,
    )
    from stripe.params._transfer_reversal_update_params import (
        TransferReversalUpdateParams as TransferReversalUpdateParams,
    )
    from stripe.params._transfer_update_params import (
        TransferUpdateParams as TransferUpdateParams,
    )
    from stripe.params._webhook_endpoint_create_params import (
        WebhookEndpointCreateParams as WebhookEndpointCreateParams,
    )
    from stripe.params._webhook_endpoint_delete_params import (
        WebhookEndpointDeleteParams as WebhookEndpointDeleteParams,
    )
    from stripe.params._webhook_endpoint_list_params import (
        WebhookEndpointListParams as WebhookEndpointListParams,
    )
    from stripe.params._webhook_endpoint_modify_params import (
        WebhookEndpointModifyParams as WebhookEndpointModifyParams,
    )
    from stripe.params._webhook_endpoint_retrieve_params import (
        WebhookEndpointRetrieveParams as WebhookEndpointRetrieveParams,
    )
    from stripe.params._webhook_endpoint_update_params import (
        WebhookEndpointUpdateParams as WebhookEndpointUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "apps": ("stripe.params.apps", True),
    "billing": ("stripe.params.billing", True),
    "billing_portal": ("stripe.params.billing_portal", True),
    "checkout": ("stripe.params.checkout", True),
    "climate": ("stripe.params.climate", True),
    "entitlements": ("stripe.params.entitlements", True),
    "financial_connections": ("stripe.params.financial_connections", True),
    "forwarding": ("stripe.params.forwarding", True),
    "identity": ("stripe.params.identity", True),
    "issuing": ("stripe.params.issuing", True),
    "radar": ("stripe.params.radar", True),
    "reporting": ("stripe.params.reporting", True),
    "sigma": ("stripe.params.sigma", True),
    "tax": ("stripe.params.tax", True),
    "terminal": ("stripe.params.terminal", True),
    "test_helpers": ("stripe.params.test_helpers", True),
    "treasury": ("stripe.params.treasury", True),
    "AccountCapabilityListParams": (
        "stripe.params._account_capability_list_params",
        False,
    ),
    "AccountCapabilityRetrieveParams": (
        "stripe.params._account_capability_retrieve_params",
        False,
    ),
    "AccountCapabilityUpdateParams": (
        "stripe.params._account_capability_update_params",
        False,
    ),
    "AccountCreateExternalAccountParams": (
        "stripe.params._account_create_external_account_params",
        False,
    ),
    "AccountCreateExternalAccountParamsBankAccount": (
        "stripe.params._account_create_external_account_params",
        False,
    ),
    "AccountCreateExternalAccountParamsCard": (
        "stripe.params._account_create_external_account_params",
        False,
    ),
    "AccountCreateExternalAccountParamsCardToken": (
        "stripe.params._account_create_external_account_params",
        False,
    ),
    "AccountCreateLoginLinkParams": (
        "stripe.params._account_create_login_link_params",
        False,
    ),
    "AccountCreateParams": ("stripe.params._account_create_params", False),
    "AccountCreateParamsBankAccount": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsBusinessProfile": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsBusinessProfileAnnualRevenue": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsBusinessProfileMonthlyEstimatedRevenue": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsBusinessProfileSupportAddress": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilities": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAcssDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAffirmPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAfterpayClearpayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAlmaPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAmazonPayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesAuBecsDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBacsDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBancontactPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBilliePayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBlikPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesBoletoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesCardIssuing": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesCardPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesCartesBancairesPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesCashappPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesCryptoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesEpsPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesFpxPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesGbBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesGiropayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesGrabpayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesIdealPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesIndiaInternationalPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesJcbPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesJpBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesKakaoPayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesKlarnaPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesKonbiniPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesKrCardPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesLegacyPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesLinkPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesMbWayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesMobilepayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesMultibancoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesMxBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesNaverPayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesNzBankAccountBecsDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesOxxoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesP24Payments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPayByBankPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPaycoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPaynowPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPaytoPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPixPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesPromptpayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesRevolutPayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSamsungPayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSatispayPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSepaBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSepaDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSofortPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesSwishPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesTaxReportingUs1099K": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesTaxReportingUs1099Misc": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesTransfers": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesTreasury": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesTwintPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesUsBankAccountAchPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesUsBankTransferPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCapabilitiesZipPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCard": ("stripe.params._account_create_params", False),
    "AccountCreateParamsCardToken": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompany": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyAddress": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyAddressKana": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyAddressKanji": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyDirectorshipDeclaration": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyOwnershipDeclaration": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyRegistrationDate": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyRepresentativeDeclaration": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyVerification": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsCompanyVerificationDocument": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsController": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsControllerFees": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsControllerLosses": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsControllerStripeDashboard": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocuments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsBankAccountOwnershipVerification": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsCompanyLicense": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsCompanyMemorandumOfAssociation": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsCompanyMinisterialDecree": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsCompanyRegistrationVerification": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsCompanyTaxIdVerification": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsProofOfAddress": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsProofOfRegistration": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsProofOfRegistrationSigner": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnership": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsGroups": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividual": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualAddress": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualAddressKana": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualAddressKanji": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualDob": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualRegisteredAddress": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualRelationship": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualVerification": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualVerificationAdditionalDocument": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsIndividualVerificationDocument": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettings": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsBacsDebitPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsBranding": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsCardIssuing": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsCardIssuingTosAcceptance": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsCardPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsCardPaymentsDeclineOn": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsInvoices": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsPayments": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsPayouts": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsPayoutsSchedule": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsTreasury": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsSettingsTreasuryTosAcceptance": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreateParamsTosAcceptance": (
        "stripe.params._account_create_params",
        False,
    ),
    "AccountCreatePersonParams": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsAdditionalTosAcceptances": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsAdditionalTosAcceptancesAccount": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsAddress": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsAddressKana": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsAddressKanji": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsDob": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsDocuments": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsDocumentsCompanyAuthorization": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsDocumentsPassport": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsDocumentsVisa": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsRegisteredAddress": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsRelationship": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsUsCfpbData": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsUsCfpbDataEthnicityDetails": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsUsCfpbDataRaceDetails": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsVerification": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsVerificationAdditionalDocument": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountCreatePersonParamsVerificationDocument": (
        "stripe.params._account_create_person_params",
        False,
    ),
    "AccountDeleteExternalAccountParams": (
        "stripe.params._account_delete_external_account_params",
        False,
    ),
    "AccountDeleteParams": ("stripe.params._account_delete_params", False),
    "AccountDeletePersonParams": (
        "stripe.params._account_delete_person_params",
        False,
    ),
    "AccountExternalAccountCreateParams": (
        "stripe.params._account_external_account_create_params",
        False,
    ),
    "AccountExternalAccountCreateParamsBankAccount": (
        "stripe.params._account_external_account_create_params",
        False,
    ),
    "AccountExternalAccountCreateParamsCard": (
        "stripe.params._account_external_account_create_params",
        False,
    ),
    "AccountExternalAccountCreateParamsCardToken": (
        "stripe.params._account_external_account_create_params",
        False,
    ),
    "AccountExternalAccountDeleteParams": (
        "stripe.params._account_external_account_delete_params",
        False,
    ),
    "AccountExternalAccountListParams": (
        "stripe.params._account_external_account_list_params",
        False,
    ),
    "AccountExternalAccountRetrieveParams": (
        "stripe.params._account_external_account_retrieve_params",
        False,
    ),
    "AccountExternalAccountUpdateParams": (
        "stripe.params._account_external_account_update_params",
        False,
    ),
    "AccountExternalAccountUpdateParamsDocuments": (
        "stripe.params._account_external_account_update_params",
        False,
    ),
    "AccountExternalAccountUpdateParamsDocumentsBankAccountOwnershipVerification": (
        "stripe.params._account_external_account_update_params",
        False,
    ),
    "AccountLinkCreateParams": (
        "stripe.params._account_link_create_params",
        False,
    ),
    "AccountLinkCreateParamsCollectionOptions": (
        "stripe.params._account_link_create_params",
        False,
    ),
    "AccountListCapabilitiesParams": (
        "stripe.params._account_list_capabilities_params",
        False,
    ),
    "AccountListExternalAccountsParams": (
        "stripe.params._account_list_external_accounts_params",
        False,
    ),
    "AccountListParams": ("stripe.params._account_list_params", False),
    "AccountListParamsCreated": ("stripe.params._account_list_params", False),
    "AccountListPersonsParams": (
        "stripe.params._account_list_persons_params",
        False,
    ),
    "AccountListPersonsParamsRelationship": (
        "stripe.params._account_list_persons_params",
        False,
    ),
    "AccountLoginLinkCreateParams": (
        "stripe.params._account_login_link_create_params",
        False,
    ),
    "AccountModifyCapabilityParams": (
        "stripe.params._account_modify_capability_params",
        False,
    ),
    "AccountModifyExternalAccountParams": (
        "stripe.params._account_modify_external_account_params",
        False,
    ),
    "AccountModifyExternalAccountParamsDocuments": (
        "stripe.params._account_modify_external_account_params",
        False,
    ),
    "AccountModifyExternalAccountParamsDocumentsBankAccountOwnershipVerification": (
        "stripe.params._account_modify_external_account_params",
        False,
    ),
    "AccountModifyPersonParams": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsAdditionalTosAcceptances": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsAdditionalTosAcceptancesAccount": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsAddress": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsAddressKana": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsAddressKanji": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsDob": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsDocuments": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsDocumentsCompanyAuthorization": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsDocumentsPassport": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsDocumentsVisa": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsRegisteredAddress": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsRelationship": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsUsCfpbData": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsUsCfpbDataEthnicityDetails": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsUsCfpbDataRaceDetails": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsVerification": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsVerificationAdditionalDocument": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountModifyPersonParamsVerificationDocument": (
        "stripe.params._account_modify_person_params",
        False,
    ),
    "AccountPersonCreateParams": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsAdditionalTosAcceptances": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsAdditionalTosAcceptancesAccount": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsAddress": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsAddressKana": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsAddressKanji": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsDob": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsDocuments": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsDocumentsCompanyAuthorization": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsDocumentsPassport": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsDocumentsVisa": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsRegisteredAddress": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsRelationship": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsUsCfpbData": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsUsCfpbDataEthnicityDetails": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsUsCfpbDataRaceDetails": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsVerification": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsVerificationAdditionalDocument": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonCreateParamsVerificationDocument": (
        "stripe.params._account_person_create_params",
        False,
    ),
    "AccountPersonDeleteParams": (
        "stripe.params._account_person_delete_params",
        False,
    ),
    "AccountPersonListParams": (
        "stripe.params._account_person_list_params",
        False,
    ),
    "AccountPersonListParamsRelationship": (
        "stripe.params._account_person_list_params",
        False,
    ),
    "AccountPersonRetrieveParams": (
        "stripe.params._account_person_retrieve_params",
        False,
    ),
    "AccountPersonUpdateParams": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsAdditionalTosAcceptances": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsAdditionalTosAcceptancesAccount": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsAddress": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsAddressKana": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsAddressKanji": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsDob": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsDocuments": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsDocumentsCompanyAuthorization": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsDocumentsPassport": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsDocumentsVisa": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsRegisteredAddress": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsRelationship": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsUsCfpbData": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsUsCfpbDataEthnicityDetails": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsUsCfpbDataRaceDetails": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsVerification": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsVerificationAdditionalDocument": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonUpdateParamsVerificationDocument": (
        "stripe.params._account_person_update_params",
        False,
    ),
    "AccountPersonsParams": ("stripe.params._account_persons_params", False),
    "AccountPersonsParamsRelationship": (
        "stripe.params._account_persons_params",
        False,
    ),
    "AccountRejectParams": ("stripe.params._account_reject_params", False),
    "AccountRetrieveCapabilityParams": (
        "stripe.params._account_retrieve_capability_params",
        False,
    ),
    "AccountRetrieveCurrentParams": (
        "stripe.params._account_retrieve_current_params",
        False,
    ),
    "AccountRetrieveExternalAccountParams": (
        "stripe.params._account_retrieve_external_account_params",
        False,
    ),
    "AccountRetrieveParams": ("stripe.params._account_retrieve_params", False),
    "AccountRetrievePersonParams": (
        "stripe.params._account_retrieve_person_params",
        False,
    ),
    "AccountSessionCreateParams": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponents": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsAccountManagement": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsAccountManagementFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsAccountOnboarding": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsAccountOnboardingFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsBalances": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsBalancesFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsDisputesList": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsDisputesListFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsDocuments": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsDocumentsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsFinancialAccount": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsFinancialAccountFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsFinancialAccountTransactions": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsFinancialAccountTransactionsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsInstantPayoutsPromotion": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsInstantPayoutsPromotionFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsIssuingCard": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsIssuingCardFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsIssuingCardsList": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsIssuingCardsListFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsNotificationBanner": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsNotificationBannerFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPaymentDetails": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPaymentDetailsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPaymentDisputes": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPaymentDisputesFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayments": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPaymentsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayoutDetails": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayoutDetailsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayouts": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayoutsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayoutsList": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsPayoutsListFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsTaxRegistrations": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsTaxRegistrationsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsTaxSettings": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountSessionCreateParamsComponentsTaxSettingsFeatures": (
        "stripe.params._account_session_create_params",
        False,
    ),
    "AccountUpdateParams": ("stripe.params._account_update_params", False),
    "AccountUpdateParamsBankAccount": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsBusinessProfile": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsBusinessProfileAnnualRevenue": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsBusinessProfileMonthlyEstimatedRevenue": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsBusinessProfileSupportAddress": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilities": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAcssDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAffirmPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAfterpayClearpayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAlmaPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAmazonPayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesAuBecsDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBacsDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBancontactPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBilliePayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBlikPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesBoletoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesCardIssuing": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesCardPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesCartesBancairesPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesCashappPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesCryptoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesEpsPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesFpxPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesGbBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesGiropayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesGrabpayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesIdealPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesIndiaInternationalPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesJcbPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesJpBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesKakaoPayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesKlarnaPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesKonbiniPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesKrCardPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesLegacyPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesLinkPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesMbWayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesMobilepayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesMultibancoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesMxBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesNaverPayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesNzBankAccountBecsDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesOxxoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesP24Payments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPayByBankPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPaycoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPaynowPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPaytoPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPixPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesPromptpayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesRevolutPayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSamsungPayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSatispayPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSepaBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSepaDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSofortPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesSwishPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesTaxReportingUs1099K": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesTaxReportingUs1099Misc": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesTransfers": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesTreasury": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesTwintPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesUsBankAccountAchPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesUsBankTransferPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCapabilitiesZipPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCard": ("stripe.params._account_update_params", False),
    "AccountUpdateParamsCardToken": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompany": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyAddress": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyAddressKana": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyAddressKanji": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyDirectorshipDeclaration": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyOwnershipDeclaration": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyRegistrationDate": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyRepresentativeDeclaration": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyVerification": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsCompanyVerificationDocument": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocuments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsBankAccountOwnershipVerification": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsCompanyLicense": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsCompanyMemorandumOfAssociation": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsCompanyMinisterialDecree": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsCompanyRegistrationVerification": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsCompanyTaxIdVerification": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsProofOfAddress": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsProofOfRegistration": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsProofOfRegistrationSigner": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnership": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsDocumentsProofOfUltimateBeneficialOwnershipSigner": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsGroups": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividual": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualAddress": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualAddressKana": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualAddressKanji": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualDob": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualRegisteredAddress": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualRelationship": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualVerification": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualVerificationAdditionalDocument": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsIndividualVerificationDocument": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettings": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsBacsDebitPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsBranding": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsCardIssuing": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsCardIssuingTosAcceptance": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsCardPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsCardPaymentsDeclineOn": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsInvoices": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsPayments": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsPayouts": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsPayoutsSchedule": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsTreasury": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsSettingsTreasuryTosAcceptance": (
        "stripe.params._account_update_params",
        False,
    ),
    "AccountUpdateParamsTosAcceptance": (
        "stripe.params._account_update_params",
        False,
    ),
    "ApplePayDomainCreateParams": (
        "stripe.params._apple_pay_domain_create_params",
        False,
    ),
    "ApplePayDomainDeleteParams": (
        "stripe.params._apple_pay_domain_delete_params",
        False,
    ),
    "ApplePayDomainListParams": (
        "stripe.params._apple_pay_domain_list_params",
        False,
    ),
    "ApplePayDomainRetrieveParams": (
        "stripe.params._apple_pay_domain_retrieve_params",
        False,
    ),
    "ApplicationFeeCreateRefundParams": (
        "stripe.params._application_fee_create_refund_params",
        False,
    ),
    "ApplicationFeeListParams": (
        "stripe.params._application_fee_list_params",
        False,
    ),
    "ApplicationFeeListParamsCreated": (
        "stripe.params._application_fee_list_params",
        False,
    ),
    "ApplicationFeeListRefundsParams": (
        "stripe.params._application_fee_list_refunds_params",
        False,
    ),
    "ApplicationFeeModifyRefundParams": (
        "stripe.params._application_fee_modify_refund_params",
        False,
    ),
    "ApplicationFeeRefundCreateParams": (
        "stripe.params._application_fee_refund_create_params",
        False,
    ),
    "ApplicationFeeRefundListParams": (
        "stripe.params._application_fee_refund_list_params",
        False,
    ),
    "ApplicationFeeRefundParams": (
        "stripe.params._application_fee_refund_params",
        False,
    ),
    "ApplicationFeeRefundRetrieveParams": (
        "stripe.params._application_fee_refund_retrieve_params",
        False,
    ),
    "ApplicationFeeRefundUpdateParams": (
        "stripe.params._application_fee_refund_update_params",
        False,
    ),
    "ApplicationFeeRetrieveParams": (
        "stripe.params._application_fee_retrieve_params",
        False,
    ),
    "ApplicationFeeRetrieveRefundParams": (
        "stripe.params._application_fee_retrieve_refund_params",
        False,
    ),
    "BalanceRetrieveParams": ("stripe.params._balance_retrieve_params", False),
    "BalanceSettingsModifyParams": (
        "stripe.params._balance_settings_modify_params",
        False,
    ),
    "BalanceSettingsModifyParamsPayments": (
        "stripe.params._balance_settings_modify_params",
        False,
    ),
    "BalanceSettingsModifyParamsPaymentsPayouts": (
        "stripe.params._balance_settings_modify_params",
        False,
    ),
    "BalanceSettingsModifyParamsPaymentsPayoutsSchedule": (
        "stripe.params._balance_settings_modify_params",
        False,
    ),
    "BalanceSettingsModifyParamsPaymentsSettlementTiming": (
        "stripe.params._balance_settings_modify_params",
        False,
    ),
    "BalanceSettingsRetrieveParams": (
        "stripe.params._balance_settings_retrieve_params",
        False,
    ),
    "BalanceSettingsUpdateParams": (
        "stripe.params._balance_settings_update_params",
        False,
    ),
    "BalanceSettingsUpdateParamsPayments": (
        "stripe.params._balance_settings_update_params",
        False,
    ),
    "BalanceSettingsUpdateParamsPaymentsPayouts": (
        "stripe.params._balance_settings_update_params",
        False,
    ),
    "BalanceSettingsUpdateParamsPaymentsPayoutsSchedule": (
        "stripe.params._balance_settings_update_params",
        False,
    ),
    "BalanceSettingsUpdateParamsPaymentsSettlementTiming": (
        "stripe.params._balance_settings_update_params",
        False,
    ),
    "BalanceTransactionListParams": (
        "stripe.params._balance_transaction_list_params",
        False,
    ),
    "BalanceTransactionListParamsCreated": (
        "stripe.params._balance_transaction_list_params",
        False,
    ),
    "BalanceTransactionRetrieveParams": (
        "stripe.params._balance_transaction_retrieve_params",
        False,
    ),
    "BankAccountDeleteParams": (
        "stripe.params._bank_account_delete_params",
        False,
    ),
    "CardDeleteParams": ("stripe.params._card_delete_params", False),
    "ChargeCaptureParams": ("stripe.params._charge_capture_params", False),
    "ChargeCaptureParamsTransferData": (
        "stripe.params._charge_capture_params",
        False,
    ),
    "ChargeCreateParams": ("stripe.params._charge_create_params", False),
    "ChargeCreateParamsDestination": (
        "stripe.params._charge_create_params",
        False,
    ),
    "ChargeCreateParamsRadarOptions": (
        "stripe.params._charge_create_params",
        False,
    ),
    "ChargeCreateParamsShipping": (
        "stripe.params._charge_create_params",
        False,
    ),
    "ChargeCreateParamsShippingAddress": (
        "stripe.params._charge_create_params",
        False,
    ),
    "ChargeCreateParamsTransferData": (
        "stripe.params._charge_create_params",
        False,
    ),
    "ChargeListParams": ("stripe.params._charge_list_params", False),
    "ChargeListParamsCreated": ("stripe.params._charge_list_params", False),
    "ChargeListRefundsParams": (
        "stripe.params._charge_list_refunds_params",
        False,
    ),
    "ChargeModifyParams": ("stripe.params._charge_modify_params", False),
    "ChargeModifyParamsFraudDetails": (
        "stripe.params._charge_modify_params",
        False,
    ),
    "ChargeModifyParamsShipping": (
        "stripe.params._charge_modify_params",
        False,
    ),
    "ChargeModifyParamsShippingAddress": (
        "stripe.params._charge_modify_params",
        False,
    ),
    "ChargeRetrieveParams": ("stripe.params._charge_retrieve_params", False),
    "ChargeRetrieveRefundParams": (
        "stripe.params._charge_retrieve_refund_params",
        False,
    ),
    "ChargeSearchParams": ("stripe.params._charge_search_params", False),
    "ChargeUpdateParams": ("stripe.params._charge_update_params", False),
    "ChargeUpdateParamsFraudDetails": (
        "stripe.params._charge_update_params",
        False,
    ),
    "ChargeUpdateParamsShipping": (
        "stripe.params._charge_update_params",
        False,
    ),
    "ChargeUpdateParamsShippingAddress": (
        "stripe.params._charge_update_params",
        False,
    ),
    "ConfirmationTokenCreateParams": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodData": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAcssDebit": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAffirm": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAlipay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAlma": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAmazonPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBacsDebit": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBancontact": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillie": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillingDetails": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBlik": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataBoleto": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCashapp": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCrypto": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataEps": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataFpx": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataGiropay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataGrabpay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataIdeal": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataInteracPresent": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKakaoPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKlarna": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKonbini": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataKrCard": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataLink": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMbWay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMobilepay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataMultibanco": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataNaverPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataOxxo": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataP24": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayByBank": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayco": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPaynow": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPaypal": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPayto": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPix": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataPromptpay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataRadarOptions": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataRevolutPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSamsungPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSatispay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSepaDebit": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSofort": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataSwish": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataTwint": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataWechatPay": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodDataZip": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptions": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCard": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsShipping": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenCreateParamsShippingAddress": (
        "stripe.params._confirmation_token_create_params",
        False,
    ),
    "ConfirmationTokenRetrieveParams": (
        "stripe.params._confirmation_token_retrieve_params",
        False,
    ),
    "CountrySpecListParams": (
        "stripe.params._country_spec_list_params",
        False,
    ),
    "CountrySpecRetrieveParams": (
        "stripe.params._country_spec_retrieve_params",
        False,
    ),
    "CouponCreateParams": ("stripe.params._coupon_create_params", False),
    "CouponCreateParamsAppliesTo": (
        "stripe.params._coupon_create_params",
        False,
    ),
    "CouponCreateParamsCurrencyOptions": (
        "stripe.params._coupon_create_params",
        False,
    ),
    "CouponDeleteParams": ("stripe.params._coupon_delete_params", False),
    "CouponListParams": ("stripe.params._coupon_list_params", False),
    "CouponListParamsCreated": ("stripe.params._coupon_list_params", False),
    "CouponModifyParams": ("stripe.params._coupon_modify_params", False),
    "CouponModifyParamsCurrencyOptions": (
        "stripe.params._coupon_modify_params",
        False,
    ),
    "CouponRetrieveParams": ("stripe.params._coupon_retrieve_params", False),
    "CouponUpdateParams": ("stripe.params._coupon_update_params", False),
    "CouponUpdateParamsCurrencyOptions": (
        "stripe.params._coupon_update_params",
        False,
    ),
    "CreditNoteCreateParams": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteCreateParamsLine": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteCreateParamsLineTaxAmount": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteCreateParamsRefund": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteCreateParamsRefundPaymentRecordRefund": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteCreateParamsShippingCost": (
        "stripe.params._credit_note_create_params",
        False,
    ),
    "CreditNoteLineItemListParams": (
        "stripe.params._credit_note_line_item_list_params",
        False,
    ),
    "CreditNoteListLinesParams": (
        "stripe.params._credit_note_list_lines_params",
        False,
    ),
    "CreditNoteListParams": ("stripe.params._credit_note_list_params", False),
    "CreditNoteListParamsCreated": (
        "stripe.params._credit_note_list_params",
        False,
    ),
    "CreditNoteModifyParams": (
        "stripe.params._credit_note_modify_params",
        False,
    ),
    "CreditNotePreviewLinesListParams": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesListParamsLine": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesListParamsLineTaxAmount": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesListParamsRefund": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesListParamsRefundPaymentRecordRefund": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesListParamsShippingCost": (
        "stripe.params._credit_note_preview_lines_list_params",
        False,
    ),
    "CreditNotePreviewLinesParams": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewLinesParamsLine": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewLinesParamsLineTaxAmount": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewLinesParamsRefund": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewLinesParamsRefundPaymentRecordRefund": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewLinesParamsShippingCost": (
        "stripe.params._credit_note_preview_lines_params",
        False,
    ),
    "CreditNotePreviewParams": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNotePreviewParamsLine": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNotePreviewParamsLineTaxAmount": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNotePreviewParamsRefund": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNotePreviewParamsRefundPaymentRecordRefund": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNotePreviewParamsShippingCost": (
        "stripe.params._credit_note_preview_params",
        False,
    ),
    "CreditNoteRetrieveParams": (
        "stripe.params._credit_note_retrieve_params",
        False,
    ),
    "CreditNoteUpdateParams": (
        "stripe.params._credit_note_update_params",
        False,
    ),
    "CreditNoteVoidCreditNoteParams": (
        "stripe.params._credit_note_void_credit_note_params",
        False,
    ),
    "CustomerBalanceTransactionCreateParams": (
        "stripe.params._customer_balance_transaction_create_params",
        False,
    ),
    "CustomerBalanceTransactionListParams": (
        "stripe.params._customer_balance_transaction_list_params",
        False,
    ),
    "CustomerBalanceTransactionListParamsCreated": (
        "stripe.params._customer_balance_transaction_list_params",
        False,
    ),
    "CustomerBalanceTransactionRetrieveParams": (
        "stripe.params._customer_balance_transaction_retrieve_params",
        False,
    ),
    "CustomerBalanceTransactionUpdateParams": (
        "stripe.params._customer_balance_transaction_update_params",
        False,
    ),
    "CustomerCashBalanceRetrieveParams": (
        "stripe.params._customer_cash_balance_retrieve_params",
        False,
    ),
    "CustomerCashBalanceTransactionListParams": (
        "stripe.params._customer_cash_balance_transaction_list_params",
        False,
    ),
    "CustomerCashBalanceTransactionRetrieveParams": (
        "stripe.params._customer_cash_balance_transaction_retrieve_params",
        False,
    ),
    "CustomerCashBalanceUpdateParams": (
        "stripe.params._customer_cash_balance_update_params",
        False,
    ),
    "CustomerCashBalanceUpdateParamsSettings": (
        "stripe.params._customer_cash_balance_update_params",
        False,
    ),
    "CustomerCreateBalanceTransactionParams": (
        "stripe.params._customer_create_balance_transaction_params",
        False,
    ),
    "CustomerCreateFundingInstructionsParams": (
        "stripe.params._customer_create_funding_instructions_params",
        False,
    ),
    "CustomerCreateFundingInstructionsParamsBankTransfer": (
        "stripe.params._customer_create_funding_instructions_params",
        False,
    ),
    "CustomerCreateFundingInstructionsParamsBankTransferEuBankTransfer": (
        "stripe.params._customer_create_funding_instructions_params",
        False,
    ),
    "CustomerCreateParams": ("stripe.params._customer_create_params", False),
    "CustomerCreateParamsAddress": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsCashBalance": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsCashBalanceSettings": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsInvoiceSettings": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsInvoiceSettingsCustomField": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsInvoiceSettingsRenderingOptions": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsShipping": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsShippingAddress": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsTax": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateParamsTaxIdDatum": (
        "stripe.params._customer_create_params",
        False,
    ),
    "CustomerCreateSourceParams": (
        "stripe.params._customer_create_source_params",
        False,
    ),
    "CustomerCreateTaxIdParams": (
        "stripe.params._customer_create_tax_id_params",
        False,
    ),
    "CustomerDeleteDiscountParams": (
        "stripe.params._customer_delete_discount_params",
        False,
    ),
    "CustomerDeleteParams": ("stripe.params._customer_delete_params", False),
    "CustomerDeleteSourceParams": (
        "stripe.params._customer_delete_source_params",
        False,
    ),
    "CustomerDeleteTaxIdParams": (
        "stripe.params._customer_delete_tax_id_params",
        False,
    ),
    "CustomerFundCashBalanceParams": (
        "stripe.params._customer_fund_cash_balance_params",
        False,
    ),
    "CustomerFundingInstructionsCreateParams": (
        "stripe.params._customer_funding_instructions_create_params",
        False,
    ),
    "CustomerFundingInstructionsCreateParamsBankTransfer": (
        "stripe.params._customer_funding_instructions_create_params",
        False,
    ),
    "CustomerFundingInstructionsCreateParamsBankTransferEuBankTransfer": (
        "stripe.params._customer_funding_instructions_create_params",
        False,
    ),
    "CustomerListBalanceTransactionsParams": (
        "stripe.params._customer_list_balance_transactions_params",
        False,
    ),
    "CustomerListBalanceTransactionsParamsCreated": (
        "stripe.params._customer_list_balance_transactions_params",
        False,
    ),
    "CustomerListCashBalanceTransactionsParams": (
        "stripe.params._customer_list_cash_balance_transactions_params",
        False,
    ),
    "CustomerListParams": ("stripe.params._customer_list_params", False),
    "CustomerListParamsCreated": (
        "stripe.params._customer_list_params",
        False,
    ),
    "CustomerListPaymentMethodsParams": (
        "stripe.params._customer_list_payment_methods_params",
        False,
    ),
    "CustomerListSourcesParams": (
        "stripe.params._customer_list_sources_params",
        False,
    ),
    "CustomerListTaxIdsParams": (
        "stripe.params._customer_list_tax_ids_params",
        False,
    ),
    "CustomerModifyBalanceTransactionParams": (
        "stripe.params._customer_modify_balance_transaction_params",
        False,
    ),
    "CustomerModifyCashBalanceParams": (
        "stripe.params._customer_modify_cash_balance_params",
        False,
    ),
    "CustomerModifyCashBalanceParamsSettings": (
        "stripe.params._customer_modify_cash_balance_params",
        False,
    ),
    "CustomerModifyParams": ("stripe.params._customer_modify_params", False),
    "CustomerModifyParamsAddress": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsCashBalance": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsCashBalanceSettings": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsInvoiceSettings": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsInvoiceSettingsCustomField": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsInvoiceSettingsRenderingOptions": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsShipping": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsShippingAddress": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifyParamsTax": (
        "stripe.params._customer_modify_params",
        False,
    ),
    "CustomerModifySourceParams": (
        "stripe.params._customer_modify_source_params",
        False,
    ),
    "CustomerModifySourceParamsOwner": (
        "stripe.params._customer_modify_source_params",
        False,
    ),
    "CustomerModifySourceParamsOwnerAddress": (
        "stripe.params._customer_modify_source_params",
        False,
    ),
    "CustomerPaymentMethodListParams": (
        "stripe.params._customer_payment_method_list_params",
        False,
    ),
    "CustomerPaymentMethodRetrieveParams": (
        "stripe.params._customer_payment_method_retrieve_params",
        False,
    ),
    "CustomerPaymentSourceCreateParams": (
        "stripe.params._customer_payment_source_create_params",
        False,
    ),
    "CustomerPaymentSourceDeleteParams": (
        "stripe.params._customer_payment_source_delete_params",
        False,
    ),
    "CustomerPaymentSourceListParams": (
        "stripe.params._customer_payment_source_list_params",
        False,
    ),
    "CustomerPaymentSourceRetrieveParams": (
        "stripe.params._customer_payment_source_retrieve_params",
        False,
    ),
    "CustomerPaymentSourceUpdateParams": (
        "stripe.params._customer_payment_source_update_params",
        False,
    ),
    "CustomerPaymentSourceUpdateParamsOwner": (
        "stripe.params._customer_payment_source_update_params",
        False,
    ),
    "CustomerPaymentSourceUpdateParamsOwnerAddress": (
        "stripe.params._customer_payment_source_update_params",
        False,
    ),
    "CustomerPaymentSourceVerifyParams": (
        "stripe.params._customer_payment_source_verify_params",
        False,
    ),
    "CustomerRetrieveBalanceTransactionParams": (
        "stripe.params._customer_retrieve_balance_transaction_params",
        False,
    ),
    "CustomerRetrieveCashBalanceParams": (
        "stripe.params._customer_retrieve_cash_balance_params",
        False,
    ),
    "CustomerRetrieveCashBalanceTransactionParams": (
        "stripe.params._customer_retrieve_cash_balance_transaction_params",
        False,
    ),
    "CustomerRetrieveParams": (
        "stripe.params._customer_retrieve_params",
        False,
    ),
    "CustomerRetrievePaymentMethodParams": (
        "stripe.params._customer_retrieve_payment_method_params",
        False,
    ),
    "CustomerRetrieveSourceParams": (
        "stripe.params._customer_retrieve_source_params",
        False,
    ),
    "CustomerRetrieveTaxIdParams": (
        "stripe.params._customer_retrieve_tax_id_params",
        False,
    ),
    "CustomerSearchParams": ("stripe.params._customer_search_params", False),
    "CustomerSessionCreateParams": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponents": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsBuyButton": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsCustomerSheet": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsCustomerSheetFeatures": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsMobilePaymentElement": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsMobilePaymentElementFeatures": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsPaymentElement": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsPaymentElementFeatures": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerSessionCreateParamsComponentsPricingTable": (
        "stripe.params._customer_session_create_params",
        False,
    ),
    "CustomerTaxIdCreateParams": (
        "stripe.params._customer_tax_id_create_params",
        False,
    ),
    "CustomerTaxIdDeleteParams": (
        "stripe.params._customer_tax_id_delete_params",
        False,
    ),
    "CustomerTaxIdListParams": (
        "stripe.params._customer_tax_id_list_params",
        False,
    ),
    "CustomerTaxIdRetrieveParams": (
        "stripe.params._customer_tax_id_retrieve_params",
        False,
    ),
    "CustomerUpdateParams": ("stripe.params._customer_update_params", False),
    "CustomerUpdateParamsAddress": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsCashBalance": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsCashBalanceSettings": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsInvoiceSettings": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsInvoiceSettingsCustomField": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsInvoiceSettingsRenderingOptions": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsShipping": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsShippingAddress": (
        "stripe.params._customer_update_params",
        False,
    ),
    "CustomerUpdateParamsTax": (
        "stripe.params._customer_update_params",
        False,
    ),
    "DisputeCloseParams": ("stripe.params._dispute_close_params", False),
    "DisputeListParams": ("stripe.params._dispute_list_params", False),
    "DisputeListParamsCreated": ("stripe.params._dispute_list_params", False),
    "DisputeModifyParams": ("stripe.params._dispute_modify_params", False),
    "DisputeModifyParamsEvidence": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidence": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceEnhancedEvidenceVisaCompliance": (
        "stripe.params._dispute_modify_params",
        False,
    ),
    "DisputeRetrieveParams": ("stripe.params._dispute_retrieve_params", False),
    "DisputeUpdateParams": ("stripe.params._dispute_update_params", False),
    "DisputeUpdateParamsEvidence": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidence": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransaction": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3DisputedTransactionShippingAddress": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransaction": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompellingEvidence3PriorUndisputedTransactionShippingAddress": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceEnhancedEvidenceVisaCompliance": (
        "stripe.params._dispute_update_params",
        False,
    ),
    "EphemeralKeyCreateParams": (
        "stripe.params._ephemeral_key_create_params",
        False,
    ),
    "EphemeralKeyDeleteParams": (
        "stripe.params._ephemeral_key_delete_params",
        False,
    ),
    "EventListParams": ("stripe.params._event_list_params", False),
    "EventListParamsCreated": ("stripe.params._event_list_params", False),
    "EventRetrieveParams": ("stripe.params._event_retrieve_params", False),
    "ExchangeRateListParams": (
        "stripe.params._exchange_rate_list_params",
        False,
    ),
    "ExchangeRateRetrieveParams": (
        "stripe.params._exchange_rate_retrieve_params",
        False,
    ),
    "FileCreateParams": ("stripe.params._file_create_params", False),
    "FileCreateParamsFileLinkData": (
        "stripe.params._file_create_params",
        False,
    ),
    "FileLinkCreateParams": ("stripe.params._file_link_create_params", False),
    "FileLinkListParams": ("stripe.params._file_link_list_params", False),
    "FileLinkListParamsCreated": (
        "stripe.params._file_link_list_params",
        False,
    ),
    "FileLinkModifyParams": ("stripe.params._file_link_modify_params", False),
    "FileLinkRetrieveParams": (
        "stripe.params._file_link_retrieve_params",
        False,
    ),
    "FileLinkUpdateParams": ("stripe.params._file_link_update_params", False),
    "FileListParams": ("stripe.params._file_list_params", False),
    "FileListParamsCreated": ("stripe.params._file_list_params", False),
    "FileRetrieveParams": ("stripe.params._file_retrieve_params", False),
    "InvoiceAddLinesParams": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLine": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLineDiscount": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLinePeriod": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLinePriceData": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLinePriceDataProductData": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLinePricing": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLineTaxAmount": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAddLinesParamsLineTaxAmountTaxRateData": (
        "stripe.params._invoice_add_lines_params",
        False,
    ),
    "InvoiceAttachPaymentParams": (
        "stripe.params._invoice_attach_payment_params",
        False,
    ),
    "InvoiceCreateParams": ("stripe.params._invoice_create_params", False),
    "InvoiceCreateParamsAutomaticTax": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsAutomaticTaxLiability": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsCustomField": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsDiscount": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsFromInvoice": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsIssuer": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettings": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallments": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsRendering": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsRenderingPdf": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCost": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateData": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimate": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMaximum": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateDataDeliveryEstimateMinimum": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateDataFixedAmount": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingDetails": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsShippingDetailsAddress": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreateParamsTransferData": (
        "stripe.params._invoice_create_params",
        False,
    ),
    "InvoiceCreatePreviewParams": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsAutomaticTax": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsAutomaticTaxLiability": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetails": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetailsAddress": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetailsShipping": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetailsShippingAddress": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetailsTax": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsCustomerDetailsTaxId": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsInvoiceItem": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsInvoiceItemDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsInvoiceItemPeriod": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsInvoiceItemPriceData": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsIssuer": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetails": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsBillingMode": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsBillingModeFlexible": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhase": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItem": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriod": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodEnd": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodStart": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPriceData": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTax": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTaxLiability": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseBillingThresholds": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseDuration": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettings": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettingsIssuer": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseItem": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemBillingThresholds": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceData": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceDataRecurring": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsScheduleDetailsPhaseTransferData": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetails": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsBillingMode": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsBillingModeFlexible": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsItem": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsItemBillingThresholds": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsItemDiscount": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceData": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceDataRecurring": (
        "stripe.params._invoice_create_preview_params",
        False,
    ),
    "InvoiceDeleteParams": ("stripe.params._invoice_delete_params", False),
    "InvoiceFinalizeInvoiceParams": (
        "stripe.params._invoice_finalize_invoice_params",
        False,
    ),
    "InvoiceItemCreateParams": (
        "stripe.params._invoice_item_create_params",
        False,
    ),
    "InvoiceItemCreateParamsDiscount": (
        "stripe.params._invoice_item_create_params",
        False,
    ),
    "InvoiceItemCreateParamsPeriod": (
        "stripe.params._invoice_item_create_params",
        False,
    ),
    "InvoiceItemCreateParamsPriceData": (
        "stripe.params._invoice_item_create_params",
        False,
    ),
    "InvoiceItemCreateParamsPricing": (
        "stripe.params._invoice_item_create_params",
        False,
    ),
    "InvoiceItemDeleteParams": (
        "stripe.params._invoice_item_delete_params",
        False,
    ),
    "InvoiceItemListParams": (
        "stripe.params._invoice_item_list_params",
        False,
    ),
    "InvoiceItemListParamsCreated": (
        "stripe.params._invoice_item_list_params",
        False,
    ),
    "InvoiceItemModifyParams": (
        "stripe.params._invoice_item_modify_params",
        False,
    ),
    "InvoiceItemModifyParamsDiscount": (
        "stripe.params._invoice_item_modify_params",
        False,
    ),
    "InvoiceItemModifyParamsPeriod": (
        "stripe.params._invoice_item_modify_params",
        False,
    ),
    "InvoiceItemModifyParamsPriceData": (
        "stripe.params._invoice_item_modify_params",
        False,
    ),
    "InvoiceItemModifyParamsPricing": (
        "stripe.params._invoice_item_modify_params",
        False,
    ),
    "InvoiceItemRetrieveParams": (
        "stripe.params._invoice_item_retrieve_params",
        False,
    ),
    "InvoiceItemUpdateParams": (
        "stripe.params._invoice_item_update_params",
        False,
    ),
    "InvoiceItemUpdateParamsDiscount": (
        "stripe.params._invoice_item_update_params",
        False,
    ),
    "InvoiceItemUpdateParamsPeriod": (
        "stripe.params._invoice_item_update_params",
        False,
    ),
    "InvoiceItemUpdateParamsPriceData": (
        "stripe.params._invoice_item_update_params",
        False,
    ),
    "InvoiceItemUpdateParamsPricing": (
        "stripe.params._invoice_item_update_params",
        False,
    ),
    "InvoiceLineItemListParams": (
        "stripe.params._invoice_line_item_list_params",
        False,
    ),
    "InvoiceLineItemUpdateParams": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsDiscount": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsPeriod": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsPriceData": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsPriceDataProductData": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsPricing": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsTaxAmount": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceLineItemUpdateParamsTaxAmountTaxRateData": (
        "stripe.params._invoice_line_item_update_params",
        False,
    ),
    "InvoiceListLinesParams": (
        "stripe.params._invoice_list_lines_params",
        False,
    ),
    "InvoiceListParams": ("stripe.params._invoice_list_params", False),
    "InvoiceListParamsCreated": ("stripe.params._invoice_list_params", False),
    "InvoiceListParamsDueDate": ("stripe.params._invoice_list_params", False),
    "InvoiceMarkUncollectibleParams": (
        "stripe.params._invoice_mark_uncollectible_params",
        False,
    ),
    "InvoiceModifyParams": ("stripe.params._invoice_modify_params", False),
    "InvoiceModifyParamsAutomaticTax": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsAutomaticTaxLiability": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsCustomField": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsDiscount": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsIssuer": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettings": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallments": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsRendering": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsRenderingPdf": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCost": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateData": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimate": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMaximum": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateDataDeliveryEstimateMinimum": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateDataFixedAmount": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingCostShippingRateDataFixedAmountCurrencyOptions": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingDetails": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsShippingDetailsAddress": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoiceModifyParamsTransferData": (
        "stripe.params._invoice_modify_params",
        False,
    ),
    "InvoicePayParams": ("stripe.params._invoice_pay_params", False),
    "InvoicePaymentListParams": (
        "stripe.params._invoice_payment_list_params",
        False,
    ),
    "InvoicePaymentListParamsCreated": (
        "stripe.params._invoice_payment_list_params",
        False,
    ),
    "InvoicePaymentListParamsPayment": (
        "stripe.params._invoice_payment_list_params",
        False,
    ),
    "InvoicePaymentRetrieveParams": (
        "stripe.params._invoice_payment_retrieve_params",
        False,
    ),
    "InvoiceRemoveLinesParams": (
        "stripe.params._invoice_remove_lines_params",
        False,
    ),
    "InvoiceRemoveLinesParamsLine": (
        "stripe.params._invoice_remove_lines_params",
        False,
    ),
    "InvoiceRenderingTemplateArchiveParams": (
        "stripe.params._invoice_rendering_template_archive_params",
        False,
    ),
    "InvoiceRenderingTemplateListParams": (
        "stripe.params._invoice_rendering_template_list_params",
        False,
    ),
    "InvoiceRenderingTemplateRetrieveParams": (
        "stripe.params._invoice_rendering_template_retrieve_params",
        False,
    ),
    "InvoiceRenderingTemplateUnarchiveParams": (
        "stripe.params._invoice_rendering_template_unarchive_params",
        False,
    ),
    "InvoiceRetrieveParams": ("stripe.params._invoice_retrieve_params", False),
    "InvoiceSearchParams": ("stripe.params._invoice_search_params", False),
    "InvoiceSendInvoiceParams": (
        "stripe.params._invoice_send_invoice_params",
        False,
    ),
    "InvoiceUpdateLinesParams": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLine": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLineDiscount": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLinePeriod": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLinePriceData": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLinePriceDataProductData": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLinePricing": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLineTaxAmount": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateLinesParamsLineTaxAmountTaxRateData": (
        "stripe.params._invoice_update_lines_params",
        False,
    ),
    "InvoiceUpdateParams": ("stripe.params._invoice_update_params", False),
    "InvoiceUpdateParamsAutomaticTax": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsAutomaticTaxLiability": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsCustomField": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsDiscount": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsIssuer": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettings": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallments": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsRendering": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsRenderingPdf": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCost": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateData": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimate": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMaximum": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMinimum": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateDataFixedAmount": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingDetails": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsShippingDetailsAddress": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceUpdateParamsTransferData": (
        "stripe.params._invoice_update_params",
        False,
    ),
    "InvoiceVoidInvoiceParams": (
        "stripe.params._invoice_void_invoice_params",
        False,
    ),
    "MandateRetrieveParams": ("stripe.params._mandate_retrieve_params", False),
    "PaymentAttemptRecordListParams": (
        "stripe.params._payment_attempt_record_list_params",
        False,
    ),
    "PaymentAttemptRecordRetrieveParams": (
        "stripe.params._payment_attempt_record_retrieve_params",
        False,
    ),
    "PaymentIntentAmountDetailsLineItemListParams": (
        "stripe.params._payment_intent_amount_details_line_item_list_params",
        False,
    ),
    "PaymentIntentApplyCustomerBalanceParams": (
        "stripe.params._payment_intent_apply_customer_balance_params",
        False,
    ),
    "PaymentIntentCancelParams": (
        "stripe.params._payment_intent_cancel_params",
        False,
    ),
    "PaymentIntentCaptureParams": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetails": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsAmountDetailsTax": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsHooks": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsHooksInputs": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsHooksInputsTax": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsPaymentDetails": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentCaptureParamsTransferData": (
        "stripe.params._payment_intent_capture_params",
        False,
    ),
    "PaymentIntentConfirmParams": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetails": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsAmountDetailsTax": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsHooks": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsHooksInputs": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsHooksInputsTax": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsMandateData": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsMandateDataCustomerAcceptance": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOffline": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsMandateDataCustomerAcceptanceOnline": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentDetails": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodData": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAcssDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAffirm": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAlipay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAlma": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAmazonPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBacsDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBancontact": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBillie": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBillingDetails": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBlik": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataBoleto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataCashapp": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataCrypto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataEps": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataFpx": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataGiropay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataGrabpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataIdeal": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataInteracPresent": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataKakaoPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataKlarna": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataKonbini": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataKrCard": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataLink": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataMbWay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataMobilepay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataMultibanco": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataNaverPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataOxxo": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataP24": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPayByBank": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPayco": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPaynow": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPaypal": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPayto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPix": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataPromptpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataRadarOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataRevolutPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataSamsungPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataSatispay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataSepaDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataSofort": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataSwish": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataTwint": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataWechatPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodDataZip": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAffirm": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAfterpayClearpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAlipay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAlma": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsAuBecsDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBancontact": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBillie": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBlik": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsBoleto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardPresentRouting": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCashapp": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCrypto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsEps": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsFpx": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsGiropay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsGrabpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsIdeal": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsInteracPresent": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKakaoPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKonbini": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsKrCard": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsLink": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsMbWay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsMobilepay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsMultibanco": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsNaverPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsNzBankAccount": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsOxxo": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsP24": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPayByBank": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPayco": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPaynow": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPayto": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPix": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsPromptpay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsRevolutPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSamsungPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSatispay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSofort": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsSwish": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsTwint": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsWechatPay": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsPaymentMethodOptionsZip": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsRadarOptions": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsShipping": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentConfirmParamsShippingAddress": (
        "stripe.params._payment_intent_confirm_params",
        False,
    ),
    "PaymentIntentCreateParams": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetails": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAmountDetailsTax": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsAutomaticPaymentMethods": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsHooks": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsHooksInputs": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsHooksInputsTax": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsMandateData": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsMandateDataCustomerAcceptance": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsMandateDataCustomerAcceptanceOffline": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsMandateDataCustomerAcceptanceOnline": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentDetails": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodData": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAcssDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAffirm": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAlipay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAlma": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAmazonPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBacsDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBancontact": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBillie": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBillingDetails": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBlik": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataBoleto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataCashapp": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataCrypto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataEps": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataFpx": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataGiropay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataGrabpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataIdeal": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataInteracPresent": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataKakaoPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataKlarna": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataKonbini": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataKrCard": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataLink": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataMbWay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataMobilepay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataMultibanco": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataNaverPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataOxxo": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataP24": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPayByBank": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPayco": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPaynow": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPaypal": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPayto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPix": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataPromptpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataRadarOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataRevolutPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataSamsungPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataSatispay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataSepaDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataSofort": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataSwish": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataTwint": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataWechatPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodDataZip": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAffirm": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAfterpayClearpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAlipay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAlma": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsAuBecsDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBancontact": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBillie": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBlik": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsBoleto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardPresentRouting": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCashapp": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCrypto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsEps": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsFpx": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsGiropay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsGrabpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsIdeal": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsInteracPresent": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKakaoPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKonbini": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsKrCard": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsLink": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsMbWay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsMobilepay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsMultibanco": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsNaverPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsNzBankAccount": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsOxxo": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsP24": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPayByBank": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPayco": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPaynow": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPayto": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPix": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsPromptpay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsRevolutPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSamsungPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSatispay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSofort": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsSwish": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsTwint": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsWechatPay": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsPaymentMethodOptionsZip": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsRadarOptions": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsShipping": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsShippingAddress": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentCreateParamsTransferData": (
        "stripe.params._payment_intent_create_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParams": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetails": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsAmountDetailsTax": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsHooks": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsHooksInputs": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsHooksInputsTax": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsPaymentDetails": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentIncrementAuthorizationParamsTransferData": (
        "stripe.params._payment_intent_increment_authorization_params",
        False,
    ),
    "PaymentIntentListAmountDetailsLineItemsParams": (
        "stripe.params._payment_intent_list_amount_details_line_items_params",
        False,
    ),
    "PaymentIntentListParams": (
        "stripe.params._payment_intent_list_params",
        False,
    ),
    "PaymentIntentListParamsCreated": (
        "stripe.params._payment_intent_list_params",
        False,
    ),
    "PaymentIntentModifyParams": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetails": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsAmountDetailsTax": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsHooks": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsHooksInputs": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsHooksInputsTax": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentDetails": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodData": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAcssDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAffirm": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAlipay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAlma": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAmazonPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBacsDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBancontact": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBillie": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBillingDetails": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBlik": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataBoleto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataCashapp": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataCrypto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataEps": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataFpx": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataGiropay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataGrabpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataIdeal": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataInteracPresent": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataKakaoPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataKlarna": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataKonbini": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataKrCard": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataLink": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataMbWay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataMobilepay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataMultibanco": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataNaverPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataOxxo": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataP24": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPayByBank": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPayco": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPaynow": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPaypal": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPayto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPix": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataPromptpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataRadarOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataRevolutPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataSamsungPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataSatispay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataSepaDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataSofort": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataSwish": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataTwint": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataWechatPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodDataZip": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAffirm": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAfterpayClearpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAlipay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAlma": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsAuBecsDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBancontact": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBillie": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBlik": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsBoleto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardPresentRouting": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCashapp": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCrypto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsEps": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsFpx": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsGiropay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsGrabpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsIdeal": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsInteracPresent": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKakaoPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKonbini": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsKrCard": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsLink": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsMbWay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsMobilepay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsMultibanco": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsNaverPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsNzBankAccount": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsOxxo": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsP24": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPayByBank": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPayco": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPaynow": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPayto": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPix": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsPromptpay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsRevolutPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSamsungPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSatispay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSofort": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsSwish": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsTwint": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsWechatPay": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsPaymentMethodOptionsZip": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsShipping": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsShippingAddress": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentModifyParamsTransferData": (
        "stripe.params._payment_intent_modify_params",
        False,
    ),
    "PaymentIntentRetrieveParams": (
        "stripe.params._payment_intent_retrieve_params",
        False,
    ),
    "PaymentIntentSearchParams": (
        "stripe.params._payment_intent_search_params",
        False,
    ),
    "PaymentIntentUpdateParams": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetails": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItem": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsLineItemTax": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsShipping": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsAmountDetailsTax": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsHooks": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsHooksInputs": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsHooksInputsTax": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentDetails": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodData": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAcssDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAffirm": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAlipay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAlma": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAmazonPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBacsDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBancontact": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBillie": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBillingDetails": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBlik": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataBoleto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataCashapp": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataCrypto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataEps": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataFpx": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataGiropay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataGrabpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataIdeal": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataInteracPresent": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataKakaoPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataKlarna": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataKonbini": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataKrCard": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataLink": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataMbWay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataMobilepay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataMultibanco": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataNaverPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataOxxo": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataP24": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPayByBank": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPayco": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPaynow": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPaypal": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPayto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPix": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataPromptpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataRadarOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataRevolutPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataSamsungPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataSatispay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataSepaDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataSofort": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataSwish": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataTwint": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataWechatPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodDataZip": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAffirm": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAfterpayClearpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAlipay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAlma": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsAuBecsDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBancontact": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBillie": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBlik": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsBoleto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCard": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallments": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardInstallmentsPlan": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardPresentRouting": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCashapp": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCrypto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsEps": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsFpx": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsGiropay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsGrabpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsIdeal": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsInteracPresent": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKakaoPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKlarna": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKonbini": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsKrCard": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsLink": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsMbWay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsMobilepay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsMultibanco": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsNaverPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsNzBankAccount": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsOxxo": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsP24": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPayByBank": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPayco": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPaynow": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPaypal": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPayto": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPix": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsPromptpay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsRevolutPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSamsungPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSatispay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSofort": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsSwish": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsTwint": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsWechatPay": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsPaymentMethodOptionsZip": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsShipping": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsShippingAddress": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentUpdateParamsTransferData": (
        "stripe.params._payment_intent_update_params",
        False,
    ),
    "PaymentIntentVerifyMicrodepositsParams": (
        "stripe.params._payment_intent_verify_microdeposits_params",
        False,
    ),
    "PaymentLinkCreateParams": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsAfterCompletion": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsAfterCompletionHostedConfirmation": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsAfterCompletionRedirect": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsAutomaticTax": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsAutomaticTaxLiability": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsConsentCollection": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsConsentCollectionPaymentMethodReuseAgreement": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomField": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomFieldDropdown": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomFieldDropdownOption": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomFieldLabel": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomFieldNumeric": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomFieldText": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomText": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomTextAfterSubmit": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomTextShippingAddress": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomTextSubmit": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsCustomTextTermsOfServiceAcceptance": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsInvoiceCreation": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsInvoiceCreationInvoiceData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsInvoiceCreationInvoiceDataCustomField": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsInvoiceCreationInvoiceDataIssuer": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsInvoiceCreationInvoiceDataRenderingOptions": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsLineItem": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsLineItemAdjustableQuantity": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsLineItemPriceData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsLineItemPriceDataProductData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsLineItemPriceDataRecurring": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsNameCollection": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsNameCollectionBusiness": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsNameCollectionIndividual": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsOptionalItem": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsOptionalItemAdjustableQuantity": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsPaymentIntentData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsPhoneNumberCollection": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsRestrictions": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsRestrictionsCompletedSessions": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsShippingAddressCollection": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsShippingOption": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsSubscriptionData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsSubscriptionDataInvoiceSettings": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsSubscriptionDataInvoiceSettingsIssuer": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsSubscriptionDataTrialSettings": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsSubscriptionDataTrialSettingsEndBehavior": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsTaxIdCollection": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkCreateParamsTransferData": (
        "stripe.params._payment_link_create_params",
        False,
    ),
    "PaymentLinkLineItemListParams": (
        "stripe.params._payment_link_line_item_list_params",
        False,
    ),
    "PaymentLinkListLineItemsParams": (
        "stripe.params._payment_link_list_line_items_params",
        False,
    ),
    "PaymentLinkListParams": (
        "stripe.params._payment_link_list_params",
        False,
    ),
    "PaymentLinkModifyParams": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsAfterCompletion": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsAfterCompletionHostedConfirmation": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsAfterCompletionRedirect": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsAutomaticTax": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsAutomaticTaxLiability": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomField": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomFieldDropdown": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomFieldDropdownOption": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomFieldLabel": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomFieldNumeric": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomFieldText": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomText": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomTextAfterSubmit": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomTextShippingAddress": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomTextSubmit": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsCustomTextTermsOfServiceAcceptance": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsInvoiceCreation": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsInvoiceCreationInvoiceData": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsInvoiceCreationInvoiceDataCustomField": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsInvoiceCreationInvoiceDataIssuer": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsInvoiceCreationInvoiceDataRenderingOptions": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsLineItem": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsLineItemAdjustableQuantity": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsNameCollection": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsNameCollectionBusiness": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsNameCollectionIndividual": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsOptionalItem": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsOptionalItemAdjustableQuantity": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsPaymentIntentData": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsPhoneNumberCollection": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsRestrictions": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsRestrictionsCompletedSessions": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsShippingAddressCollection": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsSubscriptionData": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsSubscriptionDataInvoiceSettings": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsSubscriptionDataInvoiceSettingsIssuer": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsSubscriptionDataTrialSettings": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsSubscriptionDataTrialSettingsEndBehavior": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkModifyParamsTaxIdCollection": (
        "stripe.params._payment_link_modify_params",
        False,
    ),
    "PaymentLinkRetrieveParams": (
        "stripe.params._payment_link_retrieve_params",
        False,
    ),
    "PaymentLinkUpdateParams": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsAfterCompletion": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsAfterCompletionHostedConfirmation": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsAfterCompletionRedirect": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsAutomaticTax": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsAutomaticTaxLiability": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomField": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomFieldDropdown": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomFieldDropdownOption": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomFieldLabel": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomFieldNumeric": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomFieldText": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomText": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomTextAfterSubmit": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomTextShippingAddress": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomTextSubmit": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsCustomTextTermsOfServiceAcceptance": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsInvoiceCreation": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsInvoiceCreationInvoiceData": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsInvoiceCreationInvoiceDataCustomField": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsInvoiceCreationInvoiceDataIssuer": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsInvoiceCreationInvoiceDataRenderingOptions": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsLineItem": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsLineItemAdjustableQuantity": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsNameCollection": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsNameCollectionBusiness": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsNameCollectionIndividual": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsOptionalItem": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsOptionalItemAdjustableQuantity": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsPaymentIntentData": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsPhoneNumberCollection": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsRestrictions": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsRestrictionsCompletedSessions": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsShippingAddressCollection": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsSubscriptionData": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsSubscriptionDataInvoiceSettings": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsSubscriptionDataInvoiceSettingsIssuer": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsSubscriptionDataTrialSettings": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsSubscriptionDataTrialSettingsEndBehavior": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentLinkUpdateParamsTaxIdCollection": (
        "stripe.params._payment_link_update_params",
        False,
    ),
    "PaymentMethodAttachParams": (
        "stripe.params._payment_method_attach_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParams": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAcssDebit": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAcssDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAffirm": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAffirmDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAfterpayClearpay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAfterpayClearpayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAlipay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAlipayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAlma": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAlmaDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAmazonPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAmazonPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsApplePay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsApplePayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsApplePayLater": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsApplePayLaterDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAuBecsDebit": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsAuBecsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBacsDebit": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBacsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBancontact": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBancontactDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBillie": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBillieDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBlik": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBlikDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBoleto": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsBoletoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCard": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCardDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCartesBancaires": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCartesBancairesDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCashapp": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCashappDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCrypto": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCryptoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCustomerBalance": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsCustomerBalanceDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsEps": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsEpsDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsFpx": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsFpxDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsFrMealVoucherConecs": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsFrMealVoucherConecsDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGiropay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGiropayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGooglePay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGooglePayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGrabpay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsGrabpayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsIdeal": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsIdealDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsJcb": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsJcbDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKakaoPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKakaoPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKlarna": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKlarnaDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKonbini": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKonbiniDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKrCard": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsKrCardDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsLink": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsLinkDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMbWay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMbWayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMobilepay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMobilepayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMultibanco": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsMultibancoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsNaverPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsNaverPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsNzBankAccount": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsNzBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsOxxo": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsOxxoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsP24": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsP24DisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPayByBank": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPayByBankDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPayco": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaycoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaynow": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaynowDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaypal": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaypalDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPayto": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPaytoDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPix": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPixDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPromptpay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsPromptpayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsRevolutPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsRevolutPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSamsungPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSamsungPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSatispay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSatispayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSepaDebit": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSepaDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSofort": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSofortDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSwish": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsSwishDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsTwint": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsTwintDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsUsBankAccount": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsUsBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsWechatPay": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsWechatPayDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsZip": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationCreateParamsZipDisplayPreference": (
        "stripe.params._payment_method_configuration_create_params",
        False,
    ),
    "PaymentMethodConfigurationListParams": (
        "stripe.params._payment_method_configuration_list_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParams": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAcssDebit": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAcssDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAffirm": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAffirmDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAfterpayClearpay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAfterpayClearpayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAlipay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAlipayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAlma": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAlmaDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAmazonPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAmazonPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsApplePay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsApplePayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsApplePayLater": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsApplePayLaterDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAuBecsDebit": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsAuBecsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBacsDebit": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBacsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBancontact": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBancontactDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBillie": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBillieDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBlik": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBlikDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBoleto": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsBoletoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCard": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCardDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCartesBancaires": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCartesBancairesDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCashapp": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCashappDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCrypto": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCryptoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCustomerBalance": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsCustomerBalanceDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsEps": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsEpsDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsFpx": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsFpxDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsFrMealVoucherConecs": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsFrMealVoucherConecsDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGiropay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGiropayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGooglePay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGooglePayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGrabpay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsGrabpayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsIdeal": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsIdealDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsJcb": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsJcbDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKakaoPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKakaoPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKlarna": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKlarnaDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKonbini": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKonbiniDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKrCard": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsKrCardDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsLink": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsLinkDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMbWay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMbWayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMobilepay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMobilepayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMultibanco": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsMultibancoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsNaverPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsNaverPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsNzBankAccount": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsNzBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsOxxo": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsOxxoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsP24": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsP24DisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPayByBank": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPayByBankDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPayco": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaycoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaynow": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaynowDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaypal": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaypalDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPayto": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPaytoDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPix": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPixDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPromptpay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsPromptpayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsRevolutPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsRevolutPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSamsungPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSamsungPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSatispay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSatispayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSepaDebit": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSepaDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSofort": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSofortDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSwish": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsSwishDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsTwint": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsTwintDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsUsBankAccount": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsUsBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsWechatPay": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsWechatPayDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsZip": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationModifyParamsZipDisplayPreference": (
        "stripe.params._payment_method_configuration_modify_params",
        False,
    ),
    "PaymentMethodConfigurationRetrieveParams": (
        "stripe.params._payment_method_configuration_retrieve_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParams": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAcssDebit": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAcssDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAffirm": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAffirmDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAfterpayClearpay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAfterpayClearpayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAlipay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAlipayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAlma": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAlmaDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAmazonPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAmazonPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsApplePay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsApplePayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsApplePayLater": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsApplePayLaterDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAuBecsDebit": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsAuBecsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBacsDebit": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBacsDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBancontact": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBancontactDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBillie": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBillieDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBlik": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBlikDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBoleto": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsBoletoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCard": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCardDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCartesBancaires": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCartesBancairesDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCashapp": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCashappDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCrypto": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCryptoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCustomerBalance": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsCustomerBalanceDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsEps": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsEpsDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsFpx": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsFpxDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsFrMealVoucherConecs": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsFrMealVoucherConecsDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGiropay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGiropayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGooglePay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGooglePayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGrabpay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsGrabpayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsIdeal": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsIdealDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsJcb": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsJcbDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKakaoPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKakaoPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKlarna": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKlarnaDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKonbini": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKonbiniDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKrCard": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsKrCardDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsLink": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsLinkDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMbWay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMbWayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMobilepay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMobilepayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMultibanco": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsMultibancoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsNaverPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsNaverPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsNzBankAccount": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsNzBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsOxxo": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsOxxoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsP24": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsP24DisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPayByBank": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPayByBankDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPayco": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaycoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaynow": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaynowDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaypal": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaypalDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPayto": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPaytoDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPix": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPixDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPromptpay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsPromptpayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsRevolutPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsRevolutPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSamsungPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSamsungPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSatispay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSatispayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSepaDebit": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSepaDebitDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSofort": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSofortDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSwish": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsSwishDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsTwint": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsTwintDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsUsBankAccount": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsUsBankAccountDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsWechatPay": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsWechatPayDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsZip": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodConfigurationUpdateParamsZipDisplayPreference": (
        "stripe.params._payment_method_configuration_update_params",
        False,
    ),
    "PaymentMethodCreateParams": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAcssDebit": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAffirm": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAfterpayClearpay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAlipay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAlma": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAmazonPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsAuBecsDebit": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBacsDebit": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBancontact": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBillie": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBillingDetails": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBillingDetailsAddress": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBlik": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsBoleto": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCard": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCardNetworks": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCashapp": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCrypto": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCustom": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsCustomerBalance": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsEps": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsFpx": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsGiropay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsGrabpay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsIdeal": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsInteracPresent": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsKakaoPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsKlarna": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsKlarnaDob": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsKonbini": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsKrCard": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsLink": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsMbWay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsMobilepay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsMultibanco": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsNaverPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsNzBankAccount": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsOxxo": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsP24": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPayByBank": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPayco": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPaynow": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPaypal": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPayto": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPix": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsPromptpay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsRadarOptions": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsRevolutPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsSamsungPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsSatispay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsSepaDebit": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsSofort": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsSwish": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsTwint": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsUsBankAccount": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsWechatPay": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodCreateParamsZip": (
        "stripe.params._payment_method_create_params",
        False,
    ),
    "PaymentMethodDetachParams": (
        "stripe.params._payment_method_detach_params",
        False,
    ),
    "PaymentMethodDomainCreateParams": (
        "stripe.params._payment_method_domain_create_params",
        False,
    ),
    "PaymentMethodDomainListParams": (
        "stripe.params._payment_method_domain_list_params",
        False,
    ),
    "PaymentMethodDomainModifyParams": (
        "stripe.params._payment_method_domain_modify_params",
        False,
    ),
    "PaymentMethodDomainRetrieveParams": (
        "stripe.params._payment_method_domain_retrieve_params",
        False,
    ),
    "PaymentMethodDomainUpdateParams": (
        "stripe.params._payment_method_domain_update_params",
        False,
    ),
    "PaymentMethodDomainValidateParams": (
        "stripe.params._payment_method_domain_validate_params",
        False,
    ),
    "PaymentMethodListParams": (
        "stripe.params._payment_method_list_params",
        False,
    ),
    "PaymentMethodModifyParams": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsBillingDetails": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsBillingDetailsAddress": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsCard": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsCardNetworks": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsPayto": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodModifyParamsUsBankAccount": (
        "stripe.params._payment_method_modify_params",
        False,
    ),
    "PaymentMethodRetrieveParams": (
        "stripe.params._payment_method_retrieve_params",
        False,
    ),
    "PaymentMethodUpdateParams": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsBillingDetails": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsBillingDetailsAddress": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsCard": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsCardNetworks": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsPayto": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentMethodUpdateParamsUsBankAccount": (
        "stripe.params._payment_method_update_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptCanceledParams": (
        "stripe.params._payment_record_report_payment_attempt_canceled_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptFailedParams": (
        "stripe.params._payment_record_report_payment_attempt_failed_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptGuaranteedParams": (
        "stripe.params._payment_record_report_payment_attempt_guaranteed_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptInformationalParams": (
        "stripe.params._payment_record_report_payment_attempt_informational_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptInformationalParamsCustomerDetails": (
        "stripe.params._payment_record_report_payment_attempt_informational_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptInformationalParamsShippingDetails": (
        "stripe.params._payment_record_report_payment_attempt_informational_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptInformationalParamsShippingDetailsAddress": (
        "stripe.params._payment_record_report_payment_attempt_informational_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParams": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsFailed": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsGuaranteed": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetails": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetails": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetailsAddress": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsCustom": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsShippingDetails": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentAttemptParamsShippingDetailsAddress": (
        "stripe.params._payment_record_report_payment_attempt_params",
        False,
    ),
    "PaymentRecordReportPaymentParams": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsAmountRequested": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsCustomerDetails": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsFailed": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsGuaranteed": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsPaymentMethodDetails": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetails": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetailsAddress": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsPaymentMethodDetailsCustom": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsProcessorDetails": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsProcessorDetailsCustom": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsShippingDetails": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportPaymentParamsShippingDetailsAddress": (
        "stripe.params._payment_record_report_payment_params",
        False,
    ),
    "PaymentRecordReportRefundParams": (
        "stripe.params._payment_record_report_refund_params",
        False,
    ),
    "PaymentRecordReportRefundParamsAmount": (
        "stripe.params._payment_record_report_refund_params",
        False,
    ),
    "PaymentRecordReportRefundParamsProcessorDetails": (
        "stripe.params._payment_record_report_refund_params",
        False,
    ),
    "PaymentRecordReportRefundParamsProcessorDetailsCustom": (
        "stripe.params._payment_record_report_refund_params",
        False,
    ),
    "PaymentRecordReportRefundParamsRefunded": (
        "stripe.params._payment_record_report_refund_params",
        False,
    ),
    "PaymentRecordRetrieveParams": (
        "stripe.params._payment_record_retrieve_params",
        False,
    ),
    "PayoutCancelParams": ("stripe.params._payout_cancel_params", False),
    "PayoutCreateParams": ("stripe.params._payout_create_params", False),
    "PayoutListParams": ("stripe.params._payout_list_params", False),
    "PayoutListParamsArrivalDate": (
        "stripe.params._payout_list_params",
        False,
    ),
    "PayoutListParamsCreated": ("stripe.params._payout_list_params", False),
    "PayoutModifyParams": ("stripe.params._payout_modify_params", False),
    "PayoutRetrieveParams": ("stripe.params._payout_retrieve_params", False),
    "PayoutReverseParams": ("stripe.params._payout_reverse_params", False),
    "PayoutUpdateParams": ("stripe.params._payout_update_params", False),
    "PlanCreateParams": ("stripe.params._plan_create_params", False),
    "PlanCreateParamsProduct": ("stripe.params._plan_create_params", False),
    "PlanCreateParamsTier": ("stripe.params._plan_create_params", False),
    "PlanCreateParamsTransformUsage": (
        "stripe.params._plan_create_params",
        False,
    ),
    "PlanDeleteParams": ("stripe.params._plan_delete_params", False),
    "PlanListParams": ("stripe.params._plan_list_params", False),
    "PlanListParamsCreated": ("stripe.params._plan_list_params", False),
    "PlanModifyParams": ("stripe.params._plan_modify_params", False),
    "PlanRetrieveParams": ("stripe.params._plan_retrieve_params", False),
    "PlanUpdateParams": ("stripe.params._plan_update_params", False),
    "PriceCreateParams": ("stripe.params._price_create_params", False),
    "PriceCreateParamsCurrencyOptions": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsCurrencyOptionsCustomUnitAmount": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsCurrencyOptionsTier": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsCustomUnitAmount": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsProductData": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsRecurring": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceCreateParamsTier": ("stripe.params._price_create_params", False),
    "PriceCreateParamsTransformQuantity": (
        "stripe.params._price_create_params",
        False,
    ),
    "PriceListParams": ("stripe.params._price_list_params", False),
    "PriceListParamsCreated": ("stripe.params._price_list_params", False),
    "PriceListParamsRecurring": ("stripe.params._price_list_params", False),
    "PriceModifyParams": ("stripe.params._price_modify_params", False),
    "PriceModifyParamsCurrencyOptions": (
        "stripe.params._price_modify_params",
        False,
    ),
    "PriceModifyParamsCurrencyOptionsCustomUnitAmount": (
        "stripe.params._price_modify_params",
        False,
    ),
    "PriceModifyParamsCurrencyOptionsTier": (
        "stripe.params._price_modify_params",
        False,
    ),
    "PriceRetrieveParams": ("stripe.params._price_retrieve_params", False),
    "PriceSearchParams": ("stripe.params._price_search_params", False),
    "PriceUpdateParams": ("stripe.params._price_update_params", False),
    "PriceUpdateParamsCurrencyOptions": (
        "stripe.params._price_update_params",
        False,
    ),
    "PriceUpdateParamsCurrencyOptionsCustomUnitAmount": (
        "stripe.params._price_update_params",
        False,
    ),
    "PriceUpdateParamsCurrencyOptionsTier": (
        "stripe.params._price_update_params",
        False,
    ),
    "ProductCreateFeatureParams": (
        "stripe.params._product_create_feature_params",
        False,
    ),
    "ProductCreateParams": ("stripe.params._product_create_params", False),
    "ProductCreateParamsDefaultPriceData": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsDefaultPriceDataCurrencyOptions": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsDefaultPriceDataCurrencyOptionsCustomUnitAmount": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsDefaultPriceDataCurrencyOptionsTier": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsDefaultPriceDataCustomUnitAmount": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsDefaultPriceDataRecurring": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsMarketingFeature": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductCreateParamsPackageDimensions": (
        "stripe.params._product_create_params",
        False,
    ),
    "ProductDeleteFeatureParams": (
        "stripe.params._product_delete_feature_params",
        False,
    ),
    "ProductDeleteParams": ("stripe.params._product_delete_params", False),
    "ProductFeatureCreateParams": (
        "stripe.params._product_feature_create_params",
        False,
    ),
    "ProductFeatureDeleteParams": (
        "stripe.params._product_feature_delete_params",
        False,
    ),
    "ProductFeatureListParams": (
        "stripe.params._product_feature_list_params",
        False,
    ),
    "ProductFeatureRetrieveParams": (
        "stripe.params._product_feature_retrieve_params",
        False,
    ),
    "ProductListFeaturesParams": (
        "stripe.params._product_list_features_params",
        False,
    ),
    "ProductListParams": ("stripe.params._product_list_params", False),
    "ProductListParamsCreated": ("stripe.params._product_list_params", False),
    "ProductModifyParams": ("stripe.params._product_modify_params", False),
    "ProductModifyParamsMarketingFeature": (
        "stripe.params._product_modify_params",
        False,
    ),
    "ProductModifyParamsPackageDimensions": (
        "stripe.params._product_modify_params",
        False,
    ),
    "ProductRetrieveFeatureParams": (
        "stripe.params._product_retrieve_feature_params",
        False,
    ),
    "ProductRetrieveParams": ("stripe.params._product_retrieve_params", False),
    "ProductSearchParams": ("stripe.params._product_search_params", False),
    "ProductUpdateParams": ("stripe.params._product_update_params", False),
    "ProductUpdateParamsMarketingFeature": (
        "stripe.params._product_update_params",
        False,
    ),
    "ProductUpdateParamsPackageDimensions": (
        "stripe.params._product_update_params",
        False,
    ),
    "PromotionCodeCreateParams": (
        "stripe.params._promotion_code_create_params",
        False,
    ),
    "PromotionCodeCreateParamsPromotion": (
        "stripe.params._promotion_code_create_params",
        False,
    ),
    "PromotionCodeCreateParamsRestrictions": (
        "stripe.params._promotion_code_create_params",
        False,
    ),
    "PromotionCodeCreateParamsRestrictionsCurrencyOptions": (
        "stripe.params._promotion_code_create_params",
        False,
    ),
    "PromotionCodeListParams": (
        "stripe.params._promotion_code_list_params",
        False,
    ),
    "PromotionCodeListParamsCreated": (
        "stripe.params._promotion_code_list_params",
        False,
    ),
    "PromotionCodeModifyParams": (
        "stripe.params._promotion_code_modify_params",
        False,
    ),
    "PromotionCodeModifyParamsRestrictions": (
        "stripe.params._promotion_code_modify_params",
        False,
    ),
    "PromotionCodeModifyParamsRestrictionsCurrencyOptions": (
        "stripe.params._promotion_code_modify_params",
        False,
    ),
    "PromotionCodeRetrieveParams": (
        "stripe.params._promotion_code_retrieve_params",
        False,
    ),
    "PromotionCodeUpdateParams": (
        "stripe.params._promotion_code_update_params",
        False,
    ),
    "PromotionCodeUpdateParamsRestrictions": (
        "stripe.params._promotion_code_update_params",
        False,
    ),
    "PromotionCodeUpdateParamsRestrictionsCurrencyOptions": (
        "stripe.params._promotion_code_update_params",
        False,
    ),
    "QuoteAcceptParams": ("stripe.params._quote_accept_params", False),
    "QuoteCancelParams": ("stripe.params._quote_cancel_params", False),
    "QuoteComputedUpfrontLineItemsListParams": (
        "stripe.params._quote_computed_upfront_line_items_list_params",
        False,
    ),
    "QuoteCreateParams": ("stripe.params._quote_create_params", False),
    "QuoteCreateParamsAutomaticTax": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsAutomaticTaxLiability": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsDiscount": ("stripe.params._quote_create_params", False),
    "QuoteCreateParamsFromQuote": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsInvoiceSettings": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsInvoiceSettingsIssuer": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsLineItem": ("stripe.params._quote_create_params", False),
    "QuoteCreateParamsLineItemDiscount": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsLineItemPriceData": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsLineItemPriceDataRecurring": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsSubscriptionData": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsSubscriptionDataBillingMode": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsSubscriptionDataBillingModeFlexible": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteCreateParamsTransferData": (
        "stripe.params._quote_create_params",
        False,
    ),
    "QuoteFinalizeQuoteParams": (
        "stripe.params._quote_finalize_quote_params",
        False,
    ),
    "QuoteLineItemListParams": (
        "stripe.params._quote_line_item_list_params",
        False,
    ),
    "QuoteListComputedUpfrontLineItemsParams": (
        "stripe.params._quote_list_computed_upfront_line_items_params",
        False,
    ),
    "QuoteListLineItemsParams": (
        "stripe.params._quote_list_line_items_params",
        False,
    ),
    "QuoteListParams": ("stripe.params._quote_list_params", False),
    "QuoteModifyParams": ("stripe.params._quote_modify_params", False),
    "QuoteModifyParamsAutomaticTax": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsAutomaticTaxLiability": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsDiscount": ("stripe.params._quote_modify_params", False),
    "QuoteModifyParamsInvoiceSettings": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsInvoiceSettingsIssuer": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsLineItem": ("stripe.params._quote_modify_params", False),
    "QuoteModifyParamsLineItemDiscount": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsLineItemPriceData": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsLineItemPriceDataRecurring": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsSubscriptionData": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuoteModifyParamsTransferData": (
        "stripe.params._quote_modify_params",
        False,
    ),
    "QuotePdfParams": ("stripe.params._quote_pdf_params", False),
    "QuoteRetrieveParams": ("stripe.params._quote_retrieve_params", False),
    "QuoteUpdateParams": ("stripe.params._quote_update_params", False),
    "QuoteUpdateParamsAutomaticTax": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsAutomaticTaxLiability": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsDiscount": ("stripe.params._quote_update_params", False),
    "QuoteUpdateParamsInvoiceSettings": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsInvoiceSettingsIssuer": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsLineItem": ("stripe.params._quote_update_params", False),
    "QuoteUpdateParamsLineItemDiscount": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsLineItemPriceData": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsLineItemPriceDataRecurring": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsSubscriptionData": (
        "stripe.params._quote_update_params",
        False,
    ),
    "QuoteUpdateParamsTransferData": (
        "stripe.params._quote_update_params",
        False,
    ),
    "RefundCancelParams": ("stripe.params._refund_cancel_params", False),
    "RefundCreateParams": ("stripe.params._refund_create_params", False),
    "RefundExpireParams": ("stripe.params._refund_expire_params", False),
    "RefundListParams": ("stripe.params._refund_list_params", False),
    "RefundListParamsCreated": ("stripe.params._refund_list_params", False),
    "RefundModifyParams": ("stripe.params._refund_modify_params", False),
    "RefundRetrieveParams": ("stripe.params._refund_retrieve_params", False),
    "RefundUpdateParams": ("stripe.params._refund_update_params", False),
    "ReviewApproveParams": ("stripe.params._review_approve_params", False),
    "ReviewListParams": ("stripe.params._review_list_params", False),
    "ReviewListParamsCreated": ("stripe.params._review_list_params", False),
    "ReviewRetrieveParams": ("stripe.params._review_retrieve_params", False),
    "SetupAttemptListParams": (
        "stripe.params._setup_attempt_list_params",
        False,
    ),
    "SetupAttemptListParamsCreated": (
        "stripe.params._setup_attempt_list_params",
        False,
    ),
    "SetupIntentCancelParams": (
        "stripe.params._setup_intent_cancel_params",
        False,
    ),
    "SetupIntentConfirmParams": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsMandateData": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsMandateDataCustomerAcceptance": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsMandateDataCustomerAcceptanceOffline": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsMandateDataCustomerAcceptanceOnline": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodData": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAcssDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAffirm": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAlipay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAlma": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAmazonPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBacsDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBancontact": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBillie": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBillingDetails": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBlik": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataBoleto": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataCashapp": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataCrypto": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataEps": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataFpx": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataGiropay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataGrabpay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataIdeal": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataInteracPresent": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataKakaoPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataKlarna": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataKonbini": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataKrCard": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataLink": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataMbWay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataMobilepay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataMultibanco": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataNaverPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataOxxo": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataP24": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPayByBank": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPayco": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPaynow": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPaypal": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPayto": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPix": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataPromptpay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataRadarOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataRevolutPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataSamsungPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataSatispay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataSepaDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataSofort": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataSwish": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataTwint": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataWechatPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodDataZip": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCard": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsKlarna": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsLink": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsPaypal": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsPayto": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentConfirmParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._setup_intent_confirm_params",
        False,
    ),
    "SetupIntentCreateParams": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsAutomaticPaymentMethods": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsMandateData": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsMandateDataCustomerAcceptance": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsMandateDataCustomerAcceptanceOffline": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsMandateDataCustomerAcceptanceOnline": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodData": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAcssDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAffirm": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAlipay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAlma": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAmazonPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBacsDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBancontact": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBillie": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBillingDetails": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBlik": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataBoleto": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataCashapp": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataCrypto": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataEps": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataFpx": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataGiropay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataGrabpay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataIdeal": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataInteracPresent": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataKakaoPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataKlarna": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataKonbini": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataKrCard": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataLink": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataMbWay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataMobilepay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataMultibanco": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataNaverPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataOxxo": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataP24": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPayByBank": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPayco": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPaynow": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPaypal": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPayto": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPix": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataPromptpay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataRadarOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataRevolutPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataSamsungPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataSatispay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataSepaDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataSofort": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataSwish": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataTwint": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataWechatPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodDataZip": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCard": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsKlarna": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsLink": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsPaypal": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsPayto": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentCreateParamsSingleUse": (
        "stripe.params._setup_intent_create_params",
        False,
    ),
    "SetupIntentListParams": (
        "stripe.params._setup_intent_list_params",
        False,
    ),
    "SetupIntentListParamsCreated": (
        "stripe.params._setup_intent_list_params",
        False,
    ),
    "SetupIntentModifyParams": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodData": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAcssDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAffirm": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAlipay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAlma": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAmazonPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBacsDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBancontact": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBillie": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBillingDetails": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBlik": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataBoleto": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataCashapp": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataCrypto": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataEps": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataFpx": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataGiropay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataGrabpay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataIdeal": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataInteracPresent": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataKakaoPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataKlarna": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataKonbini": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataKrCard": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataLink": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataMbWay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataMobilepay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataMultibanco": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataNaverPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataOxxo": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataP24": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPayByBank": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPayco": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPaynow": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPaypal": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPayto": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPix": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataPromptpay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataRadarOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataRevolutPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataSamsungPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataSatispay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataSepaDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataSofort": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataSwish": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataTwint": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataWechatPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodDataZip": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCard": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsKlarna": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsLink": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsPaypal": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsPayto": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentModifyParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._setup_intent_modify_params",
        False,
    ),
    "SetupIntentRetrieveParams": (
        "stripe.params._setup_intent_retrieve_params",
        False,
    ),
    "SetupIntentUpdateParams": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodData": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAcssDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAffirm": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAfterpayClearpay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAlipay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAlma": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAmazonPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataAuBecsDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBacsDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBancontact": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBillie": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBillingDetails": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBillingDetailsAddress": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBlik": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataBoleto": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataCashapp": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataCrypto": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataCustomerBalance": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataEps": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataFpx": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataGiropay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataGrabpay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataIdeal": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataInteracPresent": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataKakaoPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataKlarna": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataKlarnaDob": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataKonbini": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataKrCard": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataLink": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataMbWay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataMobilepay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataMultibanco": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataNaverPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataNzBankAccount": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataOxxo": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataP24": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPayByBank": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPayco": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPaynow": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPaypal": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPayto": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPix": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataPromptpay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataRadarOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataRevolutPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataSamsungPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataSatispay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataSepaDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataSofort": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataSwish": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataTwint": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataUsBankAccount": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataWechatPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodDataZip": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsAcssDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsAmazonPay": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsBacsDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsBacsDebitMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCard": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCardPresent": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecure": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsCardThreeDSecureNetworkOptionsCartesBancaires": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsKlarna": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsKlarnaOnDemand": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscription": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsLink": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsPaypal": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsPayto": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsSepaDebit": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsSepaDebitMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountMandateOptions": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentUpdateParamsPaymentMethodOptionsUsBankAccountNetworks": (
        "stripe.params._setup_intent_update_params",
        False,
    ),
    "SetupIntentVerifyMicrodepositsParams": (
        "stripe.params._setup_intent_verify_microdeposits_params",
        False,
    ),
    "ShippingRateCreateParams": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateCreateParamsDeliveryEstimate": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateCreateParamsDeliveryEstimateMaximum": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateCreateParamsDeliveryEstimateMinimum": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateCreateParamsFixedAmount": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateCreateParamsFixedAmountCurrencyOptions": (
        "stripe.params._shipping_rate_create_params",
        False,
    ),
    "ShippingRateListParams": (
        "stripe.params._shipping_rate_list_params",
        False,
    ),
    "ShippingRateListParamsCreated": (
        "stripe.params._shipping_rate_list_params",
        False,
    ),
    "ShippingRateModifyParams": (
        "stripe.params._shipping_rate_modify_params",
        False,
    ),
    "ShippingRateModifyParamsFixedAmount": (
        "stripe.params._shipping_rate_modify_params",
        False,
    ),
    "ShippingRateModifyParamsFixedAmountCurrencyOptions": (
        "stripe.params._shipping_rate_modify_params",
        False,
    ),
    "ShippingRateRetrieveParams": (
        "stripe.params._shipping_rate_retrieve_params",
        False,
    ),
    "ShippingRateUpdateParams": (
        "stripe.params._shipping_rate_update_params",
        False,
    ),
    "ShippingRateUpdateParamsFixedAmount": (
        "stripe.params._shipping_rate_update_params",
        False,
    ),
    "ShippingRateUpdateParamsFixedAmountCurrencyOptions": (
        "stripe.params._shipping_rate_update_params",
        False,
    ),
    "SourceCreateParams": ("stripe.params._source_create_params", False),
    "SourceCreateParamsMandate": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsMandateAcceptance": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsMandateAcceptanceOffline": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsMandateAcceptanceOnline": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsOwner": ("stripe.params._source_create_params", False),
    "SourceCreateParamsOwnerAddress": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsReceiver": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsRedirect": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsSourceOrder": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsSourceOrderItem": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsSourceOrderShipping": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceCreateParamsSourceOrderShippingAddress": (
        "stripe.params._source_create_params",
        False,
    ),
    "SourceDetachParams": ("stripe.params._source_detach_params", False),
    "SourceListSourceTransactionsParams": (
        "stripe.params._source_list_source_transactions_params",
        False,
    ),
    "SourceModifyParams": ("stripe.params._source_modify_params", False),
    "SourceModifyParamsMandate": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsMandateAcceptance": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsMandateAcceptanceOffline": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsMandateAcceptanceOnline": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsOwner": ("stripe.params._source_modify_params", False),
    "SourceModifyParamsOwnerAddress": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsSourceOrder": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsSourceOrderItem": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsSourceOrderShipping": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceModifyParamsSourceOrderShippingAddress": (
        "stripe.params._source_modify_params",
        False,
    ),
    "SourceRetrieveParams": ("stripe.params._source_retrieve_params", False),
    "SourceTransactionListParams": (
        "stripe.params._source_transaction_list_params",
        False,
    ),
    "SourceUpdateParams": ("stripe.params._source_update_params", False),
    "SourceUpdateParamsMandate": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsMandateAcceptance": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsMandateAcceptanceOffline": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsMandateAcceptanceOnline": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsOwner": ("stripe.params._source_update_params", False),
    "SourceUpdateParamsOwnerAddress": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsSourceOrder": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsSourceOrderItem": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsSourceOrderShipping": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceUpdateParamsSourceOrderShippingAddress": (
        "stripe.params._source_update_params",
        False,
    ),
    "SourceVerifyParams": ("stripe.params._source_verify_params", False),
    "SubscriptionCancelParams": (
        "stripe.params._subscription_cancel_params",
        False,
    ),
    "SubscriptionCancelParamsCancellationDetails": (
        "stripe.params._subscription_cancel_params",
        False,
    ),
    "SubscriptionCreateParams": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItem": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItemDiscount": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItemPeriod": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAddInvoiceItemPriceData": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAutomaticTax": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsAutomaticTaxLiability": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsBillingCycleAnchorConfig": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsBillingMode": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsBillingModeFlexible": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsBillingThresholds": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsDiscount": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsInvoiceSettings": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsInvoiceSettingsIssuer": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsItem": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsItemBillingThresholds": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsItemDiscount": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsItemPriceData": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsItemPriceDataRecurring": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettings": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsPendingInvoiceItemInterval": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsTransferData": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsTrialSettings": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionCreateParamsTrialSettingsEndBehavior": (
        "stripe.params._subscription_create_params",
        False,
    ),
    "SubscriptionDeleteDiscountParams": (
        "stripe.params._subscription_delete_discount_params",
        False,
    ),
    "SubscriptionItemCreateParams": (
        "stripe.params._subscription_item_create_params",
        False,
    ),
    "SubscriptionItemCreateParamsBillingThresholds": (
        "stripe.params._subscription_item_create_params",
        False,
    ),
    "SubscriptionItemCreateParamsDiscount": (
        "stripe.params._subscription_item_create_params",
        False,
    ),
    "SubscriptionItemCreateParamsPriceData": (
        "stripe.params._subscription_item_create_params",
        False,
    ),
    "SubscriptionItemCreateParamsPriceDataRecurring": (
        "stripe.params._subscription_item_create_params",
        False,
    ),
    "SubscriptionItemDeleteParams": (
        "stripe.params._subscription_item_delete_params",
        False,
    ),
    "SubscriptionItemListParams": (
        "stripe.params._subscription_item_list_params",
        False,
    ),
    "SubscriptionItemModifyParams": (
        "stripe.params._subscription_item_modify_params",
        False,
    ),
    "SubscriptionItemModifyParamsBillingThresholds": (
        "stripe.params._subscription_item_modify_params",
        False,
    ),
    "SubscriptionItemModifyParamsDiscount": (
        "stripe.params._subscription_item_modify_params",
        False,
    ),
    "SubscriptionItemModifyParamsPriceData": (
        "stripe.params._subscription_item_modify_params",
        False,
    ),
    "SubscriptionItemModifyParamsPriceDataRecurring": (
        "stripe.params._subscription_item_modify_params",
        False,
    ),
    "SubscriptionItemRetrieveParams": (
        "stripe.params._subscription_item_retrieve_params",
        False,
    ),
    "SubscriptionItemUpdateParams": (
        "stripe.params._subscription_item_update_params",
        False,
    ),
    "SubscriptionItemUpdateParamsBillingThresholds": (
        "stripe.params._subscription_item_update_params",
        False,
    ),
    "SubscriptionItemUpdateParamsDiscount": (
        "stripe.params._subscription_item_update_params",
        False,
    ),
    "SubscriptionItemUpdateParamsPriceData": (
        "stripe.params._subscription_item_update_params",
        False,
    ),
    "SubscriptionItemUpdateParamsPriceDataRecurring": (
        "stripe.params._subscription_item_update_params",
        False,
    ),
    "SubscriptionListParams": (
        "stripe.params._subscription_list_params",
        False,
    ),
    "SubscriptionListParamsAutomaticTax": (
        "stripe.params._subscription_list_params",
        False,
    ),
    "SubscriptionListParamsCreated": (
        "stripe.params._subscription_list_params",
        False,
    ),
    "SubscriptionListParamsCurrentPeriodEnd": (
        "stripe.params._subscription_list_params",
        False,
    ),
    "SubscriptionListParamsCurrentPeriodStart": (
        "stripe.params._subscription_list_params",
        False,
    ),
    "SubscriptionMigrateParams": (
        "stripe.params._subscription_migrate_params",
        False,
    ),
    "SubscriptionMigrateParamsBillingMode": (
        "stripe.params._subscription_migrate_params",
        False,
    ),
    "SubscriptionMigrateParamsBillingModeFlexible": (
        "stripe.params._subscription_migrate_params",
        False,
    ),
    "SubscriptionModifyParams": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItem": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItemDiscount": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItemPeriod": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAddInvoiceItemPriceData": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAutomaticTax": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsAutomaticTaxLiability": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsBillingThresholds": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsCancellationDetails": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsDiscount": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsInvoiceSettings": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsInvoiceSettingsIssuer": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsItem": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsItemBillingThresholds": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsItemDiscount": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsItemPriceData": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsItemPriceDataRecurring": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPauseCollection": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettings": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsPendingInvoiceItemInterval": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsTransferData": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsTrialSettings": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionModifyParamsTrialSettingsEndBehavior": (
        "stripe.params._subscription_modify_params",
        False,
    ),
    "SubscriptionResumeParams": (
        "stripe.params._subscription_resume_params",
        False,
    ),
    "SubscriptionRetrieveParams": (
        "stripe.params._subscription_retrieve_params",
        False,
    ),
    "SubscriptionScheduleCancelParams": (
        "stripe.params._subscription_schedule_cancel_params",
        False,
    ),
    "SubscriptionScheduleCreateParams": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsBillingMode": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsBillingModeFlexible": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettings": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTax": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsBillingThresholds": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettings": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsDefaultSettingsTransferData": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhase": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItem": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemDiscount": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriod": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPriceData": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAutomaticTax": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseBillingThresholds": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseDiscount": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseDuration": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseInvoiceSettings": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseItem": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseItemBillingThresholds": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseItemDiscount": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseItemPriceData": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseItemPriceDataRecurring": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleCreateParamsPhaseTransferData": (
        "stripe.params._subscription_schedule_create_params",
        False,
    ),
    "SubscriptionScheduleListParams": (
        "stripe.params._subscription_schedule_list_params",
        False,
    ),
    "SubscriptionScheduleListParamsCanceledAt": (
        "stripe.params._subscription_schedule_list_params",
        False,
    ),
    "SubscriptionScheduleListParamsCompletedAt": (
        "stripe.params._subscription_schedule_list_params",
        False,
    ),
    "SubscriptionScheduleListParamsCreated": (
        "stripe.params._subscription_schedule_list_params",
        False,
    ),
    "SubscriptionScheduleListParamsReleasedAt": (
        "stripe.params._subscription_schedule_list_params",
        False,
    ),
    "SubscriptionScheduleModifyParams": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettings": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTax": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsBillingThresholds": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettings": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsDefaultSettingsTransferData": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhase": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItem": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItemDiscount": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriod": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAddInvoiceItemPriceData": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAutomaticTax": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseBillingThresholds": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseDiscount": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseDuration": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseInvoiceSettings": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseItem": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseItemBillingThresholds": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseItemDiscount": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseItemPriceData": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseItemPriceDataRecurring": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleModifyParamsPhaseTransferData": (
        "stripe.params._subscription_schedule_modify_params",
        False,
    ),
    "SubscriptionScheduleReleaseParams": (
        "stripe.params._subscription_schedule_release_params",
        False,
    ),
    "SubscriptionScheduleRetrieveParams": (
        "stripe.params._subscription_schedule_retrieve_params",
        False,
    ),
    "SubscriptionScheduleUpdateParams": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettings": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTax": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsBillingThresholds": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettings": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsDefaultSettingsTransferData": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhase": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItem": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemDiscount": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriod": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAddInvoiceItemPriceData": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAutomaticTax": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseAutomaticTaxLiability": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseBillingThresholds": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseDiscount": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseDuration": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseInvoiceSettings": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseInvoiceSettingsIssuer": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseItem": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseItemBillingThresholds": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseItemDiscount": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseItemPriceData": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseItemPriceDataRecurring": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionScheduleUpdateParamsPhaseTransferData": (
        "stripe.params._subscription_schedule_update_params",
        False,
    ),
    "SubscriptionSearchParams": (
        "stripe.params._subscription_search_params",
        False,
    ),
    "SubscriptionUpdateParams": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItem": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItemDiscount": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItemPeriod": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItemPeriodEnd": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItemPeriodStart": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAddInvoiceItemPriceData": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAutomaticTax": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsAutomaticTaxLiability": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsBillingThresholds": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsCancellationDetails": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsDiscount": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsInvoiceSettings": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsInvoiceSettingsIssuer": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsItem": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsItemBillingThresholds": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsItemDiscount": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsItemPriceData": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsItemPriceDataRecurring": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPauseCollection": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettings": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptions": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCard": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCardMandateOptions": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPayto": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsPendingInvoiceItemInterval": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsTransferData": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsTrialSettings": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "SubscriptionUpdateParamsTrialSettingsEndBehavior": (
        "stripe.params._subscription_update_params",
        False,
    ),
    "TaxCodeListParams": ("stripe.params._tax_code_list_params", False),
    "TaxCodeRetrieveParams": (
        "stripe.params._tax_code_retrieve_params",
        False,
    ),
    "TaxIdCreateParams": ("stripe.params._tax_id_create_params", False),
    "TaxIdCreateParamsOwner": ("stripe.params._tax_id_create_params", False),
    "TaxIdDeleteParams": ("stripe.params._tax_id_delete_params", False),
    "TaxIdListParams": ("stripe.params._tax_id_list_params", False),
    "TaxIdListParamsOwner": ("stripe.params._tax_id_list_params", False),
    "TaxIdRetrieveParams": ("stripe.params._tax_id_retrieve_params", False),
    "TaxRateCreateParams": ("stripe.params._tax_rate_create_params", False),
    "TaxRateListParams": ("stripe.params._tax_rate_list_params", False),
    "TaxRateListParamsCreated": ("stripe.params._tax_rate_list_params", False),
    "TaxRateModifyParams": ("stripe.params._tax_rate_modify_params", False),
    "TaxRateRetrieveParams": (
        "stripe.params._tax_rate_retrieve_params",
        False,
    ),
    "TaxRateUpdateParams": ("stripe.params._tax_rate_update_params", False),
    "TokenCreateParams": ("stripe.params._token_create_params", False),
    "TokenCreateParamsAccount": ("stripe.params._token_create_params", False),
    "TokenCreateParamsAccountCompany": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyAddress": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyAddressKana": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyAddressKanji": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyDirectorshipDeclaration": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyOwnershipDeclaration": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyRegistrationDate": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyRepresentativeDeclaration": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyVerification": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountCompanyVerificationDocument": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividual": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualAddress": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualAddressKana": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualAddressKanji": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualDob": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualRegisteredAddress": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualRelationship": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualVerification": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualVerificationAdditionalDocument": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsAccountIndividualVerificationDocument": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsBankAccount": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsCard": ("stripe.params._token_create_params", False),
    "TokenCreateParamsCardNetworks": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsCvcUpdate": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPerson": ("stripe.params._token_create_params", False),
    "TokenCreateParamsPersonAdditionalTosAcceptances": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonAdditionalTosAcceptancesAccount": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonAddress": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonAddressKana": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonAddressKanji": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonDob": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonDocuments": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonDocumentsCompanyAuthorization": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonDocumentsPassport": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonDocumentsVisa": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonRegisteredAddress": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonRelationship": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonUsCfpbData": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonUsCfpbDataEthnicityDetails": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonUsCfpbDataRaceDetails": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonVerification": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonVerificationAdditionalDocument": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPersonVerificationDocument": (
        "stripe.params._token_create_params",
        False,
    ),
    "TokenCreateParamsPii": ("stripe.params._token_create_params", False),
    "TokenRetrieveParams": ("stripe.params._token_retrieve_params", False),
    "TopupCancelParams": ("stripe.params._topup_cancel_params", False),
    "TopupCreateParams": ("stripe.params._topup_create_params", False),
    "TopupListParams": ("stripe.params._topup_list_params", False),
    "TopupListParamsAmount": ("stripe.params._topup_list_params", False),
    "TopupListParamsCreated": ("stripe.params._topup_list_params", False),
    "TopupModifyParams": ("stripe.params._topup_modify_params", False),
    "TopupRetrieveParams": ("stripe.params._topup_retrieve_params", False),
    "TopupUpdateParams": ("stripe.params._topup_update_params", False),
    "TransferCreateParams": ("stripe.params._transfer_create_params", False),
    "TransferCreateReversalParams": (
        "stripe.params._transfer_create_reversal_params",
        False,
    ),
    "TransferListParams": ("stripe.params._transfer_list_params", False),
    "TransferListParamsCreated": (
        "stripe.params._transfer_list_params",
        False,
    ),
    "TransferListReversalsParams": (
        "stripe.params._transfer_list_reversals_params",
        False,
    ),
    "TransferModifyParams": ("stripe.params._transfer_modify_params", False),
    "TransferModifyReversalParams": (
        "stripe.params._transfer_modify_reversal_params",
        False,
    ),
    "TransferRetrieveParams": (
        "stripe.params._transfer_retrieve_params",
        False,
    ),
    "TransferRetrieveReversalParams": (
        "stripe.params._transfer_retrieve_reversal_params",
        False,
    ),
    "TransferReversalCreateParams": (
        "stripe.params._transfer_reversal_create_params",
        False,
    ),
    "TransferReversalListParams": (
        "stripe.params._transfer_reversal_list_params",
        False,
    ),
    "TransferReversalRetrieveParams": (
        "stripe.params._transfer_reversal_retrieve_params",
        False,
    ),
    "TransferReversalUpdateParams": (
        "stripe.params._transfer_reversal_update_params",
        False,
    ),
    "TransferUpdateParams": ("stripe.params._transfer_update_params", False),
    "WebhookEndpointCreateParams": (
        "stripe.params._webhook_endpoint_create_params",
        False,
    ),
    "WebhookEndpointDeleteParams": (
        "stripe.params._webhook_endpoint_delete_params",
        False,
    ),
    "WebhookEndpointListParams": (
        "stripe.params._webhook_endpoint_list_params",
        False,
    ),
    "WebhookEndpointModifyParams": (
        "stripe.params._webhook_endpoint_modify_params",
        False,
    ),
    "WebhookEndpointRetrieveParams": (
        "stripe.params._webhook_endpoint_retrieve_params",
        False,
    ),
    "WebhookEndpointUpdateParams": (
        "stripe.params._webhook_endpoint_update_params",
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
