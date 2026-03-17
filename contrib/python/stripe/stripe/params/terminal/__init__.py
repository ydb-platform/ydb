# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.terminal._configuration_create_params import (
        ConfigurationCreateParams as ConfigurationCreateParams,
        ConfigurationCreateParamsBbposWisepad3 as ConfigurationCreateParamsBbposWisepad3,
        ConfigurationCreateParamsBbposWiseposE as ConfigurationCreateParamsBbposWiseposE,
        ConfigurationCreateParamsCellular as ConfigurationCreateParamsCellular,
        ConfigurationCreateParamsOffline as ConfigurationCreateParamsOffline,
        ConfigurationCreateParamsRebootWindow as ConfigurationCreateParamsRebootWindow,
        ConfigurationCreateParamsStripeS700 as ConfigurationCreateParamsStripeS700,
        ConfigurationCreateParamsStripeS710 as ConfigurationCreateParamsStripeS710,
        ConfigurationCreateParamsTipping as ConfigurationCreateParamsTipping,
        ConfigurationCreateParamsTippingAed as ConfigurationCreateParamsTippingAed,
        ConfigurationCreateParamsTippingAud as ConfigurationCreateParamsTippingAud,
        ConfigurationCreateParamsTippingCad as ConfigurationCreateParamsTippingCad,
        ConfigurationCreateParamsTippingChf as ConfigurationCreateParamsTippingChf,
        ConfigurationCreateParamsTippingCzk as ConfigurationCreateParamsTippingCzk,
        ConfigurationCreateParamsTippingDkk as ConfigurationCreateParamsTippingDkk,
        ConfigurationCreateParamsTippingEur as ConfigurationCreateParamsTippingEur,
        ConfigurationCreateParamsTippingGbp as ConfigurationCreateParamsTippingGbp,
        ConfigurationCreateParamsTippingGip as ConfigurationCreateParamsTippingGip,
        ConfigurationCreateParamsTippingHkd as ConfigurationCreateParamsTippingHkd,
        ConfigurationCreateParamsTippingHuf as ConfigurationCreateParamsTippingHuf,
        ConfigurationCreateParamsTippingJpy as ConfigurationCreateParamsTippingJpy,
        ConfigurationCreateParamsTippingMxn as ConfigurationCreateParamsTippingMxn,
        ConfigurationCreateParamsTippingMyr as ConfigurationCreateParamsTippingMyr,
        ConfigurationCreateParamsTippingNok as ConfigurationCreateParamsTippingNok,
        ConfigurationCreateParamsTippingNzd as ConfigurationCreateParamsTippingNzd,
        ConfigurationCreateParamsTippingPln as ConfigurationCreateParamsTippingPln,
        ConfigurationCreateParamsTippingRon as ConfigurationCreateParamsTippingRon,
        ConfigurationCreateParamsTippingSek as ConfigurationCreateParamsTippingSek,
        ConfigurationCreateParamsTippingSgd as ConfigurationCreateParamsTippingSgd,
        ConfigurationCreateParamsTippingUsd as ConfigurationCreateParamsTippingUsd,
        ConfigurationCreateParamsVerifoneP400 as ConfigurationCreateParamsVerifoneP400,
        ConfigurationCreateParamsWifi as ConfigurationCreateParamsWifi,
        ConfigurationCreateParamsWifiEnterpriseEapPeap as ConfigurationCreateParamsWifiEnterpriseEapPeap,
        ConfigurationCreateParamsWifiEnterpriseEapTls as ConfigurationCreateParamsWifiEnterpriseEapTls,
        ConfigurationCreateParamsWifiPersonalPsk as ConfigurationCreateParamsWifiPersonalPsk,
    )
    from stripe.params.terminal._configuration_delete_params import (
        ConfigurationDeleteParams as ConfigurationDeleteParams,
    )
    from stripe.params.terminal._configuration_list_params import (
        ConfigurationListParams as ConfigurationListParams,
    )
    from stripe.params.terminal._configuration_modify_params import (
        ConfigurationModifyParams as ConfigurationModifyParams,
        ConfigurationModifyParamsBbposWisepad3 as ConfigurationModifyParamsBbposWisepad3,
        ConfigurationModifyParamsBbposWiseposE as ConfigurationModifyParamsBbposWiseposE,
        ConfigurationModifyParamsCellular as ConfigurationModifyParamsCellular,
        ConfigurationModifyParamsOffline as ConfigurationModifyParamsOffline,
        ConfigurationModifyParamsRebootWindow as ConfigurationModifyParamsRebootWindow,
        ConfigurationModifyParamsStripeS700 as ConfigurationModifyParamsStripeS700,
        ConfigurationModifyParamsStripeS710 as ConfigurationModifyParamsStripeS710,
        ConfigurationModifyParamsTipping as ConfigurationModifyParamsTipping,
        ConfigurationModifyParamsTippingAed as ConfigurationModifyParamsTippingAed,
        ConfigurationModifyParamsTippingAud as ConfigurationModifyParamsTippingAud,
        ConfigurationModifyParamsTippingCad as ConfigurationModifyParamsTippingCad,
        ConfigurationModifyParamsTippingChf as ConfigurationModifyParamsTippingChf,
        ConfigurationModifyParamsTippingCzk as ConfigurationModifyParamsTippingCzk,
        ConfigurationModifyParamsTippingDkk as ConfigurationModifyParamsTippingDkk,
        ConfigurationModifyParamsTippingEur as ConfigurationModifyParamsTippingEur,
        ConfigurationModifyParamsTippingGbp as ConfigurationModifyParamsTippingGbp,
        ConfigurationModifyParamsTippingGip as ConfigurationModifyParamsTippingGip,
        ConfigurationModifyParamsTippingHkd as ConfigurationModifyParamsTippingHkd,
        ConfigurationModifyParamsTippingHuf as ConfigurationModifyParamsTippingHuf,
        ConfigurationModifyParamsTippingJpy as ConfigurationModifyParamsTippingJpy,
        ConfigurationModifyParamsTippingMxn as ConfigurationModifyParamsTippingMxn,
        ConfigurationModifyParamsTippingMyr as ConfigurationModifyParamsTippingMyr,
        ConfigurationModifyParamsTippingNok as ConfigurationModifyParamsTippingNok,
        ConfigurationModifyParamsTippingNzd as ConfigurationModifyParamsTippingNzd,
        ConfigurationModifyParamsTippingPln as ConfigurationModifyParamsTippingPln,
        ConfigurationModifyParamsTippingRon as ConfigurationModifyParamsTippingRon,
        ConfigurationModifyParamsTippingSek as ConfigurationModifyParamsTippingSek,
        ConfigurationModifyParamsTippingSgd as ConfigurationModifyParamsTippingSgd,
        ConfigurationModifyParamsTippingUsd as ConfigurationModifyParamsTippingUsd,
        ConfigurationModifyParamsVerifoneP400 as ConfigurationModifyParamsVerifoneP400,
        ConfigurationModifyParamsWifi as ConfigurationModifyParamsWifi,
        ConfigurationModifyParamsWifiEnterpriseEapPeap as ConfigurationModifyParamsWifiEnterpriseEapPeap,
        ConfigurationModifyParamsWifiEnterpriseEapTls as ConfigurationModifyParamsWifiEnterpriseEapTls,
        ConfigurationModifyParamsWifiPersonalPsk as ConfigurationModifyParamsWifiPersonalPsk,
    )
    from stripe.params.terminal._configuration_retrieve_params import (
        ConfigurationRetrieveParams as ConfigurationRetrieveParams,
    )
    from stripe.params.terminal._configuration_update_params import (
        ConfigurationUpdateParams as ConfigurationUpdateParams,
        ConfigurationUpdateParamsBbposWisepad3 as ConfigurationUpdateParamsBbposWisepad3,
        ConfigurationUpdateParamsBbposWiseposE as ConfigurationUpdateParamsBbposWiseposE,
        ConfigurationUpdateParamsCellular as ConfigurationUpdateParamsCellular,
        ConfigurationUpdateParamsOffline as ConfigurationUpdateParamsOffline,
        ConfigurationUpdateParamsRebootWindow as ConfigurationUpdateParamsRebootWindow,
        ConfigurationUpdateParamsStripeS700 as ConfigurationUpdateParamsStripeS700,
        ConfigurationUpdateParamsStripeS710 as ConfigurationUpdateParamsStripeS710,
        ConfigurationUpdateParamsTipping as ConfigurationUpdateParamsTipping,
        ConfigurationUpdateParamsTippingAed as ConfigurationUpdateParamsTippingAed,
        ConfigurationUpdateParamsTippingAud as ConfigurationUpdateParamsTippingAud,
        ConfigurationUpdateParamsTippingCad as ConfigurationUpdateParamsTippingCad,
        ConfigurationUpdateParamsTippingChf as ConfigurationUpdateParamsTippingChf,
        ConfigurationUpdateParamsTippingCzk as ConfigurationUpdateParamsTippingCzk,
        ConfigurationUpdateParamsTippingDkk as ConfigurationUpdateParamsTippingDkk,
        ConfigurationUpdateParamsTippingEur as ConfigurationUpdateParamsTippingEur,
        ConfigurationUpdateParamsTippingGbp as ConfigurationUpdateParamsTippingGbp,
        ConfigurationUpdateParamsTippingGip as ConfigurationUpdateParamsTippingGip,
        ConfigurationUpdateParamsTippingHkd as ConfigurationUpdateParamsTippingHkd,
        ConfigurationUpdateParamsTippingHuf as ConfigurationUpdateParamsTippingHuf,
        ConfigurationUpdateParamsTippingJpy as ConfigurationUpdateParamsTippingJpy,
        ConfigurationUpdateParamsTippingMxn as ConfigurationUpdateParamsTippingMxn,
        ConfigurationUpdateParamsTippingMyr as ConfigurationUpdateParamsTippingMyr,
        ConfigurationUpdateParamsTippingNok as ConfigurationUpdateParamsTippingNok,
        ConfigurationUpdateParamsTippingNzd as ConfigurationUpdateParamsTippingNzd,
        ConfigurationUpdateParamsTippingPln as ConfigurationUpdateParamsTippingPln,
        ConfigurationUpdateParamsTippingRon as ConfigurationUpdateParamsTippingRon,
        ConfigurationUpdateParamsTippingSek as ConfigurationUpdateParamsTippingSek,
        ConfigurationUpdateParamsTippingSgd as ConfigurationUpdateParamsTippingSgd,
        ConfigurationUpdateParamsTippingUsd as ConfigurationUpdateParamsTippingUsd,
        ConfigurationUpdateParamsVerifoneP400 as ConfigurationUpdateParamsVerifoneP400,
        ConfigurationUpdateParamsWifi as ConfigurationUpdateParamsWifi,
        ConfigurationUpdateParamsWifiEnterpriseEapPeap as ConfigurationUpdateParamsWifiEnterpriseEapPeap,
        ConfigurationUpdateParamsWifiEnterpriseEapTls as ConfigurationUpdateParamsWifiEnterpriseEapTls,
        ConfigurationUpdateParamsWifiPersonalPsk as ConfigurationUpdateParamsWifiPersonalPsk,
    )
    from stripe.params.terminal._connection_token_create_params import (
        ConnectionTokenCreateParams as ConnectionTokenCreateParams,
    )
    from stripe.params.terminal._location_create_params import (
        LocationCreateParams as LocationCreateParams,
        LocationCreateParamsAddress as LocationCreateParamsAddress,
        LocationCreateParamsAddressKana as LocationCreateParamsAddressKana,
        LocationCreateParamsAddressKanji as LocationCreateParamsAddressKanji,
    )
    from stripe.params.terminal._location_delete_params import (
        LocationDeleteParams as LocationDeleteParams,
    )
    from stripe.params.terminal._location_list_params import (
        LocationListParams as LocationListParams,
    )
    from stripe.params.terminal._location_modify_params import (
        LocationModifyParams as LocationModifyParams,
        LocationModifyParamsAddress as LocationModifyParamsAddress,
        LocationModifyParamsAddressKana as LocationModifyParamsAddressKana,
        LocationModifyParamsAddressKanji as LocationModifyParamsAddressKanji,
    )
    from stripe.params.terminal._location_retrieve_params import (
        LocationRetrieveParams as LocationRetrieveParams,
    )
    from stripe.params.terminal._location_update_params import (
        LocationUpdateParams as LocationUpdateParams,
        LocationUpdateParamsAddress as LocationUpdateParamsAddress,
        LocationUpdateParamsAddressKana as LocationUpdateParamsAddressKana,
        LocationUpdateParamsAddressKanji as LocationUpdateParamsAddressKanji,
    )
    from stripe.params.terminal._onboarding_link_create_params import (
        OnboardingLinkCreateParams as OnboardingLinkCreateParams,
        OnboardingLinkCreateParamsLinkOptions as OnboardingLinkCreateParamsLinkOptions,
        OnboardingLinkCreateParamsLinkOptionsAppleTermsAndConditions as OnboardingLinkCreateParamsLinkOptionsAppleTermsAndConditions,
    )
    from stripe.params.terminal._reader_cancel_action_params import (
        ReaderCancelActionParams as ReaderCancelActionParams,
    )
    from stripe.params.terminal._reader_collect_inputs_params import (
        ReaderCollectInputsParams as ReaderCollectInputsParams,
        ReaderCollectInputsParamsInput as ReaderCollectInputsParamsInput,
        ReaderCollectInputsParamsInputCustomText as ReaderCollectInputsParamsInputCustomText,
        ReaderCollectInputsParamsInputSelection as ReaderCollectInputsParamsInputSelection,
        ReaderCollectInputsParamsInputSelectionChoice as ReaderCollectInputsParamsInputSelectionChoice,
        ReaderCollectInputsParamsInputToggle as ReaderCollectInputsParamsInputToggle,
    )
    from stripe.params.terminal._reader_collect_payment_method_params import (
        ReaderCollectPaymentMethodParams as ReaderCollectPaymentMethodParams,
        ReaderCollectPaymentMethodParamsCollectConfig as ReaderCollectPaymentMethodParamsCollectConfig,
        ReaderCollectPaymentMethodParamsCollectConfigTipping as ReaderCollectPaymentMethodParamsCollectConfigTipping,
    )
    from stripe.params.terminal._reader_confirm_payment_intent_params import (
        ReaderConfirmPaymentIntentParams as ReaderConfirmPaymentIntentParams,
        ReaderConfirmPaymentIntentParamsConfirmConfig as ReaderConfirmPaymentIntentParamsConfirmConfig,
    )
    from stripe.params.terminal._reader_create_params import (
        ReaderCreateParams as ReaderCreateParams,
    )
    from stripe.params.terminal._reader_delete_params import (
        ReaderDeleteParams as ReaderDeleteParams,
    )
    from stripe.params.terminal._reader_list_params import (
        ReaderListParams as ReaderListParams,
    )
    from stripe.params.terminal._reader_modify_params import (
        ReaderModifyParams as ReaderModifyParams,
    )
    from stripe.params.terminal._reader_present_payment_method_params import (
        ReaderPresentPaymentMethodParams as ReaderPresentPaymentMethodParams,
        ReaderPresentPaymentMethodParamsCard as ReaderPresentPaymentMethodParamsCard,
        ReaderPresentPaymentMethodParamsCardPresent as ReaderPresentPaymentMethodParamsCardPresent,
        ReaderPresentPaymentMethodParamsInteracPresent as ReaderPresentPaymentMethodParamsInteracPresent,
    )
    from stripe.params.terminal._reader_process_payment_intent_params import (
        ReaderProcessPaymentIntentParams as ReaderProcessPaymentIntentParams,
        ReaderProcessPaymentIntentParamsProcessConfig as ReaderProcessPaymentIntentParamsProcessConfig,
        ReaderProcessPaymentIntentParamsProcessConfigTipping as ReaderProcessPaymentIntentParamsProcessConfigTipping,
    )
    from stripe.params.terminal._reader_process_setup_intent_params import (
        ReaderProcessSetupIntentParams as ReaderProcessSetupIntentParams,
        ReaderProcessSetupIntentParamsProcessConfig as ReaderProcessSetupIntentParamsProcessConfig,
    )
    from stripe.params.terminal._reader_refund_payment_params import (
        ReaderRefundPaymentParams as ReaderRefundPaymentParams,
        ReaderRefundPaymentParamsRefundPaymentConfig as ReaderRefundPaymentParamsRefundPaymentConfig,
    )
    from stripe.params.terminal._reader_retrieve_params import (
        ReaderRetrieveParams as ReaderRetrieveParams,
    )
    from stripe.params.terminal._reader_set_reader_display_params import (
        ReaderSetReaderDisplayParams as ReaderSetReaderDisplayParams,
        ReaderSetReaderDisplayParamsCart as ReaderSetReaderDisplayParamsCart,
        ReaderSetReaderDisplayParamsCartLineItem as ReaderSetReaderDisplayParamsCartLineItem,
    )
    from stripe.params.terminal._reader_succeed_input_collection_params import (
        ReaderSucceedInputCollectionParams as ReaderSucceedInputCollectionParams,
    )
    from stripe.params.terminal._reader_timeout_input_collection_params import (
        ReaderTimeoutInputCollectionParams as ReaderTimeoutInputCollectionParams,
    )
    from stripe.params.terminal._reader_update_params import (
        ReaderUpdateParams as ReaderUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ConfigurationCreateParams": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsBbposWisepad3": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsBbposWiseposE": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsCellular": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsOffline": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsRebootWindow": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsStripeS700": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsStripeS710": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTipping": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingAed": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingAud": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingCad": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingChf": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingCzk": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingDkk": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingEur": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingGbp": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingGip": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingHkd": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingHuf": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingJpy": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingMxn": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingMyr": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingNok": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingNzd": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingPln": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingRon": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingSek": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingSgd": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsTippingUsd": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsVerifoneP400": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsWifi": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsWifiEnterpriseEapPeap": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsWifiEnterpriseEapTls": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsWifiPersonalPsk": (
        "stripe.params.terminal._configuration_create_params",
        False,
    ),
    "ConfigurationDeleteParams": (
        "stripe.params.terminal._configuration_delete_params",
        False,
    ),
    "ConfigurationListParams": (
        "stripe.params.terminal._configuration_list_params",
        False,
    ),
    "ConfigurationModifyParams": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsBbposWisepad3": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsBbposWiseposE": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsCellular": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsOffline": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsRebootWindow": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsStripeS700": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsStripeS710": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTipping": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingAed": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingAud": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingCad": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingChf": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingCzk": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingDkk": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingEur": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingGbp": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingGip": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingHkd": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingHuf": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingJpy": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingMxn": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingMyr": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingNok": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingNzd": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingPln": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingRon": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingSek": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingSgd": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsTippingUsd": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsVerifoneP400": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsWifi": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsWifiEnterpriseEapPeap": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsWifiEnterpriseEapTls": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsWifiPersonalPsk": (
        "stripe.params.terminal._configuration_modify_params",
        False,
    ),
    "ConfigurationRetrieveParams": (
        "stripe.params.terminal._configuration_retrieve_params",
        False,
    ),
    "ConfigurationUpdateParams": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsBbposWisepad3": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsBbposWiseposE": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsCellular": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsOffline": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsRebootWindow": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsStripeS700": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsStripeS710": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTipping": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingAed": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingAud": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingCad": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingChf": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingCzk": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingDkk": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingEur": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingGbp": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingGip": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingHkd": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingHuf": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingJpy": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingMxn": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingMyr": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingNok": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingNzd": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingPln": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingRon": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingSek": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingSgd": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsTippingUsd": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsVerifoneP400": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsWifi": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsWifiEnterpriseEapPeap": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsWifiEnterpriseEapTls": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsWifiPersonalPsk": (
        "stripe.params.terminal._configuration_update_params",
        False,
    ),
    "ConnectionTokenCreateParams": (
        "stripe.params.terminal._connection_token_create_params",
        False,
    ),
    "LocationCreateParams": (
        "stripe.params.terminal._location_create_params",
        False,
    ),
    "LocationCreateParamsAddress": (
        "stripe.params.terminal._location_create_params",
        False,
    ),
    "LocationCreateParamsAddressKana": (
        "stripe.params.terminal._location_create_params",
        False,
    ),
    "LocationCreateParamsAddressKanji": (
        "stripe.params.terminal._location_create_params",
        False,
    ),
    "LocationDeleteParams": (
        "stripe.params.terminal._location_delete_params",
        False,
    ),
    "LocationListParams": (
        "stripe.params.terminal._location_list_params",
        False,
    ),
    "LocationModifyParams": (
        "stripe.params.terminal._location_modify_params",
        False,
    ),
    "LocationModifyParamsAddress": (
        "stripe.params.terminal._location_modify_params",
        False,
    ),
    "LocationModifyParamsAddressKana": (
        "stripe.params.terminal._location_modify_params",
        False,
    ),
    "LocationModifyParamsAddressKanji": (
        "stripe.params.terminal._location_modify_params",
        False,
    ),
    "LocationRetrieveParams": (
        "stripe.params.terminal._location_retrieve_params",
        False,
    ),
    "LocationUpdateParams": (
        "stripe.params.terminal._location_update_params",
        False,
    ),
    "LocationUpdateParamsAddress": (
        "stripe.params.terminal._location_update_params",
        False,
    ),
    "LocationUpdateParamsAddressKana": (
        "stripe.params.terminal._location_update_params",
        False,
    ),
    "LocationUpdateParamsAddressKanji": (
        "stripe.params.terminal._location_update_params",
        False,
    ),
    "OnboardingLinkCreateParams": (
        "stripe.params.terminal._onboarding_link_create_params",
        False,
    ),
    "OnboardingLinkCreateParamsLinkOptions": (
        "stripe.params.terminal._onboarding_link_create_params",
        False,
    ),
    "OnboardingLinkCreateParamsLinkOptionsAppleTermsAndConditions": (
        "stripe.params.terminal._onboarding_link_create_params",
        False,
    ),
    "ReaderCancelActionParams": (
        "stripe.params.terminal._reader_cancel_action_params",
        False,
    ),
    "ReaderCollectInputsParams": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectInputsParamsInput": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectInputsParamsInputCustomText": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectInputsParamsInputSelection": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectInputsParamsInputSelectionChoice": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectInputsParamsInputToggle": (
        "stripe.params.terminal._reader_collect_inputs_params",
        False,
    ),
    "ReaderCollectPaymentMethodParams": (
        "stripe.params.terminal._reader_collect_payment_method_params",
        False,
    ),
    "ReaderCollectPaymentMethodParamsCollectConfig": (
        "stripe.params.terminal._reader_collect_payment_method_params",
        False,
    ),
    "ReaderCollectPaymentMethodParamsCollectConfigTipping": (
        "stripe.params.terminal._reader_collect_payment_method_params",
        False,
    ),
    "ReaderConfirmPaymentIntentParams": (
        "stripe.params.terminal._reader_confirm_payment_intent_params",
        False,
    ),
    "ReaderConfirmPaymentIntentParamsConfirmConfig": (
        "stripe.params.terminal._reader_confirm_payment_intent_params",
        False,
    ),
    "ReaderCreateParams": (
        "stripe.params.terminal._reader_create_params",
        False,
    ),
    "ReaderDeleteParams": (
        "stripe.params.terminal._reader_delete_params",
        False,
    ),
    "ReaderListParams": ("stripe.params.terminal._reader_list_params", False),
    "ReaderModifyParams": (
        "stripe.params.terminal._reader_modify_params",
        False,
    ),
    "ReaderPresentPaymentMethodParams": (
        "stripe.params.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsCard": (
        "stripe.params.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsCardPresent": (
        "stripe.params.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsInteracPresent": (
        "stripe.params.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderProcessPaymentIntentParams": (
        "stripe.params.terminal._reader_process_payment_intent_params",
        False,
    ),
    "ReaderProcessPaymentIntentParamsProcessConfig": (
        "stripe.params.terminal._reader_process_payment_intent_params",
        False,
    ),
    "ReaderProcessPaymentIntentParamsProcessConfigTipping": (
        "stripe.params.terminal._reader_process_payment_intent_params",
        False,
    ),
    "ReaderProcessSetupIntentParams": (
        "stripe.params.terminal._reader_process_setup_intent_params",
        False,
    ),
    "ReaderProcessSetupIntentParamsProcessConfig": (
        "stripe.params.terminal._reader_process_setup_intent_params",
        False,
    ),
    "ReaderRefundPaymentParams": (
        "stripe.params.terminal._reader_refund_payment_params",
        False,
    ),
    "ReaderRefundPaymentParamsRefundPaymentConfig": (
        "stripe.params.terminal._reader_refund_payment_params",
        False,
    ),
    "ReaderRetrieveParams": (
        "stripe.params.terminal._reader_retrieve_params",
        False,
    ),
    "ReaderSetReaderDisplayParams": (
        "stripe.params.terminal._reader_set_reader_display_params",
        False,
    ),
    "ReaderSetReaderDisplayParamsCart": (
        "stripe.params.terminal._reader_set_reader_display_params",
        False,
    ),
    "ReaderSetReaderDisplayParamsCartLineItem": (
        "stripe.params.terminal._reader_set_reader_display_params",
        False,
    ),
    "ReaderSucceedInputCollectionParams": (
        "stripe.params.terminal._reader_succeed_input_collection_params",
        False,
    ),
    "ReaderTimeoutInputCollectionParams": (
        "stripe.params.terminal._reader_timeout_input_collection_params",
        False,
    ),
    "ReaderUpdateParams": (
        "stripe.params.terminal._reader_update_params",
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
