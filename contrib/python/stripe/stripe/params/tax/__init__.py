# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.tax._association_find_params import (
        AssociationFindParams as AssociationFindParams,
    )
    from stripe.params.tax._calculation_create_params import (
        CalculationCreateParams as CalculationCreateParams,
        CalculationCreateParamsCustomerDetails as CalculationCreateParamsCustomerDetails,
        CalculationCreateParamsCustomerDetailsAddress as CalculationCreateParamsCustomerDetailsAddress,
        CalculationCreateParamsCustomerDetailsTaxId as CalculationCreateParamsCustomerDetailsTaxId,
        CalculationCreateParamsLineItem as CalculationCreateParamsLineItem,
        CalculationCreateParamsShipFromDetails as CalculationCreateParamsShipFromDetails,
        CalculationCreateParamsShipFromDetailsAddress as CalculationCreateParamsShipFromDetailsAddress,
        CalculationCreateParamsShippingCost as CalculationCreateParamsShippingCost,
    )
    from stripe.params.tax._calculation_line_item_list_params import (
        CalculationLineItemListParams as CalculationLineItemListParams,
    )
    from stripe.params.tax._calculation_list_line_items_params import (
        CalculationListLineItemsParams as CalculationListLineItemsParams,
    )
    from stripe.params.tax._calculation_retrieve_params import (
        CalculationRetrieveParams as CalculationRetrieveParams,
    )
    from stripe.params.tax._registration_create_params import (
        RegistrationCreateParams as RegistrationCreateParams,
        RegistrationCreateParamsCountryOptions as RegistrationCreateParamsCountryOptions,
        RegistrationCreateParamsCountryOptionsAe as RegistrationCreateParamsCountryOptionsAe,
        RegistrationCreateParamsCountryOptionsAeStandard as RegistrationCreateParamsCountryOptionsAeStandard,
        RegistrationCreateParamsCountryOptionsAl as RegistrationCreateParamsCountryOptionsAl,
        RegistrationCreateParamsCountryOptionsAlStandard as RegistrationCreateParamsCountryOptionsAlStandard,
        RegistrationCreateParamsCountryOptionsAm as RegistrationCreateParamsCountryOptionsAm,
        RegistrationCreateParamsCountryOptionsAo as RegistrationCreateParamsCountryOptionsAo,
        RegistrationCreateParamsCountryOptionsAoStandard as RegistrationCreateParamsCountryOptionsAoStandard,
        RegistrationCreateParamsCountryOptionsAt as RegistrationCreateParamsCountryOptionsAt,
        RegistrationCreateParamsCountryOptionsAtStandard as RegistrationCreateParamsCountryOptionsAtStandard,
        RegistrationCreateParamsCountryOptionsAu as RegistrationCreateParamsCountryOptionsAu,
        RegistrationCreateParamsCountryOptionsAuStandard as RegistrationCreateParamsCountryOptionsAuStandard,
        RegistrationCreateParamsCountryOptionsAw as RegistrationCreateParamsCountryOptionsAw,
        RegistrationCreateParamsCountryOptionsAwStandard as RegistrationCreateParamsCountryOptionsAwStandard,
        RegistrationCreateParamsCountryOptionsAz as RegistrationCreateParamsCountryOptionsAz,
        RegistrationCreateParamsCountryOptionsBa as RegistrationCreateParamsCountryOptionsBa,
        RegistrationCreateParamsCountryOptionsBaStandard as RegistrationCreateParamsCountryOptionsBaStandard,
        RegistrationCreateParamsCountryOptionsBb as RegistrationCreateParamsCountryOptionsBb,
        RegistrationCreateParamsCountryOptionsBbStandard as RegistrationCreateParamsCountryOptionsBbStandard,
        RegistrationCreateParamsCountryOptionsBd as RegistrationCreateParamsCountryOptionsBd,
        RegistrationCreateParamsCountryOptionsBdStandard as RegistrationCreateParamsCountryOptionsBdStandard,
        RegistrationCreateParamsCountryOptionsBe as RegistrationCreateParamsCountryOptionsBe,
        RegistrationCreateParamsCountryOptionsBeStandard as RegistrationCreateParamsCountryOptionsBeStandard,
        RegistrationCreateParamsCountryOptionsBf as RegistrationCreateParamsCountryOptionsBf,
        RegistrationCreateParamsCountryOptionsBfStandard as RegistrationCreateParamsCountryOptionsBfStandard,
        RegistrationCreateParamsCountryOptionsBg as RegistrationCreateParamsCountryOptionsBg,
        RegistrationCreateParamsCountryOptionsBgStandard as RegistrationCreateParamsCountryOptionsBgStandard,
        RegistrationCreateParamsCountryOptionsBh as RegistrationCreateParamsCountryOptionsBh,
        RegistrationCreateParamsCountryOptionsBhStandard as RegistrationCreateParamsCountryOptionsBhStandard,
        RegistrationCreateParamsCountryOptionsBj as RegistrationCreateParamsCountryOptionsBj,
        RegistrationCreateParamsCountryOptionsBs as RegistrationCreateParamsCountryOptionsBs,
        RegistrationCreateParamsCountryOptionsBsStandard as RegistrationCreateParamsCountryOptionsBsStandard,
        RegistrationCreateParamsCountryOptionsBy as RegistrationCreateParamsCountryOptionsBy,
        RegistrationCreateParamsCountryOptionsCa as RegistrationCreateParamsCountryOptionsCa,
        RegistrationCreateParamsCountryOptionsCaProvinceStandard as RegistrationCreateParamsCountryOptionsCaProvinceStandard,
        RegistrationCreateParamsCountryOptionsCd as RegistrationCreateParamsCountryOptionsCd,
        RegistrationCreateParamsCountryOptionsCdStandard as RegistrationCreateParamsCountryOptionsCdStandard,
        RegistrationCreateParamsCountryOptionsCh as RegistrationCreateParamsCountryOptionsCh,
        RegistrationCreateParamsCountryOptionsChStandard as RegistrationCreateParamsCountryOptionsChStandard,
        RegistrationCreateParamsCountryOptionsCl as RegistrationCreateParamsCountryOptionsCl,
        RegistrationCreateParamsCountryOptionsCm as RegistrationCreateParamsCountryOptionsCm,
        RegistrationCreateParamsCountryOptionsCo as RegistrationCreateParamsCountryOptionsCo,
        RegistrationCreateParamsCountryOptionsCr as RegistrationCreateParamsCountryOptionsCr,
        RegistrationCreateParamsCountryOptionsCv as RegistrationCreateParamsCountryOptionsCv,
        RegistrationCreateParamsCountryOptionsCy as RegistrationCreateParamsCountryOptionsCy,
        RegistrationCreateParamsCountryOptionsCyStandard as RegistrationCreateParamsCountryOptionsCyStandard,
        RegistrationCreateParamsCountryOptionsCz as RegistrationCreateParamsCountryOptionsCz,
        RegistrationCreateParamsCountryOptionsCzStandard as RegistrationCreateParamsCountryOptionsCzStandard,
        RegistrationCreateParamsCountryOptionsDe as RegistrationCreateParamsCountryOptionsDe,
        RegistrationCreateParamsCountryOptionsDeStandard as RegistrationCreateParamsCountryOptionsDeStandard,
        RegistrationCreateParamsCountryOptionsDk as RegistrationCreateParamsCountryOptionsDk,
        RegistrationCreateParamsCountryOptionsDkStandard as RegistrationCreateParamsCountryOptionsDkStandard,
        RegistrationCreateParamsCountryOptionsEc as RegistrationCreateParamsCountryOptionsEc,
        RegistrationCreateParamsCountryOptionsEe as RegistrationCreateParamsCountryOptionsEe,
        RegistrationCreateParamsCountryOptionsEeStandard as RegistrationCreateParamsCountryOptionsEeStandard,
        RegistrationCreateParamsCountryOptionsEg as RegistrationCreateParamsCountryOptionsEg,
        RegistrationCreateParamsCountryOptionsEs as RegistrationCreateParamsCountryOptionsEs,
        RegistrationCreateParamsCountryOptionsEsStandard as RegistrationCreateParamsCountryOptionsEsStandard,
        RegistrationCreateParamsCountryOptionsEt as RegistrationCreateParamsCountryOptionsEt,
        RegistrationCreateParamsCountryOptionsEtStandard as RegistrationCreateParamsCountryOptionsEtStandard,
        RegistrationCreateParamsCountryOptionsFi as RegistrationCreateParamsCountryOptionsFi,
        RegistrationCreateParamsCountryOptionsFiStandard as RegistrationCreateParamsCountryOptionsFiStandard,
        RegistrationCreateParamsCountryOptionsFr as RegistrationCreateParamsCountryOptionsFr,
        RegistrationCreateParamsCountryOptionsFrStandard as RegistrationCreateParamsCountryOptionsFrStandard,
        RegistrationCreateParamsCountryOptionsGb as RegistrationCreateParamsCountryOptionsGb,
        RegistrationCreateParamsCountryOptionsGbStandard as RegistrationCreateParamsCountryOptionsGbStandard,
        RegistrationCreateParamsCountryOptionsGe as RegistrationCreateParamsCountryOptionsGe,
        RegistrationCreateParamsCountryOptionsGn as RegistrationCreateParamsCountryOptionsGn,
        RegistrationCreateParamsCountryOptionsGnStandard as RegistrationCreateParamsCountryOptionsGnStandard,
        RegistrationCreateParamsCountryOptionsGr as RegistrationCreateParamsCountryOptionsGr,
        RegistrationCreateParamsCountryOptionsGrStandard as RegistrationCreateParamsCountryOptionsGrStandard,
        RegistrationCreateParamsCountryOptionsHr as RegistrationCreateParamsCountryOptionsHr,
        RegistrationCreateParamsCountryOptionsHrStandard as RegistrationCreateParamsCountryOptionsHrStandard,
        RegistrationCreateParamsCountryOptionsHu as RegistrationCreateParamsCountryOptionsHu,
        RegistrationCreateParamsCountryOptionsHuStandard as RegistrationCreateParamsCountryOptionsHuStandard,
        RegistrationCreateParamsCountryOptionsId as RegistrationCreateParamsCountryOptionsId,
        RegistrationCreateParamsCountryOptionsIe as RegistrationCreateParamsCountryOptionsIe,
        RegistrationCreateParamsCountryOptionsIeStandard as RegistrationCreateParamsCountryOptionsIeStandard,
        RegistrationCreateParamsCountryOptionsIn as RegistrationCreateParamsCountryOptionsIn,
        RegistrationCreateParamsCountryOptionsIs as RegistrationCreateParamsCountryOptionsIs,
        RegistrationCreateParamsCountryOptionsIsStandard as RegistrationCreateParamsCountryOptionsIsStandard,
        RegistrationCreateParamsCountryOptionsIt as RegistrationCreateParamsCountryOptionsIt,
        RegistrationCreateParamsCountryOptionsItStandard as RegistrationCreateParamsCountryOptionsItStandard,
        RegistrationCreateParamsCountryOptionsJp as RegistrationCreateParamsCountryOptionsJp,
        RegistrationCreateParamsCountryOptionsJpStandard as RegistrationCreateParamsCountryOptionsJpStandard,
        RegistrationCreateParamsCountryOptionsKe as RegistrationCreateParamsCountryOptionsKe,
        RegistrationCreateParamsCountryOptionsKg as RegistrationCreateParamsCountryOptionsKg,
        RegistrationCreateParamsCountryOptionsKh as RegistrationCreateParamsCountryOptionsKh,
        RegistrationCreateParamsCountryOptionsKr as RegistrationCreateParamsCountryOptionsKr,
        RegistrationCreateParamsCountryOptionsKz as RegistrationCreateParamsCountryOptionsKz,
        RegistrationCreateParamsCountryOptionsLa as RegistrationCreateParamsCountryOptionsLa,
        RegistrationCreateParamsCountryOptionsLk as RegistrationCreateParamsCountryOptionsLk,
        RegistrationCreateParamsCountryOptionsLt as RegistrationCreateParamsCountryOptionsLt,
        RegistrationCreateParamsCountryOptionsLtStandard as RegistrationCreateParamsCountryOptionsLtStandard,
        RegistrationCreateParamsCountryOptionsLu as RegistrationCreateParamsCountryOptionsLu,
        RegistrationCreateParamsCountryOptionsLuStandard as RegistrationCreateParamsCountryOptionsLuStandard,
        RegistrationCreateParamsCountryOptionsLv as RegistrationCreateParamsCountryOptionsLv,
        RegistrationCreateParamsCountryOptionsLvStandard as RegistrationCreateParamsCountryOptionsLvStandard,
        RegistrationCreateParamsCountryOptionsMa as RegistrationCreateParamsCountryOptionsMa,
        RegistrationCreateParamsCountryOptionsMd as RegistrationCreateParamsCountryOptionsMd,
        RegistrationCreateParamsCountryOptionsMe as RegistrationCreateParamsCountryOptionsMe,
        RegistrationCreateParamsCountryOptionsMeStandard as RegistrationCreateParamsCountryOptionsMeStandard,
        RegistrationCreateParamsCountryOptionsMk as RegistrationCreateParamsCountryOptionsMk,
        RegistrationCreateParamsCountryOptionsMkStandard as RegistrationCreateParamsCountryOptionsMkStandard,
        RegistrationCreateParamsCountryOptionsMr as RegistrationCreateParamsCountryOptionsMr,
        RegistrationCreateParamsCountryOptionsMrStandard as RegistrationCreateParamsCountryOptionsMrStandard,
        RegistrationCreateParamsCountryOptionsMt as RegistrationCreateParamsCountryOptionsMt,
        RegistrationCreateParamsCountryOptionsMtStandard as RegistrationCreateParamsCountryOptionsMtStandard,
        RegistrationCreateParamsCountryOptionsMx as RegistrationCreateParamsCountryOptionsMx,
        RegistrationCreateParamsCountryOptionsMy as RegistrationCreateParamsCountryOptionsMy,
        RegistrationCreateParamsCountryOptionsNg as RegistrationCreateParamsCountryOptionsNg,
        RegistrationCreateParamsCountryOptionsNl as RegistrationCreateParamsCountryOptionsNl,
        RegistrationCreateParamsCountryOptionsNlStandard as RegistrationCreateParamsCountryOptionsNlStandard,
        RegistrationCreateParamsCountryOptionsNo as RegistrationCreateParamsCountryOptionsNo,
        RegistrationCreateParamsCountryOptionsNoStandard as RegistrationCreateParamsCountryOptionsNoStandard,
        RegistrationCreateParamsCountryOptionsNp as RegistrationCreateParamsCountryOptionsNp,
        RegistrationCreateParamsCountryOptionsNz as RegistrationCreateParamsCountryOptionsNz,
        RegistrationCreateParamsCountryOptionsNzStandard as RegistrationCreateParamsCountryOptionsNzStandard,
        RegistrationCreateParamsCountryOptionsOm as RegistrationCreateParamsCountryOptionsOm,
        RegistrationCreateParamsCountryOptionsOmStandard as RegistrationCreateParamsCountryOptionsOmStandard,
        RegistrationCreateParamsCountryOptionsPe as RegistrationCreateParamsCountryOptionsPe,
        RegistrationCreateParamsCountryOptionsPh as RegistrationCreateParamsCountryOptionsPh,
        RegistrationCreateParamsCountryOptionsPl as RegistrationCreateParamsCountryOptionsPl,
        RegistrationCreateParamsCountryOptionsPlStandard as RegistrationCreateParamsCountryOptionsPlStandard,
        RegistrationCreateParamsCountryOptionsPt as RegistrationCreateParamsCountryOptionsPt,
        RegistrationCreateParamsCountryOptionsPtStandard as RegistrationCreateParamsCountryOptionsPtStandard,
        RegistrationCreateParamsCountryOptionsRo as RegistrationCreateParamsCountryOptionsRo,
        RegistrationCreateParamsCountryOptionsRoStandard as RegistrationCreateParamsCountryOptionsRoStandard,
        RegistrationCreateParamsCountryOptionsRs as RegistrationCreateParamsCountryOptionsRs,
        RegistrationCreateParamsCountryOptionsRsStandard as RegistrationCreateParamsCountryOptionsRsStandard,
        RegistrationCreateParamsCountryOptionsRu as RegistrationCreateParamsCountryOptionsRu,
        RegistrationCreateParamsCountryOptionsSa as RegistrationCreateParamsCountryOptionsSa,
        RegistrationCreateParamsCountryOptionsSe as RegistrationCreateParamsCountryOptionsSe,
        RegistrationCreateParamsCountryOptionsSeStandard as RegistrationCreateParamsCountryOptionsSeStandard,
        RegistrationCreateParamsCountryOptionsSg as RegistrationCreateParamsCountryOptionsSg,
        RegistrationCreateParamsCountryOptionsSgStandard as RegistrationCreateParamsCountryOptionsSgStandard,
        RegistrationCreateParamsCountryOptionsSi as RegistrationCreateParamsCountryOptionsSi,
        RegistrationCreateParamsCountryOptionsSiStandard as RegistrationCreateParamsCountryOptionsSiStandard,
        RegistrationCreateParamsCountryOptionsSk as RegistrationCreateParamsCountryOptionsSk,
        RegistrationCreateParamsCountryOptionsSkStandard as RegistrationCreateParamsCountryOptionsSkStandard,
        RegistrationCreateParamsCountryOptionsSn as RegistrationCreateParamsCountryOptionsSn,
        RegistrationCreateParamsCountryOptionsSr as RegistrationCreateParamsCountryOptionsSr,
        RegistrationCreateParamsCountryOptionsSrStandard as RegistrationCreateParamsCountryOptionsSrStandard,
        RegistrationCreateParamsCountryOptionsTh as RegistrationCreateParamsCountryOptionsTh,
        RegistrationCreateParamsCountryOptionsTj as RegistrationCreateParamsCountryOptionsTj,
        RegistrationCreateParamsCountryOptionsTr as RegistrationCreateParamsCountryOptionsTr,
        RegistrationCreateParamsCountryOptionsTw as RegistrationCreateParamsCountryOptionsTw,
        RegistrationCreateParamsCountryOptionsTz as RegistrationCreateParamsCountryOptionsTz,
        RegistrationCreateParamsCountryOptionsUa as RegistrationCreateParamsCountryOptionsUa,
        RegistrationCreateParamsCountryOptionsUg as RegistrationCreateParamsCountryOptionsUg,
        RegistrationCreateParamsCountryOptionsUs as RegistrationCreateParamsCountryOptionsUs,
        RegistrationCreateParamsCountryOptionsUsLocalAmusementTax as RegistrationCreateParamsCountryOptionsUsLocalAmusementTax,
        RegistrationCreateParamsCountryOptionsUsLocalLeaseTax as RegistrationCreateParamsCountryOptionsUsLocalLeaseTax,
        RegistrationCreateParamsCountryOptionsUsStateSalesTax as RegistrationCreateParamsCountryOptionsUsStateSalesTax,
        RegistrationCreateParamsCountryOptionsUsStateSalesTaxElection as RegistrationCreateParamsCountryOptionsUsStateSalesTaxElection,
        RegistrationCreateParamsCountryOptionsUy as RegistrationCreateParamsCountryOptionsUy,
        RegistrationCreateParamsCountryOptionsUyStandard as RegistrationCreateParamsCountryOptionsUyStandard,
        RegistrationCreateParamsCountryOptionsUz as RegistrationCreateParamsCountryOptionsUz,
        RegistrationCreateParamsCountryOptionsVn as RegistrationCreateParamsCountryOptionsVn,
        RegistrationCreateParamsCountryOptionsZa as RegistrationCreateParamsCountryOptionsZa,
        RegistrationCreateParamsCountryOptionsZaStandard as RegistrationCreateParamsCountryOptionsZaStandard,
        RegistrationCreateParamsCountryOptionsZm as RegistrationCreateParamsCountryOptionsZm,
        RegistrationCreateParamsCountryOptionsZw as RegistrationCreateParamsCountryOptionsZw,
        RegistrationCreateParamsCountryOptionsZwStandard as RegistrationCreateParamsCountryOptionsZwStandard,
    )
    from stripe.params.tax._registration_list_params import (
        RegistrationListParams as RegistrationListParams,
    )
    from stripe.params.tax._registration_modify_params import (
        RegistrationModifyParams as RegistrationModifyParams,
    )
    from stripe.params.tax._registration_retrieve_params import (
        RegistrationRetrieveParams as RegistrationRetrieveParams,
    )
    from stripe.params.tax._registration_update_params import (
        RegistrationUpdateParams as RegistrationUpdateParams,
    )
    from stripe.params.tax._settings_modify_params import (
        SettingsModifyParams as SettingsModifyParams,
        SettingsModifyParamsDefaults as SettingsModifyParamsDefaults,
        SettingsModifyParamsHeadOffice as SettingsModifyParamsHeadOffice,
        SettingsModifyParamsHeadOfficeAddress as SettingsModifyParamsHeadOfficeAddress,
    )
    from stripe.params.tax._settings_retrieve_params import (
        SettingsRetrieveParams as SettingsRetrieveParams,
    )
    from stripe.params.tax._settings_update_params import (
        SettingsUpdateParams as SettingsUpdateParams,
        SettingsUpdateParamsDefaults as SettingsUpdateParamsDefaults,
        SettingsUpdateParamsHeadOffice as SettingsUpdateParamsHeadOffice,
        SettingsUpdateParamsHeadOfficeAddress as SettingsUpdateParamsHeadOfficeAddress,
    )
    from stripe.params.tax._transaction_create_from_calculation_params import (
        TransactionCreateFromCalculationParams as TransactionCreateFromCalculationParams,
    )
    from stripe.params.tax._transaction_create_reversal_params import (
        TransactionCreateReversalParams as TransactionCreateReversalParams,
        TransactionCreateReversalParamsLineItem as TransactionCreateReversalParamsLineItem,
        TransactionCreateReversalParamsShippingCost as TransactionCreateReversalParamsShippingCost,
    )
    from stripe.params.tax._transaction_line_item_list_params import (
        TransactionLineItemListParams as TransactionLineItemListParams,
    )
    from stripe.params.tax._transaction_list_line_items_params import (
        TransactionListLineItemsParams as TransactionListLineItemsParams,
    )
    from stripe.params.tax._transaction_retrieve_params import (
        TransactionRetrieveParams as TransactionRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AssociationFindParams": (
        "stripe.params.tax._association_find_params",
        False,
    ),
    "CalculationCreateParams": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsCustomerDetails": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsCustomerDetailsAddress": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsCustomerDetailsTaxId": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsLineItem": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsShipFromDetails": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsShipFromDetailsAddress": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationCreateParamsShippingCost": (
        "stripe.params.tax._calculation_create_params",
        False,
    ),
    "CalculationLineItemListParams": (
        "stripe.params.tax._calculation_line_item_list_params",
        False,
    ),
    "CalculationListLineItemsParams": (
        "stripe.params.tax._calculation_list_line_items_params",
        False,
    ),
    "CalculationRetrieveParams": (
        "stripe.params.tax._calculation_retrieve_params",
        False,
    ),
    "RegistrationCreateParams": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptions": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAl": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAlStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAm": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAo": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAoStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAtStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAu": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAuStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAw": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAwStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsAz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBaStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBb": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBbStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBd": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBdStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBf": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBfStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBgStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBh": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBhStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBj": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBs": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBsStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsBy": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCaProvinceStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCd": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCdStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCh": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsChStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCl": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCm": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCo": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCv": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCy": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCyStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsCzStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsDe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsDeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsDk": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsDkStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEc": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEs": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEsStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsEtStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsFi": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsFiStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsFr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsFrStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGb": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGbStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGn": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGnStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsGrStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsHr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsHrStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsHu": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsHuStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsId": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIn": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIs": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIsStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsIt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsItStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsJp": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsJpStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsKe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsKg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsKh": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsKr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsKz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLk": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLtStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLu": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLuStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLv": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsLvStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMd": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMk": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMkStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMrStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMtStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMx": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsMy": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNl": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNlStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNo": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNoStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNp": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsNzStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsOm": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsOmStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPh": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPl": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPlStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPt": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsPtStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsRo": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsRoStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsRs": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsRsStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsRu": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSe": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSeStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSgStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSi": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSiStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSk": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSkStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSn": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsSrStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsTh": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsTj": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsTr": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsTw": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsTz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUg": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUs": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUsLocalAmusementTax": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUsLocalLeaseTax": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUsStateSalesTax": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUsStateSalesTaxElection": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUy": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUyStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsUz": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsVn": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsZa": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsZaStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsZm": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsZw": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationCreateParamsCountryOptionsZwStandard": (
        "stripe.params.tax._registration_create_params",
        False,
    ),
    "RegistrationListParams": (
        "stripe.params.tax._registration_list_params",
        False,
    ),
    "RegistrationModifyParams": (
        "stripe.params.tax._registration_modify_params",
        False,
    ),
    "RegistrationRetrieveParams": (
        "stripe.params.tax._registration_retrieve_params",
        False,
    ),
    "RegistrationUpdateParams": (
        "stripe.params.tax._registration_update_params",
        False,
    ),
    "SettingsModifyParams": (
        "stripe.params.tax._settings_modify_params",
        False,
    ),
    "SettingsModifyParamsDefaults": (
        "stripe.params.tax._settings_modify_params",
        False,
    ),
    "SettingsModifyParamsHeadOffice": (
        "stripe.params.tax._settings_modify_params",
        False,
    ),
    "SettingsModifyParamsHeadOfficeAddress": (
        "stripe.params.tax._settings_modify_params",
        False,
    ),
    "SettingsRetrieveParams": (
        "stripe.params.tax._settings_retrieve_params",
        False,
    ),
    "SettingsUpdateParams": (
        "stripe.params.tax._settings_update_params",
        False,
    ),
    "SettingsUpdateParamsDefaults": (
        "stripe.params.tax._settings_update_params",
        False,
    ),
    "SettingsUpdateParamsHeadOffice": (
        "stripe.params.tax._settings_update_params",
        False,
    ),
    "SettingsUpdateParamsHeadOfficeAddress": (
        "stripe.params.tax._settings_update_params",
        False,
    ),
    "TransactionCreateFromCalculationParams": (
        "stripe.params.tax._transaction_create_from_calculation_params",
        False,
    ),
    "TransactionCreateReversalParams": (
        "stripe.params.tax._transaction_create_reversal_params",
        False,
    ),
    "TransactionCreateReversalParamsLineItem": (
        "stripe.params.tax._transaction_create_reversal_params",
        False,
    ),
    "TransactionCreateReversalParamsShippingCost": (
        "stripe.params.tax._transaction_create_reversal_params",
        False,
    ),
    "TransactionLineItemListParams": (
        "stripe.params.tax._transaction_line_item_list_params",
        False,
    ),
    "TransactionListLineItemsParams": (
        "stripe.params.tax._transaction_list_line_items_params",
        False,
    ),
    "TransactionRetrieveParams": (
        "stripe.params.tax._transaction_retrieve_params",
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
