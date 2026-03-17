# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class RegistrationCreateParams(RequestOptions):
    active_from: Union[Literal["now"], int]
    """
    Time at which the Tax Registration becomes active. It can be either `now` to indicate the current time, or a future timestamp measured in seconds since the Unix epoch.
    """
    country: str
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    country_options: "RegistrationCreateParamsCountryOptions"
    """
    Specific options for a registration in the specified `country`.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired[int]
    """
    If set, the Tax Registration stops being active at this time. If not set, the Tax Registration will be active indefinitely. Timestamp measured in seconds since the Unix epoch.
    """


_RegistrationCreateParamsCountryOptionsBase = TypedDict(
    "RegistrationCreateParamsCountryOptions",
    {
        "in": NotRequired["RegistrationCreateParamsCountryOptionsIn"],
        "is": NotRequired["RegistrationCreateParamsCountryOptionsIs"],
    },
)


class RegistrationCreateParamsCountryOptions(
    _RegistrationCreateParamsCountryOptionsBase,
):
    ae: NotRequired["RegistrationCreateParamsCountryOptionsAe"]
    """
    Options for the registration in AE.
    """
    al: NotRequired["RegistrationCreateParamsCountryOptionsAl"]
    """
    Options for the registration in AL.
    """
    am: NotRequired["RegistrationCreateParamsCountryOptionsAm"]
    """
    Options for the registration in AM.
    """
    ao: NotRequired["RegistrationCreateParamsCountryOptionsAo"]
    """
    Options for the registration in AO.
    """
    at: NotRequired["RegistrationCreateParamsCountryOptionsAt"]
    """
    Options for the registration in AT.
    """
    au: NotRequired["RegistrationCreateParamsCountryOptionsAu"]
    """
    Options for the registration in AU.
    """
    aw: NotRequired["RegistrationCreateParamsCountryOptionsAw"]
    """
    Options for the registration in AW.
    """
    az: NotRequired["RegistrationCreateParamsCountryOptionsAz"]
    """
    Options for the registration in AZ.
    """
    ba: NotRequired["RegistrationCreateParamsCountryOptionsBa"]
    """
    Options for the registration in BA.
    """
    bb: NotRequired["RegistrationCreateParamsCountryOptionsBb"]
    """
    Options for the registration in BB.
    """
    bd: NotRequired["RegistrationCreateParamsCountryOptionsBd"]
    """
    Options for the registration in BD.
    """
    be: NotRequired["RegistrationCreateParamsCountryOptionsBe"]
    """
    Options for the registration in BE.
    """
    bf: NotRequired["RegistrationCreateParamsCountryOptionsBf"]
    """
    Options for the registration in BF.
    """
    bg: NotRequired["RegistrationCreateParamsCountryOptionsBg"]
    """
    Options for the registration in BG.
    """
    bh: NotRequired["RegistrationCreateParamsCountryOptionsBh"]
    """
    Options for the registration in BH.
    """
    bj: NotRequired["RegistrationCreateParamsCountryOptionsBj"]
    """
    Options for the registration in BJ.
    """
    bs: NotRequired["RegistrationCreateParamsCountryOptionsBs"]
    """
    Options for the registration in BS.
    """
    by: NotRequired["RegistrationCreateParamsCountryOptionsBy"]
    """
    Options for the registration in BY.
    """
    ca: NotRequired["RegistrationCreateParamsCountryOptionsCa"]
    """
    Options for the registration in CA.
    """
    cd: NotRequired["RegistrationCreateParamsCountryOptionsCd"]
    """
    Options for the registration in CD.
    """
    ch: NotRequired["RegistrationCreateParamsCountryOptionsCh"]
    """
    Options for the registration in CH.
    """
    cl: NotRequired["RegistrationCreateParamsCountryOptionsCl"]
    """
    Options for the registration in CL.
    """
    cm: NotRequired["RegistrationCreateParamsCountryOptionsCm"]
    """
    Options for the registration in CM.
    """
    co: NotRequired["RegistrationCreateParamsCountryOptionsCo"]
    """
    Options for the registration in CO.
    """
    cr: NotRequired["RegistrationCreateParamsCountryOptionsCr"]
    """
    Options for the registration in CR.
    """
    cv: NotRequired["RegistrationCreateParamsCountryOptionsCv"]
    """
    Options for the registration in CV.
    """
    cy: NotRequired["RegistrationCreateParamsCountryOptionsCy"]
    """
    Options for the registration in CY.
    """
    cz: NotRequired["RegistrationCreateParamsCountryOptionsCz"]
    """
    Options for the registration in CZ.
    """
    de: NotRequired["RegistrationCreateParamsCountryOptionsDe"]
    """
    Options for the registration in DE.
    """
    dk: NotRequired["RegistrationCreateParamsCountryOptionsDk"]
    """
    Options for the registration in DK.
    """
    ec: NotRequired["RegistrationCreateParamsCountryOptionsEc"]
    """
    Options for the registration in EC.
    """
    ee: NotRequired["RegistrationCreateParamsCountryOptionsEe"]
    """
    Options for the registration in EE.
    """
    eg: NotRequired["RegistrationCreateParamsCountryOptionsEg"]
    """
    Options for the registration in EG.
    """
    es: NotRequired["RegistrationCreateParamsCountryOptionsEs"]
    """
    Options for the registration in ES.
    """
    et: NotRequired["RegistrationCreateParamsCountryOptionsEt"]
    """
    Options for the registration in ET.
    """
    fi: NotRequired["RegistrationCreateParamsCountryOptionsFi"]
    """
    Options for the registration in FI.
    """
    fr: NotRequired["RegistrationCreateParamsCountryOptionsFr"]
    """
    Options for the registration in FR.
    """
    gb: NotRequired["RegistrationCreateParamsCountryOptionsGb"]
    """
    Options for the registration in GB.
    """
    ge: NotRequired["RegistrationCreateParamsCountryOptionsGe"]
    """
    Options for the registration in GE.
    """
    gn: NotRequired["RegistrationCreateParamsCountryOptionsGn"]
    """
    Options for the registration in GN.
    """
    gr: NotRequired["RegistrationCreateParamsCountryOptionsGr"]
    """
    Options for the registration in GR.
    """
    hr: NotRequired["RegistrationCreateParamsCountryOptionsHr"]
    """
    Options for the registration in HR.
    """
    hu: NotRequired["RegistrationCreateParamsCountryOptionsHu"]
    """
    Options for the registration in HU.
    """
    id: NotRequired["RegistrationCreateParamsCountryOptionsId"]
    """
    Options for the registration in ID.
    """
    ie: NotRequired["RegistrationCreateParamsCountryOptionsIe"]
    """
    Options for the registration in IE.
    """
    it: NotRequired["RegistrationCreateParamsCountryOptionsIt"]
    """
    Options for the registration in IT.
    """
    jp: NotRequired["RegistrationCreateParamsCountryOptionsJp"]
    """
    Options for the registration in JP.
    """
    ke: NotRequired["RegistrationCreateParamsCountryOptionsKe"]
    """
    Options for the registration in KE.
    """
    kg: NotRequired["RegistrationCreateParamsCountryOptionsKg"]
    """
    Options for the registration in KG.
    """
    kh: NotRequired["RegistrationCreateParamsCountryOptionsKh"]
    """
    Options for the registration in KH.
    """
    kr: NotRequired["RegistrationCreateParamsCountryOptionsKr"]
    """
    Options for the registration in KR.
    """
    kz: NotRequired["RegistrationCreateParamsCountryOptionsKz"]
    """
    Options for the registration in KZ.
    """
    la: NotRequired["RegistrationCreateParamsCountryOptionsLa"]
    """
    Options for the registration in LA.
    """
    lk: NotRequired["RegistrationCreateParamsCountryOptionsLk"]
    """
    Options for the registration in LK.
    """
    lt: NotRequired["RegistrationCreateParamsCountryOptionsLt"]
    """
    Options for the registration in LT.
    """
    lu: NotRequired["RegistrationCreateParamsCountryOptionsLu"]
    """
    Options for the registration in LU.
    """
    lv: NotRequired["RegistrationCreateParamsCountryOptionsLv"]
    """
    Options for the registration in LV.
    """
    ma: NotRequired["RegistrationCreateParamsCountryOptionsMa"]
    """
    Options for the registration in MA.
    """
    md: NotRequired["RegistrationCreateParamsCountryOptionsMd"]
    """
    Options for the registration in MD.
    """
    me: NotRequired["RegistrationCreateParamsCountryOptionsMe"]
    """
    Options for the registration in ME.
    """
    mk: NotRequired["RegistrationCreateParamsCountryOptionsMk"]
    """
    Options for the registration in MK.
    """
    mr: NotRequired["RegistrationCreateParamsCountryOptionsMr"]
    """
    Options for the registration in MR.
    """
    mt: NotRequired["RegistrationCreateParamsCountryOptionsMt"]
    """
    Options for the registration in MT.
    """
    mx: NotRequired["RegistrationCreateParamsCountryOptionsMx"]
    """
    Options for the registration in MX.
    """
    my: NotRequired["RegistrationCreateParamsCountryOptionsMy"]
    """
    Options for the registration in MY.
    """
    ng: NotRequired["RegistrationCreateParamsCountryOptionsNg"]
    """
    Options for the registration in NG.
    """
    nl: NotRequired["RegistrationCreateParamsCountryOptionsNl"]
    """
    Options for the registration in NL.
    """
    no: NotRequired["RegistrationCreateParamsCountryOptionsNo"]
    """
    Options for the registration in NO.
    """
    np: NotRequired["RegistrationCreateParamsCountryOptionsNp"]
    """
    Options for the registration in NP.
    """
    nz: NotRequired["RegistrationCreateParamsCountryOptionsNz"]
    """
    Options for the registration in NZ.
    """
    om: NotRequired["RegistrationCreateParamsCountryOptionsOm"]
    """
    Options for the registration in OM.
    """
    pe: NotRequired["RegistrationCreateParamsCountryOptionsPe"]
    """
    Options for the registration in PE.
    """
    ph: NotRequired["RegistrationCreateParamsCountryOptionsPh"]
    """
    Options for the registration in PH.
    """
    pl: NotRequired["RegistrationCreateParamsCountryOptionsPl"]
    """
    Options for the registration in PL.
    """
    pt: NotRequired["RegistrationCreateParamsCountryOptionsPt"]
    """
    Options for the registration in PT.
    """
    ro: NotRequired["RegistrationCreateParamsCountryOptionsRo"]
    """
    Options for the registration in RO.
    """
    rs: NotRequired["RegistrationCreateParamsCountryOptionsRs"]
    """
    Options for the registration in RS.
    """
    ru: NotRequired["RegistrationCreateParamsCountryOptionsRu"]
    """
    Options for the registration in RU.
    """
    sa: NotRequired["RegistrationCreateParamsCountryOptionsSa"]
    """
    Options for the registration in SA.
    """
    se: NotRequired["RegistrationCreateParamsCountryOptionsSe"]
    """
    Options for the registration in SE.
    """
    sg: NotRequired["RegistrationCreateParamsCountryOptionsSg"]
    """
    Options for the registration in SG.
    """
    si: NotRequired["RegistrationCreateParamsCountryOptionsSi"]
    """
    Options for the registration in SI.
    """
    sk: NotRequired["RegistrationCreateParamsCountryOptionsSk"]
    """
    Options for the registration in SK.
    """
    sn: NotRequired["RegistrationCreateParamsCountryOptionsSn"]
    """
    Options for the registration in SN.
    """
    sr: NotRequired["RegistrationCreateParamsCountryOptionsSr"]
    """
    Options for the registration in SR.
    """
    th: NotRequired["RegistrationCreateParamsCountryOptionsTh"]
    """
    Options for the registration in TH.
    """
    tj: NotRequired["RegistrationCreateParamsCountryOptionsTj"]
    """
    Options for the registration in TJ.
    """
    tr: NotRequired["RegistrationCreateParamsCountryOptionsTr"]
    """
    Options for the registration in TR.
    """
    tw: NotRequired["RegistrationCreateParamsCountryOptionsTw"]
    """
    Options for the registration in TW.
    """
    tz: NotRequired["RegistrationCreateParamsCountryOptionsTz"]
    """
    Options for the registration in TZ.
    """
    ua: NotRequired["RegistrationCreateParamsCountryOptionsUa"]
    """
    Options for the registration in UA.
    """
    ug: NotRequired["RegistrationCreateParamsCountryOptionsUg"]
    """
    Options for the registration in UG.
    """
    us: NotRequired["RegistrationCreateParamsCountryOptionsUs"]
    """
    Options for the registration in US.
    """
    uy: NotRequired["RegistrationCreateParamsCountryOptionsUy"]
    """
    Options for the registration in UY.
    """
    uz: NotRequired["RegistrationCreateParamsCountryOptionsUz"]
    """
    Options for the registration in UZ.
    """
    vn: NotRequired["RegistrationCreateParamsCountryOptionsVn"]
    """
    Options for the registration in VN.
    """
    za: NotRequired["RegistrationCreateParamsCountryOptionsZa"]
    """
    Options for the registration in ZA.
    """
    zm: NotRequired["RegistrationCreateParamsCountryOptionsZm"]
    """
    Options for the registration in ZM.
    """
    zw: NotRequired["RegistrationCreateParamsCountryOptionsZw"]
    """
    Options for the registration in ZW.
    """


class RegistrationCreateParamsCountryOptionsAe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAeStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsAl(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAlStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAlStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsAm(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAo(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAoStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAoStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsAt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAtStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsAtStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsAu(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAuStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAuStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsAw(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsAwStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsAwStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsAz(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBa(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBaStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBaStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBb(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBbStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBbStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBd(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBdStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBdStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsBeStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsBf(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBfStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBfStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBg(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBgStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsBgStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsBh(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBhStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBhStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBj(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBs(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsBsStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsBsStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsBy(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCa(TypedDict):
    province_standard: NotRequired[
        "RegistrationCreateParamsCountryOptionsCaProvinceStandard"
    ]
    """
    Options for the provincial tax registration.
    """
    type: Literal["province_standard", "simplified", "standard"]
    """
    Type of registration to be created in Canada.
    """


class RegistrationCreateParamsCountryOptionsCaProvinceStandard(TypedDict):
    province: str
    """
    Two-letter CA province code ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class RegistrationCreateParamsCountryOptionsCd(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsCdStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCdStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsCh(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsChStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsChStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsCl(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCm(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCo(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCr(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCv(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsCy(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsCyStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsCyStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsCz(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsCzStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsCzStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsDe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsDeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsDeStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsDk(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsDkStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsDkStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsEc(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsEe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsEeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsEeStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsEg(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsEs(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsEsStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsEsStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsEt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsEtStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsEtStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsFi(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsFiStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsFiStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsFr(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsFrStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsFrStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsGb(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsGbStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsGbStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsGe(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsGn(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsGnStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsGnStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsGr(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsGrStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsGrStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsHr(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsHrStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsHrStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsHu(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsHuStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsHuStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsId(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsIe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsIeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsIeStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsIn(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsIs(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsIsStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsIsStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsIt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsItStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsItStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsJp(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsJpStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsJpStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsKe(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsKg(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsKh(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsKr(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsKz(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsLa(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsLk(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsLt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsLtStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsLtStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsLu(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsLuStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsLuStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsLv(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsLvStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsLvStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsMa(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMd(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsMeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMeStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsMk(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsMkStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMkStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsMr(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsMrStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMrStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsMt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsMtStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsMtStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsMx(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsMy(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsNg(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsNl(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsNlStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsNlStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsNo(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsNoStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsNoStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsNp(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsNz(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsNzStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsNzStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsOm(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsOmStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsOmStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsPe(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsPh(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsPl(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsPlStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsPlStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsPt(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsPtStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsPtStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsRo(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsRoStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsRoStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsRs(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsRsStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsRsStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsRu(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsSa(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsSe(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsSeStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsSeStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsSg(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsSgStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsSgStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsSi(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsSiStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsSiStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsSk(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsSkStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
    """
    Type of registration to be created in an EU country.
    """


class RegistrationCreateParamsCountryOptionsSkStandard(TypedDict):
    place_of_supply_scheme: Literal[
        "inbound_goods", "small_seller", "standard"
    ]
    """
    Place of supply scheme used in an EU standard registration.
    """


class RegistrationCreateParamsCountryOptionsSn(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsSr(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsSrStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsSrStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsTh(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsTj(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsTr(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsTw(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsTz(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsUa(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsUg(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsUs(TypedDict):
    local_amusement_tax: NotRequired[
        "RegistrationCreateParamsCountryOptionsUsLocalAmusementTax"
    ]
    """
    Options for the local amusement tax registration.
    """
    local_lease_tax: NotRequired[
        "RegistrationCreateParamsCountryOptionsUsLocalLeaseTax"
    ]
    """
    Options for the local lease tax registration.
    """
    state: str
    """
    Two-letter US state code ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """
    state_sales_tax: NotRequired[
        "RegistrationCreateParamsCountryOptionsUsStateSalesTax"
    ]
    """
    Options for the state sales tax registration.
    """
    type: Literal[
        "local_amusement_tax",
        "local_lease_tax",
        "state_communications_tax",
        "state_retail_delivery_fee",
        "state_sales_tax",
    ]
    """
    Type of registration to be created in the US.
    """


class RegistrationCreateParamsCountryOptionsUsLocalAmusementTax(TypedDict):
    jurisdiction: str
    """
    A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction. Supported FIPS codes are: `02154` (Arlington Heights), `05248` (Bensenville), `06613` (Bloomington), `10906` (Campton Hills), `14000` (Chicago), `21696` (East Dundee), `24582` (Evanston), `45421` (Lynwood), `48892` (Midlothian), `64343` (River Grove), `64421` (Riverside), `65806` (Roselle), and `68081` (Schiller Park).
    """


class RegistrationCreateParamsCountryOptionsUsLocalLeaseTax(TypedDict):
    jurisdiction: str
    """
    A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction. Supported FIPS codes are: `14000` (Chicago).
    """


class RegistrationCreateParamsCountryOptionsUsStateSalesTax(TypedDict):
    elections: List[
        "RegistrationCreateParamsCountryOptionsUsStateSalesTaxElection"
    ]
    """
    Elections for the state sales tax registration.
    """


class RegistrationCreateParamsCountryOptionsUsStateSalesTaxElection(TypedDict):
    jurisdiction: NotRequired[str]
    """
    A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction. Supported FIPS codes are: `003` (Allegheny County) and `60000` (Philadelphia City).
    """
    type: Literal[
        "local_use_tax", "simplified_sellers_use_tax", "single_local_use_tax"
    ]
    """
    The type of the election for the state sales tax registration.
    """


class RegistrationCreateParamsCountryOptionsUy(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsUyStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsUyStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsUz(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsVn(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsZa(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsZaStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsZaStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """


class RegistrationCreateParamsCountryOptionsZm(TypedDict):
    type: Literal["simplified"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsZw(TypedDict):
    standard: NotRequired["RegistrationCreateParamsCountryOptionsZwStandard"]
    """
    Options for the standard registration.
    """
    type: Literal["standard"]
    """
    Type of registration to be created in `country`.
    """


class RegistrationCreateParamsCountryOptionsZwStandard(TypedDict):
    place_of_supply_scheme: NotRequired[Literal["inbound_goods", "standard"]]
    """
    Place of supply scheme used in an standard registration.
    """
