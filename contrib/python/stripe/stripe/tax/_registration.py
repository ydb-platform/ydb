# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.tax._registration_create_params import (
        RegistrationCreateParams,
    )
    from stripe.params.tax._registration_list_params import (
        RegistrationListParams,
    )
    from stripe.params.tax._registration_modify_params import (
        RegistrationModifyParams,
    )
    from stripe.params.tax._registration_retrieve_params import (
        RegistrationRetrieveParams,
    )


class Registration(
    CreateableAPIResource["Registration"],
    ListableAPIResource["Registration"],
    UpdateableAPIResource["Registration"],
):
    """
    A Tax `Registration` lets us know that your business is registered to collect tax on payments within a region, enabling you to [automatically collect tax](https://docs.stripe.com/tax).

    Stripe doesn't register on your behalf with the relevant authorities when you create a Tax `Registration` object. For more information on how to register to collect tax, see [our guide](https://docs.stripe.com/tax/registering).

    Related guide: [Using the Registrations API](https://docs.stripe.com/tax/registrations-api)
    """

    OBJECT_NAME: ClassVar[Literal["tax.registration"]] = "tax.registration"

    class CountryOptions(StripeObject):
        class Ae(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Al(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Am(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ao(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class At(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Au(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Aw(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Az(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ba(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Bb(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Bd(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Be(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Bf(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Bg(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Bh(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Bj(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Bs(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class By(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ca(StripeObject):
            class ProvinceStandard(StripeObject):
                province: str
                """
                Two-letter CA province code ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
                """

            province_standard: Optional[ProvinceStandard]
            type: Literal["province_standard", "simplified", "standard"]
            """
            Type of registration in Canada.
            """
            _inner_class_types = {"province_standard": ProvinceStandard}

        class Cd(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Ch(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Cl(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Cm(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Co(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Cr(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Cv(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Cy(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Cz(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class De(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Dk(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Ec(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ee(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Eg(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Es(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Et(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Fi(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Fr(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Gb(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Ge(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Gn(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Gr(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Hr(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Hu(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Id(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ie(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class In(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Is(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class It(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Jp(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Ke(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Kg(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Kh(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Kr(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Kz(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class La(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Lk(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Lt(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Lu(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Lv(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Ma(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Md(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Me(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Mk(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Mr(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Mt(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Mx(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class My(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ng(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Nl(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class No(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Np(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Nz(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Om(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Pe(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ph(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Pl(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Pt(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Ro(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Rs(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Ru(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Sa(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Se(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Sg(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal["inbound_goods", "standard"]
                """
                Place of supply scheme used in an Default standard registration.
                """

            standard: Optional[Standard]
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """
            _inner_class_types = {"standard": Standard}

        class Si(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Sk(StripeObject):
            class Standard(StripeObject):
                place_of_supply_scheme: Literal[
                    "inbound_goods", "small_seller", "standard"
                ]
                """
                Place of supply scheme used in an EU standard registration.
                """

            standard: Optional[Standard]
            type: Literal["ioss", "oss_non_union", "oss_union", "standard"]
            """
            Type of registration in an EU country.
            """
            _inner_class_types = {"standard": Standard}

        class Sn(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Sr(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Th(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Tj(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Tr(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Tw(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Tz(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ua(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Ug(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Us(StripeObject):
            class LocalAmusementTax(StripeObject):
                jurisdiction: str
                """
                A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction.
                """

            class LocalLeaseTax(StripeObject):
                jurisdiction: str
                """
                A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction.
                """

            class StateSalesTax(StripeObject):
                class Election(StripeObject):
                    jurisdiction: Optional[str]
                    """
                    A [FIPS code](https://www.census.gov/library/reference/code-lists/ansi.html) representing the local jurisdiction.
                    """
                    type: Literal[
                        "local_use_tax",
                        "simplified_sellers_use_tax",
                        "single_local_use_tax",
                    ]
                    """
                    The type of the election for the state sales tax registration.
                    """

                elections: Optional[List[Election]]
                """
                Elections for the state sales tax registration.
                """
                _inner_class_types = {"elections": Election}

            local_amusement_tax: Optional[LocalAmusementTax]
            local_lease_tax: Optional[LocalLeaseTax]
            state: str
            """
            Two-letter US state code ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
            """
            state_sales_tax: Optional[StateSalesTax]
            type: Literal[
                "local_amusement_tax",
                "local_lease_tax",
                "state_communications_tax",
                "state_retail_delivery_fee",
                "state_sales_tax",
            ]
            """
            Type of registration in the US.
            """
            _inner_class_types = {
                "local_amusement_tax": LocalAmusementTax,
                "local_lease_tax": LocalLeaseTax,
                "state_sales_tax": StateSalesTax,
            }

        class Uy(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Uz(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Vn(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Za(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        class Zm(StripeObject):
            type: Literal["simplified"]
            """
            Type of registration in `country`.
            """

        class Zw(StripeObject):
            type: Literal["standard"]
            """
            Type of registration in `country`.
            """

        ae: Optional[Ae]
        al: Optional[Al]
        am: Optional[Am]
        ao: Optional[Ao]
        at: Optional[At]
        au: Optional[Au]
        aw: Optional[Aw]
        az: Optional[Az]
        ba: Optional[Ba]
        bb: Optional[Bb]
        bd: Optional[Bd]
        be: Optional[Be]
        bf: Optional[Bf]
        bg: Optional[Bg]
        bh: Optional[Bh]
        bj: Optional[Bj]
        bs: Optional[Bs]
        by: Optional[By]
        ca: Optional[Ca]
        cd: Optional[Cd]
        ch: Optional[Ch]
        cl: Optional[Cl]
        cm: Optional[Cm]
        co: Optional[Co]
        cr: Optional[Cr]
        cv: Optional[Cv]
        cy: Optional[Cy]
        cz: Optional[Cz]
        de: Optional[De]
        dk: Optional[Dk]
        ec: Optional[Ec]
        ee: Optional[Ee]
        eg: Optional[Eg]
        es: Optional[Es]
        et: Optional[Et]
        fi: Optional[Fi]
        fr: Optional[Fr]
        gb: Optional[Gb]
        ge: Optional[Ge]
        gn: Optional[Gn]
        gr: Optional[Gr]
        hr: Optional[Hr]
        hu: Optional[Hu]
        id: Optional[Id]
        ie: Optional[Ie]
        in_: Optional[In]
        is_: Optional[Is]
        it: Optional[It]
        jp: Optional[Jp]
        ke: Optional[Ke]
        kg: Optional[Kg]
        kh: Optional[Kh]
        kr: Optional[Kr]
        kz: Optional[Kz]
        la: Optional[La]
        lk: Optional[Lk]
        lt: Optional[Lt]
        lu: Optional[Lu]
        lv: Optional[Lv]
        ma: Optional[Ma]
        md: Optional[Md]
        me: Optional[Me]
        mk: Optional[Mk]
        mr: Optional[Mr]
        mt: Optional[Mt]
        mx: Optional[Mx]
        my: Optional[My]
        ng: Optional[Ng]
        nl: Optional[Nl]
        no: Optional[No]
        np: Optional[Np]
        nz: Optional[Nz]
        om: Optional[Om]
        pe: Optional[Pe]
        ph: Optional[Ph]
        pl: Optional[Pl]
        pt: Optional[Pt]
        ro: Optional[Ro]
        rs: Optional[Rs]
        ru: Optional[Ru]
        sa: Optional[Sa]
        se: Optional[Se]
        sg: Optional[Sg]
        si: Optional[Si]
        sk: Optional[Sk]
        sn: Optional[Sn]
        sr: Optional[Sr]
        th: Optional[Th]
        tj: Optional[Tj]
        tr: Optional[Tr]
        tw: Optional[Tw]
        tz: Optional[Tz]
        ua: Optional[Ua]
        ug: Optional[Ug]
        us: Optional[Us]
        uy: Optional[Uy]
        uz: Optional[Uz]
        vn: Optional[Vn]
        za: Optional[Za]
        zm: Optional[Zm]
        zw: Optional[Zw]
        _inner_class_types = {
            "ae": Ae,
            "al": Al,
            "am": Am,
            "ao": Ao,
            "at": At,
            "au": Au,
            "aw": Aw,
            "az": Az,
            "ba": Ba,
            "bb": Bb,
            "bd": Bd,
            "be": Be,
            "bf": Bf,
            "bg": Bg,
            "bh": Bh,
            "bj": Bj,
            "bs": Bs,
            "by": By,
            "ca": Ca,
            "cd": Cd,
            "ch": Ch,
            "cl": Cl,
            "cm": Cm,
            "co": Co,
            "cr": Cr,
            "cv": Cv,
            "cy": Cy,
            "cz": Cz,
            "de": De,
            "dk": Dk,
            "ec": Ec,
            "ee": Ee,
            "eg": Eg,
            "es": Es,
            "et": Et,
            "fi": Fi,
            "fr": Fr,
            "gb": Gb,
            "ge": Ge,
            "gn": Gn,
            "gr": Gr,
            "hr": Hr,
            "hu": Hu,
            "id": Id,
            "ie": Ie,
            "in": In,
            "is": Is,
            "it": It,
            "jp": Jp,
            "ke": Ke,
            "kg": Kg,
            "kh": Kh,
            "kr": Kr,
            "kz": Kz,
            "la": La,
            "lk": Lk,
            "lt": Lt,
            "lu": Lu,
            "lv": Lv,
            "ma": Ma,
            "md": Md,
            "me": Me,
            "mk": Mk,
            "mr": Mr,
            "mt": Mt,
            "mx": Mx,
            "my": My,
            "ng": Ng,
            "nl": Nl,
            "no": No,
            "np": Np,
            "nz": Nz,
            "om": Om,
            "pe": Pe,
            "ph": Ph,
            "pl": Pl,
            "pt": Pt,
            "ro": Ro,
            "rs": Rs,
            "ru": Ru,
            "sa": Sa,
            "se": Se,
            "sg": Sg,
            "si": Si,
            "sk": Sk,
            "sn": Sn,
            "sr": Sr,
            "th": Th,
            "tj": Tj,
            "tr": Tr,
            "tw": Tw,
            "tz": Tz,
            "ua": Ua,
            "ug": Ug,
            "us": Us,
            "uy": Uy,
            "uz": Uz,
            "vn": Vn,
            "za": Za,
            "zm": Zm,
            "zw": Zw,
        }
        _field_remappings = {"in_": "in", "is_": "is"}

    active_from: int
    """
    Time at which the registration becomes active. Measured in seconds since the Unix epoch.
    """
    country: str
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    country_options: CountryOptions
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    expires_at: Optional[int]
    """
    If set, the registration stops being active at this time. If not set, the registration will be active indefinitely. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["tax.registration"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["active", "expired", "scheduled"]
    """
    The status of the registration. This field is present for convenience and can be deduced from `active_from` and `expires_at`.
    """

    @classmethod
    def create(
        cls, **params: Unpack["RegistrationCreateParams"]
    ) -> "Registration":
        """
        Creates a new Tax Registration object.
        """
        return cast(
            "Registration",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["RegistrationCreateParams"]
    ) -> "Registration":
        """
        Creates a new Tax Registration object.
        """
        return cast(
            "Registration",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["RegistrationListParams"]
    ) -> ListObject["Registration"]:
        """
        Returns a list of Tax Registration objects.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["RegistrationListParams"]
    ) -> ListObject["Registration"]:
        """
        Returns a list of Tax Registration objects.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["RegistrationModifyParams"]
    ) -> "Registration":
        """
        Updates an existing Tax Registration object.

        A registration cannot be deleted after it has been created. If you wish to end a registration you may do so by setting expires_at.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Registration",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["RegistrationModifyParams"]
    ) -> "Registration":
        """
        Updates an existing Tax Registration object.

        A registration cannot be deleted after it has been created. If you wish to end a registration you may do so by setting expires_at.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Registration",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["RegistrationRetrieveParams"]
    ) -> "Registration":
        """
        Returns a Tax Registration object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["RegistrationRetrieveParams"]
    ) -> "Registration":
        """
        Returns a Tax Registration object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"country_options": CountryOptions}
