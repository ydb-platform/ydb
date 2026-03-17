# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._application import Application
    from stripe._customer import Customer
    from stripe.params._tax_id_create_params import TaxIdCreateParams
    from stripe.params._tax_id_delete_params import TaxIdDeleteParams
    from stripe.params._tax_id_list_params import TaxIdListParams
    from stripe.params._tax_id_retrieve_params import TaxIdRetrieveParams


class TaxId(
    CreateableAPIResource["TaxId"],
    DeletableAPIResource["TaxId"],
    ListableAPIResource["TaxId"],
):
    """
    You can add one or multiple tax IDs to a [customer](https://docs.stripe.com/api/customers) or account.
    Customer and account tax IDs get displayed on related invoices and credit notes.

    Related guides: [Customer tax identification numbers](https://docs.stripe.com/billing/taxes/tax-ids), [Account tax IDs](https://docs.stripe.com/invoicing/connect#account-tax-ids)
    """

    OBJECT_NAME: ClassVar[Literal["tax_id"]] = "tax_id"

    class Owner(StripeObject):
        account: Optional[ExpandableField["Account"]]
        """
        The account being referenced when `type` is `account`.
        """
        application: Optional[ExpandableField["Application"]]
        """
        The Connect Application being referenced when `type` is `application`.
        """
        customer: Optional[ExpandableField["Customer"]]
        """
        The customer being referenced when `type` is `customer`.
        """
        customer_account: Optional[str]
        """
        The Account representing the customer being referenced when `type` is `customer`.
        """
        type: Literal["account", "application", "customer", "self"]
        """
        Type of owner referenced.
        """

    class Verification(StripeObject):
        status: Literal["pending", "unavailable", "unverified", "verified"]
        """
        Verification status, one of `pending`, `verified`, `unverified`, or `unavailable`.
        """
        verified_address: Optional[str]
        """
        Verified address.
        """
        verified_name: Optional[str]
        """
        Verified name.
        """

    country: Optional[str]
    """
    Two-letter ISO code representing the country of the tax ID.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    ID of the customer.
    """
    customer_account: Optional[str]
    """
    ID of the Account representing the customer.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["tax_id"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    owner: Optional[Owner]
    """
    The account or customer the tax ID belongs to.
    """
    type: Literal[
        "ad_nrt",
        "ae_trn",
        "al_tin",
        "am_tin",
        "ao_tin",
        "ar_cuit",
        "au_abn",
        "au_arn",
        "aw_tin",
        "az_tin",
        "ba_tin",
        "bb_tin",
        "bd_bin",
        "bf_ifu",
        "bg_uic",
        "bh_vat",
        "bj_ifu",
        "bo_tin",
        "br_cnpj",
        "br_cpf",
        "bs_tin",
        "by_tin",
        "ca_bn",
        "ca_gst_hst",
        "ca_pst_bc",
        "ca_pst_mb",
        "ca_pst_sk",
        "ca_qst",
        "cd_nif",
        "ch_uid",
        "ch_vat",
        "cl_tin",
        "cm_niu",
        "cn_tin",
        "co_nit",
        "cr_tin",
        "cv_nif",
        "de_stn",
        "do_rcn",
        "ec_ruc",
        "eg_tin",
        "es_cif",
        "et_tin",
        "eu_oss_vat",
        "eu_vat",
        "gb_vat",
        "ge_vat",
        "gn_nif",
        "hk_br",
        "hr_oib",
        "hu_tin",
        "id_npwp",
        "il_vat",
        "in_gst",
        "is_vat",
        "jp_cn",
        "jp_rn",
        "jp_trn",
        "ke_pin",
        "kg_tin",
        "kh_tin",
        "kr_brn",
        "kz_bin",
        "la_tin",
        "li_uid",
        "li_vat",
        "lk_vat",
        "ma_vat",
        "md_vat",
        "me_pib",
        "mk_vat",
        "mr_nif",
        "mx_rfc",
        "my_frp",
        "my_itn",
        "my_sst",
        "ng_tin",
        "no_vat",
        "no_voec",
        "np_pan",
        "nz_gst",
        "om_vat",
        "pe_ruc",
        "ph_tin",
        "pl_nip",
        "ro_tin",
        "rs_pib",
        "ru_inn",
        "ru_kpp",
        "sa_vat",
        "sg_gst",
        "sg_uen",
        "si_tin",
        "sn_ninea",
        "sr_fin",
        "sv_nit",
        "th_vat",
        "tj_tin",
        "tr_tin",
        "tw_vat",
        "tz_vat",
        "ua_vat",
        "ug_tin",
        "unknown",
        "us_ein",
        "uy_ruc",
        "uz_tin",
        "uz_vat",
        "ve_rif",
        "vn_tin",
        "za_vat",
        "zm_tin",
        "zw_tin",
    ]
    """
    Type of the tax ID, one of `ad_nrt`, `ae_trn`, `al_tin`, `am_tin`, `ao_tin`, `ar_cuit`, `au_abn`, `au_arn`, `aw_tin`, `az_tin`, `ba_tin`, `bb_tin`, `bd_bin`, `bf_ifu`, `bg_uic`, `bh_vat`, `bj_ifu`, `bo_tin`, `br_cnpj`, `br_cpf`, `bs_tin`, `by_tin`, `ca_bn`, `ca_gst_hst`, `ca_pst_bc`, `ca_pst_mb`, `ca_pst_sk`, `ca_qst`, `cd_nif`, `ch_uid`, `ch_vat`, `cl_tin`, `cm_niu`, `cn_tin`, `co_nit`, `cr_tin`, `cv_nif`, `de_stn`, `do_rcn`, `ec_ruc`, `eg_tin`, `es_cif`, `et_tin`, `eu_oss_vat`, `eu_vat`, `gb_vat`, `ge_vat`, `gn_nif`, `hk_br`, `hr_oib`, `hu_tin`, `id_npwp`, `il_vat`, `in_gst`, `is_vat`, `jp_cn`, `jp_rn`, `jp_trn`, `ke_pin`, `kg_tin`, `kh_tin`, `kr_brn`, `kz_bin`, `la_tin`, `li_uid`, `li_vat`, `lk_vat`, `ma_vat`, `md_vat`, `me_pib`, `mk_vat`, `mr_nif`, `mx_rfc`, `my_frp`, `my_itn`, `my_sst`, `ng_tin`, `no_vat`, `no_voec`, `np_pan`, `nz_gst`, `om_vat`, `pe_ruc`, `ph_tin`, `pl_nip`, `ro_tin`, `rs_pib`, `ru_inn`, `ru_kpp`, `sa_vat`, `sg_gst`, `sg_uen`, `si_tin`, `sn_ninea`, `sr_fin`, `sv_nit`, `th_vat`, `tj_tin`, `tr_tin`, `tw_vat`, `tz_vat`, `ua_vat`, `ug_tin`, `us_ein`, `uy_ruc`, `uz_tin`, `uz_vat`, `ve_rif`, `vn_tin`, `za_vat`, `zm_tin`, or `zw_tin`. Note that some legacy tax IDs have type `unknown`
    """
    value: str
    """
    Value of the tax ID.
    """
    verification: Optional[Verification]
    """
    Tax ID verification information.
    """

    @classmethod
    def create(cls, **params: Unpack["TaxIdCreateParams"]) -> "TaxId":
        """
        Creates a new account or customer tax_id object.
        """
        return cast(
            "TaxId",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["TaxIdCreateParams"]
    ) -> "TaxId":
        """
        Creates a new account or customer tax_id object.
        """
        return cast(
            "TaxId",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "TaxId",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["TaxIdDeleteParams"]) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        ...

    @overload
    def delete(self, **params: Unpack["TaxIdDeleteParams"]) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "TaxId",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TaxIdDeleteParams"]
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(cls, **params: Unpack["TaxIdListParams"]) -> ListObject["TaxId"]:
        """
        Returns a list of tax IDs.
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
        cls, **params: Unpack["TaxIdListParams"]
    ) -> ListObject["TaxId"]:
        """
        Returns a list of tax IDs.
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
    def retrieve(
        cls, id: str, **params: Unpack["TaxIdRetrieveParams"]
    ) -> "TaxId":
        """
        Retrieves an account or customer tax_id object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TaxIdRetrieveParams"]
    ) -> "TaxId":
        """
        Retrieves an account or customer tax_id object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"owner": Owner, "verification": Verification}
