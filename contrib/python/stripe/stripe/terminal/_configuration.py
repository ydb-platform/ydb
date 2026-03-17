# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File
    from stripe.params.terminal._configuration_create_params import (
        ConfigurationCreateParams,
    )
    from stripe.params.terminal._configuration_delete_params import (
        ConfigurationDeleteParams,
    )
    from stripe.params.terminal._configuration_list_params import (
        ConfigurationListParams,
    )
    from stripe.params.terminal._configuration_modify_params import (
        ConfigurationModifyParams,
    )
    from stripe.params.terminal._configuration_retrieve_params import (
        ConfigurationRetrieveParams,
    )


class Configuration(
    CreateableAPIResource["Configuration"],
    DeletableAPIResource["Configuration"],
    ListableAPIResource["Configuration"],
    UpdateableAPIResource["Configuration"],
):
    """
    A Configurations object represents how features should be configured for terminal readers.
    For information about how to use it, see the [Terminal configurations documentation](https://docs.stripe.com/terminal/fleet/configurations-overview).
    """

    OBJECT_NAME: ClassVar[Literal["terminal.configuration"]] = (
        "terminal.configuration"
    )

    class BbposWisepad3(StripeObject):
        splashscreen: Optional[ExpandableField["File"]]
        """
        A File ID representing an image to display on the reader
        """

    class BbposWiseposE(StripeObject):
        splashscreen: Optional[ExpandableField["File"]]
        """
        A File ID representing an image to display on the reader
        """

    class Cellular(StripeObject):
        enabled: bool
        """
        Whether a cellular-capable reader can connect to the internet over cellular.
        """

    class Offline(StripeObject):
        enabled: Optional[bool]
        """
        Determines whether to allow transactions to be collected while reader is offline. Defaults to false.
        """

    class RebootWindow(StripeObject):
        end_hour: int
        """
        Integer between 0 to 23 that represents the end hour of the reboot time window. The value must be different than the start_hour.
        """
        start_hour: int
        """
        Integer between 0 to 23 that represents the start hour of the reboot time window.
        """

    class StripeS700(StripeObject):
        splashscreen: Optional[ExpandableField["File"]]
        """
        A File ID representing an image to display on the reader
        """

    class StripeS710(StripeObject):
        splashscreen: Optional[ExpandableField["File"]]
        """
        A File ID representing an image to display on the reader
        """

    class Tipping(StripeObject):
        class Aed(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Aud(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Cad(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Chf(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Czk(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Dkk(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Eur(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Gbp(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Gip(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Hkd(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Huf(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Jpy(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Mxn(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Myr(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Nok(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Nzd(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Pln(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Ron(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Sek(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Sgd(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        class Usd(StripeObject):
            fixed_amounts: Optional[List[int]]
            """
            Fixed amounts displayed when collecting a tip
            """
            percentages: Optional[List[int]]
            """
            Percentages displayed when collecting a tip
            """
            smart_tip_threshold: Optional[int]
            """
            Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
            """

        aed: Optional[Aed]
        aud: Optional[Aud]
        cad: Optional[Cad]
        chf: Optional[Chf]
        czk: Optional[Czk]
        dkk: Optional[Dkk]
        eur: Optional[Eur]
        gbp: Optional[Gbp]
        gip: Optional[Gip]
        hkd: Optional[Hkd]
        huf: Optional[Huf]
        jpy: Optional[Jpy]
        mxn: Optional[Mxn]
        myr: Optional[Myr]
        nok: Optional[Nok]
        nzd: Optional[Nzd]
        pln: Optional[Pln]
        ron: Optional[Ron]
        sek: Optional[Sek]
        sgd: Optional[Sgd]
        usd: Optional[Usd]
        _inner_class_types = {
            "aed": Aed,
            "aud": Aud,
            "cad": Cad,
            "chf": Chf,
            "czk": Czk,
            "dkk": Dkk,
            "eur": Eur,
            "gbp": Gbp,
            "gip": Gip,
            "hkd": Hkd,
            "huf": Huf,
            "jpy": Jpy,
            "mxn": Mxn,
            "myr": Myr,
            "nok": Nok,
            "nzd": Nzd,
            "pln": Pln,
            "ron": Ron,
            "sek": Sek,
            "sgd": Sgd,
            "usd": Usd,
        }

    class VerifoneP400(StripeObject):
        splashscreen: Optional[ExpandableField["File"]]
        """
        A File ID representing an image to display on the reader
        """

    class Wifi(StripeObject):
        class EnterpriseEapPeap(StripeObject):
            ca_certificate_file: Optional[str]
            """
            A File ID representing a PEM file containing the server certificate
            """
            password: str
            """
            Password for connecting to the WiFi network
            """
            ssid: str
            """
            Name of the WiFi network
            """
            username: str
            """
            Username for connecting to the WiFi network
            """

        class EnterpriseEapTls(StripeObject):
            ca_certificate_file: Optional[str]
            """
            A File ID representing a PEM file containing the server certificate
            """
            client_certificate_file: str
            """
            A File ID representing a PEM file containing the client certificate
            """
            private_key_file: str
            """
            A File ID representing a PEM file containing the client RSA private key
            """
            private_key_file_password: Optional[str]
            """
            Password for the private key file
            """
            ssid: str
            """
            Name of the WiFi network
            """

        class PersonalPsk(StripeObject):
            password: str
            """
            Password for connecting to the WiFi network
            """
            ssid: str
            """
            Name of the WiFi network
            """

        enterprise_eap_peap: Optional[EnterpriseEapPeap]
        enterprise_eap_tls: Optional[EnterpriseEapTls]
        personal_psk: Optional[PersonalPsk]
        type: Literal[
            "enterprise_eap_peap", "enterprise_eap_tls", "personal_psk"
        ]
        """
        Security type of the WiFi network. The hash with the corresponding name contains the credentials for this security type.
        """
        _inner_class_types = {
            "enterprise_eap_peap": EnterpriseEapPeap,
            "enterprise_eap_tls": EnterpriseEapTls,
            "personal_psk": PersonalPsk,
        }

    bbpos_wisepad3: Optional[BbposWisepad3]
    bbpos_wisepos_e: Optional[BbposWiseposE]
    cellular: Optional[Cellular]
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    is_account_default: Optional[bool]
    """
    Whether this Configuration is the default for your account
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    name: Optional[str]
    """
    String indicating the name of the Configuration object, set by the user
    """
    object: Literal["terminal.configuration"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    offline: Optional[Offline]
    reboot_window: Optional[RebootWindow]
    stripe_s700: Optional[StripeS700]
    stripe_s710: Optional[StripeS710]
    tipping: Optional[Tipping]
    verifone_p400: Optional[VerifoneP400]
    wifi: Optional[Wifi]

    @classmethod
    def create(
        cls, **params: Unpack["ConfigurationCreateParams"]
    ) -> "Configuration":
        """
        Creates a new Configuration object.
        """
        return cast(
            "Configuration",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ConfigurationCreateParams"]
    ) -> "Configuration":
        """
        Creates a new Configuration object.
        """
        return cast(
            "Configuration",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Configuration",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Configuration",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ConfigurationDeleteParams"]
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["ConfigurationListParams"]
    ) -> ListObject["Configuration"]:
        """
        Returns a list of Configuration objects.
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
        cls, **params: Unpack["ConfigurationListParams"]
    ) -> ListObject["Configuration"]:
        """
        Returns a list of Configuration objects.
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
        cls, id: str, **params: Unpack["ConfigurationModifyParams"]
    ) -> "Configuration":
        """
        Updates a new Configuration object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Configuration",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ConfigurationModifyParams"]
    ) -> "Configuration":
        """
        Updates a new Configuration object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Configuration",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ConfigurationRetrieveParams"]
    ) -> "Configuration":
        """
        Retrieves a Configuration object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ConfigurationRetrieveParams"]
    ) -> "Configuration":
        """
        Retrieves a Configuration object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "bbpos_wisepad3": BbposWisepad3,
        "bbpos_wisepos_e": BbposWiseposE,
        "cellular": Cellular,
        "offline": Offline,
        "reboot_window": RebootWindow,
        "stripe_s700": StripeS700,
        "stripe_s710": StripeS710,
        "tipping": Tipping,
        "verifone_p400": VerifoneP400,
        "wifi": Wifi,
    }
