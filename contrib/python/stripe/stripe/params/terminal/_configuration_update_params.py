# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ConfigurationUpdateParams(TypedDict):
    bbpos_wisepad3: NotRequired[
        "Literal['']|ConfigurationUpdateParamsBbposWisepad3"
    ]
    """
    An object containing device type specific settings for BBPOS WisePad 3 readers.
    """
    bbpos_wisepos_e: NotRequired[
        "Literal['']|ConfigurationUpdateParamsBbposWiseposE"
    ]
    """
    An object containing device type specific settings for BBPOS WisePOS E readers.
    """
    cellular: NotRequired["Literal['']|ConfigurationUpdateParamsCellular"]
    """
    Configuration for cellular connectivity.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    name: NotRequired[str]
    """
    Name of the configuration
    """
    offline: NotRequired["Literal['']|ConfigurationUpdateParamsOffline"]
    """
    Configurations for collecting transactions offline.
    """
    reboot_window: NotRequired[
        "Literal['']|ConfigurationUpdateParamsRebootWindow"
    ]
    """
    Reboot time settings for readers. that support customized reboot time configuration.
    """
    stripe_s700: NotRequired["Literal['']|ConfigurationUpdateParamsStripeS700"]
    """
    An object containing device type specific settings for Stripe S700 readers.
    """
    stripe_s710: NotRequired["Literal['']|ConfigurationUpdateParamsStripeS710"]
    """
    An object containing device type specific settings for Stripe S710 readers.
    """
    tipping: NotRequired["Literal['']|ConfigurationUpdateParamsTipping"]
    """
    Tipping configurations for readers that support on-reader tips.
    """
    verifone_p400: NotRequired[
        "Literal['']|ConfigurationUpdateParamsVerifoneP400"
    ]
    """
    An object containing device type specific settings for Verifone P400 readers.
    """
    wifi: NotRequired["Literal['']|ConfigurationUpdateParamsWifi"]
    """
    Configurations for connecting to a WiFi network.
    """


class ConfigurationUpdateParamsBbposWisepad3(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationUpdateParamsBbposWiseposE(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image to display on the reader
    """


class ConfigurationUpdateParamsCellular(TypedDict):
    enabled: bool
    """
    Determines whether to allow the reader to connect to a cellular network. Defaults to false.
    """


class ConfigurationUpdateParamsOffline(TypedDict):
    enabled: bool
    """
    Determines whether to allow transactions to be collected while reader is offline. Defaults to false.
    """


class ConfigurationUpdateParamsRebootWindow(TypedDict):
    end_hour: int
    """
    Integer between 0 to 23 that represents the end hour of the reboot time window. The value must be different than the start_hour.
    """
    start_hour: int
    """
    Integer between 0 to 23 that represents the start hour of the reboot time window.
    """


class ConfigurationUpdateParamsStripeS700(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationUpdateParamsStripeS710(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationUpdateParamsTipping(TypedDict):
    aed: NotRequired["ConfigurationUpdateParamsTippingAed"]
    """
    Tipping configuration for AED
    """
    aud: NotRequired["ConfigurationUpdateParamsTippingAud"]
    """
    Tipping configuration for AUD
    """
    cad: NotRequired["ConfigurationUpdateParamsTippingCad"]
    """
    Tipping configuration for CAD
    """
    chf: NotRequired["ConfigurationUpdateParamsTippingChf"]
    """
    Tipping configuration for CHF
    """
    czk: NotRequired["ConfigurationUpdateParamsTippingCzk"]
    """
    Tipping configuration for CZK
    """
    dkk: NotRequired["ConfigurationUpdateParamsTippingDkk"]
    """
    Tipping configuration for DKK
    """
    eur: NotRequired["ConfigurationUpdateParamsTippingEur"]
    """
    Tipping configuration for EUR
    """
    gbp: NotRequired["ConfigurationUpdateParamsTippingGbp"]
    """
    Tipping configuration for GBP
    """
    gip: NotRequired["ConfigurationUpdateParamsTippingGip"]
    """
    Tipping configuration for GIP
    """
    hkd: NotRequired["ConfigurationUpdateParamsTippingHkd"]
    """
    Tipping configuration for HKD
    """
    huf: NotRequired["ConfigurationUpdateParamsTippingHuf"]
    """
    Tipping configuration for HUF
    """
    jpy: NotRequired["ConfigurationUpdateParamsTippingJpy"]
    """
    Tipping configuration for JPY
    """
    mxn: NotRequired["ConfigurationUpdateParamsTippingMxn"]
    """
    Tipping configuration for MXN
    """
    myr: NotRequired["ConfigurationUpdateParamsTippingMyr"]
    """
    Tipping configuration for MYR
    """
    nok: NotRequired["ConfigurationUpdateParamsTippingNok"]
    """
    Tipping configuration for NOK
    """
    nzd: NotRequired["ConfigurationUpdateParamsTippingNzd"]
    """
    Tipping configuration for NZD
    """
    pln: NotRequired["ConfigurationUpdateParamsTippingPln"]
    """
    Tipping configuration for PLN
    """
    ron: NotRequired["ConfigurationUpdateParamsTippingRon"]
    """
    Tipping configuration for RON
    """
    sek: NotRequired["ConfigurationUpdateParamsTippingSek"]
    """
    Tipping configuration for SEK
    """
    sgd: NotRequired["ConfigurationUpdateParamsTippingSgd"]
    """
    Tipping configuration for SGD
    """
    usd: NotRequired["ConfigurationUpdateParamsTippingUsd"]
    """
    Tipping configuration for USD
    """


class ConfigurationUpdateParamsTippingAed(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingAud(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingCad(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingChf(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingCzk(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingDkk(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingEur(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingGbp(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingGip(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingHkd(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingHuf(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingJpy(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingMxn(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingMyr(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingNok(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingNzd(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingPln(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingRon(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingSek(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingSgd(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsTippingUsd(TypedDict):
    fixed_amounts: NotRequired[List[int]]
    """
    Fixed amounts displayed when collecting a tip
    """
    percentages: NotRequired[List[int]]
    """
    Percentages displayed when collecting a tip
    """
    smart_tip_threshold: NotRequired[int]
    """
    Below this amount, fixed amounts will be displayed; above it, percentages will be displayed
    """


class ConfigurationUpdateParamsVerifoneP400(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationUpdateParamsWifi(TypedDict):
    enterprise_eap_peap: NotRequired[
        "ConfigurationUpdateParamsWifiEnterpriseEapPeap"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-PEAP authentication method.
    """
    enterprise_eap_tls: NotRequired[
        "ConfigurationUpdateParamsWifiEnterpriseEapTls"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-TLS authentication method.
    """
    personal_psk: NotRequired["ConfigurationUpdateParamsWifiPersonalPsk"]
    """
    Credentials for a WPA-Personal WiFi network.
    """
    type: Literal["enterprise_eap_peap", "enterprise_eap_tls", "personal_psk"]
    """
    Security type of the WiFi network. Fill out the hash with the corresponding name to provide the set of credentials for this security type.
    """


class ConfigurationUpdateParamsWifiEnterpriseEapPeap(TypedDict):
    ca_certificate_file: NotRequired[str]
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


class ConfigurationUpdateParamsWifiEnterpriseEapTls(TypedDict):
    ca_certificate_file: NotRequired[str]
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
    private_key_file_password: NotRequired[str]
    """
    Password for the private key file
    """
    ssid: str
    """
    Name of the WiFi network
    """


class ConfigurationUpdateParamsWifiPersonalPsk(TypedDict):
    password: str
    """
    Password for connecting to the WiFi network
    """
    ssid: str
    """
    Name of the WiFi network
    """
