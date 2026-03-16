# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ConfigurationModifyParams(RequestOptions):
    bbpos_wisepad3: NotRequired[
        "Literal['']|ConfigurationModifyParamsBbposWisepad3"
    ]
    """
    An object containing device type specific settings for BBPOS WisePad 3 readers.
    """
    bbpos_wisepos_e: NotRequired[
        "Literal['']|ConfigurationModifyParamsBbposWiseposE"
    ]
    """
    An object containing device type specific settings for BBPOS WisePOS E readers.
    """
    cellular: NotRequired["Literal['']|ConfigurationModifyParamsCellular"]
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
    offline: NotRequired["Literal['']|ConfigurationModifyParamsOffline"]
    """
    Configurations for collecting transactions offline.
    """
    reboot_window: NotRequired[
        "Literal['']|ConfigurationModifyParamsRebootWindow"
    ]
    """
    Reboot time settings for readers. that support customized reboot time configuration.
    """
    stripe_s700: NotRequired["Literal['']|ConfigurationModifyParamsStripeS700"]
    """
    An object containing device type specific settings for Stripe S700 readers.
    """
    stripe_s710: NotRequired["Literal['']|ConfigurationModifyParamsStripeS710"]
    """
    An object containing device type specific settings for Stripe S710 readers.
    """
    tipping: NotRequired["Literal['']|ConfigurationModifyParamsTipping"]
    """
    Tipping configurations for readers that support on-reader tips.
    """
    verifone_p400: NotRequired[
        "Literal['']|ConfigurationModifyParamsVerifoneP400"
    ]
    """
    An object containing device type specific settings for Verifone P400 readers.
    """
    wifi: NotRequired["Literal['']|ConfigurationModifyParamsWifi"]
    """
    Configurations for connecting to a WiFi network.
    """


class ConfigurationModifyParamsBbposWisepad3(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationModifyParamsBbposWiseposE(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image to display on the reader
    """


class ConfigurationModifyParamsCellular(TypedDict):
    enabled: bool
    """
    Determines whether to allow the reader to connect to a cellular network. Defaults to false.
    """


class ConfigurationModifyParamsOffline(TypedDict):
    enabled: bool
    """
    Determines whether to allow transactions to be collected while reader is offline. Defaults to false.
    """


class ConfigurationModifyParamsRebootWindow(TypedDict):
    end_hour: int
    """
    Integer between 0 to 23 that represents the end hour of the reboot time window. The value must be different than the start_hour.
    """
    start_hour: int
    """
    Integer between 0 to 23 that represents the start hour of the reboot time window.
    """


class ConfigurationModifyParamsStripeS700(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationModifyParamsStripeS710(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationModifyParamsTipping(TypedDict):
    aed: NotRequired["ConfigurationModifyParamsTippingAed"]
    """
    Tipping configuration for AED
    """
    aud: NotRequired["ConfigurationModifyParamsTippingAud"]
    """
    Tipping configuration for AUD
    """
    cad: NotRequired["ConfigurationModifyParamsTippingCad"]
    """
    Tipping configuration for CAD
    """
    chf: NotRequired["ConfigurationModifyParamsTippingChf"]
    """
    Tipping configuration for CHF
    """
    czk: NotRequired["ConfigurationModifyParamsTippingCzk"]
    """
    Tipping configuration for CZK
    """
    dkk: NotRequired["ConfigurationModifyParamsTippingDkk"]
    """
    Tipping configuration for DKK
    """
    eur: NotRequired["ConfigurationModifyParamsTippingEur"]
    """
    Tipping configuration for EUR
    """
    gbp: NotRequired["ConfigurationModifyParamsTippingGbp"]
    """
    Tipping configuration for GBP
    """
    gip: NotRequired["ConfigurationModifyParamsTippingGip"]
    """
    Tipping configuration for GIP
    """
    hkd: NotRequired["ConfigurationModifyParamsTippingHkd"]
    """
    Tipping configuration for HKD
    """
    huf: NotRequired["ConfigurationModifyParamsTippingHuf"]
    """
    Tipping configuration for HUF
    """
    jpy: NotRequired["ConfigurationModifyParamsTippingJpy"]
    """
    Tipping configuration for JPY
    """
    mxn: NotRequired["ConfigurationModifyParamsTippingMxn"]
    """
    Tipping configuration for MXN
    """
    myr: NotRequired["ConfigurationModifyParamsTippingMyr"]
    """
    Tipping configuration for MYR
    """
    nok: NotRequired["ConfigurationModifyParamsTippingNok"]
    """
    Tipping configuration for NOK
    """
    nzd: NotRequired["ConfigurationModifyParamsTippingNzd"]
    """
    Tipping configuration for NZD
    """
    pln: NotRequired["ConfigurationModifyParamsTippingPln"]
    """
    Tipping configuration for PLN
    """
    ron: NotRequired["ConfigurationModifyParamsTippingRon"]
    """
    Tipping configuration for RON
    """
    sek: NotRequired["ConfigurationModifyParamsTippingSek"]
    """
    Tipping configuration for SEK
    """
    sgd: NotRequired["ConfigurationModifyParamsTippingSgd"]
    """
    Tipping configuration for SGD
    """
    usd: NotRequired["ConfigurationModifyParamsTippingUsd"]
    """
    Tipping configuration for USD
    """


class ConfigurationModifyParamsTippingAed(TypedDict):
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


class ConfigurationModifyParamsTippingAud(TypedDict):
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


class ConfigurationModifyParamsTippingCad(TypedDict):
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


class ConfigurationModifyParamsTippingChf(TypedDict):
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


class ConfigurationModifyParamsTippingCzk(TypedDict):
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


class ConfigurationModifyParamsTippingDkk(TypedDict):
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


class ConfigurationModifyParamsTippingEur(TypedDict):
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


class ConfigurationModifyParamsTippingGbp(TypedDict):
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


class ConfigurationModifyParamsTippingGip(TypedDict):
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


class ConfigurationModifyParamsTippingHkd(TypedDict):
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


class ConfigurationModifyParamsTippingHuf(TypedDict):
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


class ConfigurationModifyParamsTippingJpy(TypedDict):
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


class ConfigurationModifyParamsTippingMxn(TypedDict):
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


class ConfigurationModifyParamsTippingMyr(TypedDict):
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


class ConfigurationModifyParamsTippingNok(TypedDict):
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


class ConfigurationModifyParamsTippingNzd(TypedDict):
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


class ConfigurationModifyParamsTippingPln(TypedDict):
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


class ConfigurationModifyParamsTippingRon(TypedDict):
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


class ConfigurationModifyParamsTippingSek(TypedDict):
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


class ConfigurationModifyParamsTippingSgd(TypedDict):
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


class ConfigurationModifyParamsTippingUsd(TypedDict):
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


class ConfigurationModifyParamsVerifoneP400(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationModifyParamsWifi(TypedDict):
    enterprise_eap_peap: NotRequired[
        "ConfigurationModifyParamsWifiEnterpriseEapPeap"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-PEAP authentication method.
    """
    enterprise_eap_tls: NotRequired[
        "ConfigurationModifyParamsWifiEnterpriseEapTls"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-TLS authentication method.
    """
    personal_psk: NotRequired["ConfigurationModifyParamsWifiPersonalPsk"]
    """
    Credentials for a WPA-Personal WiFi network.
    """
    type: Literal["enterprise_eap_peap", "enterprise_eap_tls", "personal_psk"]
    """
    Security type of the WiFi network. Fill out the hash with the corresponding name to provide the set of credentials for this security type.
    """


class ConfigurationModifyParamsWifiEnterpriseEapPeap(TypedDict):
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


class ConfigurationModifyParamsWifiEnterpriseEapTls(TypedDict):
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


class ConfigurationModifyParamsWifiPersonalPsk(TypedDict):
    password: str
    """
    Password for connecting to the WiFi network
    """
    ssid: str
    """
    Name of the WiFi network
    """
