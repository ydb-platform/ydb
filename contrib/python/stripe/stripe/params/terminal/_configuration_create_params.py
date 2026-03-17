# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ConfigurationCreateParams(RequestOptions):
    bbpos_wisepad3: NotRequired["ConfigurationCreateParamsBbposWisepad3"]
    """
    An object containing device type specific settings for BBPOS WisePad 3 readers.
    """
    bbpos_wisepos_e: NotRequired["ConfigurationCreateParamsBbposWiseposE"]
    """
    An object containing device type specific settings for BBPOS WisePOS E readers.
    """
    cellular: NotRequired["Literal['']|ConfigurationCreateParamsCellular"]
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
    offline: NotRequired["Literal['']|ConfigurationCreateParamsOffline"]
    """
    Configurations for collecting transactions offline.
    """
    reboot_window: NotRequired["ConfigurationCreateParamsRebootWindow"]
    """
    Reboot time settings for readers. that support customized reboot time configuration.
    """
    stripe_s700: NotRequired["ConfigurationCreateParamsStripeS700"]
    """
    An object containing device type specific settings for Stripe S700 readers.
    """
    stripe_s710: NotRequired["ConfigurationCreateParamsStripeS710"]
    """
    An object containing device type specific settings for Stripe S710 readers.
    """
    tipping: NotRequired["Literal['']|ConfigurationCreateParamsTipping"]
    """
    Tipping configurations for readers that support on-reader tips.
    """
    verifone_p400: NotRequired["ConfigurationCreateParamsVerifoneP400"]
    """
    An object containing device type specific settings for Verifone P400 readers.
    """
    wifi: NotRequired["Literal['']|ConfigurationCreateParamsWifi"]
    """
    Configurations for connecting to a WiFi network.
    """


class ConfigurationCreateParamsBbposWisepad3(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationCreateParamsBbposWiseposE(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image to display on the reader
    """


class ConfigurationCreateParamsCellular(TypedDict):
    enabled: bool
    """
    Determines whether to allow the reader to connect to a cellular network. Defaults to false.
    """


class ConfigurationCreateParamsOffline(TypedDict):
    enabled: bool
    """
    Determines whether to allow transactions to be collected while reader is offline. Defaults to false.
    """


class ConfigurationCreateParamsRebootWindow(TypedDict):
    end_hour: int
    """
    Integer between 0 to 23 that represents the end hour of the reboot time window. The value must be different than the start_hour.
    """
    start_hour: int
    """
    Integer between 0 to 23 that represents the start hour of the reboot time window.
    """


class ConfigurationCreateParamsStripeS700(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationCreateParamsStripeS710(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationCreateParamsTipping(TypedDict):
    aed: NotRequired["ConfigurationCreateParamsTippingAed"]
    """
    Tipping configuration for AED
    """
    aud: NotRequired["ConfigurationCreateParamsTippingAud"]
    """
    Tipping configuration for AUD
    """
    cad: NotRequired["ConfigurationCreateParamsTippingCad"]
    """
    Tipping configuration for CAD
    """
    chf: NotRequired["ConfigurationCreateParamsTippingChf"]
    """
    Tipping configuration for CHF
    """
    czk: NotRequired["ConfigurationCreateParamsTippingCzk"]
    """
    Tipping configuration for CZK
    """
    dkk: NotRequired["ConfigurationCreateParamsTippingDkk"]
    """
    Tipping configuration for DKK
    """
    eur: NotRequired["ConfigurationCreateParamsTippingEur"]
    """
    Tipping configuration for EUR
    """
    gbp: NotRequired["ConfigurationCreateParamsTippingGbp"]
    """
    Tipping configuration for GBP
    """
    gip: NotRequired["ConfigurationCreateParamsTippingGip"]
    """
    Tipping configuration for GIP
    """
    hkd: NotRequired["ConfigurationCreateParamsTippingHkd"]
    """
    Tipping configuration for HKD
    """
    huf: NotRequired["ConfigurationCreateParamsTippingHuf"]
    """
    Tipping configuration for HUF
    """
    jpy: NotRequired["ConfigurationCreateParamsTippingJpy"]
    """
    Tipping configuration for JPY
    """
    mxn: NotRequired["ConfigurationCreateParamsTippingMxn"]
    """
    Tipping configuration for MXN
    """
    myr: NotRequired["ConfigurationCreateParamsTippingMyr"]
    """
    Tipping configuration for MYR
    """
    nok: NotRequired["ConfigurationCreateParamsTippingNok"]
    """
    Tipping configuration for NOK
    """
    nzd: NotRequired["ConfigurationCreateParamsTippingNzd"]
    """
    Tipping configuration for NZD
    """
    pln: NotRequired["ConfigurationCreateParamsTippingPln"]
    """
    Tipping configuration for PLN
    """
    ron: NotRequired["ConfigurationCreateParamsTippingRon"]
    """
    Tipping configuration for RON
    """
    sek: NotRequired["ConfigurationCreateParamsTippingSek"]
    """
    Tipping configuration for SEK
    """
    sgd: NotRequired["ConfigurationCreateParamsTippingSgd"]
    """
    Tipping configuration for SGD
    """
    usd: NotRequired["ConfigurationCreateParamsTippingUsd"]
    """
    Tipping configuration for USD
    """


class ConfigurationCreateParamsTippingAed(TypedDict):
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


class ConfigurationCreateParamsTippingAud(TypedDict):
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


class ConfigurationCreateParamsTippingCad(TypedDict):
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


class ConfigurationCreateParamsTippingChf(TypedDict):
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


class ConfigurationCreateParamsTippingCzk(TypedDict):
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


class ConfigurationCreateParamsTippingDkk(TypedDict):
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


class ConfigurationCreateParamsTippingEur(TypedDict):
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


class ConfigurationCreateParamsTippingGbp(TypedDict):
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


class ConfigurationCreateParamsTippingGip(TypedDict):
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


class ConfigurationCreateParamsTippingHkd(TypedDict):
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


class ConfigurationCreateParamsTippingHuf(TypedDict):
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


class ConfigurationCreateParamsTippingJpy(TypedDict):
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


class ConfigurationCreateParamsTippingMxn(TypedDict):
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


class ConfigurationCreateParamsTippingMyr(TypedDict):
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


class ConfigurationCreateParamsTippingNok(TypedDict):
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


class ConfigurationCreateParamsTippingNzd(TypedDict):
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


class ConfigurationCreateParamsTippingPln(TypedDict):
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


class ConfigurationCreateParamsTippingRon(TypedDict):
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


class ConfigurationCreateParamsTippingSek(TypedDict):
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


class ConfigurationCreateParamsTippingSgd(TypedDict):
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


class ConfigurationCreateParamsTippingUsd(TypedDict):
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


class ConfigurationCreateParamsVerifoneP400(TypedDict):
    splashscreen: NotRequired["Literal['']|str"]
    """
    A File ID representing an image you want to display on the reader.
    """


class ConfigurationCreateParamsWifi(TypedDict):
    enterprise_eap_peap: NotRequired[
        "ConfigurationCreateParamsWifiEnterpriseEapPeap"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-PEAP authentication method.
    """
    enterprise_eap_tls: NotRequired[
        "ConfigurationCreateParamsWifiEnterpriseEapTls"
    ]
    """
    Credentials for a WPA-Enterprise WiFi network using the EAP-TLS authentication method.
    """
    personal_psk: NotRequired["ConfigurationCreateParamsWifiPersonalPsk"]
    """
    Credentials for a WPA-Personal WiFi network.
    """
    type: Literal["enterprise_eap_peap", "enterprise_eap_tls", "personal_psk"]
    """
    Security type of the WiFi network. Fill out the hash with the corresponding name to provide the set of credentials for this security type.
    """


class ConfigurationCreateParamsWifiEnterpriseEapPeap(TypedDict):
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


class ConfigurationCreateParamsWifiEnterpriseEapTls(TypedDict):
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


class ConfigurationCreateParamsWifiPersonalPsk(TypedDict):
    password: str
    """
    Password for connecting to the WiFi network
    """
    ssid: str
    """
    Name of the WiFi network
    """
