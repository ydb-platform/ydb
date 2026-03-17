"""Auto-generated file, do not edit by hand. CN metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_CN = PhoneMetadata(id='CN', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[19]\\d{2,5}', possible_length=(3, 4, 5, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='1(?:1[09]|2(?:[02]|1\\d\\d|395))', example_number='110', possible_length=(3, 5)),
    emergency=PhoneNumberDesc(national_number_pattern='1(?:1[09]|20)', example_number='110', possible_length=(3,)),
    short_code=PhoneNumberDesc(national_number_pattern='1(?:00|1[0249]|2395|6[08])|9[56]\\d{3,4}|12[023]|1(?:0(?:[0-26]\\d|8)|21\\d)\\d', example_number='100', possible_length=(3, 4, 5, 6)),
    standard_rate=PhoneNumberDesc(national_number_pattern='1(?:0(?:[0-26]\\d|8)\\d|1[24]|23|6[08])|9[56]\\d{3,4}|100', example_number='100', possible_length=(3, 4, 5, 6)),
    sms_services=PhoneNumberDesc(national_number_pattern='12110', example_number='12110', possible_length=(5,)),
    short_data=True)
