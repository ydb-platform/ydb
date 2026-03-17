"""Auto-generated file, do not edit by hand. MF metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_MF = PhoneMetadata(id='MF', country_code=590, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:590\\d|7090)\\d{5}|(?:69|80|9\\d)\\d{7}', possible_length=(9,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='590(?:0[079]|[14]3|[27][79]|3[03-7]|5[0-268]|87)\\d{4}', example_number='590271234', possible_length=(9,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:69(?:0\\d\\d|1(?:2[2-9]|3[0-5])|4(?:0[89]|1[2-6]|9\\d)|6(?:1[016-9]|5[0-4]|[67]\\d))|7090[0-4])\\d{4}', example_number='690001234', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-5]\\d{6}', example_number='800012345', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='9(?:(?:39[5-7]|76[018])\\d|475[0-6])\\d{4}', example_number='976012345', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    mobile_number_portable_region=True)
