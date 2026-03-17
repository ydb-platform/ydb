"""Auto-generated file, do not edit by hand. VG metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_VG = PhoneMetadata(id='VG', country_code=1, international_prefix='011',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:284|[58]\\d\\d|900)\\d{7}', possible_length=(10,), possible_length_local_only=(7,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='284(?:229|4(?:22|9[45])|774|8(?:52|6[459]))\\d{4}', example_number='2842291234', possible_length=(10,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='284(?:245|3(?:0[0-3]|4[0-7]|68|9[34])|4(?:4[0-6]|68|9[69])|5(?:4[0-7]|68|9[69]))\\d{4}', example_number='2843001234', possible_length=(10,), possible_length_local_only=(7,)),
    toll_free=PhoneNumberDesc(national_number_pattern='8(?:00|33|44|55|66|77|88)[2-9]\\d{6}', example_number='8002345678', possible_length=(10,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='900[2-9]\\d{6}', example_number='9002345678', possible_length=(10,)),
    personal_number=PhoneNumberDesc(national_number_pattern='52(?:3(?:[2-46-9][02-9]\\d|5(?:[02-46-9]\\d|5[0-46-9]))|4(?:[2-478][02-9]\\d|5(?:[034]\\d|2[024-9]|5[0-46-9])|6(?:0[1-9]|[2-9]\\d)|9(?:[05-9]\\d|2[0-5]|49)))\\d{4}|52[34][2-9]1[02-9]\\d{4}|5(?:00|2[125-9]|33|44|66|77|88)[2-9]\\d{6}', example_number='5002345678', possible_length=(10,)),
    national_prefix='1',
    national_prefix_for_parsing='([2-578]\\d{6})$|1',
    national_prefix_transform_rule='284\\1',
    leading_digits='284',
    mobile_number_portable_region=True)
