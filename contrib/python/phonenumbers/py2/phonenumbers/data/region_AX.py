"""Auto-generated file, do not edit by hand. AX metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_AX = PhoneMetadata(id='AX', country_code=358, international_prefix='00|99(?:[01469]|5(?:[14]1|3[23]|5[59]|77|88|9[09]))',
    general_desc=PhoneNumberDesc(national_number_pattern='2\\d{4,9}|35\\d{4,5}|(?:60\\d\\d|800)\\d{4,6}|7\\d{5,11}|(?:[14]\\d|3[0-46-9]|50)\\d{4,8}', possible_length=(5, 6, 7, 8, 9, 10, 11, 12)),
    fixed_line=PhoneNumberDesc(national_number_pattern='18[1-8]\\d{3,6}', example_number='181234567', possible_length=(6, 7, 8, 9)),
    mobile=PhoneNumberDesc(national_number_pattern='4946\\d{2,6}|(?:4[0-8]|50)\\d{4,8}', example_number='412345678', possible_length=(6, 7, 8, 9, 10)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{4,6}', example_number='800123456', possible_length=(7, 8, 9)),
    premium_rate=PhoneNumberDesc(national_number_pattern='[67]00\\d{5,6}', example_number='600123456', possible_length=(8, 9)),
    uan=PhoneNumberDesc(national_number_pattern='20\\d{4,8}|60[12]\\d{5,6}|7(?:099\\d{4,5}|5[03-9]\\d{3,7})|20[2-59]\\d\\d|(?:606|7(?:0[78]|1|3\\d))\\d{7}|(?:10|29|3[09]|70[1-5]\\d)\\d{4,8}', example_number='10112345', possible_length=(5, 6, 7, 8, 9, 10, 11, 12)),
    preferred_international_prefix='00',
    national_prefix='0',
    national_prefix_for_parsing='0',
    leading_digits='18')
