"""Auto-generated file, do not edit by hand. MY metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_MY = PhoneMetadata(id='MY', country_code=60, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='1\\d{8,9}|(?:3\\d|[4-9])\\d{7}', possible_length=(8, 9, 10), possible_length_local_only=(6, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='4270\\d{4}|(?:3(?:2[0-36-9]|3[0-368]|4[0-278]|5[0-24-8]|6[0-467]|7[1246-9]|8\\d|9[0-57])\\d|4(?:2[0-689]|[3-79]\\d|8[1-35689])|5(?:2[0-589]|[3468]\\d|5[0-489]|7[1-9]|9[23])|6(?:2[2-9]|3[1357-9]|[46]\\d|5[0-6]|7[0-35-9]|85|9[015-8])|7(?:[2579]\\d|3[03-68]|4[0-8]|6[5-9]|8[0-35-9])|8(?:[24][2-8]|3[2-5]|5[2-7]|6[2-589]|7[2-578]|[89][2-9])|9(?:0[57]|13|[25-7]\\d|[3489][0-8]))\\d{5}', example_number='323856789', possible_length=(8, 9), possible_length_local_only=(6, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='1(?:1888[689]|4400|8(?:47|8[27])[0-4])\\d{4}|1(?:0(?:[23568]\\d|4[0-6]|7[016-9]|9[0-8])|1(?:[1-5]\\d\\d|6(?:0[5-9]|[1-9]\\d)|7(?:[0-4]\\d|5[0-7]))|(?:[269]\\d|[37][1-9]|4[235-9])\\d|5(?:31|9\\d\\d)|8(?:1[23]|[236]\\d|4[06]|5(?:46|[7-9])|7[016-9]|8[01]|9[0-8]))\\d{5}', example_number='123456789', possible_length=(9, 10)),
    toll_free=PhoneNumberDesc(national_number_pattern='1[378]00\\d{6}', example_number='1300123456', possible_length=(10,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='1600\\d{6}', example_number='1600123456', possible_length=(10,)),
    voip=PhoneNumberDesc(national_number_pattern='15(?:4(?:6[0-4]\\d|8(?:0[125]|[17]\\d|21|3[01]|4[01589]|5[014]|6[02]))|6(?:32[0-6]|78\\d))\\d{4}', example_number='1546012345', possible_length=(10,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d)(\\d{3})(\\d{4})', format='\\1-\\2 \\3', leading_digits_pattern=['[4-79]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1-\\2 \\3', leading_digits_pattern=['1(?:[02469]|[378][1-9]|53)|8', '1(?:[02469]|[37][1-9]|53|8(?:[1-46-9]|5[7-9]))|8'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d)(\\d{4})(\\d{4})', format='\\1-\\2 \\3', leading_digits_pattern=['3'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d)(\\d{3})(\\d{2})(\\d{4})', format='\\1-\\2-\\3-\\4', leading_digits_pattern=['1(?:[367]|80)']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{4})', format='\\1-\\2 \\3', leading_digits_pattern=['15'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{4})', format='\\1-\\2 \\3', leading_digits_pattern=['1'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
