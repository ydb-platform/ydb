replace_has_builtin_in()
{
    sed 's/#if __has_builtin('${1}')/#if __has_builtin('${1}') \&\& !defined(__CUDACC__)/g' -i include/__type_traits/${2}
}

replace_has_extension_in()
{
    sed 's/#if __has_extension('${1}')/#if __has_extension('${1}') \&\& !defined(__CUDACC__)/g' -i include/__type_traits/${2}
}

replace_has_builtin_in __array_extent extent.h
replace_has_builtin_in __is_compound is_compound.h
replace_has_builtin_in __is_const is_const.h
replace_has_builtin_in __is_destructible is_destructible.h
replace_has_builtin_in __is_function is_function.h
replace_has_builtin_in __is_fundamental is_fundamental.h
replace_has_builtin_in __is_integral is_integral.h
replace_has_builtin_in __is_member_function_pointer is_member_function_pointer.h
replace_has_builtin_in __is_member_object_pointer is_member_object_pointer.h
replace_has_builtin_in __is_member_pointer is_member_pointer.h
replace_has_builtin_in __is_object is_object.h
replace_has_builtin_in __is_pointer is_pointer.h
replace_has_builtin_in __is_scalar is_scalar.h
replace_has_builtin_in __is_signed is_signed.h
replace_has_builtin_in __is_trivially_destructible is_trivially_destructible.h
replace_has_builtin_in __is_unsigned is_unsigned.h
replace_has_builtin_in __is_void is_void.h
replace_has_builtin_in __is_volatile is_volatile.h
replace_has_builtin_in __is_trivially_relocatable is_trivially_relocatable.h

replace_has_extension_in datasizeof datasizeof.h
