set -xue

replace_has_builtin_in()
{
    sed 's/if __has_builtin('${1}')/if __has_builtin('${1}') \&\& !defined(__CUDACC__)/g' -i include/${2}
}

replace_has_extension_in()
{
    sed 's/if __has_extension('${1}')/if __has_extension('${1}') \&\& !defined(__CUDACC__)/g' -i include/${2}
}

replace_has_builtin_in_type_traits()
{
    replace_has_builtin_in ${1} __type_traits/${2}
}

replace_has_extension_in_type_traits()
{
    replace_has_extension_in ${1} __type_traits/${2}
}

replace_has_builtin_in_type_traits __array_extent extent.h
replace_has_builtin_in_type_traits __is_compound is_compound.h
replace_has_builtin_in_type_traits __is_const is_const.h
replace_has_builtin_in_type_traits __is_destructible is_destructible.h
replace_has_builtin_in_type_traits __is_function is_function.h
replace_has_builtin_in_type_traits __is_fundamental is_fundamental.h
replace_has_builtin_in_type_traits __is_integral is_integral.h
replace_has_builtin_in_type_traits __is_member_pointer is_member_pointer.h
replace_has_builtin_in_type_traits __is_nothrow_convertible is_nothrow_convertible.h
replace_has_builtin_in_type_traits __is_object is_object.h
replace_has_builtin_in_type_traits __is_pointer is_pointer.h
replace_has_builtin_in_type_traits __is_scalar is_scalar.h
replace_has_builtin_in_type_traits __is_signed is_signed.h
replace_has_builtin_in_type_traits __is_trivially_destructible is_trivially_destructible.h
replace_has_builtin_in_type_traits __is_trivially_relocatable is_trivially_relocatable.h
replace_has_builtin_in_type_traits __is_unsigned is_unsigned.h
replace_has_builtin_in_type_traits __is_void is_void.h
replace_has_builtin_in_type_traits __is_volatile is_volatile.h

replace_has_builtin_in __builtin_verbose_trap __assertion_handler
replace_has_builtin_in __builtin_clzg __bit/countl.h
replace_has_builtin_in __builtin_ctzg __bit/countr.h

replace_has_extension_in_type_traits datasizeof datasizeof.h
