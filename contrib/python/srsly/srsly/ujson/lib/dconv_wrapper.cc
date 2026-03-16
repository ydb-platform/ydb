#include "double-conversion.h"

namespace double_conversion
{
  static StringToDoubleConverter* s2d_instance = NULL;
  static DoubleToStringConverter* d2s_instance = NULL;

  extern "C"
  {
    void dconv_d2s_init(int flags,
                        const char* infinity_symbol,
                        const char* nan_symbol,
                        char exponent_character,
                        int decimal_in_shortest_low,
                        int decimal_in_shortest_high,
                        int max_leading_padding_zeroes_in_precision_mode,
                        int max_trailing_padding_zeroes_in_precision_mode)
    {
        d2s_instance = new DoubleToStringConverter(flags, infinity_symbol, nan_symbol,
                                        exponent_character, decimal_in_shortest_low,
                                        decimal_in_shortest_high, max_leading_padding_zeroes_in_precision_mode,
                                        max_trailing_padding_zeroes_in_precision_mode);
    }

    int dconv_d2s(double value, char* buf, int buflen, int* strlength)
    {
        StringBuilder sb(buf, buflen);
        int success =  static_cast<int>(d2s_instance->ToShortest(value, &sb));
        *strlength = success ? sb.position() : -1;
        return success;
    }

    void dconv_d2s_free()
    {
        delete d2s_instance;
        d2s_instance = NULL;
    }

    void dconv_s2d_init(int flags, double empty_string_value, 
                        double junk_string_value, const char* infinity_symbol,
                        const char* nan_symbol)
    {
        s2d_instance = new StringToDoubleConverter(flags, empty_string_value,
                            junk_string_value, infinity_symbol, nan_symbol);
    }

    double dconv_s2d(const char* buffer, int length, int* processed_characters_count)
    {
        return s2d_instance->StringToDouble(buffer, length, processed_characters_count);
    }

    void dconv_s2d_free()
    {
        delete s2d_instance;
        s2d_instance = NULL;
    }
  }
}
