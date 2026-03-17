#ifndef ISO_CALENDER_H
#define ISO_CALENDER_H

int
iso_to_ymd(const int iso_year, const int iso_week, const int iso_day,
           int *year, int *month, int *day);

int
ordinal_to_ymd(const int iso_year, const int ordinal_day, int *year,
               int *month, int *day);

#endif
