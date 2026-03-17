#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline int is_leap(int year) {
    return ((year % 4 == 0 && year % 100 != 0) || year % 400 ==0);
}

int readstat_dta_num_days(const char *s, char **dest) {
    int daysPerMonth[] =     {31,28,31,30,31,30,31,31,30,31,30,31};
    int daysPerMonthLeap[] = {31,29,31,30,31,30,31,31,30,31,30,31};
    int year, month, day;
    if (strlen(s) == 0) {
        *dest = (char*) s;
        return 0;
    }
    int ret = sscanf(s, "%d-%d-%d", &year, &month, &day);
    month--;
    if (month < 0 || month > 11 || ret!=3) {
        *dest = (char*)s;
        return 0;
    }
    int maxdays = (is_leap(year) ? daysPerMonthLeap : daysPerMonth)[month]; 
    if (day < 1 || day > maxdays) {
        *dest = (char*)s;
        return 0;
    } else {
        int days = 0;

        for (int i=year; i<1960; i++) {
            days -= is_leap(i) ? 366 : 365;
        }

        for (int i=1960; i<year; i++) {
            days += is_leap(i) ? 366 : 365;
        }
       
        for (int m=0; m<month; m++) {
            days += is_leap(year) ? daysPerMonthLeap[m] : daysPerMonth[m];
        }

        days += day-1;
        char buf[1024];
        *dest = (char*)s + snprintf(buf, sizeof(buf), "%d-%d-%d", year, month+1, day); 
        return days;
    }
}

char* readstat_dta_days_string(int days, char* dest, int size) {
    // TODO: Candidate for clean up
    int yr = 1960;
    int month = 0;
    int day = 1;
    int daysPerMonth[] =     {31,28,31,30,31,30,31,31,30,31,30,31};
    int daysPerMonthLeap[] = {31,29,31,30,31,30,31,31,30,31,30,31};
    if (days < 0) {
        yr = 1959;
        month = 11;
        days = - days;
        while (days > 0) {
            int days_in_year = is_leap(yr) ? 366 : 365;
            if (days > days_in_year) {
                yr-=1;
                days-=days_in_year;
                continue;
            }
            int days_in_month = is_leap(yr) ? daysPerMonthLeap[month] : daysPerMonth[month];
            if (days > days_in_month) {
                month-=1;
                days-=days_in_month;
                continue;
            }
            day = days_in_month-days + 1;
            days = 0;
        }
    } else {
        while (days > 0) {
            int days_in_year = is_leap(yr) ? 366 : 365;
            if (days >= days_in_year) {
                yr+=1;
                days-=days_in_year;
                continue;
            }
            int days_in_month = is_leap(yr) ? daysPerMonthLeap[month] : daysPerMonth[month];
            if (days >= days_in_month) {
                month+=1;
                days-=days_in_month;
                continue;
            }
            day+= days;
            days = 0;
        }
    }
    snprintf(dest, size, "%04d-%02d-%02d", yr, month+1, day);
    return dest;
}
