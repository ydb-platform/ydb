#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline int is_leap(int year) {
    return ((year % 4 == 0 && year % 100 != 0) || year % 400 ==0);
}

double readstat_sav_date_parse(const char *s, char **dest) {
    // A SPSS date stored as the number of seconds since the start of the Gregorian calendar (midnight, Oct 14, 1582)
    // Through the C interface in savReaderWriter I've verifed that leap seconds is ignored
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
        *dest =(char*)s;
        return 0;
    } else {
        int days = 0;

        for (int i=1582; i<year; i++) {
            days += is_leap(i) ? 366 : 365;
        }
       
        for (int m=0; m<month; m++) {
            days += is_leap(year) ? daysPerMonthLeap[m] : daysPerMonth[m];
        }

        days += day-1;
        char buf[1024];
        *dest = (char*)s + snprintf(buf, sizeof(buf), "%d-%d-%d", year, month+1, day); 
        return (days * 86400.0) - 24710400; // 24710400 is the number of seconds in 1582 before Oct 14
    }
}

char* readstat_sav_date_string(double seconds, char* dest, int size) {
    int yr = 1582;
    int month = 0;
    int day = 1;
    int daysPerMonth[] =     {31,28,31,30,31,30,31,31,30,31,30,31};
    int daysPerMonthLeap[] = {31,29,31,30,31,30,31,31,30,31,30,31};
    double secs = seconds;
    secs += 24710400.0;
    double days = secs / 86400.0;
    double err = days - (long long)days;
    if (err != 0.0) {
        fprintf(stderr, "%s:%d time not supported. seconds was %lf, err was %lf\n", __FILE__, __LINE__, seconds, err);
        return NULL;
    }

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
    snprintf(dest, size, "%04d-%02d-%02d", yr, month+1, day);
    return dest;
}
