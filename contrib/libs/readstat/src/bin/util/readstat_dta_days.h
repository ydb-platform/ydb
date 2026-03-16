#ifndef __READSTAT_DTA_DAYS_H
#define __READSTAT_DTA_DAYS_H

int readstat_dta_num_days(const char *s, char** dest);
char* readstat_dta_days_string(int days, char* dest, int size);

#endif
