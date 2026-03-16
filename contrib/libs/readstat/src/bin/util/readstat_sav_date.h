#ifndef __READSTAT_SAV_DATE_H
#define __READSTAT_SAV_DATE_H

double readstat_sav_date_parse(const char *s, char **dest);
char* readstat_sav_date_string(double seconds, char* dest, int size);

#endif
