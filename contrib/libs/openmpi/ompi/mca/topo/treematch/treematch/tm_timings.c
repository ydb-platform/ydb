#include "tm_timings.h"

static CLOCK_T time_tab[MAX_CLOCK];
static int clock_num = -1;

void get_time(void)
{
  clock_num++;

  if(clock_num>MAX_CLOCK-1)
    return;

  CLOCK(time_tab[clock_num]);
}

double time_diff(void)
{
  CLOCK_T t2,t1;

  if(clock_num>MAX_CLOCK-1){
    clock_num--;
    return -1.0;
  }

  if(clock_num < 0){
    return -2.0;
  }

  CLOCK(t2);
  t1=time_tab[clock_num--];

  return CLOCK_DIFF(t2,t1);
}
