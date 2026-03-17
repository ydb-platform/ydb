#include "tm_verbose.h"
#include <stdio.h>
static unsigned int verbose_level = ERROR;
static FILE *output = NULL;

void tm_set_verbose_level(unsigned int level){
  verbose_level = level;
}

unsigned int tm_get_verbose_level(){
  return verbose_level;
}

int tm_open_verbose_file(char *filename){
  output = fopen(filename,"w");
  if(output == NULL)
    return 0;
  else
    return 1;
}

int tm_close_verbose_file(void){
  if(output != NULL)
    return fclose(output);
  
  return 0;
}

FILE *tm_get_verbose_output(){
  if(!output)
    return stdout;
  else
    return output;
}
