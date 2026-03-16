#include <sys/time.h>
#include <stdlib.h>
#include <time.h>

#include "ompi_config.h"
#include "sharedfp_individual.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"

#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"

double mca_sharedfp_individual_gettime(void)
{

	struct timeval timestamp;
	double seconds  = 0.0;
	double microsec = 0.0;

	gettimeofday(&timestamp,NULL);
	seconds  = (double)timestamp.tv_sec;
	microsec = ((double)timestamp.tv_usec)/((double)1000000.0);

	return (seconds+microsec);
}
