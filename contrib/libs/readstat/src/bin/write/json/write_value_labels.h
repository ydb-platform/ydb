#ifndef __WRITE_VALUE_LABELS_H
#define __WRITE_VALUE_LABELS_H

#include "../../../readstat.h"
#include "../../extract_metadata.h"

void add_val_labels(struct context *ctx, readstat_variable_t *variable, const char *val_labels);

#endif
