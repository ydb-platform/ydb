// -*- c++ -*-
//
// Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
//                         University Research and Technology
//                         Corporation.  All rights reserved.
// Copyright (c) 2004-2005 The University of Tennessee and The University
//                         of Tennessee Research Foundation.  All rights
//                         reserved.
// Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
//                         University of Stuttgart.  All rights reserved.
// Copyright (c) 2004-2005 The Regents of the University of California.
//                         All rights reserved.
// Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

//
// Point-to-Point Communication
//

void
Attach_buffer(void* buffer, int size);

int
Detach_buffer(void*& buffer);

//
// Process Topologies
//

void
Compute_dims(int nnodes, int ndims, int dims[]);

//
// Environmental Inquiry
//

int
Add_error_class();

int
Add_error_code(int errorclass);

void
Add_error_string(int errorcode, const char* string);

void
Get_processor_name(char* name, int& resultlen);

void
Get_error_string(int errorcode, char* string, int& resultlen);

int
Get_error_class(int errorcode);

double
Wtime();

double
Wtick();

void
Init(int& argc, char**& argv);

void
Init();

OMPI_DECLSPEC void
InitializeIntercepts();

void
Real_init();

void
Finalize();

bool
Is_initialized();

bool
Is_finalized();

//
// External Interfaces
//

int
Init_thread(int &argc, char**&argv, int required);

int
Init_thread(int required);

bool
Is_thread_main();

int
Query_thread();


//
// Miscellany
//


void*
Alloc_mem(Aint size, const Info& info);


void
Free_mem(void* base);

//
// Process Creation
//

void
Close_port(const char* port_name);


void
Lookup_name(const char* service_name, const Info& info, char* port_name);


void
Open_port(const Info& info, char* port_name);


void
Publish_name(const char* service_name, const Info& info,
	     const char* port_name);

void
Unpublish_name(const char* service_name, const Info& info,
	       const char* port_name);

//
// Profiling
//

void
Pcontrol(const int level, ...);

void
Get_version(int& version, int& subversion);

MPI::Aint
Get_address(void* location);




