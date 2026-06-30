#pragma once

// macOS doesn't have the reentrant gethostbyname_r and gethostbyaddr_r functions
// They use thread-local storage for the standard functions instead
#undef HAVE_GETHOSTBYNAME_R
#undef HAVE_GETHOSTBYADDR_R
#undef GETHOSTBYNAME_R_NARGS
#undef GETHOSTBYADDR_R_NARGS

// Define other macOS-specific settings
#define HAVE_GETHOSTBYNAME 1
#define HAVE_GETHOSTBYADDR 1
