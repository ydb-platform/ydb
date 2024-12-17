#ifndef JINJA2CPP_CONFIG_H
#define JINJA2CPP_CONFIG_H

// The Jinja2C++ library version in the form major * 10000 + minor * 100 + patch.
#define JINJA2CPP_VERSION 10100

#ifdef _WIN32
#define JINJA2_DECLSPEC(S) __declspec(S)
#if _MSC_VER
#pragma warning(disable : 4251)
#endif
#else
#define JINJA2_DECLSPEC(S)
#endif

#ifdef JINJA2CPP_BUILD_AS_SHARED
#define JINJA2CPP_EXPORT JINJA2_DECLSPEC(dllexport)
#define JINJA2CPP_SHARED_LIB
#elif JINJA2CPP_LINK_AS_SHARED
#define JINJA2CPP_EXPORT JINJA2_DECLSPEC(dllimport)
#define JINJA2CPP_SHARED_LIB
#else
#define JINJA2CPP_EXPORT
#define JINJA2CPP_STATIC_LIB
#endif

#endif // JINJA2CPP_CONFIG_H
