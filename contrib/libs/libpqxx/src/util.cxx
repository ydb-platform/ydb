/** Various utility functions.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <new>
#include <thread>

extern "C"
{
#include "libpq-fe.h"
}

#include "pqxx/except"
#include "pqxx/util"


using namespace pqxx::internal;


pqxx::thread_safety_model pqxx::describe_thread_safety() noexcept
{
  thread_safety_model model;

  if (PQisthreadsafe())
  {
    model.safe_libpq = true;
  }
  else
  {
    model.safe_libpq = false;
    model.description += "Using a libpq build that is not thread-safe.\n";
  }

  // Sadly I'm not aware of any way to avoid this just yet.
  model.safe_kerberos = false;
  model.description +=
	"Kerberos is not thread-safe.  If your application uses Kerberos, "
	"protect all calls to Kerberos or libpqxx using a global lock.\n";

  return model;
}


std::string pqxx::internal::namedclass::description() const
{
  try
  {
    std::string desc = classname();
    if (not name().empty()) desc += " '" + name() + "'";
    return desc;
  }
  catch (const std::exception &)
  {
    // Oops, string composition failed!  Probably out of memory.
    // Let's try something easier.
  }
  return name().empty() ? classname() : name();
}


void pqxx::internal::CheckUniqueRegistration(const namedclass *New,
    const namedclass *Old)
{
  if (New == nullptr)
    throw internal_error{"null pointer registered."};
  if (Old)
  {
    if (Old == New)
      throw usage_error{"Started twice: " + New->description()};
    throw usage_error{
	"Started " + New->description() + " while " + Old->description() +
	" still active."};
  }
}


void pqxx::internal::CheckUniqueUnregistration(const namedclass *New,
    const namedclass *Old)
{
  if (New != Old)
  {
    if (New == nullptr)
      throw usage_error{
	"Expected to close " + Old->description() + ", "
	"but got null pointer instead."};
    if (Old == nullptr)
      throw usage_error{"Closed while not open: " + New->description()};
    throw usage_error{
	"Closed " + New->description() + "; "
	"expected to close " + Old->description()};
  }
}


void pqxx::internal::freepqmem(const void *p) noexcept
{
  PQfreemem(const_cast<void *>(p));
}


void pqxx::internal::freemallocmem(const void *p) noexcept
{
  free(const_cast<void *>(p));
}


void pqxx::internal::sleep_seconds(int s)
{
  std::this_thread::sleep_for(std::chrono::seconds(s));
}
