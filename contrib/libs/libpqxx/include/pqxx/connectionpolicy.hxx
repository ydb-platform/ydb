/** Definition of the connection policy classes.
 *
 * Interface for defining connection policies
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/connection instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_CONNECTIONPOLICY
#define PQXX_H_CONNECTIONPOLICY

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include <string>

#include "pqxx/internal/libpq-forward.hxx"


namespace pqxx
{

/**
 * @addtogroup connection Connection classes
 */
//@{

class PQXX_LIBEXPORT connectionpolicy
{
public:
  using handle = internal::pq::PGconn *;

  explicit connectionpolicy(const std::string &opts);
  virtual ~connectionpolicy() noexcept;

  const std::string &options() const noexcept { return m_options; }

  virtual handle do_startconnect(handle orig);
  virtual handle do_completeconnect(handle orig);
  virtual handle do_dropconnect(handle orig) noexcept;
  virtual handle do_disconnect(handle orig) noexcept;
  virtual bool is_ready(handle) const noexcept;

protected:
  handle normalconnect(handle);

private:
  std::string m_options;
};

//@}
} // namespace

#include "pqxx/compiler-internal-post.hxx"

#endif
