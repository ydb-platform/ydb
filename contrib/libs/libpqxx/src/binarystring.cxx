/** Implementation of bytea (binary string) conversions.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>

extern "C"
{
#include "libpq-fe.h"
}

#include "pqxx/binarystring"
#include "pqxx/field"


using namespace pqxx::internal;

namespace
{
using unsigned_char = unsigned char;
using buffer = std::pair<unsigned char *, size_t>;


buffer to_buffer(const void *data, size_t len)
{
  void *const output{malloc(len + 1)};
  if (output == nullptr) throw std::bad_alloc{};
  static_cast<char *>(output)[len] = '\0';
  memcpy(static_cast<char *>(output), data, len);
  return buffer{static_cast<unsigned char *>(output), len};
}


buffer to_buffer(const std::string &source)
{
  return to_buffer(source.c_str(), source.size());
}



buffer unescape(const unsigned char escaped[])
{
#ifdef _WIN32
  /* On Windows only, the return value from PQunescapeBytea() must be freed
   * using PQfreemem.  Copy to a buffer allocated by libpqxx, so that the
   * binarystring's buffer can be freed uniformly,
   */
  size_t unescaped_len = 0;
  // TODO: Use make_unique once we require C++14.  Sooo much easier.
  std::unique_ptr<unsigned char, void(*)(unsigned char *)> A(
	PQunescapeBytea(const_cast<unsigned char *>(escaped), &unescaped_len),
	freepqmem_templated<unsigned char>);
  void *data = A.get();
  if (data == nullptr) throw std::bad_alloc{};
  return to_buffer(data, unescaped_len);
#else
  /* On non-Windows platforms, it's okay to free libpq-allocated memory using
   * free().  No extra copy needed.
   */
  buffer unescaped;
  unescaped.first = PQunescapeBytea(
	const_cast<unsigned char *>(escaped), &unescaped.second);
  if (unescaped.first == nullptr) throw std::bad_alloc{};
  return unescaped;
#endif
}

} // namespace


pqxx::binarystring::binarystring(const field &F) :
  m_buf{make_smart_pointer()},
  m_size{0}
{
  buffer unescaped{unescape(reinterpret_cast<const_pointer>(F.c_str()))};
  m_buf = make_smart_pointer(unescaped.first);
  m_size = unescaped.second;
}


pqxx::binarystring::binarystring(const std::string &s) :
  m_buf{make_smart_pointer()},
  m_size{s.size()}
{
  m_buf = make_smart_pointer(to_buffer(s).first);
}


pqxx::binarystring::binarystring(const void *binary_data, size_t len) :
  m_buf{make_smart_pointer()},
  m_size{len}
{
  m_buf = make_smart_pointer(to_buffer(binary_data, len).first);
}


bool pqxx::binarystring::operator==(const binarystring &rhs) const noexcept
{
  if (rhs.size() != size()) return false;
  return std::memcmp(data(), rhs.data(), size()) == 0;
}


pqxx::binarystring &pqxx::binarystring::operator=(const binarystring &rhs)
{
  m_buf = rhs.m_buf;
  m_size = rhs.m_size;
  return *this;
}


pqxx::binarystring::const_reference pqxx::binarystring::at(size_type n) const
{
  if (n >= m_size)
  {
    if (m_size == 0)
      throw std::out_of_range{"Accessing empty binarystring"};
    throw std::out_of_range{
	"binarystring index out of range: " +
	to_string(n) + " (should be below " + to_string(m_size) + ")"};
  }
  return data()[n];
}


void pqxx::binarystring::swap(binarystring &rhs)
{
  m_buf.swap(rhs.m_buf);

  // This part very obviously can't go wrong, so do it last
  const auto s = m_size;
  m_size = rhs.m_size;
  rhs.m_size = s;
}


std::string pqxx::binarystring::str() const
{
  return std::string{get(), m_size};
}
