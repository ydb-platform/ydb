/** Large Objects interface.
 *
 * Allows access to large objects directly, or through I/O streams.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/largeobject instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_LARGEOBJECT
#define PQXX_H_LARGEOBJECT

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include <streambuf>

#include "pqxx/dbtransaction.hxx"


namespace pqxx
{
/// Identity of a large object
/** This class encapsulates the identity of a large object.  To access the
 * contents of the object, create a largeobjectaccess, a largeobject_streambuf,
 * or an ilostream, an olostream or a lostream around the largeobject.
 *
 * A largeobject must be accessed only from within a backend transaction, but
 * the object's identity remains valid as long as the object exists.
 */
class PQXX_LIBEXPORT largeobject
{
public:
  using size_type = large_object_size_type;

  /// Refer to a nonexistent large object (similar to what a null pointer does)
  largeobject() noexcept =default;					//[t48]

  /// Create new large object
  /** @param T Backend transaction in which the object is to be created
   */
  explicit largeobject(dbtransaction &T);				//[t48]

  /// Wrap object with given oid
  /** Convert combination of a transaction and object identifier into a
   * large object identity.  Does not affect the database.
   * @param O Object identifier for the given object
   */
  explicit largeobject(oid O) noexcept : m_id{O} {}			//[t48]

  /// Import large object from a local file
  /** Creates a large object containing the data found in the given file.
   * @param T Backend transaction in which the large object is to be created
   * @param File A filename on the client program's filesystem
   */
  largeobject(dbtransaction &T, const std::string &File);		//[t53]

  /// Take identity of an opened large object
  /** Copy identity of already opened large object.  Note that this may be done
   * as an implicit conversion.
   * @param O Already opened large object to copy identity from
   */
  largeobject(const largeobjectaccess &O) noexcept;			//[t50]

  /// Object identifier
  /** The number returned by this function identifies the large object in the
   * database we're connected to (or oid_none is returned if we refer to the
   * null object).
   */
  oid id() const noexcept { return m_id; }				//[t48]

  /**
   * @name Identity comparisons
   *
   * These operators compare the object identifiers of large objects.  This has
   * nothing to do with the objects' actual contents; use them only for keeping
   * track of containers of references to large objects and such.
   */
  //@{
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator==(const largeobject &other) const			//[t51]
	  { return m_id == other.m_id; }
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator!=(const largeobject &other) const			//[t51]
	  { return m_id != other.m_id; }
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator<=(const largeobject &other) const			//[t51]
	  { return m_id <= other.m_id; }
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator>=(const largeobject &other) const			//[t51]
	  { return m_id >= other.m_id; }
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator<(const largeobject &other) const			//[t51]
	  { return m_id < other.m_id; }
  /// Compare object identities
  /** @warning Only valid between large objects in the same database. */
  bool operator>(const largeobject &other) const			//[t51]
	  { return m_id > other.m_id; }
  //@}

  /// Export large object's contents to a local file
  /** Writes the data stored in the large object to the given file.
   * @param T Transaction in which the object is to be accessed
   * @param File A filename on the client's filesystem
   */
  void to_file(dbtransaction &T, const std::string &File) const;	//[t52]

  /// Delete large object from database
  /** Unlike its low-level equivalent cunlink, this will throw an exception if
   * deletion fails.
   * @param T Transaction in which the object is to be deleted
   */
  void remove(dbtransaction &T) const;					//[t48]

protected:
  PQXX_PURE static internal::pq::PGconn *raw_connection(
	const dbtransaction &T);

  PQXX_PRIVATE std::string reason(const connection_base &, int err) const;

private:
  oid m_id = oid_none;
};


// TODO: New hierarchy with separate read / write / mixed-mode access

/// Accessor for large object's contents.
class PQXX_LIBEXPORT largeobjectaccess : private largeobject
{
public:
  using largeobject::size_type;
  using off_type = long;
  using pos_type = size_type;

  /// Open mode: @c in, @c out (can be combined with the "or" operator)
  /** According to the C++ standard, these should be in @c std::ios_base.  We
   * take them from @c std::ios instead, which should be safe because it
   * inherits the same definition, to accommodate gcc 2.95 & 2.96.
   */
  using openmode = std::ios::openmode;

  /// Seek direction: @c beg, @c cur, @c end
  /** According to the C++ standard, these should be in @c std::ios_base.  We
   * take them from @c std::ios instead, which should be safe because it
   * inherits the same definition, to accommodate gcc 2.95 & 2.96.
   */
  using seekdir = std::ios::seekdir;

  /// Create new large object and open it
  /**
   * @param T Backend transaction in which the object is to be created
   * @param mode Access mode, defaults to ios_base::in | ios_base::out
   */
  explicit largeobjectaccess(						//[t51]
	dbtransaction &T,
	openmode mode=std::ios::in|std::ios::out);

  /// Open large object with given oid
  /** Convert combination of a transaction and object identifier into a
   * large object identity.  Does not affect the database.
   * @param T Transaction in which the object is to be accessed
   * @param O Object identifier for the given object
   * @param mode Access mode, defaults to ios_base::in | ios_base::out
   */
  largeobjectaccess(							//[t52]
	dbtransaction &T,
	oid O,
	openmode mode=std::ios::in|std::ios::out);

  /// Open given large object
  /** Open a large object with the given identity for reading and/or writing
   * @param T Transaction in which the object is to be accessed
   * @param O Identity for the large object to be accessed
   * @param mode Access mode, defaults to ios_base::in | ios_base::out
   */
  largeobjectaccess(							//[t50]
	dbtransaction &T,
	largeobject O,
	openmode mode=std::ios::in|std::ios::out);

  /// Import large object from a local file and open it
  /** Creates a large object containing the data found in the given file.
   * @param T Backend transaction in which the large object is to be created
   * @param File A filename on the client program's filesystem
   * @param mode Access mode, defaults to ios_base::in | ios_base::out
   */
  largeobjectaccess(							//[t55]
	dbtransaction &T,
	const std::string &File,
	openmode mode=std::ios::in|std::ios::out);

  ~largeobjectaccess() noexcept { close(); }

  /// Object identifier
  /** The number returned by this function uniquely identifies the large object
   * in the context of the database we're connected to.
   */
  using largeobject::id;

  /// Export large object's contents to a local file
  /** Writes the data stored in the large object to the given file.
   * @param File A filename on the client's filesystem
   */
  void to_file(const std::string &File) const				//[t54]
	{ largeobject::to_file(m_trans, File); }

  using largeobject::to_file;

  /**
   * @name High-level access to object contents
   */
  //@{
  /// Write data to large object
  /** If not all bytes could be written, an exception is thrown.
   * @param Buf Data to write
   * @param Len Number of bytes from Buf to write
   */
  void write(const char Buf[], size_type Len);				//[t51]

  /// Write string to large object
  /** If not all bytes could be written, an exception is thrown.
   * @param Buf Data to write; no terminating zero is written
   */
  void write(const std::string &Buf)					//[t50]
	{ write(Buf.c_str(), static_cast<size_type>(Buf.size())); }

  /// Read data from large object
  /** Throws an exception if an error occurs while reading.
   * @param Buf Location to store the read data in
   * @param Len Number of bytes to try and read
   * @return Number of bytes read, which may be less than the number requested
   * if the end of the large object is reached
   */
  size_type read(char Buf[], size_type Len);				//[t50]

  /// Seek in large object's data stream
  /** Throws an exception if an error occurs.
   * @return The new position in the large object
   */
  size_type seek(size_type dest, seekdir dir);				//[t51]

  /// Report current position in large object's data stream
  /** Throws an exception if an error occurs.
   * @return The current position in the large object
   */
  size_type tell() const;						//[t50]
  //@}

  /**
   * @name Low-level access to object contents
   *
   * These functions provide a more "C-like" access interface, returning special
   * values instead of throwing exceptions on error.  These functions are
   * generally best avoided in favour of the high-level access functions, which
   * behave more like C++ functions should.
   */
  //@{
  /// Seek in large object's data stream
  /** Does not throw exception in case of error; inspect return value and
   * @c errno instead.
   * @param dest Offset to go to
   * @param dir Origin to which dest is relative: ios_base::beg (from beginning
   *        of the object), ios_base::cur (from current access position), or
   *        ios_base;:end (from end of object)
   * @return New position in large object, or -1 if an error occurred.
   */
  pos_type cseek(off_type dest, seekdir dir) noexcept;			//[t50]

  /// Write to large object's data stream
  /** Does not throw exception in case of error; inspect return value and
   * @c errno instead.
   * @param Buf Data to write
   * @param Len Number of bytes to write
   * @return Number of bytes actually written, or -1 if an error occurred.
   */
  off_type cwrite(const char Buf[], size_type Len) noexcept;		//[t50]

  /// Read from large object's data stream
  /** Does not throw exception in case of error; inspect return value and
   * @c errno instead.
   * @param Buf Area where incoming bytes should be stored
   * @param Len Number of bytes to read
   * @return Number of bytes actually read, or -1 if an error occurred.
   */
  off_type cread(char Buf[], size_type Len) noexcept;			//[t50]

  /// Report current position in large object's data stream
  /** Does not throw exception in case of error; inspect return value and
   * @c errno instead.
   * @return Current position in large object, of -1 if an error occurred.
   */
  pos_type ctell() const noexcept;					//[t50]
  //@}

  /**
   * @name Error/warning output
   */
  //@{
  /// Issue message to transaction's notice processor
  void process_notice(const std::string &) noexcept;			//[t50]
  //@}

  using largeobject::remove;

  using largeobject::operator==;
  using largeobject::operator!=;
  using largeobject::operator<;
  using largeobject::operator<=;
  using largeobject::operator>;
  using largeobject::operator>=;

private:
  PQXX_PRIVATE std::string reason(int err) const;
  internal::pq::PGconn *raw_connection() const
	{ return largeobject::raw_connection(m_trans); }

  PQXX_PRIVATE void open(openmode mode);
  void close() noexcept;

  dbtransaction &m_trans;
  int m_fd = -1;

  largeobjectaccess() =delete;
  largeobjectaccess(const largeobjectaccess &) =delete;
  largeobjectaccess operator=(const largeobjectaccess &) =delete;
};


/// Streambuf to use large objects in standard I/O streams
/** The standard streambuf classes provide uniform access to data storage such
 * as files or string buffers, so they can be accessed using standard input or
 * output streams.  This streambuf implementation provides similar access to
 * large objects, so they can be read and written using the same stream classes.
 *
 * @warning This class may not work properly in compiler environments that don't
 * fully support Standard-compliant streambufs, such as g++ 2.95 or older.
 */
template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class largeobject_streambuf :
    public std::basic_streambuf<CHAR, TRAITS>
{
  using size_type = long;
public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;
  using openmode = largeobjectaccess::openmode;
  using seekdir = largeobjectaccess::seekdir;

  largeobject_streambuf(						//[t48]
	dbtransaction &T,
	largeobject O,
	openmode mode=std::ios::in|std::ios::out,
	size_type BufSize=512) :
    m_bufsize{BufSize},
    m_obj{T, O, mode},
    m_g{nullptr},
    m_p{nullptr}
	{ initialize(mode); }

  largeobject_streambuf(						//[t48]
	dbtransaction &T,
	oid O,
	openmode mode=std::ios::in|std::ios::out,
	size_type BufSize=512) :
    m_bufsize{BufSize},
    m_obj{T, O, mode},
    m_g{nullptr},
    m_p{nullptr}
	{ initialize(mode); }

  virtual ~largeobject_streambuf() noexcept
	{ delete [] m_p; delete [] m_g; }


  /// For use by large object stream classes
  void process_notice(const std::string &s) { m_obj.process_notice(s); }

protected:
  virtual int sync() override
  {
    // setg() sets eback, gptr, egptr
    this->setg(this->eback(), this->eback(), this->egptr());
    return overflow(EoF());
  }

  virtual pos_type seekoff(
	off_type offset,
	seekdir dir,
	openmode)
	override
  {
    return AdjustEOF(m_obj.cseek(largeobjectaccess::off_type(offset), dir));
  }

  virtual pos_type seekpos(pos_type pos, openmode) override
  {
    const largeobjectaccess::pos_type newpos = m_obj.cseek(
	largeobjectaccess::off_type(pos),
	std::ios::beg);
    return AdjustEOF(newpos);
  }

  virtual int_type overflow(int_type ch = EoF()) override
  {
    char *const pp = this->pptr();
    if (pp == nullptr) return EoF();
    char *const pb = this->pbase();
    int_type res = 0;

    if (pp > pb) res = int_type(AdjustEOF(m_obj.cwrite(pb, pp-pb)));
    this->setp(m_p, m_p + m_bufsize);

    // Write that one more character, if it's there.
    if (ch != EoF())
    {
      *this->pptr() = char(ch);
      this->pbump(1);
    }
    return res;
  }

  virtual int_type underflow() override
  {
    if (this->gptr() == nullptr) return EoF();
    char *const eb = this->eback();
    const int_type res(static_cast<int_type>(
	AdjustEOF(m_obj.cread(this->eback(), m_bufsize))));
    this->setg(eb, eb, eb + ((res==EoF()) ? 0 : res));
    return ((res == 0) or (res == EoF())) ? EoF() : *eb;
  }

private:
  /// Shortcut for traits_type::eof().
  static int_type EoF() { return traits_type::eof(); }

  /// Helper: change error position of -1 to EOF (probably a no-op).
  template<typename INTYPE>
  static std::streampos AdjustEOF(INTYPE pos)
	{ return (pos==-1) ? std::streampos(EoF()) : std::streampos(pos); }

  void initialize(openmode mode)
  {
    if (mode & std::ios::in)
    {
      m_g = new char_type[unsigned(m_bufsize)];
      this->setg(m_g, m_g, m_g);
    }
    if (mode & std::ios::out)
    {
      m_p = new char_type[unsigned(m_bufsize)];
      this->setp(m_p, m_p + m_bufsize);
    }
  }

  const size_type m_bufsize;
  largeobjectaccess m_obj;

  /// Get & put buffers.
  char_type *m_g, *m_p;
};


/// Input stream that gets its data from a large object.
/** Use this class exactly as you would any other istream to read data from a
 * large object.  All formatting and streaming operations of @c std::istream are
 * supported.  What you'll typically want to use, however, is the ilostream
 * alias (which defines a basic_ilostream for @c char).  This is similar to
 * how e.g. @c std::ifstream relates to @c std::basic_ifstream.
 *
 * Currently only works for <tt><char, std::char_traits<char>></tt>.
 */
template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class basic_ilostream :
    public std::basic_istream<CHAR, TRAITS>
{
  using super = std::basic_istream<CHAR, TRAITS>;

public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;

  /// Create a basic_ilostream
  /**
   * @param T Transaction in which this stream is to exist
   * @param O Large object to access
   * @param BufSize Size of buffer to use internally (optional)
   */
  basic_ilostream(							//[t57]
	dbtransaction &T,
        largeobject O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::in, BufSize}
	{ super::init(&m_buf); }

  /// Create a basic_ilostream
  /**
   * @param T Transaction in which this stream is to exist
   * @param O Identifier of a large object to access
   * @param BufSize Size of buffer to use internally (optional)
   */
  basic_ilostream(							//[t48]
	dbtransaction &T,
        oid O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::in, BufSize}
	{ super::init(&m_buf); }

private:
  largeobject_streambuf<CHAR,TRAITS> m_buf;
};

using ilostream = basic_ilostream<char>;


/// Output stream that writes data back to a large object
/** Use this class exactly as you would any other ostream to write data to a
 * large object.  All formatting and streaming operations of @c std::ostream are
 * supported.  What you'll typically want to use, however, is the olostream
 * alias (which defines a basic_olostream for @c char).  This is similar to
 * how e.g. @c std::ofstream is related to @c std::basic_ofstream.
 *
 * Currently only works for <tt><char, std::char_traits<char>></tt>.
 */
template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class basic_olostream :
    public std::basic_ostream<CHAR, TRAITS>
{
  using super = std::basic_ostream<CHAR, TRAITS>;
public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;

  /// Create a basic_olostream
  /**
   * @param T transaction in which this stream is to exist
   * @param O a large object to access
   * @param BufSize size of buffer to use internally (optional)
   */
  basic_olostream(							//[t48]
	dbtransaction &T,
        largeobject O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::out, BufSize}
	{ super::init(&m_buf); }

  /// Create a basic_olostream
  /**
   * @param T transaction in which this stream is to exist
   * @param O a large object to access
   * @param BufSize size of buffer to use internally (optional)
   */
  basic_olostream(							//[t57]
	dbtransaction &T,
	oid O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::out, BufSize}
	{ super::init(&m_buf); }

  ~basic_olostream()
  {
    try
    {
      m_buf.pubsync(); m_buf.pubsync();
    }
    catch (const std::exception &e)
    {
      m_buf.process_notice(e.what());
    }
  }

private:
  largeobject_streambuf<CHAR,TRAITS> m_buf;
};

using olostream = basic_olostream<char>;


/// Stream that reads and writes a large object
/** Use this class exactly as you would a std::iostream to read data from, or
 * write data to a large object.  All formatting and streaming operations of
 * @c std::iostream are supported.  What you'll typically want to use, however,
 * is the lostream alias (which defines a basic_lostream for @c char).  This
 * is similar to how e.g. @c std::fstream is related to @c std::basic_fstream.
 *
 * Currently only works for <tt><char, std::char_traits<char>></tt>.
 */
template<typename CHAR=char, typename TRAITS=std::char_traits<CHAR>>
  class basic_lostream :
    public std::basic_iostream<CHAR, TRAITS>
{
  using super = std::basic_iostream<CHAR, TRAITS>;

public:
  using char_type = CHAR;
  using traits_type = TRAITS;
  using int_type = typename traits_type::int_type;
  using pos_type = typename traits_type::pos_type;
  using off_type = typename traits_type::off_type;

  /// Create a basic_lostream
  /**
   * @param T Transaction in which this stream is to exist
   * @param O Large object to access
   * @param BufSize Size of buffer to use internally (optional)
   */
  basic_lostream(							//[t59]
	dbtransaction &T,
	largeobject O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::in | std::ios::out, BufSize}
	{ super::init(&m_buf); }

  /// Create a basic_lostream
  /**
   * @param T Transaction in which this stream is to exist
   * @param O Large object to access
   * @param BufSize Size of buffer to use internally (optional)
   */
  basic_lostream(							//[t59]
	dbtransaction &T,
	oid O,
	largeobject::size_type BufSize=512) :
    super{nullptr},
    m_buf{T, O, std::ios::in | std::ios::out, BufSize}
	{ super::init(&m_buf); }

  ~basic_lostream()
  {
    try
    {
      m_buf.pubsync(); m_buf.pubsync();
    }
    catch (const std::exception &e)
    {
      m_buf.process_notice(e.what());
    }
  }

private:
  largeobject_streambuf<CHAR,TRAITS> m_buf;
};

using lostream = basic_lostream<char>;

} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"

#endif
