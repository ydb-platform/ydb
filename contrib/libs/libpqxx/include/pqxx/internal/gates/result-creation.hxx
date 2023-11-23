#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE result_creation : callgate<const result>
{
  friend class pqxx::connection_base;
  friend class pqxx::pipeline;

  result_creation(reference x) : super(x) {}

  static result create(
        internal::pq::PGresult *rhs,
        const std::string &query,
        encoding_group enc)
  {
    return result(rhs, query, enc);
  }

  void check_status() const { return home().check_status(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
