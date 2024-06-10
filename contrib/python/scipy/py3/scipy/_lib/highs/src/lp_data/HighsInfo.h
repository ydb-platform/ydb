/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2022 at the University of Edinburgh    */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/*    Authors: Julian Hall, Ivet Galabova, Leona Gottwald and Michael    */
/*    Feldmeier                                                          */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file lp_data/HighsInfo.h
 * @brief
 */
#ifndef LP_DATA_HIGHS_INFO_H_
#define LP_DATA_HIGHS_INFO_H_

#include <cstring>  // For strchr
#include <vector>

#include "lp_data/HConst.h"
#include "lp_data/HighsStatus.h"

class HighsOptions;

enum class InfoStatus { kOk = 0, kUnknownInfo, kIllegalValue, kUnavailable };

class InfoRecord {
 public:
  HighsInfoType type;
  std::string name;
  std::string description;
  bool advanced;

  InfoRecord(HighsInfoType Xtype, std::string Xname, std::string Xdescription,
             bool Xadvanced) {
    this->type = Xtype;
    this->name = Xname;
    this->description = Xdescription;
    this->advanced = Xadvanced;
  }

  virtual ~InfoRecord() {}
};

class InfoRecordInt64 : public InfoRecord {
 public:
  int64_t* value;
  int64_t default_value;
  InfoRecordInt64(std::string Xname, std::string Xdescription, bool Xadvanced,
                  int64_t* Xvalue_pointer, int64_t Xdefault_value)
      : InfoRecord(HighsInfoType::kInt64, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    default_value = Xdefault_value;
    *value = default_value;
  }

  virtual ~InfoRecordInt64() {}
};

class InfoRecordInt : public InfoRecord {
 public:
  HighsInt* value;
  HighsInt default_value;
  InfoRecordInt(std::string Xname, std::string Xdescription, bool Xadvanced,
                HighsInt* Xvalue_pointer, HighsInt Xdefault_value)
      : InfoRecord(HighsInfoType::kInt, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    default_value = Xdefault_value;
    *value = default_value;
  }

  virtual ~InfoRecordInt() {}
};

class InfoRecordDouble : public InfoRecord {
 public:
  double* value;
  double default_value;
  InfoRecordDouble(std::string Xname, std::string Xdescription, bool Xadvanced,
                   double* Xvalue_pointer, double Xdefault_value)
      : InfoRecord(HighsInfoType::kDouble, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    default_value = Xdefault_value;
    *value = default_value;
  }

  virtual ~InfoRecordDouble() {}
};

InfoStatus getInfoIndex(const HighsOptions& options, const std::string& name,
                        const std::vector<InfoRecord*>& info_records,
                        HighsInt& index);

InfoStatus checkInfo(const HighsOptions& options,
                     const std::vector<InfoRecord*>& info_records);
InfoStatus checkInfo(const InfoRecordInt& info);
InfoStatus checkInfo(const InfoRecordDouble& info);

InfoStatus getLocalInfoValue(const HighsOptions& options,
                             const std::string& name, const bool valid,
                             const std::vector<InfoRecord*>& info_records,
                             int64_t& value);
InfoStatus getLocalInfoValue(const HighsOptions& options,
                             const std::string& name, const bool valid,
                             const std::vector<InfoRecord*>& info_records,
                             HighsInt& value);
InfoStatus getLocalInfoValue(const HighsOptions& options,
                             const std::string& name, const bool valid,
                             const std::vector<InfoRecord*>& info_records,
                             double& value);

HighsStatus writeInfoToFile(FILE* file, const bool valid,
                            const std::vector<InfoRecord*>& info_records,
                            const bool html = false);
void reportInfo(FILE* file, const std::vector<InfoRecord*>& info_records,
                const bool html = false);
void reportInfo(FILE* file, const InfoRecordInt64& info,
                const bool html = false);
void reportInfo(FILE* file, const InfoRecordInt& info, const bool html = false);
void reportInfo(FILE* file, const InfoRecordDouble& info,
                const bool html = false);

// For now, but later change so HiGHS properties are string based so that new
// info (for debug and testing too) can be added easily. The info below
// are just what has been used to parse info from argv.
// todo: when creating the new info don't forget underscores for class
// variables but no underscores for struct
struct HighsInfoStruct {
  bool valid;
  int64_t mip_node_count;
  HighsInt simplex_iteration_count;
  HighsInt ipm_iteration_count;
  HighsInt qp_iteration_count;
  HighsInt crossover_iteration_count;
  HighsInt primal_solution_status;
  HighsInt dual_solution_status;
  HighsInt basis_validity;
  double objective_function_value;
  double mip_dual_bound;
  double mip_gap;
  double max_integrality_violation;
  HighsInt num_primal_infeasibilities;
  double max_primal_infeasibility;
  double sum_primal_infeasibilities;
  HighsInt num_dual_infeasibilities;
  double max_dual_infeasibility;
  double sum_dual_infeasibilities;
};

class HighsInfo : public HighsInfoStruct {
 public:
  HighsInfo() { initRecords(); }

  HighsInfo(const HighsInfo& info) {
    initRecords();
    HighsInfoStruct::operator=(info);
  }

  HighsInfo(HighsInfo&& info) {
    records = std::move(info.records);
    HighsInfoStruct::operator=(std::move(info));
  }

  const HighsInfo& operator=(const HighsInfo& other) {
    if (&other != this) {
      if ((HighsInt)records.size() == 0) initRecords();
      HighsInfoStruct::operator=(other);
    }
    return *this;
  }

  const HighsInfo& operator=(HighsInfo&& other) {
    if (&other != this) {
      if ((HighsInt)records.size() == 0) initRecords();
      HighsInfoStruct::operator=(other);
    }
    return *this;
  }

  virtual ~HighsInfo() {
    if (records.size() > 0) deleteRecords();
  }

  void invalidate();

 private:
  void deleteRecords() {
    for (HighsUInt i = 0; i < records.size(); i++) delete records[i];
  }

  void initRecords() {
    InfoRecordInt64* record_int64;
    InfoRecordInt* record_int;
    InfoRecordDouble* record_double;
    bool advanced;
    advanced = false;

    record_int = new InfoRecordInt("simplex_iteration_count",
                                   "Iteration count for simplex solver",
                                   advanced, &simplex_iteration_count, 0);
    records.push_back(record_int);

    record_int = new InfoRecordInt("ipm_iteration_count",
                                   "Iteration count for IPM solver", advanced,
                                   &ipm_iteration_count, 0);
    records.push_back(record_int);

    record_int =
        new InfoRecordInt("qp_iteration_count", "Iteration count for QP solver",
                          advanced, &qp_iteration_count, 0);
    records.push_back(record_int);

    record_int = new InfoRecordInt("crossover_iteration_count",
                                   "Iteration count for crossover", advanced,
                                   &crossover_iteration_count, 0);
    records.push_back(record_int);

    record_int =
        new InfoRecordInt("qp_iteration_count", "Iteration count for QP solver",
                          advanced, &qp_iteration_count, 0);
    records.push_back(record_int);

    record_int = new InfoRecordInt("primal_solution_status",
                                   "Model primal solution status: 0 => No "
                                   "solution; 1 => Infeasible point; "
                                   "2 => Feasible point",
                                   advanced, &primal_solution_status,
                                   kSolutionStatusNone);
    records.push_back(record_int);

    record_int =
        new InfoRecordInt("dual_solution_status",
                          "Model dual solution status: 0 => No solution; 1 => "
                          "Infeasible point; 2 "
                          "=> Feasible point",
                          advanced, &dual_solution_status, kSolutionStatusNone);
    records.push_back(record_int);

    record_int = new InfoRecordInt(
        "basis_validity", "Model basis validity: 0 => Invalid; 1 => Valid",
        advanced, &basis_validity, kBasisValidityInvalid);
    records.push_back(record_int);

    record_double = new InfoRecordDouble("objective_function_value",
                                         "Objective function value", advanced,
                                         &objective_function_value, 0);
    records.push_back(record_double);

    record_int64 =
        new InfoRecordInt64("mip_node_count", "MIP solver node count", advanced,
                            &mip_node_count, 0);
    records.push_back(record_int64);

    record_double =
        new InfoRecordDouble("mip_dual_bound", "MIP solver dual bound",
                             advanced, &mip_dual_bound, 0);
    records.push_back(record_double);

    record_double = new InfoRecordDouble("mip_gap", "MIP solver gap (%)",
                                         advanced, &mip_gap, 0);
    records.push_back(record_double);

    record_double = new InfoRecordDouble("max_integrality_violation",
                                         "Max integrality violation", advanced,
                                         &max_integrality_violation, 0);
    records.push_back(record_double);

    record_int = new InfoRecordInt("num_primal_infeasibilities",
                                   "Number of primal infeasibilities", advanced,
                                   &num_primal_infeasibilities, -1);
    records.push_back(record_int);

    record_double = new InfoRecordDouble(
        "max_primal_infeasibility", "Maximum primal infeasibility", advanced,
        &max_primal_infeasibility, 0);
    records.push_back(record_double);

    record_double = new InfoRecordDouble(
        "sum_primal_infeasibilities", "Sum of primal infeasibilities", advanced,
        &sum_primal_infeasibilities, 0);
    records.push_back(record_double);

    record_int = new InfoRecordInt("num_dual_infeasibilities",
                                   "Number of dual infeasibilities", advanced,
                                   &num_dual_infeasibilities, -1);
    records.push_back(record_int);

    record_double = new InfoRecordDouble("max_dual_infeasibility",
                                         "Maximum dual infeasibility", advanced,
                                         &max_dual_infeasibility, 0);
    records.push_back(record_double);

    record_double = new InfoRecordDouble(
        "sum_dual_infeasibilities", "Sum of dual infeasibilities", advanced,
        &sum_dual_infeasibilities, 0);
    records.push_back(record_double);
  }

 public:
  std::vector<InfoRecord*> records;
};

#endif
