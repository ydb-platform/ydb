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
/**@file util/HighsMatrixPic.h
 * @brief Class-independent utilities for HiGHS
 */
#ifndef UTIL_HIGHSMATRIXPIC_H_
#define UTIL_HIGHSMATRIXPIC_H_

#include <string>
#include <vector>

#include "HConfig.h"
#include "lp_data/HighsLp.h"
#include "lp_data/HighsOptions.h"

HighsStatus writeLpMatrixPicToFile(const HighsOptions& options,
                                   const std::string fileprefix,
                                   const HighsLp& lp);

HighsStatus writeMatrixPicToFile(const HighsOptions& options,
                                 const std::string fileprefix,
                                 const HighsInt numRow, const HighsInt numCol,
                                 const std::vector<HighsInt>& Astart,
                                 const std::vector<HighsInt>& Aindex);

HighsStatus writeRmatrixPicToFile(const HighsOptions& options,
                                  const std::string fileprefix,
                                  const HighsInt numRow, const HighsInt numCol,
                                  const std::vector<HighsInt>& ARstart,
                                  const std::vector<HighsInt>& ARindex);

#endif  // UTIL_HIGHSMATRIXPIC_H_
