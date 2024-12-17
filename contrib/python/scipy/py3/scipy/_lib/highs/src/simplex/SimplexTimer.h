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
/**@file simplex/SimplexTimer.h
 * @brief Indices of simplex iClocks
 */
#ifndef SIMPLEX_SIMPLEXTIMER_H_
#define SIMPLEX_SIMPLEXTIMER_H_

// Clocks for profiling the dual simplex solver
enum iClockSimplex {
  SimplexTotalClock = 0,     //!< Total time for simplex
  SimplexIzDseWtClock,       //!< Total time to initialise DSE weights
  SimplexDualPhase1Clock,    //!< Total time for dual simplex phase 1
  SimplexDualPhase2Clock,    //!< Total time for dual simplex phase 2
  SimplexPrimalPhase1Clock,  //!< Total time for primal simplex phase 1
  SimplexPrimalPhase2Clock,  //!< Total time for primal simplex phase 2
  Group1Clock,               //!< Group for SIP

  IterateClock,               //!< Top level timing of HDual::solvePhase1() and
                              //!< HDual::solvePhase2()
  IterateDualRebuildClock,    //!< Second level timing of dual rebuild()
  IteratePrimalRebuildClock,  //!< Second level timing of primal rebuild()
  IterateChuzrClock,          //!< Second level timing of CHUZR
  IterateChuzcClock,          //!< Second level timing of CHUZC
  IterateFtranClock,          //!< Second level timing of FTRAN
  IterateVerifyClock,         //!< Second level timing of numerical check
  IterateDualClock,           //!< Second level timing of dual update
  IteratePrimalClock,         //!< Second level timing of primal update
  IterateDevexIzClock,        //!< Second level timing of initialise Devex
  IteratePivotsClock,         //!< Second level timing of pivoting

  initialiseSimplexLpBasisAndFactorClock,  //!< initialise Simplex LP, its basis
                                           //!< and factor
  ScaleClock,                              //!< Scale
  CrashClock,                              //!< Crash
  BasisConditionClock,                     //!< Basis condition estimation
  matrixSetupClock,                        //!< HMatrix setup
  setNonbasicMoveClock,                    //!< set nonbasicMove
  allocateSimplexArraysClock,              //!< allocate simplex arrays
  initialiseSimplexCostBoundsClock,  //!< initialise simplex cost and bounds

  DseIzClock,        //!< DSE weight initialisation
  InvertClock,       //!< Invert in dual rebuild()
  PermWtClock,       //!< Permutation of SED weights each side of INVERT in dual
                     //!< rebuild()
  ComputeDualClock,  //!< Computation of dual values in dual rebuild()
  CorrectDualClock,  //!< Correction of dual values in dual rebuild()
  CollectPrIfsClock,   //!< Identification of primal infeasibilities in dual
                       //!< rebuild()
  ComputePrIfsClock,   //!< Computation of num/max/sum of primal infeasibilities
  ComputeDuIfsClock,   //!< Computation of num/max/sum of dual infeasibilities
  ComputePrimalClock,  //!< Computation of primal values in dual rebuild()
  ComputeDuObjClock,  //!< Computation of dual objective value in dual rebuild()
  ComputePrObjClock,  //!< Computation of primalal objective value in primal
                      //!< rebuild()
  ReportRebuildClock,          //!< Reporting of log line in dual rebuild()
  ChuzrDualClock,              //!< CHUZR - Dual
  Chuzr1Clock,                 //!< CHUZR - Primal stage 1
  Chuzr2Clock,                 //!< CHUZR - Primal stage 2
  ChuzcPrimalClock,            //!< CHUZC - Primal
  ChuzcHyperInitialiselClock,  //!< CHUZC - Hyper-sparse initialisation
  ChuzcHyperBasicFeasibilityChangeClock,  //!< CHUZC - Hyper-sparse after phase
                                          //!< 1 basic feasibility changes
  ChuzcHyperDualClock,  //!< CHUZC - Hyper-sparse after dual update
  ChuzcHyperClock,      //!< CHUZC - Hyper-sparse
  Chuzc0Clock,          //!< CHUZC - Dual stage 0
  PriceChuzc1Clock,     //!< PRICE + CHUZC - Dual stage 1: parallel
  Chuzc1Clock,          //!< CHUZC - Dual stage 1
  Chuzc2Clock,          //!< CHUZC - Dual stage 2
  Chuzc3Clock,          //!< CHUZC - Dual stage 3
  Chuzc4Clock,          //!< CHUZC - Dual stage 4

  Chuzc4a0Clock,  //!< CHUZC - Dual stage 4a0
  Chuzc4a1Clock,  //!< CHUZC - Dual stage 4a1
  Chuzc4bClock,   //!< CHUZC - Dual stage 4b
  Chuzc4cClock,   //!< CHUZC - Dual stage 4c
  Chuzc4dClock,   //!< CHUZC - Dual stage 4d
  Chuzc4eClock,   //!< CHUZC - Dual stage 4e

  Chuzc5Clock,   //!< CHUZC - Dual stage 5
  DevexWtClock,  //!< Calculation of Devex weight of entering variable
  BtranClock,    //!< BTRAN - row p of inverse
  BtranBasicFeasibilityChangeClock,       //!< BTRAN - primal simplex phase 1
  BtranFullClock,                         //!< BTRAN - full RHS
  PriceClock,                             //!< PRICE - row p of tableau
  PriceBasicFeasibilityChangeClock,       //!< PRICE - primal simplex phase 1
  PriceFullClock,                         //!< PRICE - full
  FtranClock,                             //!< FTRAN - pivotal column
  FtranDseClock,                          //!< FTRAN for DSE weights
  BtranPseClock,                          //!< BTRAN for PSE weights
  FtranMixParClock,                       //!< FTRAN for PAMI - parallel
  FtranMixFinalClock,                     //!< FTRAN for PAMI - final
  FtranBfrtClock,                         //!< FTRAN for BFRT
  UpdateRowClock,                         //!< Update of dual values
  UpdateDualClock,                        //!< Update of dual values
  UpdateDualBasicFeasibilityChangeClock,  //!< Update of dual values in primal
                                          //!< phase 1
  UpdatePrimalClock,                      //!< Update of primal values
  DevexIzClock,            //!< Initialisation of new Devex framework
  DevexUpdateWeightClock,  //!< Update Devex weights
  DseUpdateWeightClock,    //!< Update DSE weights
  UpdatePivotsClock,       //!< Update indices of basic and nonbasic after basis
                           //!< change
  UpdateFactorClock,       //!< Update the representation of \f$B^{-1}\f$
  UpdateMatrixClock,  //!< Update the row-wise copy of the constraint matrix for
                      //!< nonbasic columns
  UpdateRowEpClock,   //!< Update the tableau rows in PAMI

  SimplexNumClock  //!< Number of simplex clocks
};

class SimplexTimer {
 public:
  void initialiseSimplexClocks(HighsTimerClock& simplex_timer_clock) {
    HighsTimer* timer_pointer = simplex_timer_clock.timer_pointer_;
    std::vector<HighsInt>& clock = simplex_timer_clock.clock_;
    clock.resize(SimplexNumClock);
    clock[SimplexTotalClock] = timer_pointer->clock_def("Simplex total", "STT");
    clock[SimplexIzDseWtClock] = timer_pointer->clock_def("Iz DSE Wt", "IWT");
    clock[SimplexDualPhase1Clock] =
        timer_pointer->clock_def("Dual Phase 1", "DP1");
    clock[SimplexDualPhase2Clock] =
        timer_pointer->clock_def("Dual Phase 2", "DP2");
    clock[SimplexPrimalPhase1Clock] =
        timer_pointer->clock_def("Primal Phase 1", "PP1");
    clock[SimplexPrimalPhase2Clock] =
        timer_pointer->clock_def("Primal Phase 2", "PP2");
    clock[Group1Clock] = timer_pointer->clock_def("GROUP1", "GP1");
    clock[IterateClock] = timer_pointer->clock_def("ITERATE", "ITR");
    clock[IterateDualRebuildClock] =
        timer_pointer->clock_def("DUAL REBUILD", "DRB");
    clock[IteratePrimalRebuildClock] =
        timer_pointer->clock_def("PRIMAL REBUILD", "PRB");
    clock[IterateChuzrClock] = timer_pointer->clock_def("CHUZR", "CZR");
    clock[IterateChuzcClock] = timer_pointer->clock_def("CHUZC", "CZC");
    clock[IterateFtranClock] = timer_pointer->clock_def("FTRAN", "FTR");
    clock[IterateVerifyClock] = timer_pointer->clock_def("VERIFY", "VRF");
    clock[IterateDualClock] = timer_pointer->clock_def("DUAL", "UDU");
    clock[IteratePrimalClock] = timer_pointer->clock_def("PRIMAL", "UPR");
    clock[IterateDevexIzClock] = timer_pointer->clock_def("DEVEX_IZ", "DVI");
    clock[IteratePivotsClock] = timer_pointer->clock_def("PIVOTS", "PIV");
    clock[initialiseSimplexLpBasisAndFactorClock] =
        timer_pointer->clock_def("IZ_SIMPLEX_LP_DEF", "ISD");
    clock[allocateSimplexArraysClock] =
        timer_pointer->clock_def("ALLOC_SIMPLEX_ARRS", "ASA");
    clock[initialiseSimplexCostBoundsClock] =
        timer_pointer->clock_def("IZ_SIMPLEX_CO_BD", "ICB");
    clock[ScaleClock] = timer_pointer->clock_def("SCALE", "SCL");
    clock[CrashClock] = timer_pointer->clock_def("CRASH", "CSH");
    clock[BasisConditionClock] =
        timer_pointer->clock_def("BASIS_CONDITION", "CON");
    clock[matrixSetupClock] = timer_pointer->clock_def("MATRIX_SETUP", "FST");
    clock[setNonbasicMoveClock] =
        timer_pointer->clock_def("SET_NONBASICMOVE", "SNM");
    clock[DseIzClock] = timer_pointer->clock_def("DSE_IZ", "DEI");
    clock[InvertClock] = timer_pointer->clock_def("INVERT", "INV");
    clock[PermWtClock] = timer_pointer->clock_def("PERM_WT", "PWT");
    clock[ComputeDualClock] = timer_pointer->clock_def("COMPUTE_DUAL", "CPD");
    clock[CorrectDualClock] = timer_pointer->clock_def("CORRECT_DUAL", "CRD");
    clock[ComputePrimalClock] =
        timer_pointer->clock_def("COMPUTE_PRIMAL", "CPP");
    clock[CollectPrIfsClock] =
        timer_pointer->clock_def("COLLECT_PR_IFS", "IFS");
    clock[ComputePrIfsClock] =
        timer_pointer->clock_def("COMPUTE_PR_IFS", "PIF");
    clock[ComputeDuIfsClock] =
        timer_pointer->clock_def("COMPUTE_DU_IFS", "DIF");
    clock[ComputeDuObjClock] =
        timer_pointer->clock_def("COMPUTE_DU_OBJ", "DOB");
    clock[ComputePrObjClock] =
        timer_pointer->clock_def("COMPUTE_PR_OBJ", "POB");
    clock[ReportRebuildClock] =
        timer_pointer->clock_def("REPORT_REBUILD", "RPR");
    clock[ChuzrDualClock] = timer_pointer->clock_def("CHUZR_DUAL", "CRD");
    clock[Chuzr1Clock] = timer_pointer->clock_def("CHUZR1", "CR1");
    clock[Chuzr2Clock] = timer_pointer->clock_def("CHUZR2", "CR2");
    clock[ChuzcPrimalClock] = timer_pointer->clock_def("CHUZC_PRIMAL", "CCP");
    clock[ChuzcHyperInitialiselClock] =
        timer_pointer->clock_def("CHUZC_HYPER_IZ", "CHI");
    clock[ChuzcHyperBasicFeasibilityChangeClock] =
        timer_pointer->clock_def("CHUZC_HYPER_FEAS", "CHF");
    clock[ChuzcHyperDualClock] =
        timer_pointer->clock_def("CHUZC_HYPER_DUAL", "CHD");
    clock[ChuzcHyperClock] = timer_pointer->clock_def("CHUZC_HYPER", "CHC");
    clock[Chuzc0Clock] = timer_pointer->clock_def("CHUZC0", "CC0");
    clock[PriceChuzc1Clock] = timer_pointer->clock_def("PRICE_CHUZC1", "PC1");
    clock[Chuzc1Clock] = timer_pointer->clock_def("CHUZC1", "CC1");
    clock[Chuzc2Clock] = timer_pointer->clock_def("CHUZC2", "CC2");
    clock[Chuzc3Clock] = timer_pointer->clock_def("CHUZC3", "CC3");
    clock[Chuzc4Clock] = timer_pointer->clock_def("CHUZC4", "CC4");
    clock[Chuzc4a0Clock] = timer_pointer->clock_def("CHUZC4a0", "C40");
    clock[Chuzc4a1Clock] = timer_pointer->clock_def("CHUZC4a1", "C41");
    clock[Chuzc4bClock] = timer_pointer->clock_def("CHUZC4b", "C4b");
    clock[Chuzc4cClock] = timer_pointer->clock_def("CHUZC4c", "C4c");
    clock[Chuzc4dClock] = timer_pointer->clock_def("CHUZC4d", "C4d");
    clock[Chuzc4eClock] = timer_pointer->clock_def("CHUZC4e", "C4e");
    clock[Chuzc5Clock] = timer_pointer->clock_def("CHUZC5", "CC5");
    clock[DevexWtClock] = timer_pointer->clock_def("DEVEX_WT", "DWT");
    clock[BtranClock] = timer_pointer->clock_def("BTRAN", "REP");
    clock[BtranBasicFeasibilityChangeClock] =
        timer_pointer->clock_def("BTRAN_FEAS", "BT1");
    clock[BtranFullClock] = timer_pointer->clock_def("BTRAN_FULL", "BTF");
    clock[PriceClock] = timer_pointer->clock_def("PRICE", "RAP");
    clock[PriceBasicFeasibilityChangeClock] =
        timer_pointer->clock_def("PRICE_FEAS", "PC1");
    clock[PriceFullClock] = timer_pointer->clock_def("PRICE_FULL", "PCF");
    clock[FtranClock] = timer_pointer->clock_def("FTRAN", "COL");
    clock[FtranDseClock] = timer_pointer->clock_def("FTRAN_DSE", "DSE");
    clock[BtranPseClock] = timer_pointer->clock_def("BTRAN_PSE", "PSE");
    clock[FtranMixParClock] = timer_pointer->clock_def("FTRAN_MIX_PAR", "FMP");
    clock[FtranMixFinalClock] =
        timer_pointer->clock_def("FTRAN_MIX_FINAL", "FMF");
    clock[FtranBfrtClock] = timer_pointer->clock_def("FTRAN_BFRT", "BFR");
    clock[UpdateRowClock] = timer_pointer->clock_def("UPDATE_ROW", "UPR");
    clock[UpdateDualClock] = timer_pointer->clock_def("UPDATE_DUAL", "UPD");
    clock[UpdateDualBasicFeasibilityChangeClock] =
        timer_pointer->clock_def("UPDATE_DUAL_FEAS", "UD1");
    clock[UpdatePrimalClock] = timer_pointer->clock_def("UPDATE_PRIMAL", "UPP");
    clock[DevexIzClock] = timer_pointer->clock_def("DEVEX_IZ", "DIZ");
    clock[DevexUpdateWeightClock] =
        timer_pointer->clock_def("UPDATE_DVX_WEIGHT", "UDW");
    clock[DseUpdateWeightClock] =
        timer_pointer->clock_def("UPDATE_DSE_WEIGHT", "USW");
    clock[UpdatePivotsClock] = timer_pointer->clock_def("UPDATE_PIVOTS", "UPP");
    clock[UpdateFactorClock] = timer_pointer->clock_def("UPDATE_FACTOR", "UPF");
    clock[UpdateMatrixClock] = timer_pointer->clock_def("UPDATE_MATRIX", "UPM");
    clock[UpdateRowEpClock] = timer_pointer->clock_def("UPDATE_ROW_EP", "UPR");
  }

  bool reportSimplexClockList(const char* grepStamp,
                              const std::vector<HighsInt> simplex_clock_list,
                              const HighsTimerClock& simplex_timer_clock,
                              const double tolerance_percent_report_ = -1) {
    HighsTimer* timer_pointer = simplex_timer_clock.timer_pointer_;
    const std::vector<HighsInt>& clock = simplex_timer_clock.clock_;
    HighsInt simplex_clock_list_size = simplex_clock_list.size();
    std::vector<HighsInt> clockList;
    clockList.resize(simplex_clock_list_size);
    for (HighsInt en = 0; en < simplex_clock_list_size; en++) {
      clockList[en] = clock[simplex_clock_list[en]];
    }
    const double ideal_sum_time =
        timer_pointer->clock_time[clock[SimplexTotalClock]];
    const double tolerance_percent_report =
        tolerance_percent_report_ >= 0 ? tolerance_percent_report_ : 1e-8;
    return timer_pointer->reportOnTolerance(
        grepStamp, clockList, ideal_sum_time, tolerance_percent_report);
  };

  void reportChuzc4ClockList(const std::vector<HighsInt> simplex_clock_list,
                             const HighsTimerClock& simplex_timer_clock) {
    HighsTimer* timer_pointer = simplex_timer_clock.timer_pointer_;
    const std::vector<HighsInt>& clock = simplex_timer_clock.clock_;
    HighsInt simplex_clock_list_size = simplex_clock_list.size();
    std::vector<HighsInt> clockList;
    clockList.resize(simplex_clock_list_size);
    for (HighsInt en = 0; en < simplex_clock_list_size; en++) {
      clockList[en] = clock[simplex_clock_list[en]];
    }
    const double ideal_sum_time = timer_pointer->read(clock[Chuzc4Clock]);
    printf("reportChuzc4ClockList: ideal_sum_time = %g\n", ideal_sum_time);
    timer_pointer->reportOnTolerance("CHUZC4:", clockList, ideal_sum_time,
                                     1e-8);
  };

  void reportSimplexTotalClock(const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{SimplexTotalClock};
    reportSimplexClockList("SimplexTotal", simplex_clock_list,
                           simplex_timer_clock);
  };

  void reportSimplexPhasesClock(const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{
        SimplexIzDseWtClock, SimplexDualPhase1Clock, SimplexDualPhase2Clock,
        SimplexPrimalPhase2Clock};
    reportSimplexClockList("SimplexPhases", simplex_clock_list,
                           simplex_timer_clock);
  };

  void reportDualSimplexIterateClock(
      const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{IterateClock};
    reportSimplexClockList("SimplexIterate", simplex_clock_list,
                           simplex_timer_clock);
  };

  void reportDualSimplexOuterClock(const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{
        IterateDualRebuildClock, IterateChuzrClock,   IterateChuzcClock,
        IterateFtranClock,       IterateVerifyClock,  IterateDualClock,
        IteratePrimalClock,      IterateDevexIzClock, IteratePivotsClock};
    reportSimplexClockList("SimplexOuter", simplex_clock_list,
                           simplex_timer_clock);
  };

  bool reportSimplexInnerClock(const HighsTimerClock& simplex_timer_clock,
                               const double tolerance_percent_report_ = -1) {
    const std::vector<HighsInt> simplex_clock_list{
        initialiseSimplexLpBasisAndFactorClock,
        allocateSimplexArraysClock,
        initialiseSimplexCostBoundsClock,
        setNonbasicMoveClock,
        DevexIzClock,
        DseIzClock,
        ComputeDualClock,
        CorrectDualClock,
        ComputePrimalClock,
        CollectPrIfsClock,
        ComputePrIfsClock,
        ComputeDuIfsClock,
        ComputeDuObjClock,
        ComputePrObjClock,
        InvertClock,
        ReportRebuildClock,
        PermWtClock,
        ChuzcPrimalClock,
        ChuzcHyperInitialiselClock,
        ChuzcHyperBasicFeasibilityChangeClock,
        ChuzcHyperDualClock,
        ChuzcHyperClock,
        Chuzc0Clock,
        Chuzc1Clock,
        Chuzc2Clock,
        Chuzc3Clock,
        Chuzc4Clock,
        Chuzc5Clock,
        FtranClock,
        ChuzrDualClock,
        Chuzr1Clock,
        Chuzr2Clock,
        BtranClock,
        PriceClock,
        BtranBasicFeasibilityChangeClock,
        PriceBasicFeasibilityChangeClock,
        UpdateDualBasicFeasibilityChangeClock,
        FtranBfrtClock,
        FtranDseClock,
        BtranPseClock,
        BtranFullClock,
        PriceFullClock,
        DevexWtClock,
        DevexUpdateWeightClock,
        DseUpdateWeightClock,
        UpdatePrimalClock,
        UpdateDualClock,
        UpdatePivotsClock,
        UpdateFactorClock,
        UpdateMatrixClock};
    return reportSimplexClockList("SimplexInner", simplex_clock_list,
                                  simplex_timer_clock,
                                  tolerance_percent_report_);
  };

  void reportSimplexChuzc4Clock(const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{Chuzc4a0Clock, Chuzc4a1Clock,
                                                   Chuzc4bClock,  Chuzc4cClock,
                                                   Chuzc4dClock,  Chuzc4eClock};
    reportChuzc4ClockList(simplex_clock_list, simplex_timer_clock);
  };

  void reportSimplexMultiInnerClock(
      const HighsTimerClock& simplex_timer_clock) {
    const std::vector<HighsInt> simplex_clock_list{
        ScaleClock,
        CrashClock,
        BasisConditionClock,
        DseIzClock,
        InvertClock,
        PermWtClock,
        ComputeDualClock,
        CorrectDualClock,
        ComputePrimalClock,
        CollectPrIfsClock,
        ComputePrIfsClock,
        ComputeDuIfsClock,
        ComputeDuObjClock,
        ComputePrObjClock,
        ReportRebuildClock,
        ChuzrDualClock,
        Chuzr1Clock,
        Chuzr2Clock,
        BtranClock,
        BtranBasicFeasibilityChangeClock,
        BtranFullClock,
        PriceClock,
        PriceBasicFeasibilityChangeClock,
        PriceFullClock,
        ChuzcPrimalClock,
        ChuzcHyperInitialiselClock,
        ChuzcHyperClock,
        Chuzc0Clock,
        PriceChuzc1Clock,
        Chuzc1Clock,
        Chuzc2Clock,
        Chuzc3Clock,
        Chuzc4Clock,
        Chuzc5Clock,
        DevexWtClock,
        FtranClock,
        FtranBfrtClock,
        FtranDseClock,
        BtranPseClock,
        FtranMixParClock,
        FtranMixFinalClock,
        UpdateRowClock,
        UpdateDualClock,
        UpdateDualBasicFeasibilityChangeClock,
        UpdatePrimalClock,
        DevexUpdateWeightClock,
        DseUpdateWeightClock,
        DevexIzClock,
        UpdatePivotsClock,
        UpdateFactorClock,
        UpdateMatrixClock};
    reportSimplexClockList("SimplexMultiInner", simplex_clock_list,
                           simplex_timer_clock);
  };
};
#endif /* SIMPLEX_SIMPLEXTIMER_H_ */
