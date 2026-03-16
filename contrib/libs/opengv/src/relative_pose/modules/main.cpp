/******************************************************************************
 * Author:   Laurent Kneip                                                    *
 * Contact:  kneip.laurent@gmail.com                                          *
 * License:  Copyright (c) 2013 Laurent Kneip, ANU. All rights reserved.      *
 *                                                                            *
 * Redistribution and use in source and binary forms, with or without         *
 * modification, are permitted provided that the following conditions         *
 * are met:                                                                   *
 * * Redistributions of source code must retain the above copyright           *
 *   notice, this list of conditions and the following disclaimer.            *
 * * Redistributions in binary form must reproduce the above copyright        *
 *   notice, this list of conditions and the following disclaimer in the      *
 *   documentation and/or other materials provided with the distribution.     *
 * * Neither the name of ANU nor the names of its contributors may be         *
 *   used to endorse or promote products derived from this software without   *
 *   specific prior written permission.                                       *
 *                                                                            *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"*
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  *
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE *
 * ARE DISCLAIMED. IN NO EVENT SHALL ANU OR THE CONTRIBUTORS BE LIABLE        *
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL *
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR *
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER *
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT         *
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY  *
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF     *
 * SUCH DAMAGE.                                                               *
 ******************************************************************************/


#include <math.h>
#include <vector>
#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

#include <opengv/relative_pose/modules/main.hpp>
#include <opengv/relative_pose/modules/fivept_nister/modules.hpp>
#include <opengv/relative_pose/modules/fivept_stewenius/modules.hpp>
#include <opengv/relative_pose/modules/fivept_kneip/modules.hpp>
#include <opengv/relative_pose/modules/eigensolver/modules.hpp>
#include <opengv/relative_pose/modules/sixpt/modules.hpp>
#include <opengv/relative_pose/modules/ge/modules.hpp>
#include <opengv/OptimizationFunctor.hpp>
#include <opengv/math/arun.hpp>
#include <opengv/math/cayley.hpp>
#include <opengv/math/Sturm.hpp>

#include <stdio.h>
#include <iostream>

void
opengv::relative_pose::modules::fivept_stewenius_main(
    const Eigen::Matrix<double,9,4> & EE,
    complexEssentials_t & complexEssentials )
{
  Eigen::Matrix<double,10,20> A;
  fivept_stewenius::composeA(EE,A);

  Eigen::Matrix<double,10,10> A1 = A.block(0,0,10,10);
  Eigen::Matrix<double,10,10> A2 = A.block(0,10,10,10);

  Eigen::FullPivLU< Eigen::Matrix<double,10,10> > luA1(A1);
  Eigen::Matrix<double,10,10> A3 = luA1.inverse()*A2;

  Eigen::Matrix<double,10,10> M;
  M.block(0,0,3,10) = -A3.block(0,0,3,10);
  M.block(3,0,2,10) = -A3.block(4,0,2,10);
  M.row(5) = -A3.row(7);
  M.block(6,0,4,10) = Eigen::Matrix<double,4,10>::Zero();
  M(6,0) = 1.0;
  M(7,1) = 1.0;
  M(8,3) = 1.0;
  M(9,6) = 1.0;

  Eigen::EigenSolver< Eigen::Matrix<double,10,10> > Eig(M,true);

  //Eigen::Matrix<std::complex<double>,10,1> D = Eig.eigenvalues();
  Eigen::Matrix<std::complex<double>,10,10> V = Eig.eigenvectors();

  Eigen::Matrix<std::complex<double>,3,10> V1;
  V1 = V.block(6,0,3,10);

  Eigen::Matrix<std::complex<double>,3,10> V2;
  V2.row(0) = V.row(9);
  V2.row(1) = V.row(9);
  V2.row(2) = V.row(9);

  Eigen::Matrix<std::complex<double>,4,10> SOLS;

  for( int r = 0; r < 3; r++ )
  {
    for( int c = 0; c < 10; c++ )
      SOLS(r,c) = V1(r,c)/V2(r,c);
  }

  SOLS.row(3) = Eigen::Matrix<std::complex<double>,1,10>::
      Constant(std::complex<double>(1.0,0.0));

  Eigen::Matrix<std::complex<double>,9,10> Evec = EE * SOLS;

  Eigen::Matrix<std::complex<double>,1,10> norms;
  for( int c = 0; c < 10; c++ )
  {
    norms(0,c) = std::complex<double>(0.0,0.0);

    for( int r = 0; r < 9; r++ )
      norms(0,c) += pow( Evec(r,c), 2 );

    norms(0,c) = sqrt(norms(0,c));
  }

  Eigen::Matrix<std::complex<double>,9,10> EvecNorms;
  for( int i = 0; i < 9; i++ )
    EvecNorms.row(i) = norms;

  for( int r = 0; r < 9; r++ )
  {
    for( int c = 0; c < 10; c++ )
      Evec(r,c) = Evec(r,c) / EvecNorms(r,c);
  }

  for( int c = 0; c < 10; c++ )
  {
    complexEssential_t complexEssential;

    complexEssential.row(0) = Evec.block(0,c,3,1).transpose();
    complexEssential.row(1) = Evec.block(3,c,3,1).transpose();
    complexEssential.row(2) = Evec.block(6,c,3,1).transpose();

    complexEssentials.push_back(complexEssential);
  }
}

void
opengv::relative_pose::modules::fivept_nister_main(
    const Eigen::Matrix<double,9,4> & EE,
    essentials_t & essentials )
{
  Eigen::Matrix<double,10,20> A;
  fivept_nister::composeA(EE,A);

  Eigen::Matrix<double,10,10> A1 = A.block(0,0,10,10);
  Eigen::Matrix<double,10,10> A2 = A.block(0,10,10,10);

  Eigen::FullPivLU< Eigen::Matrix<double,10,10> > luA1(A1);
  Eigen::Matrix<double,10,10> A3 = luA1.inverse()*A2;

  Eigen::Matrix<double,1,4> b11_part1 = Eigen::Matrix<double,1,4>::Zero();
  b11_part1.block<1,3>(0,1) = A3.block<1,3>(4,0);
  Eigen::Matrix<double,1,4> b11_part2 = Eigen::Matrix<double,1,4>::Zero();
  b11_part2.block<1,3>(0,0) = A3.block<1,3>(5,0);
  Eigen::Matrix<double,1,4> b11 = b11_part1 - b11_part2;

  Eigen::Matrix<double,1,4> b21_part1 = Eigen::Matrix<double,1,4>::Zero();
  b21_part1.block<1,3>(0,1) = A3.block<1,3>(6,0);
  Eigen::Matrix<double,1,4> b21_part2 = Eigen::Matrix<double,1,4>::Zero();
  b21_part2.block<1,3>(0,0) = A3.block<1,3>(7,0);
  Eigen::Matrix<double,1,4> b21 = b21_part1 - b21_part2;

  Eigen::Matrix<double,1,4> b31_part1 = Eigen::Matrix<double,1,4>::Zero();
  b31_part1.block<1,3>(0,1) = A3.block<1,3>(8,0);
  Eigen::Matrix<double,1,4> b31_part2 = Eigen::Matrix<double,1,4>::Zero();
  b31_part2.block<1,3>(0,0) = A3.block<1,3>(9,0);
  Eigen::Matrix<double,1,4> b31 = b31_part1 - b31_part2;

  Eigen::Matrix<double,1,4> b12_part1 = Eigen::Matrix<double,1,4>::Zero();
  b12_part1.block<1,3>(0,1) = A3.block<1,3>(4,3);
  Eigen::Matrix<double,1,4> b12_part2 = Eigen::Matrix<double,1,4>::Zero();
  b12_part2.block<1,3>(0,0) = A3.block<1,3>(5,3);
  Eigen::Matrix<double,1,4> b12 = b12_part1 - b12_part2;

  Eigen::Matrix<double,1,4> b22_part1 = Eigen::Matrix<double,1,4>::Zero();
  b22_part1.block<1,3>(0,1) = A3.block<1,3>(6,3);
  Eigen::Matrix<double,1,4> b22_part2 = Eigen::Matrix<double,1,4>::Zero();
  b22_part2.block<1,3>(0,0) = A3.block<1,3>(7,3);
  Eigen::Matrix<double,1,4> b22 = b22_part1 - b22_part2;

  Eigen::Matrix<double,1,4> b32_part1 = Eigen::Matrix<double,1,4>::Zero();
  b32_part1.block<1,3>(0,1) = A3.block<1,3>(8,3);
  Eigen::Matrix<double,1,4> b32_part2 = Eigen::Matrix<double,1,4>::Zero();
  b32_part2.block<1,3>(0,0) = A3.block<1,3>(9,3);
  Eigen::Matrix<double,1,4> b32 = b32_part1 - b32_part2;

  Eigen::Matrix<double,1,5> b13_part1 = Eigen::Matrix<double,1,5>::Zero();
  b13_part1.block<1,4>(0,1) = A3.block<1,4>(4,6);
  Eigen::Matrix<double,1,5> b13_part2 = Eigen::Matrix<double,1,5>::Zero();
  b13_part2.block<1,4>(0,0) = A3.block<1,4>(5,6);
  Eigen::Matrix<double,1,5> b13 = b13_part1 - b13_part2;

  Eigen::Matrix<double,1,5> b23_part1 = Eigen::Matrix<double,1,5>::Zero();
  b23_part1.block<1,4>(0,1) = A3.block<1,4>(6,6);
  Eigen::Matrix<double,1,5> b23_part2 = Eigen::Matrix<double,1,5>::Zero();
  b23_part2.block<1,4>(0,0) = A3.block<1,4>(7,6);
  Eigen::Matrix<double,1,5> b23 = b23_part1 - b23_part2;

  Eigen::Matrix<double,1,5> b33_part1 = Eigen::Matrix<double,1,5>::Zero();
  b33_part1.block<1,4>(0,1) = A3.block<1,4>(8,6);
  Eigen::Matrix<double,1,5> b33_part2 = Eigen::Matrix<double,1,5>::Zero();
  b33_part2.block<1,4>(0,0) = A3.block<1,4>(9,6);
  Eigen::Matrix<double,1,5> b33 = b33_part1 - b33_part2;

  Eigen::Matrix<double,1,8> p1_part1;
  fivept_nister::computeSeventhOrderPolynomial(b23,b12,p1_part1);
  Eigen::Matrix<double,1,8> p1_part2;
  fivept_nister::computeSeventhOrderPolynomial(b13,b22,p1_part2);
  Eigen::Matrix<double,1,8> p1 = p1_part1 - p1_part2;
  Eigen::Matrix<double,1,8> p2_part1;
  fivept_nister::computeSeventhOrderPolynomial(b13,b21,p2_part1);
  Eigen::Matrix<double,1,8> p2_part2;
  fivept_nister::computeSeventhOrderPolynomial(b23,b11,p2_part2);
  Eigen::Matrix<double,1,8> p2 = p2_part1 - p2_part2;
  Eigen::Matrix<double,1,7> p3_part1;
  fivept_nister::computeSixthOrderPolynomial(b11,b22,p3_part1);
  Eigen::Matrix<double,1,7> p3_part2;
  fivept_nister::computeSixthOrderPolynomial(b12,b21,p3_part2);
  Eigen::Matrix<double,1,7> p3 = p3_part1 - p3_part2;

  Eigen::Matrix<double,1,11> p_order10_part1;
  fivept_nister::computeTenthOrderPolynomialFrom73(p1,b31,p_order10_part1);
  Eigen::Matrix<double,1,11> p_order10_part2;
  fivept_nister::computeTenthOrderPolynomialFrom73(p2,b32,p_order10_part2);
  Eigen::Matrix<double,1,11> p_order10_part3;
  fivept_nister::computeTenthOrderPolynomialFrom64(p3,b33,p_order10_part3);
  Eigen::Matrix<double,1,11> p_order10 =
      p_order10_part1 + p_order10_part2 + p_order10_part3;

  math::Sturm sturmSequence(p_order10);
  std::vector<double> roots = sturmSequence.findRoots();

  Eigen::MatrixXd Evec(9,roots.size());

  for( size_t i = 0; i < roots.size(); i++ )
  {
    double z = roots[i];
    double x = fivept_nister::polyVal(p1,z)/fivept_nister::polyVal(p3,z);
    double y = fivept_nister::polyVal(p2,z)/fivept_nister::polyVal(p3,z);

    //pollishing here
    fivept_nister::pollishCoefficients(A,x,y,z);

    Evec.col(i) = x*EE.col(0) + y*EE.col(1) + z*EE.col(2) + EE.col(3);
  }

  Eigen::MatrixXd norms(1,roots.size());
  for( size_t c = 0; c < roots.size(); c++ )
  {
    norms(0,c) = 0.0;

    for( int r = 0; r < 9; r++ )
      norms(0,c) += pow( Evec(r,c), 2 );

    norms(0,c) = sqrt(norms(0,c));
  }

  Eigen::MatrixXd EvecNorms(9,roots.size());
  for( size_t i = 0; i < 9; i++ )
    EvecNorms.row(i) = norms;

  for( size_t r = 0; r < 9; r++ )
  {
    for( size_t c = 0; c < roots.size(); c++ )
      Evec(r,c) = Evec(r,c) / EvecNorms(r,c);
  }

  for( size_t c = 0; c < roots.size(); c++ )
  {
    essential_t essential;

    essential.row(0) = Evec.block<3,1>(0,c).transpose();
    essential.row(1) = Evec.block<3,1>(3,c).transpose();
    essential.row(2) = Evec.block<3,1>(6,c).transpose();

    essentials.push_back(essential);
  }
}

void
opengv::relative_pose::modules::fivept_kneip_main(
    const Eigen::Matrix<double,3,5> & f1,
    const Eigen::Matrix<double,3,5> & f2,
    rotations_t & rotations )
{
  Eigen::Matrix<double,66,197> groebnerMatrix =
      Eigen::Matrix<double,66,197>::Zero();
  Eigen::Matrix3d temp1 = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d temp2 = Eigen::Matrix3d::Zero();
  std::vector<Eigen::Matrix3d, Eigen::aligned_allocator<Eigen::Matrix3d> > c_1;
  c_1.push_back(temp1);
  c_1.push_back(temp1);
  c_1.push_back(temp1);
  std::vector<Eigen::Matrix3d, Eigen::aligned_allocator<Eigen::Matrix3d> > c_2;
  c_2.push_back(temp2);
  c_2.push_back(temp2);
  c_2.push_back(temp2);

  int currentRow = 0;

  for( int firstFeat = 0; firstFeat < 5; firstFeat++ )
  {
    for( int secondFeat = firstFeat + 1; secondFeat < 5; secondFeat++ )
    {
      temp1 = f1.col(firstFeat)*f1.col(secondFeat).transpose();
      temp2 = f2.col(firstFeat)*f2.col(secondFeat).transpose();

      for( int thirdFeat = secondFeat + 1; thirdFeat < 5; thirdFeat++ )
      {
        c_1[0] = temp1 * f1(0,thirdFeat);
        c_1[1] = temp1 * f1(1,thirdFeat);
        c_1[2] = temp1 * f1(2,thirdFeat);
        c_2[0] = temp2 * f2(0,thirdFeat);
        c_2[1] = temp2 * f2(1,thirdFeat);
        c_2[2] = temp2 * f2(2,thirdFeat);

        groebnerMatrix.row(currentRow++) =
            fivept_kneip::initEpncpRowR( c_1, c_2 );
      }
    }
  }

  fivept_kneip::initMatrix(groebnerMatrix);
  fivept_kneip::computeBasis(groebnerMatrix);

  Eigen::Matrix<double,20,20> M = Eigen::Matrix<double,20,20>::Zero();
  M.block<10,20>(0,0) = -groebnerMatrix.block<10,20>(51,177);
  M(10,1) = 1.0;
  M(11,2) = 1.0;
  M(12,3) = 1.0;
  M(13,4) = 1.0;
  M(14,5) = 1.0;
  M(15,6) = 1.0;
  M(16,7) = 1.0;
  M(17,8) = 1.0;
  M(18,9) = 1.0;
  M(19,18) = 1.0;

  Eigen::EigenSolver< Eigen::Matrix<double,20,20> > Eig(M,true);
  Eigen::Matrix<std::complex<double>,20,1> D = Eig.eigenvalues();
  Eigen::Matrix<std::complex<double>,20,20> V = Eig.eigenvectors();

  //Eigen::Matrix<std::complex<double>,9,1> tempVector;
  rotation_t finalRotation = Eigen::Matrix3d::Zero();
  for( unsigned int i = 0; i < 20; i++ )
  {
    std::complex<double> tempp;
    tempp = D[i];

    //check if we have a real solution
    if( fabs(tempp.imag()) < 0.1 )
    {
      tempp = V(18,i)/V(19,i);
      finalRotation(0,0) = tempp.real();// tempVector[0] = tempp;
      tempp = V(17,i)/V(19,i);
      finalRotation(0,1) = tempp.real();// tempVector[1] = tempp;
      tempp = V(16,i)/V(19,i);
      finalRotation(0,2) = tempp.real();// tempVector[2] = tempp;
      tempp = V(15,i)/V(19,i);
      finalRotation(1,0) = tempp.real();// tempVector[3] = tempp;
      tempp = V(14,i)/V(19,i);
      finalRotation(1,1) = tempp.real();// tempVector[4] = tempp;
      tempp = V(13,i)/V(19,i);
      finalRotation(1,2) = tempp.real();// tempVector[5] = tempp;
      tempp = V(12,i)/V(19,i);
      finalRotation(2,0) = tempp.real();// tempVector[6] = tempp;
      tempp = V(11,i)/V(19,i);
      finalRotation(2,1) = tempp.real();// tempVector[7] = tempp;
      tempp = V(10,i)/V(19,i);
      finalRotation(2,2) = tempp.real();// tempVector[8] = tempp;

      double tempNorm = finalRotation.row(1).norm();
      finalRotation.row(1) = finalRotation.row(1) / tempNorm;
      tempNorm = finalRotation.row(2).norm();
      finalRotation.row(2) = finalRotation.row(2) / tempNorm;

      //check if the normalized rotation matrix has determinant close enough to 1
      if( fabs( finalRotation.determinant() - 1.0 ) < 0.1 )
      {
        Eigen::Matrix3d eval;
        double totalEval = 0.0;
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(2) = f1.col(2).cross(finalRotation*f2.col(2));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(2) = f1.col(3).cross(finalRotation*f2.col(3));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(2).cross(finalRotation*f2.col(2));
        eval.col(2) = f1.col(3).cross(finalRotation*f2.col(3));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(2).cross(finalRotation*f2.col(2));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(0).cross(finalRotation*f2.col(0));
        eval.col(1) = f1.col(3).cross(finalRotation*f2.col(3));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(1) = f1.col(2).cross(finalRotation*f2.col(2));
        eval.col(2) = f1.col(3).cross(finalRotation*f2.col(3));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(1) = f1.col(2).cross(finalRotation*f2.col(2));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(1).cross(finalRotation*f2.col(1));
        eval.col(1) = f1.col(3).cross(finalRotation*f2.col(3));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        eval.col(0) = f1.col(2).cross(finalRotation*f2.col(2));
        eval.col(1) = f1.col(3).cross(finalRotation*f2.col(3));
        eval.col(2) = f1.col(4).cross(finalRotation*f2.col(4));
        totalEval += fabs(eval.determinant());
        totalEval += fabs(
            finalRotation(0,0)*finalRotation(0,0)+
            finalRotation(0,1)*finalRotation(0,1)+
            finalRotation(0,2)*finalRotation(0,2)-1);
        totalEval += fabs(
            finalRotation(0,0)*finalRotation(1,0)+
            finalRotation(0,1)*finalRotation(1,1)+
            finalRotation(0,2)*finalRotation(1,2));
        totalEval += fabs(
            finalRotation(0,0)*finalRotation(2,0)+
            finalRotation(0,1)*finalRotation(2,1)+
            finalRotation(0,2)*finalRotation(2,2));
        totalEval += fabs(
            finalRotation(1,0)*finalRotation(1,0)+
            finalRotation(1,1)*finalRotation(1,1)+
            finalRotation(1,2)*finalRotation(1,2)-1);
        totalEval += fabs(
            finalRotation(1,0)*finalRotation(2,0)+
            finalRotation(1,1)*finalRotation(2,1)+
            finalRotation(1,2)*finalRotation(2,2));
        totalEval += fabs(
            finalRotation(2,0)*finalRotation(2,0)+
            finalRotation(2,1)*finalRotation(2,1)+
            finalRotation(2,2)*finalRotation(2,2)-1);
        totalEval += fabs(
            finalRotation(1,0)*finalRotation(2,1)-
            finalRotation(2,0)*finalRotation(1,1)-finalRotation(0,2));
        totalEval += fabs(
            finalRotation(2,0)*finalRotation(0,1)-
            finalRotation(0,0)*finalRotation(2,1)-finalRotation(1,2));
        totalEval += fabs(
            finalRotation(0,0)*finalRotation(1,1)-
            finalRotation(1,0)*finalRotation(0,1)-finalRotation(2,2));

        //check if the initial constraints are fullfilled to a sufficient extend
        if( totalEval < 0.001 )
        {
          Eigen::Matrix<double,3,5> normalVectors;
          normalVectors.col(0) = f1.col(0).cross(finalRotation * f2.col(0));
          normalVectors.col(1) = f1.col(1).cross(finalRotation * f2.col(1));
          normalVectors.col(2) = f1.col(2).cross(finalRotation * f2.col(2));
          normalVectors.col(3) = f1.col(3).cross(finalRotation * f2.col(3));
          normalVectors.col(4) = f1.col(4).cross(finalRotation * f2.col(4));

          Eigen::Vector3d trans01 =
              normalVectors.col(0).cross(normalVectors.col(1));
          Eigen::Vector3d trans02 =
              normalVectors.col(0).cross(normalVectors.col(2));
          Eigen::Vector3d trans03 =
              normalVectors.col(0).cross(normalVectors.col(3));
          Eigen::Vector3d trans04 =
              normalVectors.col(0).cross(normalVectors.col(4));
          Eigen::Vector3d trans12 =
              normalVectors.col(1).cross(normalVectors.col(2));
          Eigen::Vector3d trans13 =
              normalVectors.col(1).cross(normalVectors.col(3));
          Eigen::Vector3d trans14 =
              normalVectors.col(1).cross(normalVectors.col(4));
          Eigen::Vector3d trans23 =
              normalVectors.col(2).cross(normalVectors.col(3));
          Eigen::Vector3d trans24 =
              normalVectors.col(2).cross(normalVectors.col(4));
          Eigen::Vector3d trans34 =
              normalVectors.col(3).cross(normalVectors.col(4));

          Eigen::Vector3d tempVector1;
          Eigen::Vector3d tempVector2;

          bool positive = true;

          for( int i = 0; i < 5; i++ )
          {
            tempVector1 = trans01.cross( f1.col(i) );
            tempVector2 = trans01.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans02.cross( f1.col(i) );
            tempVector2 = trans02.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans03.cross( f1.col(i) );
            tempVector2 = trans03.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans04.cross( f1.col(i) );
            tempVector2 = trans04.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans12.cross( f1.col(i) );
            tempVector2 = trans12.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans13.cross( f1.col(i) );
            tempVector2 = trans13.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans14.cross( f1.col(i) );
            tempVector2 = trans14.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans23.cross( f1.col(i) );
            tempVector2 = trans23.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans24.cross( f1.col(i) );
            tempVector2 = trans24.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
            tempVector1 = trans34.cross( f1.col(i) );
            tempVector2 = trans34.cross( finalRotation * f2.col(i) );
            if( tempVector1.dot(tempVector2) < 0 )
            {
              positive = false;
              break;
            }
          }

          //finally, check if the cheiriality constraint is fullfilled to
          //sufficient extend
          if( positive )
            rotations.push_back(finalRotation);
        }
      }
    }
  }
}

namespace opengv
{
namespace relative_pose
{
namespace modules
{

struct Eigensolver_step : OptimizationFunctor<double>
{
  const Eigen::Matrix3d & _xxF;
  const Eigen::Matrix3d & _yyF;
  const Eigen::Matrix3d & _zzF;
  const Eigen::Matrix3d & _xyF;
  const Eigen::Matrix3d & _yzF;
  const Eigen::Matrix3d & _zxF;

  Eigensolver_step(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF ) :
    OptimizationFunctor<double>(3,3),
    _xxF(xxF),_yyF(yyF),_zzF(zzF),_xyF(xyF),_yzF(yzF),_zxF(zxF) {}

  int operator()(const Eigen::VectorXd &x, Eigen::VectorXd &fvec) const
  {
    cayley_t cayley = x;
    Eigen::Matrix<double,1,3> jacobian;
    eigensolver::getSmallestEVwithJacobian(
        _xxF,_yyF,_zzF,_xyF,_yzF,_zxF,cayley,jacobian);

    fvec[0] = jacobian(0,0);
    fvec[1] = jacobian(0,1);
    fvec[2] = jacobian(0,2);
    return 0;
  }
};

}
}
}

void
opengv::relative_pose::modules::eigensolver_main(
  const Eigen::Matrix3d & xxF,
  const Eigen::Matrix3d & yyF,
  const Eigen::Matrix3d & zzF,
  const Eigen::Matrix3d & xyF,
  const Eigen::Matrix3d & yzF,
  const Eigen::Matrix3d & zxF,
  eigensolverOutput_t & output )
{
  const int n=3;
  VectorXd x(n);

  x = math::rot2cayley(output.rotation);
  Eigensolver_step functor(xxF,yyF,zzF,xyF,yzF,zxF );
  NumericalDiff<Eigensolver_step> numDiff(functor);
  LevenbergMarquardt< NumericalDiff<Eigensolver_step> > lm(numDiff);

  lm.resetParameters();
  lm.parameters.ftol = 0.00005;
  lm.parameters.xtol = 1.E1*NumTraits<double>::epsilon();
  lm.parameters.maxfev = 100;
  lm.minimize(x);

  cayley_t cayley = x;
  rotation_t R = math::cayley2rot(cayley);

  Eigen::Matrix3d M = eigensolver::composeM(xxF,yyF,zzF,xyF,yzF,zxF,cayley);
  Eigen::EigenSolver< Eigen::Matrix3d > Eig(M,true);
  Eigen::Matrix<std::complex<double>,3,1> D_complex = Eig.eigenvalues();
  Eigen::Matrix<std::complex<double>,3,3> V_complex = Eig.eigenvectors();
  eigenvalues_t D;
  eigenvectors_t V;
  for(size_t i = 0; i < 3; i++)
  {
    D[i] = D_complex[i].real();
    for(size_t j = 0; j < 3; j++)
      V(i,j) = V_complex(i,j).real();
  }

  double translationMagnitude = sqrt(pow(D[1],2) + pow(D[2],2));
  translation_t t = translationMagnitude * V.col(0);

  output.translation = t;
  output.rotation = R;
  output.eigenvalues = D;
  output.eigenvectors = V;

}

void
opengv::relative_pose::modules::sixpt_main(
  Eigen::Matrix<double,6,6> & L1,
  Eigen::Matrix<double,6,6> & L2,
  rotations_t & solutions)
{
  
  //create vectors of Pluecker coordinates
  std::vector< Eigen::Matrix<double,6,1>, Eigen::aligned_allocator<Eigen::Matrix<double,6,1> > > L1vec;
  std::vector< Eigen::Matrix<double,6,1>, Eigen::aligned_allocator<Eigen::Matrix<double,6,1> > > L2vec;
  for(int i = 0; i < 6; i++ )
  {
    L1vec.push_back( L1.col(i) );
    L2vec.push_back( L2.col(i) );
  }
  
  //setup the action matrix
  Eigen::Matrix<double,64,64> Action = Eigen::Matrix<double,64,64>::Zero();
  sixpt::setupAction( L1vec, L2vec, Action );
  
  //finally eigen-decompose the action-matrix and obtain the solutions
  Eigen::EigenSolver< Eigen::Matrix<double,64,64> > Eig(Action,true);
  Eigen::Matrix<std::complex<double>,64,64> EV = Eig.eigenvectors();
  
  solutions.reserve(64);  
  for( int c = 0; c < 64; c++ )
  {
    cayley_t solution;
    for( int r = 0; r < 3; r++ )
    {
      std::complex<double> temp = EV(60+r,c)/EV(63,c);
      solution[r] = temp.real();
    }
    
    solutions.push_back(math::cayley2rot(solution).transpose());
  }
}

namespace opengv
{
namespace relative_pose
{
namespace modules
{

struct Ge_step : OptimizationFunctor<double>
{
  const Eigen::Matrix3d & _xxF;
  const Eigen::Matrix3d & _yyF;
  const Eigen::Matrix3d & _zzF;
  const Eigen::Matrix3d & _xyF;
  const Eigen::Matrix3d & _yzF;
  const Eigen::Matrix3d & _zxF;
  const Eigen::Matrix<double,3,9> & _x1P;
  const Eigen::Matrix<double,3,9> & _y1P;
  const Eigen::Matrix<double,3,9> & _z1P;
  const Eigen::Matrix<double,3,9> & _x2P;
  const Eigen::Matrix<double,3,9> & _y2P;
  const Eigen::Matrix<double,3,9> & _z2P;
  const Eigen::Matrix<double,9,9> & _m11P;
  const Eigen::Matrix<double,9,9> & _m12P;
  const Eigen::Matrix<double,9,9> & _m22P;

  Ge_step(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P ) :
    OptimizationFunctor<double>(3,3),
    _xxF(xxF),_yyF(yyF),_zzF(zzF),_xyF(xyF),_yzF(yzF),_zxF(zxF),
    _x1P(x1P),_y1P(y1P),_z1P(z1P),_x2P(x2P),_y2P(y2P),_z2P(z2P),
    _m11P(m11P),_m12P(m12P),_m22P(m22P) {}

  int operator()(const Eigen::VectorXd &x, Eigen::VectorXd &fvec) const
  {
    cayley_t cayley = x;
    Eigen::Matrix<double,1,3> jacobian;
    ge::getCostWithJacobian(
        _xxF,_yyF,_zzF,_xyF,_yzF,_zxF,
        _x1P,_y1P,_z1P,_x2P,_y2P,_z2P,_m11P,_m12P,_m22P,cayley,jacobian,1);

    fvec[0] = jacobian(0,0);
    fvec[1] = jacobian(0,1);
    fvec[2] = jacobian(0,2);
    
    return 0;
  }
};

}
}
}

void
opengv::relative_pose::modules::ge_main(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    const cayley_t & startingPoint,
    geOutput_t & output )
{
  //this one doesn't work, probably because of double numerical differentiation
  //use ge_main2, which is an implementation of gradient descent
  const int n=3;
  VectorXd x(n);

  x = startingPoint;
  Ge_step functor(xxF,yyF,zzF,xyF,yzF,zxF,
      x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P);
  NumericalDiff<Ge_step> numDiff(functor);
  LevenbergMarquardt< NumericalDiff<Ge_step> > lm(numDiff);

  lm.resetParameters();
  lm.parameters.ftol = 0.000001;//1.E1*NumTraits<double>::epsilon();
  lm.parameters.xtol = 1.E1*NumTraits<double>::epsilon();
  lm.parameters.maxfev = 100;
  lm.minimize(x);

  cayley_t cayley = x;
  rotation_t R = math::cayley2rot(cayley);
  
  Eigen::Matrix4d G = ge::composeG(xxF,yyF,zzF,xyF,yzF,zxF,
      x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley);
  
  Eigen::EigenSolver< Eigen::Matrix4d > Eig(G,true);
  Eigen::Matrix<std::complex<double>,4,1> D_complex = Eig.eigenvalues();
  Eigen::Matrix<std::complex<double>,4,4> V_complex = Eig.eigenvectors();
  Eigen::Vector4d D;
  Eigen::Matrix4d V;
  for(size_t i = 0; i < 4; i++)
  {
    D[i] = D_complex[i].real();
    for(size_t j = 0; j < 4; j++)
      V(i,j) = V_complex(i,j).real();
  }

  double factor = V(3,0);
  Eigen::Vector4d t = (1.0/factor) * V.col(0);

  output.translation = t;
  output.rotation = R;
  output.eigenvalues = D;
  output.eigenvectors = V;
}

void
opengv::relative_pose::modules::ge_main2(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    const cayley_t & startingPoint,
    geOutput_t & output )
{
  //todo: the optimization strategy is something that can possibly be improved:
  //-one idea is to check the gradient at the new sampling point, if that derives
  // too much, we have to stop
  //-another idea consists of having linear change of lambda, instead of exponential (safer, but slower)
  
  double lambda = 0.01;
  double maxLambda = 0.08;
  double modifier = 2.0;
  int maxIterations = 50;
  double min_xtol = 0.00001;
  bool disablingIncrements = true;
  bool print = false;

  cayley_t cayley;
  
  double disturbanceAmplitude = 0.3;
  bool found = false;
  int randomTrialCount = 0;
  
  while( !found && randomTrialCount < 5 )
  {
    if(randomTrialCount > 2)
      disturbanceAmplitude = 0.6;
	
    if( randomTrialCount == 0 )
      cayley = startingPoint;
    else
    {
      cayley = startingPoint;
      Eigen::Vector3d disturbance;
      disturbance[0] = (((double) rand())/ ((double) RAND_MAX)-0.5)*2.0*disturbanceAmplitude;
      disturbance[1] = (((double) rand())/ ((double) RAND_MAX)-0.5)*2.0*disturbanceAmplitude;
      disturbance[2] = (((double) rand())/ ((double) RAND_MAX)-0.5)*2.0*disturbanceAmplitude;
      cayley += disturbance;
    }
	
    lambda = 0.01;
    int iterations = 0;
    double smallestEV = ge::getCost(xxF,yyF,zzF,xyF,yzF,zxF,
        x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,1);
    
    while( iterations < maxIterations )
    {
      Eigen::Matrix<double,1,3> jacobian;
      ge::getQuickJacobian(xxF,yyF,zzF,xyF,yzF,zxF,
          x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,smallestEV,jacobian,1);
      
      double norm = sqrt(pow(jacobian[0],2.0) + pow(jacobian[1],2.0) + pow(jacobian[2],2.0));
      cayley_t normalizedJacobian = (1/norm) * jacobian.transpose();
      
      cayley_t samplingPoint = cayley - lambda * normalizedJacobian;
      double samplingEV = ge::getCost(xxF,yyF,zzF,xyF,yzF,zxF,
          x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,samplingPoint,1);
      
      if(print)
      {
        std::cout << iterations << ": " << samplingPoint.transpose();
        std::cout << " lambda: " << lambda << " EV: " << samplingEV << std::endl;
      }
      
      if( iterations == 0 || !disablingIncrements )
      {
        while( samplingEV < smallestEV )
        {
          smallestEV = samplingEV;
          if( lambda * modifier > maxLambda )
            break;
          lambda *= modifier;
          samplingPoint = cayley - lambda * normalizedJacobian;
          samplingEV = ge::getCost(xxF,yyF,zzF,xyF,yzF,zxF,
              x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,samplingPoint,1);
          
          if(print)
          {
            std::cout << iterations << ": " << samplingPoint.transpose();
            std::cout << " lambda: " << lambda << " EV: " << samplingEV << std::endl;
          }
        }
      }
      
      while( samplingEV > smallestEV )
      {
        lambda /= modifier;
        samplingPoint = cayley - lambda * normalizedJacobian;
        samplingEV = ge::getCost(xxF,yyF,zzF,xyF,yzF,zxF,
            x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,samplingPoint,1);
        
        if(print)
        {
          std::cout << iterations << ": " << samplingPoint.transpose();
          std::cout << " lambda: " << lambda << " EV: " << samplingEV << std::endl;
        }
      }
      
      //apply update
      cayley = samplingPoint;
      smallestEV = samplingEV;
      
      //stopping condition (check if the update was too small)
      if( lambda < min_xtol )
        break;
      
      iterations++;
    }
    
    //try to see if we can robustly identify each time we enter up in the wrong minimum
    if( cayley.norm() < 0.01 )
    {
      //we are close to the origin, test the EV 2
      double ev2 = ge::getCost(xxF,yyF,zzF,xyF,yzF,zxF,
            x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,0);
      if( ev2 > 0.001 )
        randomTrialCount++;
      else
        found = true;
    }
    else
      found = true;
  }
  
  Eigen::Matrix4d G = ge::composeG(xxF,yyF,zzF,xyF,yzF,zxF,
      x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley);
  
  Eigen::EigenSolver< Eigen::Matrix4d > Eig(G,true);
  Eigen::Matrix<std::complex<double>,4,1> D_complex = Eig.eigenvalues();
  Eigen::Matrix<std::complex<double>,4,4> V_complex = Eig.eigenvectors();
  Eigen::Vector4d D;
  Eigen::Matrix4d V;
  for(size_t i = 0; i < 4; i++)
  {
    D[i] = D_complex[i].real();
    for(size_t j = 0; j < 4; j++)
      V(i,j) = V_complex(i,j).real();
  }

  double factor = V(3,0);
  Eigen::Vector4d t = (1.0/factor) * V.col(0);

  output.translation = t;
  output.rotation = math::cayley2rot(cayley);
  output.eigenvalues = D;
  output.eigenvectors = V;
}

void
opengv::relative_pose::modules::ge_plot(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    geOutput_t & output )
{
  cayley_t cayley = math::rot2cayley(output.rotation);
  
  ge::getEV(xxF,yyF,zzF,xyF,yzF,zxF,
      x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,output.eigenvalues);
  
  output.eigenvectors = ge::composeG(xxF,yyF,zzF,xyF,yzF,zxF,
      x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley);
}
