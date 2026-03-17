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


#include <opengv/absolute_pose/modules/gpnp3/modules.hpp>


void
opengv::absolute_pose::modules::gpnp3::compute(
    Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  sPolynomial4(groebnerMatrix);

  sPolynomial5(groebnerMatrix);
  groebnerRow4_000_f(groebnerMatrix,5);

  sPolynomial6(groebnerMatrix);
  groebnerRow4_000_f(groebnerMatrix,6);
  groebnerRow5_000_f(groebnerMatrix,6);

  sPolynomial7(groebnerMatrix);
  groebnerRow5_100_f(groebnerMatrix,7);
  groebnerRow6_100_f(groebnerMatrix,7);
  groebnerRow4_000_f(groebnerMatrix,7);
  groebnerRow5_000_f(groebnerMatrix,7);
  groebnerRow6_000_f(groebnerMatrix,7);

  sPolynomial8(groebnerMatrix);
  groebnerRow4_100_f(groebnerMatrix,8);
  groebnerRow5_100_f(groebnerMatrix,8);
  groebnerRow6_100_f(groebnerMatrix,8);
  groebnerRow7_000_f(groebnerMatrix,8);
  groebnerRow3_000_f(groebnerMatrix,8);
  groebnerRow4_000_f(groebnerMatrix,8);
  groebnerRow5_000_f(groebnerMatrix,8);
  groebnerRow6_000_f(groebnerMatrix,8);

  sPolynomial9(groebnerMatrix);
  groebnerRow5_010_f(groebnerMatrix,9);
  groebnerRow3_100_f(groebnerMatrix,9);
  groebnerRow4_100_f(groebnerMatrix,9);
  groebnerRow5_100_f(groebnerMatrix,9);
  groebnerRow6_100_f(groebnerMatrix,9);
  groebnerRow7_000_f(groebnerMatrix,9);
  groebnerRow8_000_f(groebnerMatrix,9);
  groebnerRow3_000_f(groebnerMatrix,9);
  groebnerRow4_000_f(groebnerMatrix,9);
  groebnerRow5_000_f(groebnerMatrix,9);
  groebnerRow6_000_f(groebnerMatrix,9);

  sPolynomial10(groebnerMatrix);
  groebnerRow4_010_f(groebnerMatrix,10);
  groebnerRow5_010_f(groebnerMatrix,10);
  groebnerRow3_100_f(groebnerMatrix,10);
  groebnerRow4_100_f(groebnerMatrix,10);
  groebnerRow5_100_f(groebnerMatrix,10);
  groebnerRow6_100_f(groebnerMatrix,10);
  groebnerRow9_100_f(groebnerMatrix,10);
  groebnerRow8_000_f(groebnerMatrix,10);
  groebnerRow3_000_f(groebnerMatrix,10);
  groebnerRow4_000_f(groebnerMatrix,10);
  groebnerRow5_000_f(groebnerMatrix,10);
  groebnerRow6_000_f(groebnerMatrix,10);
  groebnerRow9_000_f(groebnerMatrix,10);

  sPolynomial11(groebnerMatrix);
  groebnerRow6_000_f(groebnerMatrix,11);
  groebnerRow9_000_f(groebnerMatrix,11);
  groebnerRow10_000_f(groebnerMatrix,11);

  sPolynomial12(groebnerMatrix);
  groebnerRow10_100_f(groebnerMatrix,12);
  groebnerRow11_100_f(groebnerMatrix,12);
  groebnerRow9_000_f(groebnerMatrix,12);
  groebnerRow10_000_f(groebnerMatrix,12);
  groebnerRow11_000_f(groebnerMatrix,12);

  sPolynomial13(groebnerMatrix);
  groebnerRow10_100_f(groebnerMatrix,13);
  groebnerRow11_010_f(groebnerMatrix,13);
  groebnerRow12_010_f(groebnerMatrix,13);
  groebnerRow11_100_f(groebnerMatrix,13);
  groebnerRow12_100_f(groebnerMatrix,13);
  groebnerRow10_000_f(groebnerMatrix,13);
  groebnerRow11_000_f(groebnerMatrix,13);
  groebnerRow12_000_f(groebnerMatrix,13);

  sPolynomial14(groebnerMatrix);
  groebnerRow11_000_f(groebnerMatrix,14);
  groebnerRow12_000_f(groebnerMatrix,14);
  groebnerRow13_000_f(groebnerMatrix,14);

}
