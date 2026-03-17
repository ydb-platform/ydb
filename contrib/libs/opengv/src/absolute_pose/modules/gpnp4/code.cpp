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


#include <opengv/absolute_pose/modules/gpnp4/modules.hpp>


void
opengv::absolute_pose::modules::gpnp4::compute(
    Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  sPolynomial5(groebnerMatrix);

  sPolynomial6(groebnerMatrix);
  groebnerRow5_0000_f(groebnerMatrix,6);

  sPolynomial7(groebnerMatrix);
  groebnerRow5_0000_f(groebnerMatrix,7);
  groebnerRow6_0000_f(groebnerMatrix,7);

  sPolynomial8(groebnerMatrix);
  groebnerRow5_0000_f(groebnerMatrix,8);
  groebnerRow6_0000_f(groebnerMatrix,8);
  groebnerRow7_0000_f(groebnerMatrix,8);

  sPolynomial9(groebnerMatrix);
  groebnerRow7_0100_f(groebnerMatrix,9);
  groebnerRow8_0100_f(groebnerMatrix,9);
  groebnerRow5_1000_f(groebnerMatrix,9);
  groebnerRow6_1000_f(groebnerMatrix,9);
  groebnerRow7_1000_f(groebnerMatrix,9);
  groebnerRow8_1000_f(groebnerMatrix,9);
  groebnerRow5_0000_f(groebnerMatrix,9);
  groebnerRow6_0000_f(groebnerMatrix,9);
  groebnerRow7_0000_f(groebnerMatrix,9);
  groebnerRow8_0000_f(groebnerMatrix,9);

  sPolynomial10(groebnerMatrix);
  groebnerRow6_0100_f(groebnerMatrix,10);
  groebnerRow7_0100_f(groebnerMatrix,10);
  groebnerRow8_0100_f(groebnerMatrix,10);
  groebnerRow9_0000_f(groebnerMatrix,10);
  groebnerRow5_1000_f(groebnerMatrix,10);
  groebnerRow6_1000_f(groebnerMatrix,10);
  groebnerRow7_1000_f(groebnerMatrix,10);
  groebnerRow8_1000_f(groebnerMatrix,10);
  groebnerRow5_0000_f(groebnerMatrix,10);
  groebnerRow6_0000_f(groebnerMatrix,10);
  groebnerRow7_0000_f(groebnerMatrix,10);
  groebnerRow8_0000_f(groebnerMatrix,10);

  sPolynomial11(groebnerMatrix);
  groebnerRow6_0100_f(groebnerMatrix,11);
  groebnerRow7_0100_f(groebnerMatrix,11);
  groebnerRow8_0100_f(groebnerMatrix,11);
  groebnerRow9_0000_f(groebnerMatrix,11);
  groebnerRow4_1000_f(groebnerMatrix,11);
  groebnerRow5_1000_f(groebnerMatrix,11);
  groebnerRow6_1000_f(groebnerMatrix,11);
  groebnerRow7_1000_f(groebnerMatrix,11);
  groebnerRow8_1000_f(groebnerMatrix,11);
  groebnerRow10_0000_f(groebnerMatrix,11);
  groebnerRow4_0000_f(groebnerMatrix,11);
  groebnerRow5_0000_f(groebnerMatrix,11);
  groebnerRow6_0000_f(groebnerMatrix,11);
  groebnerRow7_0000_f(groebnerMatrix,11);
  groebnerRow8_0000_f(groebnerMatrix,11);

  sPolynomial12(groebnerMatrix);
  groebnerRow5_0100_f(groebnerMatrix,12);
  groebnerRow6_0100_f(groebnerMatrix,12);
  groebnerRow7_0100_f(groebnerMatrix,12);
  groebnerRow8_0100_f(groebnerMatrix,12);
  groebnerRow9_0000_f(groebnerMatrix,12);
  groebnerRow4_1000_f(groebnerMatrix,12);
  groebnerRow5_1000_f(groebnerMatrix,12);
  groebnerRow6_1000_f(groebnerMatrix,12);
  groebnerRow7_1000_f(groebnerMatrix,12);
  groebnerRow8_1000_f(groebnerMatrix,12);
  groebnerRow10_0000_f(groebnerMatrix,12);
  groebnerRow11_0000_f(groebnerMatrix,12);
  groebnerRow4_0000_f(groebnerMatrix,12);
  groebnerRow5_0000_f(groebnerMatrix,12);
  groebnerRow6_0000_f(groebnerMatrix,12);
  groebnerRow7_0000_f(groebnerMatrix,12);
  groebnerRow8_0000_f(groebnerMatrix,12);

  sPolynomial13(groebnerMatrix);
  groebnerRow6_0010_f(groebnerMatrix,13);
  groebnerRow4_0100_f(groebnerMatrix,13);
  groebnerRow5_0100_f(groebnerMatrix,13);
  groebnerRow6_0100_f(groebnerMatrix,13);
  groebnerRow7_0100_f(groebnerMatrix,13);
  groebnerRow8_0100_f(groebnerMatrix,13);
  groebnerRow9_0000_f(groebnerMatrix,13);
  groebnerRow4_1000_f(groebnerMatrix,13);
  groebnerRow5_1000_f(groebnerMatrix,13);
  groebnerRow6_1000_f(groebnerMatrix,13);
  groebnerRow7_1000_f(groebnerMatrix,13);
  groebnerRow8_1000_f(groebnerMatrix,13);
  groebnerRow10_0000_f(groebnerMatrix,13);
  groebnerRow11_0000_f(groebnerMatrix,13);
  groebnerRow12_0000_f(groebnerMatrix,13);
  groebnerRow4_0000_f(groebnerMatrix,13);
  groebnerRow5_0000_f(groebnerMatrix,13);
  groebnerRow6_0000_f(groebnerMatrix,13);
  groebnerRow7_0000_f(groebnerMatrix,13);
  groebnerRow8_0000_f(groebnerMatrix,13);

  sPolynomial14(groebnerMatrix);
  groebnerRow5_0010_f(groebnerMatrix,14);
  groebnerRow6_0010_f(groebnerMatrix,14);
  groebnerRow4_0100_f(groebnerMatrix,14);
  groebnerRow5_0100_f(groebnerMatrix,14);
  groebnerRow6_0100_f(groebnerMatrix,14);
  groebnerRow7_0100_f(groebnerMatrix,14);
  groebnerRow8_0100_f(groebnerMatrix,14);
  groebnerRow9_0000_f(groebnerMatrix,14);
  groebnerRow4_1000_f(groebnerMatrix,14);
  groebnerRow5_1000_f(groebnerMatrix,14);
  groebnerRow6_1000_f(groebnerMatrix,14);
  groebnerRow7_1000_f(groebnerMatrix,14);
  groebnerRow8_1000_f(groebnerMatrix,14);
  groebnerRow10_0000_f(groebnerMatrix,14);
  groebnerRow11_0000_f(groebnerMatrix,14);
  groebnerRow12_0000_f(groebnerMatrix,14);
  groebnerRow13_0000_f(groebnerMatrix,14);
  groebnerRow4_0000_f(groebnerMatrix,14);
  groebnerRow5_0000_f(groebnerMatrix,14);
  groebnerRow6_0000_f(groebnerMatrix,14);
  groebnerRow7_0000_f(groebnerMatrix,14);
  groebnerRow8_0000_f(groebnerMatrix,14);

  sPolynomial15(groebnerMatrix);
  groebnerRow14_1000_f(groebnerMatrix,15);
  groebnerRow9_0000_f(groebnerMatrix,15);
  groebnerRow7_1000_f(groebnerMatrix,15);
  groebnerRow8_1000_f(groebnerMatrix,15);
  groebnerRow10_0000_f(groebnerMatrix,15);
  groebnerRow11_0000_f(groebnerMatrix,15);
  groebnerRow12_0000_f(groebnerMatrix,15);
  groebnerRow13_0000_f(groebnerMatrix,15);
  groebnerRow14_0000_f(groebnerMatrix,15);
  groebnerRow7_0000_f(groebnerMatrix,15);
  groebnerRow8_0000_f(groebnerMatrix,15);

  sPolynomial16(groebnerMatrix);
  groebnerRow13_1000_f(groebnerMatrix,16);
  groebnerRow14_1000_f(groebnerMatrix,16);
  groebnerRow8_0100_f(groebnerMatrix,16);
  groebnerRow15_0100_f(groebnerMatrix,16);
  groebnerRow5_1000_f(groebnerMatrix,16);
  groebnerRow6_1000_f(groebnerMatrix,16);
  groebnerRow7_1000_f(groebnerMatrix,16);
  groebnerRow8_1000_f(groebnerMatrix,16);
  groebnerRow15_1000_f(groebnerMatrix,16);
  groebnerRow11_0000_f(groebnerMatrix,16);
  groebnerRow12_0000_f(groebnerMatrix,16);
  groebnerRow13_0000_f(groebnerMatrix,16);
  groebnerRow14_0000_f(groebnerMatrix,16);
  groebnerRow5_0000_f(groebnerMatrix,16);
  groebnerRow6_0000_f(groebnerMatrix,16);
  groebnerRow7_0000_f(groebnerMatrix,16);
  groebnerRow8_0000_f(groebnerMatrix,16);
  groebnerRow15_0000_f(groebnerMatrix,16);

  sPolynomial17(groebnerMatrix);
  groebnerRow12_1000_f(groebnerMatrix,17);
  groebnerRow13_1000_f(groebnerMatrix,17);
  groebnerRow14_1000_f(groebnerMatrix,17);
  groebnerRow7_0100_f(groebnerMatrix,17);
  groebnerRow8_0100_f(groebnerMatrix,17);
  groebnerRow15_0100_f(groebnerMatrix,17);
  groebnerRow4_1000_f(groebnerMatrix,17);
  groebnerRow5_1000_f(groebnerMatrix,17);
  groebnerRow6_1000_f(groebnerMatrix,17);
  groebnerRow7_1000_f(groebnerMatrix,17);
  groebnerRow8_1000_f(groebnerMatrix,17);
  groebnerRow15_1000_f(groebnerMatrix,17);
  groebnerRow16_1000_f(groebnerMatrix,17);
  groebnerRow12_0000_f(groebnerMatrix,17);
  groebnerRow13_0000_f(groebnerMatrix,17);
  groebnerRow14_0000_f(groebnerMatrix,17);
  groebnerRow4_0000_f(groebnerMatrix,17);
  groebnerRow5_0000_f(groebnerMatrix,17);
  groebnerRow6_0000_f(groebnerMatrix,17);
  groebnerRow7_0000_f(groebnerMatrix,17);
  groebnerRow8_0000_f(groebnerMatrix,17);
  groebnerRow15_0000_f(groebnerMatrix,17);
  groebnerRow16_0000_f(groebnerMatrix,17);

  sPolynomial18(groebnerMatrix);
  groebnerRow14_0001_f(groebnerMatrix,18);
  groebnerRow14_0010_f(groebnerMatrix,18);
  groebnerRow13_1000_f(groebnerMatrix,18);
  groebnerRow14_1000_f(groebnerMatrix,18);
  groebnerRow7_0100_f(groebnerMatrix,18);
  groebnerRow8_0100_f(groebnerMatrix,18);
  groebnerRow15_0100_f(groebnerMatrix,18);
  groebnerRow4_1000_f(groebnerMatrix,18);
  groebnerRow5_1000_f(groebnerMatrix,18);
  groebnerRow6_1000_f(groebnerMatrix,18);
  groebnerRow7_1000_f(groebnerMatrix,18);
  groebnerRow8_1000_f(groebnerMatrix,18);
  groebnerRow15_1000_f(groebnerMatrix,18);
  groebnerRow16_1000_f(groebnerMatrix,18);
  groebnerRow17_1000_f(groebnerMatrix,18);
  groebnerRow13_0000_f(groebnerMatrix,18);
  groebnerRow14_0000_f(groebnerMatrix,18);
  groebnerRow4_0000_f(groebnerMatrix,18);
  groebnerRow5_0000_f(groebnerMatrix,18);
  groebnerRow6_0000_f(groebnerMatrix,18);
  groebnerRow7_0000_f(groebnerMatrix,18);
  groebnerRow8_0000_f(groebnerMatrix,18);
  groebnerRow15_0000_f(groebnerMatrix,18);
  groebnerRow16_0000_f(groebnerMatrix,18);
  groebnerRow17_0000_f(groebnerMatrix,18);

  sPolynomial19(groebnerMatrix);
  groebnerRow14_0000_f(groebnerMatrix,19);
  groebnerRow15_0000_f(groebnerMatrix,19);
  groebnerRow16_0000_f(groebnerMatrix,19);
  groebnerRow17_0000_f(groebnerMatrix,19);
  groebnerRow18_0000_f(groebnerMatrix,19);

  sPolynomial20(groebnerMatrix);
  groebnerRow18_1000_f(groebnerMatrix,20);
  groebnerRow19_1000_f(groebnerMatrix,20);
  groebnerRow15_0000_f(groebnerMatrix,20);
  groebnerRow16_0000_f(groebnerMatrix,20);
  groebnerRow17_0000_f(groebnerMatrix,20);
  groebnerRow18_0000_f(groebnerMatrix,20);
  groebnerRow19_0000_f(groebnerMatrix,20);

  sPolynomial21(groebnerMatrix);
  groebnerRow17_1000_f(groebnerMatrix,21);
  groebnerRow18_1000_f(groebnerMatrix,21);
  groebnerRow19_1000_f(groebnerMatrix,21);
  groebnerRow15_0000_f(groebnerMatrix,21);
  groebnerRow20_1000_f(groebnerMatrix,21);
  groebnerRow17_0000_f(groebnerMatrix,21);
  groebnerRow18_0000_f(groebnerMatrix,21);
  groebnerRow19_0000_f(groebnerMatrix,21);
  groebnerRow20_0000_f(groebnerMatrix,21);

  sPolynomial22(groebnerMatrix);
  groebnerRow19_0001_f(groebnerMatrix,22);
  groebnerRow19_0010_f(groebnerMatrix,22);
  groebnerRow18_1000_f(groebnerMatrix,22);
  groebnerRow19_1000_f(groebnerMatrix,22);
  groebnerRow20_0001_f(groebnerMatrix,22);
  groebnerRow20_0010_f(groebnerMatrix,22);
  groebnerRow21_0010_f(groebnerMatrix,22);
  groebnerRow20_0100_f(groebnerMatrix,22);
  groebnerRow21_0100_f(groebnerMatrix,22);
  groebnerRow15_0000_f(groebnerMatrix,22);
  groebnerRow20_1000_f(groebnerMatrix,22);
  groebnerRow21_1000_f(groebnerMatrix,22);
  groebnerRow18_0000_f(groebnerMatrix,22);
  groebnerRow19_0000_f(groebnerMatrix,22);
  groebnerRow20_0000_f(groebnerMatrix,22);
  groebnerRow21_0000_f(groebnerMatrix,22);

  sPolynomial23(groebnerMatrix);
  groebnerRow20_1100_f(groebnerMatrix,23);
  groebnerRow21_1100_f(groebnerMatrix,23);
  groebnerRow22_1100_f(groebnerMatrix,23);
  groebnerRow19_0001_f(groebnerMatrix,23);
  groebnerRow19_0010_f(groebnerMatrix,23);
  groebnerRow19_0100_f(groebnerMatrix,23);
  groebnerRow19_1000_f(groebnerMatrix,23);
  groebnerRow20_0001_f(groebnerMatrix,23);
  groebnerRow20_0010_f(groebnerMatrix,23);
  groebnerRow21_0010_f(groebnerMatrix,23);
  groebnerRow20_0100_f(groebnerMatrix,23);
  groebnerRow21_0100_f(groebnerMatrix,23);
  groebnerRow22_0100_f(groebnerMatrix,23);
  groebnerRow20_1000_f(groebnerMatrix,23);
  groebnerRow21_1000_f(groebnerMatrix,23);
  groebnerRow22_1000_f(groebnerMatrix,23);
  groebnerRow19_0000_f(groebnerMatrix,23);
  groebnerRow20_0000_f(groebnerMatrix,23);
  groebnerRow21_0000_f(groebnerMatrix,23);
  groebnerRow22_0000_f(groebnerMatrix,23);

  sPolynomial24(groebnerMatrix);
  groebnerRow20_0000_f(groebnerMatrix,24);
  groebnerRow21_0000_f(groebnerMatrix,24);
  groebnerRow22_0000_f(groebnerMatrix,24);
  groebnerRow23_0000_f(groebnerMatrix,24);

}
