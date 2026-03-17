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
opengv::absolute_pose::modules::gpnp3::init(
    Eigen::Matrix<double,15,18> & groebnerMatrix,
    const Eigen::Matrix<double,12,1> & a,
    Eigen::Matrix<double,12,1> & n,
    Eigen::Matrix<double,12,1> & m,
    Eigen::Matrix<double,12,1> & k,
    Eigen::Vector3d & c0,
    Eigen::Vector3d & c1,
    Eigen::Vector3d & c2,
    Eigen::Vector3d & c3 )
{
  Eigen::Vector3d temp = c0-c1;
  double c01w = temp.norm()*temp.norm();
  temp = c0-c2;
  double c02w = temp.norm()*temp.norm();
  temp = c0-c3;
  double c03w = temp.norm()*temp.norm();
  temp = c1-c2;
  double c12w = temp.norm()*temp.norm();

  groebnerMatrix(0,8) = ((((((((-2*k(0,0)*k(3,0)-2*k(1,0)*k(4,0))-2*k(2,0)*k(5,0))+pow(k(0,0),2))+pow(k(3,0),2))+pow(k(1,0),2))+pow(k(4,0),2))+pow(k(2,0),2))+pow(k(5,0),2));
  groebnerMatrix(0,9) = (((((((((((2*m(0,0)*k(0,0)-2*m(0,0)*k(3,0))-2*k(0,0)*m(3,0))+2*m(3,0)*k(3,0))+2*m(1,0)*k(1,0))-2*m(1,0)*k(4,0))-2*k(1,0)*m(4,0))+2*m(4,0)*k(4,0))+2*m(2,0)*k(2,0))-2*m(2,0)*k(5,0))-2*k(2,0)*m(5,0))+2*m(5,0)*k(5,0));
  groebnerMatrix(0,10) = ((((((((-2*m(0,0)*m(3,0)-2*m(1,0)*m(4,0))-2*m(2,0)*m(5,0))+pow(m(0,0),2))+pow(m(3,0),2))+pow(m(1,0),2))+pow(m(4,0),2))+pow(m(2,0),2))+pow(m(5,0),2));
  groebnerMatrix(0,11) = (((((((((((2*n(0,0)*k(0,0)-2*n(0,0)*k(3,0))-2*k(0,0)*n(3,0))+2*n(3,0)*k(3,0))+2*n(1,0)*k(1,0))-2*n(1,0)*k(4,0))-2*k(1,0)*n(4,0))+2*n(4,0)*k(4,0))+2*n(2,0)*k(2,0))-2*n(2,0)*k(5,0))-2*k(2,0)*n(5,0))+2*n(5,0)*k(5,0));
  groebnerMatrix(0,12) = (((((((((((2*n(0,0)*m(0,0)-2*n(0,0)*m(3,0))-2*m(0,0)*n(3,0))+2*n(3,0)*m(3,0))+2*n(1,0)*m(1,0))-2*n(1,0)*m(4,0))-2*m(1,0)*n(4,0))+2*n(4,0)*m(4,0))+2*n(2,0)*m(2,0))-2*n(2,0)*m(5,0))-2*m(2,0)*n(5,0))+2*n(5,0)*m(5,0));
  groebnerMatrix(0,13) = ((((((((-2*n(0,0)*n(3,0)-2*n(1,0)*n(4,0))-2*n(2,0)*n(5,0))+pow(n(0,0),2))+pow(n(3,0),2))+pow(n(1,0),2))+pow(n(4,0),2))+pow(n(2,0),2))+pow(n(5,0),2));
  groebnerMatrix(0,14) = (((((((((((2*a(0,0)*k(0,0)-2*a(0,0)*k(3,0))-2*k(0,0)*a(3,0))+2*a(3,0)*k(3,0))+2*a(1,0)*k(1,0))-2*a(1,0)*k(4,0))-2*k(1,0)*a(4,0))+2*a(4,0)*k(4,0))+2*a(2,0)*k(2,0))-2*a(2,0)*k(5,0))-2*k(2,0)*a(5,0))+2*a(5,0)*k(5,0));
  groebnerMatrix(0,15) = (((((((((((2*a(0,0)*m(0,0)-2*a(0,0)*m(3,0))-2*m(0,0)*a(3,0))+2*a(3,0)*m(3,0))+2*a(1,0)*m(1,0))-2*a(1,0)*m(4,0))-2*m(1,0)*a(4,0))+2*a(4,0)*m(4,0))+2*a(2,0)*m(2,0))-2*a(2,0)*m(5,0))-2*m(2,0)*a(5,0))+2*a(5,0)*m(5,0));
  groebnerMatrix(0,16) = (((((((((((2*a(0,0)*n(0,0)-2*a(0,0)*n(3,0))-2*n(0,0)*a(3,0))+2*a(3,0)*n(3,0))+2*a(1,0)*n(1,0))-2*a(1,0)*n(4,0))-2*n(1,0)*a(4,0))+2*a(4,0)*n(4,0))+2*a(2,0)*n(2,0))-2*a(2,0)*n(5,0))-2*n(2,0)*a(5,0))+2*a(5,0)*n(5,0));
  groebnerMatrix(0,17) = (((((((((-c01w+pow(a(0,0),2))-2*a(0,0)*a(3,0))+pow(a(3,0),2))+pow(a(1,0),2))-2*a(1,0)*a(4,0))+pow(a(4,0),2))+pow(a(2,0),2))-2*a(2,0)*a(5,0))+pow(a(5,0),2));
  groebnerMatrix(1,8) = ((((((((-2*k(0,0)*k(6,0)-2*k(1,0)*k(7,0))-2*k(2,0)*k(8,0))+pow(k(0,0),2))+pow(k(1,0),2))+pow(k(2,0),2))+pow(k(6,0),2))+pow(k(7,0),2))+pow(k(8,0),2));
  groebnerMatrix(1,9) = (((((((((((2*m(0,0)*k(0,0)+2*m(1,0)*k(1,0))+2*m(2,0)*k(2,0))-2*m(0,0)*k(6,0))-2*k(0,0)*m(6,0))+2*m(6,0)*k(6,0))-2*m(1,0)*k(7,0))-2*k(1,0)*m(7,0))+2*m(7,0)*k(7,0))-2*m(2,0)*k(8,0))-2*k(2,0)*m(8,0))+2*m(8,0)*k(8,0));
  groebnerMatrix(1,10) = ((((((((-2*m(0,0)*m(6,0)-2*m(1,0)*m(7,0))-2*m(2,0)*m(8,0))+pow(m(0,0),2))+pow(m(1,0),2))+pow(m(2,0),2))+pow(m(6,0),2))+pow(m(7,0),2))+pow(m(8,0),2));
  groebnerMatrix(1,11) = (((((((((((2*n(0,0)*k(0,0)+2*n(1,0)*k(1,0))+2*n(2,0)*k(2,0))-2*n(0,0)*k(6,0))-2*k(0,0)*n(6,0))+2*n(6,0)*k(6,0))-2*n(1,0)*k(7,0))-2*k(1,0)*n(7,0))+2*n(7,0)*k(7,0))-2*n(2,0)*k(8,0))-2*k(2,0)*n(8,0))+2*n(8,0)*k(8,0));
  groebnerMatrix(1,12) = (((((((((((2*n(0,0)*m(0,0)+2*n(1,0)*m(1,0))+2*n(2,0)*m(2,0))-2*n(0,0)*m(6,0))-2*m(0,0)*n(6,0))+2*n(6,0)*m(6,0))-2*n(1,0)*m(7,0))-2*m(1,0)*n(7,0))+2*n(7,0)*m(7,0))-2*n(2,0)*m(8,0))-2*m(2,0)*n(8,0))+2*n(8,0)*m(8,0));
  groebnerMatrix(1,13) = ((((((((-2*n(0,0)*n(6,0)-2*n(1,0)*n(7,0))-2*n(2,0)*n(8,0))+pow(n(0,0),2))+pow(n(1,0),2))+pow(n(2,0),2))+pow(n(6,0),2))+pow(n(7,0),2))+pow(n(8,0),2));
  groebnerMatrix(1,14) = (((((((((((2*a(0,0)*k(0,0)+2*a(1,0)*k(1,0))+2*a(2,0)*k(2,0))-2*a(0,0)*k(6,0))-2*k(0,0)*a(6,0))+2*a(6,0)*k(6,0))-2*a(1,0)*k(7,0))-2*k(1,0)*a(7,0))+2*a(7,0)*k(7,0))-2*a(2,0)*k(8,0))-2*k(2,0)*a(8,0))+2*a(8,0)*k(8,0));
  groebnerMatrix(1,15) = (((((((((((2*a(0,0)*m(0,0)+2*a(1,0)*m(1,0))+2*a(2,0)*m(2,0))-2*a(0,0)*m(6,0))-2*m(0,0)*a(6,0))+2*a(6,0)*m(6,0))-2*a(1,0)*m(7,0))-2*m(1,0)*a(7,0))+2*a(7,0)*m(7,0))-2*a(2,0)*m(8,0))-2*m(2,0)*a(8,0))+2*a(8,0)*m(8,0));
  groebnerMatrix(1,16) = (((((((((((2*a(0,0)*n(0,0)+2*a(1,0)*n(1,0))+2*a(2,0)*n(2,0))-2*a(0,0)*n(6,0))-2*n(0,0)*a(6,0))+2*a(6,0)*n(6,0))-2*a(1,0)*n(7,0))-2*n(1,0)*a(7,0))+2*a(7,0)*n(7,0))-2*a(2,0)*n(8,0))-2*n(2,0)*a(8,0))+2*a(8,0)*n(8,0));
  groebnerMatrix(1,17) = (((((((((-c02w+pow(a(0,0),2))+pow(a(1,0),2))+pow(a(2,0),2))-2*a(0,0)*a(6,0))+pow(a(6,0),2))-2*a(1,0)*a(7,0))+pow(a(7,0),2))-2*a(2,0)*a(8,0))+pow(a(8,0),2));
  groebnerMatrix(2,8) = ((((((((pow(k(0,0),2)+pow(k(1,0),2))+pow(k(2,0),2))+pow(k(9,0),2))+pow(k(10,0),2))+pow(k(11,0),2))-2*k(0,0)*k(9,0))-2*k(1,0)*k(10,0))-2*k(2,0)*k(11,0));
  groebnerMatrix(2,9) = (((((((((((2*m(0,0)*k(0,0)+2*m(1,0)*k(1,0))+2*m(2,0)*k(2,0))-2*m(0,0)*k(9,0))-2*k(0,0)*m(9,0))+2*m(9,0)*k(9,0))-2*m(1,0)*k(10,0))-2*k(1,0)*m(10,0))+2*m(10,0)*k(10,0))-2*m(2,0)*k(11,0))-2*k(2,0)*m(11,0))+2*m(11,0)*k(11,0));
  groebnerMatrix(2,10) = ((((((((pow(m(0,0),2)+pow(m(1,0),2))+pow(m(2,0),2))+pow(m(9,0),2))+pow(m(10,0),2))+pow(m(11,0),2))-2*m(0,0)*m(9,0))-2*m(1,0)*m(10,0))-2*m(2,0)*m(11,0));
  groebnerMatrix(2,11) = (((((((((((2*n(0,0)*k(0,0)+2*n(1,0)*k(1,0))+2*n(2,0)*k(2,0))-2*n(0,0)*k(9,0))-2*k(0,0)*n(9,0))+2*n(9,0)*k(9,0))-2*n(1,0)*k(10,0))-2*k(1,0)*n(10,0))+2*n(10,0)*k(10,0))-2*n(2,0)*k(11,0))-2*k(2,0)*n(11,0))+2*n(11,0)*k(11,0));
  groebnerMatrix(2,12) = (((((((((((2*n(0,0)*m(0,0)+2*n(1,0)*m(1,0))+2*n(2,0)*m(2,0))-2*n(0,0)*m(9,0))-2*m(0,0)*n(9,0))+2*n(9,0)*m(9,0))-2*n(1,0)*m(10,0))-2*m(1,0)*n(10,0))+2*n(10,0)*m(10,0))-2*n(2,0)*m(11,0))-2*m(2,0)*n(11,0))+2*n(11,0)*m(11,0));
  groebnerMatrix(2,13) = ((((((((pow(n(0,0),2)+pow(n(1,0),2))+pow(n(2,0),2))+pow(n(9,0),2))+pow(n(10,0),2))+pow(n(11,0),2))-2*n(0,0)*n(9,0))-2*n(1,0)*n(10,0))-2*n(2,0)*n(11,0));
  groebnerMatrix(2,14) = (((((((((((2*a(0,0)*k(0,0)+2*a(1,0)*k(1,0))+2*a(2,0)*k(2,0))-2*a(0,0)*k(9,0))-2*k(0,0)*a(9,0))+2*a(9,0)*k(9,0))-2*a(1,0)*k(10,0))-2*k(1,0)*a(10,0))+2*a(10,0)*k(10,0))-2*a(2,0)*k(11,0))-2*k(2,0)*a(11,0))+2*a(11,0)*k(11,0));
  groebnerMatrix(2,15) = (((((((((((2*a(0,0)*m(0,0)+2*a(1,0)*m(1,0))+2*a(2,0)*m(2,0))-2*a(0,0)*m(9,0))-2*m(0,0)*a(9,0))+2*a(9,0)*m(9,0))-2*a(1,0)*m(10,0))-2*m(1,0)*a(10,0))+2*a(10,0)*m(10,0))-2*a(2,0)*m(11,0))-2*m(2,0)*a(11,0))+2*a(11,0)*m(11,0));
  groebnerMatrix(2,16) = (((((((((((2*a(0,0)*n(0,0)+2*a(1,0)*n(1,0))+2*a(2,0)*n(2,0))-2*a(0,0)*n(9,0))-2*n(0,0)*a(9,0))+2*a(9,0)*n(9,0))-2*a(1,0)*n(10,0))-2*n(1,0)*a(10,0))+2*a(10,0)*n(10,0))-2*a(2,0)*n(11,0))-2*n(2,0)*a(11,0))+2*a(11,0)*n(11,0));
  groebnerMatrix(2,17) = (((((((((-c03w+pow(a(0,0),2))+pow(a(1,0),2))+pow(a(2,0),2))-2*a(0,0)*a(9,0))+pow(a(9,0),2))-2*a(1,0)*a(10,0))+pow(a(10,0),2))-2*a(2,0)*a(11,0))+pow(a(11,0),2));
  groebnerMatrix(3,8) = ((((((((pow(k(3,0),2)+pow(k(4,0),2))+pow(k(5,0),2))+pow(k(6,0),2))+pow(k(7,0),2))+pow(k(8,0),2))-2*k(3,0)*k(6,0))-2*k(4,0)*k(7,0))-2*k(5,0)*k(8,0));
  groebnerMatrix(3,9) = (((((((((((2*m(3,0)*k(3,0)+2*m(4,0)*k(4,0))+2*m(5,0)*k(5,0))+2*m(6,0)*k(6,0))+2*m(7,0)*k(7,0))+2*m(8,0)*k(8,0))-2*m(3,0)*k(6,0))-2*k(3,0)*m(6,0))-2*m(4,0)*k(7,0))-2*k(4,0)*m(7,0))-2*m(5,0)*k(8,0))-2*k(5,0)*m(8,0));
  groebnerMatrix(3,10) = ((((((((pow(m(3,0),2)+pow(m(4,0),2))+pow(m(5,0),2))+pow(m(6,0),2))+pow(m(7,0),2))+pow(m(8,0),2))-2*m(3,0)*m(6,0))-2*m(4,0)*m(7,0))-2*m(5,0)*m(8,0));
  groebnerMatrix(3,11) = (((((((((((2*n(3,0)*k(3,0)+2*n(4,0)*k(4,0))+2*n(5,0)*k(5,0))+2*n(6,0)*k(6,0))+2*n(7,0)*k(7,0))+2*n(8,0)*k(8,0))-2*n(3,0)*k(6,0))-2*k(3,0)*n(6,0))-2*n(4,0)*k(7,0))-2*k(4,0)*n(7,0))-2*n(5,0)*k(8,0))-2*k(5,0)*n(8,0));
  groebnerMatrix(3,12) = (((((((((((2*n(3,0)*m(3,0)+2*n(4,0)*m(4,0))+2*n(5,0)*m(5,0))+2*n(6,0)*m(6,0))+2*n(7,0)*m(7,0))+2*n(8,0)*m(8,0))-2*n(3,0)*m(6,0))-2*m(3,0)*n(6,0))-2*n(4,0)*m(7,0))-2*m(4,0)*n(7,0))-2*n(5,0)*m(8,0))-2*m(5,0)*n(8,0));
  groebnerMatrix(3,13) = ((((((((pow(n(3,0),2)+pow(n(4,0),2))+pow(n(5,0),2))+pow(n(6,0),2))+pow(n(7,0),2))+pow(n(8,0),2))-2*n(3,0)*n(6,0))-2*n(4,0)*n(7,0))-2*n(5,0)*n(8,0));
  groebnerMatrix(3,14) = (((((((((((2*a(3,0)*k(3,0)+2*a(4,0)*k(4,0))+2*a(5,0)*k(5,0))+2*a(6,0)*k(6,0))+2*a(7,0)*k(7,0))+2*a(8,0)*k(8,0))-2*a(3,0)*k(6,0))-2*k(3,0)*a(6,0))-2*a(4,0)*k(7,0))-2*k(4,0)*a(7,0))-2*a(5,0)*k(8,0))-2*k(5,0)*a(8,0));
  groebnerMatrix(3,15) = (((((((((((2*a(3,0)*m(3,0)+2*a(4,0)*m(4,0))+2*a(5,0)*m(5,0))+2*a(6,0)*m(6,0))+2*a(7,0)*m(7,0))+2*a(8,0)*m(8,0))-2*a(3,0)*m(6,0))-2*m(3,0)*a(6,0))-2*a(4,0)*m(7,0))-2*m(4,0)*a(7,0))-2*a(5,0)*m(8,0))-2*m(5,0)*a(8,0));
  groebnerMatrix(3,16) = (((((((((((2*a(3,0)*n(3,0)+2*a(4,0)*n(4,0))+2*a(5,0)*n(5,0))+2*a(6,0)*n(6,0))+2*a(7,0)*n(7,0))+2*a(8,0)*n(8,0))-2*a(3,0)*n(6,0))-2*n(3,0)*a(6,0))-2*a(4,0)*n(7,0))-2*n(4,0)*a(7,0))-2*a(5,0)*n(8,0))-2*n(5,0)*a(8,0));
  groebnerMatrix(3,17) = (((((((((-c12w+pow(a(3,0),2))+pow(a(4,0),2))+pow(a(5,0),2))+pow(a(6,0),2))+pow(a(7,0),2))+pow(a(8,0),2))-2*a(3,0)*a(6,0))-2*a(4,0)*a(7,0))-2*a(5,0)*a(8,0));
}
