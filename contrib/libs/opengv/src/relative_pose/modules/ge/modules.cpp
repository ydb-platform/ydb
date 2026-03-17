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


#include <opengv/relative_pose/modules/ge/modules.hpp>
#include <opengv/math/cayley.hpp>
#include <iostream>

void
opengv::relative_pose::modules::ge::getEV(
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
    const cayley_t & cayley,
    Eigen::Vector4d & roots )
{
  Eigen::Matrix4d G = composeG(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley);
  
  // now compute the roots in closed-form
  //double G00_2 = G(0,0) * G(0,0);
  double G01_2 = G(0,1) * G(0,1);
  double G02_2 = G(0,2) * G(0,2);
  double G03_2 = G(0,3) * G(0,3);
  //double G11_2 = G(1,1) * G(1,1);
  double G12_2 = G(1,2) * G(1,2);
  double G13_2 = G(1,3) * G(1,3);
  //double G22_2 = G(2,2) * G(2,2);
  double G23_2 = G(2,3) * G(2,3);
  //double G33_2 = G(3,3) * G(3,3);
  
  double B = -G(3,3)-G(2,2)-G(1,1)-G(0,0);
  double C = -G23_2+G(2,2)*G(3,3)-G13_2-G12_2+G(1,1)*G(3,3)+G(1,1)*G(2,2)-G03_2-G02_2-G01_2+G(0,0)*G(3,3)+G(0,0)*G(2,2)+G(0,0)*G(1,1);
  double D = G13_2*G(2,2)-2.0*G(1,2)*G(1,3)*G(2,3)+G12_2*G(3,3)+G(1,1)*G23_2-G(1,1)*G(2,2)*G(3,3)+G03_2*G(2,2)+G03_2*G(1,1)-2.0*G(0,2)*G(0,3)*G(2,3)+G02_2*G(3,3)+G02_2*G(1,1)-2.0*G(0,1)*G(0,3)*G(1,3)-2.0*G(0,1)*G(0,2)*G(1,2)+G01_2*G(3,3)+G01_2*G(2,2)+G(0,0)*G23_2-G(0,0)*G(2,2)*G(3,3)+G(0,0)*G13_2+G(0,0)*G12_2-G(0,0)*G(1,1)*G(3,3)-G(0,0)*G(1,1)*G(2,2);
  double E = G03_2*G12_2-G03_2*G(1,1)*G(2,2)-2.0*G(0,2)*G(0,3)*G(1,2)*G(1,3)+2.0*G(0,2)*G(0,3)*G(1,1)*G(2,3)+G02_2*G13_2-G02_2*G(1,1)*G(3,3)+2.0*G(0,1)*G(0,3)*G(1,3)*G(2,2)-2.0*G(0,1)*G(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G(0,2)*G(1,3)*G(2,3)+2.0*G(0,1)*G(0,2)*G(1,2)*G(3,3)+G01_2*G23_2-G01_2*G(2,2)*G(3,3)-G(0,0)*G13_2*G(2,2)+2.0*G(0,0)*G(1,2)*G(1,3)*G(2,3)-G(0,0)*G12_2*G(3,3)-G(0,0)*G(1,1)*G23_2+G(0,0)*G(1,1)*G(2,2)*G(3,3);

  double B_pw2 = B*B;
  double B_pw3 = B_pw2*B;
  double B_pw4 = B_pw3*B;
  double alpha = -0.375*B_pw2+C;
  double beta = B_pw3/8.0-B*C/2.0+D;
  double gamma = -0.01171875*B_pw4+B_pw2*C/16.0-B*D/4.0+E;
  double alpha_pw2 = alpha*alpha;
  double alpha_pw3 = alpha_pw2*alpha;
  double p = -alpha_pw2/12.0-gamma;
  double q = -alpha_pw3/108.0+alpha*gamma/3.0-pow(beta,2.0)/8.0;
  double helper1 = -pow(p,3.0)/27.0;
  double theta2 = pow( helper1, (1.0/3.0) );
  double theta1 = sqrt(theta2) * cos( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) );
  double y = -(5.0/6.0)*alpha - ( (1.0/3.0) * p * theta1 - theta1 * theta2) / theta2;
  double w = sqrt(alpha+2.0*y);
  
  //we currently disable the computation of all other roots, they are not used
  //roots[0] = -B/4.0 + 0.5*w + 0.5*sqrt(-3.0*alpha-2.0*y-2.0*beta/w);
  //roots[1] = -B/4.0 + 0.5*w - 0.5*sqrt(-3.0*alpha-2.0*y-2.0*beta/w);
  double temp1 = -B/4.0 - 0.5*w;
  double temp2 = 0.5*sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  //roots[2] = -B/4.0 - 0.5*w + 0.5*sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  //roots[3] = -B/4.0 - 0.5*w - 0.5*sqrt(-3.0*alpha-2.0*y+2.0*beta/w); //this is the smallest one!
  roots[2] = temp1 + temp2;
  roots[3] = temp1 - temp2;
}

double
opengv::relative_pose::modules::ge::getCost(
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
    const cayley_t & cayley,
    int step )
{
  Eigen::Vector4d roots;
  getEV(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,roots);
  
  double cost = 0.0;
  
  if( step == 0 )
    cost = roots[2];
  if( step == 1 )
    cost = roots[3];
  
  return cost;
}

  /////////////////////////// Code for analytical computation of jacobian (for smallest root) ///////////////////////
  /*
  Eigen::Matrix4d G_jac1 = Eigen::Matrix4d::Zero();
  Eigen::Matrix4d G_jac2 = Eigen::Matrix4d::Zero();
  Eigen::Matrix4d G_jac3 = Eigen::Matrix4d::Zero();

  Eigen::Matrix4d G = composeGwithJacobians(
      xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,G_jac1,G_jac2,G_jac3);

  double B = -G(3,3)-G(2,2)-G(1,1)-G(0,0);
  double B_jac1 = -G_jac1(3,3)-G_jac1(2,2)-G_jac1(1,1)-G_jac1(0,0);
  double B_jac2 = -G_jac2(3,3)-G_jac2(2,2)-G_jac2(1,1)-G_jac2(0,0);
  double B_jac3 = -G_jac3(3,3)-G_jac3(2,2)-G_jac3(1,1)-G_jac3(0,0);
  
  double C = -pow(G(2,3),2)+G(2,2)*G(3,3)-pow(G(1,3),2)-pow(G(1,2),2)+G(1,1)*G(3,3)+G(1,1)*G(2,2)-pow(G(0,3),2)-pow(G(0,2),2)-pow(G(0,1),2)+G(0,0)*G(3,3)+G(0,0)*G(2,2)+G(0,0)*G(1,1);
  double C_jac1 = -2.0*G(2,3)*G_jac1(2,3)+G_jac1(2,2)*G(3,3)+G(2,2)*G_jac1(3,3)-2.0*G(1,3)*G_jac1(1,3)-2.0*G(1,2)*G_jac1(1,2)+G_jac1(1,1)*G(3,3)+G(1,1)*G_jac1(3,3)+G_jac1(1,1)*G(2,2)+G(1,1)*G_jac1(2,2)-2.0*G(0,3)*G_jac1(0,3)-2.0*G(0,2)*G_jac1(0,2)-2.0*G(0,1)*G_jac1(0,1)+G_jac1(0,0)*G(3,3)+G(0,0)*G_jac1(3,3)+G_jac1(0,0)*G(2,2)+G(0,0)*G_jac1(2,2)+G_jac1(0,0)*G(1,1)+G(0,0)*G_jac1(1,1);
  double C_jac2 = -2.0*G(2,3)*G_jac2(2,3)+G_jac2(2,2)*G(3,3)+G(2,2)*G_jac2(3,3)-2.0*G(1,3)*G_jac2(1,3)-2.0*G(1,2)*G_jac2(1,2)+G_jac2(1,1)*G(3,3)+G(1,1)*G_jac2(3,3)+G_jac2(1,1)*G(2,2)+G(1,1)*G_jac2(2,2)-2.0*G(0,3)*G_jac2(0,3)-2.0*G(0,2)*G_jac2(0,2)-2.0*G(0,1)*G_jac2(0,1)+G_jac2(0,0)*G(3,3)+G(0,0)*G_jac2(3,3)+G_jac2(0,0)*G(2,2)+G(0,0)*G_jac2(2,2)+G_jac2(0,0)*G(1,1)+G(0,0)*G_jac2(1,1);
  double C_jac3 = -2.0*G(2,3)*G_jac3(2,3)+G_jac3(2,2)*G(3,3)+G(2,2)*G_jac3(3,3)-2.0*G(1,3)*G_jac3(1,3)-2.0*G(1,2)*G_jac3(1,2)+G_jac3(1,1)*G(3,3)+G(1,1)*G_jac3(3,3)+G_jac3(1,1)*G(2,2)+G(1,1)*G_jac3(2,2)-2.0*G(0,3)*G_jac3(0,3)-2.0*G(0,2)*G_jac3(0,2)-2.0*G(0,1)*G_jac3(0,1)+G_jac3(0,0)*G(3,3)+G(0,0)*G_jac3(3,3)+G_jac3(0,0)*G(2,2)+G(0,0)*G_jac3(2,2)+G_jac3(0,0)*G(1,1)+G(0,0)*G_jac3(1,1);
  
  double D = pow(G(1,3),2)*G(2,2)-2.0*G(1,2)*G(1,3)*G(2,3)+pow(G(1,2),2)*G(3,3)+G(1,1)*pow(G(2,3),2)-G(1,1)*G(2,2)*G(3,3)+pow(G(0,3),2)*G(2,2)+pow(G(0,3),2)*G(1,1)-2.0*G(0,2)*G(0,3)*G(2,3)+pow(G(0,2),2)*G(3,3)+pow(G(0,2),2)*G(1,1)-2.0*G(0,1)*G(0,3)*G(1,3)-2.0*G(0,1)*G(0,2)*G(1,2)+pow(G(0,1),2)*G(3,3)+pow(G(0,1),2)*G(2,2)+G(0,0)*pow(G(2,3),2)-G(0,0)*G(2,2)*G(3,3)+G(0,0)*pow(G(1,3),2)+G(0,0)*pow(G(1,2),2)-G(0,0)*G(1,1)*G(3,3)-G(0,0)*G(1,1)*G(2,2);
  double D_jac1 = 2.0*G(1,3)*G_jac1(1,3)*G(2,2)+pow(G(1,3),2)*G_jac1(2,2)-2.0*G_jac1(1,2)*G(1,3)*G(2,3)-2.0*G(1,2)*G_jac1(1,3)*G(2,3)-2.0*G(1,2)*G(1,3)*G_jac1(2,3)+2.0*G(1,2)*G_jac1(1,2)*G(3,3)+pow(G(1,2),2)*G_jac1(3,3)+G_jac1(1,1)*pow(G(2,3),2)+G(1,1)*2.0*G(2,3)*G_jac1(2,3)-G_jac1(1,1)*G(2,2)*G(3,3)-G(1,1)*G_jac1(2,2)*G(3,3)-G(1,1)*G(2,2)*G_jac1(3,3)+2.0*G(0,3)*G_jac1(0,3)*G(2,2)+pow(G(0,3),2)*G_jac1(2,2)+2.0*G(0,3)*G_jac1(0,3)*G(1,1)+pow(G(0,3),2)*G_jac1(1,1)-2.0*G_jac1(0,2)*G(0,3)*G(2,3)-2.0*G(0,2)*G_jac1(0,3)*G(2,3)-2.0*G(0,2)*G(0,3)*G_jac1(2,3)+2.0*G(0,2)*G_jac1(0,2)*G(3,3)+pow(G(0,2),2)*G_jac1(3,3)+2.0*G(0,2)*G_jac1(0,2)*G(1,1)+pow(G(0,2),2)*G_jac1(1,1)-2.0*G_jac1(0,1)*G(0,3)*G(1,3)-2.0*G(0,1)*G_jac1(0,3)*G(1,3)-2.0*G(0,1)*G(0,3)*G_jac1(1,3)-2.0*G_jac1(0,1)*G(0,2)*G(1,2)-2.0*G(0,1)*G_jac1(0,2)*G(1,2)-2.0*G(0,1)*G(0,2)*G_jac1(1,2)+2.0*G(0,1)*G_jac1(0,1)*G(3,3)+pow(G(0,1),2)*G_jac1(3,3)+2.0*G(0,1)*G_jac1(0,1)*G(2,2)+pow(G(0,1),2)*G_jac1(2,2)+G_jac1(0,0)*pow(G(2,3),2)+G(0,0)*2.0*G(2,3)*G_jac1(2,3)-G_jac1(0,0)*G(2,2)*G(3,3)-G(0,0)*G_jac1(2,2)*G(3,3)-G(0,0)*G(2,2)*G_jac1(3,3)+G_jac1(0,0)*pow(G(1,3),2)+G(0,0)*2.0*G(1,3)*G_jac1(1,3)+G_jac1(0,0)*pow(G(1,2),2)+G(0,0)*2.0*G(1,2)*G_jac1(1,2)-G_jac1(0,0)*G(1,1)*G(3,3)-G(0,0)*G_jac1(1,1)*G(3,3)-G(0,0)*G(1,1)*G_jac1(3,3)-G_jac1(0,0)*G(1,1)*G(2,2)-G(0,0)*G_jac1(1,1)*G(2,2)-G(0,0)*G(1,1)*G_jac1(2,2);
  double D_jac2 = 2.0*G(1,3)*G_jac2(1,3)*G(2,2)+pow(G(1,3),2)*G_jac2(2,2)-2.0*G_jac2(1,2)*G(1,3)*G(2,3)-2.0*G(1,2)*G_jac2(1,3)*G(2,3)-2.0*G(1,2)*G(1,3)*G_jac2(2,3)+2.0*G(1,2)*G_jac2(1,2)*G(3,3)+pow(G(1,2),2)*G_jac2(3,3)+G_jac2(1,1)*pow(G(2,3),2)+G(1,1)*2.0*G(2,3)*G_jac2(2,3)-G_jac2(1,1)*G(2,2)*G(3,3)-G(1,1)*G_jac2(2,2)*G(3,3)-G(1,1)*G(2,2)*G_jac2(3,3)+2.0*G(0,3)*G_jac2(0,3)*G(2,2)+pow(G(0,3),2)*G_jac2(2,2)+2.0*G(0,3)*G_jac2(0,3)*G(1,1)+pow(G(0,3),2)*G_jac2(1,1)-2.0*G_jac2(0,2)*G(0,3)*G(2,3)-2.0*G(0,2)*G_jac2(0,3)*G(2,3)-2.0*G(0,2)*G(0,3)*G_jac2(2,3)+2.0*G(0,2)*G_jac2(0,2)*G(3,3)+pow(G(0,2),2)*G_jac2(3,3)+2.0*G(0,2)*G_jac2(0,2)*G(1,1)+pow(G(0,2),2)*G_jac2(1,1)-2.0*G_jac2(0,1)*G(0,3)*G(1,3)-2.0*G(0,1)*G_jac2(0,3)*G(1,3)-2.0*G(0,1)*G(0,3)*G_jac2(1,3)-2.0*G_jac2(0,1)*G(0,2)*G(1,2)-2.0*G(0,1)*G_jac2(0,2)*G(1,2)-2.0*G(0,1)*G(0,2)*G_jac2(1,2)+2.0*G(0,1)*G_jac2(0,1)*G(3,3)+pow(G(0,1),2)*G_jac2(3,3)+2.0*G(0,1)*G_jac2(0,1)*G(2,2)+pow(G(0,1),2)*G_jac2(2,2)+G_jac2(0,0)*pow(G(2,3),2)+G(0,0)*2.0*G(2,3)*G_jac2(2,3)-G_jac2(0,0)*G(2,2)*G(3,3)-G(0,0)*G_jac2(2,2)*G(3,3)-G(0,0)*G(2,2)*G_jac2(3,3)+G_jac2(0,0)*pow(G(1,3),2)+G(0,0)*2.0*G(1,3)*G_jac2(1,3)+G_jac2(0,0)*pow(G(1,2),2)+G(0,0)*2.0*G(1,2)*G_jac2(1,2)-G_jac2(0,0)*G(1,1)*G(3,3)-G(0,0)*G_jac2(1,1)*G(3,3)-G(0,0)*G(1,1)*G_jac2(3,3)-G_jac2(0,0)*G(1,1)*G(2,2)-G(0,0)*G_jac2(1,1)*G(2,2)-G(0,0)*G(1,1)*G_jac2(2,2);
  double D_jac3 = 2.0*G(1,3)*G_jac3(1,3)*G(2,2)+pow(G(1,3),2)*G_jac3(2,2)-2.0*G_jac3(1,2)*G(1,3)*G(2,3)-2.0*G(1,2)*G_jac3(1,3)*G(2,3)-2.0*G(1,2)*G(1,3)*G_jac3(2,3)+2.0*G(1,2)*G_jac3(1,2)*G(3,3)+pow(G(1,2),2)*G_jac3(3,3)+G_jac3(1,1)*pow(G(2,3),2)+G(1,1)*2.0*G(2,3)*G_jac3(2,3)-G_jac3(1,1)*G(2,2)*G(3,3)-G(1,1)*G_jac3(2,2)*G(3,3)-G(1,1)*G(2,2)*G_jac3(3,3)+2.0*G(0,3)*G_jac3(0,3)*G(2,2)+pow(G(0,3),2)*G_jac3(2,2)+2.0*G(0,3)*G_jac3(0,3)*G(1,1)+pow(G(0,3),2)*G_jac3(1,1)-2.0*G_jac3(0,2)*G(0,3)*G(2,3)-2.0*G(0,2)*G_jac3(0,3)*G(2,3)-2.0*G(0,2)*G(0,3)*G_jac3(2,3)+2.0*G(0,2)*G_jac3(0,2)*G(3,3)+pow(G(0,2),2)*G_jac3(3,3)+2.0*G(0,2)*G_jac3(0,2)*G(1,1)+pow(G(0,2),2)*G_jac3(1,1)-2.0*G_jac3(0,1)*G(0,3)*G(1,3)-2.0*G(0,1)*G_jac3(0,3)*G(1,3)-2.0*G(0,1)*G(0,3)*G_jac3(1,3)-2.0*G_jac3(0,1)*G(0,2)*G(1,2)-2.0*G(0,1)*G_jac3(0,2)*G(1,2)-2.0*G(0,1)*G(0,2)*G_jac3(1,2)+2.0*G(0,1)*G_jac3(0,1)*G(3,3)+pow(G(0,1),2)*G_jac3(3,3)+2.0*G(0,1)*G_jac3(0,1)*G(2,2)+pow(G(0,1),2)*G_jac3(2,2)+G_jac3(0,0)*pow(G(2,3),2)+G(0,0)*2.0*G(2,3)*G_jac3(2,3)-G_jac3(0,0)*G(2,2)*G(3,3)-G(0,0)*G_jac3(2,2)*G(3,3)-G(0,0)*G(2,2)*G_jac3(3,3)+G_jac3(0,0)*pow(G(1,3),2)+G(0,0)*2.0*G(1,3)*G_jac3(1,3)+G_jac3(0,0)*pow(G(1,2),2)+G(0,0)*2.0*G(1,2)*G_jac3(1,2)-G_jac3(0,0)*G(1,1)*G(3,3)-G(0,0)*G_jac3(1,1)*G(3,3)-G(0,0)*G(1,1)*G_jac3(3,3)-G_jac3(0,0)*G(1,1)*G(2,2)-G(0,0)*G_jac3(1,1)*G(2,2)-G(0,0)*G(1,1)*G_jac3(2,2);
  
  double E = pow(G(0,3),2)*pow(G(1,2),2)-pow(G(0,3),2)*G(1,1)*G(2,2)-2.0*G(0,2)*G(0,3)*G(1,2)*G(1,3)+2.0*G(0,2)*G(0,3)*G(1,1)*G(2,3)+pow(G(0,2),2)*pow(G(1,3),2)-pow(G(0,2),2)*G(1,1)*G(3,3)+2.0*G(0,1)*G(0,3)*G(1,3)*G(2,2)-2.0*G(0,1)*G(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G(0,2)*G(1,3)*G(2,3)+2.0*G(0,1)*G(0,2)*G(1,2)*G(3,3)+pow(G(0,1),2)*pow(G(2,3),2)-pow(G(0,1),2)*G(2,2)*G(3,3)-G(0,0)*pow(G(1,3),2)*G(2,2)+2.0*G(0,0)*G(1,2)*G(1,3)*G(2,3)-G(0,0)*pow(G(1,2),2)*G(3,3)-G(0,0)*G(1,1)*pow(G(2,3),2)+G(0,0)*G(1,1)*G(2,2)*G(3,3);
  double E_jac1 = 2.0*G(0,3)*G_jac1(0,3)*pow(G(1,2),2)+pow(G(0,3),2)*2.0*G(1,2)*G_jac1(1,2)-2.0*G(0,3)*G_jac1(0,3)*G(1,1)*G(2,2)-pow(G(0,3),2)*G_jac1(1,1)*G(2,2)-pow(G(0,3),2)*G(1,1)*G_jac1(2,2)-2.0*G_jac1(0,2)*G(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G_jac1(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G_jac1(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G(1,2)*G_jac1(1,3)+2.0*G_jac1(0,2)*G(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G_jac1(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G_jac1(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G(1,1)*G_jac1(2,3) +2.0*G(0,2)*G_jac1(0,2)*pow(G(1,3),2)+pow(G(0,2),2)*2.0*G(1,3)*G_jac1(1,3)-2.0*G(0,2)*G_jac1(0,2)*G(1,1)*G(3,3)-pow(G(0,2),2)*G_jac1(1,1)*G(3,3)-pow(G(0,2),2)*G(1,1)*G_jac1(3,3)+2.0*G_jac1(0,1)*G(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G_jac1(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G_jac1(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G(1,3)*G_jac1(2,2)-2.0*G_jac1(0,1)*G(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G_jac1(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G_jac1(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G(1,2)*G_jac1(2,3)-2.0*G_jac1(0,1)*G(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G_jac1(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G_jac1(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G(1,3)*G_jac1(2,3)+2.0*G_jac1(0,1)*G(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G_jac1(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G_jac1(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G(1,2)*G_jac1(3,3)+2.0*G(0,1)*G_jac1(0,1)*pow(G(2,3),2)+pow(G(0,1),2)*2.0*G(2,3)*G_jac1(2,3)-2.0*G(0,1)*G_jac1(0,1)*G(2,2)*G(3,3)-pow(G(0,1),2)*G_jac1(2,2)*G(3,3)-pow(G(0,1),2)*G(2,2)*G_jac1(3,3)-G_jac1(0,0)*pow(G(1,3),2)*G(2,2)-G(0,0)*2.0*G(1,3)*G_jac1(1,3)*G(2,2)-G(0,0)*pow(G(1,3),2)*G_jac1(2,2)+2.0*G_jac1(0,0)*G(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G_jac1(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G_jac1(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G(1,3)*G_jac1(2,3)-G_jac1(0,0)*pow(G(1,2),2)*G(3,3)-G(0,0)*2.0*G(1,2)*G_jac1(1,2)*G(3,3)-G(0,0)*pow(G(1,2),2)*G_jac1(3,3)-G_jac1(0,0)*G(1,1)*pow(G(2,3),2)-G(0,0)*G_jac1(1,1)*pow(G(2,3),2)-G(0,0)*G(1,1)*2.0*G(2,3)*G_jac1(2,3)+G_jac1(0,0)*G(1,1)*G(2,2)*G(3,3)+G(0,0)*G_jac1(1,1)*G(2,2)*G(3,3)+G(0,0)*G(1,1)*G_jac1(2,2)*G(3,3)+G(0,0)*G(1,1)*G(2,2)*G_jac1(3,3);
  double E_jac2 = 2.0*G(0,3)*G_jac2(0,3)*pow(G(1,2),2)+pow(G(0,3),2)*2.0*G(1,2)*G_jac2(1,2)-2.0*G(0,3)*G_jac2(0,3)*G(1,1)*G(2,2)-pow(G(0,3),2)*G_jac2(1,1)*G(2,2)-pow(G(0,3),2)*G(1,1)*G_jac2(2,2)-2.0*G_jac2(0,2)*G(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G_jac2(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G_jac2(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G(1,2)*G_jac2(1,3)+2.0*G_jac2(0,2)*G(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G_jac2(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G_jac2(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G(1,1)*G_jac2(2,3) +2.0*G(0,2)*G_jac2(0,2)*pow(G(1,3),2)+pow(G(0,2),2)*2.0*G(1,3)*G_jac2(1,3)-2.0*G(0,2)*G_jac2(0,2)*G(1,1)*G(3,3)-pow(G(0,2),2)*G_jac2(1,1)*G(3,3)-pow(G(0,2),2)*G(1,1)*G_jac2(3,3)+2.0*G_jac2(0,1)*G(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G_jac2(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G_jac2(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G(1,3)*G_jac2(2,2)-2.0*G_jac2(0,1)*G(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G_jac2(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G_jac2(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G(1,2)*G_jac2(2,3)-2.0*G_jac2(0,1)*G(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G_jac2(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G_jac2(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G(1,3)*G_jac2(2,3)+2.0*G_jac2(0,1)*G(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G_jac2(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G_jac2(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G(1,2)*G_jac2(3,3)+2.0*G(0,1)*G_jac2(0,1)*pow(G(2,3),2)+pow(G(0,1),2)*2.0*G(2,3)*G_jac2(2,3)-2.0*G(0,1)*G_jac2(0,1)*G(2,2)*G(3,3)-pow(G(0,1),2)*G_jac2(2,2)*G(3,3)-pow(G(0,1),2)*G(2,2)*G_jac2(3,3)-G_jac2(0,0)*pow(G(1,3),2)*G(2,2)-G(0,0)*2.0*G(1,3)*G_jac2(1,3)*G(2,2)-G(0,0)*pow(G(1,3),2)*G_jac2(2,2)+2.0*G_jac2(0,0)*G(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G_jac2(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G_jac2(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G(1,3)*G_jac2(2,3)-G_jac2(0,0)*pow(G(1,2),2)*G(3,3)-G(0,0)*2.0*G(1,2)*G_jac2(1,2)*G(3,3)-G(0,0)*pow(G(1,2),2)*G_jac2(3,3)-G_jac2(0,0)*G(1,1)*pow(G(2,3),2)-G(0,0)*G_jac2(1,1)*pow(G(2,3),2)-G(0,0)*G(1,1)*2.0*G(2,3)*G_jac2(2,3)+G_jac2(0,0)*G(1,1)*G(2,2)*G(3,3)+G(0,0)*G_jac2(1,1)*G(2,2)*G(3,3)+G(0,0)*G(1,1)*G_jac2(2,2)*G(3,3)+G(0,0)*G(1,1)*G(2,2)*G_jac2(3,3);
  double E_jac3 = 2.0*G(0,3)*G_jac3(0,3)*pow(G(1,2),2)+pow(G(0,3),2)*2.0*G(1,2)*G_jac3(1,2)-2.0*G(0,3)*G_jac3(0,3)*G(1,1)*G(2,2)-pow(G(0,3),2)*G_jac3(1,1)*G(2,2)-pow(G(0,3),2)*G(1,1)*G_jac3(2,2)-2.0*G_jac3(0,2)*G(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G_jac3(0,3)*G(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G_jac3(1,2)*G(1,3)-2.0*G(0,2)*G(0,3)*G(1,2)*G_jac3(1,3)+2.0*G_jac3(0,2)*G(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G_jac3(0,3)*G(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G_jac3(1,1)*G(2,3)+2.0*G(0,2)*G(0,3)*G(1,1)*G_jac3(2,3) +2.0*G(0,2)*G_jac3(0,2)*pow(G(1,3),2)+pow(G(0,2),2)*2.0*G(1,3)*G_jac3(1,3)-2.0*G(0,2)*G_jac3(0,2)*G(1,1)*G(3,3)-pow(G(0,2),2)*G_jac3(1,1)*G(3,3)-pow(G(0,2),2)*G(1,1)*G_jac3(3,3)+2.0*G_jac3(0,1)*G(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G_jac3(0,3)*G(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G_jac3(1,3)*G(2,2)+2.0*G(0,1)*G(0,3)*G(1,3)*G_jac3(2,2)-2.0*G_jac3(0,1)*G(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G_jac3(0,3)*G(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G_jac3(1,2)*G(2,3)-2.0*G(0,1)*G(0,3)*G(1,2)*G_jac3(2,3)-2.0*G_jac3(0,1)*G(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G_jac3(0,2)*G(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G_jac3(1,3)*G(2,3)-2.0*G(0,1)*G(0,2)*G(1,3)*G_jac3(2,3)+2.0*G_jac3(0,1)*G(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G_jac3(0,2)*G(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G_jac3(1,2)*G(3,3)+2.0*G(0,1)*G(0,2)*G(1,2)*G_jac3(3,3)+2.0*G(0,1)*G_jac3(0,1)*pow(G(2,3),2)+pow(G(0,1),2)*2.0*G(2,3)*G_jac3(2,3)-2.0*G(0,1)*G_jac3(0,1)*G(2,2)*G(3,3)-pow(G(0,1),2)*G_jac3(2,2)*G(3,3)-pow(G(0,1),2)*G(2,2)*G_jac3(3,3)-G_jac3(0,0)*pow(G(1,3),2)*G(2,2)-G(0,0)*2.0*G(1,3)*G_jac3(1,3)*G(2,2)-G(0,0)*pow(G(1,3),2)*G_jac3(2,2)+2.0*G_jac3(0,0)*G(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G_jac3(1,2)*G(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G_jac3(1,3)*G(2,3)+2.0*G(0,0)*G(1,2)*G(1,3)*G_jac3(2,3)-G_jac3(0,0)*pow(G(1,2),2)*G(3,3)-G(0,0)*2.0*G(1,2)*G_jac3(1,2)*G(3,3)-G(0,0)*pow(G(1,2),2)*G_jac3(3,3)-G_jac3(0,0)*G(1,1)*pow(G(2,3),2)-G(0,0)*G_jac3(1,1)*pow(G(2,3),2)-G(0,0)*G(1,1)*2.0*G(2,3)*G_jac3(2,3)+G_jac3(0,0)*G(1,1)*G(2,2)*G(3,3)+G(0,0)*G_jac3(1,1)*G(2,2)*G(3,3)+G(0,0)*G(1,1)*G_jac3(2,2)*G(3,3)+G(0,0)*G(1,1)*G(2,2)*G_jac3(3,3);

  double B_pw2 = B*B;
  double B_pw2_jac1 = 2.0*B*B_jac1;
  double B_pw2_jac2 = 2.0*B*B_jac2;
  double B_pw2_jac3 = 2.0*B*B_jac3;
  double B_pw3 = B_pw2*B;
  double B_pw3_jac1 = 3.0*B_pw2*B_jac1;
  double B_pw3_jac2 = 3.0*B_pw2*B_jac2;
  double B_pw3_jac3 = 3.0*B_pw2*B_jac3;
  double B_pw4 = B_pw3*B;
  double B_pw4_jac1 = 4.0*B_pw3*B_jac1;
  double B_pw4_jac2 = 4.0*B_pw3*B_jac2;
  double B_pw4_jac3 = 4.0*B_pw3*B_jac3;

  double alpha = -3.0*B_pw2/8.0+C;
  double alpha_jac1 = -3.0*B_pw2_jac1/8.0+C_jac1;
  double alpha_jac2 = -3.0*B_pw2_jac2/8.0+C_jac2;
  double alpha_jac3 = -3.0*B_pw2_jac3/8.0+C_jac3;
  
  double beta = B_pw3/8.0-B*C/2.0+D;
  double beta_jac1 = B_pw3_jac1/8.0-(B_jac1*C+B*C_jac1)/2.0+D_jac1;
  double beta_jac2 = B_pw3_jac2/8.0-(B_jac2*C+B*C_jac2)/2.0+D_jac2;
  double beta_jac3 = B_pw3_jac3/8.0-(B_jac3*C+B*C_jac3)/2.0+D_jac3;
  
  double gamma = -3.0*B_pw4/256.0+B_pw2*C/16.0-B*D/4.0+E;
  double gamma_jac1 = -3.0*B_pw4_jac1/256.0+(B_pw2_jac1*C+B_pw2*C_jac1)/16.0-(B_jac1*D+B*D_jac1)/4.0+E_jac1;
  double gamma_jac2 = -3.0*B_pw4_jac2/256.0+(B_pw2_jac2*C+B_pw2*C_jac2)/16.0-(B_jac2*D+B*D_jac2)/4.0+E_jac2;
  double gamma_jac3 = -3.0*B_pw4_jac3/256.0+(B_pw2_jac3*C+B_pw2*C_jac3)/16.0-(B_jac3*D+B*D_jac3)/4.0+E_jac3;

  double alpha_pw2 = alpha*alpha;
  double alpha_pw2_jac1 = 2.0*alpha*alpha_jac1;
  double alpha_pw2_jac2 = 2.0*alpha*alpha_jac2;
  double alpha_pw2_jac3 = 2.0*alpha*alpha_jac3;
  double alpha_pw3 = alpha_pw2*alpha;
  double alpha_pw3_jac1 = 3.0*alpha_pw2*alpha_jac1;
  double alpha_pw3_jac2 = 3.0*alpha_pw2*alpha_jac2;
  double alpha_pw3_jac3 = 3.0*alpha_pw2*alpha_jac3;

  double p = -alpha_pw2/12.0-gamma;
  double p_jac1 = -alpha_pw2_jac1/12.0-gamma_jac1;
  double p_jac2 = -alpha_pw2_jac2/12.0-gamma_jac2;
  double p_jac3 = -alpha_pw2_jac3/12.0-gamma_jac3;
  
  double q = -alpha_pw3/108.0+alpha*gamma/3.0-pow(beta,2.0)/8.0;
  double q_jac1 = -alpha_pw3_jac1/108.0+(alpha_jac1*gamma+alpha*gamma_jac1)/3.0-2.0*beta*beta_jac1/8.0;
  double q_jac2 = -alpha_pw3_jac2/108.0+(alpha_jac2*gamma+alpha*gamma_jac2)/3.0-2.0*beta*beta_jac2/8.0;
  double q_jac3 = -alpha_pw3_jac3/108.0+(alpha_jac3*gamma+alpha*gamma_jac3)/3.0-2.0*beta*beta_jac3/8.0;
  
  double helper1 = -pow(p,3.0)/27.0;
  double helper1_jac1 = -3.0*pow(p,2.0)*p_jac1/27.0;
  double helper1_jac2 = -3.0*pow(p,2.0)*p_jac2/27.0;
  double helper1_jac3 = -3.0*pow(p,2.0)*p_jac3/27.0;
  
  double theta1 = pow( helper1, (1.0/6.0) ) * cos( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) );
  double theta1_jac1 = (1.0/6.0)*pow( helper1, (-5.0/6.0) ) * helper1_jac1 * cos( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) - pow( helper1, (1.0/6.0) ) * sin( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) * ( 2.0 * helper1 * q_jac1 - q * helper1_jac1) / ( 12.0 * pow(helper1,1.5) * sqrt(1.0 - q / (4.0*helper1) ) ) ;
  double theta1_jac2 = (1.0/6.0)*pow( helper1, (-5.0/6.0) ) * helper1_jac2 * cos( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) - pow( helper1, (1.0/6.0) ) * sin( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) * ( 2.0 * helper1 * q_jac2 - q * helper1_jac2) / ( 12.0 * pow(helper1,1.5) * sqrt(1.0 - q / (4.0*helper1) ) ) ;
  double theta1_jac3 = (1.0/6.0)*pow( helper1, (-5.0/6.0) ) * helper1_jac3 * cos( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) - pow( helper1, (1.0/6.0) ) * sin( (1.0/3.0) * acos( (-q/2.0) / sqrt(helper1) ) ) * ( 2.0 * helper1 * q_jac3 - q * helper1_jac3) / ( 12.0 * pow(helper1,1.5) * sqrt(1.0 - q / (4.0*helper1) ) ) ;
  
  double theta2 = pow( helper1 , (1.0/3.0) );
  double theta2_jac1 = (1.0/3.0) * pow( helper1 , (-2.0/3.0) ) * helper1_jac1;
  double theta2_jac2 = (1.0/3.0) * pow( helper1 , (-2.0/3.0) ) * helper1_jac2;
  double theta2_jac3 = (1.0/3.0) * pow( helper1 , (-2.0/3.0) ) * helper1_jac3;
  
  double y = -(5.0/6.0)*alpha - ( (1.0/3.0) * p * theta1 - theta1 * theta2) / theta2;
  double y_jac1 = -(5.0/6.0)*alpha_jac1 - ( ( (1.0/3.0) * p_jac1 * theta1 + (1.0/3.0) * p * theta1_jac1 - theta1_jac1 * theta2 - theta1 * theta2_jac1 )*theta2 - ( (1.0/3.0) * p * theta1 - theta1 * theta2) * theta2_jac1 ) / pow(theta2,2.0);
  double y_jac2 = -(5.0/6.0)*alpha_jac2 - ( ( (1.0/3.0) * p_jac2 * theta1 + (1.0/3.0) * p * theta1_jac2 - theta1_jac2 * theta2 - theta1 * theta2_jac2 )*theta2 - ( (1.0/3.0) * p * theta1 - theta1 * theta2) * theta2_jac2 ) / pow(theta2,2.0);
  double y_jac3 = -(5.0/6.0)*alpha_jac3 - ( ( (1.0/3.0) * p_jac3 * theta1 + (1.0/3.0) * p * theta1_jac3 - theta1_jac3 * theta2 - theta1 * theta2_jac3 )*theta2 - ( (1.0/3.0) * p * theta1 - theta1 * theta2) * theta2_jac3 ) / pow(theta2,2.0);

  double w = sqrt(alpha+2.0*y);
  double w_jac1 = (alpha_jac1 + 2.0*y_jac1)/(2.0*sqrt(alpha+2.0*y));
  double w_jac2 = (alpha_jac2 + 2.0*y_jac2)/(2.0*sqrt(alpha+2.0*y));
  double w_jac3 = (alpha_jac3 + 2.0*y_jac3)/(2.0*sqrt(alpha+2.0*y));
  
  double smallestEV = -B/4.0 -0.5*w -0.5*sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  jacobian[0] = -B_jac1/4.0 -0.5*w_jac1 -0.25*( -3.0*alpha_jac1-2.0*y_jac1+2.0*(beta_jac1*w-beta*w_jac1)/pow(w,2.0) )/sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  jacobian[1] = -B_jac2/4.0 -0.5*w_jac2 -0.25*( -3.0*alpha_jac2-2.0*y_jac2+2.0*(beta_jac2*w-beta*w_jac2)/pow(w,2.0) )/sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  jacobian[2] = -B_jac3/4.0 -0.5*w_jac3 -0.25*( -3.0*alpha_jac3-2.0*y_jac3+2.0*(beta_jac3*w-beta*w_jac3)/pow(w,2.0) )/sqrt(-3.0*alpha-2.0*y+2.0*beta/w);
  
  return smallestEV;
  */
  /////////////////////////////////////////////////////// end of Method 1 ///////////////////////////////////////////////////////////////////////////////////
  

double
opengv::relative_pose::modules::ge::
    getCostWithJacobian(
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
    const cayley_t & cayley,
    Eigen::Matrix<double,1,3> & jacobian,
    int step )
{
  double eps = 0.00000001;
  double cost = getCost(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley,step);
  
  for( int j = 0; j < 3; j++ )
  {
    cayley_t cayley_j = cayley; cayley_j[j] += eps;
    double cost_j = getCost(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley_j,step);
    
    cayley_j = cayley; cayley_j[j] -= eps;
    double cost_jm = getCost(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley_j,step);
    jacobian(0,j) = (cost_j - cost_jm); //division by eps can be ommited
  }
  
  return cost;
}

void
opengv::relative_pose::modules::ge::getQuickJacobian(
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
    const cayley_t & cayley,
    double currentValue,
    Eigen::Matrix<double,1,3> & jacobian,
    int step )
{
  double eps = 0.00000001;
  
  for( int j = 0; j < 3; j++ )
  {
    cayley_t cayley_j = cayley; cayley_j[j] += eps;
    double cost_j = getCost(xxF,yyF,zzF,xyF,yzF,zxF,x1P,y1P,z1P,x2P,y2P,z2P,m11P,m12P,m22P,cayley_j,step);
    jacobian(0,j) = (cost_j - currentValue); //division by eps can be ommited
  }
}

Eigen::Matrix4d
opengv::relative_pose::modules::ge::composeG(
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
  const cayley_t & cayley)
{
  Eigen::Matrix4d G;
  rotation_t R = math::cayley2rot_reduced(cayley);

  //todo: Fill the matrix G using the precomputed summation terms
  Eigen::Vector3d xxFr1t = xxF*R.row(1).transpose();
  Eigen::Vector3d yyFr0t = yyF*R.row(0).transpose();
  Eigen::Vector3d zzFr0t = zzF*R.row(0).transpose();
  Eigen::Vector3d yzFr0t = yzF*R.row(0).transpose();
  Eigen::Vector3d xyFr1t = xyF*R.row(1).transpose();
  Eigen::Vector3d xyFr2t = xyF*R.row(2).transpose();
  Eigen::Vector3d zxFr1t = zxF*R.row(1).transpose();
  Eigen::Vector3d zxFr2t = zxF*R.row(2).transpose();
  
  double temp;
  temp =      R.row(2)*yyF*R.row(2).transpose();
  G(0,0)  = temp;
  temp = -2.0*R.row(2)*yzF*R.row(1).transpose();
  G(0,0) += temp;
  temp =      R.row(1)*zzF*R.row(1).transpose();
  G(0,0) += temp;

  temp =      R.row(2)*yzFr0t;
  G(0,1)  = temp;
  temp = -1.0*R.row(2)*xyFr2t;
  G(0,1) += temp;
  temp = -1.0*R.row(1)*zzFr0t;
  G(0,1) += temp;
  temp =      R.row(1)*zxFr2t;
  G(0,1) += temp;

  temp =      R.row(2)*xyFr1t;
  G(0,2)  = temp;
  temp = -1.0*R.row(2)*yyFr0t;
  G(0,2) += temp;
  temp = -1.0*R.row(1)*zxFr1t;
  G(0,2) += temp;
  temp =      R.row(1)*yzFr0t;
  G(0,2) += temp;

  temp =      R.row(0)*zzFr0t;
  G(1,1)  = temp;
  temp = -2.0*R.row(0)*zxFr2t;
  G(1,1) += temp;
  temp =      R.row(2)*xxF*R.row(2).transpose();
  G(1,1) += temp;

  temp =      R.row(0)*zxFr1t;
  G(1,2)  = temp;
  temp = -1.0*R.row(0)*yzFr0t;
  G(1,2) += temp;
  temp = -1.0*R.row(2)*xxFr1t;
  G(1,2) += temp;
  temp =      R.row(0)*xyFr2t;
  G(1,2) += temp;

  temp =      R.row(1)*xxFr1t;
  G(2,2)  = temp;
  temp = -2.0*R.row(0)*xyFr1t;
  G(2,2) += temp;
  temp =      R.row(0)*yyFr0t;
  G(2,2) += temp;

  G(1,0) = G(0,1);
  G(2,0) = G(0,2);
  G(2,1) = G(1,2);

  //the generalized terms:
  Eigen::Matrix<double,1,9> Rows;
  Rows.block<1,3>(0,0) = R.row(0);
  Rows.block<1,3>(0,3) = R.row(1);
  Rows.block<1,3>(0,6) = R.row(2);
  Eigen::Matrix<double,9,1> Rowst = Rows.transpose();
  
  Eigen::Matrix<double,9,1> Cols;
  Cols.block<3,1>(0,0) = R.col(0);
  Cols.block<3,1>(3,0) = R.col(1);
  Cols.block<3,1>(6,0) = R.col(2);
  
  Eigen::Vector3d x1PC = x1P*Cols;
  Eigen::Vector3d y1PC = y1P*Cols;
  Eigen::Vector3d z1PC = z1P*Cols;
  
  Eigen::Vector3d x2PR = x2P*Rowst;
  Eigen::Vector3d y2PR = y2P*Rowst;
  Eigen::Vector3d z2PR = z2P*Rowst;
  
  temp =      R.row(2)*y1PC;
  G(0,3)  = temp;
  temp =      R.row(2)*y2PR;
  G(0,3) += temp;
  temp = -1.0*R.row(1)*z1PC;
  G(0,3) += temp;
  temp = -1.0*R.row(1)*z2PR;
  G(0,3) += temp;
  
  temp =      R.row(0)*z1PC;
  G(1,3)  = temp;
  temp =      R.row(0)*z2PR;
  G(1,3) += temp;
  temp = -1.0*R.row(2)*x1PC;
  G(1,3) += temp;
  temp = -1.0*R.row(2)*x2PR;
  G(1,3) += temp;
  
  temp =      R.row(1)*x1PC;
  G(2,3)  = temp;
  temp =      R.row(1)*x2PR;
  G(2,3) += temp;
  temp = -1.0*R.row(0)*y1PC;
  G(2,3) += temp;
  temp = -1.0*R.row(0)*y2PR;
  G(2,3) += temp;
  
  temp = -1.0*Cols.transpose()*m11P*Cols;
  G(3,3)  = temp;
  temp = -1.0*Rows*m22P*Rowst;
  G(3,3) += temp;
  temp = -2.0*Rows*m12P*Cols;
  G(3,3) += temp;
  
  G(3,0) = G(0,3);
  G(3,1) = G(1,3);
  G(3,2) = G(2,3);

  return G;
}

Eigen::Matrix4d
opengv::relative_pose::modules::ge::composeGwithJacobians(
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
    const cayley_t & cayley,
    Eigen::Matrix4d & G_jac1,
    Eigen::Matrix4d & G_jac2,
    Eigen::Matrix4d & G_jac3 )
{
  rotation_t R = math::cayley2rot_reduced(cayley);
  rotation_t R_jac1;
  rotation_t R_jac2;
  rotation_t R_jac3;

  R_jac1(0,0) =  2.0*cayley[0];
  R_jac1(0,1) =  2.0*cayley[1];
  R_jac1(0,2) =  2.0*cayley[2];
  R_jac1(1,0) =  2.0*cayley[1];
  R_jac1(1,1) = -2.0*cayley[0];
  R_jac1(1,2) = -2.0;
  R_jac1(2,0) =  2.0*cayley[2];
  R_jac1(2,1) =  2.0;
  R_jac1(2,2) = -2.0*cayley[0];
  R_jac2(0,0) = -2.0*cayley[1];
  R_jac2(0,1) =  2.0*cayley[0];
  R_jac2(0,2) =  2.0;
  R_jac2(1,0) =  2.0*cayley[0];
  R_jac2(1,1) =  2.0*cayley[1];
  R_jac2(1,2) =  2.0*cayley[2];
  R_jac2(2,0) = -2.0;
  R_jac2(2,1) =  2.0*cayley[2];
  R_jac2(2,2) = -2.0*cayley[1];
  R_jac3(0,0) = -2.0*cayley[2];
  R_jac3(0,1) = -2.0;
  R_jac3(0,2) =  2.0*cayley[0];
  R_jac3(1,0) =  2.0;
  R_jac3(1,1) = -2.0*cayley[2];
  R_jac3(1,2) =  2.0*cayley[1];
  R_jac3(2,0) =  2.0*cayley[0];
  R_jac3(2,1) =  2.0*cayley[1];
  R_jac3(2,2) =  2.0*cayley[2];

  //Fill the matrix G using the precomputed summation terms. Plus Jacobian.
  Eigen::Matrix4d G;
  double temp;
  temp =      R.row(2)*yyF*R.row(2).transpose();
  G(0,0)  = temp;
  temp = -2.0*R.row(2)*yzF*R.row(1).transpose();
  G(0,0) += temp;
  temp =      R.row(1)*zzF*R.row(1).transpose();
  G(0,0) += temp;
  temp = 2.0*R_jac1.row(2)*yyF*R.row(2).transpose();
  G_jac1(0,0)  = temp;
  temp = -2.0*R_jac1.row(2)*yzF*R.row(1).transpose();
  G_jac1(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac1.row(1).transpose();
  G_jac1(0,0) += temp;
  temp = 2.0*R_jac1.row(1)*zzF*R.row(1).transpose();
  G_jac1(0,0) += temp;
  temp = 2.0*R_jac2.row(2)*yyF*R.row(2).transpose();
  G_jac2(0,0)  = temp;
  temp = -2.0*R_jac2.row(2)*yzF*R.row(1).transpose();
  G_jac2(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac2.row(1).transpose();
  G_jac2(0,0) += temp;
  temp = 2.0*R_jac2.row(1)*zzF*R.row(1).transpose();
  G_jac2(0,0) += temp;
  temp = 2.0*R_jac3.row(2)*yyF*R.row(2).transpose();
  G_jac3(0,0)  = temp;
  temp = -2.0*R_jac3.row(2)*yzF*R.row(1).transpose();
  G_jac3(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac3.row(1).transpose();
  G_jac3(0,0) += temp;
  temp = 2.0*R_jac3.row(1)*zzF*R.row(1).transpose();
  G_jac3(0,0) += temp;

  temp =      R.row(2)*yzF*R.row(0).transpose();
  G(0,1)  = temp;
  temp = -1.0*R.row(2)*xyF*R.row(2).transpose();
  G(0,1) += temp;
  temp = -1.0*R.row(1)*zzF*R.row(0).transpose();
  G(0,1) += temp;
  temp =      R.row(1)*zxF*R.row(2).transpose();
  G(0,1) += temp;
  temp = R_jac1.row(2)*yzF*R.row(0).transpose();
  G_jac1(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac1.row(0).transpose();
  G_jac1(0,1) += temp;
  temp = -2.0*R_jac1.row(2)*xyF*R.row(2).transpose();
  G_jac1(0,1) += temp;
  temp = -R_jac1.row(1)*zzF*R.row(0).transpose();
  G_jac1(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac1.row(0).transpose();
  G_jac1(0,1) += temp;
  temp = R_jac1.row(1)*zxF*R.row(2).transpose();
  G_jac1(0,1) += temp;
  temp = R.row(1)*zxF*R_jac1.row(2).transpose();
  G_jac1(0,1) += temp;
  temp = R_jac2.row(2)*yzF*R.row(0).transpose();
  G_jac2(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac2.row(0).transpose();
  G_jac2(0,1) += temp;
  temp = -2.0*R_jac2.row(2)*xyF*R.row(2).transpose();
  G_jac2(0,1) += temp;
  temp = -R_jac2.row(1)*zzF*R.row(0).transpose();
  G_jac2(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac2.row(0).transpose();
  G_jac2(0,1) += temp;
  temp = R_jac2.row(1)*zxF*R.row(2).transpose();
  G_jac2(0,1) += temp;
  temp = R.row(1)*zxF*R_jac2.row(2).transpose();
  G_jac2(0,1) += temp;
  temp = R_jac3.row(2)*yzF*R.row(0).transpose();
  G_jac3(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac3.row(0).transpose();
  G_jac3(0,1) += temp;
  temp = -2.0*R_jac3.row(2)*xyF*R.row(2).transpose();
  G_jac3(0,1) += temp;
  temp = -R_jac3.row(1)*zzF*R.row(0).transpose();
  G_jac3(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac3.row(0).transpose();
  G_jac3(0,1) += temp;
  temp = R_jac3.row(1)*zxF*R.row(2).transpose();
  G_jac3(0,1) += temp;
  temp = R.row(1)*zxF*R_jac3.row(2).transpose();
  G_jac3(0,1) += temp;

  temp =      R.row(2)*xyF*R.row(1).transpose();
  G(0,2)  = temp;
  temp = -1.0*R.row(2)*yyF*R.row(0).transpose();
  G(0,2) += temp;
  temp = -1.0*R.row(1)*zxF*R.row(1).transpose();
  G(0,2) += temp;
  temp =      R.row(1)*yzF*R.row(0).transpose();
  G(0,2) += temp;
  temp = R_jac1.row(2)*xyF*R.row(1).transpose();
  G_jac1(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac1.row(1).transpose();
  G_jac1(0,2) += temp;
  temp = -R_jac1.row(2)*yyF*R.row(0).transpose();
  G_jac1(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac1.row(0).transpose();
  G_jac1(0,2) += temp;
  temp = -2.0*R_jac1.row(1)*zxF*R.row(1).transpose();
  G_jac1(0,2) += temp;
  temp = R_jac1.row(1)*yzF*R.row(0).transpose();
  G_jac1(0,2) += temp;
  temp = R.row(1)*yzF*R_jac1.row(0).transpose();
  G_jac1(0,2) += temp;
  temp = R_jac2.row(2)*xyF*R.row(1).transpose();
  G_jac2(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac2.row(1).transpose();
  G_jac2(0,2) += temp;
  temp = -R_jac2.row(2)*yyF*R.row(0).transpose();
  G_jac2(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac2.row(0).transpose();
  G_jac2(0,2) += temp;
  temp = -2.0*R_jac2.row(1)*zxF*R.row(1).transpose();
  G_jac2(0,2) += temp;
  temp = R_jac2.row(1)*yzF*R.row(0).transpose();
  G_jac2(0,2) += temp;
  temp = R.row(1)*yzF*R_jac2.row(0).transpose();
  G_jac2(0,2) += temp;
  temp = R_jac3.row(2)*xyF*R.row(1).transpose();
  G_jac3(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac3.row(1).transpose();
  G_jac3(0,2) += temp;
  temp = -R_jac3.row(2)*yyF*R.row(0).transpose();
  G_jac3(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac3.row(0).transpose();
  G_jac3(0,2) += temp;
  temp = -2.0*R_jac3.row(1)*zxF*R.row(1).transpose();
  G_jac3(0,2) += temp;
  temp = R_jac3.row(1)*yzF*R.row(0).transpose();
  G_jac3(0,2) += temp;
  temp = R.row(1)*yzF*R_jac3.row(0).transpose();
  G_jac3(0,2) += temp;

  temp =      R.row(0)*zzF*R.row(0).transpose();
  G(1,1)  = temp;
  temp = -2.0*R.row(0)*zxF*R.row(2).transpose();
  G(1,1) += temp;
  temp =      R.row(2)*xxF*R.row(2).transpose();
  G(1,1) += temp;
  temp = 2.0*R_jac1.row(0)*zzF*R.row(0).transpose();
  G_jac1(1,1)  = temp;
  temp = -2.0*R_jac1.row(0)*zxF*R.row(2).transpose();
  G_jac1(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac1.row(2).transpose();
  G_jac1(1,1) += temp;
  temp = 2.0*R_jac1.row(2)*xxF*R.row(2).transpose();
  G_jac1(1,1) += temp;
  temp = 2.0*R_jac2.row(0)*zzF*R.row(0).transpose();
  G_jac2(1,1)  = temp;
  temp = -2.0*R_jac2.row(0)*zxF*R.row(2).transpose();
  G_jac2(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac2.row(2).transpose();
  G_jac2(1,1) += temp;
  temp = 2.0*R_jac2.row(2)*xxF*R.row(2).transpose();
  G_jac2(1,1) += temp;
  temp = 2.0*R_jac3.row(0)*zzF*R.row(0).transpose();
  G_jac3(1,1)  = temp;
  temp = -2.0*R_jac3.row(0)*zxF*R.row(2).transpose();
  G_jac3(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac3.row(2).transpose();
  G_jac3(1,1) += temp;
  temp = 2.0*R_jac3.row(2)*xxF*R.row(2).transpose();
  G_jac3(1,1) += temp;

  temp =      R.row(0)*zxF*R.row(1).transpose();
  G(1,2)  = temp;
  temp = -1.0*R.row(0)*yzF*R.row(0).transpose();
  G(1,2) += temp;
  temp = -1.0*R.row(2)*xxF*R.row(1).transpose();
  G(1,2) += temp;
  temp =      R.row(2)*xyF*R.row(0).transpose();
  G(1,2) += temp;
  temp = R_jac1.row(0)*zxF*R.row(1).transpose();
  G_jac1(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac1.row(1).transpose();
  G_jac1(1,2) += temp;
  temp = -2.0*R_jac1.row(0)*yzF*R.row(0).transpose();
  G_jac1(1,2) += temp;
  temp = -R_jac1.row(2)*xxF*R.row(1).transpose();
  G_jac1(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac1.row(1).transpose();
  G_jac1(1,2) += temp;
  temp = R_jac1.row(2)*xyF*R.row(0).transpose();
  G_jac1(1,2) += temp;
  temp = R.row(2)*xyF*R_jac1.row(0).transpose();
  G_jac1(1,2) += temp;
  temp = R_jac2.row(0)*zxF*R.row(1).transpose();
  G_jac2(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac2.row(1).transpose();
  G_jac2(1,2) += temp;
  temp = -2.0*R_jac2.row(0)*yzF*R.row(0).transpose();
  G_jac2(1,2) += temp;
  temp = -R_jac2.row(2)*xxF*R.row(1).transpose();
  G_jac2(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac2.row(1).transpose();
  G_jac2(1,2) += temp;
  temp = R_jac2.row(2)*xyF*R.row(0).transpose();
  G_jac2(1,2) += temp;
  temp = R.row(2)*xyF*R_jac2.row(0).transpose();
  G_jac2(1,2) += temp;
  temp = R_jac3.row(0)*zxF*R.row(1).transpose();
  G_jac3(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac3.row(1).transpose();
  G_jac3(1,2) += temp;
  temp = -2.0*R_jac3.row(0)*yzF*R.row(0).transpose();
  G_jac3(1,2) += temp;
  temp = -R_jac3.row(2)*xxF*R.row(1).transpose();
  G_jac3(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac3.row(1).transpose();
  G_jac3(1,2) += temp;
  temp = R_jac3.row(2)*xyF*R.row(0).transpose();
  G_jac3(1,2) += temp;
  temp = R.row(2)*xyF*R_jac3.row(0).transpose();
  G_jac3(1,2) += temp;

  temp =      R.row(1)*xxF*R.row(1).transpose();
  G(2,2)  = temp;
  temp = -2.0*R.row(0)*xyF*R.row(1).transpose();
  G(2,2) += temp;
  temp =      R.row(0)*yyF*R.row(0).transpose();
  G(2,2) += temp;
  temp = 2.0*R_jac1.row(1)*xxF*R.row(1).transpose();
  G_jac1(2,2)  = temp;
  temp = -2.0*R_jac1.row(0)*xyF*R.row(1).transpose();
  G_jac1(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac1.row(1).transpose();
  G_jac1(2,2) += temp;
  temp = 2.0*R_jac1.row(0)*yyF*R.row(0).transpose();
  G_jac1(2,2) += temp;
  temp = 2.0*R_jac2.row(1)*xxF*R.row(1).transpose();
  G_jac2(2,2)  = temp;
  temp = -2.0*R_jac2.row(0)*xyF*R.row(1).transpose();
  G_jac2(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac2.row(1).transpose();
  G_jac2(2,2) += temp;
  temp = 2.0*R_jac2.row(0)*yyF*R.row(0).transpose();
  G_jac2(2,2) += temp;
  temp = 2.0*R_jac3.row(1)*xxF*R.row(1).transpose();
  G_jac3(2,2)  = temp;
  temp = -2.0*R_jac3.row(0)*xyF*R.row(1).transpose();
  G_jac3(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac3.row(1).transpose();
  G_jac3(2,2) += temp;
  temp = 2.0*R_jac3.row(0)*yyF*R.row(0).transpose();
  G_jac3(2,2) += temp;

  G(1,0) = G(0,1);
  G(2,0) = G(0,2);
  G(2,1) = G(1,2);
  G_jac1(1,0) = G_jac1(0,1);
  G_jac1(2,0) = G_jac1(0,2);
  G_jac1(2,1) = G_jac1(1,2);
  G_jac2(1,0) = G_jac2(0,1);
  G_jac2(2,0) = G_jac2(0,2);
  G_jac2(2,1) = G_jac2(1,2);
  G_jac3(1,0) = G_jac3(0,1);
  G_jac3(2,0) = G_jac3(0,2);
  G_jac3(2,1) = G_jac3(1,2);

  //the generalized terms:
  Eigen::Matrix<double,1,9> Rows;
  Rows.block<1,3>(0,0) = R.row(0);
  Rows.block<1,3>(0,3) = R.row(1);
  Rows.block<1,3>(0,6) = R.row(2);
  
  Eigen::Matrix<double,9,1> Cols;
  Cols.block<3,1>(0,0) = R.col(0);
  Cols.block<3,1>(3,0) = R.col(1);
  Cols.block<3,1>(6,0) = R.col(2);
  
  Eigen::Matrix<double,1,9> Rows_jac1;
  Rows_jac1.block<1,3>(0,0) = R_jac1.row(0);
  Rows_jac1.block<1,3>(0,3) = R_jac1.row(1);
  Rows_jac1.block<1,3>(0,6) = R_jac1.row(2);
  Eigen::Matrix<double,1,9> Rows_jac2;
  Rows_jac2.block<1,3>(0,0) = R_jac2.row(0);
  Rows_jac2.block<1,3>(0,3) = R_jac2.row(1);
  Rows_jac2.block<1,3>(0,6) = R_jac2.row(2);
  Eigen::Matrix<double,1,9> Rows_jac3;
  Rows_jac3.block<1,3>(0,0) = R_jac3.row(0);
  Rows_jac3.block<1,3>(0,3) = R_jac3.row(1);
  Rows_jac3.block<1,3>(0,6) = R_jac3.row(2);
  
  Eigen::Matrix<double,9,1> Cols_jac1;
  Cols_jac1.block<3,1>(0,0) = R_jac1.col(0);
  Cols_jac1.block<3,1>(3,0) = R_jac1.col(1);
  Cols_jac1.block<3,1>(6,0) = R_jac1.col(2);
  Eigen::Matrix<double,9,1> Cols_jac2;
  Cols_jac2.block<3,1>(0,0) = R_jac2.col(0);
  Cols_jac2.block<3,1>(3,0) = R_jac2.col(1);
  Cols_jac2.block<3,1>(6,0) = R_jac2.col(2);
  Eigen::Matrix<double,9,1> Cols_jac3;
  Cols_jac3.block<3,1>(0,0) = R_jac3.col(0);
  Cols_jac3.block<3,1>(3,0) = R_jac3.col(1);
  Cols_jac3.block<3,1>(6,0) = R_jac3.col(2);
  
  temp =      R.row(2)*y1P*Cols;
  G(0,3)  = temp;
  temp =      R.row(2)*y2P*Rows.transpose();
  G(0,3) += temp;
  temp = -1.0*R.row(1)*z1P*Cols;
  G(0,3) += temp;
  temp = -1.0*R.row(1)*z2P*Rows.transpose();
  G(0,3) += temp;
  temp =      R_jac1.row(2)*y1P*Cols;
  G_jac1(0,3)  = temp;
  temp =      R.row(2)*y1P*Cols_jac1;
  G_jac1(0,3) += temp;
  temp =      R_jac1.row(2)*y2P*Rows.transpose();
  G_jac1(0,3) += temp;
  temp =      R.row(2)*y2P*Rows_jac1.transpose();
  G_jac1(0,3) += temp;
  temp = -1.0*R_jac1.row(1)*z1P*Cols;
  G_jac1(0,3) += temp;
  temp = -1.0*R.row(1)*z1P*Cols_jac1;
  G_jac1(0,3) += temp;
  temp = -1.0*R_jac1.row(1)*z2P*Rows.transpose();
  G_jac1(0,3) += temp;
  temp = -1.0*R.row(1)*z2P*Rows_jac1.transpose();
  G_jac1(0,3) += temp;
  temp =      R_jac2.row(2)*y1P*Cols;
  G_jac2(0,3)  = temp;
  temp =      R.row(2)*y1P*Cols_jac2;
  G_jac2(0,3) += temp;
  temp =      R_jac2.row(2)*y2P*Rows.transpose();
  G_jac2(0,3) += temp;
  temp =      R.row(2)*y2P*Rows_jac2.transpose();
  G_jac2(0,3) += temp;
  temp = -1.0*R_jac2.row(1)*z1P*Cols;
  G_jac2(0,3) += temp;
  temp = -1.0*R.row(1)*z1P*Cols_jac2;
  G_jac2(0,3) += temp;
  temp = -1.0*R_jac2.row(1)*z2P*Rows.transpose();
  G_jac2(0,3) += temp;
  temp = -1.0*R.row(1)*z2P*Rows_jac2.transpose();
  G_jac2(0,3) += temp;
  temp =      R_jac3.row(2)*y1P*Cols;
  G_jac3(0,3)  = temp;
  temp =      R.row(2)*y1P*Cols_jac3;
  G_jac3(0,3) += temp;
  temp =      R_jac3.row(2)*y2P*Rows.transpose();
  G_jac3(0,3) += temp;
  temp =      R.row(2)*y2P*Rows_jac3.transpose();
  G_jac3(0,3) += temp;
  temp = -1.0*R_jac3.row(1)*z1P*Cols;
  G_jac3(0,3) += temp;
  temp = -1.0*R.row(1)*z1P*Cols_jac3;
  G_jac3(0,3) += temp;
  temp = -1.0*R_jac3.row(1)*z2P*Rows.transpose();
  G_jac3(0,3) += temp;
  temp = -1.0*R.row(1)*z2P*Rows_jac3.transpose();
  G_jac3(0,3) += temp;
  
  temp =      R.row(0)*z1P*Cols;
  G(1,3)  = temp;
  temp =      R.row(0)*z2P*Rows.transpose();
  G(1,3) += temp;
  temp = -1.0*R.row(2)*x1P*Cols;
  G(1,3) += temp;
  temp = -1.0*R.row(2)*x2P*Rows.transpose();
  G(1,3) += temp;
  temp =      R_jac1.row(0)*z1P*Cols;
  G_jac1(1,3)  = temp;
  temp =      R.row(0)*z1P*Cols_jac1;
  G_jac1(1,3) += temp;
  temp =      R_jac1.row(0)*z2P*Rows.transpose();
  G_jac1(1,3) += temp;
  temp =      R.row(0)*z2P*Rows_jac1.transpose();
  G_jac1(1,3) += temp;
  temp = -1.0*R_jac1.row(2)*x1P*Cols;
  G_jac1(1,3) += temp;
  temp = -1.0*R.row(2)*x1P*Cols_jac1;
  G_jac1(1,3) += temp;
  temp = -1.0*R_jac1.row(2)*x2P*Rows.transpose();
  G_jac1(1,3) += temp;
  temp = -1.0*R.row(2)*x2P*Rows_jac1.transpose();
  G_jac1(1,3) += temp;
  temp =      R_jac2.row(0)*z1P*Cols;
  G_jac2(1,3)  = temp;
  temp =      R.row(0)*z1P*Cols_jac2;
  G_jac2(1,3) += temp;
  temp =      R_jac2.row(0)*z2P*Rows.transpose();
  G_jac2(1,3) += temp;
  temp =      R.row(0)*z2P*Rows_jac2.transpose();
  G_jac2(1,3) += temp;
  temp = -1.0*R_jac2.row(2)*x1P*Cols;
  G_jac2(1,3) += temp;
  temp = -1.0*R.row(2)*x1P*Cols_jac2;
  G_jac2(1,3) += temp;
  temp = -1.0*R_jac2.row(2)*x2P*Rows.transpose();
  G_jac2(1,3) += temp;
  temp = -1.0*R.row(2)*x2P*Rows_jac2.transpose();
  G_jac2(1,3) += temp;
  temp =      R_jac3.row(0)*z1P*Cols;
  G_jac3(1,3)  = temp;
  temp =      R.row(0)*z1P*Cols_jac3;
  G_jac3(1,3) += temp;
  temp =      R_jac3.row(0)*z2P*Rows.transpose();
  G_jac3(1,3) += temp;
  temp =      R.row(0)*z2P*Rows_jac3.transpose();
  G_jac3(1,3) += temp;
  temp = -1.0*R_jac3.row(2)*x1P*Cols;
  G_jac3(1,3) += temp;
  temp = -1.0*R.row(2)*x1P*Cols_jac3;
  G_jac3(1,3) += temp;
  temp = -1.0*R_jac3.row(2)*x2P*Rows.transpose();
  G_jac3(1,3) += temp;
  temp = -1.0*R.row(2)*x2P*Rows_jac3.transpose();
  G_jac3(1,3) += temp;
  
  temp =      R.row(1)*x1P*Cols;
  G(2,3)  = temp;
  temp =      R.row(1)*x2P*Rows.transpose();
  G(2,3) += temp;
  temp = -1.0*R.row(0)*y1P*Cols;
  G(2,3) += temp;
  temp = -1.0*R.row(0)*y2P*Rows.transpose();
  G(2,3) += temp;
  temp =      R_jac1.row(1)*x1P*Cols;
  G_jac1(2,3)  = temp;
  temp =      R.row(1)*x1P*Cols_jac1;
  G_jac1(2,3) += temp;
  temp =      R_jac1.row(1)*x2P*Rows.transpose();
  G_jac1(2,3) += temp;
  temp =      R.row(1)*x2P*Rows_jac1.transpose();
  G_jac1(2,3) += temp;
  temp = -1.0*R_jac1.row(0)*y1P*Cols;
  G_jac1(2,3) += temp;
  temp = -1.0*R.row(0)*y1P*Cols_jac1;
  G_jac1(2,3) += temp;
  temp = -1.0*R_jac1.row(0)*y2P*Rows.transpose();
  G_jac1(2,3) += temp;
  temp = -1.0*R.row(0)*y2P*Rows_jac1.transpose();
  G_jac1(2,3) += temp;
  temp =      R_jac2.row(1)*x1P*Cols;
  G_jac2(2,3)  = temp;
  temp =      R.row(1)*x1P*Cols_jac2;
  G_jac2(2,3) += temp;
  temp =      R_jac2.row(1)*x2P*Rows.transpose();
  G_jac2(2,3) += temp;
  temp =      R.row(1)*x2P*Rows_jac2.transpose();
  G_jac2(2,3) += temp;
  temp = -1.0*R_jac2.row(0)*y1P*Cols;
  G_jac2(2,3) += temp;
  temp = -1.0*R.row(0)*y1P*Cols_jac2;
  G_jac2(2,3) += temp;
  temp = -1.0*R_jac2.row(0)*y2P*Rows.transpose();
  G_jac2(2,3) += temp;
  temp = -1.0*R.row(0)*y2P*Rows_jac2.transpose();
  G_jac2(2,3) += temp;
  temp =      R_jac3.row(1)*x1P*Cols;
  G_jac3(2,3)  = temp;
  temp =      R.row(1)*x1P*Cols_jac3;
  G_jac3(2,3) += temp;
  temp =      R_jac3.row(1)*x2P*Rows.transpose();
  G_jac3(2,3) += temp;
  temp =      R.row(1)*x2P*Rows_jac3.transpose();
  G_jac3(2,3) += temp;
  temp = -1.0*R_jac3.row(0)*y1P*Cols;
  G_jac3(2,3) += temp;
  temp = -1.0*R.row(0)*y1P*Cols_jac3;
  G_jac3(2,3) += temp;
  temp = -1.0*R_jac3.row(0)*y2P*Rows.transpose();
  G_jac3(2,3) += temp;
  temp = -1.0*R.row(0)*y2P*Rows_jac3.transpose();
  G_jac3(2,3) += temp;
  
  temp = -1.0*Cols.transpose()*m11P*Cols;
  G(3,3)  = temp;
  temp = -1.0*Rows*m22P*Rows.transpose();
  G(3,3) += temp;
  temp = -2.0*Rows*m12P*Cols;
  G(3,3) += temp;
  temp = -2.0*Cols.transpose()*m11P*Cols_jac1;
  G_jac1(3,3)  = temp;
  temp = -2.0*Rows_jac1*m22P*Rows.transpose();
  G_jac1(3,3) += temp;
  temp = -2.0*Rows_jac1*m12P*Cols;
  G_jac1(3,3) += temp;
  temp = -2.0*Rows*m12P*Cols_jac1;
  G_jac1(3,3) += temp;
  temp = -2.0*Cols.transpose()*m11P*Cols_jac2;
  G_jac2(3,3)  = temp;
  temp = -2.0*Rows_jac2*m22P*Rows.transpose();
  G_jac2(3,3) += temp;
  temp = -2.0*Rows_jac2*m12P*Cols;
  G_jac2(3,3) += temp;
  temp = -2.0*Rows*m12P*Cols_jac2;
  G_jac2(3,3) += temp;
  temp = -2.0*Cols.transpose()*m11P*Cols_jac3;
  G_jac3(3,3)  = temp;
  temp = -2.0*Rows_jac3*m22P*Rows.transpose();
  G_jac3(3,3) += temp;
  temp = -2.0*Rows_jac3*m12P*Cols;
  G_jac3(3,3) += temp;
  temp = -2.0*Rows*m12P*Cols_jac3;
  G_jac3(3,3) += temp;
  
  G(3,0) = G(0,3);
  G(3,1) = G(1,3);
  G(3,2) = G(2,3);
  G_jac1(3,0) = G_jac1(0,3);
  G_jac1(3,1) = G_jac1(1,3);
  G_jac1(3,2) = G_jac1(2,3);
  G_jac2(3,0) = G_jac2(0,3);
  G_jac2(3,1) = G_jac2(1,3);
  G_jac2(3,2) = G_jac2(2,3);
  G_jac3(3,0) = G_jac3(0,3);
  G_jac3(3,1) = G_jac3(1,3);
  G_jac3(3,2) = G_jac3(2,3);

  return G;
}
