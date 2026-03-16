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


#include <opengv/point_cloud/methods.hpp>
#include <opengv/Indices.hpp>

#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

#include <opengv/OptimizationFunctor.hpp>
#include <opengv/math/arun.hpp>
#include <opengv/math/cayley.hpp>

namespace opengv
{
namespace point_cloud
{

transformation_t threept_arun(
    const PointCloudAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 2);

  //derive the centroid of the two point-clouds
  point_t pointsCenter1 = Eigen::Vector3d::Zero();
  point_t pointsCenter2 = Eigen::Vector3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    pointsCenter1 += adapter.getPoint1(indices[i]);
    pointsCenter2 += adapter.getPoint2(indices[i]);
  }

  pointsCenter1 = pointsCenter1 / numberCorrespondences;
  pointsCenter2 = pointsCenter2 / numberCorrespondences;

  //compute the matrix H = sum(f'*f^{T})
  Eigen::MatrixXd Hcross(3,3);
  Hcross = Eigen::Matrix3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    Eigen::Vector3d f = adapter.getPoint1(indices[i]) - pointsCenter1;
    Eigen::Vector3d fprime = adapter.getPoint2(indices[i]) - pointsCenter2;
    Hcross += fprime * f.transpose();
  }

  //decompose this matrix (SVD) to obtain rotation
  rotation_t rotation = math::arun(Hcross);
  translation_t translation = pointsCenter1 - rotation*pointsCenter2;
  transformation_t solution;
  solution.block<3,3>(0,0) = rotation;
  solution.col(3) = translation;

  return solution;
};

}
}

opengv::transformation_t
opengv::point_cloud::threept_arun( const PointCloudAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return threept_arun(adapter,idx);
};

opengv::transformation_t
opengv::point_cloud::threept_arun(
    const PointCloudAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return threept_arun(adapter,idx);
};

namespace opengv
{
namespace point_cloud
{

struct OptimizeNonlinearFunctor1 : OptimizationFunctor<double>
{
  PointCloudAdapterBase & _adapter;
  const Indices & _indices;

  OptimizeNonlinearFunctor1(
      PointCloudAdapterBase & adapter,
      const Indices & indices ) :
      OptimizationFunctor<double>(6,indices.size()),
      _adapter(adapter),
      _indices(indices) {}

  int operator()(const VectorXd &x, VectorXd &fvec) const
  {
    assert( x.size() == 6 );
    assert( (unsigned int) fvec.size() == _indices.size());

    //compute the current position
    transformation_t transformation;
    transformation.col(3) = x.block<3,1>(0,0);
    cayley_t cayley = x.block<3,1>(3,0);
    transformation.block<3,3>(0,0) = math::cayley2rot(cayley);

    Eigen::Matrix<double,4,1> p_hom;
    p_hom[3] = 1.0;

    for( size_t i = 0; i < _indices.size(); i++ )
    {
      p_hom.block<3,1>(0,0) = _adapter.getPoint2(_indices[i]);
      point_t transformedPoint = transformation * p_hom;
      translation_t error = _adapter.getPoint1(_indices[i]) - transformedPoint;
      fvec[i] = error.norm();
    }

    return 0;
  }
};

transformation_t optimize_nonlinear(
    PointCloudAdapterBase & adapter,
    const Indices & indices )
{
  const int n=6;
  VectorXd x(n);

  x.block<3,1>(0,0) = adapter.gett12();
  x.block<3,1>(3,0) = math::rot2cayley(adapter.getR12());

  OptimizeNonlinearFunctor1 functor( adapter, indices );
  NumericalDiff<OptimizeNonlinearFunctor1 > numDiff(functor);
  LevenbergMarquardt< NumericalDiff<OptimizeNonlinearFunctor1 > > lm(numDiff);

  lm.resetParameters();
  lm.parameters.ftol = 1.E10*NumTraits<double>::epsilon();
  lm.parameters.xtol = 1.E10*NumTraits<double>::epsilon();
  lm.parameters.maxfev = 1000;
  lm.minimize(x);

  transformation_t transformation;
  transformation.col(3) = x.block<3,1>(0,0);
  transformation.block<3,3>(0,0) = math::cayley2rot(x.block<3,1>(3,0));
  return transformation;
}

}
}

opengv::transformation_t
opengv::point_cloud::optimize_nonlinear( PointCloudAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return optimize_nonlinear(adapter,idx);
}

opengv::transformation_t
opengv::point_cloud::optimize_nonlinear(
    PointCloudAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return optimize_nonlinear(adapter,idx);
}
