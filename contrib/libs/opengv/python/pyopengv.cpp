#include <iostream>
#include <vector>
#include <opengv/absolute_pose/AbsoluteAdapterBase.hpp>
#include <opengv/absolute_pose/methods.hpp>
#include <opengv/relative_pose/RelativeAdapterBase.hpp>
#include <opengv/relative_pose/methods.hpp>
#include <opengv/sac/Ransac.hpp>
#include <opengv/sac/Lmeds.hpp>
#include <opengv/sac_problems/absolute_pose/AbsolutePoseSacProblem.hpp>
#include <opengv/sac_problems/relative_pose/CentralRelativePoseSacProblem.hpp>
#include <opengv/sac_problems/relative_pose/RotationOnlySacProblem.hpp>
#include <opengv/triangulation/methods.hpp>

#include "types.hpp"


namespace pyopengv {


opengv::bearingVector_t bearingVectorFromArray(
    const pyarray_d &array,
    size_t index )
{
  opengv::bearingVector_t v;
  v[0] = *array.data(index, 0);
  v[1] = *array.data(index, 1);
  v[2] = *array.data(index, 2);
  return v;
}

opengv::point_t pointFromArray(
    const pyarray_d &array,
    size_t index )
{
  opengv::point_t p;
  p[0] = *array.data(index, 0);
  p[1] = *array.data(index, 1);
  p[2] = *array.data(index, 2);
  return p;
}

py::object arrayFromPoints( const opengv::points_t &points )
{
  std::vector<double> data(points.size() * 3);
  for (size_t i = 0; i < points.size(); ++i) {
    data[3 * i + 0] = points[i][0];
    data[3 * i + 1] = points[i][1];
    data[3 * i + 2] = points[i][2];
  }
  return py_array_from_data(&data[0], points.size(), 3);
}

py::object arrayFromTranslation( const opengv::translation_t &t )
{
  return py_array_from_data(t.data(), 3);
}

py::object arrayFromRotation( const opengv::rotation_t &R )
{
  Eigen::Matrix<double, 3, 3, Eigen::RowMajor> R_row_major = R;
  return py_array_from_data(R_row_major.data(), 3, 3);
}

py::list listFromRotations( const opengv::rotations_t &Rs )
{
  py::list retn;
  for (size_t i = 0; i < Rs.size(); ++i) {
    retn.append(arrayFromRotation(Rs[i]));
  }
  return retn;
}

py::object arrayFromEssential( const opengv::essential_t &E )
{
  Eigen::Matrix<double, 3, 3, Eigen::RowMajor> E_row_major = E;
  return py_array_from_data(E_row_major.data(), 3, 3);
}

py::list listFromEssentials( const opengv::essentials_t &Es )
{
  py::list retn;
  for (size_t i = 0; i < Es.size(); ++i) {
    retn.append(arrayFromEssential(Es[i]));
  }
  return retn;
}

py::object arrayFromTransformation( const opengv::transformation_t &t )
{
  Eigen::Matrix<double, 3, 4, Eigen::RowMajor> t_row_major = t;
  return py_array_from_data(t_row_major.data(), 3, 4);
}

py::list listFromTransformations( const opengv::transformations_t &t )
{
  py::list retn;
  for (size_t i = 0; i < t.size(); ++i) {
    retn.append(arrayFromTransformation(t[i]));
  }
  return retn;
}

std::vector<int> getNindices( int n )
{
  std::vector<int> indices;
  for(int i = 0; i < n; i++)
    indices.push_back(i);
  return indices;
}


namespace absolute_pose {

class CentralAbsoluteAdapter : public opengv::absolute_pose::AbsoluteAdapterBase
{
protected:
  using AbsoluteAdapterBase::_t;
  using AbsoluteAdapterBase::_R;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  CentralAbsoluteAdapter(
      pyarray_d &bearingVectors,
      pyarray_d &points )
    : _bearingVectors(bearingVectors)
    , _points(points)
  {}

  CentralAbsoluteAdapter(
      pyarray_d &bearingVectors,
      pyarray_d &points,
      pyarray_d &R )
    : _bearingVectors(bearingVectors)
    , _points(points)
  {
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 3; ++j) {
        _R(i, j) = *R.data(i, j);
      }
    }
  }

  CentralAbsoluteAdapter(
      pyarray_d &bearingVectors,
      pyarray_d &points,
      pyarray_d &t,
      pyarray_d &R )
    : _bearingVectors(bearingVectors)
    , _points(points)
  {
    for (int i = 0; i < 3; ++i) {
      _t(i) = *t.data(i);
    }
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 3; ++j) {
        _R(i, j) = *R.data(i, j);
      }
    }
  }

  virtual ~CentralAbsoluteAdapter() {}

  //Access of correspondences

  virtual opengv::bearingVector_t getBearingVector( size_t index ) const {
    return bearingVectorFromArray(_bearingVectors, index);
  }

  virtual double getWeight( size_t index ) const {
    return 1.0;
  }

  virtual opengv::translation_t getCamOffset( size_t index ) const {
    return Eigen::Vector3d::Zero();
  }

  virtual opengv::rotation_t getCamRotation( size_t index ) const {
    return opengv::rotation_t::Identity();
  }

  virtual opengv::point_t getPoint( size_t index ) const {
    return pointFromArray(_points, index);
  }

  virtual size_t getNumberCorrespondences() const {
    return _bearingVectors.shape(0);
  }

protected:
  pyarray_d _bearingVectors;
  pyarray_d _points;
};



py::object p2p( pyarray_d &v, pyarray_d &p, pyarray_d &R )
{
  CentralAbsoluteAdapter adapter(v, p, R);
  return arrayFromTranslation(
    opengv::absolute_pose::p2p(adapter, 0, 1));
}

py::object p3p_kneip( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return listFromTransformations(
    opengv::absolute_pose::p3p_kneip(adapter, 0, 1, 2));
}

py::object p3p_gao( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return listFromTransformations(
    opengv::absolute_pose::p3p_gao(adapter, 0, 1, 2));
}

py::object gp3p( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return listFromTransformations(
    opengv::absolute_pose::gp3p(adapter, 0, 1, 2));
}

py::object epnp( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return arrayFromTransformation(
    opengv::absolute_pose::epnp(adapter));
}

py::object gpnp( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return arrayFromTransformation(
    opengv::absolute_pose::gpnp(adapter));
}

py::object upnp( pyarray_d &v, pyarray_d &p )
{
  CentralAbsoluteAdapter adapter(v, p);
  return listFromTransformations(
    opengv::absolute_pose::upnp(adapter));
}

py::object optimize_nonlinear( pyarray_d &v,
                               pyarray_d &p,
                               pyarray_d &t,
                               pyarray_d &R )
{
  CentralAbsoluteAdapter adapter(v, p, t, R);
  return arrayFromTransformation(
    opengv::absolute_pose::optimize_nonlinear(adapter));
}

py::object ransac(
    pyarray_d &v,
    pyarray_d &p,
    std::string algo_name,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::absolute_pose;

  CentralAbsoluteAdapter adapter(v, p);

  // Create a ransac problem
  AbsolutePoseSacProblem::algorithm_t algorithm = AbsolutePoseSacProblem::KNEIP;
  if (algo_name == "TWOPT") algorithm = AbsolutePoseSacProblem::TWOPT;
  else if (algo_name == "KNEIP") algorithm = AbsolutePoseSacProblem::KNEIP;
  else if (algo_name == "GAO") algorithm = AbsolutePoseSacProblem::GAO;
  else if (algo_name == "EPNP") algorithm = AbsolutePoseSacProblem::EPNP;
  else if (algo_name == "GP3P") algorithm = AbsolutePoseSacProblem::GP3P;

  std::shared_ptr<AbsolutePoseSacProblem>
      absposeproblem_ptr(
        new AbsolutePoseSacProblem(adapter, algorithm));

  // Create a ransac solver for the problem
  opengv::sac::Ransac<AbsolutePoseSacProblem> ransac;

  ransac.sac_model_ = absposeproblem_ptr;
  ransac.threshold_ = threshold;
  ransac.max_iterations_ = max_iterations;
  ransac.probability_ = probability;

  // Solve
  ransac.computeModel();
  return arrayFromTransformation(ransac.model_coefficients_);
}

py::object lmeds(
    pyarray_d &v,
    pyarray_d &p,
    std::string algo_name,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::absolute_pose;

  CentralAbsoluteAdapter adapter(v, p);

  // Create a lmeds problem
  AbsolutePoseSacProblem::algorithm_t algorithm = AbsolutePoseSacProblem::KNEIP;
  if (algo_name == "TWOPT") algorithm = AbsolutePoseSacProblem::TWOPT;
  else if (algo_name == "KNEIP") algorithm = AbsolutePoseSacProblem::KNEIP;
  else if (algo_name == "GAO") algorithm = AbsolutePoseSacProblem::GAO;
  else if (algo_name == "EPNP") algorithm = AbsolutePoseSacProblem::EPNP;
  else if (algo_name == "GP3P") algorithm = AbsolutePoseSacProblem::GP3P;

  std::shared_ptr<AbsolutePoseSacProblem>
      absposeproblem_ptr(
        new AbsolutePoseSacProblem(adapter, algorithm));

  // Create a ransac solver for the problem
  opengv::sac::Lmeds<AbsolutePoseSacProblem> lmeds;

  lmeds.sac_model_ = absposeproblem_ptr;
  lmeds.threshold_ = threshold;
  lmeds.max_iterations_ = max_iterations;
  lmeds.probability_ = probability;

  // Solve
  lmeds.computeModel();
  return arrayFromTransformation(lmeds.model_coefficients_);
}

} // namespace absolute_pose


namespace relative_pose
{

class CentralRelativeAdapter : public opengv::relative_pose::RelativeAdapterBase
{
protected:
  using RelativeAdapterBase::_t12;
  using RelativeAdapterBase::_R12;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  CentralRelativeAdapter(
      pyarray_d &bearingVectors1,
      pyarray_d &bearingVectors2 )
    : _bearingVectors1(bearingVectors1)
    , _bearingVectors2(bearingVectors2)
  {}

  CentralRelativeAdapter(
      pyarray_d &bearingVectors1,
      pyarray_d &bearingVectors2,
      pyarray_d &R12 )
    : _bearingVectors1(bearingVectors1)
    , _bearingVectors2(bearingVectors2)
  {
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 3; ++j) {
        _R12(i, j) = *R12.data(i, j);
      }
    }
  }

  CentralRelativeAdapter(
      pyarray_d &bearingVectors1,
      pyarray_d &bearingVectors2,
      pyarray_d &t12,
      pyarray_d &R12 )
    : _bearingVectors1(bearingVectors1)
    , _bearingVectors2(bearingVectors2)
  {
    for (int i = 0; i < 3; ++i) {
      _t12(i) = *t12.data(i);
    }
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 3; ++j) {
        _R12(i, j) = *R12.data(i, j);
      }
    }
  }

  virtual ~CentralRelativeAdapter() {}

  virtual opengv::bearingVector_t getBearingVector1( size_t index ) const {
    return bearingVectorFromArray(_bearingVectors1, index);
  }

  virtual opengv::bearingVector_t getBearingVector2( size_t index ) const {
    return bearingVectorFromArray(_bearingVectors2, index);
  }

  virtual double getWeight( size_t index ) const {
    return 1.0;
  }

  virtual opengv::translation_t getCamOffset1( size_t index ) const {
    return Eigen::Vector3d::Zero();
  }

  virtual opengv::rotation_t getCamRotation1( size_t index ) const {
    return opengv::rotation_t::Identity();
  }

  virtual opengv::translation_t getCamOffset2( size_t index ) const {
    return Eigen::Vector3d::Zero();
  }

  virtual opengv::rotation_t getCamRotation2( size_t index ) const {
    return opengv::rotation_t::Identity();
  }

  virtual size_t getNumberCorrespondences() const {
    return _bearingVectors1.shape(0);
  }

protected:
  pyarray_d _bearingVectors1;
  pyarray_d _bearingVectors2;
};


py::object twopt( pyarray_d &b1, pyarray_d &b2, pyarray_d &R )
{
  CentralRelativeAdapter adapter(b1, b2, R);
  return arrayFromTranslation(
    opengv::relative_pose::twopt(adapter, true, 0, 1));
}

py::object twopt_rotationOnly( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return arrayFromRotation(
    opengv::relative_pose::twopt_rotationOnly(adapter, 0, 1));
}

py::object rotationOnly( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return arrayFromRotation(
    opengv::relative_pose::rotationOnly(adapter));
}

py::object fivept_nister( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return listFromEssentials(
    opengv::relative_pose::fivept_nister(adapter));
}

py::object fivept_kneip( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return listFromRotations(
    opengv::relative_pose::fivept_kneip(adapter, getNindices(5)));
}

py::object sevenpt( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return listFromEssentials(
    opengv::relative_pose::sevenpt(adapter));
}

py::object eightpt( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return arrayFromEssential(
    opengv::relative_pose::eightpt(adapter));
}

py::object eigensolver( pyarray_d &b1, pyarray_d &b2, pyarray_d &R )
{
  CentralRelativeAdapter adapter(b1, b2, R);
  return arrayFromRotation(
    opengv::relative_pose::eigensolver(adapter));
}

py::object sixpt( pyarray_d &b1, pyarray_d &b2 )
{
  CentralRelativeAdapter adapter(b1, b2);
  return listFromRotations(
    opengv::relative_pose::sixpt(adapter));
}

py::object optimize_nonlinear( pyarray_d &b1,
                               pyarray_d &b2,
                               pyarray_d &t12,
                               pyarray_d &R12 )
{
  CentralRelativeAdapter adapter(b1, b2, t12, R12);
  return arrayFromTransformation(
    opengv::relative_pose::optimize_nonlinear(adapter));
}

py::object ransac(
    pyarray_d &b1,
    pyarray_d &b2,
    std::string algo_name,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::relative_pose;

  CentralRelativeAdapter adapter(b1, b2);

  // Create a ransac problem
  CentralRelativePoseSacProblem::algorithm_t algorithm = CentralRelativePoseSacProblem::NISTER;
  if (algo_name == "STEWENIUS") algorithm = CentralRelativePoseSacProblem::STEWENIUS;
  else if (algo_name == "NISTER") algorithm = CentralRelativePoseSacProblem::NISTER;
  else if (algo_name == "SEVENPT") algorithm = CentralRelativePoseSacProblem::SEVENPT;
  else if (algo_name == "EIGHTPT") algorithm = CentralRelativePoseSacProblem::EIGHTPT;

  std::shared_ptr<CentralRelativePoseSacProblem>
      relposeproblem_ptr(
        new CentralRelativePoseSacProblem(adapter, algorithm));

  // Create a ransac solver for the problem
  opengv::sac::Ransac<CentralRelativePoseSacProblem> ransac;

  ransac.sac_model_ = relposeproblem_ptr;
  ransac.threshold_ = threshold;
  ransac.max_iterations_ = max_iterations;
  ransac.probability_ = probability;

  // Solve
  ransac.computeModel();
  return arrayFromTransformation(ransac.model_coefficients_);
}

py::object lmeds(
    pyarray_d &b1,
    pyarray_d &b2,
    std::string algo_name,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::relative_pose;

  CentralRelativeAdapter adapter(b1, b2);

  // Create a lmeds problem
  CentralRelativePoseSacProblem::algorithm_t algorithm = CentralRelativePoseSacProblem::NISTER;
  if (algo_name == "STEWENIUS") algorithm = CentralRelativePoseSacProblem::STEWENIUS;
  else if (algo_name == "NISTER") algorithm = CentralRelativePoseSacProblem::NISTER;
  else if (algo_name == "SEVENPT") algorithm = CentralRelativePoseSacProblem::SEVENPT;
  else if (algo_name == "EIGHTPT") algorithm = CentralRelativePoseSacProblem::EIGHTPT;

  std::shared_ptr<CentralRelativePoseSacProblem>
      relposeproblem_ptr(
        new CentralRelativePoseSacProblem(adapter, algorithm));

  // Create a lmeds solver for the problem
  opengv::sac::Lmeds<CentralRelativePoseSacProblem> lmeds;

  lmeds.sac_model_ = relposeproblem_ptr;
  lmeds.threshold_ = threshold;
  lmeds.max_iterations_ = max_iterations;
  lmeds.probability_ = probability;

  // Solve
  lmeds.computeModel();
  return arrayFromTransformation(lmeds.model_coefficients_);
}

py::object ransac_rotationOnly(
    pyarray_d &b1,
    pyarray_d &b2,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::relative_pose;

  CentralRelativeAdapter adapter(b1, b2);

  std::shared_ptr<RotationOnlySacProblem>
      relposeproblem_ptr(
        new RotationOnlySacProblem(adapter));

  // Create a ransac solver for the problem
  opengv::sac::Ransac<RotationOnlySacProblem> ransac;

  ransac.sac_model_ = relposeproblem_ptr;
  ransac.threshold_ = threshold;
  ransac.max_iterations_ = max_iterations;
  ransac.probability_ = probability;

  // Solve
  ransac.computeModel();
  return arrayFromRotation(ransac.model_coefficients_);
}

py::object lmeds_rotationOnly(
    pyarray_d &b1,
    pyarray_d &b2,
    double threshold,
    int max_iterations,
    double probability )
{
  using namespace opengv::sac_problems::relative_pose;

  CentralRelativeAdapter adapter(b1, b2);

  std::shared_ptr<RotationOnlySacProblem>
      relposeproblem_ptr(
        new RotationOnlySacProblem(adapter));

  // Create a lmeds solver for the problem
  opengv::sac::Lmeds<RotationOnlySacProblem> lmeds;

  lmeds.sac_model_ = relposeproblem_ptr;
  lmeds.threshold_ = threshold;
  lmeds.max_iterations_ = max_iterations;
  lmeds.probability_ = probability;

  // Solve
  lmeds.computeModel();
  return arrayFromRotation(lmeds.model_coefficients_);
}

} // namespace relative_pose

namespace triangulation
{

py::object triangulate( pyarray_d &b1,
                        pyarray_d &b2,
                        pyarray_d &t12,
                        pyarray_d &R12 )
{
  pyopengv::relative_pose::CentralRelativeAdapter adapter(b1, b2, t12, R12);

  opengv::points_t points;
  for (size_t i = 0; i < adapter.getNumberCorrespondences(); ++i)
  {
    opengv::point_t p = opengv::triangulation::triangulate(adapter, i);
    points.push_back(p);
  }
  return arrayFromPoints(points);
}

py::object triangulate2( pyarray_d &b1,
                         pyarray_d &b2,
                         pyarray_d &t12,
                         pyarray_d &R12 )
{
  pyopengv::relative_pose::CentralRelativeAdapter adapter(b1, b2, t12, R12);

  opengv::points_t points;
  for (size_t i = 0; i < adapter.getNumberCorrespondences(); ++i)
  {
    opengv::point_t p = opengv::triangulation::triangulate2(adapter, i);
    points.push_back(p);
  }
  return arrayFromPoints(points);
}


} // namespace triangulation

} // namespace pyopengv


PYBIND11_MODULE(pyopengv, m) {
  namespace py = pybind11;

  m.def("absolute_pose_p2p", pyopengv::absolute_pose::p2p);
  m.def("absolute_pose_p3p_kneip", pyopengv::absolute_pose::p3p_kneip);
  m.def("absolute_pose_p3p_gao", pyopengv::absolute_pose::p3p_gao);
  m.def("absolute_pose_gp3p", pyopengv::absolute_pose::gp3p);
  m.def("absolute_pose_epnp", pyopengv::absolute_pose::epnp);
  m.def("absolute_pose_gpnp", pyopengv::absolute_pose::gpnp);
  m.def("absolute_pose_upnp", pyopengv::absolute_pose::upnp);
  m.def("absolute_pose_optimize_nonlinear", pyopengv::absolute_pose::optimize_nonlinear);
  m.def("absolute_pose_ransac", pyopengv::absolute_pose::ransac,
        py::arg("v"),
        py::arg("p"),
        py::arg("algo_name"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );
  m.def("absolute_pose_lmeds", pyopengv::absolute_pose::lmeds,
        py::arg("v"),
        py::arg("p"),
        py::arg("algo_name"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );

  m.def("relative_pose_twopt", pyopengv::relative_pose::twopt);
  m.def("relative_pose_twopt_rotation_only", pyopengv::relative_pose::twopt_rotationOnly);
  m.def("relative_pose_rotation_only", pyopengv::relative_pose::rotationOnly);
  m.def("relative_pose_fivept_nister", pyopengv::relative_pose::fivept_nister);
  m.def("relative_pose_fivept_kneip", pyopengv::relative_pose::fivept_kneip);
  m.def("relative_pose_sevenpt", pyopengv::relative_pose::sevenpt);
  m.def("relative_pose_eightpt", pyopengv::relative_pose::eightpt);
  m.def("relative_pose_eigensolver", pyopengv::relative_pose::eigensolver);
  m.def("relative_pose_sixpt", pyopengv::relative_pose::sixpt);
  m.def("relative_pose_optimize_nonlinear", pyopengv::relative_pose::optimize_nonlinear);
  m.def("relative_pose_ransac", pyopengv::relative_pose::ransac,
        py::arg("b1"),
        py::arg("b2"),
        py::arg("algo_name"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );
  m.def("relative_pose_lmeds", pyopengv::relative_pose::lmeds,
        py::arg("b1"),
        py::arg("b2"),
        py::arg("algo_name"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );
  m.def("relative_pose_ransac_rotation_only", pyopengv::relative_pose::ransac_rotationOnly,
        py::arg("b1"),
        py::arg("b2"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );
  m.def("relative_pose_lmeds_rotation_only", pyopengv::relative_pose::lmeds_rotationOnly,
        py::arg("b1"),
        py::arg("b2"),
        py::arg("threshold"),
        py::arg("iterations") = 1000,
        py::arg("probability") = 0.99
  );



  m.def("triangulation_triangulate", pyopengv::triangulation::triangulate);
  m.def("triangulation_triangulate2", pyopengv::triangulation::triangulate2);
}
