
#include <iostream>
#include <fstream>
#include <string>
#include "types.h"
#include <Eigen/SVD>
#include <Eigen/LU>
#include <Eigen/QR>
#include <Eigen/StdVector>


namespace csfm {


typedef std::vector<Eigen::Matrix<double, 3, 4>, Eigen::aligned_allocator<Eigen::Matrix<double, 3, 4> > > vector_mat34;

enum {
  TRIANGULATION_OK = 0,
  TRIANGULATION_SMALL_ANGLE,
  TRIANGULATION_BEHIND_CAMERA,
  TRIANGULATION_BAD_REPROJECTION
};


double AngleBetweenVectors(const Eigen::Vector3d &u,
                           const Eigen::Vector3d &v) {
    double c = (u.dot(v))
               / sqrt(u.dot(u) * v.dot(v));
    if (c >= 1.0) return 0.0;
    else return acos(c);
}


py::list TriangulateReturn(int error, py::object value) {
    py::list retn;
    retn.append(error);
    retn.append(value);
    return retn;
}


Eigen::Vector4d TriangulateBearingsDLTSolve(
    const Eigen::Matrix<double, 3, Eigen::Dynamic> &bs,
    const vector_mat34 &Rts) {
  int nviews = bs.cols();
  assert(nviews == Rts.size());

  Eigen::MatrixXd A(2 * nviews, 4);
  for (int i = 0; i < nviews; i++) {
    A.row(2 * i    ) = bs(0, i) * Rts[i].row(2) - bs(2, i) * Rts[i].row(0);
    A.row(2 * i + 1) = bs(1, i) * Rts[i].row(2) - bs(2, i) * Rts[i].row(1);
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > mySVD(A, Eigen::ComputeFullV );
  Eigen::Vector4d worldPoint;
  worldPoint[0] = mySVD.matrixV()(0,3);
  worldPoint[1] = mySVD.matrixV()(1,3);
  worldPoint[2] = mySVD.matrixV()(2,3);
  worldPoint[3] = mySVD.matrixV()(3,3);

  return worldPoint;
}


py::object TriangulateBearingsDLT(const py::list &Rts_list,
                                  const py::list &bs_list,
                                  double threshold,
                                  double min_angle) {

  int n = py::len(Rts_list);
  vector_mat34 Rts;
  Eigen::Matrix<double, 3, Eigen::Dynamic> bs(3, n);
  Eigen::MatrixXd vs(3, n);
  bool angle_ok = false;
  for (int i = 0; i < n; ++i) {
    py::object oRt = Rts_list[i];
    py::object ob = bs_list[i];

    pyarray_d Rt_array(oRt);
    pyarray_d b_array(ob);

    Eigen::Map<const Eigen::MatrixXd> Rt(Rt_array.data(), 4, 3);
    Eigen::Map<const Eigen::MatrixXd> b(b_array.data(), 3, 1);

    Rts.push_back(Rt.transpose());
    bs.col(i) = b.col(0);

    // Check angle between rays
    if (!angle_ok) {
      Eigen::Vector3d xh;
      xh << b(0,0), b(1,0), b(2,0);
      Eigen::Vector3d v = Rt.block<3,3>(0,0).transpose().inverse() * xh;
      vs.col(i) << v(0), v(1), v(2);

      for (int j = 0; j < i; ++j) {
        Eigen::Vector3d a, b;
        a << vs(0, i), vs(1, i), vs(2, i);
        b << vs(0, j), vs(1, j), vs(2, j);
        double angle = AngleBetweenVectors(a, b);
        if (angle >= min_angle) {
          angle_ok = true;
        }
      }
    }
  }

  if (!angle_ok) {
    return TriangulateReturn(TRIANGULATION_SMALL_ANGLE, py::none());
  }

  Eigen::Vector4d X = TriangulateBearingsDLTSolve(bs, Rts);
  X /= X(3);

  for (int i = 0; i < n; ++i) {
    Eigen::Vector3d x_reproj = Rts[i] * X;
    Eigen::Vector3d b;
    b << bs(0, i), bs(1, i), bs(2, i);

    double error = AngleBetweenVectors(x_reproj, b);
    if (error > threshold) {
     return TriangulateReturn(TRIANGULATION_BAD_REPROJECTION, py::none());
    }
  }

  return TriangulateReturn(TRIANGULATION_OK,
                           py_array_from_data(X.data(), 3));
}


// Point minimizing the squared distance to all rays
// Closed for solution from
//   Srikumar Ramalingam, Suresh K. Lodha and Peter Sturm
//   "A generic structure-from-motion framework"
//   CVIU 2006
Eigen::Vector3d TriangulateBearingsMidpointSolve(
    const Eigen::Matrix<double, 3, Eigen::Dynamic> &os,
    const Eigen::Matrix<double, 3, Eigen::Dynamic> &bs) {
  int nviews = bs.cols();
  assert(nviews == os.cols());
  assert(nviews >= 2);

  Eigen::Matrix3d BBt;
  Eigen::Vector3d BBtA, A;
  BBt.setZero();
  BBtA.setZero();
  A.setZero();
  for (int i = 0; i < nviews; ++i) {
    BBt += bs.col(i) * bs.col(i).transpose();
    BBtA += bs.col(i) * bs.col(i).transpose() * os.col(i);
    A += os.col(i);
  }
  Eigen::Matrix3d Cinv = (nviews * Eigen::Matrix3d::Identity() - BBt).inverse();

  return (Eigen::Matrix3d::Identity() + BBt * Cinv) * A / nviews - Cinv * BBtA;
}


py::object TriangulateBearingsMidpoint(const py::list &os_list,
                                       const py::list &bs_list,
                                       const py::list &threshold_list,
                                       double min_angle) {
  int n = py::len(os_list);

  // Build Eigen matrices
  Eigen::Matrix<double, 3, Eigen::Dynamic> os(3, n);
  Eigen::Matrix<double, 3, Eigen::Dynamic> bs(3, n);
  for (int i = 0; i < n; ++i) {
    pyarray_d o_array = os_list[i].cast<pyarray_d>();
    pyarray_d b_array = bs_list[i].cast<pyarray_d>();
    const double *o = o_array.data();
    const double *b = b_array.data();
    os.col(i) <<  o[0], o[1], o[2];
    bs.col(i) <<  b[0], b[1], b[2];
  }

  // Check angle between rays
  bool angle_ok = false;
  for (int i = 0; i < n; ++i) {
    if (!angle_ok) {
      for (int j = 0; j < i; ++j) {
        Eigen::Vector3d a, b;
        a << bs(0, i), bs(1, i), bs(2, i);
        b << bs(0, j), bs(1, j), bs(2, j);
        double angle = AngleBetweenVectors(a, b);
        if (angle >= min_angle) {
          angle_ok = true;
        }
      }
    }
  }
  if (!angle_ok) {
    return TriangulateReturn(TRIANGULATION_SMALL_ANGLE, py::none());
  }

  // Triangulate
  Eigen::Vector3d X = TriangulateBearingsMidpointSolve(os, bs);

  // Check reprojection error
  for (int i = 0; i < n; ++i) {
    Eigen::Vector3d x_reproj = X - os.col(i);
    Eigen::Vector3d b = bs.col(i);

    double error = AngleBetweenVectors(x_reproj, b);
    if (error > threshold_list[i].cast<float>()) {
      return TriangulateReturn(TRIANGULATION_BAD_REPROJECTION, py::none());
    }
  }

  return TriangulateReturn(TRIANGULATION_OK,
                           py_array_from_data(X.data(), 3));
}


}

