//=============================================================================
//
// nldiffusion_functions.cpp
// Authors: Pablo F. Alcantarilla (1), Jesus Nuevo (2)
// Institutions: Toshiba Research Europe Ltd (1)
//               TrueVision Solutions (2)
// Date: 07/10/2014
// Email: pablofdezalc@gmail.com
//
// AKAZE Features Copyright 2014, Pablo F. Alcantarilla, Jesus Nuevo
// All Rights Reserved
// See LICENSE for the license information
//=============================================================================

/**
 * @file nldiffusion_functions.cpp
 * @brief Functions for nonlinear diffusion filtering applications
 * @date Oct 07, 2014
 * @author Pablo F. Alcantarilla, Jesus Nuevo
 */

#include "nldiffusion_functions.h"

using namespace std;

/* ************************************************************************* */
void gaussian_2D_convolution(const cv::Mat& src, cv::Mat& dst, size_t ksize_x,
                             size_t ksize_y, float sigma) {

  // Compute an appropriate kernel size according to the specified sigma
  if (sigma > ksize_x || sigma > ksize_y || ksize_x == 0 || ksize_y == 0) {
    ksize_x = ceil(2.0*(1.0 + (sigma-0.8)/(0.3)));
    ksize_y = ksize_x;
  }

  // The kernel size must be and odd number
  if ((ksize_x % 2) == 0)
    ksize_x += 1;

  if ((ksize_y % 2) == 0)
    ksize_y += 1;

  // Perform the Gaussian Smoothing with border replication
  cv::GaussianBlur(src, dst, cv::Size(ksize_x, ksize_y), sigma, sigma, cv::BORDER_REPLICATE);
}

/* ************************************************************************* */
void image_derivatives_scharr(const cv::Mat& src, cv::Mat& dst,
                              const size_t xorder, const size_t yorder) {
  cv::Scharr(src, dst, CV_32F, xorder, yorder, 1.0, 0, cv::BORDER_DEFAULT);
}

/* ************************************************************************* */
void pm_g1(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k) {

  cv::Size sz = Lx.size();
  float inv_k = 1.0 / (k*k);
  for (int y = 0; y < sz.height; y++) {
    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);
    float* dst_row = dst.ptr<float>(y);
    for (int x = 0; x < sz.width; x++)
      dst_row[x] = (-inv_k*(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]));
  }

  cv::exp(dst, dst);
}

/* ************************************************************************* */
void pm_g2(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k) {

  cv::Size sz = Lx.size();
  float inv_k = 1.0 / (k*k);
  for (int y = 0; y < sz.height; y++) {
    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);
    float* dst_row = dst.ptr<float>(y);
    for (int x = 0; x < sz.width; x++)
      dst_row[x] = 1.0 / (1.0+inv_k*(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]));
  }
}

/* ************************************************************************* */
void weickert_diffusivity(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k) {

  cv::Size sz = Lx.size();
  float inv_k = 1.0 / (k*k);
  for (int y = 0; y < sz.height; y++) {
    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);
    float* dst_row = dst.ptr<float>(y);
    for (int x = 0; x < sz.width; x++) {
      float dL = inv_k*(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]);
      dst_row[x] = -3.315/(dL*dL*dL*dL);
    }
  }

  cv::exp(dst, dst);
  dst = 1.0 - dst;
}

/* ************************************************************************* */
void charbonnier_diffusivity(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k) {

  cv::Size sz = Lx.size();
  float inv_k = 1.0 / (k*k);
  for (int y = 0; y < sz.height; y++) {
    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);
    float* dst_row = dst.ptr<float>(y);
    for (int x = 0; x < sz.width; x++) {
      float den = sqrt(1.0+inv_k*(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]));
      dst_row[x] = 1.0 / den;
    }
  }
}

/* ************************************************************************* */
float compute_k_percentile(const cv::Mat& img, float perc, float gscale,
                           size_t nbins, size_t ksize_x, size_t ksize_y) {

  size_t nbin = 0, nelements = 0, nthreshold = 0, k = 0;
  float kperc = 0.0, modg = 0.0, npoints = 0.0, hmax = 0.0;

  // Create the array for the histogram
  float* hist = new float[nbins];

  // Create the matrices
  cv::Mat gaussian = cv::Mat::zeros(img.rows, img.cols, CV_32F);
  cv::Mat Lx = cv::Mat::zeros(img.rows, img.cols, CV_32F);
  cv::Mat Ly = cv::Mat::zeros(img.rows, img.cols, CV_32F);

  // Set the histogram to zero
  for (size_t i = 0; i < nbins; i++)
    hist[i] = 0.0;

  // Perform the Gaussian convolution
  gaussian_2D_convolution(img, gaussian, ksize_x, ksize_y, gscale);

  // Compute the Gaussian derivatives Lx and Ly
  image_derivatives_scharr(gaussian, Lx, 1, 0);
  image_derivatives_scharr(gaussian, Ly, 0, 1);

  // Skip the borders for computing the histogram
  for (int y = 1; y < gaussian.rows-1; y++) {

    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);

    for (int x = 1; x < gaussian.cols-1; x++) {

      modg = sqrt(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]);

      // Get the maximum
      if (modg > hmax)
        hmax = modg;
    }
  }

  // Skip the borders for computing the histogram
  for (int y = 1; y < gaussian.rows-1; y++) {

    const float* Lx_row = Lx.ptr<float>(y);
    const float* Ly_row = Ly.ptr<float>(y);

    for (int x = 1; x < gaussian.cols-1; x++) {

      modg = sqrt(Lx_row[x]*Lx_row[x] + Ly_row[x]*Ly_row[x]);

      // Find the correspondent bin
      if (modg != 0.0) {
        nbin = floor(nbins*(modg/hmax));

        if (nbin == nbins) {
          nbin--;
        }

        hist[nbin]++;
        npoints++;
      }
    }
  }

  // Now find the perc of the histogram percentile
  nthreshold = (size_t)(npoints*perc);

  for (k = 0; nelements < nthreshold && k < nbins; k++)
    nelements = nelements + hist[k];

  if (nelements < nthreshold)
    kperc = 0.03;
  else
    kperc = hmax*((float)(k)/(float)nbins);

  delete [] hist;
  return kperc;
}

/* ************************************************************************* */
void compute_scharr_derivatives(const cv::Mat& src, cv::Mat& dst, const size_t xorder,
                                const size_t yorder, const size_t scale) {

  cv::Mat kx, ky;
  compute_derivative_kernels(kx, ky, xorder, yorder, scale);
  cv::sepFilter2D(src, dst, CV_32F, kx, ky);
}

/* ************************************************************************* */
void nld_step_scalar(cv::Mat& Ld, const cv::Mat& c, cv::Mat& Lstep, const float stepsize) {

  Lstep = cv::Scalar(0);

  // Diffusion all the image except borders
#ifdef _OPENMP
  omp_set_num_threads(OMP_MAX_THREADS);
#pragma omp parallel for schedule(dynamic)
#endif
  for (int y = 1; y < Lstep.rows-1; y++) {
    const float* c_row = c.ptr<float>(y);
    const float* c_row_p = c.ptr<float>(y+1);
    const float* c_row_m = c.ptr<float>(y-1);

    float* Ld_row = Ld.ptr<float>(y);
    float* Ld_row_p = Ld.ptr<float>(y+1);
    float* Ld_row_m = Ld.ptr<float>(y-1);
    float* Lstep_row = Lstep.ptr<float>(y);

    for (int x = 1; x < Lstep.cols-1; x++) {
      float xpos =  (c_row[x]+c_row[x+1])*(Ld_row[x+1]-Ld_row[x]);
      float xneg =  (c_row[x-1]+c_row[x])*(Ld_row[x]-Ld_row[x-1]);
      float ypos =  (c_row[x]+c_row_p[x])*(Ld_row_p[x]-Ld_row[x]);
      float yneg =  (c_row_m[x]+c_row[x])*(Ld_row[x]-Ld_row_m[x]);
      Lstep_row[x] = 0.5*stepsize*(xpos-xneg + ypos-yneg);
    }
  }

  // First row
  const float* c_row = c.ptr<float>(0);
  const float* c_row_p = c.ptr<float>(1);
  float* Ld_row = Ld.ptr<float>(0);
  float* Ld_row_p = Ld.ptr<float>(1);
  float* Lstep_row = Lstep.ptr<float>(0);

  for (int x = 1; x < Lstep.cols-1; x++) {
    float xpos = (c_row[x]+c_row[x+1])*(Ld_row[x+1]-Ld_row[x]);
    float xneg = (c_row[x-1]+c_row[x])*(Ld_row[x]-Ld_row[x-1]);
    float ypos = (c_row[x]+c_row_p[x])*(Ld_row_p[x]-Ld_row[x]);
    Lstep_row[x] = 0.5*stepsize*(xpos-xneg + ypos);
  }

  float xpos = (c_row[0]+c_row[1])*(Ld_row[1]-Ld_row[0]);
  float ypos = (c_row[0]+c_row_p[0])*(Ld_row_p[0]-Ld_row[0]);
  Lstep_row[0] = 0.5*stepsize*(xpos + ypos);

  int x = Lstep.cols-1;
  float xneg = (c_row[x-1]+c_row[x])*(Ld_row[x]-Ld_row[x-1]);
  ypos = (c_row[x]+c_row_p[x])*(Ld_row_p[x]-Ld_row[x]);
  Lstep_row[x] = 0.5*stepsize*(-xneg + ypos);

  // Last row
  c_row = c.ptr<float>(Lstep.rows-1);
  c_row_p = c.ptr<float>(Lstep.rows-2);
  Ld_row = Ld.ptr<float>(Lstep.rows-1);
  Ld_row_p = Ld.ptr<float>(Lstep.rows-2);
  Lstep_row = Lstep.ptr<float>(Lstep.rows-1);

  for (int x = 1; x < Lstep.cols-1; x++) {
    float xpos = (c_row[x]+c_row[x+1])*(Ld_row[x+1]-Ld_row[x]);
    float xneg = (c_row[x-1]+c_row[x])*(Ld_row[x]-Ld_row[x-1]);
    float ypos = (c_row[x]+c_row_p[x])*(Ld_row_p[x]-Ld_row[x]);
    Lstep_row[x] = 0.5*stepsize*(xpos-xneg + ypos);
  }

  xpos = (c_row[0]+c_row[1])*(Ld_row[1]-Ld_row[0]);
  ypos = (c_row[0]+c_row_p[0])*(Ld_row_p[0]-Ld_row[0]);
  Lstep_row[0] = 0.5*stepsize*(xpos + ypos);

  x = Lstep.cols-1;
  xneg = (c_row[x-1]+c_row[x])*(Ld_row[x]-Ld_row[x-1]);
  ypos = (c_row[x]+c_row_p[x])*(Ld_row_p[x]-Ld_row[x]);
  Lstep_row[x] = 0.5*stepsize*(-xneg + ypos);

  // First and last columns
  for (int i = 1; i < Lstep.rows-1; i++) {

    const float* c_row = c.ptr<float>(i);
    const float* c_row_m = c.ptr<float>(i-1);
    const float* c_row_p = c.ptr<float>(i+1);
    float* Ld_row = Ld.ptr<float>(i);
    float* Ld_row_p = Ld.ptr<float>(i+1);
    float* Ld_row_m = Ld.ptr<float>(i-1);
    Lstep_row = Lstep.ptr<float>(i);

    float xpos = (c_row[0]+c_row[1])*(Ld_row[1]-Ld_row[0]);
    float ypos = (c_row[0]+c_row_p[0])*(Ld_row_p[0]-Ld_row[0]);
    float yneg = (c_row_m[0]+c_row[0])*(Ld_row[0]-Ld_row_m[0]);
    Lstep_row[0] = 0.5*stepsize*(xpos+ypos-yneg);

    float xneg = (c_row[Lstep.cols-2]+c_row[Lstep.cols-1])*(Ld_row[Lstep.cols-1]-Ld_row[Lstep.cols-2]);
    ypos = (c_row[Lstep.cols-1]+c_row_p[Lstep.cols-1])*(Ld_row_p[Lstep.cols-1]-Ld_row[Lstep.cols-1]);
    yneg = (c_row_m[Lstep.cols-1]+c_row[Lstep.cols-1])*(Ld_row[Lstep.cols-1]-Ld_row_m[Lstep.cols-1]);
    Lstep_row[Lstep.cols-1] = 0.5*stepsize*(-xneg+ypos-yneg);
  }

  // Ld = Ld + Lstep
  for (int y = 0; y < Lstep.rows; y++) {
    float* Ld_row = Ld.ptr<float>(y);
    float* Lstep_row = Lstep.ptr<float>(y);
    for (int x = 0; x < Lstep.cols; x++) {
      Ld_row[x] = Ld_row[x] + Lstep_row[x];
    }
  }
}

/* ************************************************************************* */
void halfsample_image(const cv::Mat& src, cv::Mat& dst) {

  // Make sure the destination image is of the right size
  cv::resize(src, dst, dst.size(), 0, 0, cv::INTER_AREA);
}

/* ************************************************************************* */
void compute_derivative_kernels(cv::OutputArray kx_, cv::OutputArray ky_,
                                const size_t dx, const size_t dy, const size_t scale) {

  const int ksize = 3 + 2*(scale-1);

  // The usual Scharr kernel
  if (scale == 1) {
    cv::getDerivKernels(kx_, ky_, dx, dy, 0, true, CV_32F);
    return;
  }

  kx_.create(ksize,1,CV_32F,-1,true);
  ky_.create(ksize,1,CV_32F,-1,true);
  cv::Mat kx = kx_.getMat();
  cv::Mat ky = ky_.getMat();

  float w = 10.0/3.0;
  float norm = 1.0/(2.0*scale*(w+2.0));

  for (int k = 0; k < 2; k++) {
    cv::Mat* kernel = k == 0 ? &kx : &ky;
    int order = k == 0 ? dx : dy;
    float kerI[1000];

    for (int t = 0; t<ksize; t++)
      kerI[t] = 0;

    if (order == 0) {
      kerI[0] = norm;
      kerI[ksize/2] = w*norm;
      kerI[ksize-1] = norm;
    }
    else if (order == 1) {
      kerI[0] = -1;
      kerI[ksize/2] = 0;
      kerI[ksize-1] = 1;
    }

    cv::Mat temp(kernel->rows, kernel->cols, CV_32F, &kerI[0]);
    temp.copyTo(*kernel);
  }
}

/* ************************************************************************* */
bool check_maximum_neighbourhood(const cv::Mat& img, int dsize, float value,
                                 int row, int col, bool same_img) {

  bool response = true;

  for (int i = row-dsize; i <= row+dsize; i++) {
    for (int j = col-dsize; j <= col+dsize; j++) {
      if (i >= 0 && i < img.rows && j >= 0 && j < img.cols) {
        if (same_img == true) {
          if (i != row || j != col) {
            if ((*(img.ptr<float>(i)+j)) > value) {
              response = false;
              return response;
            }
          }
        }
        else {
          if ((*(img.ptr<float>(i)+j)) > value) {
            response = false;
            return response;
          }
        }
      }
    }
  }

  return response;
}
