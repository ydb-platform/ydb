/**
 * @file nldiffusion_functions.h
 * @brief Functions for nonlinear diffusion filtering applications
 * @date Oct 07, 2014
 * @author Pablo F. Alcantarilla, Jesus Nuevo
 */

#pragma once

/* ************************************************************************* */
#include <AKAZEConfig.h>

/* ************************************************************************* */
/// Convolve an image with a 2D Gaussian kernel
void gaussian_2D_convolution(const cv::Mat& src, cv::Mat& dst, size_t ksize_x, size_t ksize_y, float sigma);

/// This function computes image derivatives with Scharr kernel
/// @param src Input image
/// @param dst Output image
/// @param xorder Derivative order in X-direction (horizontal)
/// @param yorder Derivative order in Y-direction (vertical)
/// @note Scharr operator approximates better rotation invariance than
/// other stencils such as Sobel. See Weickert and Scharr,
/// A Scheme for Coherence-Enhancing Diffusion Filtering with Optimized Rotation Invariance,
/// Journal of Visual Communication and Image Representation 2002
void image_derivatives_scharr(const cv::Mat& src, cv::Mat& dst,
                              const size_t xorder, const size_t yorder);

/// This function computes the Perona and Malik conductivity coefficient g1
/// g1 = exp(-|dL|^2/k^2)
/// @param Lx First order image derivative in X-direction (horizontal)
/// @param Ly First order image derivative in Y-direction (vertical)
/// @param dst Output image
/// @param k Contrast factor parameter
void pm_g1(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k);

/// This function computes the Perona and Malik conductivity coefficient g2
/// g2 = 1 / (1 + dL^2 / k^2)
/// @param Lx First order image derivative in X-direction (horizontal)
/// @param Ly First order image derivative in Y-direction (vertical)
/// @param dst Output image
/// @param k Contrast factor parameter
void pm_g2(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k);

/// This function computes Weickert conductivity coefficient gw
/// @param Lx First order image derivative in X-direction (horizontal)
/// @param Ly First order image derivative in Y-direction (vertical)
/// @param dst Output image
/// @param k Contrast factor parameter
/// @note For more information check the following paper: J. Weickert
/// Applications of nonlinear diffusion in image processing and computer vision,
/// Proceedings of Algorithmy 2000
void weickert_diffusivity(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k);

/// This function computes Charbonnier conductivity coefficient gc
/// gc = 1 / sqrt(1 + dL^2 / k^2)
/// @param Lx First order image derivative in X-direction (horizontal)
/// @param Ly First order image derivative in Y-direction (vertical)
/// @param dst Output image
/// @param k Contrast factor parameter
/// @note For more information check the following paper: J. Weickert
/// Applications of nonlinear diffusion in image processing and computer vision,
/// Proceedings of Algorithmy 2000
void charbonnier_diffusivity(const cv::Mat& Lx, const cv::Mat& Ly, cv::Mat& dst, const float k);

/// This function computes a good empirical value for the k contrast factor
/// given an input image, the percentile (0-1), the gradient scale and the number of bins in the histogram
/// @param img Input image
/// @param perc Percentile of the image gradient histogram (0-1)
/// @param gscale Scale for computing the image gradient histogram
/// @param nbins Number of histogram bins
/// @param ksize_x Kernel size in X-direction (horizontal) for the Gaussian smoothing kernel
/// @param ksize_y Kernel size in Y-direction (vertical) for the Gaussian smoothing kernel
/// @return k contrast factor
float compute_k_percentile(const cv::Mat& img, float perc, float gscale,
                           size_t nbins, size_t ksize_x, size_t ksize_y);

/// This function computes Scharr image derivatives
/// @param src Input image
/// @param dst Output image
/// @param xorder Derivative order in X-direction (horizontal)
/// @param yorder Derivative order in Y-direction (vertical)
/// @param scale Scale factor for the derivative size
void compute_scharr_derivatives(const cv::Mat& src, cv::Mat& dst, const size_t xorder,
                                const size_t yorder, const size_t scale);

/// This function performs a scalar non-linear diffusion step
/// @param Ld Output image in the evolution
/// @param c Conductivity image
/// @param Lstep Previous image in the evolution
/// @param stepsize The step size in time units
/// @note Forward Euler Scheme 3x3 stencil
/// The function c is a scalar value that depends on the gradient norm
/// dL_by_ds = d(c dL_by_dx)_by_dx + d(c dL_by_dy)_by_dy
void nld_step_scalar(cv::Mat& Ld, const cv::Mat& c, cv::Mat& Lstep, const float stepsize);

/// This function downsamples the input image using OpenCV resize
/// @param img Input image to be downsampled
/// @param dst Output image with half of the resolution of the input image
void halfsample_image(const cv::Mat& src, cv::Mat& dst);

/// Compute Scharr derivative kernels for sizes different than 3
/// @param kx_ The derivative kernel in x-direction
/// @param ky_ The derivative kernel in y-direction
/// @param dx The derivative order in x-direction
/// @param dy The derivative order in y-direction
/// @param scale The kernel size
void compute_derivative_kernels(cv::OutputArray kx_, cv::OutputArray ky_,
                                const size_t dx, const size_t dy, const size_t scale);

/// This function checks if a given pixel is a maximum in a local neighbourhood
/// @param img Input image where we will perform the maximum search
/// @param dsize Half size of the neighbourhood
/// @param value Response value at (x,y) position
/// @param row Image row coordinate
/// @param col Image column coordinate
/// @param same_img Flag to indicate if the image value at (x,y) is in the input image
/// @return 1->is maximum, 0->otherwise
bool check_maximum_neighbourhood(const cv::Mat& img, int dsize, float value,
                                 int row, int col, bool same_img);
