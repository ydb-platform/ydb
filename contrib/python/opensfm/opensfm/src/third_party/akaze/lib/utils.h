/**
 * @file utils.h
 * @brief Some utilities functions
 * @date Oct 07, 2014
 * @author Pablo F. Alcantarilla, Jesus Nuevo
 */

#pragma once

/* ************************************************************************* */
// OpenCV
#include <opencv2/features2d.hpp>

// System
#include <vector>
#include <iostream>
#include <iomanip>
#include <sstream>

/* ************************************************************************* */
// Stringify common types such as int, double and others.
template <typename T>
inline std::string to_string(const T& x) {
  std::stringstream oss;
  oss << x;
  return oss.str();
}

/* ************************************************************************* */
// Stringify and format integral types as follows:
// to_formatted_string(  1, 2) produces string:  '01'
// to_formatted_string(  5, 2) produces string:  '05'
// to_formatted_string( 19, 2) produces string:  '19'
// to_formatted_string( 19, 3) produces string: '019'
template <typename Integer>
inline std::string to_formatted_string(Integer x, int num_digits) {
  std::stringstream oss;
  oss << std::setfill('0') << std::setw(num_digits) << x;
  return oss.str();
}

/* ************************************************************************* */
/// Compute the minimum value of a float image
void compute_min_32F(const cv::Mat& src, float& value);

/// Compute the maximum value of a float image
void compute_max_32F(const cv::Mat& src, float& value);

/// Convert the scale of the input image prior to visualization
void convert_scale(cv::Mat& src);

/// This function copies the input image and converts the scale of the copied image prior visualization
void copy_and_convert_scale(const cv::Mat& src, cv::Mat& dst);

/// This function draws the list of detected keypoints
void draw_keypoints(cv::Mat& img, const std::vector<cv::KeyPoint>& kpts);

/// This function saves the interest points to a regular ASCII file
/// @note The format is compatible with Mikolajczyk and Schmid evaluation
/// @param outFile Name of the output file where the points will be stored
/// @param kpts Vector of points of interest
/// @param desc Matrix that contains the extracted descriptors
/// @param save_desc Set to 1 if we want to save the descriptors
int save_keypoints(const std::string& outFile, const std::vector<cv::KeyPoint>& kpts, const cv::Mat& desc, bool save_desc);

/// This function converts matches to points using nearest neighbor distance
/// ratio matching strategy
/// @param train Vector of keypoints from the first image
/// @param query Vector of keypoints from the second image
/// @param matches Vector of nearest neighbors for each keypoint
/// @param pmatches Vector of putative matches
/// @param nndr Nearest neighbor distance ratio value
void matches2points_nndr(const std::vector<cv::KeyPoint>& train,
                         const std::vector<cv::KeyPoint>& query,
                         const std::vector<std::vector<cv::DMatch> >& matches,
                         std::vector<cv::Point2f>& pmatches, float nndr);

/// This function computes the set of inliers estimating the fundamental matrix
/// or a planar homography in a RANSAC procedure
/// @param matches Vector of putative matches
/// @param inliers Vector of inliers
/// @param error The minimum pixelic error to accept an inlier
/// @param use_fund Set to true if you want to compute a fundamental matrix
void compute_inliers_ransac(const std::vector<cv::Point2f>& matches,
                            std::vector<cv::Point2f>& inliers, float error, bool use_fund);

/// This function computes the set of inliers given a ground truth homography
/// @param matches Vector of putative matches
/// @param inliers Vector of inliers
/// @param H Ground truth homography matrix 3x3
/// @param min_error The minimum pixelic error to accept an inlier
void compute_inliers_homography(const std::vector<cv::Point2f>& matches,
                                std::vector<cv::Point2f> &inliers,
                                const cv::Mat&H, float min_error);

/// This function draws the set of the inliers between the two images
/// @param img1 First image
/// @param img2 Second image
/// @param img_com Image with the inliers
/// @param ptpairs Vector of point pairs with the set of inliers
void draw_inliers(const cv::Mat& img1, const cv::Mat& img2, cv::Mat& img_com,
                  const std::vector<cv::Point2f>& ptpairs);

/// This function draws the set of the inliers between the two images
/// @param img1 First image
/// @param img2 Second image
/// @param img_com Image with the inliers
/// @param ptpairs Vector of point pairs with the set of inliers
/// @param color The color for each method
void draw_inliers(const cv::Mat& img1, const cv::Mat& img2, cv::Mat& img_com,
                  const std::vector<cv::Point2f>& ptpairs, int color);

/// Function for reading the ground truth homography from a txt file
bool read_homography(const std::string& hFile, cv::Mat& H1toN);

/// This function shows the possible command line configuration options
void show_input_options_help(int example);

