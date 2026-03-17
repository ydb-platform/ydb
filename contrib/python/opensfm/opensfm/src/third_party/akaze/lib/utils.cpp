//=============================================================================
//
// utils.cpp
// Authors: Pablo F. Alcantarilla (1), Jesus Nuevo (2)
// Institutions: Toshiba Research Europe Ltd (1)
//               TrueVision Solutions (2)
//
// Date: 07/10/2014
// Email: pablofdezalc@gmail.com
//
// AKAZE Features Copyright 2014, Pablo F. Alcantarilla, Jesus Nuevo
// All Rights Reserved
// See LICENSE for the license information
//=============================================================================

/**
 * @file utils.cpp
 * @brief Some utilities functions
 * @date Oct 07, 2014
 * @author Pablo F. Alcantarilla, Jesus Nuevo
 */

#include "utils.h"

// OpenCV
#include <opencv2/opencv.hpp>

// System
#include <fstream>

using namespace std;

/* ************************************************************************* */
void compute_min_32F(const cv::Mat &src, float& value) {

  float aux = 1000.0;
  for (int i = 0; i < src.rows; i++) {
    for (int j = 0; j < src.cols; j++) {
      if (src.at<float>(i,j) < aux)
        aux = src.at<float>(i,j);
    }
  }
  value = aux;
}

/* ************************************************************************* */
void compute_max_32F(const cv::Mat &src, float& value) {

  float aux = 0.0;
  for (int i = 0; i < src.rows; i++) {
    for (int j = 0; j < src.cols; j++) {
      if (src.at<float>(i,j) > aux)
        aux = src.at<float>(i,j);
    }
  }
  value = aux;
}

/* ************************************************************************* */
void convert_scale(cv::Mat& src) {

  float min_val = 0, max_val = 0;
  compute_min_32F(src,min_val);
  src = src - min_val;
  compute_max_32F(src,max_val);
  src = src / max_val;
}

/* ************************************************************************* */
void copy_and_convert_scale(const cv::Mat &src, cv::Mat dst) {

  float min_val = 0, max_val = 0;
  src.copyTo(dst);
  compute_min_32F(dst,min_val);
  dst = dst - min_val;
  compute_max_32F(dst,max_val);
  dst = dst / max_val;
}

/* ************************************************************************* */
void draw_keypoints(cv::Mat& img, const std::vector<cv::KeyPoint>& kpts) {

  int x = 0, y = 0;
  float radius = 0.0;

  for (size_t i = 0; i < kpts.size(); i++) {
    x = (int)(kpts[i].pt.x+.5);
    y = (int)(kpts[i].pt.y+.5);
    radius = kpts[i].size/2.0;
    cv::circle(img, cv::Point(x,y), radius*2.50, cv::Scalar(0,255,0), 1);
    cv::circle(img, cv::Point(x,y), 1.0, cv::Scalar(0,0,255), -1);
  }
}

/* ************************************************************************* */
int save_keypoints(const string& outFile, const std::vector<cv::KeyPoint>& kpts,
                   const cv::Mat& desc, bool save_desc) {

  int nkpts = 0, dsize = 0;
  float sc = 0.0;

  nkpts = (int)(kpts.size());
  dsize = (int)(desc.cols);

  ofstream ipfile(outFile.c_str());

  if (!ipfile) {
    cerr << "Couldn't open file '" << outFile << "'!" << endl;
    return -1;
  }

  if (!save_desc) {
    ipfile << 1 << endl << nkpts << endl;
  }
  else {
    ipfile << dsize << endl << nkpts << endl;
  }

  // Save interest point with descriptor in the format of Krystian Mikolajczyk
  // for reasons of comparison with other descriptors
  for (int i = 0; i < nkpts; i++) {
    // Radius of the keypoint
    sc = (kpts[i].size);
    sc*=sc;

    ipfile  << kpts[i].pt.x /* x-location of the interest point */
            << " " << kpts[i].pt.y /* y-location of the interest point */
            << " " << 1.0/sc /* 1/r^2 */
            << " " << 0.0
            << " " << 1.0/sc; /* 1/r^2 */

    // Here comes the descriptor
    for( int j = 0; j < dsize; j++) {
      if (desc.type() == 0) {
        ipfile << " " << (int)(desc.at<unsigned char>(i,j));
      }
      else {
        ipfile << " " << (desc.at<float>(i,j));
      }
    }

    ipfile << endl;
  }


  // Close the txt file
  ipfile.close();

  return 0;
}

/* ************************************************************************* */
void matches2points_nndr(const std::vector<cv::KeyPoint>& train,
                         const std::vector<cv::KeyPoint>& query,
                         const std::vector<std::vector<cv::DMatch> >& matches,
                         std::vector<cv::Point2f>& pmatches, float nndr) {

  float dist1 = 0.0, dist2 = 0.0;
  for (size_t i = 0; i < matches.size(); i++) {
    cv::DMatch dmatch = matches[i][0];
    dist1 = matches[i][0].distance;
    dist2 = matches[i][1].distance;

    if (dist1 < nndr*dist2) {
      pmatches.push_back(train[dmatch.queryIdx].pt);
      pmatches.push_back(query[dmatch.trainIdx].pt);
    }
  }
}

/* ************************************************************************* */
void compute_inliers_ransac(const std::vector<cv::Point2f>& matches,
                            std::vector<cv::Point2f>& inliers,
                            float error, bool use_fund) {

  vector<cv::Point2f> points1, points2;
  cv::Mat H = cv::Mat::zeros(3,3,CV_32F);
  int npoints = matches.size()/2;
  cv::Mat status = cv::Mat::zeros(npoints,1,CV_8UC1);

  for (size_t i = 0; i < matches.size(); i+=2) {
    points1.push_back(matches[i]);
    points2.push_back(matches[i+1]);
  }

  if (npoints > 8) {
    if (use_fund == true)
      H = cv::findFundamentalMat(points1,points2,cv::FM_RANSAC,error,0.99,status);
    else
      H = cv::findHomography(points1,points2,cv::RANSAC,error,status);

    for (int i = 0; i < npoints; i++) {
      if (status.at<unsigned char>(i) == 1) {
        inliers.push_back(points1[i]);
        inliers.push_back(points2[i]);
      }
    }
  }
}

/* ************************************************************************* */
void compute_inliers_homography(const std::vector<cv::Point2f>& matches,
                                std::vector<cv::Point2f>& inliers, const cv::Mat& H,
                                float min_error) {

  float h11 = 0.0, h12 = 0.0, h13 = 0.0;
  float h21 = 0.0, h22 = 0.0, h23 = 0.0;
  float h31 = 0.0, h32 = 0.0, h33 = 0.0;
  float x1 = 0.0, y1 = 0.0;
  float x2 = 0.0, y2 = 0.0;
  float x2m = 0.0, y2m = 0.0;
  float dist = 0.0, s = 0.0;

  h11 = H.at<float>(0,0);
  h12 = H.at<float>(0,1);
  h13 = H.at<float>(0,2);
  h21 = H.at<float>(1,0);
  h22 = H.at<float>(1,1);
  h23 = H.at<float>(1,2);
  h31 = H.at<float>(2,0);
  h32 = H.at<float>(2,1);
  h33 = H.at<float>(2,2);

  inliers.clear();

  for (size_t i = 0; i < matches.size(); i+=2) {
    x1 = matches[i].x;
    y1 = matches[i].y;
    x2 = matches[i+1].x;
    y2 = matches[i+1].y;

    s = h31*x1 + h32*y1 + h33;
    x2m = (h11*x1 + h12*y1 + h13) / s;
    y2m = (h21*x1 + h22*y1 + h23) / s;
    dist = sqrt( pow(x2m-x2,2) + pow(y2m-y2,2));

    if (dist <= min_error) {
      inliers.push_back(matches[i]);
      inliers.push_back(matches[i+1]);
    }
  }
}

/* ************************************************************************* */
void draw_inliers(const cv::Mat& img1, const cv::Mat& img2, cv::Mat& img_com,
                  const std::vector<cv::Point2f>& ptpairs) {

  int x1 = 0, y1 = 0, x2 = 0, y2 = 0;
  float rows1 = 0.0, cols1 = 0.0;
  float rows2 = 0.0, cols2 = 0.0;
  float ufactor = 0.0, vfactor = 0.0;

  rows1 = img1.rows;
  cols1 = img1.cols;
  rows2 = img2.rows;
  cols2 = img2.cols;
  ufactor = (float)(cols1)/(float)(cols2);
  vfactor = (float)(rows1)/(float)(rows2);

  // This is in case the input images don't have the same resolution
  cv::Mat img_aux = cv::Mat(cv::Size(img1.cols, img1.rows), CV_8UC3);
  cv::resize(img2, img_aux, cv::Size(img1.cols, img1.rows), 0, 0, cv::INTER_LINEAR);

  for (int i = 0; i < img_com.rows; i++) {
    for (int j = 0; j < img_com.cols; j++) {
      if (j < img1.cols) {
        *(img_com.ptr<unsigned char>(i)+3*j) = *(img1.ptr<unsigned char>(i)+3*j);
        *(img_com.ptr<unsigned char>(i)+3*j+1) = *(img1.ptr<unsigned char>(i)+3*j+1);
        *(img_com.ptr<unsigned char>(i)+3*j+2) = *(img1.ptr<unsigned char>(i)+3*j+2);
      }
      else {
        *(img_com.ptr<unsigned char>(i)+3*j) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols));
        *(img_com.ptr<unsigned char>(i)+3*j+1) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols)+1);
        *(img_com.ptr<unsigned char>(i)+3*j+2) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols)+2);
      }
    }
  }

  for (size_t i = 0; i < ptpairs.size(); i+= 2) {
    x1 = (int)(ptpairs[i].x+.5);
    y1 = (int)(ptpairs[i].y+.5);
    x2 = (int)(ptpairs[i+1].x*ufactor+img1.cols+.5);
    y2 = (int)(ptpairs[i+1].y*vfactor+.5);
    cv::line(img_com, cv::Point(x1,y1), cv::Point(x2,y2), cv::Scalar(255,0,0), 2);
  }
}

/* ************************************************************************* */
void draw_inliers(const cv::Mat& img1, const cv::Mat& img2, cv::Mat& img_com,
                  const std::vector<cv::Point2f>& ptpairs, int color) {

  int x1 = 0, y1 = 0, x2 = 0, y2 = 0;
  float rows1 = 0.0, cols1 = 0.0;
  float rows2 = 0.0, cols2 = 0.0;
  float ufactor = 0.0, vfactor = 0.0;

  rows1 = img1.rows;
  cols1 = img1.cols;
  rows2 = img2.rows;
  cols2 = img2.cols;
  ufactor = (float)(cols1)/(float)(cols2);
  vfactor = (float)(rows1)/(float)(rows2);

  // This is in case the input images don't have the same resolution
  cv::Mat img_aux = cv::Mat(cv::Size(img1.cols, img1.rows), CV_8UC3);
  cv::resize(img2, img_aux, cv::Size(img1.cols, img1.rows), 0, 0, cv::INTER_LINEAR);

  for (int i = 0; i < img_com.rows; i++) {
    for (int j = 0; j < img_com.cols; j++) {
      if (j < img1.cols) {
        *(img_com.ptr<unsigned char>(i)+3*j) = *(img1.ptr<unsigned char>(i)+3*j);
        *(img_com.ptr<unsigned char>(i)+3*j+1) = *(img1.ptr<unsigned char>(i)+3*j+1);
        *(img_com.ptr<unsigned char>(i)+3*j+2) = *(img1.ptr<unsigned char>(i)+3*j+2);
      }
      else {
        *(img_com.ptr<unsigned char>(i)+3*j) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols));
        *(img_com.ptr<unsigned char>(i)+3*j+1) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols)+1);
        *(img_com.ptr<unsigned char>(i)+3*j+2) = *(img2.ptr<unsigned char>(i)+3*(j-img_aux.cols)+2);
      }
    }
  }

  for (size_t i = 0; i < ptpairs.size(); i+= 2) {
    x1 = (int)(ptpairs[i].x+.5);
    y1 = (int)(ptpairs[i].y+.5);
    x2 = (int)(ptpairs[i+1].x*ufactor+img1.cols+.5);
    y2 = (int)(ptpairs[i+1].y*vfactor+.5);

    if (color == 0)
      cv::line(img_com, cv::Point(x1,y1), cv::Point(x2,y2), cv::Scalar(255,255,0), 2);
    else if (color == 1)
      cv::line(img_com, cv::Point(x1,y1), cv::Point(x2,y2), cv::Scalar(255,0,0), 2);
    else if (color == 2)
      cv::line(img_com, cv::Point(x1,y1), cv::Point(x2,y2), cv::Scalar(0,0,255), 2);
  }
}

/* ************************************************************************* */
bool read_homography(const string& hFile, cv::Mat& H1toN) {

  float h11 = 0.0, h12 = 0.0, h13 = 0.0;
  float h21 = 0.0, h22 = 0.0, h23 = 0.0;
  float h31 = 0.0, h32 = 0.0, h33 = 0.0;
  const int tmp_buf_size = 256;
  char tmp_buf[tmp_buf_size];

  // Allocate memory for the OpenCV matrices
  H1toN = cv::Mat::zeros(3,3,CV_32FC1);

  string filename(hFile);
  ifstream pf;
  pf.open(filename.c_str(), std::ifstream::in);

  if (!pf.is_open())
    return false;

  pf.getline(tmp_buf,tmp_buf_size);
  sscanf(tmp_buf,"%f %f %f",&h11,&h12,&h13);

  pf.getline(tmp_buf,tmp_buf_size);
  sscanf(tmp_buf,"%f %f %f",&h21,&h22,&h23);

  pf.getline(tmp_buf,tmp_buf_size);
  sscanf(tmp_buf,"%f %f %f",&h31,&h32,&h33);

  pf.close();

  H1toN.at<float>(0,0) = h11 / h33;
  H1toN.at<float>(0,1) = h12 / h33;
  H1toN.at<float>(0,2) = h13 / h33;

  H1toN.at<float>(1,0) = h21 / h33;
  H1toN.at<float>(1,1) = h22 / h33;
  H1toN.at<float>(1,2) = h23 / h33;

  H1toN.at<float>(2,0) = h31 / h33;
  H1toN.at<float>(2,1) = h32 / h33;
  H1toN.at<float>(2,2) = h33 / h33;

  return true;
}

/* ************************************************************************* */
const size_t length = string("--descriptor_channels").size() + 2;
static inline std::ostream& cout_help() {
  cout << setw(length);
  return cout;
}

/* ************************************************************************* */
static inline std::string toUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), ::toupper);
  return s;
}

/* ************************************************************************* */
void show_input_options_help(int example) {

  fflush(stdout);
  cout << "A-KAZE Features" << endl;
  cout << "Usage: ";

  if (example == 0) {
    cout << "./akaze_features img.jpg [options]" << endl;
  }
  else if (example == 1) {
    cout << "./akaze_match img1.jpg img2.pgm [homography.txt] [options]" << endl;
  }
  else if (example == 2) {
    cout << "./akaze_compare img1.jpg img2.pgm [homography.txt] [options]" << endl;
  }
  
  cout << endl;
  cout_help() << "homography.txt is optional. If the txt file is not provided a planar homography will be estimated using RANSAC" << endl;

  cout << endl;
  cout_help() << "Options below are not mandatory. Unless specified, default arguments are used." << endl << endl;  

  // Justify on the left
  cout << left;

  // Generalities
  cout_help() << "--help" << "Show the command line options" << endl;
  cout_help() << "--verbose " << "Verbosity is required" << endl;
  cout_help() << endl;

  // Scale-space parameters
  cout_help() << "--soffset" << "Base scale offset (sigma units)" << endl;
  cout_help() << "--omax" << "Maximum octave of image evolution" << endl;
  cout_help() << "--nsublevels" << "Number of sublevels per octave" << endl;
  cout_help() << "--diffusivity" << "Diffusivity function. Possible values:" << endl;
  cout_help() << " " << "0 -> Perona-Malik, g1 = exp(-|dL|^2/k^2)" << endl;
  cout_help() << " " << "1 -> Perona-Malik, g2 = 1 / (1 + dL^2 / k^2)" << endl;
  cout_help() << " " << "2 -> Weickert diffusivity" << endl;
  cout_help() << " " << "3 -> Charbonnier diffusivity" << endl;
  cout_help() << endl;

  // Feature detection parameters.
  cout_help() << "--dthreshold" << "Feature detector threshold response for keypoints" << endl;
  cout_help() << " " << "(0.001 can be a good value)" << endl;
  cout_help() << endl;
  cout_help() << endl;

  // Descriptor parameters.
  cout_help() << "--descriptor" << "Descriptor Type. Possible values:" << endl;
  cout_help() << " " << "0 -> SURF_UPRIGHT" << endl;
  cout_help() << " " << "1 -> SURF" << endl;
  cout_help() << " " << "2 -> M-SURF_UPRIGHT," << endl;
  cout_help() << " " << "3 -> M-SURF" << endl;
  cout_help() << " " << "4 -> M-LDB_UPRIGHT" << endl;
  cout_help() << " " << "5 -> M-LDB" << endl;
  cout_help() << endl;

  cout_help() << "--descriptor_channels " << "Descriptor Channels for M-LDB. Valid values: " << endl;
  cout_help() << " " << "1 -> intensity" << endl;
  cout_help() << " " << "2 -> intensity + gradient magnitude" << endl;
  cout_help() << " " << "3 -> intensity + X and Y gradients" <<endl;
  cout_help() << endl;

  cout_help() << "--descriptor_size" << "Descriptor size for M-LDB in bits." << endl;
  cout_help() << " " << "0: means the full length descriptor (486)!!" << endl;
  cout_help() << endl;

  // Save results?
  cout_help() << "--show_results" << "Possible values below:" << endl;
  cout_help() << " " << "1 -> show detection results." << endl;
  cout_help() << " " << "0 -> don't show detection results" << endl;
  cout_help() << endl;
}
