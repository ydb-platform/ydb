import os
import yaml

default_config_yaml = '''
# Metadata
use_exif_size: yes
default_focal_prior: 0.85

# Params for features
feature_type: HAHOG           # Feature type (AKAZE, SURF, SIFT, HAHOG, ORB)
feature_root: 1               # If 1, apply square root mapping to features
feature_min_frames: 4000      # If fewer frames are detected, sift_peak_threshold/surf_hessian_threshold is reduced.
feature_process_size: 2048    # Resize the image if its size is larger than specified. Set to -1 for original size
feature_use_adaptive_suppression: no

# Params for SIFT
sift_peak_threshold: 0.1     # Smaller value -> more features
sift_edge_threshold: 10       # See OpenCV doc

# Params for SURF
surf_hessian_threshold: 3000  # Smaller value -> more features
surf_n_octaves: 4             # See OpenCV doc
surf_n_octavelayers: 2        # See OpenCV doc
surf_upright: 0               # See OpenCV doc

# Params for AKAZE (See details in lib/src/third_party/akaze/AKAZEConfig.h)
akaze_omax: 4                      # Maximum octave evolution of the image 2^sigma (coarsest scale sigma units)
akaze_dthreshold: 0.001            # Detector response threshold to accept point
akaze_descriptor: MSURF            # Feature type
akaze_descriptor_size: 0           # Size of the descriptor in bits. 0->Full size
akaze_descriptor_channels: 3       # Number of feature channels (1,2,3)
akaze_kcontrast_percentile: 0.7
akaze_use_isotropic_diffusion: no

# Params for HAHOG
hahog_peak_threshold: 0.00001
hahog_edge_threshold: 10
hahog_normalize_to_uchar: yes

# Params for general matching
lowes_ratio: 0.8              # Ratio test for matches
matcher_type: FLANN           # FLANN, BRUTEFORCE, or WORDS
symmetric_matching: yes       # Match symmetricly or one-way

# Params for FLANN matching
flann_branching: 8           # See OpenCV doc
flann_iterations: 10          # See OpenCV doc
flann_checks: 20             # Smaller -> Faster (but might lose good matches)

# Params for BoW matching
bow_file: bow_hahog_root_uchar_10000.npz
bow_words_to_match: 50        # Number of words to explore per feature.
bow_num_checks: 20            # Number of matching features to check.
bow_matcher_type: FLANN       # Matcher type to assign words to features

# Params for VLAD matching
vlad_file: bow_hahog_root_uchar_64.npz

# Params for matching
matching_gps_distance: 150            # Maximum gps distance between two images for matching
matching_gps_neighbors: 0             # Number of images to match selected by GPS distance. Set to 0 to use no limit (or disable if matching_gps_distance is also 0)
matching_time_neighbors: 0            # Number of images to match selected by time taken. Set to 0 to disable
matching_order_neighbors: 0           # Number of images to match selected by image name. Set to 0 to disable
matching_bow_neighbors: 0             # Number of images to match selected by BoW distance. Set to 0 to disable
matching_bow_gps_distance: 0          # Maximum GPS distance for preempting images before using selection by BoW distance. Set to 0 to disable
matching_bow_gps_neighbors: 0         # Number of images (selected by GPS distance) to preempt before using selection by BoW distance. Set to 0 to use no limit (or disable if matching_bow_gps_distance is also 0)
matching_bow_other_cameras: False     # If True, BoW image selection will use N neighbors from the same camera + N neighbors from any different camera.
matching_vlad_neighbors: 0            # Number of images to match selected by VLAD distance. Set to 0 to disable
matching_vlad_gps_distance: 0          # Maximum GPS distance for preempting images before using selection by VLAD distance. Set to 0 to disable
matching_vlad_gps_neighbors: 0         # Number of images (selected by GPS distance) to preempt before using selection by VLAD distance. Set to 0 to use no limit (or disable if matching_vlad_gps_distance is also 0)
matching_vlad_other_cameras: False     # If True, VLAD image selection will use N neighbors from the same camera + N neighbors from any different camera.
matching_use_filters: False           # If True, removes static matches using ad-hoc heuristics

# Params for geometric estimation
robust_matching_threshold: 0.004        # Outlier threshold for fundamental matrix estimation as portion of image width
robust_matching_calib_threshold: 0.004  # Outlier threshold for essential matrix estimation during matching in radians
robust_matching_min_match: 20           # Minimum number of matches to accept matches between two images
five_point_algo_threshold: 0.004        # Outlier threshold for essential matrix estimation during incremental reconstruction in radians
five_point_algo_min_inliers: 20         # Minimum number of inliers for considering a two view reconstruction valid
triangulation_threshold: 0.006          # Outlier threshold for accepting a triangulated point in radians
triangulation_min_ray_angle: 1.0        # Minimum angle between views to accept a triangulated point
triangulation_type: FULL                # Triangulation type : either considering all rays (FULL), or sing a RANSAC variant (ROBUST)
resection_threshold: 0.004              # Outlier threshold for resection in radians
resection_min_inliers: 10               # Minimum number of resection inliers to accept it

# Params for track creation
min_track_length: 2             # Minimum number of features/images per track

# Params for bundle adjustment
loss_function: SoftLOneLoss     # Loss function for the ceres problem (see: http://ceres-solver.org/modeling.html#lossfunction)
loss_function_threshold: 1      # Threshold on the squared residuals.  Usually cost is quadratic for smaller residuals and sub-quadratic above.
reprojection_error_sd: 0.004    # The standard deviation of the reprojection error
exif_focal_sd: 0.01             # The standard deviation of the exif focal length in log-scale
principal_point_sd: 0.01        # The standard deviation of the principal point coordinates
radial_distorsion_k1_sd: 0.01   # The standard deviation of the first radial distortion parameter
radial_distorsion_k2_sd: 0.01   # The standard deviation of the second radial distortion parameter
radial_distorsion_k3_sd: 0.01   # The standard deviation of the third radial distortion parameter
radial_distorsion_p1_sd: 0.01   # The standard deviation of the first tangential distortion parameter
radial_distorsion_p2_sd: 0.01   # The standard deviation of the second tangential distortion parameter
bundle_outlier_filtering_type: FIXED    # Type of threshold for filtering outlier : either fixed value (FIXED) or based on actual distribution (AUTO)
bundle_outlier_auto_ratio: 3.0          # For AUTO filtering type, projections with larger reprojection than ratio-times-mean, are removed
bundle_outlier_fixed_threshold: 0.006   # For FIXED filtering type, projections with larger reprojection error after bundle adjustment are removed
optimize_camera_parameters: yes         # Optimize internal camera parameters during bundle

retriangulation: yes                # Retriangulate all points from time to time
retriangulation_ratio: 1.2          # Retriangulate when the number of points grows by this ratio
bundle_interval: 999999             # Bundle after adding 'bundle_interval' cameras
bundle_new_points_ratio: 1.2        # Bundle when the number of points grows by this ratio
local_bundle_radius: 3              # Max image graph distance for images to be included in local bundle adjustment
local_bundle_min_common_points: 20  # Minimum number of common points betwenn images to be considered neighbors
local_bundle_max_shots: 30          # Max number of shots to optimize during local bundle adjustment

save_partial_reconstructions: no    # Save reconstructions at every iteration

# Params for GPS alignment
use_altitude_tag: no                  # Use or ignore EXIF altitude tag
align_method: orientation_prior       # orientation_prior or naive
align_orientation_prior: horizontal   # horizontal, vertical or no_roll
bundle_use_gps: yes                   # Enforce GPS position in bundle adjustment
bundle_use_gcp: no                    # Enforce Ground Control Point position in bundle adjustment

# Params for navigation graph
nav_min_distance: 0.01                # Minimum distance for a possible edge between two nodes
nav_step_pref_distance: 6             # Preferred distance between camera centers
nav_step_max_distance: 20             # Maximum distance for a possible step edge between two nodes
nav_turn_max_distance: 15             # Maximum distance for a possible turn edge between two nodes
nav_step_forward_view_threshold: 15   # Maximum difference of angles in degrees between viewing directions for forward steps
nav_step_view_threshold: 30           # Maximum difference of angles in degrees between viewing directions for other steps
nav_step_drift_threshold: 36          # Maximum motion drift with respect to step directions for steps in degrees
nav_turn_view_threshold: 40           # Maximum difference of angles in degrees with respect to turn directions
nav_vertical_threshold: 20            # Maximum vertical angle difference in motion and viewing direction in degrees
nav_rotation_threshold: 30            # Maximum general rotation in degrees between cameras for steps

# Params for image undistortion
undistorted_image_format: jpg         # Format in which to save the undistorted images
undistorted_image_max_size: 100000    # Max width and height of the undistorted image

# Params for depth estimation
depthmap_method: PATCH_MATCH_SAMPLE   # Raw depthmap computation algorithm (PATCH_MATCH, BRUTE_FORCE, PATCH_MATCH_SAMPLE)
depthmap_resolution: 640              # Resolution of the depth maps
depthmap_num_neighbors: 10            # Number of neighboring views
depthmap_num_matching_views: 6        # Number of neighboring views used for each depthmaps
depthmap_min_depth: 0                 # Minimum depth in meters.  Set to 0 to auto-infer from the reconstruction.
depthmap_max_depth: 0                 # Maximum depth in meters.  Set to 0 to auto-infer from the reconstruction.
depthmap_patchmatch_iterations: 3     # Number of PatchMatch iterations to run
depthmap_patch_size: 7                # Size of the correlation patch
depthmap_min_patch_sd: 1.0            # Patches with lower standard deviation are ignored
depthmap_min_correlation_score: 0.1   # Minimum correlation score to accept a depth value
depthmap_same_depth_threshold: 0.01   # Threshold to measure depth closeness
depthmap_min_consistent_views: 3      # Min number of views that should reconstruct a point for it to be valid
depthmap_save_debug_files: no         # Save debug files with partial reconstruction results

# Other params
processes: 1                  # Number of threads to use

# Params for submodel split and merge
submodel_size: 80                                                    # Average number of images per submodel
submodel_overlap: 30.0                                               # Radius of the overlapping region between submodels
submodels_relpath: "submodels"                                       # Relative path to the submodels directory
submodel_relpath_template: "submodels/submodel_%04d"                 # Template to generate the relative path to a submodel directory
submodel_images_relpath_template: "submodels/submodel_%04d/images"   # Template to generate the relative path to a submodel images directory
'''


def default_config():
    """Return default configuration"""
    return yaml.safe_load(default_config_yaml)


def load_config(filepath):
    """Load config from a config.yaml filepath"""
    config = default_config()

    if os.path.isfile(filepath):
        with open(filepath) as fin:
            new_config = yaml.safe_load(fin)
        if new_config:
            for k, v in new_config.items():
                config[k] = v

    return config
