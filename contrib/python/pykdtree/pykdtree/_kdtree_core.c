/*
pykdtree, Fast kd-tree implementation with OpenMP-enabled queries

Copyright (C) 2013 - present  Esben S. Nielsen

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or
 (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU Lesser General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
This kd-tree implementation is based on the scipy.spatial.cKDTree by
Anne M. Archibald and libANN by David M. Mount and Sunil Arya.
*/


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>

#define PA(i,d)			(pa[no_dims * pidx[i] + d])
#define PASWAP_int32_t(a,b) { uint32_t tmp = pidx[a]; pidx[a] = pidx[b]; pidx[b] = tmp; }
#define PASWAP_int64_t(a,b) { uint64_t tmp = pidx[a]; pidx[a] = pidx[b]; pidx[b] = tmp; }

#define IDX_MAX_int32_t UINT32_MAX
#define IDX_MAX_int64_t UINT64_MAX
#define DIST_MAX_float FLT_MAX
#define DIST_MAX_double DBL_MAX

#ifdef _MSC_VER
#define restrict __restrict
#endif


typedef struct
{
    float cut_val;
    int8_t cut_dim;
    uint32_t start_idx;
    uint32_t n;
    float cut_bounds_lv;
    float cut_bounds_hv;
    struct Node_float_int32_t *left_child;
    struct Node_float_int32_t *right_child;
} Node_float_int32_t;

typedef struct
{
    float *bbox;
    int8_t no_dims;
    uint32_t *pidx;
    struct Node_float_int32_t *root;
} Tree_float_int32_t;


typedef struct
{
    float cut_val;
    int8_t cut_dim;
    uint64_t start_idx;
    uint64_t n;
    float cut_bounds_lv;
    float cut_bounds_hv;
    struct Node_float_int64_t *left_child;
    struct Node_float_int64_t *right_child;
} Node_float_int64_t;

typedef struct
{
    float *bbox;
    int8_t no_dims;
    uint64_t *pidx;
    struct Node_float_int64_t *root;
} Tree_float_int64_t;


typedef struct
{
    double cut_val;
    int8_t cut_dim;
    uint32_t start_idx;
    uint32_t n;
    double cut_bounds_lv;
    double cut_bounds_hv;
    struct Node_double_int32_t *left_child;
    struct Node_double_int32_t *right_child;
} Node_double_int32_t;

typedef struct
{
    double *bbox;
    int8_t no_dims;
    uint32_t *pidx;
    struct Node_double_int32_t *root;
} Tree_double_int32_t;


typedef struct
{
    double cut_val;
    int8_t cut_dim;
    uint64_t start_idx;
    uint64_t n;
    double cut_bounds_lv;
    double cut_bounds_hv;
    struct Node_double_int64_t *left_child;
    struct Node_double_int64_t *right_child;
} Node_double_int64_t;

typedef struct
{
    double *bbox;
    int8_t no_dims;
    uint64_t *pidx;
    struct Node_double_int64_t *root;
} Tree_double_int64_t;



float calc_dist_float(float *point1_coord, float *point2_coord, int8_t no_dims);
float get_cube_offset_float(int8_t dim, float *point_coord, float *bbox);
float get_min_dist_float(float *point_coord, int8_t no_dims, float *bbox);


void insert_point_float_int32_t(uint32_t *closest_idx, float *closest_dist, uint32_t pidx, float cur_dist, uint32_t k);
void get_bounding_box_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t n, float *bbox);
int partition_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *bbox, int8_t *cut_dim,
              float *cut_val, uint32_t *n_lo);
Tree_float_int32_t* construct_tree_float_int32_t(float *pa, int8_t no_dims, uint32_t n, uint32_t bsp);
Node_float_int32_t* construct_subtree_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, uint32_t bsp, float *bbox);
Node_float_int32_t * create_node_float_int32_t(uint32_t start_idx, uint32_t n, int is_leaf);
void delete_subtree_float_int32_t(Node_float_int32_t *root);
void delete_tree_float_int32_t(Tree_float_int32_t *tree);
void print_tree_float_int32_t(Node_float_int32_t *root, int level);
void search_leaf_float_int32_t(float *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *restrict point_coord,
                 uint32_t k, uint32_t *restrict closest_idx, float *restrict closest_dist);
void search_leaf_float_int32_t_mask(float *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *restrict point_coord,
                 uint32_t k, uint8_t *restrict mask, uint32_t *restrict closest_idx, float *restrict closest_dist);
void search_splitnode_float_int32_t(Node_float_int32_t *root, float *pa, uint32_t *pidx, int8_t no_dims, float *point_coord,
                      float min_dist, uint32_t k, float distance_upper_bound, float eps_fac, uint8_t *mask, uint32_t *  closest_idx, float *closest_dist);
void search_tree_float_int32_t(Tree_float_int32_t *tree, float *pa, float *point_coords,
                 uint32_t num_points, uint32_t k,  float distance_upper_bound,
                 float eps, uint8_t *mask, uint32_t *closest_idxs, float *closest_dists);


void insert_point_float_int64_t(uint64_t *closest_idx, float *closest_dist, uint64_t pidx, float cur_dist, uint64_t k);
void get_bounding_box_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t n, float *bbox);
int partition_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *bbox, int8_t *cut_dim,
              float *cut_val, uint64_t *n_lo);
Tree_float_int64_t* construct_tree_float_int64_t(float *pa, int8_t no_dims, uint64_t n, uint64_t bsp);
Node_float_int64_t* construct_subtree_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, uint64_t bsp, float *bbox);
Node_float_int64_t * create_node_float_int64_t(uint64_t start_idx, uint64_t n, int is_leaf);
void delete_subtree_float_int64_t(Node_float_int64_t *root);
void delete_tree_float_int64_t(Tree_float_int64_t *tree);
void print_tree_float_int64_t(Node_float_int64_t *root, int level);
void search_leaf_float_int64_t(float *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *restrict point_coord,
                 uint64_t k, uint64_t *restrict closest_idx, float *restrict closest_dist);
void search_leaf_float_int64_t_mask(float *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *restrict point_coord,
                 uint64_t k, uint8_t *restrict mask, uint64_t *restrict closest_idx, float *restrict closest_dist);
void search_splitnode_float_int64_t(Node_float_int64_t *root, float *pa, uint64_t *pidx, int8_t no_dims, float *point_coord,
                      float min_dist, uint64_t k, float distance_upper_bound, float eps_fac, uint8_t *mask, uint64_t *  closest_idx, float *closest_dist);
void search_tree_float_int64_t(Tree_float_int64_t *tree, float *pa, float *point_coords,
                 uint64_t num_points, uint64_t k,  float distance_upper_bound,
                 float eps, uint8_t *mask, uint64_t *closest_idxs, float *closest_dists);


double calc_dist_double(double *point1_coord, double *point2_coord, int8_t no_dims);
double get_cube_offset_double(int8_t dim, double *point_coord, double *bbox);
double get_min_dist_double(double *point_coord, int8_t no_dims, double *bbox);


void insert_point_double_int32_t(uint32_t *closest_idx, double *closest_dist, uint32_t pidx, double cur_dist, uint32_t k);
void get_bounding_box_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t n, double *bbox);
int partition_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *bbox, int8_t *cut_dim,
              double *cut_val, uint32_t *n_lo);
Tree_double_int32_t* construct_tree_double_int32_t(double *pa, int8_t no_dims, uint32_t n, uint32_t bsp);
Node_double_int32_t* construct_subtree_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, uint32_t bsp, double *bbox);
Node_double_int32_t * create_node_double_int32_t(uint32_t start_idx, uint32_t n, int is_leaf);
void delete_subtree_double_int32_t(Node_double_int32_t *root);
void delete_tree_double_int32_t(Tree_double_int32_t *tree);
void print_tree_double_int32_t(Node_double_int32_t *root, int level);
void search_leaf_double_int32_t(double *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *restrict point_coord,
                 uint32_t k, uint32_t *restrict closest_idx, double *restrict closest_dist);
void search_leaf_double_int32_t_mask(double *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *restrict point_coord,
                 uint32_t k, uint8_t *restrict mask, uint32_t *restrict closest_idx, double *restrict closest_dist);
void search_splitnode_double_int32_t(Node_double_int32_t *root, double *pa, uint32_t *pidx, int8_t no_dims, double *point_coord,
                      double min_dist, uint32_t k, double distance_upper_bound, double eps_fac, uint8_t *mask, uint32_t *  closest_idx, double *closest_dist);
void search_tree_double_int32_t(Tree_double_int32_t *tree, double *pa, double *point_coords,
                 uint32_t num_points, uint32_t k,  double distance_upper_bound,
                 double eps, uint8_t *mask, uint32_t *closest_idxs, double *closest_dists);


void insert_point_double_int64_t(uint64_t *closest_idx, double *closest_dist, uint64_t pidx, double cur_dist, uint64_t k);
void get_bounding_box_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t n, double *bbox);
int partition_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *bbox, int8_t *cut_dim,
              double *cut_val, uint64_t *n_lo);
Tree_double_int64_t* construct_tree_double_int64_t(double *pa, int8_t no_dims, uint64_t n, uint64_t bsp);
Node_double_int64_t* construct_subtree_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, uint64_t bsp, double *bbox);
Node_double_int64_t * create_node_double_int64_t(uint64_t start_idx, uint64_t n, int is_leaf);
void delete_subtree_double_int64_t(Node_double_int64_t *root);
void delete_tree_double_int64_t(Tree_double_int64_t *tree);
void print_tree_double_int64_t(Node_double_int64_t *root, int level);
void search_leaf_double_int64_t(double *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *restrict point_coord,
                 uint64_t k, uint64_t *restrict closest_idx, double *restrict closest_dist);
void search_leaf_double_int64_t_mask(double *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *restrict point_coord,
                 uint64_t k, uint8_t *restrict mask, uint64_t *restrict closest_idx, double *restrict closest_dist);
void search_splitnode_double_int64_t(Node_double_int64_t *root, double *pa, uint64_t *pidx, int8_t no_dims, double *point_coord,
                      double min_dist, uint64_t k, double distance_upper_bound, double eps_fac, uint8_t *mask, uint64_t *  closest_idx, double *closest_dist);
void search_tree_double_int64_t(Tree_double_int64_t *tree, double *pa, double *point_coords,
                 uint64_t num_points, uint64_t k,  double distance_upper_bound,
                 double eps, uint8_t *mask, uint64_t *closest_idxs, double *closest_dists);



/************************************************
Calculate squared cartesian distance between points
Params:
    point1_coord : point 1
    point2_coord : point 2
************************************************/
float calc_dist_float(float *point1_coord, float *point2_coord, int8_t no_dims)
{
    /* Calculate squared distance */
    float dist = 0, dim_dist;
    int8_t i;
    for (i = 0; i < no_dims; i++)
    {
        dim_dist = point2_coord[i] - point1_coord[i];
        dist += dim_dist * dim_dist;
    }
    return dist;
}

/************************************************
Get squared distance from point to cube in specified dimension
Params:
    dim : dimension
    point_coord : cartesian coordinates of point
    bbox : cube
************************************************/
float get_cube_offset_float(int8_t dim, float *point_coord, float *bbox)
{
    float dim_coord = point_coord[dim];

    if (dim_coord < bbox[2 * dim])
    {
        /* Left of cube in dimension */
        return dim_coord - bbox[2 * dim];
    }
    else if (dim_coord > bbox[2 * dim + 1])
    {
        /* Right of cube in dimension */
        return dim_coord - bbox[2 * dim + 1];
    }
    else
    {
        /* Inside cube in dimension */
        return 0.;
    }
}

/************************************************
Get minimum squared distance between point and cube.
Params:
    point_coord : cartesian coordinates of point
    no_dims : number of dimensions
    bbox : cube
************************************************/
float get_min_dist_float(float *point_coord, int8_t no_dims, float *bbox)
{
    float cube_offset = 0, cube_offset_dim;
    int8_t i;

    for (i = 0; i < no_dims; i++)
    {
        cube_offset_dim = get_cube_offset_float(i, point_coord, bbox);
        cube_offset += cube_offset_dim * cube_offset_dim;
    }

    return cube_offset;
}


/************************************************
Insert point into priority queue
Params:
    closest_idx : index queue
    closest_dist : distance queue
    pidx : permutation index of data points
    cur_dist : distance to point inserted
    k : number of neighbours
************************************************/
void insert_point_float_int32_t(uint32_t *closest_idx, float *closest_dist, uint32_t pidx, float cur_dist, uint32_t k)
{
    int i;
    for (i = k - 1; i > 0; i--)
    {
        if (closest_dist[i - 1] > cur_dist)
        {
            closest_dist[i] = closest_dist[i - 1];
            closest_idx[i] = closest_idx[i - 1];
        }
        else
        {
            break;
        }
    }
    closest_idx[i] = pidx;
    closest_dist[i] = cur_dist;
}

/************************************************
Get the bounding box of a set of points
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    n : number of points
    bbox : bounding box (return)
************************************************/
void get_bounding_box_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t n, float *bbox)
{
    float cur;
    int8_t i, j;
    uint32_t bbox_idx, i2;

    /* Use first data point to initialize */
    for (i = 0; i < no_dims; i++)
    {
        bbox[2 * i] = bbox[2 * i + 1] = PA(0, i);
    }

    /* Update using rest of data points */
    for (i2 = 1; i2 < n; i2++)
    {
        for (j = 0; j < no_dims; j++)
        {
            bbox_idx = 2 * j;
            cur = PA(i2, j);
            if (cur < bbox[bbox_idx])
            {
                bbox[bbox_idx] = cur;
            }
            else if (cur > bbox[bbox_idx + 1])
            {
                bbox[bbox_idx + 1] = cur;
            }
        }
    }
}

/************************************************
Partition a range of data points by manipulation the permutation index.
The sliding midpoint rule is used for the partitioning.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bbox : bounding box of data points
    cut_dim : dimension used for partition (return)
    cut_val : value of cutting point (return)
    n_lo : number of point below cutting plane (return)
************************************************/
int partition_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *bbox, int8_t *cut_dim, float *cut_val, uint32_t *n_lo)
{
    int8_t dim = 0, i;
    uint32_t p, q, i2;
    float size = 0, min_val, max_val, split, side_len, cur_val;
    uint32_t end_idx = start_idx + n - 1;

    /* Find largest bounding box side */
    for (i = 0; i < no_dims; i++)
    {
        side_len = bbox[2 * i + 1] - bbox[2 * i];
        if (side_len > size)
        {
            dim = i;
            size = side_len;
        }
    }

    min_val = bbox[2 * dim];
    max_val = bbox[2 * dim + 1];

    /* Check for zero length or inconsistent */
    if (min_val >= max_val)
        return 1;

    /* Use middle for splitting */
    split = (min_val + max_val) / 2;

    /* Partition all data points around middle */
    p = start_idx;
    q = end_idx;
    while (p <= q)
    {
        if (PA(p, dim) < split)
        {
            p++;
        }
        else if (PA(q, dim) >= split)
        {
            /* Guard for underflow */
            if (q > 0)
            {
                q--;
            }
            else
            {
                break;
            }
        }
        else
        {
            PASWAP_int32_t(p, q);
            p++;
            q--;
        }
    }

    /* Check for empty splits */
    if (p == start_idx)
    {
        /* No points less than split.
           Split at lowest point instead.
           Minimum 1 point will be in lower box.
        */

        uint32_t j = start_idx;
        split = PA(j, dim);
        for (i2 = start_idx + 1; i2 <= end_idx; i2++)
        {
            /* Find lowest point */
            cur_val = PA(i2, dim);
            if (cur_val < split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int32_t(j, start_idx);
        p = start_idx + 1;
    }
    else if (p == end_idx + 1)
    {
        /* No points greater than split.
           Split at highest point instead.
           Minimum 1 point will be in higher box.
        */

        uint32_t j = end_idx;
        split = PA(j, dim);
        for (i2 = start_idx; i2 < end_idx; i2++)
        {
            /* Find highest point */
            cur_val = PA(i2, dim);
            if (cur_val > split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int32_t(j, end_idx);
        p = end_idx;
    }

    /* Set return values */
    *cut_dim = dim;
    *cut_val = split;
    *n_lo = p - start_idx;
    return 0;
}

/************************************************
Construct a sub tree over a range of data points.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bsp : number of points per leaf
    bbox : bounding box of set of data points
************************************************/
Node_float_int32_t* construct_subtree_float_int32_t(float *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, uint32_t bsp, float *bbox)
{
    /* Create new node */
    int is_leaf = (n <= bsp);
    Node_float_int32_t *root = create_node_float_int32_t(start_idx, n, is_leaf);
    int rval;
    int8_t cut_dim;
    uint32_t n_lo;
    float cut_val, lv, hv;
    if (is_leaf)
    {
        /* Make leaf node */
        root->cut_dim = -1;
    }
    else
    {
        /* Make split node */
        /* Partition data set and set node info */
        rval = partition_float_int32_t(pa, pidx, no_dims, start_idx, n, bbox, &cut_dim, &cut_val, &n_lo);
        if (rval == 1)
        {
            root->cut_dim = -1;
            return root;
        }
        root->cut_val = cut_val;
        root->cut_dim = cut_dim;

        /* Recurse on both subsets */
        lv = bbox[2 * cut_dim];
        hv = bbox[2 * cut_dim + 1];

        /* Set bounds for cut dimension */
        root->cut_bounds_lv = lv;
        root->cut_bounds_hv = hv;

        /* Update bounding box before call to lower subset and restore after */
        bbox[2 * cut_dim + 1] = cut_val;
        root->left_child = (struct Node_float_int32_t *)construct_subtree_float_int32_t(pa, pidx, no_dims, start_idx, n_lo, bsp, bbox);
        bbox[2 * cut_dim + 1] = hv;

        /* Update bounding box before call to higher subset and restore after */
        bbox[2 * cut_dim] = cut_val;
        root->right_child = (struct Node_float_int32_t *)construct_subtree_float_int32_t(pa, pidx, no_dims, start_idx + n_lo, n - n_lo, bsp, bbox);
        bbox[2 * cut_dim] = lv;
    }
    return root;
}

/************************************************
Construct a tree over data points.
Params:
    pa : data points
    no_dims: number of dimensions
    n :  number of data points
    bsp : number of points per leaf
************************************************/
Tree_float_int32_t* construct_tree_float_int32_t(float *pa, int8_t no_dims, uint32_t n, uint32_t bsp)
{
    Tree_float_int32_t *tree = (Tree_float_int32_t *)malloc(sizeof(Tree_float_int32_t));
    uint32_t i;
    uint32_t *pidx;
    float *bbox;

    tree->no_dims = no_dims;

    /* Initialize permutation array */
    pidx = (uint32_t *)malloc(sizeof(uint32_t) * n);
    for (i = 0; i < n; i++)
    {
        pidx[i] = i;
    }

    bbox = (float *)malloc(2 * sizeof(float) * no_dims);
    get_bounding_box_float_int32_t(pa, pidx, no_dims, n, bbox);
    tree->bbox = bbox;

    /* Construct subtree on full dataset */
    tree->root = (struct Node_float_int32_t *)construct_subtree_float_int32_t(pa, pidx, no_dims, 0, n, bsp, bbox);

    tree->pidx = pidx;
    return tree;
}

/************************************************
Create a tree node.
Params:
    start_idx : index of first data point to use
    n :  number of data points
************************************************/
Node_float_int32_t* create_node_float_int32_t(uint32_t start_idx, uint32_t n, int is_leaf)
{
    Node_float_int32_t *new_node;
    if (is_leaf)
    {
        /*
            Allocate only the part of the struct that will be used in a leaf node.
            This relies on the C99 specification of struct layout conservation and padding and
            that dereferencing is never attempted for the node pointers in a leaf.
        */
        new_node = (Node_float_int32_t *)malloc(sizeof(Node_float_int32_t) - 2 * sizeof(Node_float_int32_t *));
    }
    else
    {
        new_node = (Node_float_int32_t *)malloc(sizeof(Node_float_int32_t));
    }
    new_node->n = n;
    new_node->start_idx = start_idx;
    return new_node;
}

/************************************************
Delete subtree
Params:
    root : root node of subtree to delete
************************************************/
void delete_subtree_float_int32_t(Node_float_int32_t *root)
{
    if (root->cut_dim != -1)
    {
        delete_subtree_float_int32_t((Node_float_int32_t *)root->left_child);
        delete_subtree_float_int32_t((Node_float_int32_t *)root->right_child);
    }
    free(root);
}

/************************************************
Delete tree
Params:
    tree : Tree struct of kd tree
************************************************/
void delete_tree_float_int32_t(Tree_float_int32_t *tree)
{
    delete_subtree_float_int32_t((Node_float_int32_t *)tree->root);
    free(tree->bbox);
    free(tree->pidx);
    free(tree);
}

/************************************************
Print
************************************************/
void print_tree_float_int32_t(Node_float_int32_t *root, int level)
{
    int i;
    for (i = 0; i < level; i++)
    {
        printf(" ");
    }
    printf("(cut_val: %f, cut_dim: %i)\n", root->cut_val, root->cut_dim);
    if (root->cut_dim != -1)
        print_tree_float_int32_t((Node_float_int32_t *)root->left_child, level + 1);
    if (root->cut_dim != -1)
        print_tree_float_int32_t((Node_float_int32_t *)root->right_child, level + 1);
}

/************************************************
Search a leaf node for closest point
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_float_int32_t(float *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *restrict point_coord,
                 uint32_t k, uint32_t *restrict closest_idx, float *restrict closest_dist)
{
    float cur_dist;
    uint32_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Get distance to query point */
        cur_dist = calc_dist_float(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_float_int32_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}


/************************************************
Search a leaf node for closest point with data point mask
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_float_int32_t_mask(float *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, float *restrict point_coord,
                               uint32_t k, uint8_t *mask, uint32_t *restrict closest_idx, float *restrict closest_dist)
{
    float cur_dist;
    uint32_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Is this point masked out? */
        if (mask[pidx[start_idx + i]])
        {
            continue;
        }
        /* Get distance to query point */
        cur_dist = calc_dist_float(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_float_int32_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}

/************************************************
Search subtree for nearest to query point
Params:
    root : root node of subtree
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    point_coord : query point
    min_dist : minumum distance to nearest neighbour
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_splitnode_float_int32_t(Node_float_int32_t *root, float *pa, uint32_t *pidx, int8_t no_dims, float *point_coord, 
                      float min_dist, uint32_t k, float distance_upper_bound, float eps_fac, uint8_t *mask,
                      uint32_t *closest_idx, float *closest_dist)
{
    int8_t dim;
    float dist_left, dist_right;
    float new_offset;
    float box_diff;

    /* Skip if distance bound exeeded */
    if (min_dist > distance_upper_bound)
    {
        return;
    }

    dim = root->cut_dim;

    /* Handle leaf node */
    if (dim == -1)
    {
        if (mask)
        {
            search_leaf_float_int32_t_mask(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, mask, closest_idx, closest_dist);
        }
        else
        {
            search_leaf_float_int32_t(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, closest_idx, closest_dist);
        }
        return;
    }

    /* Get distance to cutting plane */
    new_offset = point_coord[dim] - root->cut_val;

    if (new_offset < 0)
    {
        /* Left of cutting plane */
        dist_left = min_dist;
        if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit */
            search_splitnode_float_int32_t((Node_float_int32_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Right of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = root->cut_bounds_lv - point_coord[dim];
        if (box_diff < 0)
        {
		box_diff = 0;
        }
        dist_right = min_dist - box_diff * box_diff + new_offset * new_offset;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_float_int32_t((Node_float_int32_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
    else
    {
        /* Right of cutting plane */
        dist_right = min_dist;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_float_int32_t((Node_float_int32_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Left of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = point_coord[dim] - root->cut_bounds_hv;
        if (box_diff < 0)
        {
        	box_diff = 0;
        }
        dist_left = min_dist - box_diff * box_diff + new_offset * new_offset;
	  if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit*/
            search_splitnode_float_int32_t((Node_float_int32_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
}

/************************************************
Search for nearest neighbour for a set of query points
Params:
    tree : Tree struct of kd tree
    pa : data points
    pidx : permutation index of data points
    point_coords : query points
    num_points : number of query points
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_tree_float_int32_t(Tree_float_int32_t *tree, float *pa, float *point_coords,
                 uint32_t num_points, uint32_t k, float distance_upper_bound,
                 float eps, uint8_t *mask, uint32_t *closest_idxs, float *closest_dists)
{
    float min_dist;
    float eps_fac = 1 / ((1 + eps) * (1 + eps));
    int8_t no_dims = tree->no_dims;
    float *bbox = tree->bbox;
    uint32_t *pidx = tree->pidx;
    /* use 64-bit ints for indexing to avoid overflow, use signed ints to support all Openmp implementations */
    int64_t i = 0;
    int64_t j = 0;
    int64_t local_num_points = (int64_t) num_points;
    Node_float_int32_t *root = (Node_float_int32_t *)tree->root;

    /* Queries are OpenMP enabled */
    #pragma omp parallel
    {
        /* The low chunk size is important to avoid L2 cache trashing
           for spatial coherent query datasets
        */
        #pragma omp for private(i, j) schedule(static, 100) nowait
        for (i = 0; i < local_num_points; i++)
        {
            for (j = 0; j < k; j++)
            {
                closest_idxs[i * k + j] = IDX_MAX_int32_t;
                closest_dists[i * k + j] = DIST_MAX_float;
            }
            min_dist = get_min_dist_float(point_coords + no_dims * i, no_dims, bbox);
            search_splitnode_float_int32_t(root, pa, pidx, no_dims, point_coords + no_dims * i, min_dist,
                             k, distance_upper_bound, eps_fac, mask, &closest_idxs[i * k], &closest_dists[i * k]);
        }
    }
}

/************************************************
Insert point into priority queue
Params:
    closest_idx : index queue
    closest_dist : distance queue
    pidx : permutation index of data points
    cur_dist : distance to point inserted
    k : number of neighbours
************************************************/
void insert_point_float_int64_t(uint64_t *closest_idx, float *closest_dist, uint64_t pidx, float cur_dist, uint64_t k)
{
    int i;
    for (i = k - 1; i > 0; i--)
    {
        if (closest_dist[i - 1] > cur_dist)
        {
            closest_dist[i] = closest_dist[i - 1];
            closest_idx[i] = closest_idx[i - 1];
        }
        else
        {
            break;
        }
    }
    closest_idx[i] = pidx;
    closest_dist[i] = cur_dist;
}

/************************************************
Get the bounding box of a set of points
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    n : number of points
    bbox : bounding box (return)
************************************************/
void get_bounding_box_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t n, float *bbox)
{
    float cur;
    int8_t i, j;
    uint64_t bbox_idx, i2;

    /* Use first data point to initialize */
    for (i = 0; i < no_dims; i++)
    {
        bbox[2 * i] = bbox[2 * i + 1] = PA(0, i);
    }

    /* Update using rest of data points */
    for (i2 = 1; i2 < n; i2++)
    {
        for (j = 0; j < no_dims; j++)
        {
            bbox_idx = 2 * j;
            cur = PA(i2, j);
            if (cur < bbox[bbox_idx])
            {
                bbox[bbox_idx] = cur;
            }
            else if (cur > bbox[bbox_idx + 1])
            {
                bbox[bbox_idx + 1] = cur;
            }
        }
    }
}

/************************************************
Partition a range of data points by manipulation the permutation index.
The sliding midpoint rule is used for the partitioning.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bbox : bounding box of data points
    cut_dim : dimension used for partition (return)
    cut_val : value of cutting point (return)
    n_lo : number of point below cutting plane (return)
************************************************/
int partition_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *bbox, int8_t *cut_dim, float *cut_val, uint64_t *n_lo)
{
    int8_t dim = 0, i;
    uint64_t p, q, i2;
    float size = 0, min_val, max_val, split, side_len, cur_val;
    uint64_t end_idx = start_idx + n - 1;

    /* Find largest bounding box side */
    for (i = 0; i < no_dims; i++)
    {
        side_len = bbox[2 * i + 1] - bbox[2 * i];
        if (side_len > size)
        {
            dim = i;
            size = side_len;
        }
    }

    min_val = bbox[2 * dim];
    max_val = bbox[2 * dim + 1];

    /* Check for zero length or inconsistent */
    if (min_val >= max_val)
        return 1;

    /* Use middle for splitting */
    split = (min_val + max_val) / 2;

    /* Partition all data points around middle */
    p = start_idx;
    q = end_idx;
    while (p <= q)
    {
        if (PA(p, dim) < split)
        {
            p++;
        }
        else if (PA(q, dim) >= split)
        {
            /* Guard for underflow */
            if (q > 0)
            {
                q--;
            }
            else
            {
                break;
            }
        }
        else
        {
            PASWAP_int64_t(p, q);
            p++;
            q--;
        }
    }

    /* Check for empty splits */
    if (p == start_idx)
    {
        /* No points less than split.
           Split at lowest point instead.
           Minimum 1 point will be in lower box.
        */

        uint64_t j = start_idx;
        split = PA(j, dim);
        for (i2 = start_idx + 1; i2 <= end_idx; i2++)
        {
            /* Find lowest point */
            cur_val = PA(i2, dim);
            if (cur_val < split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int64_t(j, start_idx);
        p = start_idx + 1;
    }
    else if (p == end_idx + 1)
    {
        /* No points greater than split.
           Split at highest point instead.
           Minimum 1 point will be in higher box.
        */

        uint64_t j = end_idx;
        split = PA(j, dim);
        for (i2 = start_idx; i2 < end_idx; i2++)
        {
            /* Find highest point */
            cur_val = PA(i2, dim);
            if (cur_val > split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int64_t(j, end_idx);
        p = end_idx;
    }

    /* Set return values */
    *cut_dim = dim;
    *cut_val = split;
    *n_lo = p - start_idx;
    return 0;
}

/************************************************
Construct a sub tree over a range of data points.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bsp : number of points per leaf
    bbox : bounding box of set of data points
************************************************/
Node_float_int64_t* construct_subtree_float_int64_t(float *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, uint64_t bsp, float *bbox)
{
    /* Create new node */
    int is_leaf = (n <= bsp);
    Node_float_int64_t *root = create_node_float_int64_t(start_idx, n, is_leaf);
    int rval;
    int8_t cut_dim;
    uint64_t n_lo;
    float cut_val, lv, hv;
    if (is_leaf)
    {
        /* Make leaf node */
        root->cut_dim = -1;
    }
    else
    {
        /* Make split node */
        /* Partition data set and set node info */
        rval = partition_float_int64_t(pa, pidx, no_dims, start_idx, n, bbox, &cut_dim, &cut_val, &n_lo);
        if (rval == 1)
        {
            root->cut_dim = -1;
            return root;
        }
        root->cut_val = cut_val;
        root->cut_dim = cut_dim;

        /* Recurse on both subsets */
        lv = bbox[2 * cut_dim];
        hv = bbox[2 * cut_dim + 1];

        /* Set bounds for cut dimension */
        root->cut_bounds_lv = lv;
        root->cut_bounds_hv = hv;

        /* Update bounding box before call to lower subset and restore after */
        bbox[2 * cut_dim + 1] = cut_val;
        root->left_child = (struct Node_float_int64_t *)construct_subtree_float_int64_t(pa, pidx, no_dims, start_idx, n_lo, bsp, bbox);
        bbox[2 * cut_dim + 1] = hv;

        /* Update bounding box before call to higher subset and restore after */
        bbox[2 * cut_dim] = cut_val;
        root->right_child = (struct Node_float_int64_t *)construct_subtree_float_int64_t(pa, pidx, no_dims, start_idx + n_lo, n - n_lo, bsp, bbox);
        bbox[2 * cut_dim] = lv;
    }
    return root;
}

/************************************************
Construct a tree over data points.
Params:
    pa : data points
    no_dims: number of dimensions
    n :  number of data points
    bsp : number of points per leaf
************************************************/
Tree_float_int64_t* construct_tree_float_int64_t(float *pa, int8_t no_dims, uint64_t n, uint64_t bsp)
{
    Tree_float_int64_t *tree = (Tree_float_int64_t *)malloc(sizeof(Tree_float_int64_t));
    uint64_t i;
    uint64_t *pidx;
    float *bbox;

    tree->no_dims = no_dims;

    /* Initialize permutation array */
    pidx = (uint64_t *)malloc(sizeof(uint64_t) * n);
    for (i = 0; i < n; i++)
    {
        pidx[i] = i;
    }

    bbox = (float *)malloc(2 * sizeof(float) * no_dims);
    get_bounding_box_float_int64_t(pa, pidx, no_dims, n, bbox);
    tree->bbox = bbox;

    /* Construct subtree on full dataset */
    tree->root = (struct Node_float_int64_t *)construct_subtree_float_int64_t(pa, pidx, no_dims, 0, n, bsp, bbox);

    tree->pidx = pidx;
    return tree;
}

/************************************************
Create a tree node.
Params:
    start_idx : index of first data point to use
    n :  number of data points
************************************************/
Node_float_int64_t* create_node_float_int64_t(uint64_t start_idx, uint64_t n, int is_leaf)
{
    Node_float_int64_t *new_node;
    if (is_leaf)
    {
        /*
            Allocate only the part of the struct that will be used in a leaf node.
            This relies on the C99 specification of struct layout conservation and padding and
            that dereferencing is never attempted for the node pointers in a leaf.
        */
        new_node = (Node_float_int64_t *)malloc(sizeof(Node_float_int64_t) - 2 * sizeof(Node_float_int64_t *));
    }
    else
    {
        new_node = (Node_float_int64_t *)malloc(sizeof(Node_float_int64_t));
    }
    new_node->n = n;
    new_node->start_idx = start_idx;
    return new_node;
}

/************************************************
Delete subtree
Params:
    root : root node of subtree to delete
************************************************/
void delete_subtree_float_int64_t(Node_float_int64_t *root)
{
    if (root->cut_dim != -1)
    {
        delete_subtree_float_int64_t((Node_float_int64_t *)root->left_child);
        delete_subtree_float_int64_t((Node_float_int64_t *)root->right_child);
    }
    free(root);
}

/************************************************
Delete tree
Params:
    tree : Tree struct of kd tree
************************************************/
void delete_tree_float_int64_t(Tree_float_int64_t *tree)
{
    delete_subtree_float_int64_t((Node_float_int64_t *)tree->root);
    free(tree->bbox);
    free(tree->pidx);
    free(tree);
}

/************************************************
Print
************************************************/
void print_tree_float_int64_t(Node_float_int64_t *root, int level)
{
    int i;
    for (i = 0; i < level; i++)
    {
        printf(" ");
    }
    printf("(cut_val: %f, cut_dim: %i)\n", root->cut_val, root->cut_dim);
    if (root->cut_dim != -1)
        print_tree_float_int64_t((Node_float_int64_t *)root->left_child, level + 1);
    if (root->cut_dim != -1)
        print_tree_float_int64_t((Node_float_int64_t *)root->right_child, level + 1);
}

/************************************************
Search a leaf node for closest point
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_float_int64_t(float *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *restrict point_coord,
                 uint64_t k, uint64_t *restrict closest_idx, float *restrict closest_dist)
{
    float cur_dist;
    uint64_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Get distance to query point */
        cur_dist = calc_dist_float(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_float_int64_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}


/************************************************
Search a leaf node for closest point with data point mask
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_float_int64_t_mask(float *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, float *restrict point_coord,
                               uint64_t k, uint8_t *mask, uint64_t *restrict closest_idx, float *restrict closest_dist)
{
    float cur_dist;
    uint64_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Is this point masked out? */
        if (mask[pidx[start_idx + i]])
        {
            continue;
        }
        /* Get distance to query point */
        cur_dist = calc_dist_float(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_float_int64_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}

/************************************************
Search subtree for nearest to query point
Params:
    root : root node of subtree
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    point_coord : query point
    min_dist : minumum distance to nearest neighbour
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_splitnode_float_int64_t(Node_float_int64_t *root, float *pa, uint64_t *pidx, int8_t no_dims, float *point_coord, 
                      float min_dist, uint64_t k, float distance_upper_bound, float eps_fac, uint8_t *mask,
                      uint64_t *closest_idx, float *closest_dist)
{
    int8_t dim;
    float dist_left, dist_right;
    float new_offset;
    float box_diff;

    /* Skip if distance bound exeeded */
    if (min_dist > distance_upper_bound)
    {
        return;
    }

    dim = root->cut_dim;

    /* Handle leaf node */
    if (dim == -1)
    {
        if (mask)
        {
            search_leaf_float_int64_t_mask(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, mask, closest_idx, closest_dist);
        }
        else
        {
            search_leaf_float_int64_t(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, closest_idx, closest_dist);
        }
        return;
    }

    /* Get distance to cutting plane */
    new_offset = point_coord[dim] - root->cut_val;

    if (new_offset < 0)
    {
        /* Left of cutting plane */
        dist_left = min_dist;
        if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit */
            search_splitnode_float_int64_t((Node_float_int64_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Right of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = root->cut_bounds_lv - point_coord[dim];
        if (box_diff < 0)
        {
		box_diff = 0;
        }
        dist_right = min_dist - box_diff * box_diff + new_offset * new_offset;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_float_int64_t((Node_float_int64_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
    else
    {
        /* Right of cutting plane */
        dist_right = min_dist;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_float_int64_t((Node_float_int64_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Left of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = point_coord[dim] - root->cut_bounds_hv;
        if (box_diff < 0)
        {
        	box_diff = 0;
        }
        dist_left = min_dist - box_diff * box_diff + new_offset * new_offset;
	  if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit*/
            search_splitnode_float_int64_t((Node_float_int64_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
}

/************************************************
Search for nearest neighbour for a set of query points
Params:
    tree : Tree struct of kd tree
    pa : data points
    pidx : permutation index of data points
    point_coords : query points
    num_points : number of query points
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_tree_float_int64_t(Tree_float_int64_t *tree, float *pa, float *point_coords,
                 uint64_t num_points, uint64_t k, float distance_upper_bound,
                 float eps, uint8_t *mask, uint64_t *closest_idxs, float *closest_dists)
{
    float min_dist;
    float eps_fac = 1 / ((1 + eps) * (1 + eps));
    int8_t no_dims = tree->no_dims;
    float *bbox = tree->bbox;
    uint64_t *pidx = tree->pidx;
    /* use 64-bit ints for indexing to avoid overflow, use signed ints to support all Openmp implementations */
    int64_t i = 0;
    int64_t j = 0;
    int64_t local_num_points = (int64_t) num_points;
    Node_float_int64_t *root = (Node_float_int64_t *)tree->root;

    /* Queries are OpenMP enabled */
    #pragma omp parallel
    {
        /* The low chunk size is important to avoid L2 cache trashing
           for spatial coherent query datasets
        */
        #pragma omp for private(i, j) schedule(static, 100) nowait
        for (i = 0; i < local_num_points; i++)
        {
            for (j = 0; j < k; j++)
            {
                closest_idxs[i * k + j] = IDX_MAX_int64_t;
                closest_dists[i * k + j] = DIST_MAX_float;
            }
            min_dist = get_min_dist_float(point_coords + no_dims * i, no_dims, bbox);
            search_splitnode_float_int64_t(root, pa, pidx, no_dims, point_coords + no_dims * i, min_dist,
                             k, distance_upper_bound, eps_fac, mask, &closest_idxs[i * k], &closest_dists[i * k]);
        }
    }
}

/************************************************
Calculate squared cartesian distance between points
Params:
    point1_coord : point 1
    point2_coord : point 2
************************************************/
double calc_dist_double(double *point1_coord, double *point2_coord, int8_t no_dims)
{
    /* Calculate squared distance */
    double dist = 0, dim_dist;
    int8_t i;
    for (i = 0; i < no_dims; i++)
    {
        dim_dist = point2_coord[i] - point1_coord[i];
        dist += dim_dist * dim_dist;
    }
    return dist;
}

/************************************************
Get squared distance from point to cube in specified dimension
Params:
    dim : dimension
    point_coord : cartesian coordinates of point
    bbox : cube
************************************************/
double get_cube_offset_double(int8_t dim, double *point_coord, double *bbox)
{
    double dim_coord = point_coord[dim];

    if (dim_coord < bbox[2 * dim])
    {
        /* Left of cube in dimension */
        return dim_coord - bbox[2 * dim];
    }
    else if (dim_coord > bbox[2 * dim + 1])
    {
        /* Right of cube in dimension */
        return dim_coord - bbox[2 * dim + 1];
    }
    else
    {
        /* Inside cube in dimension */
        return 0.;
    }
}

/************************************************
Get minimum squared distance between point and cube.
Params:
    point_coord : cartesian coordinates of point
    no_dims : number of dimensions
    bbox : cube
************************************************/
double get_min_dist_double(double *point_coord, int8_t no_dims, double *bbox)
{
    double cube_offset = 0, cube_offset_dim;
    int8_t i;

    for (i = 0; i < no_dims; i++)
    {
        cube_offset_dim = get_cube_offset_double(i, point_coord, bbox);
        cube_offset += cube_offset_dim * cube_offset_dim;
    }

    return cube_offset;
}


/************************************************
Insert point into priority queue
Params:
    closest_idx : index queue
    closest_dist : distance queue
    pidx : permutation index of data points
    cur_dist : distance to point inserted
    k : number of neighbours
************************************************/
void insert_point_double_int32_t(uint32_t *closest_idx, double *closest_dist, uint32_t pidx, double cur_dist, uint32_t k)
{
    int i;
    for (i = k - 1; i > 0; i--)
    {
        if (closest_dist[i - 1] > cur_dist)
        {
            closest_dist[i] = closest_dist[i - 1];
            closest_idx[i] = closest_idx[i - 1];
        }
        else
        {
            break;
        }
    }
    closest_idx[i] = pidx;
    closest_dist[i] = cur_dist;
}

/************************************************
Get the bounding box of a set of points
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    n : number of points
    bbox : bounding box (return)
************************************************/
void get_bounding_box_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t n, double *bbox)
{
    double cur;
    int8_t i, j;
    uint32_t bbox_idx, i2;

    /* Use first data point to initialize */
    for (i = 0; i < no_dims; i++)
    {
        bbox[2 * i] = bbox[2 * i + 1] = PA(0, i);
    }

    /* Update using rest of data points */
    for (i2 = 1; i2 < n; i2++)
    {
        for (j = 0; j < no_dims; j++)
        {
            bbox_idx = 2 * j;
            cur = PA(i2, j);
            if (cur < bbox[bbox_idx])
            {
                bbox[bbox_idx] = cur;
            }
            else if (cur > bbox[bbox_idx + 1])
            {
                bbox[bbox_idx + 1] = cur;
            }
        }
    }
}

/************************************************
Partition a range of data points by manipulation the permutation index.
The sliding midpoint rule is used for the partitioning.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bbox : bounding box of data points
    cut_dim : dimension used for partition (return)
    cut_val : value of cutting point (return)
    n_lo : number of point below cutting plane (return)
************************************************/
int partition_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *bbox, int8_t *cut_dim, double *cut_val, uint32_t *n_lo)
{
    int8_t dim = 0, i;
    uint32_t p, q, i2;
    double size = 0, min_val, max_val, split, side_len, cur_val;
    uint32_t end_idx = start_idx + n - 1;

    /* Find largest bounding box side */
    for (i = 0; i < no_dims; i++)
    {
        side_len = bbox[2 * i + 1] - bbox[2 * i];
        if (side_len > size)
        {
            dim = i;
            size = side_len;
        }
    }

    min_val = bbox[2 * dim];
    max_val = bbox[2 * dim + 1];

    /* Check for zero length or inconsistent */
    if (min_val >= max_val)
        return 1;

    /* Use middle for splitting */
    split = (min_val + max_val) / 2;

    /* Partition all data points around middle */
    p = start_idx;
    q = end_idx;
    while (p <= q)
    {
        if (PA(p, dim) < split)
        {
            p++;
        }
        else if (PA(q, dim) >= split)
        {
            /* Guard for underflow */
            if (q > 0)
            {
                q--;
            }
            else
            {
                break;
            }
        }
        else
        {
            PASWAP_int32_t(p, q);
            p++;
            q--;
        }
    }

    /* Check for empty splits */
    if (p == start_idx)
    {
        /* No points less than split.
           Split at lowest point instead.
           Minimum 1 point will be in lower box.
        */

        uint32_t j = start_idx;
        split = PA(j, dim);
        for (i2 = start_idx + 1; i2 <= end_idx; i2++)
        {
            /* Find lowest point */
            cur_val = PA(i2, dim);
            if (cur_val < split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int32_t(j, start_idx);
        p = start_idx + 1;
    }
    else if (p == end_idx + 1)
    {
        /* No points greater than split.
           Split at highest point instead.
           Minimum 1 point will be in higher box.
        */

        uint32_t j = end_idx;
        split = PA(j, dim);
        for (i2 = start_idx; i2 < end_idx; i2++)
        {
            /* Find highest point */
            cur_val = PA(i2, dim);
            if (cur_val > split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int32_t(j, end_idx);
        p = end_idx;
    }

    /* Set return values */
    *cut_dim = dim;
    *cut_val = split;
    *n_lo = p - start_idx;
    return 0;
}

/************************************************
Construct a sub tree over a range of data points.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bsp : number of points per leaf
    bbox : bounding box of set of data points
************************************************/
Node_double_int32_t* construct_subtree_double_int32_t(double *pa, uint32_t *pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, uint32_t bsp, double *bbox)
{
    /* Create new node */
    int is_leaf = (n <= bsp);
    Node_double_int32_t *root = create_node_double_int32_t(start_idx, n, is_leaf);
    int rval;
    int8_t cut_dim;
    uint32_t n_lo;
    double cut_val, lv, hv;
    if (is_leaf)
    {
        /* Make leaf node */
        root->cut_dim = -1;
    }
    else
    {
        /* Make split node */
        /* Partition data set and set node info */
        rval = partition_double_int32_t(pa, pidx, no_dims, start_idx, n, bbox, &cut_dim, &cut_val, &n_lo);
        if (rval == 1)
        {
            root->cut_dim = -1;
            return root;
        }
        root->cut_val = cut_val;
        root->cut_dim = cut_dim;

        /* Recurse on both subsets */
        lv = bbox[2 * cut_dim];
        hv = bbox[2 * cut_dim + 1];

        /* Set bounds for cut dimension */
        root->cut_bounds_lv = lv;
        root->cut_bounds_hv = hv;

        /* Update bounding box before call to lower subset and restore after */
        bbox[2 * cut_dim + 1] = cut_val;
        root->left_child = (struct Node_double_int32_t *)construct_subtree_double_int32_t(pa, pidx, no_dims, start_idx, n_lo, bsp, bbox);
        bbox[2 * cut_dim + 1] = hv;

        /* Update bounding box before call to higher subset and restore after */
        bbox[2 * cut_dim] = cut_val;
        root->right_child = (struct Node_double_int32_t *)construct_subtree_double_int32_t(pa, pidx, no_dims, start_idx + n_lo, n - n_lo, bsp, bbox);
        bbox[2 * cut_dim] = lv;
    }
    return root;
}

/************************************************
Construct a tree over data points.
Params:
    pa : data points
    no_dims: number of dimensions
    n :  number of data points
    bsp : number of points per leaf
************************************************/
Tree_double_int32_t* construct_tree_double_int32_t(double *pa, int8_t no_dims, uint32_t n, uint32_t bsp)
{
    Tree_double_int32_t *tree = (Tree_double_int32_t *)malloc(sizeof(Tree_double_int32_t));
    uint32_t i;
    uint32_t *pidx;
    double *bbox;

    tree->no_dims = no_dims;

    /* Initialize permutation array */
    pidx = (uint32_t *)malloc(sizeof(uint32_t) * n);
    for (i = 0; i < n; i++)
    {
        pidx[i] = i;
    }

    bbox = (double *)malloc(2 * sizeof(double) * no_dims);
    get_bounding_box_double_int32_t(pa, pidx, no_dims, n, bbox);
    tree->bbox = bbox;

    /* Construct subtree on full dataset */
    tree->root = (struct Node_double_int32_t *)construct_subtree_double_int32_t(pa, pidx, no_dims, 0, n, bsp, bbox);

    tree->pidx = pidx;
    return tree;
}

/************************************************
Create a tree node.
Params:
    start_idx : index of first data point to use
    n :  number of data points
************************************************/
Node_double_int32_t* create_node_double_int32_t(uint32_t start_idx, uint32_t n, int is_leaf)
{
    Node_double_int32_t *new_node;
    if (is_leaf)
    {
        /*
            Allocate only the part of the struct that will be used in a leaf node.
            This relies on the C99 specification of struct layout conservation and padding and
            that dereferencing is never attempted for the node pointers in a leaf.
        */
        new_node = (Node_double_int32_t *)malloc(sizeof(Node_double_int32_t) - 2 * sizeof(Node_double_int32_t *));
    }
    else
    {
        new_node = (Node_double_int32_t *)malloc(sizeof(Node_double_int32_t));
    }
    new_node->n = n;
    new_node->start_idx = start_idx;
    return new_node;
}

/************************************************
Delete subtree
Params:
    root : root node of subtree to delete
************************************************/
void delete_subtree_double_int32_t(Node_double_int32_t *root)
{
    if (root->cut_dim != -1)
    {
        delete_subtree_double_int32_t((Node_double_int32_t *)root->left_child);
        delete_subtree_double_int32_t((Node_double_int32_t *)root->right_child);
    }
    free(root);
}

/************************************************
Delete tree
Params:
    tree : Tree struct of kd tree
************************************************/
void delete_tree_double_int32_t(Tree_double_int32_t *tree)
{
    delete_subtree_double_int32_t((Node_double_int32_t *)tree->root);
    free(tree->bbox);
    free(tree->pidx);
    free(tree);
}

/************************************************
Print
************************************************/
void print_tree_double_int32_t(Node_double_int32_t *root, int level)
{
    int i;
    for (i = 0; i < level; i++)
    {
        printf(" ");
    }
    printf("(cut_val: %f, cut_dim: %i)\n", root->cut_val, root->cut_dim);
    if (root->cut_dim != -1)
        print_tree_double_int32_t((Node_double_int32_t *)root->left_child, level + 1);
    if (root->cut_dim != -1)
        print_tree_double_int32_t((Node_double_int32_t *)root->right_child, level + 1);
}

/************************************************
Search a leaf node for closest point
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_double_int32_t(double *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *restrict point_coord,
                 uint32_t k, uint32_t *restrict closest_idx, double *restrict closest_dist)
{
    double cur_dist;
    uint32_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Get distance to query point */
        cur_dist = calc_dist_double(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_double_int32_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}


/************************************************
Search a leaf node for closest point with data point mask
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_double_int32_t_mask(double *restrict pa, uint32_t *restrict pidx, int8_t no_dims, uint32_t start_idx, uint32_t n, double *restrict point_coord,
                               uint32_t k, uint8_t *mask, uint32_t *restrict closest_idx, double *restrict closest_dist)
{
    double cur_dist;
    uint32_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Is this point masked out? */
        if (mask[pidx[start_idx + i]])
        {
            continue;
        }
        /* Get distance to query point */
        cur_dist = calc_dist_double(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_double_int32_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}

/************************************************
Search subtree for nearest to query point
Params:
    root : root node of subtree
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    point_coord : query point
    min_dist : minumum distance to nearest neighbour
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_splitnode_double_int32_t(Node_double_int32_t *root, double *pa, uint32_t *pidx, int8_t no_dims, double *point_coord, 
                      double min_dist, uint32_t k, double distance_upper_bound, double eps_fac, uint8_t *mask,
                      uint32_t *closest_idx, double *closest_dist)
{
    int8_t dim;
    double dist_left, dist_right;
    double new_offset;
    double box_diff;

    /* Skip if distance bound exeeded */
    if (min_dist > distance_upper_bound)
    {
        return;
    }

    dim = root->cut_dim;

    /* Handle leaf node */
    if (dim == -1)
    {
        if (mask)
        {
            search_leaf_double_int32_t_mask(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, mask, closest_idx, closest_dist);
        }
        else
        {
            search_leaf_double_int32_t(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, closest_idx, closest_dist);
        }
        return;
    }

    /* Get distance to cutting plane */
    new_offset = point_coord[dim] - root->cut_val;

    if (new_offset < 0)
    {
        /* Left of cutting plane */
        dist_left = min_dist;
        if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit */
            search_splitnode_double_int32_t((Node_double_int32_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Right of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = root->cut_bounds_lv - point_coord[dim];
        if (box_diff < 0)
        {
		box_diff = 0;
        }
        dist_right = min_dist - box_diff * box_diff + new_offset * new_offset;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_double_int32_t((Node_double_int32_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
    else
    {
        /* Right of cutting plane */
        dist_right = min_dist;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_double_int32_t((Node_double_int32_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Left of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = point_coord[dim] - root->cut_bounds_hv;
        if (box_diff < 0)
        {
        	box_diff = 0;
        }
        dist_left = min_dist - box_diff * box_diff + new_offset * new_offset;
	  if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit*/
            search_splitnode_double_int32_t((Node_double_int32_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
}

/************************************************
Search for nearest neighbour for a set of query points
Params:
    tree : Tree struct of kd tree
    pa : data points
    pidx : permutation index of data points
    point_coords : query points
    num_points : number of query points
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_tree_double_int32_t(Tree_double_int32_t *tree, double *pa, double *point_coords,
                 uint32_t num_points, uint32_t k, double distance_upper_bound,
                 double eps, uint8_t *mask, uint32_t *closest_idxs, double *closest_dists)
{
    double min_dist;
    double eps_fac = 1 / ((1 + eps) * (1 + eps));
    int8_t no_dims = tree->no_dims;
    double *bbox = tree->bbox;
    uint32_t *pidx = tree->pidx;
    /* use 64-bit ints for indexing to avoid overflow, use signed ints to support all Openmp implementations */
    int64_t i = 0;
    int64_t j = 0;
    int64_t local_num_points = (int64_t) num_points;
    Node_double_int32_t *root = (Node_double_int32_t *)tree->root;

    /* Queries are OpenMP enabled */
    #pragma omp parallel
    {
        /* The low chunk size is important to avoid L2 cache trashing
           for spatial coherent query datasets
        */
        #pragma omp for private(i, j) schedule(static, 100) nowait
        for (i = 0; i < local_num_points; i++)
        {
            for (j = 0; j < k; j++)
            {
                closest_idxs[i * k + j] = IDX_MAX_int32_t;
                closest_dists[i * k + j] = DIST_MAX_double;
            }
            min_dist = get_min_dist_double(point_coords + no_dims * i, no_dims, bbox);
            search_splitnode_double_int32_t(root, pa, pidx, no_dims, point_coords + no_dims * i, min_dist,
                             k, distance_upper_bound, eps_fac, mask, &closest_idxs[i * k], &closest_dists[i * k]);
        }
    }
}

/************************************************
Insert point into priority queue
Params:
    closest_idx : index queue
    closest_dist : distance queue
    pidx : permutation index of data points
    cur_dist : distance to point inserted
    k : number of neighbours
************************************************/
void insert_point_double_int64_t(uint64_t *closest_idx, double *closest_dist, uint64_t pidx, double cur_dist, uint64_t k)
{
    int i;
    for (i = k - 1; i > 0; i--)
    {
        if (closest_dist[i - 1] > cur_dist)
        {
            closest_dist[i] = closest_dist[i - 1];
            closest_idx[i] = closest_idx[i - 1];
        }
        else
        {
            break;
        }
    }
    closest_idx[i] = pidx;
    closest_dist[i] = cur_dist;
}

/************************************************
Get the bounding box of a set of points
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    n : number of points
    bbox : bounding box (return)
************************************************/
void get_bounding_box_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t n, double *bbox)
{
    double cur;
    int8_t i, j;
    uint64_t bbox_idx, i2;

    /* Use first data point to initialize */
    for (i = 0; i < no_dims; i++)
    {
        bbox[2 * i] = bbox[2 * i + 1] = PA(0, i);
    }

    /* Update using rest of data points */
    for (i2 = 1; i2 < n; i2++)
    {
        for (j = 0; j < no_dims; j++)
        {
            bbox_idx = 2 * j;
            cur = PA(i2, j);
            if (cur < bbox[bbox_idx])
            {
                bbox[bbox_idx] = cur;
            }
            else if (cur > bbox[bbox_idx + 1])
            {
                bbox[bbox_idx + 1] = cur;
            }
        }
    }
}

/************************************************
Partition a range of data points by manipulation the permutation index.
The sliding midpoint rule is used for the partitioning.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bbox : bounding box of data points
    cut_dim : dimension used for partition (return)
    cut_val : value of cutting point (return)
    n_lo : number of point below cutting plane (return)
************************************************/
int partition_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *bbox, int8_t *cut_dim, double *cut_val, uint64_t *n_lo)
{
    int8_t dim = 0, i;
    uint64_t p, q, i2;
    double size = 0, min_val, max_val, split, side_len, cur_val;
    uint64_t end_idx = start_idx + n - 1;

    /* Find largest bounding box side */
    for (i = 0; i < no_dims; i++)
    {
        side_len = bbox[2 * i + 1] - bbox[2 * i];
        if (side_len > size)
        {
            dim = i;
            size = side_len;
        }
    }

    min_val = bbox[2 * dim];
    max_val = bbox[2 * dim + 1];

    /* Check for zero length or inconsistent */
    if (min_val >= max_val)
        return 1;

    /* Use middle for splitting */
    split = (min_val + max_val) / 2;

    /* Partition all data points around middle */
    p = start_idx;
    q = end_idx;
    while (p <= q)
    {
        if (PA(p, dim) < split)
        {
            p++;
        }
        else if (PA(q, dim) >= split)
        {
            /* Guard for underflow */
            if (q > 0)
            {
                q--;
            }
            else
            {
                break;
            }
        }
        else
        {
            PASWAP_int64_t(p, q);
            p++;
            q--;
        }
    }

    /* Check for empty splits */
    if (p == start_idx)
    {
        /* No points less than split.
           Split at lowest point instead.
           Minimum 1 point will be in lower box.
        */

        uint64_t j = start_idx;
        split = PA(j, dim);
        for (i2 = start_idx + 1; i2 <= end_idx; i2++)
        {
            /* Find lowest point */
            cur_val = PA(i2, dim);
            if (cur_val < split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int64_t(j, start_idx);
        p = start_idx + 1;
    }
    else if (p == end_idx + 1)
    {
        /* No points greater than split.
           Split at highest point instead.
           Minimum 1 point will be in higher box.
        */

        uint64_t j = end_idx;
        split = PA(j, dim);
        for (i2 = start_idx; i2 < end_idx; i2++)
        {
            /* Find highest point */
            cur_val = PA(i2, dim);
            if (cur_val > split)
            {
                j = i2;
                split = cur_val;
            }
        }
        PASWAP_int64_t(j, end_idx);
        p = end_idx;
    }

    /* Set return values */
    *cut_dim = dim;
    *cut_val = split;
    *n_lo = p - start_idx;
    return 0;
}

/************************************************
Construct a sub tree over a range of data points.
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims: number of dimensions
    start_idx : index of first data point to use
    n :  number of data points
    bsp : number of points per leaf
    bbox : bounding box of set of data points
************************************************/
Node_double_int64_t* construct_subtree_double_int64_t(double *pa, uint64_t *pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, uint64_t bsp, double *bbox)
{
    /* Create new node */
    int is_leaf = (n <= bsp);
    Node_double_int64_t *root = create_node_double_int64_t(start_idx, n, is_leaf);
    int rval;
    int8_t cut_dim;
    uint64_t n_lo;
    double cut_val, lv, hv;
    if (is_leaf)
    {
        /* Make leaf node */
        root->cut_dim = -1;
    }
    else
    {
        /* Make split node */
        /* Partition data set and set node info */
        rval = partition_double_int64_t(pa, pidx, no_dims, start_idx, n, bbox, &cut_dim, &cut_val, &n_lo);
        if (rval == 1)
        {
            root->cut_dim = -1;
            return root;
        }
        root->cut_val = cut_val;
        root->cut_dim = cut_dim;

        /* Recurse on both subsets */
        lv = bbox[2 * cut_dim];
        hv = bbox[2 * cut_dim + 1];

        /* Set bounds for cut dimension */
        root->cut_bounds_lv = lv;
        root->cut_bounds_hv = hv;

        /* Update bounding box before call to lower subset and restore after */
        bbox[2 * cut_dim + 1] = cut_val;
        root->left_child = (struct Node_double_int64_t *)construct_subtree_double_int64_t(pa, pidx, no_dims, start_idx, n_lo, bsp, bbox);
        bbox[2 * cut_dim + 1] = hv;

        /* Update bounding box before call to higher subset and restore after */
        bbox[2 * cut_dim] = cut_val;
        root->right_child = (struct Node_double_int64_t *)construct_subtree_double_int64_t(pa, pidx, no_dims, start_idx + n_lo, n - n_lo, bsp, bbox);
        bbox[2 * cut_dim] = lv;
    }
    return root;
}

/************************************************
Construct a tree over data points.
Params:
    pa : data points
    no_dims: number of dimensions
    n :  number of data points
    bsp : number of points per leaf
************************************************/
Tree_double_int64_t* construct_tree_double_int64_t(double *pa, int8_t no_dims, uint64_t n, uint64_t bsp)
{
    Tree_double_int64_t *tree = (Tree_double_int64_t *)malloc(sizeof(Tree_double_int64_t));
    uint64_t i;
    uint64_t *pidx;
    double *bbox;

    tree->no_dims = no_dims;

    /* Initialize permutation array */
    pidx = (uint64_t *)malloc(sizeof(uint64_t) * n);
    for (i = 0; i < n; i++)
    {
        pidx[i] = i;
    }

    bbox = (double *)malloc(2 * sizeof(double) * no_dims);
    get_bounding_box_double_int64_t(pa, pidx, no_dims, n, bbox);
    tree->bbox = bbox;

    /* Construct subtree on full dataset */
    tree->root = (struct Node_double_int64_t *)construct_subtree_double_int64_t(pa, pidx, no_dims, 0, n, bsp, bbox);

    tree->pidx = pidx;
    return tree;
}

/************************************************
Create a tree node.
Params:
    start_idx : index of first data point to use
    n :  number of data points
************************************************/
Node_double_int64_t* create_node_double_int64_t(uint64_t start_idx, uint64_t n, int is_leaf)
{
    Node_double_int64_t *new_node;
    if (is_leaf)
    {
        /*
            Allocate only the part of the struct that will be used in a leaf node.
            This relies on the C99 specification of struct layout conservation and padding and
            that dereferencing is never attempted for the node pointers in a leaf.
        */
        new_node = (Node_double_int64_t *)malloc(sizeof(Node_double_int64_t) - 2 * sizeof(Node_double_int64_t *));
    }
    else
    {
        new_node = (Node_double_int64_t *)malloc(sizeof(Node_double_int64_t));
    }
    new_node->n = n;
    new_node->start_idx = start_idx;
    return new_node;
}

/************************************************
Delete subtree
Params:
    root : root node of subtree to delete
************************************************/
void delete_subtree_double_int64_t(Node_double_int64_t *root)
{
    if (root->cut_dim != -1)
    {
        delete_subtree_double_int64_t((Node_double_int64_t *)root->left_child);
        delete_subtree_double_int64_t((Node_double_int64_t *)root->right_child);
    }
    free(root);
}

/************************************************
Delete tree
Params:
    tree : Tree struct of kd tree
************************************************/
void delete_tree_double_int64_t(Tree_double_int64_t *tree)
{
    delete_subtree_double_int64_t((Node_double_int64_t *)tree->root);
    free(tree->bbox);
    free(tree->pidx);
    free(tree);
}

/************************************************
Print
************************************************/
void print_tree_double_int64_t(Node_double_int64_t *root, int level)
{
    int i;
    for (i = 0; i < level; i++)
    {
        printf(" ");
    }
    printf("(cut_val: %f, cut_dim: %i)\n", root->cut_val, root->cut_dim);
    if (root->cut_dim != -1)
        print_tree_double_int64_t((Node_double_int64_t *)root->left_child, level + 1);
    if (root->cut_dim != -1)
        print_tree_double_int64_t((Node_double_int64_t *)root->right_child, level + 1);
}

/************************************************
Search a leaf node for closest point
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_double_int64_t(double *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *restrict point_coord,
                 uint64_t k, uint64_t *restrict closest_idx, double *restrict closest_dist)
{
    double cur_dist;
    uint64_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Get distance to query point */
        cur_dist = calc_dist_double(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_double_int64_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}


/************************************************
Search a leaf node for closest point with data point mask
Params:
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    start_idx : index of first data point to use
    size :  number of data points
    point_coord : query point
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_leaf_double_int64_t_mask(double *restrict pa, uint64_t *restrict pidx, int8_t no_dims, uint64_t start_idx, uint64_t n, double *restrict point_coord,
                               uint64_t k, uint8_t *mask, uint64_t *restrict closest_idx, double *restrict closest_dist)
{
    double cur_dist;
    uint64_t i;
    /* Loop through all points in leaf */
    for (i = 0; i < n; i++)
    {
        /* Is this point masked out? */
        if (mask[pidx[start_idx + i]])
        {
            continue;
        }
        /* Get distance to query point */
        cur_dist = calc_dist_double(&PA(start_idx + i, 0), point_coord, no_dims);
        /* Update closest info if new point is closest so far*/
        if (cur_dist < closest_dist[k - 1])
        {
            insert_point_double_int64_t(closest_idx, closest_dist, pidx[start_idx + i], cur_dist, k);
        }
    }
}

/************************************************
Search subtree for nearest to query point
Params:
    root : root node of subtree
    pa : data points
    pidx : permutation index of data points
    no_dims : number of dimensions
    point_coord : query point
    min_dist : minumum distance to nearest neighbour
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_splitnode_double_int64_t(Node_double_int64_t *root, double *pa, uint64_t *pidx, int8_t no_dims, double *point_coord, 
                      double min_dist, uint64_t k, double distance_upper_bound, double eps_fac, uint8_t *mask,
                      uint64_t *closest_idx, double *closest_dist)
{
    int8_t dim;
    double dist_left, dist_right;
    double new_offset;
    double box_diff;

    /* Skip if distance bound exeeded */
    if (min_dist > distance_upper_bound)
    {
        return;
    }

    dim = root->cut_dim;

    /* Handle leaf node */
    if (dim == -1)
    {
        if (mask)
        {
            search_leaf_double_int64_t_mask(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, mask, closest_idx, closest_dist);
        }
        else
        {
            search_leaf_double_int64_t(pa, pidx, no_dims, root->start_idx, root->n, point_coord, k, closest_idx, closest_dist);
        }
        return;
    }

    /* Get distance to cutting plane */
    new_offset = point_coord[dim] - root->cut_val;

    if (new_offset < 0)
    {
        /* Left of cutting plane */
        dist_left = min_dist;
        if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit */
            search_splitnode_double_int64_t((Node_double_int64_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Right of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = root->cut_bounds_lv - point_coord[dim];
        if (box_diff < 0)
        {
		box_diff = 0;
        }
        dist_right = min_dist - box_diff * box_diff + new_offset * new_offset;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_double_int64_t((Node_double_int64_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
    else
    {
        /* Right of cutting plane */
        dist_right = min_dist;
        if (dist_right < closest_dist[k - 1] * eps_fac)
        {
            /* Search right subtree if minimum distance is below limit*/
            search_splitnode_double_int64_t((Node_double_int64_t *)root->right_child, pa, pidx, no_dims, point_coord, dist_right, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }

        /* Left of cutting plane. Update minimum distance.
           See Algorithms for Fast Vector Quantization
           Sunil Arya and David M. Mount. */
        box_diff = point_coord[dim] - root->cut_bounds_hv;
        if (box_diff < 0)
        {
        	box_diff = 0;
        }
        dist_left = min_dist - box_diff * box_diff + new_offset * new_offset;
	  if (dist_left < closest_dist[k - 1] * eps_fac)
        {
            /* Search left subtree if minimum distance is below limit*/
            search_splitnode_double_int64_t((Node_double_int64_t *)root->left_child, pa, pidx, no_dims, point_coord, dist_left, k, distance_upper_bound, eps_fac, mask, closest_idx, closest_dist);
        }
    }
}

/************************************************
Search for nearest neighbour for a set of query points
Params:
    tree : Tree struct of kd tree
    pa : data points
    pidx : permutation index of data points
    point_coords : query points
    num_points : number of query points
    mask : boolean array of invalid (True) and valid (False) data points
    closest_idx : index of closest data point found (return)
    closest_dist : distance to closest point (return)
************************************************/
void search_tree_double_int64_t(Tree_double_int64_t *tree, double *pa, double *point_coords,
                 uint64_t num_points, uint64_t k, double distance_upper_bound,
                 double eps, uint8_t *mask, uint64_t *closest_idxs, double *closest_dists)
{
    double min_dist;
    double eps_fac = 1 / ((1 + eps) * (1 + eps));
    int8_t no_dims = tree->no_dims;
    double *bbox = tree->bbox;
    uint64_t *pidx = tree->pidx;
    /* use 64-bit ints for indexing to avoid overflow, use signed ints to support all Openmp implementations */
    int64_t i = 0;
    int64_t j = 0;
    int64_t local_num_points = (int64_t) num_points;
    Node_double_int64_t *root = (Node_double_int64_t *)tree->root;

    /* Queries are OpenMP enabled */
    #pragma omp parallel
    {
        /* The low chunk size is important to avoid L2 cache trashing
           for spatial coherent query datasets
        */
        #pragma omp for private(i, j) schedule(static, 100) nowait
        for (i = 0; i < local_num_points; i++)
        {
            for (j = 0; j < k; j++)
            {
                closest_idxs[i * k + j] = IDX_MAX_int64_t;
                closest_dists[i * k + j] = DIST_MAX_double;
            }
            min_dist = get_min_dist_double(point_coords + no_dims * i, no_dims, bbox);
            search_splitnode_double_int64_t(root, pa, pidx, no_dims, point_coords + no_dims * i, min_dist,
                             k, distance_upper_bound, eps_fac, mask, &closest_idxs[i * k], &closest_dists[i * k]);
        }
    }
}
