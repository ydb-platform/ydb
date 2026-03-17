#include "inside_polygon_int.h"
#include <stdio.h>

bool inside_polygon_int(int x, int y, int nr_coords, int x_coords[],
                        int y_coords[]) {
  // naive implementation, vulnerable to overflow:
  //  bool inside;
  //  for (int i = 0, j = nr_coords - 1; i < nr_coords; j = i++) {
  //    if (((y_coords[i] > y) != (y_coords[j] > y)) &&
  //        (x < (x_coords[j] - x_coords[i]) * (y - y_coords[i]) /
  //                     (y_coords[j] - y_coords[i]) +
  //                 x_coords[i])) {
  //      inside = !inside;
  //    }
  //  }
  //  return inside;

  bool inside, y_gt_y1, y_gt_y2, x_le_x1, x_le_x2;
  long y1, y2, x1, x2, slope1, slope2; // int64 precision
  int i, j;

  inside = false;
  // the edge from the last to the first point is checked first
  j = nr_coords - 1;
  y_gt_y1 = y > y_coords[j];
  for (i = 0; i < nr_coords; j = i++) {
    y_gt_y2 = y > y_coords[i];
    if (y_gt_y1 ^ y_gt_y2) { // XOR
      // [p1-p2] crosses horizontal line in p
      // only count crossings "right" of the point ( >= x)
      x_le_x1 = x <= x_coords[j];
      x_le_x2 = x <= x_coords[i];
      if (x_le_x1 || x_le_x2) {
        if (x_le_x1 && x_le_x2) {
          // p1 and p2 are both to the right -> valid crossing
          inside = !inside;
        } else {
          // compare the slope of the line [p1-p2] and [p-p2]
          // depending on the position of p2 this determines whether
          // the polygon edge is right or left of the point
          // to avoid expensive division the divisors (of the slope dy/dx)
          // are brought to the other side ( dy/dx > a  ==  dy > a * dx )
          // only one of the points is to the right
          // NOTE: int64 precision required to prevent overflow
          y1 = y_coords[j];
          y2 = y_coords[i];
          x1 = x_coords[j];
          x2 = x_coords[i];
          slope1 = (y2 - y) * (x2 - x1);
          slope2 = (y2 - y1) * (x2 - x);
          // NOTE: accept slope equality to also detect if p lies directly
          // on an edge
          if (y_gt_y1) {
            if (slope1 <= slope2) {
              inside = !inside;
            }
          } else { // NOT y_gt_y1
            if (slope1 >= slope2) {
              inside = !inside;
            }
          }
        }
      }
    }
    // next point
    y_gt_y1 = y_gt_y2;
  }
  return inside;
}
