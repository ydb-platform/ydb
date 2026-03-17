/******************************************************************************
 * Project:  PROJ
 * Purpose:  Grid management
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2019, Even Rouault, <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#ifndef GRIDS_HPP_INCLUDED
#define GRIDS_HPP_INCLUDED

#include <memory>
#include <vector>

#include "proj.h"
#include "proj/util.hpp"

NS_PROJ_START

struct ExtentAndRes {
    bool isGeographic; // whether extent and resolutions are in a geographic or
                       // projected CRS
    double west;       // in radian for geographic, in CRS units otherwise
    double south;      // in radian for geographic, in CRS units otherwise
    double east;       // in radian for geographic, in CRS units otherwise
    double north;      // in radian for geographic, in CRS units otherwise
    double resX;       // in radian for geographic, in CRS units otherwise
    double resY;       // in radian for geographic, in CRS units otherwise
    double invResX;    // = 1 / resX;
    double invResY;    // = 1 / resY;

    void computeInvRes();

    bool fullWorldLongitude() const;
    bool contains(const ExtentAndRes &other) const;
    bool intersects(const ExtentAndRes &other) const;
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL Grid {
  protected:
    friend class GTiffDataset;
    std::string m_name;
    int m_width;
    int m_height;
    ExtentAndRes m_extent;

    Grid(const std::string &nameIn, int widthIn, int heightIn,
         const ExtentAndRes &extentIn);

  public:
    PROJ_FOR_TEST virtual ~Grid();

    PROJ_FOR_TEST int width() const { return m_width; }
    PROJ_FOR_TEST int height() const { return m_height; }
    PROJ_FOR_TEST const ExtentAndRes &extentAndRes() const { return m_extent; }
    PROJ_FOR_TEST const std::string &name() const { return m_name; }

    virtual const std::string &metadataItem(const std::string &key,
                                            int sample = -1) const = 0;

    PROJ_FOR_TEST virtual bool isNullGrid() const { return false; }
    PROJ_FOR_TEST virtual bool hasChanged() const = 0;
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL VerticalShiftGrid : public Grid {
  protected:
    std::vector<std::unique_ptr<VerticalShiftGrid>> m_children{};

  public:
    PROJ_FOR_TEST VerticalShiftGrid(const std::string &nameIn, int widthIn,
                                    int heightIn, const ExtentAndRes &extentIn);
    PROJ_FOR_TEST ~VerticalShiftGrid() override;

    PROJ_FOR_TEST const VerticalShiftGrid *gridAt(double longitude,
                                                  double lat) const;

    PROJ_FOR_TEST virtual bool isNodata(float /*val*/,
                                        double /* multiplier */) const = 0;

    // x = 0 is western-most column, y = 0 is southern-most line
    PROJ_FOR_TEST virtual bool valueAt(int x, int y, float &out) const = 0;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx) = 0;
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL VerticalShiftGridSet {
  protected:
    std::string m_name{};
    std::string m_format{};
    std::vector<std::unique_ptr<VerticalShiftGrid>> m_grids{};

    VerticalShiftGridSet();

  public:
    PROJ_FOR_TEST virtual ~VerticalShiftGridSet();

    PROJ_FOR_TEST static std::unique_ptr<VerticalShiftGridSet>
    open(PJ_CONTEXT *ctx, const std::string &filename);

    PROJ_FOR_TEST const std::string &name() const { return m_name; }
    PROJ_FOR_TEST const std::string &format() const { return m_format; }
    PROJ_FOR_TEST const std::vector<std::unique_ptr<VerticalShiftGrid>> &
    grids() const {
        return m_grids;
    }
    PROJ_FOR_TEST const VerticalShiftGrid *gridAt(double longitude,
                                                  double lat) const;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx);
    PROJ_FOR_TEST virtual bool reopen(PJ_CONTEXT *ctx);
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL HorizontalShiftGrid : public Grid {
  protected:
    std::vector<std::unique_ptr<HorizontalShiftGrid>> m_children{};

  public:
    PROJ_FOR_TEST HorizontalShiftGrid(const std::string &nameIn, int widthIn,
                                      int heightIn,
                                      const ExtentAndRes &extentIn);
    PROJ_FOR_TEST ~HorizontalShiftGrid() override;

    PROJ_FOR_TEST const HorizontalShiftGrid *gridAt(double longitude,
                                                    double lat) const;

    // x = 0 is western-most column, y = 0 is southern-most line
    PROJ_FOR_TEST virtual bool valueAt(int x, int y,
                                       bool compensateNTConvention,
                                       float &longShift,
                                       float &latShift) const = 0;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx) = 0;
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL HorizontalShiftGridSet {
  protected:
    std::string m_name{};
    std::string m_format{};
    std::vector<std::unique_ptr<HorizontalShiftGrid>> m_grids{};

    HorizontalShiftGridSet();

  public:
    PROJ_FOR_TEST virtual ~HorizontalShiftGridSet();

    PROJ_FOR_TEST static std::unique_ptr<HorizontalShiftGridSet>
    open(PJ_CONTEXT *ctx, const std::string &filename);

    PROJ_FOR_TEST const std::string &name() const { return m_name; }
    PROJ_FOR_TEST const std::string &format() const { return m_format; }
    PROJ_FOR_TEST const std::vector<std::unique_ptr<HorizontalShiftGrid>> &
    grids() const {
        return m_grids;
    }
    PROJ_FOR_TEST const HorizontalShiftGrid *gridAt(double longitude,
                                                    double lat) const;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx);
    PROJ_FOR_TEST virtual bool reopen(PJ_CONTEXT *ctx);
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL GenericShiftGrid : public Grid {
  protected:
    std::vector<std::unique_ptr<GenericShiftGrid>> m_children{};

  public:
    PROJ_FOR_TEST GenericShiftGrid(const std::string &nameIn, int widthIn,
                                   int heightIn, const ExtentAndRes &extentIn);

    PROJ_FOR_TEST ~GenericShiftGrid() override;

    PROJ_FOR_TEST const GenericShiftGrid *gridAt(double x, double y) const;

    virtual const std::string &type() const = 0;

    PROJ_FOR_TEST virtual std::string unit(int sample) const = 0;

    PROJ_FOR_TEST virtual std::string description(int sample) const = 0;

    PROJ_FOR_TEST virtual int samplesPerPixel() const = 0;

    // x = 0 is western-most column, y = 0 is southern-most line
    PROJ_FOR_TEST virtual bool valueAt(int x, int y, int sample,
                                       float &out) const = 0;

    PROJ_FOR_TEST virtual bool valuesAt(int x_start, int y_start, int x_count,
                                        int y_count, int sample_count,
                                        const int *sample_idx, float *out,
                                        bool &nodataFound) const;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx) = 0;
};

// ---------------------------------------------------------------------------

class PROJ_GCC_DLL GenericShiftGridSet {
  protected:
    std::string m_name{};
    std::string m_format{};
    std::vector<std::unique_ptr<GenericShiftGrid>> m_grids{};

    GenericShiftGridSet();

  public:
    PROJ_FOR_TEST virtual ~GenericShiftGridSet();

    PROJ_FOR_TEST static std::unique_ptr<GenericShiftGridSet>
    open(PJ_CONTEXT *ctx, const std::string &filename);

    PROJ_FOR_TEST const std::string &name() const { return m_name; }
    PROJ_FOR_TEST const std::string &format() const { return m_format; }
    PROJ_FOR_TEST const std::vector<std::unique_ptr<GenericShiftGrid>> &
    grids() const {
        return m_grids;
    }
    PROJ_FOR_TEST const GenericShiftGrid *gridAt(double x, double y) const;
    PROJ_FOR_TEST const GenericShiftGrid *gridAt(const std::string &type,
                                                 double x, double y) const;

    PROJ_FOR_TEST virtual void reassign_context(PJ_CONTEXT *ctx);
    PROJ_FOR_TEST virtual bool reopen(PJ_CONTEXT *ctx);
};

// ---------------------------------------------------------------------------

typedef std::vector<std::unique_ptr<HorizontalShiftGridSet>> ListOfHGrids;
typedef std::vector<std::unique_ptr<VerticalShiftGridSet>> ListOfVGrids;
typedef std::vector<std::unique_ptr<GenericShiftGridSet>> ListOfGenericGrids;

ListOfVGrids pj_vgrid_init(PJ *P, const char *grids);
ListOfHGrids pj_hgrid_init(PJ *P, const char *grids);
ListOfGenericGrids pj_generic_grid_init(PJ *P, const char *grids);

PJ_LP pj_hgrid_value(PJ *P, const ListOfHGrids &grids, PJ_LP lp);
double pj_vgrid_value(PJ *P, const ListOfVGrids &, PJ_LP lp,
                      double vmultiplier);
PJ_LP pj_hgrid_apply(PJ_CONTEXT *ctx, const ListOfHGrids &grids, PJ_LP lp,
                     PJ_DIRECTION direction);

const GenericShiftGrid *pj_find_generic_grid(const ListOfGenericGrids &grids,
                                             const PJ_LP &input,
                                             GenericShiftGridSet *&gridSetOut);
bool pj_bilinear_interpolation_three_samples(
    PJ_CONTEXT *ctx, const GenericShiftGrid *grid, const PJ_LP &lp, int idx1,
    int idx2, int idx3, double &v1, double &v2, double &v3, bool &must_retry);

NS_PROJ_END

#endif // GRIDS_HPP_INCLUDED
