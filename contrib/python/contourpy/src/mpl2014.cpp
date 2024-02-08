// This file contains liberal use of asserts to assist code development and
// debugging.  Standard matplotlib builds disable asserts so they cause no
// performance reduction.  To enable the asserts, you need to undefine the
// NDEBUG macro, which is achieved by adding the following
//     undef_macros=['NDEBUG']
// to the appropriate make_extension call in setup.py, and then rebuilding.

#include "mpl2014.h"
#include "mpl_kind_code.h"
#include <algorithm>

namespace contourpy {
namespace mpl2014 {

// Point indices from current quad index.
#define POINT_SW (quad)
#define POINT_SE (quad+1)
#define POINT_NW (quad+_nx)
#define POINT_NE (quad+_nx+1)

// CacheItem masks, only accessed directly to set.  To read, use accessors
// detailed below.  1 and 2 refer to level indices (lower and upper).
#define MASK_Z_LEVEL           0x0003 // Combines the following two.
#define MASK_Z_LEVEL_1         0x0001 // z > lower_level.
#define MASK_Z_LEVEL_2         0x0002 // z > upper_level.
#define MASK_VISITED_1         0x0004 // Algorithm has visited this quad.
#define MASK_VISITED_2         0x0008
#define MASK_SADDLE_1          0x0010 // quad is a saddle quad.
#define MASK_SADDLE_2          0x0020
#define MASK_SADDLE_LEFT_1     0x0040 // Contours turn left at saddle quad.
#define MASK_SADDLE_LEFT_2     0x0080
#define MASK_SADDLE_START_SW_1 0x0100 // Next visit starts on S or W edge.
#define MASK_SADDLE_START_SW_2 0x0200
#define MASK_BOUNDARY_S        0x0400 // S edge of quad is a boundary.
#define MASK_BOUNDARY_W        0x0800 // W edge of quad is a boundary.
// EXISTS_QUAD bit is always used, but the 4 EXISTS_CORNER are only used if
// _corner_mask is true.  Only one of EXISTS_QUAD or EXISTS_??_CORNER is ever
// set per quad, hence not using unique bits for each; care is needed when
// testing for these flags as they overlap.
#define MASK_EXISTS_QUAD       0x1000 // All of quad exists (is not masked).
#define MASK_EXISTS_SW_CORNER  0x2000 // SW corner exists, NE corner is masked.
#define MASK_EXISTS_SE_CORNER  0x3000
#define MASK_EXISTS_NW_CORNER  0x4000
#define MASK_EXISTS_NE_CORNER  0x5000
#define MASK_EXISTS            0x7000 // Combines all 5 EXISTS masks.

// The following are only needed for filled contours.
#define MASK_VISITED_S        0x10000 // Algorithm has visited S boundary.
#define MASK_VISITED_W        0x20000 // Algorithm has visited W boundary.
#define MASK_VISITED_CORNER   0x40000 // Algorithm has visited corner edge.


// Accessors for various CacheItem masks.  li is shorthand for level_index.
#define Z_LEVEL(quad)            (_cache[quad] & MASK_Z_LEVEL)
#define Z_NE                     Z_LEVEL(POINT_NE)
#define Z_NW                     Z_LEVEL(POINT_NW)
#define Z_SE                     Z_LEVEL(POINT_SE)
#define Z_SW                     Z_LEVEL(POINT_SW)
#define VISITED(quad,li)         ((_cache[quad] & (li==1 ? MASK_VISITED_1 : MASK_VISITED_2)) != 0)
#define VISITED_S(quad)          ((_cache[quad] & MASK_VISITED_S) != 0)
#define VISITED_W(quad)          ((_cache[quad] & MASK_VISITED_W) != 0)
#define VISITED_CORNER(quad)     ((_cache[quad] & MASK_VISITED_CORNER) != 0)
#define SADDLE(quad,li)          ((_cache[quad] & (li==1 ? MASK_SADDLE_1 : MASK_SADDLE_2)) != 0)
#define SADDLE_LEFT(quad,li)     ((_cache[quad] & (li==1 ? MASK_SADDLE_LEFT_1 : MASK_SADDLE_LEFT_2)) != 0)
#define SADDLE_START_SW(quad,li) ((_cache[quad] & (li==1 ? MASK_SADDLE_START_SW_1 : MASK_SADDLE_START_SW_2)) != 0)
#define BOUNDARY_S(quad)         ((_cache[quad] & MASK_BOUNDARY_S) != 0)
#define BOUNDARY_W(quad)         ((_cache[quad] & MASK_BOUNDARY_W) != 0)
#define BOUNDARY_N(quad)         BOUNDARY_S(quad+_nx)
#define BOUNDARY_E(quad)         BOUNDARY_W(quad+1)
#define EXISTS_QUAD(quad)        ((_cache[quad] & MASK_EXISTS) == MASK_EXISTS_QUAD)
#define EXISTS_NONE(quad)        ((_cache[quad] & MASK_EXISTS) == 0)
// The following are only used if _corner_mask is true.
#define EXISTS_SW_CORNER(quad)   ((_cache[quad] & MASK_EXISTS) == MASK_EXISTS_SW_CORNER)
#define EXISTS_SE_CORNER(quad)   ((_cache[quad] & MASK_EXISTS) == MASK_EXISTS_SE_CORNER)
#define EXISTS_NW_CORNER(quad)   ((_cache[quad] & MASK_EXISTS) == MASK_EXISTS_NW_CORNER)
#define EXISTS_NE_CORNER(quad)   ((_cache[quad] & MASK_EXISTS) == MASK_EXISTS_NE_CORNER)
#define EXISTS_ANY_CORNER(quad)  (!EXISTS_NONE(quad) && !EXISTS_QUAD(quad))
#define EXISTS_W_EDGE(quad)      (EXISTS_QUAD(quad) || EXISTS_SW_CORNER(quad) || EXISTS_NW_CORNER(quad))
#define EXISTS_E_EDGE(quad)      (EXISTS_QUAD(quad) || EXISTS_SE_CORNER(quad) || EXISTS_NE_CORNER(quad))
#define EXISTS_S_EDGE(quad)      (EXISTS_QUAD(quad) || EXISTS_SW_CORNER(quad) || EXISTS_SE_CORNER(quad))
#define EXISTS_N_EDGE(quad)      (EXISTS_QUAD(quad) || EXISTS_NW_CORNER(quad) || EXISTS_NE_CORNER(quad))
// Note that EXISTS_NE_CORNER(quad) is equivalent to BOUNDARY_SW(quad), etc.



QuadEdge::QuadEdge(index_t quad_, Edge edge_)
    : quad(quad_), edge(edge_)
{}

bool QuadEdge::operator==(const QuadEdge& other) const
{
    return quad == other.quad && edge == other.edge;
}

std::ostream& operator<<(std::ostream& os, const QuadEdge& quad_edge)
{
    return os << quad_edge.quad << ' ' << quad_edge.edge;
}



XY::XY(const double& x_, const double& y_)
    : x(x_), y(y_)
{}

bool XY::operator==(const XY& other) const
{
    return x == other.x && y == other.y;
}

std::ostream& operator<<(std::ostream& os, const XY& xy)
{
    return os << '(' << xy.x << ' ' << xy.y << ')';
}



ContourLine::ContourLine(bool is_hole)
    : std::vector<XY>(),
      _is_hole(is_hole),
      _parent(0)
{}

void ContourLine::add_child(ContourLine* child)
{
    assert(!_is_hole && "Cannot add_child to a hole");
    assert(child != 0 && "Null child ContourLine");
    _children.push_back(child);
}

void ContourLine::clear_parent()
{
    assert(is_hole() && "Cannot clear parent of non-hole");
    assert(_parent != 0 && "Null parent ContourLine");
    _parent = 0;
}

const ContourLine::Children& ContourLine::get_children() const
{
    assert(!_is_hole && "Cannot get_children of a hole");
    return _children;
}

const ContourLine* ContourLine::get_parent() const
{
    assert(_is_hole && "Cannot get_parent of a non-hole");
    return _parent;
}

ContourLine* ContourLine::get_parent()
{
    assert(_is_hole && "Cannot get_parent of a non-hole");
    return _parent;
}

bool ContourLine::is_hole() const
{
    return _is_hole;
}

void ContourLine::set_parent(ContourLine* parent)
{
    assert(_is_hole && "Cannot set parent of a non-hole");
    assert(parent != 0 && "Null parent ContourLine");
    _parent = parent;
}

void ContourLine::write() const
{
    std::cout << "ContourLine " << this << " of " << size() << " points:";
    for (const_iterator it = begin(); it != end(); ++it)
        std::cout << ' ' << *it;
    if (is_hole())
        std::cout << " hole, parent=" << get_parent();
    else {
        std::cout << " not hole";
        if (!_children.empty()) {
            std::cout << ", children=";
            for (Children::const_iterator it = _children.begin();
                 it != _children.end(); ++it)
                std::cout << *it << ' ';
        }
    }
    std::cout << std::endl;
}



Contour::Contour()
{}

Contour::~Contour()
{
    delete_contour_lines();
}

void Contour::delete_contour_lines()
{
    for (iterator line_it = begin(); line_it != end(); ++line_it) {
        delete *line_it;
        *line_it = 0;
    }
    std::vector<ContourLine*>::clear();
}

void Contour::write() const
{
    std::cout << "Contour of " << size() << " lines." << std::endl;
    for (const_iterator it = begin(); it != end(); ++it)
        (*it)->write();
}



ParentCache::ParentCache(index_t nx, index_t x_chunk_points, index_t y_chunk_points)
    : _nx(nx),
      _x_chunk_points(x_chunk_points),
      _y_chunk_points(y_chunk_points),
      _lines(0),  // Initialised when first needed.
      _istart(0),
      _jstart(0)
{
    assert(_x_chunk_points > 0 && _y_chunk_points > 0 && "Chunk sizes must be positive");
}

ContourLine* ParentCache::get_parent(index_t quad)
{
    index_t index = index_to_index(quad);
    ContourLine* parent = _lines[index];
    while (parent == 0) {
        index -= _x_chunk_points;
        assert(index >= 0 && "Failed to find parent in chunk ParentCache");
        parent = _lines[index];
    }
    assert(parent != 0 && "Failed to find parent in chunk ParentCache");
    return parent;
}

index_t ParentCache::index_to_index(index_t quad) const
{
    index_t i = quad % _nx;
    index_t j = quad / _nx;
    index_t index = (i-_istart) + (j-_jstart)*_x_chunk_points;

    assert(i >= _istart && i < _istart + _x_chunk_points && "i-index outside chunk");
    assert(j >= _jstart && j < _jstart + _y_chunk_points && "j-index outside chunk");
    assert(index >= 0 && index < static_cast<index_t>(_lines.size()) &&
           "ParentCache index outside chunk");

    return index;
}

void ParentCache::set_chunk_starts(index_t istart, index_t jstart)
{
    assert(istart >= 0 && jstart >= 0 && "Chunk start indices cannot be negative");
    _istart = istart;
    _jstart = jstart;
    if (_lines.empty())
        _lines.resize(_x_chunk_points*_y_chunk_points, 0);
    else
        std::fill(_lines.begin(), _lines.end(), (ContourLine*)0);
}

void ParentCache::set_parent(index_t quad, ContourLine& contour_line)
{
    assert(!_lines.empty() && "Accessing ParentCache before it has been initialised");
    index_t index = index_to_index(quad);
    if (_lines[index] == 0)
        _lines[index] = contour_line.is_hole() ? contour_line.get_parent() : &contour_line;
}



Mpl2014ContourGenerator::Mpl2014ContourGenerator(
    const CoordinateArray& x, const CoordinateArray& y, const CoordinateArray& z,
    const MaskArray& mask, bool corner_mask, index_t x_chunk_size, index_t y_chunk_size)
    : _x(x),
      _y(y),
      _z(z),
      _nx(_z.ndim() > 1 ? _z.shape(1) : 0),
      _ny(_z.ndim() > 0 ? _z.shape(0) : 0),
      _n(_nx*_ny),
      _corner_mask(corner_mask),
      _x_chunk_size(calc_chunk_size(_nx, x_chunk_size)),
      _y_chunk_size(calc_chunk_size(_ny, y_chunk_size)),
      _nxchunk(calc_chunk_count(_nx, _x_chunk_size)),
      _nychunk(calc_chunk_count(_ny, _y_chunk_size)),
      _chunk_count(_nxchunk*_nychunk),
      _cache(new CacheItem[_n]),
      _parent_cache(_nx,
                    _x_chunk_size > 0 ? _x_chunk_size+1 : _nx,
                    _y_chunk_size > 0 ? _y_chunk_size+1 : _ny)
{
    if (_x.ndim() != 2 || _y.ndim() != 2 || _z.ndim() != 2)
        throw std::invalid_argument("x, y and z must all be 2D arrays");

    if (_x.shape(1) != _nx || _x.shape(0) != _ny ||
        _y.shape(1) != _nx || _y.shape(0) != _ny)
        throw std::invalid_argument("x, y and z arrays must have the same shape");

    if (_nx < 2 || _ny < 2)
        throw std::invalid_argument("x, y and z must all be at least 2x2 arrays");

    if (mask.ndim() != 0) {  // ndim == 0 if mask is not set, which is valid.
        if (mask.ndim() != 2)
            throw std::invalid_argument("mask array must be a 2D array");

        if (mask.shape(1) != _nx || mask.shape(0) != _ny)
            throw std::invalid_argument(
                "If mask is set it must be a 2D array with the same shape as z");
    }

    if (x_chunk_size < 0 || y_chunk_size < 0)
        throw std::invalid_argument("x_chunk_size and y_chunk_size cannot be negative");

    init_cache_grid(mask);
}

Mpl2014ContourGenerator::~Mpl2014ContourGenerator()
{
    delete [] _cache;
}

void Mpl2014ContourGenerator::append_contour_line_to_vertices_and_codes(
    ContourLine& contour_line, py::list& vertices_list, py::list& codes_list) const
{
    // Convert ContourLine to Python equivalent, and clear it for reuse.
    // This function is called once for each line generated in create_contour().
    // A line is either a closed line loop (in which case the last point is
    // identical to the first) or an open line strip.  Two NumPy arrays are
    // created for each line:
    //   vertices is a double array of shape (npoints, 2) containing the (x, y)
    //     coordinates of the points in the line
    //   codes is a uint8 array of shape (npoints,) containing the 'kind codes'
    //     which are defined in the Path class
    // and they are appended to the Python lists vertices_list and codes_list
    // respectively for return to the Python calling function.

    py::ssize_t npoints = static_cast<py::ssize_t>(contour_line.size());

    py::ssize_t vertices_dims[2] = {npoints, 2};
    PointArray vertices(vertices_dims);
    double* vertices_ptr = vertices.mutable_data();

    py::ssize_t codes_dims[1] = {npoints};
    CodeArray codes(codes_dims);
    unsigned char* codes_ptr = codes.mutable_data();

    for (ContourLine::const_iterator point = contour_line.begin();
         point != contour_line.end(); ++point) {
        *vertices_ptr++ = point->x;
        *vertices_ptr++ = point->y;
        *codes_ptr++ = (point == contour_line.begin() ? MOVETO : LINETO);
    }

    // Closed line loop has identical first and last (x, y) points.
    if (contour_line.size() > 1 && contour_line.front() == contour_line.back())
        *(codes_ptr-1) = CLOSEPOLY;

    vertices_list.append(vertices);
    codes_list.append(codes);

    contour_line.clear();
}

void Mpl2014ContourGenerator::append_contour_to_vertices_and_codes(
    Contour& contour, py::list& vertices_list, py::list& codes_list) const
{
    // Convert Contour to Python equivalent, and clear it for reuse.
    // This function is called once for each polygon generated in
    // create_filled_contour().  A polygon consists of an outer line loop
    // (called the parent) and zero or more inner line loops or holes (called
    // the children).  Two NumPy arrays are created for each polygon:
    //   vertices is a double array of shape (npoints, 2) containing the (x, y)
    //     coordinates of the points in the polygon (parent plus children)
    //   codes is a uint8 array of shape (npoints,) containing the 'kind codes'
    //     which are defined in the Path class
    // and they are appended to the Python lists vertices_list and codes_list
    // respectively for return to the Python calling function.

    // Convert Contour to python equivalent, and clear it.
    for (Contour::iterator line_it = contour.begin(); line_it != contour.end();
         ++line_it) {
        ContourLine& line = **line_it;
        if (line.is_hole()) {
            // If hole has already been converted to python its parent will be
            // set to 0 and it can be deleted.
            if (line.get_parent() != 0) {
                delete *line_it;
                *line_it = 0;
            }
        }
        else {
            // Non-holes are converted to python together with their child
            // holes so that they are rendered correctly.
            ContourLine::const_iterator point;
            ContourLine::Children::const_iterator children_it;

            const ContourLine::Children& children = line.get_children();
            py::ssize_t npoints = static_cast<py::ssize_t>(line.size() + 1);
            for (children_it = children.begin(); children_it != children.end(); ++children_it)
                 npoints += static_cast<py::ssize_t>((*children_it)->size() + 1);

            py::ssize_t vertices_dims[2] = {npoints, 2};
            PointArray vertices(vertices_dims);
            double* vertices_ptr = vertices.mutable_data();

            py::ssize_t codes_dims[1] = {npoints};
            CodeArray codes(codes_dims);
            unsigned char* codes_ptr = codes.mutable_data();

            for (point = line.begin(); point != line.end(); ++point) {
                *vertices_ptr++ = point->x;
                *vertices_ptr++ = point->y;
                *codes_ptr++ = (point == line.begin() ? MOVETO : LINETO);
            }
            point = line.begin();
            *vertices_ptr++ = point->x;
            *vertices_ptr++ = point->y;
            *codes_ptr++ = CLOSEPOLY;

            for (children_it = children.begin(); children_it != children.end(); ++children_it) {
                ContourLine& child = **children_it;
                for (point = child.begin(); point != child.end(); ++point) {
                    *vertices_ptr++ = point->x;
                    *vertices_ptr++ = point->y;
                    *codes_ptr++ = (point == child.begin() ? MOVETO : LINETO);
                }
                point = child.begin();
                *vertices_ptr++ = point->x;
                *vertices_ptr++ = point->y;
                *codes_ptr++ = CLOSEPOLY;

                child.clear_parent();  // To indicate it can be deleted.
            }

            vertices_list.append(vertices);
            codes_list.append(codes);

            delete *line_it;
            *line_it = 0;
        }
    }

    // Delete remaining contour lines.
    contour.delete_contour_lines();
}

index_t Mpl2014ContourGenerator::calc_chunk_count(index_t point_count, index_t chunk_size)
{
    // Accepts any values for point_count and chunk_size.

    if (chunk_size > 0 && point_count > 1) {
        index_t count = (point_count-1) / chunk_size;
        if (count*chunk_size < point_count-1)
            ++count;

        assert(count >= 1 && "Invalid chunk count");
        return count;
    }
    else
        return 1;
}

index_t Mpl2014ContourGenerator::calc_chunk_size(index_t point_count, index_t chunk_size)
{
    // Accepts any values for point_count and chunk_size.
    index_t ret = point_count - 1;
    if (chunk_size > 0)
        ret = std::min(chunk_size, ret);

    return std::max<index_t>(ret, 1);
}

void Mpl2014ContourGenerator::edge_interp(
    const QuadEdge& quad_edge, const double& level, ContourLine& contour_line)
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");
    interp(get_edge_point_index(quad_edge, true),
           get_edge_point_index(quad_edge, false),
           level, contour_line);
}

py::tuple Mpl2014ContourGenerator::filled(
    const double& lower_level, const double& upper_level)
{
    if (lower_level >= upper_level)
        throw std::invalid_argument("upper_level must be larger than lower_level");

    init_cache_levels(lower_level, upper_level);

    Contour contour;

    py::list vertices, codes;

    index_t ichunk, jchunk, istart, iend, jstart, jend;
    for (index_t ijchunk = 0; ijchunk < _chunk_count; ++ijchunk) {
        get_chunk_limits(ijchunk, ichunk, jchunk, istart, iend, jstart, jend);
        _parent_cache.set_chunk_starts(istart, jstart);

        for (index_t j = jstart; j < jend; ++j) {
            index_t quad_end = iend + j*_nx;
            for (index_t quad = istart + j*_nx; quad < quad_end; ++quad) {
                if (!EXISTS_NONE(quad))
                    single_quad_filled(contour, quad, lower_level, upper_level);
            }
        }

        // Clear VISITED_W and VISITED_S flags that are reused by later chunks.
        if (jchunk < _nychunk-1) {
            index_t quad_end = iend + jend*_nx;
            for (index_t quad = istart + jend*_nx; quad < quad_end; ++quad)
                _cache[quad] &= ~MASK_VISITED_S;
        }

        if (ichunk < _nxchunk-1) {
            index_t quad_end = iend + jend*_nx;
            for (index_t quad = iend + jstart*_nx; quad < quad_end; quad += _nx)
                _cache[quad] &= ~MASK_VISITED_W;
        }

        // Create python objects to return for this chunk.
        append_contour_to_vertices_and_codes(contour, vertices, codes);
    }

    return py::make_tuple(vertices, codes);
}

unsigned int Mpl2014ContourGenerator::follow_boundary(
    ContourLine& contour_line, QuadEdge& quad_edge, const double& lower_level,
    const double& upper_level, unsigned int level_index, const QuadEdge& start_quad_edge)
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");
    assert(is_edge_a_boundary(quad_edge) && "Not a boundary edge");
    assert((level_index == 1 || level_index == 2) && "level index must be 1 or 2");
    assert(start_quad_edge.quad >= 0 && start_quad_edge.quad < _n &&
           "Start quad index out of bounds");
    assert(start_quad_edge.edge != Edge_None && "Invalid start edge");

    // Only called for filled contours, so always updates _parent_cache.
    unsigned int end_level = 0;
    bool first_edge = true;
    bool stop = false;
    index_t& quad = quad_edge.quad;

    while (true) {
        // Levels of start and end points of quad_edge.
        unsigned int start_level =
            (first_edge ? Z_LEVEL(get_edge_point_index(quad_edge, true)) : end_level);
        index_t end_point = get_edge_point_index(quad_edge, false);
        end_level = Z_LEVEL(end_point);

        if (level_index == 1) {
            if (start_level <= level_index && end_level == 2) {
                // Increasing z, switching levels from 1 to 2.
                level_index = 2;
                stop = true;
            }
            else if (start_level >= 1 && end_level == 0) {
                // Decreasing z, keeping same level.
                stop = true;
            }
        }
        else { // level_index == 2
            if (start_level <= level_index && end_level == 2) {
                // Increasing z, keeping same level.
                stop = true;
            }
            else if (start_level >= 1 && end_level == 0) {
                // Decreasing z, switching levels from 2 to 1.
                level_index = 1;
                stop = true;
            }
        }

        if (!first_edge && !stop && quad_edge == start_quad_edge)
            // Return if reached start point of contour line.  Do this before
            // checking/setting VISITED flags as will already have been
            // visited.
            break;

        switch (quad_edge.edge) {
            case Edge_E:
                assert(!VISITED_W(quad+1) && "Already visited");
                _cache[quad+1] |= MASK_VISITED_W;
                break;
            case Edge_N:
                assert(!VISITED_S(quad+_nx) && "Already visited");
                _cache[quad+_nx] |= MASK_VISITED_S;
                break;
            case Edge_W:
                assert(!VISITED_W(quad) && "Already visited");
                _cache[quad] |= MASK_VISITED_W;
                break;
            case Edge_S:
               assert(!VISITED_S(quad) && "Already visited");
                _cache[quad] |= MASK_VISITED_S;
                break;
            case Edge_NE:
            case Edge_NW:
            case Edge_SW:
            case Edge_SE:
                assert(!VISITED_CORNER(quad) && "Already visited");
                _cache[quad] |= MASK_VISITED_CORNER;
                break;
            default:
                assert(0 && "Invalid Edge");
                break;
        }

        if (stop) {
            // Exiting boundary to enter interior.
            edge_interp(quad_edge, level_index == 1 ? lower_level : upper_level, contour_line);
            break;
        }

        move_to_next_boundary_edge(quad_edge);

        // Just moved to new quad edge, so label parent of start of quad edge.
        switch (quad_edge.edge) {
            case Edge_W:
            case Edge_SW:
            case Edge_S:
            case Edge_SE:
                if (!EXISTS_SE_CORNER(quad))
                    _parent_cache.set_parent(quad, contour_line);
                break;
            case Edge_E:
            case Edge_NE:
            case Edge_N:
            case Edge_NW:
                if (!EXISTS_SW_CORNER(quad))
                    _parent_cache.set_parent(quad + 1, contour_line);
                break;
            default:
                assert(0 && "Invalid edge");
                break;
        }

        // Add point to contour.
        get_point_xy(end_point, contour_line);

        if (first_edge)
            first_edge = false;
    }

    return level_index;
}

void Mpl2014ContourGenerator::follow_interior(
    ContourLine& contour_line, QuadEdge& quad_edge, unsigned int level_index, const double& level,
    bool want_initial_point, const QuadEdge* start_quad_edge, unsigned int start_level_index,
    bool set_parents)
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds.");
    assert(quad_edge.edge != Edge_None && "Invalid edge");
    assert((level_index == 1 || level_index == 2) && "level index must be 1 or 2");
    assert((start_quad_edge == 0 ||
            (start_quad_edge->quad >= 0 && start_quad_edge->quad < _n)) &&
           "Start quad index out of bounds.");
    assert((start_quad_edge == 0 || start_quad_edge->edge != Edge_None) && "Invalid start edge");
    assert((start_level_index == 1 || start_level_index == 2) &&
           "start level index must be 1 or 2");

    index_t& quad = quad_edge.quad;
    Edge& edge = quad_edge.edge;

    if (want_initial_point)
        edge_interp(quad_edge, level, contour_line);

    CacheItem visited_mask = (level_index == 1 ? MASK_VISITED_1 : MASK_VISITED_2);
    CacheItem saddle_mask = (level_index == 1 ? MASK_SADDLE_1 : MASK_SADDLE_2);
    Dir dir = Dir_Straight;

    while (true) {
        assert(!EXISTS_NONE(quad) && "Quad does not exist");
        assert(!(_cache[quad] & visited_mask) && "Quad already visited");

        // Determine direction to move to next quad.  If the quad is already
        // labelled as a saddle quad then the direction is easily read from
        // the cache.  Otherwise the direction is determined differently
        // depending on whether the quad is a corner quad or not.

        if (_cache[quad] & saddle_mask) {
            // Already identified as a saddle quad, so direction is easy.
            dir = (SADDLE_LEFT(quad,level_index) ? Dir_Left : Dir_Right);
            _cache[quad] |= visited_mask;
        }
        else if (EXISTS_ANY_CORNER(quad)) {
            // Need z-level of point opposite the entry edge, as that
            // determines whether contour turns left or right.
            index_t point_opposite = -1;
            switch (edge) {
                case Edge_E:
                    point_opposite = (EXISTS_SE_CORNER(quad) ? POINT_SW : POINT_NW);
                    break;
                case Edge_N:
                    point_opposite = (EXISTS_NW_CORNER(quad) ? POINT_SW : POINT_SE);
                    break;
                case Edge_W:
                    point_opposite = (EXISTS_SW_CORNER(quad) ? POINT_SE : POINT_NE);
                    break;
                case Edge_S:
                    point_opposite = (EXISTS_SW_CORNER(quad) ? POINT_NW : POINT_NE);
                    break;
                case Edge_NE: point_opposite = POINT_SW; break;
                case Edge_NW: point_opposite = POINT_SE; break;
                case Edge_SW: point_opposite = POINT_NE; break;
                case Edge_SE: point_opposite = POINT_NW; break;
                default: assert(0 && "Invalid edge"); break;
            }
            assert(point_opposite != -1 && "Failed to find opposite point");

            // Lower-level polygons (level_index == 1) always have higher
            // values to the left of the contour.  Upper-level contours
            // (level_index == 2) are reversed, which is what the fancy XOR
            // does below.
            if ((Z_LEVEL(point_opposite) >= level_index) ^ (level_index == 2))
                dir = Dir_Right;
            else
                dir = Dir_Left;
            _cache[quad] |= visited_mask;
        }
        else {
            // Calculate configuration of this quad.
            index_t point_left = -1, point_right = -1;
            switch (edge) {
                case Edge_E: point_left = POINT_SW; point_right = POINT_NW; break;
                case Edge_N: point_left = POINT_SE; point_right = POINT_SW; break;
                case Edge_W: point_left = POINT_NE; point_right = POINT_SE; break;
                case Edge_S: point_left = POINT_NW; point_right = POINT_NE; break;
                default: assert(0 && "Invalid edge"); break;
            }

            unsigned int config = ((Z_LEVEL(point_left) >= level_index) << 1) |
                                   (Z_LEVEL(point_right) >= level_index);

            // Upper level (level_index == 2) polygons are reversed compared to
            // lower level ones, i.e. higher values on the right rather than
            // the left.
            if (level_index == 2)
                config = 3 - config;

            // Calculate turn direction to move to next quad along contour line.
            if (config == 1) {
                // New saddle quad, set up cache bits for it.
                double zmid = 0.25*(get_point_z(POINT_SW) +
                                    get_point_z(POINT_SE) +
                                    get_point_z(POINT_NW) +
                                    get_point_z(POINT_NE));
                _cache[quad] |= (level_index == 1 ? MASK_SADDLE_1 : MASK_SADDLE_2);
                if ((zmid > level) ^ (level_index == 2)) {
                    dir = Dir_Right;
                }
                else {
                    dir = Dir_Left;
                    _cache[quad] |= (level_index == 1 ? MASK_SADDLE_LEFT_1 : MASK_SADDLE_LEFT_2);
                }
                if (edge == Edge_N || edge == Edge_E) {
                    // Next visit to this quad must start on S or W.
                    _cache[quad] |=
                        (level_index == 1 ? MASK_SADDLE_START_SW_1 : MASK_SADDLE_START_SW_2);
                }
            }
            else {
                // Normal (non-saddle) quad.
                dir = (config == 0 ? Dir_Left : (config == 3 ? Dir_Right : Dir_Straight));
                _cache[quad] |= visited_mask;
            }
        }

        // Use dir to determine exit edge.
        edge = get_exit_edge(quad_edge, dir);

        if (set_parents) {
            if (edge == Edge_E)
                _parent_cache.set_parent(quad+1, contour_line);
            else if (edge == Edge_W)
                _parent_cache.set_parent(quad, contour_line);
        }

        // Add new point to contour line.
        edge_interp(quad_edge, level, contour_line);

        // Stop if reached boundary.
        if (is_edge_a_boundary(quad_edge))
            break;

        move_to_next_quad(quad_edge);
        assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");

        // Return if reached start point of contour line.
        if (start_quad_edge != 0 &&
            quad_edge == *start_quad_edge &&
            level_index == start_level_index)
            break;
    }
}

py::tuple Mpl2014ContourGenerator::get_chunk_count() const
{
    return py::make_tuple(_nychunk, _nxchunk);
}

void Mpl2014ContourGenerator::get_chunk_limits(
    index_t ijchunk, index_t& ichunk, index_t& jchunk, index_t& istart, index_t& iend,
    index_t& jstart, index_t& jend)
{
    assert(ijchunk >= 0 && ijchunk < _chunk_count && "ijchunk out of bounds");
    ichunk = ijchunk % _nxchunk;
    jchunk = ijchunk / _nxchunk;
    istart = ichunk*_x_chunk_size;
    iend = (ichunk == _nxchunk-1 ? _nx : (ichunk+1)*_x_chunk_size);
    jstart = jchunk*_y_chunk_size;
    jend = (jchunk == _nychunk-1 ? _ny : (jchunk+1)*_y_chunk_size);
}

py::tuple Mpl2014ContourGenerator::get_chunk_size() const
{
    return py::make_tuple(_y_chunk_size, _x_chunk_size);
}

bool Mpl2014ContourGenerator::get_corner_mask() const
{
    return _corner_mask;
}

Edge Mpl2014ContourGenerator::get_corner_start_edge(
    index_t quad, unsigned int level_index) const
{
    assert(quad >= 0 && quad < _n && "Quad index out of bounds");
    assert((level_index == 1 || level_index == 2) && "level index must be 1 or 2");
    assert(EXISTS_ANY_CORNER(quad) && "Quad is not a corner");

    // Diagram for NE corner.  Rotate for other corners.
    //
    //           edge12
    // point1 +---------+ point2
    //          \       |
    //            \     | edge23
    //       edge31 \   |
    //                \ |
    //                  + point3
    //
    index_t point1, point2, point3;
    Edge edge12, edge23, edge31;
    switch (_cache[quad] & MASK_EXISTS) {
        case MASK_EXISTS_SW_CORNER:
            point1 = POINT_SE; point2 = POINT_SW; point3 = POINT_NW;
            edge12 = Edge_S;   edge23 = Edge_W;   edge31 = Edge_NE;
            break;
        case MASK_EXISTS_SE_CORNER:
            point1 = POINT_NE; point2 = POINT_SE; point3 = POINT_SW;
            edge12 = Edge_E;   edge23 = Edge_S;   edge31 = Edge_NW;
            break;
        case MASK_EXISTS_NW_CORNER:
            point1 = POINT_SW; point2 = POINT_NW; point3 = POINT_NE;
            edge12 = Edge_W;   edge23 = Edge_N;   edge31 = Edge_SE;
            break;
        case MASK_EXISTS_NE_CORNER:
            point1 = POINT_NW; point2 = POINT_NE; point3 = POINT_SE;
            edge12 = Edge_N;   edge23 = Edge_E;   edge31 = Edge_SW;
            break;
        default:
            assert(0 && "Invalid EXISTS for quad");
            return Edge_None;
    }

    unsigned int config = ((Z_LEVEL(point1) >= level_index) << 2) |
                          ((Z_LEVEL(point2) >= level_index) << 1) |
                          ((Z_LEVEL(point3) >= level_index) << 0);

    // Upper level (level_index == 2) polygons are reversed compared to lower
    // level ones, i.e. higher values on the right rather than the left.
    if (level_index == 2)
        config = 7 - config;

    switch (config) {
        case 0: return Edge_None;
        case 1: return edge23;
        case 2: return edge12;
        case 3: return edge12;
        case 4: return edge31;
        case 5: return edge23;
        case 6: return edge31;
        case 7: return Edge_None;
        default: assert(0 && "Invalid config"); return Edge_None;
    }
}

index_t Mpl2014ContourGenerator::get_edge_point_index(
    const QuadEdge& quad_edge, bool start) const
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");

    // Edges are ordered anticlockwise around their quad, as indicated by
    // directions of arrows in diagrams below.
    //           Full quad                    NW corner (others similar)
    //
    //  POINT_NW   Edge_N   POINT_NE         POINT_NW   Edge_N   POINT_NE
    //          +----<-----+                         +----<-----+
    //          |          |                         |         /
    //          |          |                         | quad  /
    //  Edge_W  V   quad   ^  Edge_E         Edge_W  V     ^
    //          |          |                         |   /  Edge_SE
    //          |          |                         | /
    //          +---->-----+                         +
    //  POINT_SW   Edge_S   POINT_SE         POINT_SW
    //
    const index_t& quad = quad_edge.quad;
    switch (quad_edge.edge) {
        case Edge_E:  return (start ? POINT_SE : POINT_NE);
        case Edge_N:  return (start ? POINT_NE : POINT_NW);
        case Edge_W:  return (start ? POINT_NW : POINT_SW);
        case Edge_S:  return (start ? POINT_SW : POINT_SE);
        case Edge_NE: return (start ? POINT_SE : POINT_NW);
        case Edge_NW: return (start ? POINT_NE : POINT_SW);
        case Edge_SW: return (start ? POINT_NW : POINT_SE);
        case Edge_SE: return (start ? POINT_SW : POINT_NE);
        default: assert(0 && "Invalid edge"); return 0;
    }
}

Edge Mpl2014ContourGenerator::get_exit_edge(const QuadEdge& quad_edge, Dir dir) const
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");

    const index_t& quad = quad_edge.quad;
    const Edge& edge = quad_edge.edge;
    if (EXISTS_ANY_CORNER(quad)) {
        // Corner directions are always left or right.  A corner is a triangle,
        // entered via one edge so the other two edges are the left and right
        // ones.
        switch (edge) {
            case Edge_E:
                return (EXISTS_SE_CORNER(quad)
                           ? (dir == Dir_Left ? Edge_S : Edge_NW)
                           : (dir == Dir_Right ? Edge_N : Edge_SW));
            case Edge_N:
                return (EXISTS_NW_CORNER(quad)
                           ? (dir == Dir_Right ? Edge_W : Edge_SE)
                           : (dir == Dir_Left ? Edge_E : Edge_SW));
            case Edge_W:
                return (EXISTS_SW_CORNER(quad)
                           ? (dir == Dir_Right ? Edge_S : Edge_NE)
                           : (dir == Dir_Left ? Edge_N : Edge_SE));
            case Edge_S:
                return (EXISTS_SW_CORNER(quad)
                           ? (dir == Dir_Left ? Edge_W : Edge_NE)
                           : (dir == Dir_Right ? Edge_E : Edge_NW));
            case Edge_NE: return (dir == Dir_Left ? Edge_S : Edge_W);
            case Edge_NW: return (dir == Dir_Left ? Edge_E : Edge_S);
            case Edge_SW: return (dir == Dir_Left ? Edge_N : Edge_E);
            case Edge_SE: return (dir == Dir_Left ? Edge_W : Edge_N);
            default: assert(0 && "Invalid edge"); return Edge_None;
        }
    }
    else {
        // A full quad has four edges, entered via one edge so that other three
        // edges correspond to left, straight and right directions.
        switch (edge) {
            case Edge_E:
                return (dir == Dir_Left ? Edge_S : (dir == Dir_Right ? Edge_N : Edge_W));
            case Edge_N:
                return (dir == Dir_Left ? Edge_E : (dir == Dir_Right ? Edge_W : Edge_S));
            case Edge_W:
                return (dir == Dir_Left ? Edge_N : (dir == Dir_Right ? Edge_S : Edge_E));
            case Edge_S:
                return (dir == Dir_Left ? Edge_W : (dir == Dir_Right ? Edge_E : Edge_N));
            default: assert(0 && "Invalid edge"); return Edge_None;
        }
    }
}

const double& Mpl2014ContourGenerator::get_point_x(index_t point) const
{
    assert(point >= 0 && point < _n && "Point index out of bounds.");
    return _x.data()[point];
}

const double& Mpl2014ContourGenerator::get_point_y(index_t point) const
{
    assert(point >= 0 && point < _n && "Point index out of bounds.");
    return _y.data()[point];
}

void Mpl2014ContourGenerator::get_point_xy(
    index_t point, ContourLine& contour_line) const
{
    assert(point >= 0 && point < _n && "Point index out of bounds.");
    contour_line.emplace_back(_x.data()[point], _y.data()[point]);
}

const double& Mpl2014ContourGenerator::get_point_z(index_t point) const
{
    assert(point >= 0 && point < _n && "Point index out of bounds.");
    return _z.data()[point];
}

Edge Mpl2014ContourGenerator::get_quad_start_edge(
    index_t quad, unsigned int level_index) const
{
    assert(quad >= 0 && quad < _n && "Quad index out of bounds");
    assert((level_index == 1 || level_index == 2) && "level index must be 1 or 2");
    assert(EXISTS_QUAD(quad) && "Quad does not exist");

    unsigned int config = ((Z_NW >= level_index) << 3) |
                          ((Z_NE >= level_index) << 2) |
                          ((Z_SW >= level_index) << 1) |
                          ((Z_SE >= level_index) << 0);

    // Upper level (level_index == 2) polygons are reversed compared to lower
    // level ones, i.e. higher values on the right rather than the left.
    if (level_index == 2)
        config = 15 - config;

    switch (config) {
        case  0: return Edge_None;
        case  1: return Edge_E;
        case  2: return Edge_S;
        case  3: return Edge_E;
        case  4: return Edge_N;
        case  5: return Edge_N;
        case  6:
            // If already identified as a saddle quad then the start edge is
            // read from the cache.  Otherwise return either valid start edge
            // and the subsequent call to follow_interior() will correctly set
            // up saddle bits in cache.
            if (!SADDLE(quad,level_index) || SADDLE_START_SW(quad,level_index))
                return Edge_S;
            else
                return Edge_N;
        case  7: return Edge_N;
        case  8: return Edge_W;
        case  9:
            // See comment for 6 above.
            if (!SADDLE(quad,level_index) || SADDLE_START_SW(quad,level_index))
                return Edge_W;
            else
                return Edge_E;
        case 10: return Edge_S;
        case 11: return Edge_E;
        case 12: return Edge_W;
        case 13: return Edge_W;
        case 14: return Edge_S;
        case 15: return Edge_None;
        default: assert(0 && "Invalid config"); return Edge_None;
    }
}

Edge Mpl2014ContourGenerator::get_start_edge(index_t quad, unsigned int level_index) const
{
    if (EXISTS_ANY_CORNER(quad))
        return get_corner_start_edge(quad, level_index);
    else
        return get_quad_start_edge(quad, level_index);
}

void Mpl2014ContourGenerator::init_cache_grid(const MaskArray& mask)
{
    index_t i, j, quad;

    if (mask.ndim() == 0) {
        // No mask, easy to calculate quad existence and boundaries together.
        quad = 0;
        for (j = 0; j < _ny; ++j) {
            for (i = 0; i < _nx; ++i, ++quad) {
                _cache[quad] = 0;

                if (i < _nx-1 && j < _ny-1)
                    _cache[quad] |= MASK_EXISTS_QUAD;

                if ((i % _x_chunk_size == 0 || i == _nx-1) && j < _ny-1)
                    _cache[quad] |= MASK_BOUNDARY_W;

                if ((j % _y_chunk_size == 0 || j == _ny-1) && i < _nx-1)
                    _cache[quad] |= MASK_BOUNDARY_S;
            }
        }
    }
    else {
        const bool* mask_ptr = mask.data();

        // Have mask so use two stages.
        // Stage 1, determine if quads/corners exist.
        quad = 0;
        for (j = 0; j < _ny; ++j) {
            for (i = 0; i < _nx; ++i, ++quad) {
                _cache[quad] = 0;

                if (i < _nx-1 && j < _ny-1) {
                    unsigned int config = (mask_ptr[POINT_NW] << 3) |
                                          (mask_ptr[POINT_NE] << 2) |
                                          (mask_ptr[POINT_SW] << 1) |
                                          (mask_ptr[POINT_SE] << 0);

                    if (_corner_mask) {
                         switch (config) {
                            case 0: _cache[quad] = MASK_EXISTS_QUAD; break;
                            case 1: _cache[quad] = MASK_EXISTS_NW_CORNER; break;
                            case 2: _cache[quad] = MASK_EXISTS_NE_CORNER; break;
                            case 4: _cache[quad] = MASK_EXISTS_SW_CORNER; break;
                            case 8: _cache[quad] = MASK_EXISTS_SE_CORNER; break;
                            default:
                                // Do nothing, quad is masked out.
                                break;
                        }
                    }
                    else if (config == 0)
                        _cache[quad] = MASK_EXISTS_QUAD;
                }
            }
        }

        // Stage 2, calculate W and S boundaries.  For each quad use boundary
        // data already calculated for quads to W and S, so must iterate
        // through quads in correct order (increasing i and j indices).
        // Cannot use boundary data for quads to E and N as have not yet
        // calculated it.
        quad = 0;
        for (j = 0; j < _ny; ++j) {
            for (i = 0; i < _nx; ++i, ++quad) {
                if (_corner_mask) {
                    bool W_exists_none = (i == 0 || EXISTS_NONE(quad-1));
                    bool S_exists_none = (j == 0 || EXISTS_NONE(quad-_nx));
                    bool W_exists_E_edge = (i > 0 && EXISTS_E_EDGE(quad-1));
                    bool S_exists_N_edge = (j > 0 && EXISTS_N_EDGE(quad-_nx));

                    if ((EXISTS_W_EDGE(quad) && W_exists_none) ||
                        (EXISTS_NONE(quad) && W_exists_E_edge) ||
                        (i % _x_chunk_size == 0 && EXISTS_W_EDGE(quad) && W_exists_E_edge))
                        _cache[quad] |= MASK_BOUNDARY_W;

                    if ((EXISTS_S_EDGE(quad) && S_exists_none) ||
                        (EXISTS_NONE(quad) && S_exists_N_edge) ||
                        (j % _y_chunk_size == 0 && EXISTS_S_EDGE(quad) && S_exists_N_edge))
                        _cache[quad] |= MASK_BOUNDARY_S;
                }
                else {
                    bool W_exists_quad = (i > 0 && EXISTS_QUAD(quad-1));
                    bool S_exists_quad = (j > 0 && EXISTS_QUAD(quad-_nx));

                    if ((EXISTS_QUAD(quad) != W_exists_quad) ||
                        (i % _x_chunk_size == 0 && EXISTS_QUAD(quad) && W_exists_quad))
                        _cache[quad] |= MASK_BOUNDARY_W;

                    if ((EXISTS_QUAD(quad) != S_exists_quad) ||
                        (j % _y_chunk_size == 0 && EXISTS_QUAD(quad) && S_exists_quad))
                        _cache[quad] |= MASK_BOUNDARY_S;
                }
            }
        }
    }
}

void Mpl2014ContourGenerator::init_cache_levels(
    const double& lower_level, const double& upper_level)
{
    assert(!(upper_level < lower_level) && "upper and lower levels are wrong way round");

    bool two_levels = (lower_level != upper_level);
    CacheItem keep_mask =
        (_corner_mask ? MASK_EXISTS | MASK_BOUNDARY_S | MASK_BOUNDARY_W
                      : MASK_EXISTS_QUAD | MASK_BOUNDARY_S | MASK_BOUNDARY_W);

    if (two_levels) {
        const double* z_ptr = _z.data();
        for (index_t quad = 0; quad < _n; ++quad, ++z_ptr) {
            _cache[quad] &= keep_mask;
            if (*z_ptr > upper_level)
                _cache[quad] |= MASK_Z_LEVEL_2;
            else if (*z_ptr > lower_level)
                _cache[quad] |= MASK_Z_LEVEL_1;
        }
    }
    else {
        const double* z_ptr = _z.data();
        for (index_t quad = 0; quad < _n; ++quad, ++z_ptr) {
            _cache[quad] &= keep_mask;
            if (*z_ptr > lower_level)
                _cache[quad] |= MASK_Z_LEVEL_1;
        }
   }
}

void Mpl2014ContourGenerator::interp(
    index_t point1, index_t point2, const double& level, ContourLine& contour_line) const
{
    assert(point1 >= 0 && point1 < _n && "Point index 1 out of bounds.");
    assert(point2 >= 0 && point2 < _n && "Point index 2 out of bounds.");
    assert(point1 != point2 && "Identical points");
    double fraction = (get_point_z(point2) - level) / (get_point_z(point2) - get_point_z(point1));
    contour_line.emplace_back(
        get_point_x(point1)*fraction + get_point_x(point2)*(1.0 - fraction),
        get_point_y(point1)*fraction + get_point_y(point2)*(1.0 - fraction));
}

bool Mpl2014ContourGenerator::is_edge_a_boundary(
    const QuadEdge& quad_edge) const
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");

    switch (quad_edge.edge) {
        case Edge_E:  return BOUNDARY_E(quad_edge.quad);
        case Edge_N:  return BOUNDARY_N(quad_edge.quad);
        case Edge_W:  return BOUNDARY_W(quad_edge.quad);
        case Edge_S:  return BOUNDARY_S(quad_edge.quad);
        case Edge_NE: return EXISTS_SW_CORNER(quad_edge.quad);
        case Edge_NW: return EXISTS_SE_CORNER(quad_edge.quad);
        case Edge_SW: return EXISTS_NE_CORNER(quad_edge.quad);
        case Edge_SE: return EXISTS_NW_CORNER(quad_edge.quad);
        default: assert(0 && "Invalid edge"); return true;
    }
}

py::tuple Mpl2014ContourGenerator::lines(const double& level)
{
    init_cache_levels(level, level);

    py::list vertices_list, codes_list;

    // Lines that start and end on boundaries.
    index_t ichunk, jchunk, istart, iend, jstart, jend;
    for (index_t ijchunk = 0; ijchunk < _chunk_count; ++ijchunk) {
        get_chunk_limits(ijchunk, ichunk, jchunk, istart, iend, jstart, jend);

        for (index_t j = jstart; j < jend; ++j) {
            index_t quad_end = iend + j*_nx;
            for (index_t quad = istart + j*_nx; quad < quad_end; ++quad) {
                if (EXISTS_NONE(quad) || VISITED(quad,1)) continue;

                if (BOUNDARY_S(quad) && Z_SW >= 1 && Z_SE < 1 &&
                    start_line(vertices_list, codes_list, quad, Edge_S, level)) continue;

                if (BOUNDARY_W(quad) && Z_NW >= 1 && Z_SW < 1 &&
                    start_line(vertices_list, codes_list, quad, Edge_W, level)) continue;

                if (BOUNDARY_N(quad) && Z_NE >= 1 && Z_NW < 1 &&
                    start_line(vertices_list, codes_list, quad, Edge_N, level)) continue;

                if (BOUNDARY_E(quad) && Z_SE >= 1 && Z_NE < 1 &&
                    start_line(vertices_list, codes_list, quad, Edge_E, level)) continue;

                if (_corner_mask) {
                    // Equates to NE boundary.
                    if (EXISTS_SW_CORNER(quad) && Z_SE >= 1 && Z_NW < 1 &&
                        start_line(vertices_list, codes_list, quad, Edge_NE, level)) continue;

                    // Equates to NW boundary.
                    if (EXISTS_SE_CORNER(quad) && Z_NE >= 1 && Z_SW < 1 &&
                        start_line(vertices_list, codes_list, quad, Edge_NW, level)) continue;

                    // Equates to SE boundary.
                    if (EXISTS_NW_CORNER(quad) && Z_SW >= 1 && Z_NE < 1 &&
                        start_line(vertices_list, codes_list, quad, Edge_SE, level)) continue;

                    // Equates to SW boundary.
                    if (EXISTS_NE_CORNER(quad) && Z_NW >= 1 && Z_SE < 1 &&
                        start_line(vertices_list, codes_list, quad, Edge_SW, level)) continue;
                }
            }
        }
    }

    // Internal loops.
    ContourLine contour_line(false);  // Reused for each contour line.
    for (index_t ijchunk = 0; ijchunk < _chunk_count; ++ijchunk) {
        get_chunk_limits(ijchunk, ichunk, jchunk, istart, iend, jstart, jend);

        for (index_t j = jstart; j < jend; ++j) {
            index_t quad_end = iend + j*_nx;
            for (index_t quad = istart + j*_nx; quad < quad_end; ++quad) {
                if (EXISTS_NONE(quad) || VISITED(quad,1))
                    continue;

                Edge start_edge = get_start_edge(quad, 1);
                if (start_edge == Edge_None)
                    continue;

                QuadEdge quad_edge(quad, start_edge);
                QuadEdge start_quad_edge(quad_edge);

                // To obtain output identical to that produced by legacy code,
                // sometimes need to ignore the first point and add it on the
                // end instead.
                bool ignore_first = (start_edge == Edge_N);
                follow_interior(
                    contour_line, quad_edge, 1, level, !ignore_first, &start_quad_edge, 1, false);
                if (ignore_first && !contour_line.empty())
                    contour_line.push_back(contour_line.front());
                append_contour_line_to_vertices_and_codes(contour_line, vertices_list, codes_list);

                // Repeat if saddle point but not visited.
                if (SADDLE(quad,1) && !VISITED(quad,1))
                    --quad;
            }
        }
    }

    return py::make_tuple(vertices_list, codes_list);
}

void Mpl2014ContourGenerator::move_to_next_boundary_edge(
    QuadEdge& quad_edge) const
{
    assert(is_edge_a_boundary(quad_edge) && "QuadEdge is not a boundary");

    index_t& quad = quad_edge.quad;
    Edge& edge = quad_edge.edge;

    quad = get_edge_point_index(quad_edge, false);

    // quad is now such that POINT_SW is the end point of the quad_edge passed
    // to this function.

    // To find the next boundary edge, first attempt to turn left 135 degrees
    // and if that edge is a boundary then move to it.  If not, attempt to turn
    // left 90 degrees, then left 45 degrees, then straight on, etc, until can
    // move.
    // First determine which edge to attempt first.
    int index = 0;
    switch (edge) {
        case Edge_E:  index = 0; break;
        case Edge_SE: index = 1; break;
        case Edge_S:  index = 2; break;
        case Edge_SW: index = 3; break;
        case Edge_W:  index = 4; break;
        case Edge_NW: index = 5; break;
        case Edge_N:  index = 6; break;
        case Edge_NE: index = 7; break;
        default: assert(0 && "Invalid edge"); break;
    }

    // If _corner_mask not set, only need to consider odd index in loop below.
    if (!_corner_mask)
        ++index;

    // Try each edge in turn until a boundary is found.
    int start_index = index;
    do
    {
        switch (index) {
            case 0:
                if (EXISTS_SE_CORNER(quad-_nx-1)) { // Equivalent to BOUNDARY_NW
                    quad -= _nx+1;
                    edge = Edge_NW;
                    return;
                }
                break;
            case 1:
                if (BOUNDARY_N(quad-_nx-1)) {
                    quad -= _nx+1;
                    edge = Edge_N;
                    return;
                }
                break;
            case 2:
                if (EXISTS_SW_CORNER(quad-1)) {  // Equivalent to BOUNDARY_NE
                    quad -= 1;
                    edge = Edge_NE;
                    return;
                }
                break;
            case 3:
                if (BOUNDARY_E(quad-1)) {
                    quad -= 1;
                    edge = Edge_E;
                    return;
                }
                break;
            case 4:
                if (EXISTS_NW_CORNER(quad)) {  // Equivalent to BOUNDARY_SE
                    edge = Edge_SE;
                    return;
                }
                break;
            case 5:
                if (BOUNDARY_S(quad)) {
                    edge = Edge_S;
                    return;
                }
                break;
            case 6:
                if (EXISTS_NE_CORNER(quad-_nx)) {  // Equivalent to BOUNDARY_SW
                    quad -= _nx;
                    edge = Edge_SW;
                    return;
                }
                break;
            case 7:
                if (BOUNDARY_W(quad-_nx)) {
                    quad -= _nx;
                    edge = Edge_W;
                    return;
                }
                break;
            default: assert(0 && "Invalid index"); break;
        }

        if (_corner_mask)
            index = (index + 1) % 8;
        else
            index = (index + 2) % 8;
    } while (index != start_index);

    assert(0 && "Failed to find next boundary edge");
}

void Mpl2014ContourGenerator::move_to_next_quad(QuadEdge& quad_edge) const
{
    assert(quad_edge.quad >= 0 && quad_edge.quad < _n && "Quad index out of bounds");
    assert(quad_edge.edge != Edge_None && "Invalid edge");

    // Move from quad_edge.quad to the neighbouring quad in the direction
    // specified by quad_edge.edge.
    switch (quad_edge.edge) {
        case Edge_E: quad_edge.quad += 1;   quad_edge.edge = Edge_W; break;
        case Edge_N: quad_edge.quad += _nx; quad_edge.edge = Edge_S; break;
        case Edge_W: quad_edge.quad -= 1;   quad_edge.edge = Edge_E; break;
        case Edge_S: quad_edge.quad -= _nx; quad_edge.edge = Edge_N; break;
        default: assert(0 && "Invalid edge"); break;
    }
}

void Mpl2014ContourGenerator::single_quad_filled(
    Contour& contour, index_t quad, const double& lower_level, const double& upper_level)
{
    assert(quad >= 0 && quad < _n && "Quad index out of bounds");

    // Order of checking is important here as can have different ContourLines
    // from both lower and upper levels in the same quad.  First check the S
    // edge, then move up the quad to the N edge checking as required.

    // Possible starts from S boundary.
    if (BOUNDARY_S(quad) && EXISTS_S_EDGE(quad)) {

        // Lower-level start from S boundary into interior.
        if (!VISITED_S(quad) && Z_SW >= 1 && Z_SE == 0)
            contour.push_back(start_filled(
                quad, Edge_S, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from S boundary into interior.
        if (!VISITED_S(quad) && Z_SW < 2 && Z_SE == 2)
            contour.push_back(start_filled(
                quad, Edge_S, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start following S boundary from W to E.
        if (!VISITED_S(quad) && Z_SW <= 1 && Z_SE == 1)
            contour.push_back(start_filled(
                quad, Edge_S, 1, NotHole, Boundary, lower_level, upper_level));

        // Upper-level start following S boundary from W to E.
        if (!VISITED_S(quad) && Z_SW == 2 && Z_SE == 1)
            contour.push_back(start_filled(
                quad, Edge_S, 2, NotHole, Boundary, lower_level, upper_level));
    }

    // Possible starts from W boundary.
    if (BOUNDARY_W(quad) && EXISTS_W_EDGE(quad)) {

        // Lower-level start from W boundary into interior.
        if (!VISITED_W(quad) && Z_NW >= 1 && Z_SW == 0)
            contour.push_back(start_filled(
                quad, Edge_W, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from W boundary into interior.
        if (!VISITED_W(quad) && Z_NW < 2 && Z_SW == 2)
            contour.push_back(start_filled(
                quad, Edge_W, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start following W boundary from N to S.
        if (!VISITED_W(quad) && Z_NW <= 1 && Z_SW == 1)
            contour.push_back(start_filled(
                quad, Edge_W, 1, NotHole, Boundary, lower_level, upper_level));

        // Upper-level start following W boundary from N to S.
        if (!VISITED_W(quad) && Z_NW == 2 && Z_SW == 1)
            contour.push_back(start_filled(
                quad, Edge_W, 2, NotHole, Boundary, lower_level, upper_level));
    }

    // Possible starts from NE boundary.
    if (EXISTS_SW_CORNER(quad)) {  // i.e. BOUNDARY_NE

        // Lower-level start following NE boundary from SE to NW, hole.
        if (!VISITED_CORNER(quad) && Z_NW == 1 && Z_SE == 1)
            contour.push_back(start_filled(
                quad, Edge_NE, 1, Hole, Boundary, lower_level, upper_level));
    }
    // Possible starts from SE boundary.
    else if (EXISTS_NW_CORNER(quad)) {  // i.e. BOUNDARY_SE

        // Lower-level start from N to SE.
        if (!VISITED(quad,1) && Z_NW == 0 && Z_SW == 0 && Z_NE >= 1)
            contour.push_back(start_filled(
                quad, Edge_N, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from SE to N, hole.
        if (!VISITED(quad,2) && Z_NW <  2 && Z_SW < 2 && Z_NE == 2)
            contour.push_back(start_filled(
                quad, Edge_SE, 2, Hole, Interior, lower_level, upper_level));

        // Upper-level start from N to SE.
        if (!VISITED(quad,2) && Z_NW == 2 && Z_SW == 2 && Z_NE < 2)
            contour.push_back(start_filled(
                quad, Edge_N, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start from SE to N, hole.
        if (!VISITED(quad,1) && Z_NW >= 1 && Z_SW >= 1 && Z_NE == 0)
            contour.push_back(start_filled(
                quad, Edge_SE, 1, Hole, Interior, lower_level, upper_level));
    }
    // Possible starts from NW boundary.
    else if (EXISTS_SE_CORNER(quad)) {  // i.e. BOUNDARY_NW

        // Lower-level start from NW to E.
        if (!VISITED(quad,1) && Z_SW == 0 && Z_SE == 0 && Z_NE >= 1)
            contour.push_back(start_filled(
                quad, Edge_NW, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from E to NW, hole.
        if (!VISITED(quad,2) && Z_SW < 2 && Z_SE < 2 && Z_NE == 2)
            contour.push_back(start_filled(
                quad, Edge_E, 2, Hole, Interior, lower_level, upper_level));

        // Upper-level start from NW to E.
        if (!VISITED(quad,2) && Z_SW == 2 && Z_SE == 2 && Z_NE < 2)
            contour.push_back(start_filled(
                quad, Edge_NW, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start from E to NW, hole.
        if (!VISITED(quad,1) && Z_SW >= 1 && Z_SE >= 1 && Z_NE == 0)
            contour.push_back(start_filled(
                quad, Edge_E, 1, Hole, Interior, lower_level, upper_level));
    }
    // Possible starts from SW boundary.
    else if (EXISTS_NE_CORNER(quad)) {  // i.e. BOUNDARY_SW

        // Lower-level start from SW boundary into interior.
        if (!VISITED_CORNER(quad) && Z_NW >= 1 && Z_SE == 0)
            contour.push_back(start_filled(
                quad, Edge_SW, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from SW boundary into interior.
        if (!VISITED_CORNER(quad) && Z_NW < 2 && Z_SE == 2)
            contour.push_back(start_filled(
                quad, Edge_SW, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start following SW boundary from NW to SE.
        if (!VISITED_CORNER(quad) && Z_NW <= 1 && Z_SE == 1)
            contour.push_back(start_filled(
                quad, Edge_SW, 1, NotHole, Boundary, lower_level, upper_level));

        // Upper-level start following SW boundary from NW to SE.
        if (!VISITED_CORNER(quad) && Z_NW == 2 && Z_SE == 1)
            contour.push_back(start_filled(
                quad, Edge_SW, 2, NotHole, Boundary, lower_level, upper_level));
    }

    // A full (unmasked) quad can only have a start on the NE corner, i.e. from
    // N to E (lower level) or E to N (upper level).  Any other start will have
    // already been created in a call to this function for a prior quad so we
    // don't need to test for it again here.
    //
    // The situation is complicated by the possibility that the quad is a
    // saddle quad, in which case a contour line starting on the N could leave
    // by either the W or the E.  We only need to consider those leaving E.
    //
    // A NE corner can also have a N to E or E to N start.
    if (EXISTS_QUAD(quad) || EXISTS_NE_CORNER(quad)) {

        // Lower-level start from N to E.
        if (!VISITED(quad,1) && Z_NW == 0 && Z_SE == 0 && Z_NE >= 1 &&
            (!SADDLE(quad,1) || SADDLE_LEFT(quad,1)))
            contour.push_back(start_filled(
                quad, Edge_N, 1, NotHole, Interior, lower_level, upper_level));

        // Upper-level start from E to N, hole.
        if (!VISITED(quad,2) && Z_NW < 2 && Z_SE <  2 && Z_NE == 2 &&
            (!SADDLE(quad,2) || !SADDLE_LEFT(quad,2)))
            contour.push_back(start_filled(
                quad, Edge_E, 2, Hole, Interior, lower_level, upper_level));

        // Upper-level start from N to E.
        if (!VISITED(quad,2) && Z_NW == 2 && Z_SE == 2 && Z_NE < 2 &&
            (!SADDLE(quad,2) || SADDLE_LEFT(quad,2)))
            contour.push_back(start_filled(
                quad, Edge_N, 2, NotHole, Interior, lower_level, upper_level));

        // Lower-level start from E to N, hole.
        if (!VISITED(quad,1) && Z_NW >= 1 && Z_SE >= 1 && Z_NE == 0 &&
            (!SADDLE(quad,1) || !SADDLE_LEFT(quad,1)))
            contour.push_back(start_filled(
                quad, Edge_E, 1, Hole, Interior, lower_level, upper_level));

        // All possible contours passing through the interior of this quad
        // should have already been created, so assert this.
        assert((VISITED(quad,1) || get_start_edge(quad, 1) == Edge_None) &&
               "Found start of contour that should have already been created");
        assert((VISITED(quad,2) || get_start_edge(quad, 2) == Edge_None) &&
               "Found start of contour that should have already been created");
    }

    // Lower-level start following N boundary from E to W, hole.
    // This is required for an internal masked region which is a hole in a
    // surrounding contour line.
    if (BOUNDARY_N(quad) && EXISTS_N_EDGE(quad) &&
        !VISITED_S(quad+_nx) && Z_NW == 1 && Z_NE == 1)
        contour.push_back(start_filled(
            quad, Edge_N, 1, Hole, Boundary, lower_level, upper_level));
}

ContourLine* Mpl2014ContourGenerator::start_filled(
    index_t quad, Edge edge, unsigned int start_level_index, HoleOrNot hole_or_not,
    BoundaryOrInterior boundary_or_interior, const double& lower_level, const double& upper_level)
{
    assert(quad >= 0 && quad < _n && "Quad index out of bounds");
    assert(edge != Edge_None && "Invalid edge");
    assert((start_level_index == 1 || start_level_index == 2) &&
           "start level index must be 1 or 2");

    ContourLine* contour_line = new ContourLine(hole_or_not == Hole);
    if (hole_or_not == Hole) {
        // Find and set parent ContourLine.
        ContourLine* parent = _parent_cache.get_parent(quad + 1);
        assert(parent != 0 && "Failed to find parent ContourLine");
        contour_line->set_parent(parent);
        parent->add_child(contour_line);
    }

    QuadEdge quad_edge(quad, edge);
    const QuadEdge start_quad_edge(quad_edge);
    unsigned int level_index = start_level_index;

    // If starts on interior, can only finish on interior.
    // If starts on boundary, can only finish on boundary.

    while (true) {
        if (boundary_or_interior == Interior) {
            double level = (level_index == 1 ? lower_level : upper_level);
            follow_interior(
                *contour_line, quad_edge, level_index, level, false, &start_quad_edge,
                start_level_index, true);
        }
        else {
            level_index = follow_boundary(
                *contour_line, quad_edge, lower_level, upper_level, level_index, start_quad_edge);
        }

        if (quad_edge == start_quad_edge && (boundary_or_interior == Boundary ||
                                             level_index == start_level_index))
            break;

        if (boundary_or_interior == Boundary)
            boundary_or_interior = Interior;
        else
            boundary_or_interior = Boundary;
    }

    return contour_line;
}

bool Mpl2014ContourGenerator::start_line(
    py::list& vertices_list, py::list& codes_list, index_t quad, Edge edge, const double& level)
{
    assert(is_edge_a_boundary(QuadEdge(quad, edge)) && "QuadEdge is not a boundary");

    QuadEdge quad_edge(quad, edge);
    ContourLine contour_line(false);
    follow_interior(contour_line, quad_edge, 1, level, true, 0, 1, false);

    append_contour_line_to_vertices_and_codes(contour_line, vertices_list, codes_list);

    return VISITED(quad,1);
}

/*void Mpl2014ContourGenerator::write_cache(bool grid_only) const
{
    std::cout << "-----------------------------------------------" << std::endl;
    for (index_t quad = 0; quad < _n; ++quad)
        write_cache_quad(quad, grid_only);
    std::cout << "-----------------------------------------------" << std::endl;
}

void Mpl2014ContourGenerator::write_cache_quad(
    index_t quad, bool grid_only) const
{
    index_t j = quad / _nx;
    index_t i = quad - j*_nx;
    std::cout << quad << ": i=" << i << " j=" << j << " EXISTS=" << EXISTS_QUAD(quad);
    if (_corner_mask)
        std::cout << " CORNER=" << EXISTS_SW_CORNER(quad) << EXISTS_SE_CORNER(quad)
            << EXISTS_NW_CORNER(quad) << EXISTS_NE_CORNER(quad);
    std::cout << " BNDY=" << BOUNDARY_S(quad) << BOUNDARY_W(quad);
    if (!grid_only) {
        std::cout << " Z=" << Z_LEVEL(quad)
            << " SAD=" << SADDLE(quad,1) << SADDLE(quad,2)
            << " LEFT=" << SADDLE_LEFT(quad,1) << SADDLE_LEFT(quad,2)
            << " NW=" << SADDLE_START_SW(quad,1) << SADDLE_START_SW(quad,2)
            << " VIS=" << VISITED(quad,1) << VISITED(quad,2)
            << VISITED_S(quad) << VISITED_W(quad)
            << VISITED_CORNER(quad);
    }
    std::cout << std::endl;
}*/

} // namespace mpl2014
} // namespace contourpy
