// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_COORDINATES_H_
#define CORE_FXCRT_FX_COORDINATES_H_

#include <stdint.h>

#include "core/fxcrt/span.h"

template <class BaseType>
class CFX_PTemplate {
 public:
  constexpr CFX_PTemplate() = default;
  constexpr CFX_PTemplate(BaseType new_x, BaseType new_y)
      : x(new_x), y(new_y) {}
  CFX_PTemplate(const CFX_PTemplate& other) = default;
  CFX_PTemplate& operator=(const CFX_PTemplate& other) = default;

  bool operator==(const CFX_PTemplate& other) const {
    return x == other.x && y == other.y;
  }
  bool operator!=(const CFX_PTemplate& other) const {
    return !(*this == other);
  }
  CFX_PTemplate& operator+=(const CFX_PTemplate<BaseType>& obj) {
    x += obj.x;
    y += obj.y;
    return *this;
  }
  CFX_PTemplate& operator-=(const CFX_PTemplate<BaseType>& obj) {
    x -= obj.x;
    y -= obj.y;
    return *this;
  }
  CFX_PTemplate operator+(const CFX_PTemplate& other) const {
    return CFX_PTemplate(x + other.x, y + other.y);
  }
  CFX_PTemplate operator-(const CFX_PTemplate& other) const {
    return CFX_PTemplate(x - other.x, y - other.y);
  }

  BaseType x = 0;
  BaseType y = 0;
};
using CFX_Point16 = CFX_PTemplate<int16_t>;
using CFX_Point = CFX_PTemplate<int32_t>;
using CFX_PointF = CFX_PTemplate<float>;

template <class BaseType>
class CFX_STemplate {
 public:
  constexpr CFX_STemplate() = default;
  constexpr CFX_STemplate(BaseType new_width, BaseType new_height)
      : width(new_width), height(new_height) {}
  CFX_STemplate(const CFX_STemplate& other) = default;
  CFX_STemplate& operator=(const CFX_STemplate& other) = default;

  template <typename OtherType>
  CFX_STemplate<OtherType> As() const {
    return CFX_STemplate<OtherType>(static_cast<OtherType>(width),
                                    static_cast<OtherType>(height));
  }

  void clear() {
    width = 0;
    height = 0;
  }
  bool operator==(const CFX_STemplate& other) const {
    return width == other.width && height == other.height;
  }
  bool operator!=(const CFX_STemplate& other) const {
    return !(*this == other);
  }
  CFX_STemplate& operator+=(const CFX_STemplate<BaseType>& obj) {
    width += obj.width;
    height += obj.height;
    return *this;
  }
  CFX_STemplate& operator-=(const CFX_STemplate<BaseType>& obj) {
    width -= obj.width;
    height -= obj.height;
    return *this;
  }
  CFX_STemplate& operator*=(BaseType factor) {
    width *= factor;
    height *= factor;
    return *this;
  }
  CFX_STemplate& operator/=(BaseType divisor) {
    width /= divisor;
    height /= divisor;
    return *this;
  }
  CFX_STemplate operator+(const CFX_STemplate& other) const {
    return CFX_STemplate(width + other.width, height + other.height);
  }
  CFX_STemplate operator-(const CFX_STemplate& other) const {
    return CFX_STemplate(width - other.width, height - other.height);
  }
  CFX_STemplate operator*(BaseType factor) const {
    return CFX_STemplate(width * factor, height * factor);
  }
  CFX_STemplate operator/(BaseType divisor) const {
    return CFX_STemplate(width / divisor, height / divisor);
  }

  BaseType width = 0;
  BaseType height = 0;
};
using CFX_Size = CFX_STemplate<int32_t>;
using CFX_SizeF = CFX_STemplate<float>;

template <class BaseType>
class CFX_VTemplate final : public CFX_PTemplate<BaseType> {
 public:
  using CFX_PTemplate<BaseType>::x;
  using CFX_PTemplate<BaseType>::y;

  CFX_VTemplate() : CFX_PTemplate<BaseType>() {}
  CFX_VTemplate(BaseType new_x, BaseType new_y)
      : CFX_PTemplate<BaseType>(new_x, new_y) {}

  CFX_VTemplate(const CFX_VTemplate& other) : CFX_PTemplate<BaseType>(other) {}

  CFX_VTemplate(const CFX_PTemplate<BaseType>& point1,
                const CFX_PTemplate<BaseType>& point2)
      : CFX_PTemplate<BaseType>(point2.x - point1.x, point2.y - point1.y) {}

  float Length() const;
  void Normalize();
};
using CFX_Vector = CFX_VTemplate<int32_t>;
using CFX_VectorF = CFX_VTemplate<float>;

// Rectangles.
// TODO(tsepez): Consolidate all these different rectangle classes.

// LTRB rectangles (y-axis runs downwards).
// Struct layout is compatible with win32 RECT.
struct FX_RECT {
  constexpr FX_RECT() = default;
  constexpr FX_RECT(int l, int t, int r, int b)
      : left(l), top(t), right(r), bottom(b) {}
  FX_RECT(const FX_RECT& that) = default;
  FX_RECT& operator=(const FX_RECT& that) = default;

  int Width() const { return right - left; }
  int Height() const { return bottom - top; }
  bool IsEmpty() const { return right <= left || bottom <= top; }

  bool Valid() const;

  void Normalize();
  void Intersect(const FX_RECT& src);
  void Intersect(int l, int t, int r, int b) { Intersect(FX_RECT(l, t, r, b)); }
  FX_RECT SwappedClipBox(int width, int height, bool bFlipX, bool bFlipY) const;

  void Offset(int dx, int dy) {
    left += dx;
    right += dx;
    top += dy;
    bottom += dy;
  }

  bool operator==(const FX_RECT& src) const {
    return left == src.left && right == src.right && top == src.top &&
           bottom == src.bottom;
  }

  bool Contains(int x, int y) const {
    return x >= left && x < right && y >= top && y < bottom;
  }

  int32_t left = 0;
  int32_t top = 0;
  int32_t right = 0;
  int32_t bottom = 0;
};

// LTRB rectangles (y-axis runs upwards).
class CFX_FloatRect {
 public:
  constexpr CFX_FloatRect() = default;
  constexpr CFX_FloatRect(float l, float b, float r, float t)
      : left(l), bottom(b), right(r), top(t) {}
  CFX_FloatRect(const CFX_FloatRect& that) = default;
  CFX_FloatRect& operator=(const CFX_FloatRect& that) = default;

  explicit CFX_FloatRect(const FX_RECT& rect);
  explicit CFX_FloatRect(const CFX_PointF& point);

  static CFX_FloatRect GetBBox(pdfium::span<const CFX_PointF> pPoints);

  void Normalize();

  bool IsEmpty() const { return left >= right || bottom >= top; }
  bool Contains(const CFX_PointF& point) const;
  bool Contains(const CFX_FloatRect& other_rect) const;

  void Intersect(const CFX_FloatRect& other_rect);
  void Union(const CFX_FloatRect& other_rect);

  // These may be better at rounding than ToFxRect() and friends.
  //
  // Returned rect has bounds rounded up/down such that it is contained in the
  // original.
  FX_RECT GetInnerRect() const;

  // Returned rect has bounds rounded up/down such that the original is
  // contained in it.
  FX_RECT GetOuterRect() const;

  // Returned rect has bounds rounded up/down such that the dimensions are
  // rounded up and the sum of the error in the bounds is minimized.
  FX_RECT GetClosestRect() const;

  CFX_FloatRect GetCenterSquare() const;

  void UpdateRect(const CFX_PointF& point);

  float Width() const { return right - left; }
  float Height() const { return top - bottom; }
  float Left() const { return left; }
  float Bottom() const { return bottom; }
  float Right() const { return right; }
  float Top() const { return top; }

  void Inflate(float x, float y);
  void Inflate(float other_left,
               float other_bottom,
               float other_right,
               float other_top);
  void Inflate(const CFX_FloatRect& rt);

  void Deflate(float x, float y);
  void Deflate(float other_left,
               float other_bottom,
               float other_right,
               float other_top);
  void Deflate(const CFX_FloatRect& rt);

  CFX_FloatRect GetDeflated(float x, float y) const;

  void Translate(float e, float f);

  void Scale(float fScale);
  void ScaleFromCenterPoint(float fScale);

  // GetInnerRect() and friends may be better at rounding than these methods.
  // Unlike the methods above, these two blindly floor / round the LBRT values.
  // Doing so may introduce rounding errors that are visible to users as
  // off-by-one pixels/lines.
  //
  // Floors LBRT values.
  FX_RECT ToFxRect() const;

  // Rounds LBRT values.
  FX_RECT ToRoundedFxRect() const;

  bool operator==(const CFX_FloatRect& other) const {
    return left == other.left && right == other.right && top == other.top &&
           bottom == other.bottom;
  }

  float left = 0.0f;
  float bottom = 0.0f;
  float right = 0.0f;
  float top = 0.0f;
};

// LTWH rectangles (y-axis runs downwards).
class CFX_RectF {
 public:
  using PointType = CFX_PointF;
  using SizeType = CFX_SizeF;

  constexpr CFX_RectF() = default;
  constexpr CFX_RectF(float dst_left,
                      float dst_top,
                      float dst_width,
                      float dst_height)
      : left(dst_left), top(dst_top), width(dst_width), height(dst_height) {}
  CFX_RectF(const CFX_RectF& other) = default;
  CFX_RectF& operator=(const CFX_RectF& other) = default;

  CFX_RectF(float dst_left, float dst_top, const SizeType& dst_size)
      : left(dst_left),
        top(dst_top),
        width(dst_size.width),
        height(dst_size.height) {}
  CFX_RectF(const PointType& p, float dst_width, float dst_height)
      : left(p.x), top(p.y), width(dst_width), height(dst_height) {}
  CFX_RectF(const PointType& p1, const SizeType& s2)
      : left(p1.x), top(p1.y), width(s2.width), height(s2.height) {}
  explicit CFX_RectF(const FX_RECT& that)
      : left(static_cast<float>(that.left)),
        top(static_cast<float>(that.top)),
        width(static_cast<float>(that.Width())),
        height(static_cast<float>(that.Height())) {}

  CFX_RectF& operator+=(const PointType& p) {
    left += p.x;
    top += p.y;
    return *this;
  }
  CFX_RectF& operator-=(const PointType& p) {
    left -= p.x;
    top -= p.y;
    return *this;
  }
  float right() const { return left + width; }
  float bottom() const { return top + height; }
  void Normalize() {
    if (width < 0) {
      left += width;
      width = -width;
    }
    if (height < 0) {
      top += height;
      height = -height;
    }
  }
  void Offset(float dx, float dy) {
    left += dx;
    top += dy;
  }
  void Inflate(float x, float y) {
    left -= x;
    width += x * 2;
    top -= y;
    height += y * 2;
  }
  void Inflate(const PointType& p) { Inflate(p.x, p.y); }
  void Inflate(float off_left,
               float off_top,
               float off_right,
               float off_bottom) {
    left -= off_left;
    top -= off_top;
    width += off_left + off_right;
    height += off_top + off_bottom;
  }
  void Inflate(const CFX_RectF& rt) {
    Inflate(rt.left, rt.top, rt.left + rt.width, rt.top + rt.height);
  }
  void Deflate(float x, float y) {
    left += x;
    width -= x * 2;
    top += y;
    height -= y * 2;
  }
  void Deflate(const PointType& p) { Deflate(p.x, p.y); }
  void Deflate(float off_left,
               float off_top,
               float off_right,
               float off_bottom) {
    left += off_left;
    top += off_top;
    width -= off_left + off_right;
    height -= off_top + off_bottom;
  }
  void Deflate(const CFX_RectF& rt) {
    Deflate(rt.left, rt.top, rt.top + rt.width, rt.top + rt.height);
  }
  bool IsEmpty() const { return width <= 0 || height <= 0; }
  bool IsEmpty(float fEpsilon) const {
    return width <= fEpsilon || height <= fEpsilon;
  }
  void Empty() { width = height = 0; }
  bool Contains(const PointType& p) const {
    return p.x >= left && p.x < left + width && p.y >= top &&
           p.y < top + height;
  }
  bool Contains(const CFX_RectF& rt) const {
    return rt.left >= left && rt.right() <= right() && rt.top >= top &&
           rt.bottom() <= bottom();
  }
  float Left() const { return left; }
  float Top() const { return top; }
  float Width() const { return width; }
  float Height() const { return height; }
  SizeType Size() const { return SizeType(width, height); }
  PointType TopLeft() const { return PointType(left, top); }
  PointType TopRight() const { return PointType(left + width, top); }
  PointType BottomLeft() const { return PointType(left, top + height); }
  PointType BottomRight() const {
    return PointType(left + width, top + height);
  }
  PointType Center() const {
    return PointType(left + width / 2, top + height / 2);
  }
  void Union(float x, float y);
  void Union(const PointType& p) { Union(p.x, p.y); }
  void Union(const CFX_RectF& rt);
  void Intersect(const CFX_RectF& rt);
  bool IntersectWith(const CFX_RectF& rt) const {
    CFX_RectF rect = rt;
    rect.Intersect(*this);
    return !rect.IsEmpty();
  }
  bool IntersectWith(const CFX_RectF& rt, float fEpsilon) const {
    CFX_RectF rect = rt;
    rect.Intersect(*this);
    return !rect.IsEmpty(fEpsilon);
  }
  friend bool operator==(const CFX_RectF& rc1, const CFX_RectF& rc2) {
    return rc1.left == rc2.left && rc1.top == rc2.top &&
           rc1.width == rc2.width && rc1.height == rc2.height;
  }
  friend bool operator!=(const CFX_RectF& rc1, const CFX_RectF& rc2) {
    return !(rc1 == rc2);
  }

  CFX_FloatRect ToFloatRect() const {
    // Note, we flip top/bottom here because the CFX_FloatRect has the
    // y-axis running in the opposite direction.
    return CFX_FloatRect(left, top, right(), bottom());
  }

  // Returned rect has bounds rounded up/down such that the original is
  // contained in it.
  FX_RECT GetOuterRect() const;

  float left = 0.0f;
  float top = 0.0f;
  float width = 0.0f;
  float height = 0.0f;
};

// The matrix is of the form:
// | a  b  0 |
// | c  d  0 |
// | e  f  1 |
// See PDF spec 1.7 Section 4.2.3.
//
class CFX_Matrix {
 public:
  constexpr CFX_Matrix() = default;

  constexpr CFX_Matrix(float a1,
                       float b1,
                       float c1,
                       float d1,
                       float e1,
                       float f1)
      : a(a1), b(b1), c(c1), d(d1), e(e1), f(f1) {}

  CFX_Matrix(const CFX_Matrix& other) = default;

  CFX_Matrix& operator=(const CFX_Matrix& other) = default;

  bool operator==(const CFX_Matrix& other) const {
    return a == other.a && b == other.b && c == other.c && d == other.d &&
           e == other.e && f == other.f;
  }
  bool operator!=(const CFX_Matrix& other) const { return !(*this == other); }

  CFX_Matrix operator*(const CFX_Matrix& right) const {
    return CFX_Matrix(a * right.a + b * right.c, a * right.b + b * right.d,
                      c * right.a + d * right.c, c * right.b + d * right.d,
                      e * right.a + f * right.c + right.e,
                      e * right.b + f * right.d + right.f);
  }
  CFX_Matrix& operator*=(const CFX_Matrix& other) {
    *this = *this * other;
    return *this;
  }

  bool IsIdentity() const { return *this == CFX_Matrix(); }
  CFX_Matrix GetInverse() const;

  bool Is90Rotated() const;
  bool IsScaled() const;
  bool WillScale() const { return a != 1.0f || b != 0 || c != 0 || d != 1.0f; }

  void Concat(const CFX_Matrix& right) { *this *= right; }
  void Translate(float x, float y);
  void TranslatePrepend(float x, float y);
  void Translate(int32_t x, int32_t y) {
    Translate(static_cast<float>(x), static_cast<float>(y));
  }
  void TranslatePrepend(int32_t x, int32_t y) {
    TranslatePrepend(static_cast<float>(x), static_cast<float>(y));
  }

  void Scale(float sx, float sy);
  void Rotate(float fRadian);

  void MatchRect(const CFX_FloatRect& dest, const CFX_FloatRect& src);

  float GetXUnit() const;
  float GetYUnit() const;
  CFX_FloatRect GetUnitRect() const;

  float TransformXDistance(float dx) const;
  float TransformDistance(float distance) const;

  CFX_PointF Transform(const CFX_PointF& point) const;

  CFX_RectF TransformRect(const CFX_RectF& rect) const;
  CFX_FloatRect TransformRect(const CFX_FloatRect& rect) const;

  float a = 1.0f;
  float b = 0.0f;
  float c = 0.0f;
  float d = 1.0f;
  float e = 0.0f;
  float f = 0.0f;
};

#endif  // CORE_FXCRT_FX_COORDINATES_H_
