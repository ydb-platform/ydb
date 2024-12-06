#include <cmath>
#include <array>
#include <numeric>

#include "erfinv.h"

template <size_t N>
static double polEval(double x, const std::array<double, N>& coef) {
    static_assert(N > 0, "Array coef[] should not be empty.");
    return std::accumulate(coef.crbegin() + 1, coef.crend(), coef[N - 1],
                           [x] (auto init, auto cur) {
                            return std::move(init) * x + cur;
                           });
}

namespace NMathUdf {

// https://www.jstor.org/stable/2347330
double ErfInv(double x) {
    static constexpr std::array<double, 8> a = {
        1.1975323115670912564578e0,
        4.7072688112383978012285e1,
        6.9706266534389598238465e2,
        4.8548868893843886794648e3,
        1.6235862515167575384252e4,
        2.3782041382114385731252e4,
        1.1819493347062294404278e4,
        8.8709406962545514830200e2,
    };
    static constexpr std::array<double, 8> b = {
        1.,
        4.2313330701600911252e1,
        6.8718700749205790830e2,
        5.3941960214247511077e3,
        2.1213794301586595867e4,
        3.9307895800092710610e4,
        2.8729085735721942674e4,
        5.2264952788528545610e3,
    };
    static constexpr std::array<double, 8> c = {
        1.42343711074968357734e0,
        4.63033784615654529590e0,
        5.76949722146069140550e0,
        3.64784832476320460504e0,
        1.27045825245236838258e0,
        2.41780725177450611770e-1,
        2.27238449892691845833e-2,
        7.74545014278341407640e-4,
    };
    static constexpr std::array<double, 8> d = {
        1.4142135623730950488016887e0,
        2.9036514445419946173133295e0,
        2.3707661626024532365971225e0,
        9.7547832001787427186894837e-1,
        2.0945065210512749128288442e-1,
        2.1494160384252876777097297e-2,
        7.7441459065157709165577218e-4,
        1.4859850019840355905497876e-9,
    };
    static constexpr std::array<double, 8> e = {
        6.65790464350110377720e0,
        5.46378491116411436990e0,
        1.78482653991729133580e0,
        2.96560571828504891230e-1,
        2.65321895265761230930e-2,
        1.24266094738807843860e-3,
        2.71155556874348757815e-5,
        2.01033439929228813265e-7,
    };
    static constexpr std::array<double, 8> f = {
        1.414213562373095048801689e0,
        8.482908416595164588112026e-1,
        1.936480946950659106176712e-1,
        2.103693768272068968719679e-2,
        1.112800997078859844711555e-3,
        2.611088405080593625138020e-5,
        2.010321207683943062279931e-7,
        2.891024605872965461538222e-15,
    };

    if (isnan(x) || x <= -1. || x >= 1.) {
        if (x == 1.) {
            return std::numeric_limits<double>::infinity();
        }
        if (x == -1.) {
            return -std::numeric_limits<double>::infinity();
        }
        return std::numeric_limits<double>::quiet_NaN();
    }

    double sign = (x > 0) - (x < 0);
    x = abs(x);
    if (x < 1e-7) {
        return sign * x / M_2_SQRTPI;
    }

    double ans;
    if (x <= 0.85) {
        double r = 0.180625 - 0.25 * x * x;
        ans = x * polEval(r, a) / polEval(r, b);
    } else {
        double r = std::sqrt(M_LN2 - log(1. - x)) - 1.6;
        if (r <= 3.4) {
            ans = polEval(r, c) / polEval(r, d);
        } else {
            r -= 3.4;
            ans = polEval(r, e) / polEval(r, f);
        }
    }

    return ans * sign;
}

}
