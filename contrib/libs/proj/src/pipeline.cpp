/*******************************************************************************

                       Transformation pipeline manager

                    Thomas Knudsen, 2016-05-20/2016-11-20

********************************************************************************

    Geodetic transformations are typically organized in a number of
    steps. For example, a datum shift could be carried out through
    these steps:

    1. Convert (latitude, longitude, ellipsoidal height) to
       3D geocentric cartesian coordinates (X, Y, Z)
    2. Transform the (X, Y, Z) coordinates to the new datum, using a
       7 parameter Helmert transformation.
    3. Convert (X, Y, Z) back to (latitude, longitude, ellipsoidal height)

    If the height system used is orthometric, rather than ellipsoidal,
    another step is needed at each end of the process:

    1. Add the local geoid undulation (N) to the orthometric height
       to obtain the ellipsoidal (i.e. geometric) height.
    2. Convert (latitude, longitude, ellipsoidal height) to
       3D geocentric cartesian coordinates (X, Y, Z)
    3. Transform the (X, Y, Z) coordinates to the new datum, using a
       7 parameter Helmert transformation.
    4. Convert (X, Y, Z) back to (latitude, longitude, ellipsoidal height)
    5. Subtract the local geoid undulation (N) from the ellipsoidal height
       to obtain the orthometric height.

    Additional steps can be added for e.g. change of vertical datum, so the
    list can grow fairly long. None of the steps are, however, particularly
    complex, and data flow is strictly from top to bottom.

    Hence, in principle, the first example above could be implemented using
    Unix pipelines:

    cat my_coordinates | geographic_to_xyz | helmert | xyz_to_geographic >
my_transformed_coordinates

    in the grand tradition of Software Tools [1].

    The proj pipeline driver implements a similar concept: Stringing together
    a number of steps, feeding the output of one step to the input of the next.

    It is a very powerful concept, that increases the range of relevance of the
    proj.4 system substantially. It is, however, not a particularly intrusive
    addition to the PROJ.4 code base: The implementation is by and large
completed by adding an extra projection called "pipeline" (i.e. this file),
which handles all business, and a small amount of added functionality in the
    pj_init code, implementing support for multilevel, embedded pipelines.

    Syntactically, the pipeline system introduces the "+step" keyword (which
    indicates the start of each transformation step), and reintroduces the +inv
    keyword (indicating that a given transformation step should run in reverse,
i.e. forward, when the pipeline is executed in inverse direction, and vice
versa).

    Hence, the first transformation example above, can be implemented as:

    +proj=pipeline +step proj=cart +step proj=helmert <ARGS> +step proj=cart
+inv

    Where <ARGS> indicate the Helmert arguments: 3 translations (+x=..., +y=...,
    +z=...), 3 rotations (+rx=..., +ry=..., +rz=...) and a scale factor
(+s=...). Following geodetic conventions, the rotations are given in arcseconds,
    and the scale factor is given as parts-per-million.

    [1] B. W. Kernighan & P. J. Plauger: Software tools.
        Reading, Massachusetts, Addison-Wesley, 1976, 338 pp.

********************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2016-05-20

********************************************************************************
* Copyright (c) 2016, 2017, 2018 Thomas Knudsen / SDFE
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
*
********************************************************************************/

#include <math.h>
#include <stack>
#include <stddef.h>
#include <string.h>
#include <vector>

#include "geodesic.h"
#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(pipeline, "Transformation pipeline manager");
PROJ_HEAD(pop, "Retrieve coordinate value from pipeline stack");
PROJ_HEAD(push, "Save coordinate value on pipeline stack");

/* Projection specific elements for the PJ object */
namespace { // anonymous namespace

struct Step {
    PJ *pj = nullptr;
    bool omit_fwd = false;
    bool omit_inv = false;

    Step(PJ *pjIn, bool omitFwdIn, bool omitInvIn)
        : pj(pjIn), omit_fwd(omitFwdIn), omit_inv(omitInvIn) {}
    Step(Step &&other)
        : pj(std::move(other.pj)), omit_fwd(other.omit_fwd),
          omit_inv(other.omit_inv) {
        other.pj = nullptr;
    }
    Step(const Step &) = delete;
    Step &operator=(const Step &) = delete;

    ~Step() { proj_destroy(pj); }
};

struct Pipeline {
    char **argv = nullptr;
    char **current_argv = nullptr;
    std::vector<Step> steps{};
    std::stack<double> stack[4];
};

struct PushPop {
    bool v1;
    bool v2;
    bool v3;
    bool v4;
};
} // anonymous namespace

static void pipeline_forward_4d(PJ_COORD &point, PJ *P);
static void pipeline_reverse_4d(PJ_COORD &point, PJ *P);
static PJ_XYZ pipeline_forward_3d(PJ_LPZ lpz, PJ *P);
static PJ_LPZ pipeline_reverse_3d(PJ_XYZ xyz, PJ *P);
static PJ_XY pipeline_forward(PJ_LP lp, PJ *P);
static PJ_LP pipeline_reverse(PJ_XY xy, PJ *P);

static void pipeline_reassign_context(PJ *P, PJ_CONTEXT *ctx) {
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto &step : pipeline->steps)
        proj_assign_context(step.pj, ctx);
}

static void pipeline_forward_4d(PJ_COORD &point, PJ *P) {
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto &step : pipeline->steps) {
        if (!step.omit_fwd) {
            if (!step.pj->inverted)
                pj_fwd4d(point, step.pj);
            else
                pj_inv4d(point, step.pj);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }
}

static void pipeline_reverse_4d(PJ_COORD &point, PJ *P) {
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto iterStep = pipeline->steps.rbegin();
         iterStep != pipeline->steps.rend(); ++iterStep) {
        const auto &step = *iterStep;
        if (!step.omit_inv) {
            if (step.pj->inverted)
                pj_fwd4d(point, step.pj);
            else
                pj_inv4d(point, step.pj);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }
}

static PJ_XYZ pipeline_forward_3d(PJ_LPZ lpz, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};
    point.lpz = lpz;
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto &step : pipeline->steps) {
        if (!step.omit_fwd) {
            point = pj_approx_3D_trans(step.pj, PJ_FWD, point);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }

    return point.xyz;
}

static PJ_LPZ pipeline_reverse_3d(PJ_XYZ xyz, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};
    point.xyz = xyz;
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto iterStep = pipeline->steps.rbegin();
         iterStep != pipeline->steps.rend(); ++iterStep) {
        const auto &step = *iterStep;
        if (!step.omit_inv) {
            point = proj_trans(step.pj, PJ_INV, point);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }

    return point.lpz;
}

static PJ_XY pipeline_forward(PJ_LP lp, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};
    point.lp = lp;
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto &step : pipeline->steps) {
        if (!step.omit_fwd) {
            point = pj_approx_2D_trans(step.pj, PJ_FWD, point);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }

    return point.xy;
}

static PJ_LP pipeline_reverse(PJ_XY xy, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};
    point.xy = xy;
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    for (auto iterStep = pipeline->steps.rbegin();
         iterStep != pipeline->steps.rend(); ++iterStep) {
        const auto &step = *iterStep;
        if (!step.omit_inv) {
            point = pj_approx_2D_trans(step.pj, PJ_INV, point);
            if (point.xyzt.x == HUGE_VAL) {
                break;
            }
        }
    }

    return point.lp;
}

static PJ *destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    auto pipeline = static_cast<struct Pipeline *>(P->opaque);

    free(pipeline->argv);
    free(pipeline->current_argv);

    delete pipeline;
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

/* count the number of args in pipeline definition, and mark all args as used */
static size_t argc_params(paralist *params) {
    size_t argc = 0;
    for (; params != nullptr; params = params->next) {
        argc++;
        params->used = 1;
    }
    return ++argc; /* one extra for the sentinel */
}

/* Sentinel for argument list */
static const char *argv_sentinel = "step";

/* turn paralist into argc/argv style argument list */
static char **argv_params(paralist *params, size_t argc) {
    char **argv;
    size_t i = 0;
    argv = static_cast<char **>(calloc(argc, sizeof(char *)));
    if (nullptr == argv)
        return nullptr;
    for (; params != nullptr; params = params->next)
        argv[i++] = params->param;
    argv[i++] = const_cast<char *>(argv_sentinel);
    return argv;
}

/* Being the special operator that the pipeline is, we have to handle the    */
/* ellipsoid differently than usual. In general, the pipeline operation does */
/* not need an ellipsoid, but in some cases it is beneficial nonetheless.    */
/* Unfortunately we can't use the normal ellipsoid setter in pj_init, since  */
/* it adds a +ellps parameter to the global args if nothing else is specified*/
/* This is problematic since that ellipsoid spec is then passed on to the    */
/* pipeline children. This is rarely what we want, so here we implement our  */
/* own logic instead. If an ellipsoid is set in the global args, it is used  */
/* as the pipeline ellipsoid. Otherwise we use GRS80 parameters as default.  */
/* At last we calculate the rest of the ellipsoid parameters and             */
/* re-initialize P->geod.                                                    */
static void set_ellipsoid(PJ *P) {
    paralist *cur, *attachment;
    int err = proj_errno_reset(P);

    /* Break the linked list after the global args */
    attachment = nullptr;
    for (cur = P->params; cur != nullptr; cur = cur->next)
        /* cur->next will always be non 0 given argv_sentinel presence, */
        /* but this is far from being obvious for a static analyzer */
        if (cur->next != nullptr &&
            strcmp(argv_sentinel, cur->next->param) == 0) {
            attachment = cur->next;
            cur->next = nullptr;
            break;
        }

    /* Check if there's any ellipsoid specification in the global params. */
    /* If not, use GRS80 as default                                       */
    if (0 != pj_ellipsoid(P)) {
        P->a = 6378137.0;
        P->f = 1.0 / 298.257222101;
        P->es = 2 * P->f - P->f * P->f;

        /* reset an "unerror": In this special use case, the errno is    */
        /* not an error signal, but just a reply from pj_ellipsoid,      */
        /* telling us that "No - there was no ellipsoid definition in    */
        /* the PJ you provided".                                         */
        proj_errno_reset(P);
    }
    P->a_orig = P->a;
    P->es_orig = P->es;

    if (pj_calc_ellipsoid_params(P, P->a, P->es) == 0)
        geod_init(P->geod, P->a, P->f);

    /* Re-attach the dangling list */
    /* Note: cur will always be non 0 given argv_sentinel presence, */
    /* but this is far from being obvious for a static analyzer */
    if (cur != nullptr)
        cur->next = attachment;
    proj_errno_restore(P, err);
}

PJ *OPERATION(pipeline, 0) {
    int i, nsteps = 0, argc;
    int i_pipeline = -1, i_first_step = -1, i_current_step;
    char **argv, **current_argv;

    if (P->ctx->pipelineInitRecursiongCounter == 5) {
        // Can happen for a string like:
        // proj=pipeline step "x="""," u=" proj=pipeline step ste=""[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline step ste="[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline step ste="[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline step ste="[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline p step ste="[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline step ste="[" u="
        // proj=pipeline step ste="[" u=" proj=pipeline step ""x="""""""""""
        // Probably an issue with the quoting handling code
        // But doesn't hurt to add an extra safety check
        proj_log_error(P, _("Pipeline: too deep recursion"));
        return destructor(
            P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX); /* ERROR: nested pipelines */
    }

    P->fwd4d = pipeline_forward_4d;
    P->inv4d = pipeline_reverse_4d;
    P->fwd3d = pipeline_forward_3d;
    P->inv3d = pipeline_reverse_3d;
    P->fwd = pipeline_forward;
    P->inv = pipeline_reverse;
    P->destructor = destructor;
    P->reassign_context = pipeline_reassign_context;

    /* Currently, the pipeline driver is a raw bit mover, enabling other
     * operations */
    /* to collaborate efficiently. All prep/fin stuff is done at the step
     * levels.
     */
    P->skip_fwd_prepare = 1;
    P->skip_fwd_finalize = 1;
    P->skip_inv_prepare = 1;
    P->skip_inv_finalize = 1;

    P->opaque = new (std::nothrow) Pipeline();
    if (nullptr == P->opaque)
        return destructor(P, PROJ_ERR_INVALID_OP /* ENOMEM */);

    argc = (int)argc_params(P->params);
    auto pipeline = static_cast<struct Pipeline *>(P->opaque);
    pipeline->argv = argv = argv_params(P->params, argc);
    if (nullptr == argv)
        return destructor(P, PROJ_ERR_INVALID_OP /* ENOMEM */);

    pipeline->current_argv = current_argv =
        static_cast<char **>(calloc(argc, sizeof(char *)));
    if (nullptr == current_argv)
        return destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    /* Do some syntactical sanity checking */
    for (i = 0; i < argc && argv[i] != nullptr; i++) {
        if (0 == strcmp(argv_sentinel, argv[i])) {
            if (-1 == i_pipeline) {
                proj_log_error(P, _("Pipeline: +step before +proj=pipeline"));
                return destructor(P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
            }
            if (0 == nsteps)
                i_first_step = i;
            nsteps++;
            continue;
        }

        if (0 == strcmp("proj=pipeline", argv[i])) {
            if (-1 != i_pipeline) {
                proj_log_error(P, _("Pipeline: Nesting only allowed when child "
                                    "pipelines are wrapped in '+init's"));
                return destructor(
                    P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX); /* ERROR: nested
                                                             pipelines */
            }
            i_pipeline = i;
        } else if (0 == nsteps && 0 == strncmp(argv[i], "proj=", 5)) {
            // Non-sensical to have proj= in the general pipeline parameters.
            // Would not be a big issue in itself, but this makes bad
            // performance in parsing hostile pipelines more likely, such as the
            // one of
            // https://bugs.chromium.org/p/oss-fuzz/issues/detail?id=41290
            proj_log_error(
                P, _("Pipeline: proj= operator before first step not allowed"));
            return destructor(P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        } else if (0 == nsteps && 0 == strncmp(argv[i], "o_proj=", 7)) {
            // Same as above.
            proj_log_error(
                P,
                _("Pipeline: o_proj= operator before first step not allowed"));
            return destructor(P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        }
    }
    nsteps--; /* Last instance of +step is just a sentinel */

    if (-1 == i_pipeline)
        return destructor(
            P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX); /* ERROR: no pipeline def */

    if (0 == nsteps)
        return destructor(
            P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX); /* ERROR: no pipeline def */

    set_ellipsoid(P);

    /* Now loop over all steps, building a new set of arguments for each init */
    i_current_step = i_first_step;
    for (i = 0; i < nsteps; i++) {
        int j;
        int current_argc = 0;
        int err;
        PJ *next_step = nullptr;

        /* Build a set of setup args for the current step */
        proj_log_trace(P, "Pipeline: Building arg list for step no. %d", i);

        /* First add the step specific args */
        for (j = i_current_step + 1; 0 != strcmp("step", argv[j]); j++)
            current_argv[current_argc++] = argv[j];

        i_current_step = j;

        /* Then add the global args */
        for (j = i_pipeline + 1; 0 != strcmp("step", argv[j]); j++)
            current_argv[current_argc++] = argv[j];

        proj_log_trace(P, "Pipeline: init - %s, %d", current_argv[0],
                       current_argc);
        for (j = 1; j < current_argc; j++)
            proj_log_trace(P, "    %s", current_argv[j]);

        err = proj_errno_reset(P);

        P->ctx->pipelineInitRecursiongCounter++;
        next_step = pj_create_argv_internal(P->ctx, current_argc, current_argv);
        P->ctx->pipelineInitRecursiongCounter--;
        proj_log_trace(P, "Pipeline: Step %d (%s) at %p", i, current_argv[0],
                       next_step);

        if (nullptr == next_step) {
            /* The step init failed, but possibly without setting errno. If so,
             * we say "malformed" */
            int err_to_report = proj_errno(P);
            if (0 == err_to_report)
                err_to_report = PROJ_ERR_INVALID_OP_WRONG_SYNTAX;
            proj_log_error(P, _("Pipeline: Bad step definition: %s (%s)"),
                           current_argv[0],
                           proj_context_errno_string(P->ctx, err_to_report));
            return destructor(P, err_to_report); /* ERROR: bad pipeline def */
        }
        next_step->parent = P;

        proj_errno_restore(P, err);

        /* Is this step inverted? */
        for (j = 0; j < current_argc; j++) {
            if (0 == strcmp("inv", current_argv[j])) {
                /* if +inv exists in both global and local args the forward
                 * operation should be used */
                next_step->inverted = next_step->inverted == 0 ? 1 : 0;
            }
        }

        bool omit_fwd = pj_param(P->ctx, next_step->params, "bomit_fwd").i != 0;
        bool omit_inv = pj_param(P->ctx, next_step->params, "bomit_inv").i != 0;
        pipeline->steps.emplace_back(next_step, omit_fwd, omit_inv);

        proj_log_trace(P, "Pipeline at [%p]:    step at [%p] (%s) done", P,
                       next_step, current_argv[0]);
    }

    /* Require a forward path through the pipeline */
    for (auto &step : pipeline->steps) {
        PJ *Q = step.pj;
        if (step.omit_fwd) {
            continue;
        }
        if (Q->inverted) {
            if (Q->inv || Q->inv3d || Q->inv4d) {
                continue;
            }
            proj_log_error(
                P, _("Pipeline: Inverse operation for %s is not available"),
                Q->short_name);
            return destructor(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        } else {
            if (Q->fwd || Q->fwd3d || Q->fwd4d) {
                continue;
            }
            proj_log_error(
                P, _("Pipeline: Forward operation for %s is not available"),
                Q->short_name);
            return destructor(P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        }
    }

    /* determine if an inverse operation is possible */
    for (auto &step : pipeline->steps) {
        PJ *Q = step.pj;
        if (step.omit_inv || pj_has_inverse(Q)) {
            continue;
        } else {
            P->inv = nullptr;
            P->inv3d = nullptr;
            P->inv4d = nullptr;
            break;
        }
    }

    /* Replace PJ_IO_UNITS_WHATEVER with input/output units of neighbouring
     * steps where */
    /* it make sense. It does in most cases but not always, for instance */
    /*      proj=pipeline step proj=unitconvert xy_in=deg xy_out=rad step ... */
    /* where the left-hand side units of the first step shouldn't be changed to
     * RADIANS */
    /* as it will result in deg->rad conversions in cs2cs and other
     * applications.
     */

    for (i = nsteps - 2; i >= 0; --i) {
        auto pj = pipeline->steps[i].pj;
        if (pj_left(pj) == PJ_IO_UNITS_WHATEVER &&
            pj_right(pj) == PJ_IO_UNITS_WHATEVER) {
            const auto right_pj = pipeline->steps[i + 1].pj;
            const auto right_pj_left = pj_left(right_pj);
            const auto right_pj_right = pj_right(right_pj);
            if (right_pj_left != right_pj_right ||
                right_pj_left != PJ_IO_UNITS_WHATEVER) {
                pj->left = right_pj_left;
                pj->right = right_pj_left;
            }
        }
    }

    for (i = 1; i < nsteps; i++) {
        auto pj = pipeline->steps[i].pj;
        if (pj_left(pj) == PJ_IO_UNITS_WHATEVER &&
            pj_right(pj) == PJ_IO_UNITS_WHATEVER) {
            const auto left_pj = pipeline->steps[i - 1].pj;
            const auto left_pj_left = pj_left(left_pj);
            const auto left_pj_right = pj_right(left_pj);
            if (left_pj_left != left_pj_right ||
                left_pj_right != PJ_IO_UNITS_WHATEVER) {
                pj->left = left_pj_right;
                pj->right = left_pj_right;
            }
        }
    }

    /* Check that units between each steps match each other, fail if they don't
     */
    for (i = 0; i + 1 < nsteps; i++) {
        enum pj_io_units curr_step_output = pj_right(pipeline->steps[i].pj);
        enum pj_io_units next_step_input = pj_left(pipeline->steps[i + 1].pj);

        if (curr_step_output == PJ_IO_UNITS_WHATEVER ||
            next_step_input == PJ_IO_UNITS_WHATEVER)
            continue;

        if (curr_step_output != next_step_input) {
            proj_log_error(
                P, _("Pipeline: Mismatched units between step %d and %d"),
                i + 1, i + 2);
            return destructor(P, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        }
    }

    proj_log_trace(
        P, "Pipeline: %d steps built. Determining i/o characteristics", nsteps);

    /* Determine forward input (= reverse output) data type */
    P->left = pj_left(pipeline->steps.front().pj);

    /* Now, correspondingly determine forward output (= reverse input) data type
     */
    P->right = pj_right(pipeline->steps.back().pj);
    return P;
}

static void push(PJ_COORD &point, PJ *P) {
    if (P->parent == nullptr)
        return;

    struct Pipeline *pipeline =
        static_cast<struct Pipeline *>(P->parent->opaque);
    struct PushPop *pushpop = static_cast<struct PushPop *>(P->opaque);

    if (pushpop->v1)
        pipeline->stack[0].push(point.v[0]);
    if (pushpop->v2)
        pipeline->stack[1].push(point.v[1]);
    if (pushpop->v3)
        pipeline->stack[2].push(point.v[2]);
    if (pushpop->v4)
        pipeline->stack[3].push(point.v[3]);
}

static void pop(PJ_COORD &point, PJ *P) {
    if (P->parent == nullptr)
        return;

    struct Pipeline *pipeline =
        static_cast<struct Pipeline *>(P->parent->opaque);
    struct PushPop *pushpop = static_cast<struct PushPop *>(P->opaque);

    if (pushpop->v1 && !pipeline->stack[0].empty()) {
        point.v[0] = pipeline->stack[0].top();
        pipeline->stack[0].pop();
    }

    if (pushpop->v2 && !pipeline->stack[1].empty()) {
        point.v[1] = pipeline->stack[1].top();
        pipeline->stack[1].pop();
    }

    if (pushpop->v3 && !pipeline->stack[2].empty()) {
        point.v[2] = pipeline->stack[2].top();
        pipeline->stack[2].pop();
    }

    if (pushpop->v4 && !pipeline->stack[3].empty()) {
        point.v[3] = pipeline->stack[3].top();
        pipeline->stack[3].pop();
    }
}

static PJ *setup_pushpop(PJ *P) {
    auto pushpop =
        static_cast<struct PushPop *>(calloc(1, sizeof(struct PushPop)));
    P->opaque = pushpop;
    if (nullptr == P->opaque)
        return destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    if (pj_param_exists(P->params, "v_1"))
        pushpop->v1 = true;

    if (pj_param_exists(P->params, "v_2"))
        pushpop->v2 = true;

    if (pj_param_exists(P->params, "v_3"))
        pushpop->v3 = true;

    if (pj_param_exists(P->params, "v_4"))
        pushpop->v4 = true;

    P->left = PJ_IO_UNITS_WHATEVER;
    P->right = PJ_IO_UNITS_WHATEVER;

    return P;
}

PJ *OPERATION(push, 0) {
    P->fwd4d = push;
    P->inv4d = pop;

    return setup_pushpop(P);
}

PJ *OPERATION(pop, 0) {
    P->inv4d = push;
    P->fwd4d = pop;

    return setup_pushpop(P);
}
