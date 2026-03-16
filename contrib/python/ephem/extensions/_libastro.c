/* Python header files. */

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#define PyUnicode_GetSize(o) PyUnicode_GET_SIZE(o)

#if PY_MAJOR_VERSION == 2
#define PyLong_AsLong PyInt_AsLong
#define PyLong_FromLong PyInt_FromLong
#define PyUnicode_Check3 PyString_Check
#undef PyUnicode_FromFormat
#undef PyUnicode_FromString
#undef PyUnicode_FromStringAndSize
#define PyUnicode_FromFormat PyString_FromFormat
#define PyUnicode_FromString PyString_FromString
#define PyUnicode_FromStringAndSize PyString_FromStringAndSize
#define PyUnicode_Type PyString_Type
#define PyUnicode_AsUTF8 PyString_AsString
#undef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(p, b) PyObject_HEAD_INIT(p) 0,
#define OB_REFCNT ob_refcnt
#else
#define PyUnicode_Check3 PyUnicode_Check
#define OB_REFCNT ob_base.ob_refcnt
#if PY_MINOR_VERSION == 0 || PY_MINOR_VERSION == 1 || PY_MINOR_VERSION == 2
#define PyUnicode_AsUTF8 _PyUnicode_AsString
# elif PY_MINOR_VERSION >= 3
#undef PyUnicode_GetSize
#define PyUnicode_GetSize PyUnicode_GET_LENGTH
#endif
#endif

#include "datetime.h"
#include "structmember.h"

/* The libastro header files. */

#include "astro.h"
#include "preferences.h"

/* Undo the astro.h #defines of common body attribute names. */

#undef mjd
#undef lat
#undef lng
#undef tz
#undef temp
#undef pressure
#undef elev
#undef dip
#undef epoch
#undef tznm
#undef mjed

/* Various phrases that need to be repeated frequently in docstrings. */

#define D " as a float giving radians, or a string giving degrees:minutes:seconds"
#define H " as a float giving radians, or a string giving hours:minutes:seconds"

/* Since different Body attributes are calculated with different
   routines, the compute() function itself just writes the time into
   the Body's `now' cache, and the VALID bits - stored in the user
   flags field of each `obj' - are used to coordinate lazy computation
   of the fields the user actually tries to access. */

#define VALID_GEO   FUSER0	/* Now has mjd and epoch */
#define VALID_TOPO  FUSER1	/* Now has entire Observer */
#define VALID_OBJ   FUSER2	/* object fields have been computed */
#define VALID_RISET FUSER3	/* riset fields have been computed */

#define VALID_LIBRATION FUSER4	/* moon libration fields computed */
#define VALID_COLONG  FUSER5	/* moon co-longitude fields computed */

#define VALID_CML   FUSER4	/* jupiter central meridian computed */

#define VALID_RINGS   FUSER4	/* saturn ring fields computed */

/* Global access to the module, once initialized. */

static PyObject *module = 0;

/* Core data structures. */

typedef struct {
     PyObject_HEAD
     Now now;
} Observer;

typedef struct {
     PyObject_HEAD
     Now now;			/* cache of last argument to compute() */
     Obj obj;			/* the ephemeris object */
     RiseSet riset;		/* rising and setting */
     PyObject *name;		/* object name */
} Body;

typedef Body Planet, PlanetMoon;
typedef Body FixedBody, BinaryStar;
typedef Body EllipticalBody, ParabolicBody, HyperbolicBody;

typedef struct {
     PyObject_HEAD
     Now now;			/* cache of last observer */
     Obj obj;			/* the ephemeris object */
     RiseSet riset;		/* rising and setting */
     PyObject *name;		/* object name */
     double llat, llon;		/* libration */
     double c, k, s;		/* co-longitude and illumination */
} Moon;

typedef struct {
     PyObject_HEAD
     Now now;			/* cache of last observer */
     Obj obj;			/* the ephemeris object */
     RiseSet riset;		/* rising and setting */
     PyObject *name;		/* object name */
     double cmlI, cmlII;	/* positions of Central Meridian */
} Jupiter;

typedef struct {
     PyObject_HEAD
     Now now;			/* cache of last observer */
     Obj obj;			/* the ephemeris object */
     RiseSet riset;		/* rising and setting */
     PyObject *name;		/* object name */
     double etilt, stilt;	/* tilt of rings */
} Saturn;

typedef struct {
     PyObject_HEAD
     Now now;			/* cache of last observer */
     Obj obj;			/* the ephemeris object */
     RiseSet riset;		/* rising and setting */
     PyObject *name;		/* object name */
     PyObject *catalog_number;	/* TLE catalog number */
} EarthSatellite;

/* Forward declaration. */

static int Body_obj_cir(Body *body, char *fieldname, unsigned topocentric);

/* Return the mjd of the current time. */

static double mjd_now(void)
{
     return 25567.5 + time(NULL)/3600.0/24.0;
}

/* Return the floating-point equivalent value of a Python number. */

static int PyNumber_AsDouble(PyObject *o, double *dp)
{
     PyObject *f = PyNumber_Float(o);
     if (!f) return -1;
     *dp = PyFloat_AsDouble(f);
     Py_DECREF(f);
     return 0;
}

/* Convert a base-60 ("sexagesimal") string like "02:30:00" into a
   double value like 2.5.  Uses Python split() and float(), which are
   slower than raw C but are sturdy and robust and eliminate all of the
   locale problems to which raw C calls are liable. */

static PyObject *scansexa_split = 0;

static int scansexa(PyObject *o, double *dp) {
     if (!scansexa_split) {
         scansexa_split = PyObject_GetAttrString(module, "_scansexa_split");
         if (!scansexa_split)
             return -1;
     }
     PyObject *list = PyObject_CallFunction(scansexa_split, "O", o);
     if (!list) {
          return -1;
     }
     int length = PyList_Size(list);
     double d = 0.0;
     int i;
     for (i=length-1; i>=0; i--) {
          d /= 60.0;
          PyObject *item = PyList_GetItem(list, i);  /* borrowed reference! */
          if (!item) {  /* should never happen, but just in case */
               Py_DECREF(list);
               return -1;
          }
          Py_ssize_t item_length = PyUnicode_GetSize(item);
          if (item_length == 0) {
               continue;  /* accept empty string for 0 */
          }
          PyObject *verdict = PyObject_CallMethod(item, "isspace", NULL);
          if (!verdict) {  /* shouldn't happen unless we're out of memory? */
               Py_DECREF(list);
               return -1;
          }
          int is_verdict_true = PyObject_IsTrue(verdict);
          Py_DECREF(verdict);
          if (is_verdict_true) {
               continue;  /* accept whitespace for 0 */
          }
          PyObject *float_obj = PyNumber_Float(item);
          if (!float_obj) {  /* can't parse nonempty string as float? error! */
               Py_DECREF(list);
               return -1;
          }
          double n = PyFloat_AsDouble(float_obj);
          d = copysign(d, n);
          d += n;
          Py_DECREF(float_obj);
     }
     *dp = d;
     Py_DECREF(list);
     return 0;
}

/* The libastro library offers a "getBuiltInObjs()" function that
   initializes the list of planets that XEphem displays by default.
   Rather than duplicate its logic for how to build each objects, we
   simply make a copy of one of its objects when the user asks for a
   planet or moon.

   This function is exposed by the module so that ephem.py can build
   Planet classes dynamically; each element of the list it returns is
   a tuple that looks like (7, "Planet", "Pluto"): */

static PyObject *builtin_planets(PyObject *self)
{
     PyObject *list = 0, *tuple = 0;
     Obj *objects;
     int i, n = getBuiltInObjs(&objects);

     list = PyList_New(n);
     if (!list) goto fail;

     for (i=0; i < n; i++) {
          tuple = Py_BuildValue(
               "iss", i, objects[i].pl_moon ? "PlanetMoon" : "Planet",
               objects[i].o_name);
          if (!tuple) goto fail;
          if (PyList_SetItem(list, i, tuple) == -1) goto fail;
     }

     return list;
fail:
     Py_XDECREF(list);
     Py_XDECREF(tuple);
     return 0;
}

/* This function is used internally to copy the attributes of a
   built-in libastro object into a new Planet (see above). */

static int copy_planet_from_builtin(Planet *planet, int builtin_index)
{
     Obj *builtins;
     int max = getBuiltInObjs(&builtins);
     if (builtin_index < 0 || builtin_index >= max) {
          PyErr_Format(PyExc_TypeError, "internal error: libastro has"
                       " no builtin object at slot %d", builtin_index);
          return -1;
     }
     memcpy(&planet->obj, &builtins[builtin_index], sizeof(Obj));
     return 0;
}

/* Angle: Python float which prints itself as a sexagesimal value,
   like 'hours:minutes:seconds' or 'degrees:minutes:seconds',
   depending on the factor given it. */

static PyTypeObject AngleType;

typedef struct {
     PyFloatObject f;
     double factor;
} AngleObject;

static PyObject *Angle_new(PyObject *self, PyObject *args, PyObject *kw)
{
     PyErr_SetString(PyExc_TypeError,
                     "you can only create an ephem.Angle"
                     " through ephem.degrees() or ephem.hours()");
     return 0;
}

static PyObject* new_Angle(double radians, double factor)
{
     AngleObject *ea;
     ea = PyObject_NEW(AngleObject, &AngleType);
     if (ea) {
	  ea->f.ob_fval = radians;
	  ea->factor = factor;
     }
     return (PyObject*) ea;
}

static char *Angle_format(PyObject *self)
{
     AngleObject *ea = (AngleObject*) self;
     static char buffer[13];
     fs_sexa(buffer, ea->f.ob_fval * ea->factor, 3,
             ea->factor == radhr(1) ? 360000 : 36000);
     return buffer[0] != ' ' ? buffer
	  : buffer[1] != ' ' ? buffer + 1
	  : buffer + 2;
}

static PyObject* Angle_str(PyObject *self)
{
     return PyUnicode_FromString(Angle_format(self));
}

static int Angle_print(PyObject *self, FILE *fp, int flags)
{
     fputs(Angle_format(self), fp);
     return 0;
}

static PyObject *Angle_pos(PyObject *self)
{
     Py_INCREF(self);
     return self;
}

static PyObject *Angle_neg(PyObject *self)
{
     AngleObject *ea = (AngleObject*) self;
     double radians = ea->f.ob_fval;
     return new_Angle(- radians, ea->factor);
}

static PyObject *Angle_get_norm(PyObject *self, void *v)
{
     AngleObject *ea = (AngleObject*) self;
     double radians = ea->f.ob_fval;
     if (radians < 0)
          return new_Angle(fmod(radians, 2*PI) + 2*PI, ea->factor);
     if (radians >= 2*PI)
          return new_Angle(fmod(radians, 2*PI), ea->factor);
     Py_INCREF(self);
     return self;
}

static PyObject *Angle_get_znorm(PyObject *self, void *v)
{
     AngleObject *ea = (AngleObject*) self;
     double radians = ea->f.ob_fval;
     if (radians <= -PI)
          return new_Angle(fmod(radians + PI, 2*PI) + PI, ea->factor);
     if (radians > PI)
          return new_Angle(fmod(radians - PI, 2*PI) - PI, ea->factor);
     Py_INCREF(self);
     return self;
}

static PyNumberMethods Angle_NumberMethods = {
     NULL, NULL, NULL, NULL, NULL, NULL, /* skip six fields */
#if PY_MAJOR_VERSION == 2
     NULL,
#endif
     Angle_neg, /* nb_negative */
     Angle_pos, /* nb_positive */
     NULL
};

static PyGetSetDef Angle_getset[] = {
     {"norm", Angle_get_norm, NULL,
      "Return this angle normalized to the interval [0, 2*pi).", 0},
     {"znorm", Angle_get_znorm, NULL,
      "Return this angle normalized to the interval (-pi, pi].", 0},
     {NULL}
};

static PyTypeObject AngleType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Angle",
     sizeof(AngleObject),
     0,
     0,				/* tp_dealloc */
#if PY_MAJOR_VERSION < 3
     Angle_print,		/* tp_print */
#else
     0,				/* reserved in 3.x */
#endif
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     &Angle_NumberMethods,      /* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     Angle_str,			/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     "An angle in radians that can print itself in an astronomical format.\n"
     "Use ephem.degrees() and ephem.radians() to create one.", /* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,                         /* tp_methods */
     0,				/* tp_members */
     Angle_getset,		/* tp_getset */
     0, /* &PyFloatType*/	/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     0,				/* tp_init */
     0,				/* tp_alloc */
     (newfunc) Angle_new,       /* tp_new */
     0,				/* tp_free */
};

/* Date: a Python Float that can print itself as a date, and that
   supports triple() and tuple() methods for extracting its date and
   time components. */

static PyTypeObject DateType;

typedef PyFloatObject DateObject;

static int parse_mjd_from_number(PyObject *o, double *mjdp)
{
     return PyNumber_AsDouble(o, mjdp);
}

static int parse_mjd_from_string(PyObject *so, double *mjdp)
{
     /* Run the Python code: s = so.strip() */
     PyObject *emptytuple = PyTuple_New(0);
     PyObject *split_func = PyObject_GetAttrString(so, "split");
     PyObject *pieces = PyObject_Call(split_func, emptytuple, 0);
     Py_ssize_t len = PyObject_Length(pieces);
     int year = 0, month = 1;
     double day = 1.0;

     Py_DECREF(emptytuple);
     Py_DECREF(split_func);

     if ((len < 1) || (len > 2))
	  goto fail;

     if (len >= 1) {
	  int i;
          const char *s = PyUnicode_AsUTF8(PyList_GetItem(pieces, 0));
          if (!s) goto fail;

	  /* Make sure all characters are in set '-/.0123456789' */

	  for (i=0; s[i]; i++) {
	       if (s[i] != '-' && s[i] != '/' && s[i] != '.'
		   && (s[i] < '0' || s[i] > '9')) {
		    goto fail;
               }
          }

	  f_sscandate((char*) s, PREF_YMD, &month, &day, &year);
     }

     if (len >= 2) {
          double hours;
          int status = scansexa(PyList_GetItem(pieces, 1), &hours);
          if (status == -1) {
               goto fail;
          }
	  day += hours / 24.;
     }

     cal_mjd(month, day, year, mjdp);

     Py_DECREF(pieces);
     return 0;

fail:
     if (! PyErr_Occurred()) {
	  PyObject *repr = PyObject_Repr(so);
          PyObject *complaint = PyUnicode_FromFormat(
	       "your date string %s does not look like a year/month/day"
	       " optionally followed by hours:minutes:seconds",
               PyUnicode_AsUTF8(repr));
	  PyErr_SetObject(PyExc_ValueError, complaint);
	  Py_DECREF(repr);
	  Py_DECREF(complaint);
     }
     Py_DECREF(pieces);
     return -1;
}

static int parse_mjd_from_tuple(PyObject *value, double *mjdp)
{
     double day = 1.0, hours = 0.0, minutes = 0.0, seconds = 0.0;
     int year, month = 1;
     if (!PyArg_ParseTuple(value, "i|idddd:date.tuple", &year, &month, &day,
			   &hours, &minutes, &seconds)) return -1;
     cal_mjd(month, day, year, mjdp);
     if (hours) *mjdp += hours / 24.;
     if (minutes) *mjdp += minutes / (24. * 60.);
     if (seconds) *mjdp += seconds / (24. * 60. * 60.);
     return 0;
}

static int parse_mjd_from_datetime(PyObject *value, double *mjdp)
{
     cal_mjd(PyDateTime_GET_MONTH(value),
             PyDateTime_GET_DAY(value),
             PyDateTime_GET_YEAR(value),
             mjdp);

     if (!PyDateTime_Check(value))
          return 0;  // the value is a mere Date

     *mjdp += PyDateTime_DATE_GET_HOUR(value) / 24.;
     *mjdp += PyDateTime_DATE_GET_MINUTE(value) / (24. * 60.);
     *mjdp += PyDateTime_DATE_GET_SECOND(value) / (24. * 60. * 60.);
     *mjdp += PyDateTime_DATE_GET_MICROSECOND(value)
          / (24. * 60. * 60. * 1000000.);

     PyObject *offset = PyObject_CallMethod(value, "utcoffset", NULL);
     if (!offset)
          return -1;

     if (offset == Py_None) {
          Py_DECREF(offset);
          return 0;  // no time zone information: assume UTC
     }

     PyObject *seconds = PyObject_CallMethod(offset, "total_seconds", NULL);
     Py_DECREF(offset);
     if (!seconds)
          return -1;

     double seconds_double;
     if (PyNumber_AsDouble(seconds, &seconds_double)) {
          Py_DECREF(seconds);
          return -1;
     }
     Py_DECREF(seconds);

     *mjdp -= seconds_double / (24. * 60. * 60.);
     return 0;
}

static int parse_mjd(PyObject *value, double *mjdp)
{
     if (PyNumber_Check(value))
	  return parse_mjd_from_number(value, mjdp);
     else if (PyUnicode_Check3(value))
	  return parse_mjd_from_string(value, mjdp);
     else if (PyTuple_Check(value))
	  return parse_mjd_from_tuple(value, mjdp);
     else if (PyDate_Check(value))
          return parse_mjd_from_datetime(value, mjdp);
     PyErr_SetString(PyExc_ValueError, "dates must be initialized"
                     " from a number, string, tuple, or datetime");
     return -1;
}

static void mjd_six(double mjd, int *yearp, int *monthp, int *dayp,
		    int *hourp, int *minutep, double *secondp)
{
     mjd += 0.5 / 8.64e+10;  /* half microsecond, so floor() becomes "round" */
     mjd_cal(mjd, monthp, &mjd, yearp);

     double day = floor(mjd);
     double fraction = mjd - day;
     *dayp = (int) day;

     /* Turns out a Windows "long" is only 32 bits, so "long long". */
     long long us = floor(fraction * 8.64e+10);  /* microseconds per day */
     long long minute = us / 60000000;
     us -= minute * 60000000;
     *secondp = ((double) us) / 1e6;
     *hourp = (int) (minute / 60);
     *minutep = (int) (minute - *hourp * 60);
}

static PyObject* build_Date(double mjd)
{
     DateObject *new = PyObject_New(PyFloatObject, &DateType);
     if (new) new->ob_fval = mjd;
     return (PyObject*) new;
}

static PyObject *Date_new(PyObject *self, PyObject *args, PyObject *kw)
{
     PyObject *arg;
     double mjd;
     if (kw) {
	  PyErr_SetString(PyExc_TypeError,
			  "this function does not accept keyword arguments");
	  return 0;
     }
     if (!PyArg_ParseTuple(args, "O:date", &arg)) return 0;
     if (parse_mjd(arg, &mjd)) return 0;
     return build_Date(mjd);
}

static char *Date_format_value(double value)
{
     static char buffer[64];
     int year, month, day, hour, minute;
     double second;
     /* Note the offset, which makes us round to the nearest second. */
     mjd_six(value + 0.5 / (24.0 * 60.0 * 60.0),
             &year, &month, &day, &hour, &minute, &second);
     sprintf(buffer, "%d/%d/%d %02d:%02d:%02d",
	     year, month, day, hour, minute, (int) second);
     return buffer;
}

static char *Date_format(PyObject *self)
{
     DateObject *d = (DateObject*) self;
     return Date_format_value(d->ob_fval);
}

static PyObject* Date_str(PyObject *self)
{
     return PyUnicode_FromString(Date_format(self));
}

static int Date_print(PyObject *self, FILE *fp, int flags)
{
     fputs(Date_format(self), fp);
     return 0;
}

static PyObject *Date_triple(PyObject *self)
{
     int year, month;
     double day;
     DateObject *d = (DateObject*) self;
     mjd_cal(d->ob_fval, &month, &day, &year);
     return Py_BuildValue("iid", year, month, day);
}

static PyObject *Date_tuple(PyObject *self)
{
     int year, month, day, hour, minute;
     double second;
     DateObject *d = (DateObject*) self;
     mjd_six(d->ob_fval, &year, &month, &day, &hour, &minute, &second);
     return Py_BuildValue("iiiiid", year, month, day, hour, minute, second);
}

static PyObject *Date_datetime(PyObject *self)
{
     int year, month, day, hour, minute;
     double second;
     DateObject *d = (DateObject*) self;
     mjd_six(d->ob_fval, &year, &month, &day, &hour, &minute, &second);
     return PyDateTime_FromDateAndTime(
          year, month, day, hour, minute, (int) floor(second),
          (int) floor(1e6 * fmod(second, 1.0))
          );
}

static PyMethodDef Date_methods[] = {
     {"triple", (PyCFunction) Date_triple, METH_NOARGS,
      "Return the date as a (year, month, day_with_fraction) tuple"},
     {"tuple", (PyCFunction) Date_tuple, METH_NOARGS,
      "Return the date as a (year, month, day, hour, minute, second) tuple"},
     {"datetime", (PyCFunction) Date_datetime, METH_NOARGS,
      "Return the date as a (year, month, day, hour, minute, second) tuple"},
     {NULL}
};

static PyTypeObject DateType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Date",
     sizeof(PyFloatObject),
     0,
     0,				/* tp_dealloc */
#if PY_MAJOR_VERSION < 3
     Date_print,		/* tp_print */
#else
     0,				/* tp_print slot is reserved and unused in Python 3 */
#endif
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     Date_str,			/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     "Floating point value used by ephem to represent a date.\n"
     "The value is the number of days since 1899 December 31 12:00 UT. When\n"
     "creating an instance you can pass in a Python datetime instance,"
     " timetuple,\nyear-month-day triple, or a plain float. Run str() on"
     "this object to see\nthe UTC date it represents.", /* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     Date_methods,		/* tp_methods */
     0,				/* tp_members */
     0,				/* tp_getset */
     0, /*&PyFloatType,*/	/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     0,				/* tp_init */
     0,				/* tp_alloc */
     (newfunc) Date_new,	/* tp_new */
     0,				/* tp_free */
};

/*
 * Both pairs of routines below have to access a structure member to
 * which they have been given the offset.
 */

#define THE_FLOAT (* (float*) ((char*)self + (size_t)v))
#define THE_DOUBLE (* (double*) ((char*)self + (size_t)v))

/*
 * Sexigesimal values can be assigned either floating-point numbers or
 * strings like '33:45:10', and return special Angle floats that
 * give pretty textual representations when asked for their str().
 */

typedef struct {
     int type;			/* T_FLOAT or T_DOUBLE */
     int offset;		/* offset of structure member */
     double ifactor;		/* internal units per radian */
     double efactor;		/* external units per radian */
} AngleMember;

static int parse_angle(PyObject *value, double factor, double *result)
{
     if (PyNumber_Check(value)) {
	  return PyNumber_AsDouble(value, result);
     } else if (PyUnicode_Check3(value)) {
	  double scaled;
          if (scansexa(value, &scaled) == -1) {
               return -1;
	  }
	  *result = scaled / factor;
	  return 0;
     } else {
	  PyErr_SetString(PyExc_TypeError,
			  "angle can only be created from string or number");
	  return -1;
     }
}

static double to_angle(PyObject *value, double efactor, int *status)
{
     double r;
#if PY_MAJOR_VERSION == 2
     /* Support Unicode strings under Python 2 */
     if (PyUnicode_Check(value)) {
          value = PyUnicode_AsUTF8String(value);
          if (!value) {
	       *status = -1;
               return 0;
          }
     }
#endif
     if (PyNumber_Check(value)) {
	  value = PyNumber_Float(value);
	  if (!value) {
	       *status = -1;
	       return 0;
	  }
	  r = PyFloat_AsDouble(value);
	  Py_DECREF(value);
	  *status = 0;
	  return r;
     } else if (PyUnicode_Check3(value)) {
	  double scaled;
          *status = scansexa(value, &scaled);
	  return scaled / efactor;
     } else {
	  PyErr_SetString(PyExc_TypeError,
			  "can only update value with string or number");
	  *status = -1;
	  return 0;
     }
}

/* Hours stored as radian double. */

static PyObject* getd_rh(PyObject *self, void *v)
{
     return new_Angle(THE_DOUBLE, radhr(1));
}

static int setd_rh(PyObject *self, PyObject *value, void *v)
{
     int status;
     THE_DOUBLE = to_angle(value, radhr(1), &status);
     return status;
}

/* Degrees stored as radian double. */

static PyObject* getd_rd(PyObject *self, void *v)
{
     return new_Angle(THE_DOUBLE, raddeg(1));
}

static int setd_rd(PyObject *self, PyObject *value, void *v)
{
     int status;
     THE_DOUBLE = to_angle(value, raddeg(1), &status);
     return status;
}

/* Degrees stored as degrees, but for consistency we return their
   floating point value as radians. */

static PyObject* getf_dd(PyObject *self, void *v)
{
     return new_Angle(THE_FLOAT * degrad(1), raddeg(1));
}

static int setf_dd(PyObject *self, PyObject *value, void *v)
{
     int status;
     THE_FLOAT = (float) to_angle(value, raddeg(1), &status);
     return status;
}

/* MDJ stored as double. */

static PyObject* getd_mjd(PyObject *self, void *v)
{
     return build_Date(THE_DOUBLE);
}

static int setd_mjd(PyObject *self, PyObject *value, void *v)
{
     double result;
     if (parse_mjd(value, &result)) return -1;
     THE_DOUBLE = result;
     return 0;
}

/* #undef THE_FLOAT
   #undef THE_DOUBLE */

/*
 * The following values are ones for which XEphem provides special
 * routines for their reading and writing, usually because they are
 * encoded into one or two bytes to save space in the obj structure
 * itself.  These are simply wrappers around those functions.
 */

/* Spectral codes. */

static PyObject* get_f_spect(PyObject *self, void *v)
{
     Body *b = (Body*) self;
     return PyUnicode_FromStringAndSize(b->obj.f_spect, 2);
}

static int set_f_spect(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     const char *s;
     if (!PyUnicode_Check3(value)) {
	  PyErr_SetString(PyExc_ValueError, "spectral code must be a string");
	  return -1;
     }
     s = PyUnicode_AsUTF8(value);
     if (!s)
          return -1;
     if (s[0] == '\0' || s[1] == '\0' || s[2] != '\0') {
	  PyErr_SetString(PyExc_ValueError,
			  "spectral code must be two characters long");
	  return -1;
     }
     b->obj.f_spect[0] = s[0];
     b->obj.f_spect[1] = s[1];
     return 0;
}

/* Fixed object diameter ratio. */

static PyObject* get_f_ratio(PyObject *self, void *v)
{
     Body *b = (Body*) self;
     return PyFloat_FromDouble(get_ratio(&b->obj));
}

static int set_f_ratio(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     double maj, min;
     if (!PyArg_ParseTuple(value, "dd", &maj, &min)) return -1;
     set_ratio(&b->obj, maj, min);
     return 0;
}

/* Position angle of fixed object. */

static PyObject* get_f_pa(PyObject *self, void *v)
{
     Body *b = (Body*) self;
     return PyFloat_FromDouble(get_pa(&b->obj));
}

static int set_f_pa(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     if (!PyNumber_Check(value)) {
	  PyErr_SetString(PyExc_ValueError, "position angle must be a float");
	  return -1;
     }
     set_pa(&b->obj, PyFloat_AsDouble(value));
     return 0;
}

/* Proper motion of fixed object; presented in milli-arcseconds per
   year, but stored as radians per day in a float. */

#define PROPER (1.327e-11)      /* from libastro's dbfmt.c */

static PyObject* getf_proper_ra(PyObject *self, void *v)
{
     Body *b = (Body*) self;
     return PyFloat_FromDouble(b->obj.f_pmRA * cos(b->obj.f_dec) / PROPER);
}

static int setf_proper_ra(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     if (!PyNumber_Check(value)) {
	  PyErr_SetString(PyExc_ValueError, "express proper motion"
                          " as milli-arcseconds per year");
	  return -1;
     }
     b->obj.f_pmRA = (float)
       (PyFloat_AsDouble(value) / cos(b->obj.f_dec) * PROPER);
     return 0;
}

static PyObject* getf_proper_dec(PyObject *self, void *v)
{
     Body *b = (Body*) self;
     return PyFloat_FromDouble(b->obj.f_pmdec / PROPER);
}

static int setf_proper_dec(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     if (!PyNumber_Check(value)) {
	  PyErr_SetString(PyExc_ValueError, "express proper motion"
                          " as milli-arcseconds per year");
	  return -1;
     }
     b->obj.f_pmdec = (float) (PyFloat_AsDouble(value) * PROPER);
     return 0;
}

/*
 * Observer object.
 */

/*
 * Constructor and methods.
 */

static int Observer_init(PyObject *self, PyObject *args, PyObject *kwds)
{
     Observer *o = (Observer*) self;
     static char *kwlist[] = {0};
     if (!PyArg_ParseTupleAndKeywords(args, kwds, ":Observer", kwlist))
	  return -1;
     o->now.n_mjd = mjd_now();
     o->now.n_lat = o->now.n_lng = o->now.n_tz = o->now.n_elev
	  = o->now.n_dip = 0;
     o->now.n_temp = 15.0;
     o->now.n_pressure = 1010;
     o->now.n_epoch = J2000;
     return 0;
}

static PyObject *Observer_sidereal_time(PyObject *self)
{
     Observer *o = (Observer*) self;
     double lst;
     now_lst(&o->now, &lst);
     return new_Angle(hrrad(lst), radhr(1));
}

static PyObject *Observer_radec_of(PyObject *self, PyObject *args,
				   PyObject *kwds)
{
     Observer *o = (Observer*) self;
     static char *kwlist[] = {"az", "alt", 0};
     PyObject *azo, *alto, *rao, *deco;
     double az, alt, lst, ha, ra, dec;

     if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO:Observer.radec_of",
				      kwlist, &azo, &alto))
	  return 0;

     if (parse_angle(azo, raddeg(1), &az) == -1)
	  return 0;
     if (parse_angle(alto, raddeg(1), &alt) == -1)
	  return 0;

     now_lst(&o->now, &lst);
     lst = hrrad(lst);
     unrefract(o->now.n_pressure, o->now.n_temp, alt, &alt);
     aa_hadec(o->now.n_lat, alt, az, &ha, &dec);
     ra = fmod(lst - ha, 2*PI);

     pref_set(PREF_EQUATORIAL, PREF_TOPO); /* affects call to ap_as? */
     if (o->now.n_epoch != EOD)
	  ap_as(&o->now, o->now.n_epoch, &ra, &dec);

     rao = new_Angle(ra, radhr(1));
     if (!rao) return 0;
     deco = new_Angle(dec, raddeg(1));
     if (!deco) return 0;
     return Py_BuildValue("NN", rao, deco);
}

/*
 * Member access.
 */

static PyObject *get_elev(PyObject *self, void *v)
{
     Observer *o = (Observer*) self;
     return PyFloat_FromDouble(o->now.n_elev * ERAD);
}

static int set_elev(PyObject *self, PyObject *value, void *v)
{
     int r;
     double n;
     Observer *o = (Observer*) self;
     if (!PyNumber_Check(value)) {
	  PyErr_SetString(PyExc_TypeError, "Elevation must be numeric");
	  return -1;
     }
     r = PyNumber_AsDouble(value, &n);
     if (!r) o->now.n_elev = n / ERAD;
     return 0;
}

/*
 * Observer class type.
 *
 * Note that we commandeer the n_dip field for our own purposes.
 * XEphem uses it to compute the beginning and end of twilight; since
 * we have no twilight functions, we use it to store the displacement
 * for rising and setting calculations.
 */

#define VOFF(member) ((void*) OFF(member))
#define OFF(member) offsetof(Observer, now.member)

static PyMethodDef Observer_methods[] = {
     {"sidereal_time", (PyCFunction) Observer_sidereal_time, METH_NOARGS,
      "compute the local sidereal time for this location and time"},
     {"radec_of", (PyCFunction) Observer_radec_of,
      METH_VARARGS | METH_KEYWORDS,
      "compute the right ascension and declination of a point"
      " identified by its azimuth and altitude"},
     {NULL}
};

static PyGetSetDef Observer_getset[] = {
     {"date", getd_mjd, setd_mjd, "Date", VOFF(n_mjd)},
     {"lat", getd_rd, setd_rd, "Latitude north of the Equator" D, VOFF(n_lat)},
     {"lon", getd_rd, setd_rd, "Longitude east of Greenwich" D, VOFF(n_lng)},
     {"elevation", get_elev, set_elev,
      "Elevation above sea level in meters", NULL},
     {"horizon", getd_rd, setd_rd,
      "The angle above (+) or below (-) the horizon at which an object"
      " should be considered at the moment of rising or setting" D,
      VOFF(n_dip)},
     {"epoch", getd_mjd, setd_mjd, "Precession epoch", VOFF(n_epoch)},

     /* For compatibility with older scripts: */
     {"long", getd_rd, setd_rd, "Longitude east of Greenwich" D, VOFF(n_lng)},

     {NULL}
};

static PyMemberDef Observer_members[] = {
     {"temp", T_DOUBLE, OFF(n_temp), 0,
      "alias for 'temperature' attribute"},
     {"temperature", T_DOUBLE, OFF(n_temp), 0,
      "atmospheric temperature in degrees Celsius"},
     {"pressure", T_DOUBLE, OFF(n_pressure), 0,
      "atmospheric pressure in millibar"},
     {NULL}
};

#undef OFF

static PyTypeObject ObserverType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "_libastro.Observer",
     sizeof(Observer),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags */
     "Describes an observer standing on the Earth's surface.\n"
     "This object can also store the time at which the observer is watching;\n"
     "the epoch in which they want astrometric coordinates returned; and the\n"
     "temperature and barometric pressure, which affect horizon refraction.\n"
     "See Body.compute() for how to use instances of this class.", /* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     Observer_methods,		/* tp_methods */
     Observer_members,		/* tp_members */
     Observer_getset,		/* tp_getset */
     0,				/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Observer_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0				/* tp_free */
};

/*
 *
 * BODIES
 *
 */

static PyTypeObject BodyType;
static PyTypeObject PlanetType;
static PyTypeObject JupiterType;
static PyTypeObject SaturnType;
static PyTypeObject MoonType;
static PyTypeObject PlanetMoonType;

static PyObject* Body_compute(PyObject *self, PyObject *args, PyObject *kwds);

/* Body and Planet may be initialized privately by their subclasses
   using these setup functions. */

static void Body_setup(Body *body)
{
     body->obj.o_flags = 0;
}

/* Body, Planet, and PlanetMoon are abstract classes that resist
   instantiation. */

static int Body_init(PyObject *self, PyObject *args, PyObject *kw)
{
     PyErr_SetString(PyExc_TypeError, "you cannot create a generic Body");
     return -1;
}

static int Planet_setup(Planet *planet, int builtin_index,
                        PyObject *args, PyObject *kw)
{
     if (copy_planet_from_builtin(planet, builtin_index) == -1)
          return -1;
     planet->name = 0;
     if (PyTuple_Check(args) && PyTuple_Size(args)) {
	  PyObject *result = Body_compute((PyObject*) planet, args, kw);
	  if (!result) return -1;
	  Py_DECREF(result);
     }
     return 0;
}

static int Planet_init(PyObject *self, PyObject *args, PyObject *kw)
{
     int builtin_index;
     PyObject *o = PyObject_GetAttrString(self, "__planet__");
     if (!o) {
          PyErr_SetString(PyExc_TypeError, "internal error: cannot"
                           " init Planet without a __planet__ code");
          return -1;
     }
     builtin_index = PyLong_AsLong(o);
     Py_DECREF(o);
     if ((builtin_index == -1) && PyErr_Occurred()) {
          PyErr_SetString(PyExc_TypeError, "internal error: __planet__"
                           " code must be an integer");
          return -1;
     }
     return Planet_setup((Planet*) self, builtin_index, args, kw);
}

/* But Jupiter, Saturn, and the Moon, as concrete classes, do allow
   initialization. */

static int Jupiter_init(PyObject *self, PyObject *args, PyObject *kw)
{
     return Planet_setup((Planet*) self, JUPITER, args, kw);
}

static int Saturn_init(PyObject *self, PyObject *args, PyObject *kw)
{
     return Planet_setup((Planet*) self, SATURN, args, kw);
}

static int Moon_init(PyObject *self, PyObject *args, PyObject *kw)
{
     return Planet_setup((Planet*) self, MOON, args, kw);
}

/* Bodies need to unlink any name object when finished. */

static void Body_dealloc(PyObject *self)
{
     Body *body = (Body*) self;
     Py_XDECREF(body->name);
     Py_TYPE(self)->tp_free(self);
}

/* The user-configurable body types also share symmetric
   initialization code. */

#define INIT(NAME, CODE) \
static int NAME##_init(PyObject* self, PyObject* args, PyObject *kw) \
{ \
     Body *body = (Body*) self; \
     Body_setup(body); \
     body->name = Py_None; \
     Py_INCREF(Py_None); \
     body->obj.o_name[0] = '\0'; \
     body->obj.o_type = CODE; \
     return 0; \
}

static int FixedBody_init(PyObject* self, PyObject* args, PyObject *kw)
{
     Body *body = (Body*) self;
     static char *kwlist[] = {0};
     if (!PyArg_ParseTupleAndKeywords(args, kw, ":FixedBody", kwlist))
         return -1;
     Body_setup(body);
     body->name = Py_None;
     Py_INCREF(Py_None);
     body->obj.o_name[0] = '\0';
     body->obj.o_type = FIXED;
     body->obj.f_epoch = J2000;
     return 0;
}

INIT(BinaryStar, BINARYSTAR)
INIT(EllipticalBody, ELLIPTICAL)
INIT(HyperbolicBody, HYPERBOLIC)
INIT(ParabolicBody, PARABOLIC)

static int EarthSatellite_init(PyObject* self, PyObject* args, PyObject *kw)
{
     EarthSatellite *body = (EarthSatellite*) self;
     Body_setup((Body*) body);
     body->name = Py_None;
     Py_INCREF(Py_None);
     body->catalog_number = Py_None;
     Py_INCREF(Py_None);
     body->obj.o_name[0] = '\0';
     body->obj.o_type = EARTHSAT;
     return 0;
}

static void EarthSatellite_dealloc(PyObject *self)
{
     EarthSatellite *e = (EarthSatellite*) self;
     Py_XDECREF(e->name);
     Py_XDECREF(e->catalog_number);
     Py_TYPE(self)->tp_free(self);
}

#undef INIT

/* This compute() method does not actually compute anything, since
   several different computations would be necessary to determine the
   value of all of a body's fields, and the user may not want them
   all; so compute() just stashes away the Observer or date it is
   given, and the fields are computed when the user actually accesses
   them. */

static PyObject* Body_compute(PyObject *self, PyObject *args, PyObject *kwds)
{
     Body *body = (Body*) self;
     static char *kwlist[] = {"when", "epoch", 0};
     PyObject *when_arg = 0, *epoch_arg = 0;

     if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OO:Body.compute", kwlist,
				      &when_arg, &epoch_arg))
	  return 0;

     if (when_arg && PyObject_TypeCheck(when_arg, &ObserverType)) {

	  /* compute(observer) */

	  Observer *observer = (Observer*) when_arg;
	  if (epoch_arg) {
	       PyErr_SetString(PyExc_ValueError,
			       "cannot supply an epoch= keyword argument "
			       "because an Observer specifies its own epoch");
	       return 0;
	  }
	  body->now = observer->now;
	  body->obj.o_flags = VALID_GEO | VALID_TOPO;
     } else {
	  double when_mjd, epoch_mjd;

	  /* compute(date, epoch) where date defaults to current time
	     and epoch defaults to 2000.0 */

	  if (when_arg) {
	       if (parse_mjd(when_arg, &when_mjd) == -1) goto fail;
	  } else
	       when_mjd = mjd_now();

	  if (epoch_arg) {
	       if (parse_mjd(epoch_arg, &epoch_mjd) == -1) goto fail;
	  } else
	       epoch_mjd = J2000;

	  /* Since libastro always does topocentric computation, we
	     need to provide a reasonable location and weather. */
	  body->now.n_mjd = when_mjd;
	  body->now.n_lat = body->now.n_lng = body->now.n_tz
	       = body->now.n_elev = body->now.n_dip = 0;
	  body->now.n_temp = 15.0;
	  body->now.n_pressure = 0; /* no refraction */
	  body->now.n_epoch = epoch_mjd;

	  body->obj.o_flags = VALID_GEO;
     }

     if (body->obj.o_type == EARTHSAT) {
          double days_from_epoch = fabs(body->obj.es_epoch - body->now.n_mjd);
          if (days_from_epoch > 365.0) {
	       PyErr_Format(PyExc_ValueError, "TLE elements are valid for"
               " a few weeks around their epoch, but you are asking about"
               " a date %d days from the epoch", (int) days_from_epoch);
               goto fail;
          }
     }

     Py_INCREF(Py_None);
     return Py_None;

fail:
     return 0;
}

static PyObject* Body_parallactic_angle(PyObject *self)
{
     PyObject *a1, *a2;
     Body *body = (Body*) self;
     double ha, pa;
     if (Body_obj_cir(body, "parallactic_angle", 1) == -1)
          return 0;
     radec2ha(&(body->now), body->obj.s_astrora, body->obj.s_astrodec, &ha);
     pa = parallacticLHD(body->now.n_lat, ha, body->obj.s_astrodec);
     a1 = new_Angle(pa, raddeg(1));
     if (!a1)
          return 0;
     a2 = Angle_get_znorm(a1, 0);
     Py_XDECREF(a1);
     return a2;
}

static PyObject* Body_writedb(PyObject *self)
{
     Body *body = (Body*) self;
     char line[1024];
     db_write_line(&body->obj, line);
     return PyUnicode_FromString(line);
}

void Body__copy_struct(Body *, Body *);

static PyObject* Body_copy(PyObject *self)
{
     Body *body = (Body *) self;
     Body *newbody = (Body*) Py_TYPE(self)->tp_alloc(Py_TYPE(self), 0);
     if (!newbody) return 0;
     Body__copy_struct(body, newbody);
     return (PyObject*) newbody;
}

static PyObject* Body_repr(PyObject *body_object)
{
     Body *body = (Body*) body_object;
     if (body->name) {
	  const char *name;
	  PyObject *repr, *result;
	  repr = PyObject_Repr(body->name);
	  if (!repr)
               return 0;
	  name = PyUnicode_AsUTF8(repr);
	  if (!name) {
               Py_DECREF(repr);
               return 0;
          }
	  result = PyUnicode_FromFormat("<%s %s at %p>",
                                        Py_TYPE(body)->tp_name, name, body);
	  Py_DECREF(repr);
	  return result;
     } else if (body->obj.o_name[0])
	  return PyUnicode_FromFormat("<%s \"%s\" at %p>",
                                      Py_TYPE(body)->tp_name,
                                      body->obj.o_name, body);
     else
	  return PyUnicode_FromFormat("<%s at %p>",
                                      Py_TYPE(body)->tp_name, body);
}

static PyMethodDef Body_methods[] = {
     {"compute", (PyCFunction) Body_compute, METH_VARARGS | METH_KEYWORDS,
      "compute the location of the body for the given date or Observer,"
      " or for the current time if no date is supplied"},
     {"parallactic_angle", (PyCFunction) Body_parallactic_angle, METH_NOARGS,
      "return the parallactic angle to the body; an Observer must have been"
      " provided to the most recent compute() call, because a parallactic"
      " angle is always measured with respect to a specfic observer"},
     {"writedb", (PyCFunction) Body_writedb, METH_NOARGS,
      "return a string representation of the body "
      "appropriate for inclusion in an ephem database file"},
     {"copy", (PyCFunction) Body_copy, METH_NOARGS,
      "Return a new copy of this body"},
     {"__copy__", (PyCFunction) Body_copy, METH_VARARGS,
      "Return a new copy of this body"},
     {NULL}
};

static PyObject* build_hours(double radians)
{
     return new_Angle(radians, radhr(1));
}

static PyObject* build_degrees(double radians)
{
     return new_Angle(radians, raddeg(1));
}

static PyObject* build_degrees_from_degrees(double degrees)
{
     return build_degrees(degrees / raddeg(1));
}

static PyObject* build_mag(double raw)
{
     return PyFloat_FromDouble(raw / MAGSCALE);
}

/* These are the functions which are each responsible for filling in
   some of the fields of an ephem.Body or one of its subtypes.  When
   called they determine whether the information in the fields for
   which they are responsible is up-to-date, and re-compute them if
   not.  By checking body->valid they can determine how much
   information the last compute() call supplied. */

static int Body_obj_cir(Body *body, char *fieldname, unsigned topocentric)
{
     if (body->obj.o_flags == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined until first compute()",
		       fieldname);
	  return -1;
     }
     if (topocentric && (body->obj.o_flags & VALID_TOPO) == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined because the most recent compute() "
		       "was supplied a date rather than an Observer",
		       fieldname);
	  return -1;
     }
     if (body->obj.o_flags & VALID_OBJ)
	  return 0;
     pref_set(PREF_EQUATORIAL, body->obj.o_flags & VALID_TOPO ?
	      PREF_TOPO : PREF_GEO);

     int is_error = obj_cir(& body->now, & body->obj) == -1;
     int is_inaccurate = body->obj.o_flags & NOCIRCUM;

     if (is_error || is_inaccurate) {
          char *extra = "";
          if (is_inaccurate)
               extra = " with any accuracy because its orbit is nearly"
                    " parabolic and it is very far from the Sun";
	  PyErr_Format(PyExc_RuntimeError, "cannot compute the body's position"
                       " at %s%s", Date_format_value(body->now.n_mjd), extra);
	  return -1;
     }
     body->obj.o_flags |= VALID_OBJ;
     return 0;
}

static int Body_riset_cir(Body *body, char *fieldname)
{
     static char *warning =
          "the ephem.Body attributes 'rise_time', 'rise_az',"
          " 'transit_time', 'transit_alt', 'set_time', 'set_az',"
          " 'circumpolar', and 'never_up' are deprecated;"
          " please convert your program to use the ephem.Observer"
          " functions next_rising(), previous_rising(), next_transit(),"
          " and so forth\n";
     static int warned_already = 0;
     if (!warned_already) {
          if (PyErr_Warn(PyExc_DeprecationWarning, warning)) return -1;
          warned_already = 1;
     }
     if ((body->obj.o_flags & VALID_RISET) == 0) {
	  if (body->obj.o_flags == 0) {
	       PyErr_Format(PyExc_RuntimeError, "field %s undefined"
			    " until first compute()", fieldname);
	       return -1;
	  }
	  if ((body->obj.o_flags & VALID_TOPO) == 0) {
	       PyErr_Format(PyExc_RuntimeError, "field %s undefined because"
			    " last compute() supplied a date"
			    " rather than an Observer", fieldname);
	       return -1;
	  }
	  riset_cir(& body->now, & body->obj,
		    - body->now.n_dip, & body->riset);
	  body->obj.o_flags |= VALID_RISET;
     }
     if (body->riset.rs_flags & RS_ERROR) {
	  PyErr_Format(PyExc_RuntimeError, "error computing rise, transit,"
		       " and set circumstances");
	  return -1;
     }
     return 0;
}

static int Moon_llibration(Moon *moon, char *fieldname)
{
     if (moon->obj.o_flags & VALID_LIBRATION)
	  return 0;
     if (moon->obj.o_flags == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined until first compute()", fieldname);
	  return -1;
     }
     llibration(MJD0 + moon->now.n_mjd, &moon->llat, &moon->llon);
     moon->obj.o_flags |= VALID_LIBRATION;
     return 0;
}

static int Moon_colong(Moon *moon, char *fieldname)
{
     if (moon->obj.o_flags & VALID_COLONG)
	  return 0;
     if (moon->obj.o_flags == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined until first compute()", fieldname);
	  return -1;
     }
     moon_colong(MJD0 + moon->now.n_mjd, 0, 0,
		 &moon->c, &moon->k, 0, &moon->s);
     moon->obj.o_flags |= VALID_COLONG;
     return 0;
}

static int Jupiter_cml(Jupiter *jupiter, char *fieldname)
{
     if (jupiter->obj.o_flags & VALID_CML)
          return 0;
     if (jupiter->obj.o_flags == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined until first compute()", fieldname);
	  return -1;
     }
     if (Body_obj_cir((Body*) jupiter, fieldname, 0) == -1) return -1;
     meeus_jupiter(jupiter->now.n_mjd, &jupiter->cmlI, &jupiter->cmlII, 0);
     jupiter->obj.o_flags |= VALID_CML;
     return 0;
}

static int Saturn_satrings(Saturn *saturn, char *fieldname)
{
     double lsn, rsn, bsn;
     if (saturn->obj.o_flags & VALID_RINGS)
	  return 0;
     if (saturn->obj.o_flags == 0) {
	  PyErr_Format(PyExc_RuntimeError,
		       "field %s undefined until first compute()", fieldname);
	  return -1;
     }
     if (Body_obj_cir((Body*) saturn, fieldname, 0) == -1) return -1;
     sunpos(saturn->now.n_mjd, &lsn, &rsn, &bsn);
     satrings(saturn->obj.s_hlat, saturn->obj.s_hlong, saturn->obj.s_sdist,
	      lsn + PI, rsn, MJD0 + saturn->now.n_mjd,
	      &saturn->etilt, &saturn->stilt);
     saturn->obj.o_flags |= VALID_RINGS;
     return 0;
}

/* The Body and Body-subtype fields themselves. */

#define GET_FIELD(name, field, builder) \
  static PyObject *Get_##name (PyObject *self, void *v) \
  { \
    BODY *body = (BODY*) self; \
    if (CALCULATOR(body, #name CARGS) == -1) return 0; \
    return builder(body->field); \
  }

/* Attributes computed by obj_cir that need only a date and epoch. */

#define BODY Body
#define CALCULATOR Body_obj_cir
#define CARGS ,0

GET_FIELD(epoch, now.n_epoch, build_Date)
GET_FIELD(ha, obj.s_ha, build_hours)
GET_FIELD(ra, obj.s_ra, build_hours)
GET_FIELD(dec, obj.s_dec, build_degrees)
GET_FIELD(gaera, obj.s_gaera, build_hours)
GET_FIELD(gaedec, obj.s_gaedec, build_degrees)
GET_FIELD(astrora, obj.s_astrora, build_hours)
GET_FIELD(astrodec, obj.s_astrodec, build_degrees)
GET_FIELD(elong, obj.s_elong, build_degrees_from_degrees)
GET_FIELD(mag, obj.s_mag, build_mag)
GET_FIELD(size, obj.s_size, PyFloat_FromDouble)
GET_FIELD(radius, obj.s_size * 2*PI / 360. / 60. / 60. / 2., build_degrees)

GET_FIELD(hlong, obj.s_hlong, build_degrees)
GET_FIELD(hlat, obj.s_hlat, build_degrees)
GET_FIELD(sun_distance, obj.s_sdist, PyFloat_FromDouble)
GET_FIELD(earth_distance, obj.s_edist, PyFloat_FromDouble)
GET_FIELD(phase, obj.s_phase, PyFloat_FromDouble)

GET_FIELD(x, obj.pl_x, PyFloat_FromDouble)
GET_FIELD(y, obj.pl_y, PyFloat_FromDouble)
GET_FIELD(z, obj.pl_z, PyFloat_FromDouble)
GET_FIELD(earth_visible, obj.pl_evis, PyFloat_FromDouble)
GET_FIELD(sun_visible, obj.pl_svis, PyFloat_FromDouble)

GET_FIELD(sublat, obj.s_sublat, build_degrees)
GET_FIELD(sublong, obj.s_sublng, build_degrees)
GET_FIELD(elevation, obj.s_elev, PyFloat_FromDouble)
GET_FIELD(eclipsed, obj.s_eclipsed, PyBool_FromLong)

/* Attributes computed by obj_cir that need an Observer. */

#undef CARGS
#define CARGS ,1

GET_FIELD(az, obj.s_az, build_degrees)
GET_FIELD(alt, obj.s_alt, build_degrees)

GET_FIELD(range, obj.s_range, PyFloat_FromDouble)
GET_FIELD(range_velocity, obj.s_rangev, PyFloat_FromDouble)

#undef CALCULATOR
#undef CARGS

/* Attributes computed by riset_cir, which always require an Observer,
   and which might be None. */

#define CALCULATOR Body_riset_cir
#define CARGS
#define FLAGGED(mask, builder) \
 (body->riset.rs_flags & (mask | RS_NEVERUP)) \
 ? (Py_INCREF(Py_None), Py_None) : builder

GET_FIELD(rise_time, riset.rs_risetm,
	  FLAGGED(RS_CIRCUMPOLAR | RS_NORISE, build_Date))
GET_FIELD(rise_az, riset.rs_riseaz,
	  FLAGGED(RS_CIRCUMPOLAR | RS_NORISE, build_degrees))
GET_FIELD(transit_time, riset.rs_trantm, FLAGGED(RS_NOTRANS, build_Date))
GET_FIELD(transit_alt, riset.rs_tranalt, FLAGGED(RS_NOTRANS, build_degrees))
GET_FIELD(set_time, riset.rs_settm,
	  FLAGGED(RS_CIRCUMPOLAR | RS_NOSET, build_Date))
GET_FIELD(set_az, riset.rs_setaz,
	  FLAGGED(RS_CIRCUMPOLAR | RS_NOSET, build_degrees))

#undef CALCULATOR
#undef BODY

#define BODY Moon
#define CALCULATOR Moon_llibration

GET_FIELD(libration_lat, llat, build_degrees)
GET_FIELD(libration_long, llon, build_degrees)

#undef CALCULATOR

#define CALCULATOR Moon_colong

GET_FIELD(colong, c, build_degrees)
GET_FIELD(moon_phase, k, PyFloat_FromDouble)
GET_FIELD(subsolar_lat, s, build_degrees)

#undef CALCULATOR
#undef BODY

#define BODY Jupiter
#define CALCULATOR Jupiter_cml

GET_FIELD(cmlI, cmlI, build_degrees)
GET_FIELD(cmlII, cmlII, build_degrees)

#undef CALCULATOR
#undef BODY

#define BODY Saturn
#define CALCULATOR Saturn_satrings

GET_FIELD(earth_tilt, etilt, build_degrees)
GET_FIELD(sun_tilt, stilt, build_degrees)

#undef CALCULATOR
#undef BODY

/* Attribute access that needs to be hand-written. */

static PyObject *Get_name(PyObject *self, void *v)
{
     Body *body = (Body*) self;
     if (body->name) {
	  Py_INCREF(body->name);
	  return body->name;
     } else
	  return PyUnicode_FromString(body->obj.o_name);
}

static int Set_name(PyObject *self, PyObject *value, void *v)
{
     Body *body = (Body*) self;
     const char *name = PyUnicode_AsUTF8(value);
     if (!name)
          return -1;
     strncpy(body->obj.o_name, name, MAXNM);
     body->obj.o_name[MAXNM - 1] = '\0';
     Py_XDECREF(body->name);
     Py_INCREF(value);
     body->name = value;
     return 0;
}

static int Set_mag(PyObject *self, PyObject *value, void *v)
{
     Body *b = (Body*) self;
     double mag;
     if (PyNumber_AsDouble(value, &mag) == -1) return -1;
     set_fmag(&b->obj, mag);
     return 0;
}

static PyObject *Get_circumpolar(PyObject *self, void *v)
{
     Body *body = (Body*) self;
     if (Body_riset_cir(body, "circumpolar") == -1) return 0;
     return PyBool_FromLong(body->riset.rs_flags & RS_CIRCUMPOLAR);
}

static PyObject *Get_neverup(PyObject *self, void *v)
{
     Body *body = (Body*) self;
     if (Body_riset_cir(body, "neverup") == -1) return 0;
     return PyBool_FromLong(body->riset.rs_flags & RS_NEVERUP);
}

/* Allow getting and setting of EllipticalBody magnitude model
   coefficients, which can be either H/G or gk but which use the same
   storage either way. */

static PyObject *Get_HG(PyObject *self, void *v)
{
     Body *body = (Body*) self;
     if (body->obj.e_mag.whichm != MAG_HG) {
	  PyErr_Format(PyExc_RuntimeError,
		       "this object has g/k magnitude coefficients");
	  return 0;
     }
     return PyFloat_FromDouble(THE_FLOAT);
}

static PyObject *Get_gk(PyObject *self, void *v)
{
     Body *body = (Body*) self;
     if (body->obj.e_mag.whichm != MAG_gk) {
	  PyErr_Format(PyExc_RuntimeError,
		       "this object has H/G magnitude coefficients");
	  return 0;
     }
     return PyFloat_FromDouble(THE_FLOAT);
}

static int Set_HG(PyObject *self, PyObject *value, void *v)
{
     Body *body = (Body*) self;
     double n;
     if (PyNumber_AsDouble(value, &n) == -1) return -1;
     THE_FLOAT = (float) n;
     body->obj.e_mag.whichm = MAG_HG;
     return 0;
}

static int Set_gk(PyObject *self, PyObject *value, void *v)
{
     Body *body = (Body*) self;
     double n;
     if (PyNumber_AsDouble(value, &n) == -1) return -1;
     THE_FLOAT = (float) n;
     body->obj.e_mag.whichm = MAG_gk;
     return 0;
}

/* Get/Set arrays. */

#define OFF(member) offsetof(Body, obj.member)

static PyGetSetDef Body_getset[] = {
     {"name", Get_name, 0, "object name (read-only string)"},

     {"ha", Get_ha, 0, "hour angle" H},
     {"ra", Get_ra, 0, "right ascension" H},
     {"dec", Get_dec, 0, "declination" D},
     {"g_ra", Get_gaera, 0, "apparent geocentric right ascension" H},
     {"g_dec", Get_gaedec, 0, "apparent geocentric declination" D},
     {"a_ra", Get_astrora, 0, "astrometric geocentric right ascension" H},
     {"a_dec", Get_astrodec, 0, "astrometric geocentric declination" D},
     {"a_epoch", Get_epoch, 0,
      "date giving the equinox of the body's astrometric right"
      " ascension and declination",
     },
     {"elong", Get_elong, 0, "elongation" D},
     {"mag", Get_mag, 0, "magnitude"},
     {"size", Get_size, 0, "visual size in arcseconds"},
     {"radius", Get_radius, 0, "visual radius" D},

     {"az", Get_az, 0, "azimuth" D},
     {"alt", Get_alt, 0, "altitude" D},

     {"rise_time", Get_rise_time, 0, "rise time"},
     {"rise_az", Get_rise_az, 0, "azimuth at which the body rises" D},
     {"transit_time", Get_transit_time, 0, "transit time"},
     {"transit_alt", Get_transit_alt, 0, "transit altitude" D},
     {"set_time", Get_set_time, 0, "set time"},
     {"set_az", Get_set_az, 0, "azimuth at which the body sets" D},
     {"circumpolar", Get_circumpolar, 0,
      "whether object remains above the horizon this day"},
     {"neverup", Get_neverup, 0,
      "whether object never rises above the horizon this day"},
     {NULL}
};

static PyGetSetDef Planet_getset[] = {
     {"hlon", Get_hlong, 0,
      "heliocentric longitude (but Sun().hlon means the hlon of Earth)" D},
     {"hlat", Get_hlat, 0,
      "heliocentric latitude (but Sun().hlat means the hlat of Earth)" D},
     {"sun_distance", Get_sun_distance, 0, "distance from sun in AU"},
     {"earth_distance", Get_earth_distance, 0, "distance from earth in AU"},
     {"phase", Get_phase, 0, "phase as percent of the surface illuminated"},

     /* For compatibility with older scripts: */
     {"hlong", Get_hlong, 0, "heliocentric longitude" D},

     {NULL}
};

/* Some day, when planetary moons support all Body and Planet fields,
   this will become a list only of fields peculiar to planetary moons,
   and PlanetMoon will inherit from Planet; but for now, there is no
   inheritance, and this must list every fields valid for a planetary
   moon (see libastro's plmoon.c for details). */

static PyGetSetDef PlanetMoon_getset[] = {
     {"name", Get_name, 0, "object name (read-only string)"},

     {"a_ra", Get_astrora, 0, "astrometric geocentric right ascension" H},
     {"a_dec", Get_astrodec, 0, "astrometric geocentric declination" D},

     {"g_ra", Get_gaera, 0, "apparent geocentric right ascension" H},
     {"g_dec", Get_gaedec, 0, "apparent geocentric declination" D},

     {"ra", Get_ra, 0, "right ascension" H},
     {"dec", Get_dec, 0, "declination" D},

     {"az", Get_az, 0, "azimuth" D},
     {"alt", Get_alt, 0, "altitude" D},

     {"x", Get_x, 0, "how far east or west of its planet"
      " the moon lies in the sky in planet radii; east is positive"},
     {"y", Get_y, 0, "how far north or south of its planet"
      " the moon lies in the sky in planet radii; south is positive"},
     {"z", Get_z, 0, "how much closer or farther from Earth the moon is"
      " than its planet"
      " in planet radii; closer to Earth is positive"},

     {"earth_visible", Get_earth_visible, 0, "whether visible from earth"},
     {"sun_visible", Get_sun_visible, 0, "whether visible from sun"},
     {NULL}
};

static PyGetSetDef Moon_getset[] = {
     {"libration_lat", Get_libration_lat, 0, "lunar libration in latitude" D},
     {"libration_long", Get_libration_long, 0,
      "lunar libration in longitude" D},
     {"colong", Get_colong, 0, "lunar selenographic colongitude" D},
     {"moon_phase", Get_moon_phase, 0,
      "fraction of lunar surface illuminated when viewed from earth"},
     {"subsolar_lat", Get_subsolar_lat, 0,
      "lunar latitude of subsolar point" D},
     {NULL}
};

static PyGetSetDef Jupiter_getset[] = {
     {"cmlI", Get_cmlI, 0, "central meridian longitude, System I" D},
     {"cmlII", Get_cmlII, 0, "central meridian longitude, System II" D},
     {NULL}
};

static PyGetSetDef Saturn_getset[] = {
     {"earth_tilt", Get_earth_tilt, 0,
      "tilt of rings towards Earth, positive for southward tilt," D},
     {"sun_tilt", Get_sun_tilt, 0,
      "tilt of rings towards Sun, positive for southward tilt," D},
     {NULL}
};

static PyGetSetDef FixedBody_getset[] = {
     {"name", Get_name, Set_name, "object name"},
     {"mag", Get_mag, Set_mag, "magnitude", 0},
     {"_spect",  get_f_spect, set_f_spect, "spectral codes", 0},
     {"_ratio", get_f_ratio, set_f_ratio,
      "ratio of minor to major diameter", VOFF(f_ratio)},
     {"_pa", get_f_pa, set_f_pa, "position angle E of N", VOFF(f_pa)},
     {"_epoch", getd_mjd, setd_mjd, "epoch for _ra and _dec", VOFF(f_epoch)},
     {"_ra", getd_rh, setd_rh, "fixed right ascension", VOFF(f_RA)},
     {"_dec", getd_rd, setd_rd, "fixed declination", VOFF(f_dec)},
     {"_pmra", getf_proper_ra, setf_proper_ra,
      "right ascension proper motion", 0},
     {"_pmdec", getf_proper_dec, setf_proper_dec,
      "declination proper motion", 0},
     {NULL}
};

static PyMemberDef FixedBody_members[] = {
     {"_class", T_CHAR, OFF(f_class), 0, "object classification"},
     {NULL}
};

static PyGetSetDef EllipticalBody_getset[] = {
     {"name", Get_name, Set_name, "object name"},
     {"_inc", getf_dd, setf_dd, "inclination (degrees)", VOFF(e_inc)},
     {"_Om", getf_dd, setf_dd,
      "longitude of ascending node (degrees)", VOFF(e_Om)},
     {"_om", getf_dd, setf_dd, "argument of perihelion (degrees)", VOFF(e_om)},
     {"_M", getf_dd, setf_dd, "mean anomaly (degrees)", VOFF(e_M)},
     {"_epoch_M", getd_mjd, setd_mjd, "epoch of mean anomaly",
      VOFF(e_cepoch)},
     {"_epoch", getd_mjd, setd_mjd, "epoch for _inc, _Om, and _om",
      VOFF(e_epoch)},
     {"_H", Get_HG, Set_HG, "magnitude coefficient", VOFF(e_mag.m1)},
     {"_G", Get_HG, Set_HG, "magnitude coefficient", VOFF(e_mag.m2)},
     {"_g", Get_gk, Set_gk, "magnitude coefficient", VOFF(e_mag.m1)},
     {"_k", Get_gk, Set_gk, "magnitude coefficient", VOFF(e_mag.m2)},
     {NULL}
};

static PyMemberDef EllipticalBody_members[] = {
     {"_a", T_FLOAT, OFF(e_a), 0, "mean distance (AU)"},
     {"_size", T_FLOAT, OFF(e_size), 0, "angular size at 1 AU (arcseconds)"},
     {"_e", T_DOUBLE, OFF(e_e), 0, "eccentricity"},
     {NULL}
};

static PyGetSetDef HyperbolicBody_getset[] = {
     {"name", Get_name, Set_name, "object name"},
     {"_epoch", getd_mjd, setd_mjd, "epoch date of _inc, _Om, and _om",
      VOFF(h_epoch)},
     {"_epoch_p", getd_mjd, setd_mjd, "epoch of perihelion", VOFF(h_ep)},
     {"_inc", getf_dd, setf_dd, "inclination (degrees)", VOFF(h_inc)},
     {"_Om", getf_dd, setf_dd,
      "longitude of ascending node (degrees)", VOFF(h_Om)},
     {"_om", getf_dd, setf_dd,
      "argument of perihelion (degrees)", VOFF(h_om)},
     {NULL}
};

static PyMemberDef HyperbolicBody_members[] = {
     {"_e", T_FLOAT, OFF(h_e), 0, "eccentricity"},
     {"_q", T_FLOAT, OFF(h_qp), 0, "perihelion distance (AU)"},
     {"_g", T_FLOAT, OFF(h_g), 0, "magnitude coefficient"},
     {"_k", T_FLOAT, OFF(h_k), 0, "magnitude coefficient"},
     {"_size", T_FLOAT, OFF(h_size), 0, "angular size at 1 AU (arcseconds)"},
     {NULL}
};

static PyGetSetDef ParabolicBody_getset[] = {
     {"name", Get_name, Set_name, "object name"},
     {"_epoch", getd_mjd, setd_mjd, "reference epoch", VOFF(p_epoch)},
     {"_epoch_p", getd_mjd, setd_mjd, "epoch of perihelion", VOFF(p_ep)},
     {"_inc", getf_dd, setf_dd, "inclination (degrees)", VOFF(p_inc)},
     {"_om", getf_dd, setf_dd, "argument of perihelion (degrees)", VOFF(p_om)},
     {"_Om", getf_dd, setf_dd,
      "longitude of ascending node (degrees)", VOFF(p_Om)},
     {NULL}
};

static PyMemberDef ParabolicBody_members[] = {
     {"_q", T_FLOAT, OFF(p_qp), 0, "perihelion distance (AU)"},
     {"_g", T_FLOAT, OFF(p_g), 0, "magnitude coefficient"},
     {"_k", T_FLOAT, OFF(p_k), 0, "magnitude coefficient"},
     {"_size", T_FLOAT, OFF(p_size), 0, "angular size at 1 AU (arcseconds)"},
     {NULL}
};

static PyGetSetDef EarthSatellite_getset[] = {
     {"name", Get_name, Set_name, "object name"},
     {"epoch", getd_mjd, setd_mjd, "reference epoch (mjd)", VOFF(es_epoch)},

     /* backwards compatibility */

     {"_epoch", getd_mjd, setd_mjd, "reference epoch (mjd)", VOFF(es_epoch)},
     {"_inc", getf_dd, setf_dd, "inclination (degrees)", VOFF(es_inc)},
     {"_raan", getf_dd, setf_dd,
      "right ascension of ascending node (degrees)", VOFF(es_raan)},
     {"_ap", getf_dd, setf_dd,
      "argument of perigee at epoch (degrees)", VOFF(es_ap)},
     {"_M", getf_dd, setf_dd,
      "mean anomaly (degrees from perigee at epoch)", VOFF(es_M)},

     /* results */

     {"sublat", Get_sublat, 0, "latitude beneath satellite" D},
     {"sublong", Get_sublong, 0, "longitude beneath satellite" D},
     {"elevation", Get_elevation, 0, "height above sea level in meters"},
     {"range", Get_range, 0, "distance from observer to satellite in meters"},
     {"range_velocity", Get_range_velocity, 0,
      "range rate of change in meters per second"},
     {"eclipsed", Get_eclipsed, 0, "whether satellite is in earth's shadow"},
     {NULL}
};

static PyMemberDef EarthSatellite_members[] = {
     {"n", T_DOUBLE, OFF(es_n), 0,
      "mean motion (revolutions per day)"},
     {"inc", T_FLOAT, OFF(es_inc), 0,
      "orbit inclination (degrees)"},
     {"raan", T_FLOAT, OFF(es_raan), 0,
      "right ascension of ascending node (degrees)"},
     {"e", T_FLOAT, OFF(es_e), 0,
      "eccentricity"},
     {"ap", T_FLOAT, OFF(es_ap), 0,
      "argument of perigee at epoch (degrees)"},
     {"M", T_FLOAT, OFF(es_M), 0,
      "mean anomaly (degrees from perigee at epoch)"},
     {"decay", T_FLOAT, OFF(es_decay), 0,
      "orbit decay rate (revolutions per day-squared)"},
     {"drag", T_FLOAT, OFF(es_drag), 0,
      "object drag coefficient (per earth radius)"},
     {"orbit", T_INT, OFF(es_orbit), 0,
      "integer orbit number of epoch"},

     /* backwards compatibility */

     {"_n", T_DOUBLE, OFF(es_n), 0, "mean motion (revolutions per day)"},
     {"_e", T_FLOAT, OFF(es_e), 0, "eccentricity"},
     {"_decay", T_FLOAT, OFF(es_decay), 0,
      "orbit decay rate (revolutions per day-squared)"},
     {"_drag", T_FLOAT, OFF(es_drag), 0,
      "object drag coefficient (per earth radius)"},
     {"_orbit", T_INT, OFF(es_orbit), 0, "integer orbit number of epoch"},
     {"catalog_number", T_OBJECT, offsetof(EarthSatellite, catalog_number), 0,
      "catalog number from TLE file"},
     {NULL}
};

#undef OFF

/*
 * The Object corresponds to the `any' object fields of X,
 * and implements most of the computational methods.  While Fixed
 * inherits directly from an Object, all other object types
 * inherit from PlanetObject to make the additional position
 * fields available for inspection.
 *
 * Note that each subclass is exactly the size of its parent, which is
 * rather unusual but reflects the fact that all of these are just
 * various wrappers around the same X Obj structure.
 */
static char body_doc[] =
     "A celestial body, that can compute() its sky position";

static char earth_satellite_doc[] = "\
A satellite in orbit around the Earth, usually built by passing the\
 text of a TLE entry to the `ephem.readtle()` routine. You can read\
 and write its orbital parameters through the following attributes:\n\
\n\
_ap -- argument of perigee at epoch (degrees)\n\
_decay -- orbit decay rate (revolutions per day-squared)\n\
_drag -- object drag coefficient (per earth radius)\n\
_e -- eccentricity\n\
_epoch -- reference epoch (mjd)\n\
_inc -- inclination (degrees)\n\
_M -- mean anomaly (degrees from perigee at epoch)\n\
_n -- mean motion (revolutions per day)\n\
_orbit -- integer orbit number of epoch\n\
_raan -- right ascension of ascending node (degrees)\n\
";

static PyTypeObject BodyType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Body",
     sizeof(Body),
     0,
     Body_dealloc,		/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     Body_repr,			/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     Body_methods,		/* tp_methods */
     0,				/* tp_members */
     Body_getset,		/* tp_getset */
     0,				/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Body_init,			/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject PlanetType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Planet",
     sizeof(Planet),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     0,				/* tp_members */
     Planet_getset,		/* tp_getset */
     &BodyType,			/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Planet_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject PlanetMoonType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.PlanetMoon",
     sizeof(PlanetMoon),
     0,
     Body_dealloc,		/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     Body_repr,			/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     Body_methods,		/* tp_methods */
     0,				/* tp_members */
     PlanetMoon_getset,		/* tp_getset */
     0,				/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Planet_init,               /* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject JupiterType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Jupiter",
     sizeof(Jupiter),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     "Create a Body instance representing Jupiter.",			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     0,				/* tp_members */
     Jupiter_getset,		/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Jupiter_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject SaturnType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Saturn",
     sizeof(Saturn),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     "Create a Body instance representing Saturn.",			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     0,				/* tp_members */
     Saturn_getset,		/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Saturn_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject MoonType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.Moon",
     sizeof(Moon),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     "Create a Body Instance representing the Moon.",			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     0,				/* tp_members */
     Moon_getset,		/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     Moon_init,			/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject FixedBodyType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.FixedBody",
     sizeof(FixedBody),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     FixedBody_members,		/* tp_members */
     FixedBody_getset,		/* tp_getset */
     &BodyType,			/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     FixedBody_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject BinaryStarType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.BinaryStar",
     sizeof(BinaryStar),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     0,				/* tp_members */
     0,				/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     BinaryStar_init,		/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject EllipticalBodyType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.EllipticalBody",
     sizeof(EllipticalBody),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     EllipticalBody_members,	/* tp_members */
     EllipticalBody_getset,	/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     EllipticalBody_init,	/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject HyperbolicBodyType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.HyperbolicBody",
     sizeof(HyperbolicBody),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     HyperbolicBody_members,	/* tp_members */
     HyperbolicBody_getset,	/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     HyperbolicBody_init,	/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject ParabolicBodyType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.ParabolicBody",
     sizeof(ParabolicBody),
     0,
     0,				/* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
     body_doc,			/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     ParabolicBody_members,	/* tp_members */
     ParabolicBody_getset,	/* tp_getset */
     &PlanetType,		/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     ParabolicBody_init,	/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

static PyTypeObject EarthSatelliteType = {
     PyVarObject_HEAD_INIT(NULL, 0)
     "ephem.EarthSatellite",
     sizeof(EarthSatellite),
     0,
     EarthSatellite_dealloc,    /* tp_dealloc */
     0,				/* tp_print */
     0,				/* tp_getattr */
     0,				/* tp_setattr */
     0,				/* tp_compare */
     0,				/* tp_repr */
     0,				/* tp_as_number */
     0,				/* tp_as_sequence */
     0,				/* tp_as_mapping */
     0,				/* tp_hash */
     0,				/* tp_call */
     0,				/* tp_str */
     0,				/* tp_getattro */
     0,				/* tp_setattro */
     0,				/* tp_as_buffer */
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags */
     earth_satellite_doc,	/* tp_doc */
     0,				/* tp_traverse */
     0,				/* tp_clear */
     0,				/* tp_richcompare */
     0,				/* tp_weaklistoffset */
     0,				/* tp_iter */
     0,				/* tp_iternext */
     0,				/* tp_methods */
     EarthSatellite_members,	/* tp_members */
     EarthSatellite_getset,	/* tp_getset */
     &BodyType,			/* tp_base */
     0,				/* tp_dict */
     0,				/* tp_descr_get */
     0,				/* tp_descr_set */
     0,				/* tp_dictoffset */
     EarthSatellite_init,	/* tp_init */
     0,				/* tp_alloc */
     0,				/* tp_new */
     0,				/* tp_free */
};

/* Return the current time. */

static PyObject* build_now(PyObject *self)
{
     return build_Date(mjd_now());
}

/* Compute the separation between two objects. */

static int separation_arg(PyObject *arg, double *lngi, double *lati)
{
     char err_message[] = "each separation argument "
	  "must be an Observer, an Body, "
	  "or a pair of numeric coordinates";
     if (PyObject_IsInstance(arg, (PyObject*) &ObserverType)) {
	  Observer *o = (Observer*) arg;
	  *lngi = o->now.n_lng;
	  *lati = o->now.n_lat;
	  return 0;
     } else if (PyObject_IsInstance(arg, (PyObject*) &BodyType)) {
	  Body *b = (Body*) arg;
	  if (Body_obj_cir(b, "ra", 0)) return -1;
	  *lngi = b->obj.s_ra;
	  *lati = b->obj.s_dec;
	  return 0;
     } else if (PySequence_Check(arg) && PySequence_Size(arg) == 2) {
          int rval = -1;
	  PyObject *lngo = 0, *lato = 0, *lngf = 0, *latf = 0;
	  lngo = PySequence_GetItem(arg, 0);
	  if (!lngo) goto fail;
	  lato = PySequence_GetItem(arg, 1);
	  if (!lato) goto fail;
	  if (!PyNumber_Check(lngo) || !PyNumber_Check(lato)) {
               PyErr_SetString(PyExc_TypeError, err_message);
               goto fail;
          }
	  lngf = PyNumber_Float(lngo);
	  if (!lngf) goto fail;
	  latf = PyNumber_Float(lato);
	  if (!latf) goto fail;
	  *lngi = PyFloat_AsDouble(lngf);
	  *lati = PyFloat_AsDouble(latf);
          rval = 0;
     fail:
          Py_XDECREF(lngo);
          Py_XDECREF(lato);
	  Py_XDECREF(lngf);
	  Py_XDECREF(latf);
          return rval;
     } else {
          PyErr_SetString(PyExc_TypeError, err_message);
          return -1;
     }
}

static PyObject* py_unrefract(PyObject *self, PyObject *args)
{
     double pr, tr, aa, ta;
     if (!PyArg_ParseTuple(args, "ddd:py_unrefract", &pr, &tr, &aa))
          return 0;
     unrefract(pr, tr, aa, &ta);
     return new_Angle(ta, raddeg(1));
}

static PyObject* separation(PyObject *self, PyObject *args)
{
     double plat, plng, qlat, qlng;
     double spy, cpy, px, qx, sqy, cqy, cosine;
     PyObject *p, *q;
     if (!PyArg_ParseTuple(args, "OO:separation", &p, &q)) return 0;
     if (separation_arg(p, &plng, &plat)) return 0;
     if (separation_arg(q, &qlng, &qlat)) return 0;

     /* rounding errors in the trigonometry might return >0 for this case */
     if ((plat == qlat) && (plng == qlng))
          return new_Angle(0.0, raddeg(1));

     spy = sin (plat);
     cpy = cos (plat);
     px = plng;
     qx = qlng;
     sqy = sin (qlat);
     cqy = cos (qlat);

     cosine = spy*sqy + cpy*cqy*cos(px-qx);
     if (cosine >= 1.0)  /* rounding sometimes makes this >1 */
          return new_Angle(0.0, raddeg(1));

     return new_Angle(acos(cosine), raddeg(1));
}

/* Read various database formats, with strings as input.  Note that
   `build_body_from_obj()` operates on a pair of borrowed references;
   the caller should DECREF `name` when the caller is done with it. */

static PyObject *build_body_from_obj(PyObject *name, Obj *op)
{
     PyTypeObject *type;
     Body *body;
     switch (op->o_type) {
     case FIXED:
	  type = &FixedBodyType;
	  break;
     case ELLIPTICAL:
	  type = &EllipticalBodyType;
	  break;
     case HYPERBOLIC:
	  type = &HyperbolicBodyType;
	  break;
     case PARABOLIC:
	  type = &ParabolicBodyType;
	  break;
     case EARTHSAT:
	  type = &EarthSatelliteType;
	  break;
     default:
	  PyErr_Format(PyExc_ValueError,
		       "cannot build object of unexpected type %d",
		       op->o_type);
	  return 0;
     }
     body = (Body*) PyType_GenericNew(type, 0, 0);
     if (!body)
	  return 0;
     body->obj = *op;
     if (Set_name((PyObject*) body, name, 0) == -1) {
          Py_DECREF(body);
          return 0;
     }
     return (PyObject*) body;
}

static PyObject* readdb(PyObject *self, PyObject *args)
{
     char *line, *comma, errmsg[256];
     PyObject *name;
     Obj obj;
     if (!PyArg_ParseTuple(args, "s:readdb", &line)) return 0;
     if (db_crack_line(line, &obj, 0, 0, errmsg) == -1) {
	  PyErr_SetString(PyExc_ValueError,
			  errmsg[0] ? errmsg :
			  "line does not conform to ephem database format");
	  return 0;
     }
     comma = strchr(line, ',');
     if (comma)
	  name = PyUnicode_FromStringAndSize(line, comma - line);
     else
	  name = PyUnicode_FromString(line);
     if (!name)
	  return 0;
     PyObject *body = build_body_from_obj(name, &obj);
     Py_DECREF(name);
     return body;
}

static PyObject* readtle(PyObject *self, PyObject *args)
{
     int result;
     const char *l0, *l1, *l2;
     PyObject *name, *stripped_name, *body, *catalog_number;
     Obj obj;
     if (!PyArg_ParseTuple(args, "O!ss:readtle",
			   &PyUnicode_Type, &name, &l1, &l2))
	  return 0;
     l0 = PyUnicode_AsUTF8(name);
     if (!l0)
          return 0;
     result = db_tle((char*) l0, (char*) l1, (char*) l2, &obj);
     if (result) {
	  PyErr_SetString(PyExc_ValueError,
			  (result == -2) ?
                          "incorrect TLE checksum at end of line" :
                          "line does not conform to tle format");
	  return 0;
     }
     stripped_name = PyObject_CallMethod(name, "strip", 0);
     if (!stripped_name)
	  return 0;
     body = build_body_from_obj(stripped_name, &obj);
     Py_DECREF(stripped_name);
     if (!body)
	  return 0;
     catalog_number = PyLong_FromLong((long) strtod(l1+2, 0));
     if (!catalog_number) {
          Py_DECREF(body);
	  return 0;
     }
     ((EarthSatellite*) body)->catalog_number = catalog_number;
     return body;
}

/* Create various sorts of angles. */

static PyObject *degrees(PyObject *self, PyObject *args)
{
     PyObject *o;
     double value;
     if (!PyArg_ParseTuple(args, "O:degrees", &o)) return 0;
     if (parse_angle(o, raddeg(1), &value) == -1) return 0;
     return new_Angle(value, raddeg(1));
}

static PyObject *hours(PyObject *self, PyObject *args)
{
     PyObject *o;
     double value;
     if (!PyArg_ParseTuple(args, "O:hours", &o)) return 0;
     if (parse_angle(o, radhr(1), &value) == -1) return 0;
     return new_Angle(value, radhr(1));
}

/* Finally, we wrap some helpful libastro functions. */

/* Return which page of a star atlas on which a location lies. */

static PyObject* uranometria(PyObject *self, PyObject *args)
{
     PyObject *rao, *deco;
     double ra, dec;
     if (!PyArg_ParseTuple(args, "OO:uranometria", &rao, &deco)) return 0;
     if (parse_angle(rao, radhr(1), &ra) == -1) return 0;
     if (parse_angle(deco, raddeg(1), &dec) == -1) return 0;
     return PyUnicode_FromString(um_atlas(ra, dec));
}

static PyObject* uranometria2000(PyObject *self, PyObject *args)
{
     PyObject *rao, *deco;
     double ra, dec;
     if (!PyArg_ParseTuple(args, "OO:uranometria2000", &rao, &deco)) return 0;
     if (parse_angle(rao, radhr(1), &ra) == -1) return 0;
     if (parse_angle(deco, raddeg(1), &dec) == -1) return 0;
     return PyUnicode_FromString(u2k_atlas(ra, dec));
}

static PyObject* millennium_atlas(PyObject *self, PyObject *args)
{
     PyObject *rao, *deco;
     double ra, dec;
     if (!PyArg_ParseTuple(args, "OO:millennium_atlas", &rao, &deco)) return 0;
     if (parse_angle(rao, radhr(1), &ra) == -1) return 0;
     if (parse_angle(deco, raddeg(1), &dec) == -1) return 0;
     return PyUnicode_FromString(msa_atlas(ra, dec));
}

/* Return in which constellation a particular coordinate lies. */

int cns_pick(double r, double d, double e);
char *cns_name(int id);

static PyObject* constellation
(PyObject *self, PyObject *args, PyObject *kwds)
{
     static char *kwlist[] = {"position", "epoch", NULL};
     PyObject *position_arg = 0, *epoch_arg = 0;
     PyObject *s0 = 0, *s1 = 0, *ora = 0, *odec = 0, *oepoch = 0;
     PyObject *result;
     double ra, dec, epoch = J2000;

     if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O:constellation", kwlist,
				      &position_arg, &epoch_arg))
	  return 0;

     if (PyObject_IsInstance(position_arg, (PyObject*) &BodyType)) {
	  Body *b = (Body*) position_arg;
	  if (epoch_arg) {
	       PyErr_SetString(PyExc_TypeError, "you cannot specify an epoch= "
			       "when providing a body for the position, since "
			       "bodies themselves specify the epoch of their "
			       "coordinates");
	       goto fail;
	  }
	  if (b->obj.o_flags == 0) {
	       PyErr_SetString(PyExc_TypeError, "you cannot ask about "
			       "the constellation in which a body "
			       "lies until you have used compute() to "
			       "determine its position");
	       goto fail;
	  }
	  if (Body_obj_cir(b, "ra", 0) == -1)
	       goto fail;
	  ra = b->obj.s_astrora;
	  dec = b->obj.s_astrodec;
	  epoch = b->now.n_epoch;
     } else {
	  if (!PySequence_Check(position_arg)) {
	       PyErr_SetString(PyExc_TypeError, "you must specify a position "
			       "by providing either a body or a sequence of "
			       "two numeric coordinates");
	       goto fail;
	  }
	  if (PySequence_Length(position_arg) != 2) {
	       PyErr_SetString(PyExc_ValueError, "the sequence specifying a "
			       "position must have exactly two coordinates");
	       goto fail;
	  }
	  if (epoch_arg)
	       if (parse_mjd(epoch_arg, &epoch) == -1) goto fail;

	  s0 = PySequence_GetItem(position_arg, 0);
	  if (!s0) goto fail;
	  s1 = PySequence_GetItem(position_arg, 1);
	  if (!s1 || !PyNumber_Check(s0) || !PyNumber_Check(s1)) goto fail;
	  ora = PyNumber_Float(s0);
	  if (!ora) goto fail;
	  odec = PyNumber_Float(s1);
	  if (!odec) goto fail;
	  ra = PyFloat_AsDouble(ora);
	  dec = PyFloat_AsDouble(odec);

	  if (epoch_arg) {
	       oepoch = PyNumber_Float(epoch_arg);
	       if (!oepoch) goto fail;
	       epoch = PyFloat_AsDouble(oepoch);
	  }
     }

     {
	  char *s = cns_name(cns_pick(ra, dec, epoch));
	  result = Py_BuildValue("s#s", s, 3, s+5);
	  goto leave;
     }

fail:
     result = 0;
leave:
     Py_XDECREF(s0);
     Py_XDECREF(s1);
     Py_XDECREF(ora);
     Py_XDECREF(odec);
     Py_XDECREF(oepoch);
     return result;
}

static PyObject *julian_date(PyObject *self, PyObject *args)
{
     PyObject *o = 0;
     double mjd;
     if (!PyArg_ParseTuple(args, "|O:julian_date", &o)) return 0;
     if (!o)
	  mjd = mjd_now();
     else if (PyObject_IsInstance(o, (PyObject*) &ObserverType))
	  mjd = ((Observer*) o)->now.n_mjd;
     else if (parse_mjd(o, &mjd) == -1)
	  return 0;
     return PyFloat_FromDouble(mjd + 2415020.0);
}

static PyObject *delta_t(PyObject *self, PyObject *args)
{
     PyObject *o = 0;
     double mjd;
     if (!PyArg_ParseTuple(args, "|O:delta_t", &o)) return 0;
     if (!o)
	  mjd = mjd_now();
     else if (PyObject_IsInstance(o, (PyObject*) &ObserverType))
	  mjd = ((Observer*) o)->now.n_mjd;
     else if (parse_mjd(o, &mjd) == -1)
	  return 0;
     return PyFloat_FromDouble(deltat(mjd));
}

static PyObject *moon_phases(PyObject *self, PyObject *args)
{
     PyObject *o = 0, *d;
     double mjd, mjn, mjf;
     if (!PyArg_ParseTuple(args, "|O:moon_phases", &o)) return 0;
     if (!o)
	  mjd = mjd_now();
     else if (PyObject_IsInstance(o, (PyObject*) &ObserverType))
	  mjd = ((Observer*) o)->now.n_mjd;
     else if (parse_mjd(o, &mjd) == -1)
	  return 0;
     moonnf(mjd, &mjn, &mjf);
     o = PyDict_New();
     if (!o) return 0;
     d = build_Date(mjn);
     if (!d) return 0;
     if (PyDict_SetItemString(o, "new", d) == -1) return 0;
     d = build_Date(mjf);
     if (!d) return 0;
     if (PyDict_SetItemString(o, "full", d) == -1) return 0;
     return o;
}

static PyObject *my_eq_ecl(PyObject *self, PyObject *args)
{
     double mjd, ra, dec, lg, lt;
     if (!PyArg_ParseTuple(args, "ddd:eq_ecl", &mjd, &ra, &dec)) return 0;
     eq_ecl(mjd, ra, dec, &lt, &lg);
     return Py_BuildValue("NN", build_degrees(lg), build_degrees(lt));
}

static PyObject *my_ecl_eq(PyObject *self, PyObject *args)
{
     double mjd, ra, dec, lg, lt;
     if (!PyArg_ParseTuple(args, "ddd:ecl_eq", &mjd, &lg, &lt)) return 0;
     ecl_eq(mjd, lt, lg, &ra, &dec);
     return Py_BuildValue("NN", build_hours(ra), build_degrees(dec));
}

static PyObject *my_eq_gal(PyObject *self, PyObject *args)
{
     double mjd, ra, dec, lg, lt;
     if (!PyArg_ParseTuple(args, "ddd:eq_gal", &mjd, &ra, &dec)) return 0;
     eq_gal(mjd, ra, dec, &lt, &lg);
     return Py_BuildValue("NN", build_degrees(lg), build_degrees(lt));
}

static PyObject *my_gal_eq(PyObject *self, PyObject *args)
{
     double mjd, ra, dec, lg, lt;
     if (!PyArg_ParseTuple(args, "ddd:gal_eq", &mjd, &lg, &lt)) return 0;
     gal_eq(mjd, lt, lg, &ra, &dec);
     return Py_BuildValue("NN", build_hours(ra), build_degrees(dec));
}

static PyObject *my_precess(PyObject *self, PyObject *args)
{
     double mjd1, mjd2, ra, dec;
     if (!PyArg_ParseTuple(args, "dddd:precess", &mjd1, &mjd2, &ra, &dec))
          return 0;
     precess(mjd1, mjd2, &ra, &dec);
     return Py_BuildValue("NN", build_hours(ra), build_degrees(dec));
}

static PyObject *_next_pass(PyObject *self, PyObject *args)
{
     Observer *observer;
     Body *body;
     RiseSet rs;
     if (!PyArg_ParseTuple(args, "O!O!", &ObserverType, &observer,
                           &BodyType, &body))
          return 0;
     riset_cir(& observer->now, & body->obj, - body->now.n_dip, & rs);
     if (rs.rs_flags & RS_CIRCUMPOLAR) {
          PyErr_SetString(PyExc_ValueError, "that satellite appears to be"
                          " circumpolar and so will never cross the horizon");
          return 0;
     }
     if (rs.rs_flags & RS_NEVERUP) {
          PyErr_SetString(PyExc_ValueError, "that satellite seems to stay"
                          " always below your horizon");
          return 0;
     }
     if (rs.rs_flags & RS_ERROR) {
          PyErr_SetString(PyExc_ValueError, "cannot find when that"
                          " satellite next crosses the horizon");
          return 0;
     }
     {
          PyObject *risetm, *riseaz, *trantm, *tranalt, *settm, *setaz;

          if (rs.rs_flags & RS_NORISE) {
               Py_INCREF(Py_None);
               risetm = Py_None;
               Py_INCREF(Py_None);
               riseaz = Py_None;
          } else {
               risetm = build_Date(rs.rs_risetm);
               riseaz = build_degrees(rs.rs_riseaz);
          }

          if (rs.rs_flags & (RS_NORISE | RS_NOSET | RS_NOTRANS)) {
               Py_INCREF(Py_None);
               trantm = Py_None;
               Py_INCREF(Py_None);
               tranalt = Py_None;
          } else {
               trantm = build_Date(rs.rs_trantm);
               tranalt = build_degrees(rs.rs_tranalt);
          }

          if (rs.rs_flags & (RS_NORISE | RS_NOSET)) {
               Py_INCREF(Py_None);
               settm = Py_None;
               Py_INCREF(Py_None);
               setaz = Py_None;
          } else {
               settm = build_Date(rs.rs_settm);
               setaz = build_degrees(rs.rs_setaz);
          }

          return Py_BuildValue(
               "(OOOOOO)", risetm, riseaz, trantm, tranalt, settm, setaz
               );
     }
}

/*
 * Why is this all the way down here?  It needs to refer to the type
 * objects themselves, which are defined rather late in the file.
 */

void Body__copy_struct(Body *body, Body *newbody)
{
     memcpy((void *)&newbody->now, (void *)&body->now, sizeof(Now));
     memcpy((void *)&newbody->obj, (void *)&body->obj, sizeof(Obj));
     memcpy((void *)&newbody->riset, (void *)&body->riset, sizeof(RiseSet));

     newbody->name = body->name;
     Py_XINCREF(newbody->name);

     if (PyObject_IsInstance((PyObject*) body, (PyObject*) &MoonType)) {
          Moon *a = (Moon *) newbody, *b = (Moon *) body;
          a->llat = b->llat;
          a->llon = b->llon;
          a->c = b->c;
          a->k = b->k;
          a->s = b->s;
     }
     if (PyObject_IsInstance((PyObject*) body, (PyObject*) &JupiterType)) {
          Jupiter *a = (Jupiter *) newbody, *b = (Jupiter *) body;
          a->cmlI = b->cmlI;
          a->cmlII = b->cmlII;
     }
     if (PyObject_IsInstance((PyObject*) body, (PyObject*) &SaturnType)) {
          Saturn *a = (Saturn *) newbody, *b = (Saturn *) body;
          a->etilt = b->etilt;
          a->stilt = b->stilt;
     }
     if (PyObject_IsInstance((PyObject*) body, (PyObject*) &EarthSatelliteType)) {
          EarthSatellite *a = (EarthSatellite*) newbody;
          EarthSatellite *b = (EarthSatellite*) body;
          a->catalog_number = b->catalog_number;
          Py_XINCREF(a->name);
     }
}

/*
 * The global methods table and the module initialization function.
 */

static PyMethodDef libastro_methods[] = {

     {"degrees", degrees, METH_VARARGS, "build an angle measured in degrees"},
     {"hours", hours, METH_VARARGS, "build an angle measured in hours of arc"},
     {"now", (PyCFunction) build_now, METH_NOARGS, "Return the current time"},
     {"readdb", readdb, METH_VARARGS, "Read an ephem database entry"},
     {"readtle", readtle, METH_VARARGS, "Read TLE-format satellite elements"},
     {"unrefract", py_unrefract, METH_VARARGS, "Reverse angle of refraction"},
     {"separation", separation, METH_VARARGS,
      "Return the angular separation between two objects or positions" D},

     {"uranometria", uranometria, METH_VARARGS,
      "given right ascension and declination (in radians), return the page"
      " of the original Uranometria displaying that location"},
     {"uranometria2000", uranometria2000, METH_VARARGS,
      "given right ascension and declination (in radians), return the page"
      " of the Uranometria 2000.0 displaying that location"},
     {"millennium_atlas", millennium_atlas, METH_VARARGS,
      "given right ascension and declination (in radians), return the page"
      " of the Millenium Star Atlas displaying that location"},

     {"constellation", (PyCFunction) constellation,
      METH_VARARGS | METH_KEYWORDS,
      "Return the constellation in which the object or coordinates lie"},
     {"julian_date", (PyCFunction) julian_date, METH_VARARGS,
      "Return the Julian date of the current time,"
      " or of an argument that can be converted into an ephem.Date."},
     {"delta_t", (PyCFunction) delta_t, METH_VARARGS,
      "Compute the difference between Terrestrial Time and Coordinated"
      " Universal Time."},
     {"moon_phases", (PyCFunction) moon_phases, METH_VARARGS,
      "compute the new and full moons nearest a given date"},

     {"eq_ecl", (PyCFunction) my_eq_ecl, METH_VARARGS,
      "compute the ecliptic longitude and latitude of an RA and dec"},
     {"ecl_eq", (PyCFunction) my_ecl_eq, METH_VARARGS,
      "compute the ecliptic longitude and latitude of an RA and dec"},
     {"eq_gal", (PyCFunction) my_eq_gal, METH_VARARGS,
      "compute the ecliptic longitude and latitude of an RA and dec"},
     {"gal_eq", (PyCFunction) my_gal_eq, METH_VARARGS,
      "compute the ecliptic longitude and latitude of an RA and dec"},

     {"precess", (PyCFunction) my_precess, METH_VARARGS,
      "precess a right ascension and declination to another equinox"},

     {"builtin_planets", (PyCFunction) builtin_planets, METH_NOARGS,
      "return the list of built-in planet objects"},

     {"_next_pass", (PyCFunction) _next_pass, METH_VARARGS,
      "Return as a tuple the next rising, culmination, and setting"
      " of an EarthSatellite"},

     {NULL}
};

#if PY_MAJOR_VERSION == 3

/* PyDoc_STRVAR(doc_module, */
/* "(Put documentation here.)"); */

static struct PyModuleDef libastro_module = {
     PyModuleDef_HEAD_INIT,
     "_libastro",
     "Astronomical calculations for Python",
     -1,
     libastro_methods
};

#endif

PyObject *PyInit__libastro(void)
{
     PyDateTime_IMPORT;

     /* Initialize pointers to objects external to this module. */

     AngleType.tp_base = &PyFloat_Type;
     DateType.tp_base = &PyFloat_Type;

     ObserverType.tp_new = PyType_GenericNew;
     BodyType.tp_new = PyType_GenericNew;
     PlanetMoonType.tp_new = PyType_GenericNew;

     /* Ready each type. */

     PyType_Ready(&AngleType);
     PyType_Ready(&DateType);

     PyType_Ready(&ObserverType);

     PyType_Ready(&BodyType);
     PyType_Ready(&PlanetType);
     PyType_Ready(&PlanetMoonType);

     PyType_Ready(&JupiterType);
     PyType_Ready(&SaturnType);
     PyType_Ready(&MoonType);

     PyType_Ready(&FixedBodyType);
     PyType_Ready(&BinaryStarType);
     PyType_Ready(&EllipticalBodyType);
     PyType_Ready(&HyperbolicBodyType);
     PyType_Ready(&ParabolicBodyType);
     PyType_Ready(&EarthSatelliteType);

#if PY_MAJOR_VERSION == 2
     module = Py_InitModule3("_libastro", libastro_methods,
                             "Astronomical calculations for Python");
#else
     module = PyModule_Create(&libastro_module);
#endif
     if (!module) return 0;

     {
	  struct {
	       char *name;
	       PyObject *obj;
	  } objects[] = {
	       { "Angle", (PyObject*) & AngleType },
	       { "Date", (PyObject*) & DateType },

	       { "Observer", (PyObject*) & ObserverType },

	       { "Body", (PyObject*) & BodyType },
	       { "Planet", (PyObject*) & PlanetType },
	       { "PlanetMoon", (PyObject*) & PlanetMoonType },
	       { "Jupiter", (PyObject*) & JupiterType },
	       { "Saturn", (PyObject*) & SaturnType },
	       { "Moon", (PyObject*) & MoonType },

	       { "FixedBody", (PyObject*) & FixedBodyType },
	       /* { "BinaryStar", (PyObject*) & BinaryStarType }, */
	       { "EllipticalBody", (PyObject*) & EllipticalBodyType },
	       { "ParabolicBody", (PyObject*) & ParabolicBodyType },
	       { "HyperbolicBody", (PyObject*) & HyperbolicBodyType },
	       { "EarthSatellite", (PyObject*) & EarthSatelliteType },

	       { "meters_per_au", PyFloat_FromDouble(MAU) },
	       { "earth_radius", PyFloat_FromDouble(ERAD) },
	       { "moon_radius", PyFloat_FromDouble(MRAD) },
	       { "sun_radius", PyFloat_FromDouble(SRAD) },

	       { "MJD0", PyFloat_FromDouble(MJD0) },
	       { "J2000", PyFloat_FromDouble(J2000) },

	       { NULL }
	  };
	  int i;
	  for (i=0; objects[i].name; i++)
	       if (PyModule_AddObject(module, objects[i].name, objects[i].obj)
		   == -1)
		    return 0;
     }

     /* Set a default preference. */

     pref_set(PREF_DATE_FORMAT, PREF_YMD);

     /* Tell libastro that we do not have data files anywhere. */

     setMoonDir(NULL);

     /* All done! */

     return module;
}

#if PY_MAJOR_VERSION == 2
#ifdef PyMODINIT_FUNC
PyMODINIT_FUNC
#else
DL_EXPORT(void)
#endif
init_libastro(void)
{
     PyInit__libastro();
}
#endif
