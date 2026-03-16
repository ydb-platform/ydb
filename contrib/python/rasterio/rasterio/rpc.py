import attr


@attr.s(slots=True)
class RPC:
    """Rational Polynomial Coefficients used to map (x, y, z) <-> (row, col) coordinates.

    This class contains a mapping between various RPC attributes and values.

    Attributes
    ----------
    err_bias, err_rand: float, optional
        The RMS bias and random error in meters per horizontal axis of all points in image.
    lat_off, long_off, height_off: float
        Geodetic latitude, longitude, and height offset.
    lat_scale, long_scale, height_scale: float
        Geodetic latitude, longitude, and height scaling.
    line_off, samp_off: float
        Line (row) and sample (column) offset.
    line_scale, samp_scale: float
        Line (row) and sample (column) offset.
    line_num_coeff, line_den_coeff, samp_num_coeff, samp_den_coeff: list
        The twenty coefficients describing a numerator or denominator polynomial corresponding to line (row) or sample (col).
    """

    height_off = attr.ib()
    height_scale = attr.ib()
    lat_off = attr.ib()
    lat_scale = attr.ib()
    line_den_coeff = attr.ib()
    line_num_coeff = attr.ib()
    line_off = attr.ib()
    line_scale = attr.ib()
    long_off = attr.ib()
    long_scale = attr.ib()
    samp_den_coeff = attr.ib()
    samp_num_coeff = attr.ib()
    samp_off = attr.ib()
    samp_scale = attr.ib()
    err_bias = attr.ib(default=None)
    err_rand = attr.ib(default=None)

    def to_dict(self):
        """Return a dictionary representation of RPC"""
        return attr.asdict(self)

    def to_gdal(self):
        """Serialize RPC attribute name and values in a form expected by GDAL.

        Returns
        -------
        dict

        Notes
        -----
        The `err_bias` and `err_rand` are optional, and are not written to datasets by GDAL.
        """

        out = {
            "HEIGHT_OFF": str(self.height_off),
            "HEIGHT_SCALE": str(self.height_scale),
            "LAT_OFF": str(self.lat_off),
            "LAT_SCALE": str(self.lat_scale),
            "LINE_DEN_COEFF": " ".join(map(str, self.line_den_coeff)),
            "LINE_NUM_COEFF": " ".join(map(str, self.line_num_coeff)),
            "LINE_OFF": str(self.line_off),
            "LINE_SCALE": str(self.line_scale),
            "LONG_OFF": str(self.long_off),
            "LONG_SCALE": str(self.long_scale),
            "SAMP_DEN_COEFF": " ".join(map(str, self.samp_den_coeff)),
            "SAMP_NUM_COEFF": " ".join(map(str, self.samp_num_coeff)),
            "SAMP_OFF": str(self.samp_off),
            "SAMP_SCALE": str(self.samp_scale)
        }

        if self.err_bias:
            out.update(ERR_BIAS=str(self.err_bias))
        if self.err_rand:
            out.update(ERR_RAND=str(self.err_rand))

        return out

    @classmethod
    def from_gdal(cls, rpcs):
        """Deserialize dict values to float or list.


        Returns
        -------
        RPC
        """
        out = {}

        for key, val in rpcs.items():
            # Four items have 20 floats in their values.
            if key in {"LINE_NUM_COEFF", "LINE_DEN_COEFF", "SAMP_NUM_COEFF", "SAMP_DEN_COEFF"}:
                out[key] = [float(v) for v in val.split(maxsplit=20)[:20]]
            # All other items have one float in their values but might also contain non-conforming extra text.
            else:
                out[key] = float(val.split(maxsplit=1)[0])

        return cls(
            err_bias=out.get("ERR_BIAS"),
            err_rand=out.get("ERR_RAND"),
            height_off=out["HEIGHT_OFF"],
            height_scale=out["HEIGHT_SCALE"],
            lat_off=out["LAT_OFF"],
            lat_scale=out["LAT_SCALE"],
            line_den_coeff=out["LINE_DEN_COEFF"],
            line_num_coeff=out["LINE_NUM_COEFF"],
            line_off=out["LINE_OFF"],
            line_scale=out["LINE_SCALE"],
            long_off=out["LONG_OFF"],
            long_scale=out["LONG_SCALE"],
            samp_den_coeff=out["SAMP_DEN_COEFF"],
            samp_num_coeff=out["SAMP_NUM_COEFF"],
            samp_off=out["SAMP_OFF"],
            samp_scale=out["SAMP_SCALE"],
        )
