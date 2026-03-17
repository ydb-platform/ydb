# -*- coding: utf-8 -*-

mapping = (
    u"abcdefghijklmnopqrstuvwxyzABCDEFGHILJKMNOPQRSTUVWXYZ",
    u"abcdefghijklmnopqrstuvwxyzABCDEFGHILJKMNOPQRSTUVWXYZ",
)

reversed_specific_mapping = (
    u"àÀáÁâÂãÃèÈéÉêÊëËìÌíÍîÎïÏðÐñÑòÒóÓôÔõÕùÙúÚûÛýÝÿŸ",
    u"aAaAaAaAeEeEeEeEiIiIiIiIdDnNoOoOoOaOuUuUuUyYyY",
)

reversed_specific_pre_processor_mapping = {
    u"å": u"aa",
    u"Å": u"Aa",
    u"ä": u"ae",
    u"Ä": u"Ae",
    u"æ": u"ae",
    u"Æ": u"Ae",
    u"Ç": u"Ts",
    u"ç": u"ts",
    u"ð": u"dh",
    u"ö": u"oe",
    u"Ö": u"Oe",
    u"ø": u"oe",
    u"Ø": u"Oe",
    u"ü": u"ue",
    u"Ü": u"Ue",
    u"þ": u"th",
    u"Þ": u"Th",
    u"ß": u"ss",
}
