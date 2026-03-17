from schwifty import checksum


# Bosnia and Herzegovina (BT)
# Montenegro (ME)
# North Macedonia (MK)
# Portugal (PT)
# Serbia (RS)
# Slovenia (SI)
# East Timor (TL)
@checksum.register("BT", "ME", "MK", "PT", "RS", "SI", "TL")
class DefaultAlgorithm(checksum.ISO7064_mod97_10):
    name = "default"
