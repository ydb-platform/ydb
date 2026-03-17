from schwifty import checksum


# Mauretania (MR)
# Tunesia (TN)
@checksum.register("MR", "TN")
class DefaultAlgorithm(checksum.ISO7064_mod97_10):
    name = "default"

    def post_process(self, r: int) -> int:
        return 97 - r
